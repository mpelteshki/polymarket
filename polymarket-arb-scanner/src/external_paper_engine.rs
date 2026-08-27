//! External dry-run execution using polymarket-paper-trader (pm-trader).
//!
//! This adapter is intentionally conservative:
//! - it refreshes CLOB quotes immediately before paper execution;
//! - it rebuilds basket sizing from refreshed order-book depth, not stale scanner depth;
//! - it uses the same slippage and tick-rounding logic as live execution;
//! - it measures basket parity in **basket units**, so ranked opportunities with
//!   unequal optimizer sizes can be evaluated correctly;
//! - it computes paper P&L from actual filled shares and CLOB fee metadata;
//! - it marks unhedged fill spend to zero in the conservative campaign summary.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use reqwest::Client as HttpClient;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::process::Command;
use tracing::{info, warn};

use crate::clob_client;
use crate::config::Config;
use crate::fees;
use crate::models::{
    is_external_token_id, is_supported_yes_no_full_family_plan, ArbType, ArbitrageOpportunity,
    Market, OpportunityLeg, OutcomeSide,
};

pub const PAPER_EXECUTION_ATTEMPTS_FILE: &str = "paper_execution_attempts.jsonl";
const PAPER_EXECUTION_ATTEMPT_SCHEMA_VERSION: u32 = 2;
const PAPER_EXECUTION_ROUTE: &str = "legged_clob_paper";
static PRODUCER_EXECUTABLE_SHA256: OnceLock<std::result::Result<String, String>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq)]
struct PaperOrderLeg {
    market_index: usize,
    market_slug: String,
    token_id: String,
    outcome: String,
    unit_shares: f64,
    shares: f64,
    amount_usd: f64,
    limit_price: f64,
    tick_size: f64,
    label: String,
    min_order_shares: f64,
}

#[derive(Debug, Clone)]
struct PlacedPaperOrder {
    order_id: i64,
    label: String,
}

#[derive(Debug, Clone)]
struct RawPaperTradeFill {
    trade_id: i64,
    amount_usd: f64,
    fee_usd: f64,
    shares: f64,
    avg_price: f64,
    is_partial: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PaperSubmissionKind {
    MarketTrade,
    LimitOrder,
}

impl PaperSubmissionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::MarketTrade => "market_trade",
            Self::LimitOrder => "limit_order",
        }
    }

    fn attribution_mode(self) -> &'static str {
        match self {
            Self::MarketTrade => "direct_trade_id",
            Self::LimitOrder => "exclusive_account_window",
        }
    }
}

#[derive(Debug, Clone)]
struct PaperSubmission {
    kind: PaperSubmissionKind,
    id: i64,
    market_slug: String,
    outcome: String,
    response_amount_usd: Option<f64>,
    response_shares: Option<f64>,
}

#[derive(Debug, Clone)]
struct ActualLegFill {
    market_slug: String,
    outcome: String,
    label: String,
    amount_usd: f64,
    fee_usd: f64,
    shares: f64,
    avg_price: f64,
    is_partial: bool,
    unit_shares: f64,
    fee_rate: f64,
    fee_exponent: u32,
    submission_kind: PaperSubmissionKind,
    submission_id: i64,
    trades: Vec<RawPaperTradeFill>,
}

#[derive(Debug)]
struct PaperAccountLock {
    #[cfg(unix)]
    file: File,
    key: String,
}

#[cfg(test)]
fn test_paper_account_lock() -> PaperAccountLock {
    #[cfg(unix)]
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")
            .expect("open test account-lock descriptor");
        PaperAccountLock {
            file,
            key: "test-account-lock".into(),
        }
    }
    #[cfg(not(unix))]
    {
        PaperAccountLock {
            key: "test-account-lock".into(),
        }
    }
}

#[cfg(unix)]
impl Drop for PaperAccountLock {
    fn drop(&mut self) {
        // SAFETY: `file` remains open for the lifetime of this guard.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

struct PaperPayoffCertificate {
    value: Value,
    supported_for_profit_evidence: bool,
    guaranteed_revenue_per_basket_unit: f64,
}

#[derive(Debug, Clone)]
struct BasketFillReport {
    planned_basket_units: f64,
    hedged_basket_units: f64,
    min_basket_units: f64,
    max_basket_units: f64,
    unit_drift_pct: f64,
    unit_shortfall_pct: f64,
    hedged_cost_usd: f64,
    hedged_projection_usd: f64,
    conservative_campaign_pnl_usd: f64,
    hedged_roi_pct: f64,
    conservative_campaign_roi_pct: f64,
    excess_notional_usd: f64,
    any_partial: bool,
    fills: Vec<ActualLegFill>,
}

struct PaperExecutionOutcome {
    report: PaperExecutionReport,
    fills: Vec<ActualLegFill>,
}

#[derive(Debug, Clone)]
pub struct PaperExecutionReport {
    pub attempt_id: String,
    pub planned_basket_units: f64,
    pub hedged_basket_units: f64,
    pub hedged_cost_usd: f64,
    /// Worst-case basket P&L: guaranteed hedged payout less every fill, fee, and gas outflow.
    pub conservative_pnl_usd: f64,
    pub conservative_roi_pct: f64,
    pub unhedged_notional_usd: f64,
    pub any_partial: bool,
    pub parity_ok: bool,
    pub fill_count: usize,
}

#[derive(Debug, Clone)]
struct PlanLegSnapshot {
    market: Market,
    raw_ask: f64,
    limit_price: f64,
}

fn paper_fill_evidence(fills: &[ActualLegFill]) -> (Vec<Value>, Vec<i64>) {
    let mut raw_trade_ids = Vec::new();
    let filled_legs = fills
        .iter()
        .map(|fill| {
            let raw_trades = fill
                .trades
                .iter()
                .map(|trade| {
                    raw_trade_ids.push(trade.trade_id);
                    json!({
                        "trade_id": trade.trade_id,
                        "shares": trade.shares,
                        "amount_usd": trade.amount_usd,
                        "avg_price": trade.avg_price,
                        "is_partial": trade.is_partial,
                        "fee_usd": trade.fee_usd,
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "market_slug": fill.market_slug,
                "outcome": fill.outcome,
                "label": fill.label,
                "unit_shares": fill.unit_shares,
                "shares": fill.shares,
                "notional_usd": fill.amount_usd,
                "avg_price": fill.avg_price,
                "is_partial": fill.is_partial,
                "fee_rate": fill.fee_rate,
                "fee_exponent": fill.fee_exponent,
                "recomputed_fee_usd": fill.fee_usd,
                "submission_kind": fill.submission_kind.as_str(),
                "submission_id": fill.submission_id,
                "attribution_mode": fill.submission_kind.attribution_mode(),
                "trade_ids": fill.trades.iter().map(|trade| trade.trade_id).collect::<Vec<_>>(),
                "raw_trades": raw_trades,
            })
        })
        .collect::<Vec<_>>();
    raw_trade_ids.sort_unstable();
    (filled_legs, raw_trade_ids)
}

fn append_paper_execution_attempt(config: &Config, record: &Value) -> Result<()> {
    let diagnostics_existed = config.diagnostics_dir.exists();
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating paper execution diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    if !diagnostics_existed {
        sync_parent_directory(&config.diagnostics_dir)?;
    }
    let path = config.diagnostics_dir.join(PAPER_EXECUTION_ATTEMPTS_FILE);
    let journal_existed = path.exists();
    let mut body = serde_json::to_vec(record).context("serializing paper execution attempt")?;
    body.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening paper execution attempt journal {}", path.display()))?;
    file.write_all(&body)
        .with_context(|| format!("writing paper execution attempt journal {}", path.display()))?;
    file.flush().with_context(|| {
        format!(
            "flushing paper execution attempt journal {}",
            path.display()
        )
    })?;
    file.sync_all()
        .with_context(|| format!("syncing paper execution attempt journal {}", path.display()))?;
    if !journal_existed {
        sync_directory(&config.diagnostics_dir)?;
    }
    Ok(())
}

fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_directory(if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    })
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .with_context(|| format!("opening directory for fsync {}", path.display()))?
            .sync_all()
            .with_context(|| format!("fsyncing directory {}", path.display()))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("opening executable for SHA-256: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("reading executable for SHA-256: {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn resolve_executable_path(command: &str) -> Result<PathBuf> {
    let command = command.trim();
    if command.is_empty() {
        bail!("external paper command is empty");
    }
    let direct = Path::new(command);
    if direct.is_absolute() || direct.components().count() > 1 {
        return fs::canonicalize(direct).with_context(|| {
            format!(
                "resolving external paper executable path {}",
                direct.display()
            )
        });
    }
    let path = std::env::var_os("PATH").ok_or_else(|| {
        anyhow!("PATH is unavailable while resolving external paper command '{command}'")
    })?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(command))
        .find(|candidate| candidate.is_file())
        .map(|candidate| fs::canonicalize(&candidate).unwrap_or(candidate))
        .ok_or_else(|| anyhow!("external paper command '{command}' was not found on PATH"))
}

fn producer_executable_sha256() -> Result<String> {
    PRODUCER_EXECUTABLE_SHA256
        .get_or_init(|| {
            let path = std::env::current_exe().map_err(|err| err.to_string())?;
            sha256_file(&path).map_err(|err| format!("{err:#}"))
        })
        .clone()
        .map_err(|err| anyhow!("hashing current paper producer executable failed: {err}"))
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut canonical = serde_json::Map::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_json(&values[key]));
            }
            Value::Object(canonical)
        }
        scalar => scalar.clone(),
    }
}

fn sha256_json(value: &Value) -> Result<String> {
    let body = serde_json::to_vec(&canonical_json(value))
        .context("serializing canonical paper execution profile")?;
    Ok(format!("{:x}", Sha256::digest(body)))
}

fn acquire_paper_account_lock(data_dir: &str, account: &str) -> Result<PaperAccountLock> {
    let directory = Path::new(data_dir);
    fs::create_dir_all(directory).with_context(|| {
        format!(
            "creating external paper data directory before account lock: {}",
            directory.display()
        )
    })?;
    let canonical_directory = fs::canonicalize(directory).with_context(|| {
        format!(
            "canonicalizing external paper data directory before account lock: {}",
            directory.display()
        )
    })?;
    let key_body = format!("{}\0{}", canonical_directory.display(), account);
    let key = format!("{:x}", Sha256::digest(key_body.as_bytes()));
    let path = canonical_directory.join(format!(".scanner-account-{key}.lock"));

    #[cfg(unix)]
    {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening paper account lock {}", path.display()))?;
        // SAFETY: `file` is a valid open descriptor and is retained by the guard.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            bail!(
                "another paper scanner holds account lock {} for account '{}' in {}: {}",
                path.display(),
                account,
                canonical_directory.display(),
                std::io::Error::last_os_error(),
            );
        }
        Ok(PaperAccountLock { file, key })
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        bail!("exclusive paper-account evidence locking is unsupported on this platform")
    }
}

fn is_supported_bundle_plan(opp: &ArbitrageOpportunity) -> bool {
    if opp.arb_type != ArbType::Bundle || opp.markets.len() != 1 || opp.execution_plan.len() != 2 {
        return false;
    }
    let market = &opp.markets[0];
    let condition_id = market.condition_id.trim();
    if condition_id.is_empty()
        || market.clob_token_id_yes.trim().is_empty()
        || market.clob_token_id_no.trim().is_empty()
    {
        return false;
    }
    let mut saw_yes = false;
    let mut saw_no = false;
    for leg in &opp.execution_plan {
        if leg.market_index != 0
            || leg.condition_id.trim() != condition_id
            || leg.market_slug != market.market_slug
            || (leg.unit_shares - 1.0).abs() > f64::EPSILON
        {
            return false;
        }
        match leg.outcome {
            OutcomeSide::Yes if leg.token_id.trim() == market.clob_token_id_yes.trim() => {
                if saw_yes {
                    return false;
                }
                saw_yes = true;
            }
            OutcomeSide::No if leg.token_id.trim() == market.clob_token_id_no.trim() => {
                if saw_no {
                    return false;
                }
                saw_no = true;
            }
            _ => return false,
        }
    }
    saw_yes && saw_no
}

fn paper_payoff_certificate(opp: &ArbitrageOpportunity) -> PaperPayoffCertificate {
    let mut raw_condition_ids = opp
        .markets
        .iter()
        .map(|market| market.condition_id.trim().to_string())
        .collect::<Vec<_>>();
    raw_condition_ids.sort();
    let (supported, topology, derived_revenue) = match opp.arb_type {
        ArbType::Yes if is_supported_yes_no_full_family_plan(opp) => {
            (true, "yes_full_family", Some(1.0))
        }
        ArbType::No if is_supported_yes_no_full_family_plan(opp) => {
            (true, "no_full_family", Some((opp.markets.len() - 1) as f64))
        }
        ArbType::Bundle if is_supported_bundle_plan(opp) => {
            (true, "binary_yes_no_bundle", Some(1.0))
        }
        ArbType::Yes | ArbType::No | ArbType::Bundle => (false, "invalid_topology", None),
        ArbType::MintSell | ArbType::Ranked => (false, "unsupported_arb_type", None),
    };
    let guaranteed_revenue_per_basket_unit = derived_revenue.unwrap_or(opp.guaranteed_revenue);
    PaperPayoffCertificate {
        value: json!({
            "schema_version": 1,
            "arb_type": opp.arb_type.to_string(),
            "supported_for_profit_evidence": supported,
            "topology": topology,
            "raw_market_count": opp.markets.len(),
            "raw_condition_ids": raw_condition_ids,
            "derived_guaranteed_revenue_per_basket_unit": derived_revenue,
        }),
        supported_for_profit_evidence: supported,
        guaranteed_revenue_per_basket_unit,
    }
}

fn paper_gas_policy_floor(config: &Config, leg_count: usize) -> f64 {
    if config.assume_gasless_for_proxy_signature_types && config.live_signature_type != 0 {
        0.0
    } else {
        config.gas_fallback_usd.max(0.0) * leg_count as f64
    }
}

const PAPER_PRE_SUBMIT_REJECTION_PREFIX: &str = "paper_pre_submit_rejection_v1";

#[derive(Debug)]
struct PaperPreSubmitRejection {
    code: &'static str,
    detail: String,
}

impl std::fmt::Display for PaperPreSubmitRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "paper pre-submit rejected [{}]: {}",
            self.code, self.detail
        )
    }
}

impl std::error::Error for PaperPreSubmitRejection {}

fn classify_pre_submit<T>(code: &'static str, result: Result<T>) -> Result<T> {
    result.map_err(|error| {
        anyhow!(PaperPreSubmitRejection {
            code,
            detail: format!("{error:#}"),
        })
    })
}

fn pre_submit_rejection(code: &'static str, detail: impl Into<String>) -> anyhow::Error {
    anyhow!(PaperPreSubmitRejection {
        code,
        detail: detail.into(),
    })
}

pub struct PaperFailureTradeLog {
    pub status: &'static str,
    pub note: String,
}

pub fn paper_failure_trade_log(error: &anyhow::Error) -> PaperFailureTradeLog {
    if let Some(rejection) = error.downcast_ref::<PaperPreSubmitRejection>() {
        return PaperFailureTradeLog {
            status: "pre_submit_rejected",
            note: format!(
                "{PAPER_PRE_SUBMIT_REJECTION_PREFIX}={}; {}",
                rejection.code, rejection.detail
            ),
        };
    }
    PaperFailureTradeLog {
        status: "error",
        note: paper_error_trade_note(error),
    }
}

pub fn paper_error_trade_note(error: &anyhow::Error) -> String {
    let detail = format!("{error:#}");
    let attempt_id = detail.split("paper_attempt_id=").nth(1).and_then(|tail| {
        let candidate = tail
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
            .collect::<String>();
        uuid::Uuid::parse_str(&candidate)
            .is_ok()
            .then_some(candidate)
    });
    match attempt_id {
        Some(attempt_id) => {
            format!("{detail}; paper_attempt_id={attempt_id}; paper_attempt_status=error")
        }
        None => format!("{detail}; paper_attempt_status=error"),
    }
}

fn round_down_to_step(value: f64, step: f64) -> f64 {
    let step = if step.is_finite() && step > 0.0 {
        step
    } else {
        0.0001
    };
    ((value / step).floor() * step * 1_000_000.0).round() / 1_000_000.0
}

fn round_down_to_cents(value: f64) -> f64 {
    round_down_to_step(value, 0.01)
}

fn round_to_cents(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn paper_order_amount_usd(
    shares: f64,
    expected_price: f64,
    limit_price: f64,
    use_limit: bool,
) -> f64 {
    if use_limit {
        round_down_to_cents(limit_price * shares)
    } else {
        round_to_cents(expected_price * shares)
    }
}

fn limit_buy_amount_arg(leg: &PaperOrderLeg) -> String {
    format!("{:.2}", leg.amount_usd)
}

fn live_style_limit_price(raw_ask: f64, market: &Market, config: &Config) -> f64 {
    let adjusted = raw_ask * (1.0 + config.live_slippage_bps as f64 / 10_000.0);
    clob_client::round_up_to_tick(adjusted.min(0.99), market.tick_size())
}

fn ensure_paper_submit_fresh(final_refresh_started_at: Instant, config: &Config) -> Result<()> {
    let age = final_refresh_started_at.elapsed();
    let max_age = Duration::from_millis(config.live_max_refresh_to_submit_ms.max(1));
    if age > max_age {
        bail!(
            "paper execution aborted before submit: final quote refresh age={}ms > LIVE_MAX_REFRESH_TO_SUBMIT_MS={}ms",
            age.as_millis(),
            config.live_max_refresh_to_submit_ms,
        );
    }
    Ok(())
}

fn inferred_gas_cost(opp: &ArbitrageOpportunity) -> f64 {
    opp.estimated_total_gas_cost_usd.max(0.0)
}

fn ensure_signal_fresh(opp: &ArbitrageOpportunity, config: &Config) -> Result<()> {
    let age_secs = (Utc::now() - opp.detected_at).num_seconds();
    if !config.signal_is_fresh(age_secs) {
        bail!(
            "signal too old for paper execution: age={}s > MAX_SIGNAL_AGE_SECONDS={}s",
            age_secs,
            config.max_signal_age_secs
        );
    }
    Ok(())
}

fn reject_external_token_opportunity(opp: &ArbitrageOpportunity) -> Result<()> {
    for leg in &opp.execution_plan {
        let token_id = leg.token_id.trim();
        if is_external_token_id(token_id) {
            bail!(
                "paper execution refuses external token id '{}' for event {} ({})",
                token_id,
                opp.event_id,
                opp.arb_type
            );
        }
    }
    for market in &opp.markets {
        for token_id in [&market.clob_token_id_yes, &market.clob_token_id_no] {
            let token_id = token_id.trim();
            if is_external_token_id(token_id) {
                bail!(
                    "paper execution refuses external token id '{}' for event {} ({})",
                    token_id,
                    opp.event_id,
                    opp.arb_type
                );
            }
        }
    }
    Ok(())
}

fn plan_market<'a>(markets: &'a [Market], leg: &OpportunityLeg) -> Result<&'a Market> {
    markets.get(leg.market_index).ok_or_else(|| {
        anyhow!(
            "execution plan references missing market index {}",
            leg.market_index
        )
    })
}

fn plan_leg_ask(market: &Market, outcome: OutcomeSide) -> Option<f64> {
    match outcome {
        OutcomeSide::Yes => market.clob_yes_ask,
        OutcomeSide::No => market.clob_no_ask,
    }
}

fn required_quotes_present(markets: &[Market], plan: &[OpportunityLeg]) -> bool {
    plan.iter().all(|leg| {
        plan_market(markets, leg)
            .map(|market| !market.closed && market.has_full_quote_for_outcome(leg.outcome))
            .unwrap_or(false)
    })
}

fn basket_unit_step(plan: &[OpportunityLeg], config: &Config) -> f64 {
    plan.iter()
        .filter_map(|leg| {
            if leg.unit_shares > f64::EPSILON {
                Some(config.order_size_step_shares / leg.unit_shares)
            } else {
                None
            }
        })
        .fold(config.order_size_step_shares, f64::max)
}

fn executable_position_size_usd(opp: &ArbitrageOpportunity, config: &Config) -> f64 {
    let requested = config.effective_paper_position_size_usd();
    if opp.max_executable_size_usd.is_finite() && opp.max_executable_size_usd > 0.0 {
        requested.min(opp.max_executable_size_usd)
    } else {
        requested
    }
}

fn refreshed_plan_snapshots(
    markets: &[Market],
    opp: &ArbitrageOpportunity,
    config: &Config,
) -> Result<Vec<PlanLegSnapshot>> {
    opp.execution_plan
        .iter()
        .map(|leg| {
            let market = plan_market(markets, leg)?.clone();
            let raw_ask = plan_leg_ask(&market, leg.outcome).ok_or_else(|| {
                anyhow!(
                    "missing refreshed {} ask for '{}'",
                    leg.outcome,
                    market.question
                )
            })?;
            let limit_price = live_style_limit_price(raw_ask, &market, config);
            Ok(PlanLegSnapshot {
                market,
                raw_ask,
                limit_price,
            })
        })
        .collect()
}

fn projected_trade_metrics(
    opp: &ArbitrageOpportunity,
    plan_snapshots: &[PlanLegSnapshot],
    basket_units: f64,
    _config: &Config,
    inferred_gas_cost_usd: f64,
    guaranteed_revenue_per_basket_unit: f64,
) -> Result<(f64, f64, f64, f64)> {
    if basket_units <= f64::EPSILON {
        bail!("basket units must be positive");
    }
    if plan_snapshots.len() != opp.execution_plan.len() {
        bail!("plan snapshot length mismatch");
    }

    let mut total_cost_usd = 0.0;
    let mut total_fees_usd = 0.0;
    for (leg, snapshot) in opp.execution_plan.iter().zip(plan_snapshots.iter()) {
        let shares = basket_units * leg.unit_shares;
        total_cost_usd += snapshot.limit_price * shares;
        let schedule = fees::verified_clob_fee_schedule(&snapshot.market).ok_or_else(|| {
            anyhow!(
                "paper projection missing freshly verified CLOB fd.r/fd.e for '{} [{}]'",
                leg.question,
                leg.outcome,
            )
        })?;
        total_fees_usd += fees::total_fee_with_curve(
            snapshot.limit_price,
            shares,
            schedule.rate,
            schedule.exponent,
        );
    }

    let projected_pnl_usd = basket_units * guaranteed_revenue_per_basket_unit
        - total_cost_usd
        - total_fees_usd
        - inferred_gas_cost_usd;
    let projected_roi_pct = if total_cost_usd > f64::EPSILON {
        projected_pnl_usd / total_cost_usd * 100.0
    } else {
        0.0
    };
    Ok((
        total_cost_usd,
        total_fees_usd,
        projected_pnl_usd,
        projected_roi_pct,
    ))
}

fn final_plan_snapshots_from_legs(
    legs: &[PaperOrderLeg],
    plan_snapshots: &[PlanLegSnapshot],
) -> Result<Vec<PlanLegSnapshot>> {
    if legs.len() != plan_snapshots.len() {
        bail!("paper leg count does not match refreshed plan snapshot count");
    }
    Ok(legs
        .iter()
        .zip(plan_snapshots.iter())
        .map(|(leg, snapshot)| PlanLegSnapshot {
            market: snapshot.market.clone(),
            raw_ask: snapshot.raw_ask,
            limit_price: leg.limit_price,
        })
        .collect())
}

async fn refresh_plan_fee_schedules(
    http: &HttpClient,
    config: &Config,
    opp: &ArbitrageOpportunity,
    plan_snapshots: &mut [PlanLegSnapshot],
) -> Result<()> {
    if plan_snapshots.len() != opp.execution_plan.len() {
        bail!("paper fee refresh plan snapshot length mismatch");
    }
    let condition_ids = opp
        .execution_plan
        .iter()
        .map(|leg| leg.condition_id.clone())
        .collect::<Vec<_>>();
    let schedules = clob_client::get_live_fee_schedules(http, config, &condition_ids)
        .await
        .context("paper pre-submit CLOB V2 fee refresh failed")?;

    for (leg, snapshot) in opp.execution_plan.iter().zip(plan_snapshots.iter_mut()) {
        if snapshot.market.condition_id.trim() != leg.condition_id.trim() {
            bail!(
                "paper fee refresh condition mismatch for '{} [{}]': snapshot={} plan={}",
                leg.question,
                leg.outcome,
                snapshot.market.condition_id,
                leg.condition_id,
            );
        }
        let schedule = schedules.get(leg.condition_id.trim()).ok_or_else(|| {
            anyhow!(
                "paper pre-submit CLOB V2 fee schedule missing condition_id={} for '{} [{}]'",
                leg.condition_id,
                leg.question,
                leg.outcome,
            )
        })?;
        snapshot.market.clob_fee_rate = Some(schedule.rate);
        snapshot.market.clob_fee_exponent = Some(schedule.exponent);
    }
    Ok(())
}

async fn refresh_and_validate(
    http: &HttpClient,
    config: &Config,
    opp: &ArbitrageOpportunity,
) -> Result<(Vec<Market>, Vec<PlanLegSnapshot>)> {
    reject_external_token_opportunity(opp)?;
    ensure_signal_fresh(opp, config)?;
    if opp.execution_plan.is_empty() {
        bail!("paper execution requires a non-empty execution plan");
    }

    let mut markets = opp.markets.clone();
    let ok = clob_client::enrich_event_markets(http, config, &mut markets).await;
    if !ok {
        bail!(
            "fresh CLOB enrichment was incomplete for event {} ({}); scan-time and partially refreshed quotes are not valid paper evidence",
            opp.event_id,
            opp.arb_type,
        );
    }

    if config.paper_require_full_clob_quotes
        && !required_quotes_present(&markets, &opp.execution_plan)
    {
        bail!(
            "paper execution requires full live CLOB coverage for event {} ({})",
            opp.event_id,
            opp.arb_type
        );
    }

    let plan_snapshots = refreshed_plan_snapshots(&markets, opp, config)?;

    Ok((markets, plan_snapshots))
}

async fn build_limit_legs(
    http: &HttpClient,
    config: &Config,
    opp: &ArbitrageOpportunity,
    plan_snapshots: &[PlanLegSnapshot],
) -> Result<(Vec<PaperOrderLeg>, f64)> {
    let requested_position_usd = executable_position_size_usd(opp, config);
    if requested_position_usd <= f64::EPSILON {
        bail!("non-positive paper position size after executable-size cap");
    }
    if plan_snapshots.len() != opp.execution_plan.len() {
        bail!("plan snapshot length mismatch");
    }

    let mut seen_tokens = HashSet::new();
    let mut token_ids = Vec::new();
    for leg in &opp.execution_plan {
        if leg.token_id.trim().is_empty() {
            bail!("execution plan leg '{}' is missing token id", leg.question);
        }
        if leg.unit_shares <= f64::EPSILON {
            bail!(
                "execution plan leg '{}' has non-positive unit_shares",
                leg.question
            );
        }
        if seen_tokens.insert(leg.token_id.clone()) {
            token_ids.push(leg.token_id.clone());
        }
    }
    let depth_snapshots = clob_client::get_depth_snapshots(http, config, &token_ids)
        .await
        .context("paper execution depth /books refresh failed")?;

    let mut max_basket_units_allowed = f64::INFINITY;
    let mut basket_cost_per_unit = 0.0;

    for (leg, snapshot) in opp.execution_plan.iter().zip(plan_snapshots.iter()) {
        let depth = depth_snapshots
            .get(&leg.token_id)
            .with_context(|| format!("paper execution depth /books missing {}", leg.token_id))?;
        let required_depth_usd = config
            .external_paper_min_order_usd
            .max(snapshot.market.min_order_size_shares() * snapshot.limit_price);
        let available_shares = depth.available_shares_at_price(snapshot.limit_price);
        let available_limit_notional = available_shares * snapshot.limit_price;
        if available_limit_notional + f64::EPSILON < required_depth_usd {
            bail!(
                "paper leg '{}' is not executable: depth ${available_limit_notional:.2} < required ${required_depth_usd:.2}",
                leg.question,
            );
        }

        let max_shares_for_leg = available_shares;
        max_basket_units_allowed =
            max_basket_units_allowed.min(max_shares_for_leg / leg.unit_shares);
        basket_cost_per_unit += snapshot.limit_price * leg.unit_shares;
    }

    if basket_cost_per_unit <= f64::EPSILON {
        bail!("paper basket total cost is non-positive");
    }

    let unit_step = basket_unit_step(&opp.execution_plan, config);
    let mut planned_basket_units = round_down_to_step(
        (requested_position_usd / basket_cost_per_unit).min(max_basket_units_allowed),
        unit_step,
    );
    if planned_basket_units <= f64::EPSILON {
        bail!("calculated paper basket size is non-positive");
    }

    // One depth-aware repricing pass at the actual intended basket size.
    let mut adjusted_snapshots = Vec::with_capacity(plan_snapshots.len());
    for (leg, snapshot) in opp.execution_plan.iter().zip(plan_snapshots.iter()) {
        let depth = depth_snapshots
            .get(&leg.token_id)
            .with_context(|| format!("paper execution depth /books missing {}", leg.token_id))?;
        let shares = planned_basket_units * leg.unit_shares;
        let avg_ask = depth.average_ask_for_shares(shares).with_context(|| {
            format!(
                "missing depth-aware average ask for paper leg '{}' at intended size",
                leg.question
            )
        })?;
        let cutoff_ask = depth.cutoff_ask_for_shares(shares).with_context(|| {
            format!(
                "missing depth-aware cutoff ask for paper leg '{}' at intended size",
                leg.question
            )
        })?;
        adjusted_snapshots.push(PlanLegSnapshot {
            market: snapshot.market.clone(),
            raw_ask: avg_ask,
            limit_price: live_style_limit_price(cutoff_ask, &snapshot.market, config),
        });
        if leg.unit_shares <= f64::EPSILON {
            bail!(
                "execution plan leg '{}' has non-positive unit_shares",
                leg.question
            );
        }
    }

    let adjusted_cost_per_unit: f64 = opp
        .execution_plan
        .iter()
        .zip(adjusted_snapshots.iter())
        .map(|(leg, snapshot)| leg.unit_shares * snapshot.limit_price)
        .sum();
    if adjusted_cost_per_unit > f64::EPSILON {
        planned_basket_units = round_down_to_step(
            (requested_position_usd / adjusted_cost_per_unit).min(max_basket_units_allowed),
            unit_step,
        );
    }
    if planned_basket_units <= f64::EPSILON {
        bail!("calculated paper basket size is non-positive after depth-aware repricing");
    }

    let mut legs = Vec::new();
    for (leg, snapshot) in opp
        .execution_plan
        .iter()
        .zip(adjusted_snapshots.into_iter())
    {
        let shares = planned_basket_units * leg.unit_shares;
        let depth = depth_snapshots
            .get(&leg.token_id)
            .with_context(|| format!("paper execution depth /books missing {}", leg.token_id))?;
        let expected_price = depth.average_ask_for_shares(shares).with_context(|| {
            format!(
                "missing depth-aware average ask for paper leg '{}' at final size",
                leg.question
            )
        })?;
        let cutoff_price = depth.cutoff_ask_for_shares(shares).with_context(|| {
            format!(
                "missing depth-aware cutoff ask for paper leg '{}' at final size",
                leg.question
            )
        })?;
        let limit_price = live_style_limit_price(cutoff_price, &snapshot.market, config);
        let amount_usd = paper_order_amount_usd(
            shares,
            expected_price,
            limit_price,
            config.effective_paper_use_limit_orders(),
        );
        let min_order_shares = snapshot.market.min_order_size_shares();

        if shares + f64::EPSILON < min_order_shares {
            bail!(
                "paper leg '{}' would be {:.4} shares, below market minimum {:.4} shares",
                leg.question,
                shares,
                min_order_shares,
            );
        }
        if amount_usd + f64::EPSILON < config.external_paper_min_order_usd {
            bail!(
                "paper leg '{}' would be ${amount_usd:.2}, below required minimum ${:.2}",
                leg.question,
                config.external_paper_min_order_usd,
            );
        }

        legs.push(PaperOrderLeg {
            market_index: leg.market_index,
            market_slug: leg.market_slug.clone(),
            token_id: leg.token_id.clone(),
            outcome: leg.outcome.as_str().to_string(),
            unit_shares: leg.unit_shares,
            shares,
            amount_usd,
            limit_price,
            tick_size: snapshot.market.tick_size(),
            label: leg.question.clone(),
            min_order_shares,
        });
    }

    Ok((legs, planned_basket_units))
}

fn is_rate_limit(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("429") || msg.contains("too many requests") || msg.contains("rate limit")
}

fn json_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_u64().map(|u| u as i64))
            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
    })
}

fn json_f64(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|v| {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
    })
}

fn json_bool(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(|v| {
        v.as_bool()
            .or_else(|| v.as_i64().map(|n| n != 0))
            .or_else(|| {
                v.as_str()
                    .map(|s| matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            })
    })
}

fn normalize_json_key(key: &str) -> String {
    key.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}

fn json_value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|n| n as f64))
        .or_else(|| value.as_u64().map(|n| n as f64))
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn json_find_f64_recursive(value: &Value, aliases: &[&str]) -> Option<f64> {
    let normalized_aliases: Vec<String> = aliases
        .iter()
        .map(|alias| normalize_json_key(alias))
        .collect();
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let normalized_key = normalize_json_key(key);
                if normalized_aliases
                    .iter()
                    .any(|alias| alias == &normalized_key)
                {
                    if let Some(number) = json_value_as_f64(child) {
                        return Some(number);
                    }
                }
            }
            for child in map.values() {
                if let Some(number) = json_find_f64_recursive(child, aliases) {
                    return Some(number);
                }
            }
            None
        }
        Value::Array(items) => items
            .iter()
            .find_map(|child| json_find_f64_recursive(child, aliases)),
        _ => None,
    }
}

pub struct ExternalPaperEngine {
    command: String,
    data_dir: String,
    account: String,
    account_lock: PaperAccountLock,
    order_type: String,
    limit_order_type: String,
    use_limit_orders: bool,
    filled_baskets: usize,
    parity_accepted_baskets: usize,
    parity_rejected_baskets: usize,
    attempted_legs: usize,
    executed_legs: usize,
    conservative_campaign_pnl_usd: f64,
    unhedged_notional_usd: f64,
}

impl ExternalPaperEngine {
    pub async fn new(config: &Config) -> Result<Self> {
        let resolved_command = resolve_executable_path(&config.external_paper_command)
            .context("resolving external paper adapter before initialization")?;
        let command = resolved_command.to_string_lossy().into_owned();
        let data_dir = config.external_paper_data_dir.to_string_lossy().to_string();
        let account = config.external_paper_account.clone();
        let order_type = config.external_paper_order_type.to_ascii_lowercase();
        let limit_order_type = config.external_paper_limit_order_type.to_ascii_lowercase();
        let use_limit_orders = config.effective_paper_use_limit_orders();

        if use_limit_orders {
            if limit_order_type != "gtc" {
                bail!(
                    "unsupported EXTERNAL_PAPER_LIMIT_ORDER_TYPE='{}'. Use 'gtc' when PAPER_USE_LIMIT_ORDERS=true; GTD is not supported because no per-order expiry is configured.",
                    limit_order_type
                );
            }
        } else if !matches!(order_type.as_str(), "fok" | "fak") {
            bail!(
                "unsupported EXTERNAL_PAPER_ORDER_TYPE='{}'. Use 'fok' or 'fak' when PAPER_USE_LIMIT_ORDERS=false.",
                order_type
            );
        }

        // Own this lock for the engine lifetime. Per-attempt locking permits
        // two scanners to alternate mutations on one account and corrupt the
        // campaign baseline even though their individual calls are serialized.
        let account_lock = acquire_paper_account_lock(&data_dir, &account)?;
        let mut engine = Self {
            command,
            data_dir,
            account,
            account_lock,
            order_type,
            limit_order_type,
            use_limit_orders,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };

        engine
            .ensure_initialized(config.external_paper_init_balance_usd)
            .await?;
        engine.sync_pending_orders().await?;

        Ok(engine)
    }

    async fn run_json(&self, args: &[String]) -> Result<Value> {
        let mut command = Command::new(&self.command);
        command.arg("--data-dir").arg(&self.data_dir);
        command.arg("--account").arg(&self.account);
        for arg in args {
            command.arg(arg);
        }

        let output = command.output().await.with_context(|| {
            format!(
                "failed to execute external paper command '{}': {:?}",
                self.command, args
            )
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            bail!(
                "external paper command failed (status={}): stdout='{}' stderr='{}'",
                output.status,
                stdout,
                stderr
            );
        }

        let stdout = String::from_utf8(output.stdout)
            .context("external paper command produced non-UTF8 output")?;
        let trimmed = stdout.trim();
        if trimmed.is_empty() {
            bail!("external paper command produced empty output");
        }

        let parsed: Value = serde_json::from_str(trimmed)
            .with_context(|| format!("failed to parse external paper JSON output: {}", trimmed))?;

        if parsed.get("ok").and_then(Value::as_bool).unwrap_or(false) {
            return Ok(parsed);
        }

        let message = parsed
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown external paper error");
        let code = parsed
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN");
        bail!("external paper API error ({code}): {message}")
    }

    async fn latest_trade_id(&self) -> Result<i64> {
        let args = vec![
            "history".to_string(),
            "--limit".to_string(),
            "1".to_string(),
        ];
        let value = self.run_json(&args).await?;
        Ok(value
            .get("data")
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| json_i64(row, "id"))
            .unwrap_or(0))
    }

    async fn recent_trades(&self, limit: usize) -> Result<Vec<Value>> {
        let args = vec![
            "history".to_string(),
            "--limit".to_string(),
            limit.max(1).to_string(),
        ];
        let value = self.run_json(&args).await?;
        Ok(value
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    pub async fn execute_canary(&mut self, amount_usd: f64, market_limit: usize) -> Result<Value> {
        let amount_usd = if amount_usd.is_finite() && amount_usd > 0.0 {
            amount_usd
        } else {
            1.0
        };
        let market_limit = market_limit.max(1);
        let markets = self
            .run_json(&[
                "markets".to_string(),
                "list".to_string(),
                "--limit".to_string(),
                market_limit.to_string(),
                "--sort".to_string(),
                "liquidity".to_string(),
            ])
            .await
            .context("paper execution canary failed to list markets")?;
        let candidates = markets
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut attempts = Vec::new();

        for market in candidates {
            let active = market
                .get("active")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let closed = market
                .get("closed")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            if !active || closed {
                continue;
            }
            let Some(slug) = market.get("slug").and_then(Value::as_str) else {
                continue;
            };
            let Some(outcome) = market
                .get("outcomes")
                .and_then(Value::as_array)
                .and_then(|outcomes| outcomes.first())
                .and_then(Value::as_str)
            else {
                continue;
            };
            let price = market
                .get("outcome_prices")
                .and_then(Value::as_array)
                .and_then(|prices| prices.first())
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            if !(0.001..0.999).contains(&price) {
                continue;
            }
            let question = market
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let buy_args = vec![
                "buy".to_string(),
                slug.to_string(),
                outcome.to_string(),
                format!("{amount_usd:.2}"),
                "--type".to_string(),
                "fok".to_string(),
            ];
            match self.run_json(&buy_args).await {
                Ok(buy) => {
                    let shares = buy
                        .get("data")
                        .and_then(|data| data.get("trade"))
                        .and_then(|trade| json_f64(trade, "shares"))
                        .unwrap_or(0.0);
                    let trade_id = buy
                        .get("data")
                        .and_then(|data| data.get("trade"))
                        .and_then(|trade| json_i64(trade, "id"));
                    if shares <= 0.0 || trade_id.is_none() {
                        attempts.push(json!({
                            "slug": slug,
                            "outcome": outcome,
                            "question": question,
                            "error": "buy_response_missing_positive_fill",
                            "response": buy,
                        }));
                        continue;
                    }
                    self.attempted_legs += 1;
                    self.executed_legs += 1;
                    let history = self
                        .run_json(&[
                            "history".to_string(),
                            "--limit".to_string(),
                            "10".to_string(),
                        ])
                        .await
                        .context("paper execution canary failed to read history")?;
                    let balance = self
                        .run_json(&["balance".to_string()])
                        .await
                        .context("paper execution canary failed to read balance")?;
                    let trade_count = history
                        .get("data")
                        .and_then(Value::as_array)
                        .map(Vec::len)
                        .unwrap_or(0);
                    return Ok(json!({
                        "ok": true,
                        "generated_at": Utc::now().to_rfc3339(),
                        "source": "rust_external_paper_engine",
                        "data_dir": self.data_dir,
                        "account": self.account,
                        "market": {
                            "slug": slug,
                            "outcome": outcome,
                            "question": question,
                        },
                        "amount_usd": amount_usd,
                        "live_trade_attempted": false,
                        "trade_id": trade_id,
                        "shares": shares,
                        "avg_price": buy
                            .get("data")
                            .and_then(|data| data.get("trade"))
                            .and_then(|trade| json_f64(trade, "avg_price")),
                        "order_type": buy
                            .get("data")
                            .and_then(|data| data.get("trade"))
                            .and_then(|trade| trade.get("order_type"))
                            .and_then(Value::as_str),
                        "trade_count": trade_count,
                        "buy": buy,
                        "history": history,
                        "balance": balance,
                        "failed_attempts": attempts,
                    }));
                }
                Err(err) => attempts.push(json!({
                    "slug": slug,
                    "outcome": outcome,
                    "question": question,
                    "error": format!("{err:#}"),
                })),
            }
        }

        bail!(
            "paper execution canary could not fill any candidate market; attempts={}",
            attempts.len()
        )
    }

    async fn collect_new_fills(
        &self,
        since_trade_id: i64,
        legs: &[PaperOrderLeg],
        plan_snapshots: &[PlanLegSnapshot],
        submissions: &[PaperSubmission],
        history_limit: usize,
    ) -> Result<Vec<ActualLegFill>> {
        if legs.len() != plan_snapshots.len() || legs.len() != submissions.len() {
            bail!("paper fill fee metadata does not match submitted leg count");
        }
        let rows = self.recent_trades(history_limit).await?;
        let mut by_leg: HashMap<(String, String), ActualLegFill> = HashMap::new();
        let mut planned_keys = HashSet::new();
        for leg in legs {
            if !planned_keys.insert((leg.market_slug.clone(), leg.outcome.clone())) {
                bail!(
                    "paper fill evidence requires unique market_slug/outcome legs; duplicate {} [{}]",
                    leg.market_slug,
                    leg.outcome,
                );
            }
        }
        let mut submission_by_key = HashMap::new();
        let mut submission_ids = HashSet::new();
        for submission in submissions {
            let key = (submission.market_slug.clone(), submission.outcome.clone());
            if !planned_keys.contains(&key)
                || submission_by_key.insert(key, submission).is_some()
                || submission.id <= 0
                || !submission_ids.insert((submission.kind, submission.id))
                || (submission.kind == PaperSubmissionKind::MarketTrade
                    && submission.id <= since_trade_id)
            {
                bail!("paper submission evidence is duplicate, replayed, or not in the plan");
            }
        }
        let mut seen_trade_ids = HashSet::new();
        let mut post_baseline_rows = 0_usize;

        for row in rows {
            let trade_id = json_i64(&row, "id")
                .ok_or_else(|| anyhow!("paper fill history contains a row without a trade id"))?;
            if trade_id <= since_trade_id {
                continue;
            }
            post_baseline_rows += 1;

            let side = row
                .get("side")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            if side != "buy" {
                bail!("paper account history contains unattributed post-baseline non-buy trade id {trade_id}");
            }

            let market_slug = row
                .get("market_slug")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let outcome = row
                .get("outcome")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            if market_slug.is_empty() || outcome.is_empty() {
                bail!("paper fill trade id {trade_id} is missing market/outcome attribution");
            }

            let Some((leg_index, leg)) = legs
                .iter()
                .enumerate()
                .find(|(_, leg)| leg.market_slug == market_slug && leg.outcome == outcome)
            else {
                bail!(
                    "paper account history contains unattributed post-baseline trade id {trade_id}"
                );
            };
            let submission = submission_by_key
                .get(&(market_slug.clone(), outcome.clone()))
                .copied()
                .ok_or_else(|| anyhow!("paper fill has no matching scanner submission"))?;
            if submission.kind == PaperSubmissionKind::MarketTrade && submission.id != trade_id {
                bail!(
                    "paper market submission/history id mismatch for {} [{}]: response={} history={trade_id}",
                    market_slug,
                    outcome,
                    submission.id,
                );
            }
            if by_leg.contains_key(&(market_slug.clone(), outcome.clone())) {
                bail!(
                    "paper account history has ambiguous multiple post-baseline fills for {} [{}]",
                    market_slug,
                    outcome,
                );
            }

            if trade_id <= 0 || !seen_trade_ids.insert(trade_id) {
                bail!("paper fill history contains duplicate/invalid trade id {trade_id}");
            }

            let amount_usd = json_f64(&row, "amount_usd").unwrap_or(0.0);
            let shares = json_f64(&row, "shares").unwrap_or(0.0);
            let is_partial = json_bool(&row, "is_partial").unwrap_or(false);
            if !amount_usd.is_finite()
                || amount_usd <= f64::EPSILON
                || !shares.is_finite()
                || shares <= f64::EPSILON
            {
                bail!(
                    "paper fill trade id {trade_id} has invalid amount/shares: amount={amount_usd} shares={shares}"
                );
            }
            let avg_price = if shares > f64::EPSILON {
                amount_usd / shares
            } else {
                0.0
            };
            if !avg_price.is_finite() || avg_price <= 0.0 || avg_price > 1.0 {
                bail!("paper fill trade id {trade_id} has invalid derived avg price {avg_price}");
            }
            if submission.response_amount_usd.is_some_and(|expected| {
                (expected - amount_usd).abs() > 1e-8_f64.max(expected.abs() * 1e-10)
            }) || submission.response_shares.is_some_and(|expected| {
                (expected - shares).abs() > 1e-8_f64.max(expected.abs() * 1e-10)
            }) {
                bail!("paper market submission/history economics mismatch for trade id {trade_id}");
            }
            // pm-trader's reported fee uses legacy endpoint semantics. Scanner
            // evidence always recomputes it from refreshed CLOB `fd.r`/`fd.e`.
            let schedule = fees::verified_clob_fee_schedule(&plan_snapshots[leg_index].market)
                .ok_or_else(|| {
                    anyhow!(
                        "paper fill for '{} [{}]' is missing supported CLOB fee metadata",
                        leg.label,
                        leg.outcome,
                    )
                })?;
            let fee_usd =
                fees::total_fee_with_curve(avg_price, shares, schedule.rate, schedule.exponent);

            let entry = by_leg
                .entry((market_slug.clone(), outcome.clone()))
                .or_insert_with(|| ActualLegFill {
                    market_slug: market_slug.clone(),
                    outcome: outcome.clone(),
                    label: leg.label.clone(),
                    amount_usd: 0.0,
                    fee_usd: 0.0,
                    shares: 0.0,
                    avg_price: 0.0,
                    is_partial: false,
                    unit_shares: leg.unit_shares,
                    fee_rate: schedule.rate,
                    fee_exponent: schedule.exponent,
                    submission_kind: submission.kind,
                    submission_id: submission.id,
                    trades: Vec::new(),
                });
            entry.amount_usd += amount_usd;
            entry.fee_usd += fee_usd;
            entry.shares += shares;
            entry.is_partial |= is_partial;
            entry.trades.push(RawPaperTradeFill {
                trade_id,
                amount_usd,
                fee_usd,
                shares,
                avg_price,
                is_partial,
            });
        }

        if post_baseline_rows != submissions.len() {
            bail!(
                "paper account attribution window contains {post_baseline_rows} trades for {} scanner submissions",
                submissions.len(),
            );
        }

        let mut fills = Vec::new();
        let mut missing = Vec::new();
        for leg in legs {
            let key = (leg.market_slug.clone(), leg.outcome.clone());
            match by_leg.remove(&key) {
                Some(mut fill) => {
                    fill.trades.sort_by_key(|trade| trade.trade_id);
                    fill.avg_price = if fill.shares > f64::EPSILON {
                        fill.amount_usd / fill.shares
                    } else {
                        0.0
                    };
                    fills.push(fill);
                }
                None => missing.push(format!("{} [{}]", leg.label, leg.outcome)),
            }
        }

        if !missing.is_empty() {
            bail!(
                "paper basket filled but could not recover all trade fills from history; missing: {}",
                missing.join(", ")
            );
        }

        Ok(fills)
    }

    fn analyze_basket_fills(
        &self,
        fills: Vec<ActualLegFill>,
        planned_basket_units: f64,
        guaranteed_revenue_per_basket_unit: f64,
        gas_cost_usd: f64,
    ) -> Result<BasketFillReport> {
        if fills.is_empty() {
            bail!("no paper fills found for basket analysis");
        }

        let mut min_units = f64::INFINITY;
        let mut max_units: f64 = 0.0;
        let mut any_partial = false;
        for fill in &fills {
            if fill.shares <= f64::EPSILON || fill.unit_shares <= f64::EPSILON {
                bail!(
                    "paper fill for '{}' has non-positive shares or unit_shares",
                    fill.label
                );
            }
            let units = fill.shares / fill.unit_shares;
            min_units = min_units.min(units);
            max_units = max_units.max(units);
            any_partial |= fill.is_partial;
        }

        let hedged_units = min_units;
        if !hedged_units.is_finite() || hedged_units <= f64::EPSILON {
            bail!("paper basket has no hedgeable basket units");
        }

        let unit_drift_pct = ((max_units - min_units) / hedged_units.max(0.0001)) * 100.0;
        let unit_shortfall_pct = if planned_basket_units > f64::EPSILON {
            ((planned_basket_units - hedged_units).max(0.0) / planned_basket_units) * 100.0
        } else {
            0.0
        };

        let mut hedged_cost = 0.0;
        let mut excess_notional_usd = 0.0;
        for fill in &fills {
            let realized_units = fill.shares / fill.unit_shares;
            let total_outflow = fill.amount_usd + fill.fee_usd;
            let hedged_fraction = (hedged_units / realized_units.max(0.0001)).clamp(0.0, 1.0);
            hedged_cost += total_outflow * hedged_fraction;
            excess_notional_usd += total_outflow * (1.0 - hedged_fraction);
        }

        let guaranteed_revenue = hedged_units * guaranteed_revenue_per_basket_unit;
        let hedged_projection_usd = guaranteed_revenue - hedged_cost - gas_cost_usd;
        // Any excess fill is not part of the guaranteed payoff. Mark its full
        // outflow to zero so a rejected/partial basket cannot inflate evidence.
        let conservative_campaign_pnl_usd = hedged_projection_usd - excess_notional_usd;
        let hedged_roi_pct = if hedged_cost > f64::EPSILON {
            hedged_projection_usd / hedged_cost * 100.0
        } else {
            0.0
        };
        let total_outflow_usd = hedged_cost + excess_notional_usd;
        let conservative_campaign_roi_pct = if total_outflow_usd > f64::EPSILON {
            conservative_campaign_pnl_usd / total_outflow_usd * 100.0
        } else {
            0.0
        };

        Ok(BasketFillReport {
            planned_basket_units,
            hedged_basket_units: hedged_units,
            min_basket_units: min_units,
            max_basket_units: max_units,
            unit_drift_pct,
            unit_shortfall_pct,
            hedged_cost_usd: hedged_cost,
            hedged_projection_usd,
            conservative_campaign_pnl_usd,
            hedged_roi_pct,
            conservative_campaign_roi_pct,
            excess_notional_usd,
            any_partial,
            fills,
        })
    }

    fn basket_matches_live_parity(&self, report: &BasketFillReport, config: &Config) -> bool {
        !report.any_partial
            && report.unit_drift_pct <= config.paper_max_share_mismatch_pct
            && report.unit_shortfall_pct <= config.paper_max_share_mismatch_pct
            && report.conservative_campaign_pnl_usd >= config.min_net_profit_usd
            && report.conservative_campaign_roi_pct >= config.min_roi_pct
    }

    async fn execute_leg_with_retries(
        &self,
        leg: &PaperOrderLeg,
        baseline_trade_id: i64,
        final_refresh_started_at: Instant,
        config: &Config,
    ) -> Result<PaperSubmission> {
        let max_rate_limit_retries = 3;
        let mut rate_limit_retries = 0;

        loop {
            ensure_paper_submit_fresh(final_refresh_started_at, config)?;
            let args = vec![
                "buy".to_string(),
                leg.market_slug.clone(),
                leg.outcome.clone(),
                format!("{:.2}", leg.amount_usd),
                "--type".to_string(),
                self.order_type.clone(),
            ];

            match self.run_json(&args).await {
                Ok(value) => {
                    let trade = value
                        .get("data")
                        .and_then(|data| data.get("trade"))
                        .and_then(Value::as_object)
                        .ok_or_else(|| anyhow!("pm-trader buy response missing trade object"))?;
                    let trade_value = Value::Object(trade.clone());
                    let trade_id = json_i64(&trade_value, "id")
                        .filter(|trade_id| *trade_id > baseline_trade_id)
                        .ok_or_else(|| {
                            anyhow!(
                                "pm-trader buy response has missing/replayed trade id at or below baseline {baseline_trade_id}"
                            )
                        })?;
                    let market_slug = trade
                        .get("market_slug")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let outcome = trade
                        .get("outcome")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    let side = trade
                        .get("side")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    let amount_usd = json_f64(&trade_value, "amount_usd").unwrap_or(0.0);
                    let shares = json_f64(&trade_value, "shares").unwrap_or(0.0);
                    if market_slug != leg.market_slug
                        || outcome != leg.outcome
                        || side != "buy"
                        || !amount_usd.is_finite()
                        || amount_usd <= f64::EPSILON
                        || !shares.is_finite()
                        || shares <= f64::EPSILON
                    {
                        bail!(
                            "pm-trader buy response does not match submitted leg '{} [{}]'",
                            leg.market_slug,
                            leg.outcome,
                        );
                    }
                    return Ok(PaperSubmission {
                        kind: PaperSubmissionKind::MarketTrade,
                        id: trade_id,
                        market_slug: leg.market_slug.clone(),
                        outcome: leg.outcome.clone(),
                        response_amount_usd: Some(amount_usd),
                        response_shares: Some(shares),
                    });
                }
                Err(err) if is_rate_limit(&err) && rate_limit_retries < max_rate_limit_retries => {
                    rate_limit_retries += 1;
                    let sleep_ms = 500 * (1 << rate_limit_retries);
                    warn!(
                        "Rate limited on {} ({}). Retrying in {}ms (attempt {}/{})",
                        leg.label,
                        leg.outcome,
                        sleep_ms,
                        rate_limit_retries,
                        max_rate_limit_retries
                    );
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn place_limit_order_with_retries(
        &self,
        leg: &PaperOrderLeg,
        final_refresh_started_at: Instant,
        config: &Config,
    ) -> Result<PlacedPaperOrder> {
        let max_rate_limit_retries = 3;
        let mut rate_limit_retries = 0;

        loop {
            ensure_paper_submit_fresh(final_refresh_started_at, config)?;
            let args = vec![
                "orders".to_string(),
                "place".to_string(),
                leg.market_slug.clone(),
                leg.outcome.clone(),
                "buy".to_string(),
                limit_buy_amount_arg(leg),
                clob_client::format_price_for_tick(leg.limit_price, leg.tick_size),
                "--type".to_string(),
                self.limit_order_type.clone(),
            ];

            match self.run_json(&args).await {
                Ok(value) => {
                    let order_id = value
                        .get("data")
                        .and_then(|data| json_i64(data, "id"))
                        .ok_or_else(|| {
                            anyhow!("pm-trader limit order response missing order id")
                        })?;
                    return Ok(PlacedPaperOrder {
                        order_id,
                        label: leg.label.clone(),
                    });
                }
                Err(err) if is_rate_limit(&err) && rate_limit_retries < max_rate_limit_retries => {
                    rate_limit_retries += 1;
                    let sleep_ms = 500 * (1 << rate_limit_retries);
                    warn!(
                        "Rate limited while placing limit order on {} ({}). Retrying in {}ms (attempt {}/{})",
                        leg.label,
                        leg.outcome,
                        sleep_ms,
                        rate_limit_retries,
                        max_rate_limit_retries
                    );
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn pending_limit_order_ids(&self) -> Result<HashSet<i64>> {
        let args = vec!["orders".to_string(), "list".to_string()];
        let value = self.run_json(&args).await?;
        let mut ids = HashSet::new();
        if let Some(items) = value.get("data").and_then(Value::as_array) {
            for item in items {
                if let Some(id) = json_i64(item, "id") {
                    ids.insert(id);
                }
            }
        }
        Ok(ids)
    }

    async fn cancel_limit_orders(&self, order_ids: &[i64]) {
        for order_id in order_ids {
            let args = vec![
                "orders".to_string(),
                "cancel".to_string(),
                order_id.to_string(),
            ];
            if let Err(err) = self.run_json(&args).await {
                warn!("failed to cancel paper limit order {}: {}", order_id, err);
            }
        }
    }

    async fn cancel_limit_orders_verified(&self, order_ids: &[i64]) -> Result<()> {
        if order_ids.is_empty() {
            return Ok(());
        }

        let mut cancel_errors = Vec::new();
        for order_id in order_ids {
            let args = vec![
                "orders".to_string(),
                "cancel".to_string(),
                order_id.to_string(),
            ];
            if let Err(err) = self.run_json(&args).await {
                cancel_errors.push(format!("{order_id}: {err}"));
            }
        }

        let pending = self
            .pending_limit_order_ids()
            .await
            .context("failed to verify pending paper limit orders after cancel")?;
        let remaining: Vec<i64> = order_ids
            .iter()
            .copied()
            .filter(|id| pending.contains(id))
            .collect();
        if !remaining.is_empty() {
            bail!(
                "paper limit order cleanup failed; still pending ids={remaining:?}; cancel_errors={}",
                if cancel_errors.is_empty() {
                    "none".to_string()
                } else {
                    cancel_errors.join(", ")
                }
            );
        }
        if !cancel_errors.is_empty() {
            warn!(
                "paper limit order cleanup saw cancel errors but verified no pending target orders remain: {}",
                cancel_errors.join(", ")
            );
        }
        Ok(())
    }

    async fn await_limit_basket(
        &self,
        placed_orders: &[PlacedPaperOrder],
        config: &Config,
    ) -> Result<()> {
        let target_ids: HashSet<i64> = placed_orders.iter().map(|o| o.order_id).collect();
        let mut filled_ids: HashSet<i64> = HashSet::new();
        let deadline = Instant::now() + Duration::from_secs(config.live_fill_poll_timeout_secs);
        let poll_every = Duration::from_millis(config.live_fill_poll_interval_ms.max(100));

        loop {
            let args = vec!["orders".to_string(), "check".to_string()];
            let value = self.run_json(&args).await?;
            let mut terminal_failures = Vec::new();

            if let Some(updates) = value.get("data").and_then(Value::as_array) {
                for update in updates {
                    let order = update.get("order").unwrap_or(&Value::Null);
                    let Some(order_id) = json_i64(order, "id") else {
                        continue;
                    };
                    if !target_ids.contains(&order_id) {
                        continue;
                    }

                    let action = update
                        .get("action")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    match action.as_str() {
                        "filled" => {
                            filled_ids.insert(order_id);
                        }
                        "rejected" | "expired" => {
                            let label = placed_orders
                                .iter()
                                .find(|p| p.order_id == order_id)
                                .map(|p| p.label.clone())
                                .unwrap_or_else(|| order_id.to_string());
                            let reason = update
                                .get("reason")
                                .and_then(Value::as_str)
                                .unwrap_or("unknown reason");
                            terminal_failures
                                .push(format!("{} (id={}): {}", label, order_id, reason));
                        }
                        _ => {}
                    }
                }
            }

            if !terminal_failures.is_empty() {
                let pending = self.pending_limit_order_ids().await.unwrap_or_default();
                let to_cancel: Vec<i64> = target_ids.intersection(&pending).copied().collect();
                self.cancel_limit_orders(&to_cancel).await;
                bail!(
                    "paper limit basket failed: {}",
                    terminal_failures.join(", ")
                );
            }

            let pending = self.pending_limit_order_ids().await?;
            let our_pending: Vec<i64> = target_ids.intersection(&pending).copied().collect();

            if filled_ids.len() == target_ids.len() {
                return Ok(());
            }

            if our_pending.is_empty() && filled_ids.len() < target_ids.len() {
                bail!(
                    "paper limit basket became incomplete: filled {}/{} legs and no pending orders remain",
                    filled_ids.len(),
                    target_ids.len()
                );
            }

            if Instant::now() >= deadline {
                self.cancel_limit_orders(&our_pending).await;
                bail!(
                    "paper limit basket timed out after {}s with {}/{} legs filled",
                    config.live_fill_poll_timeout_secs,
                    filled_ids.len(),
                    target_ids.len()
                );
            }

            tokio::time::sleep(poll_every).await;
        }
    }

    pub async fn sync_pending_orders(&self) -> Result<()> {
        if !self.use_limit_orders {
            return Ok(());
        }
        let args = vec!["orders".to_string(), "check".to_string()];
        self.run_json(&args)
            .await
            .context("failed to reconcile pending paper limit orders")?;
        let mut pending: Vec<i64> = self.pending_limit_order_ids().await?.into_iter().collect();
        pending.sort_unstable();
        if !pending.is_empty() {
            warn!(
                "found {} pending paper limit orders before new basket; cancelling and verifying clean account",
                pending.len()
            );
            self.cancel_limit_orders_verified(&pending).await?;
        }
        Ok(())
    }

    pub async fn reconcile_pending_orders_exclusive(&self) -> Result<()> {
        // The engine retains `account_lock` for its full lifetime.
        self.sync_pending_orders().await
    }

    async fn ensure_initialized(&mut self, init_balance_usd: f64) -> Result<()> {
        let balance_args = vec!["balance".to_string()];
        match self.run_json(&balance_args).await {
            Ok(_) => {
                info!("External paper engine ready (account='{}').", self.account);
                return Ok(());
            }
            Err(err) => {
                warn!("External paper engine balance check failed; attempting init: {err}");
            }
        }

        let init_args = vec![
            "init".to_string(),
            "--balance".to_string(),
            format!("{:.2}", init_balance_usd),
        ];
        self.run_json(&init_args)
            .await
            .context("failed to init external paper account")?;

        self.run_json(&balance_args)
            .await
            .context("paper account init verification failed")?;

        info!(
            "Initialized external paper account='{}' with starting balance ${:.2}.",
            self.account, init_balance_usd
        );
        Ok(())
    }

    pub async fn execute_opportunity(
        &mut self,
        opp: &ArbitrageOpportunity,
        config: &Config,
        http: &HttpClient,
    ) -> Result<PaperExecutionReport> {
        reject_external_token_opportunity(opp)?;
        classify_pre_submit("signal_freshness", ensure_signal_fresh(opp, config))?;
        if opp.execution_plan.is_empty() {
            bail!("paper execution requires a non-empty execution plan");
        }
        if self.use_limit_orders && opp.execution_plan.len() > config.max_batchable_legs() {
            bail!(
                "paper limit-parity mode refuses {}-leg basket because live batching is capped at {} legs",
                opp.execution_plan.len(),
                config.max_batchable_legs()
            );
        }
        let payoff_certificate = paper_payoff_certificate(opp);
        if !payoff_certificate.supported_for_profit_evidence {
            return Err(pre_submit_rejection(
                "payoff_certificate",
                format!(
                    "paper execution refuses unsupported profitability-evidence topology for {}",
                    opp.arb_type
                ),
            ));
        }

        let account_lock_key = self.account_lock.key.clone();

        if self.use_limit_orders {
            self.sync_pending_orders().await?;
        }

        // Resolve and hash immutable evidence identities before starting the
        // adapter baseline read or quote-to-submit freshness clock.  Every
        // adapter call uses this same canonical path.
        let launch_fingerprint = config
            .launch_config_fingerprint()
            .context("building paper attempt launch-config fingerprint")?;
        let producer_sha256 = producer_executable_sha256()?;
        let adapter_executable_path = resolve_executable_path(&self.command)?;
        let adapter_executable_sha256 = sha256_file(&adapter_executable_path)
            .context("hashing resolved external paper adapter executable")?;
        let order_mode = if self.use_limit_orders {
            "limit"
        } else {
            "market_style"
        };
        let effective_order_type = if self.use_limit_orders {
            self.limit_order_type.clone()
        } else {
            self.order_type.clone()
        };
        let live_order_type = config.live_order_type.trim().to_ascii_lowercase();
        let baseline_trade_id = self
            .latest_trade_id()
            .await
            .context("failed to read baseline paper trade history before submit")?;
        let final_refresh_started_at = Instant::now();
        let (_refreshed_markets, plan_snapshots) = classify_pre_submit(
            "fresh_refresh",
            refresh_and_validate(http, config, opp).await,
        )?;
        let (legs, planned_basket_units) = classify_pre_submit(
            "depth_validation",
            build_limit_legs(http, config, opp, &plan_snapshots).await,
        )?;
        let mut final_snapshots = final_plan_snapshots_from_legs(&legs, &plan_snapshots)?;
        classify_pre_submit(
            "fee_metadata",
            refresh_plan_fee_schedules(http, config, opp, &mut final_snapshots).await,
        )?;
        let planned_condition_tokens = opp
            .execution_plan
            .iter()
            .map(|leg| (leg.condition_id.clone(), leg.token_id.clone()))
            .collect::<Vec<_>>();
        classify_pre_submit(
            "orderability",
            clob_client::verify_live_orderable_markets(http, config, &planned_condition_tokens)
                .await
                .context("paper pre-submit CLOB orderability check failed"),
        )?;

        let payoff_certificate_sha256 = sha256_json(&payoff_certificate.value)?;
        let guaranteed_revenue_per_basket_unit =
            payoff_certificate.guaranteed_revenue_per_basket_unit;
        let gas_policy_floor_usd = paper_gas_policy_floor(config, legs.len());
        let gas_cost_usd = inferred_gas_cost(opp).max(gas_policy_floor_usd);
        let (projected_cost_usd, _projected_fees_usd, projected_pnl_usd, projected_roi_pct) =
            classify_pre_submit(
                "fee_projection",
                projected_trade_metrics(
                    opp,
                    &final_snapshots,
                    planned_basket_units,
                    config,
                    gas_cost_usd,
                    guaranteed_revenue_per_basket_unit,
                ),
            )?;
        if projected_pnl_usd < config.min_net_profit_usd || projected_roi_pct < config.min_roi_pct {
            return Err(pre_submit_rejection(
                "final_profit",
                format!(
                    "paper execution aborted after final sizing: projected_cost=${projected_cost_usd:.4} projected_pnl=${projected_pnl_usd:.4} projected_roi={projected_roi_pct:.2}% min_net=${:.4} min_roi={:.2}%",
                config.min_net_profit_usd,
                config.min_roi_pct,
                ),
            ));
        }

        classify_pre_submit(
            "submit_freshness",
            ensure_paper_submit_fresh(final_refresh_started_at, config),
        )?;
        let execution_profile = json!({
            "schema_version": 1,
            "execution_route": PAPER_EXECUTION_ROUTE,
            "live_route_compatible": false,
            "order_mode": order_mode,
            "effective_order_type": effective_order_type.clone(),
            "live_order_type": live_order_type.clone(),
            "paper_use_limit_orders_requested": config.paper_use_limit_orders,
            "effective_paper_use_limit_orders": self.use_limit_orders,
            "full_clob_required": config.paper_require_full_clob_quotes,
            "match_live_position_size": config.paper_match_live_position_size,
            "effective_position_size_usd": config.effective_paper_position_size_usd(),
            "live_position_size_usd": config.live_trade_position_size_usd,
            "paper_max_share_mismatch_pct": config.paper_max_share_mismatch_pct,
            "min_net_profit_usd": config.min_net_profit_usd,
            "min_roi_pct": config.min_roi_pct,
            "max_signal_age_secs": config.max_signal_age_secs,
            "gas_fallback_usd": config.gas_fallback_usd,
            "assume_gasless_for_proxy_signature_types": config.assume_gasless_for_proxy_signature_types,
            "live_signature_type": config.live_signature_type,
            "exclusive_paper_account_lock": true,
            "order_size_step_shares": config.order_size_step_shares,
            "validate_opportunities_at_target_size": config.validate_opportunities_at_target_size,
            "execute_only_full_clob_prices": config.execute_only_full_clob_prices,
            "live_slippage_bps": config.live_slippage_bps,
            "live_edge_haircut_usd": config.live_edge_haircut_usd,
            "live_edge_haircut_bps": config.live_edge_haircut_bps,
            "live_min_leg_size_usd": config.live_min_leg_size_usd,
            "live_max_refresh_to_submit_ms": config.live_max_refresh_to_submit_ms,
            "fresh_clob_enrichment_complete": true,
            "fresh_depth_complete": true,
            "fresh_fee_schedules_complete": true,
            "pre_submit_orderability_complete": true,
            "clob_api_url": config.clob_api_url,
            "gamma_api_url": config.gamma_api_url,
            "external_paper_command": self.command.clone(),
            "external_paper_executable_path": adapter_executable_path.to_string_lossy(),
            "external_paper_executable_sha256": adapter_executable_sha256.clone(),
            "producer_version": env!("CARGO_PKG_VERSION"),
            "producer_executable_sha256": producer_sha256.clone(),
        });
        let execution_profile_sha256 = sha256_json(&execution_profile)?;
        let attempt_id = uuid::Uuid::new_v4().to_string();
        let planned_legs = classify_pre_submit(
            "fee_metadata",
            legs.iter()
                .zip(final_snapshots.iter())
                .map(|(leg, snapshot)| {
                    let schedule =
                        fees::verified_clob_fee_schedule(&snapshot.market).ok_or_else(|| {
                            anyhow!(
                                "paper attempt journal missing freshly verified fd.r/fd.e for {}",
                                leg.label
                            )
                        })?;
                    Ok(json!({
                        "condition_id": snapshot.market.condition_id,
                        "token_id": leg.token_id,
                        "market_slug": leg.market_slug,
                        "outcome": leg.outcome,
                        "unit_shares": leg.unit_shares,
                        "shares": leg.shares,
                        "amount_usd": leg.amount_usd,
                        "limit_price": leg.limit_price,
                        "fee_rate": schedule.rate,
                        "fee_exponent": schedule.exponent,
                    }))
                })
                .collect::<Result<Vec<_>>>(),
        )?;
        append_paper_execution_attempt(
            config,
            &json!({
                "schema_version": PAPER_EXECUTION_ATTEMPT_SCHEMA_VERSION,
                "attempt_id": attempt_id,
                "recorded_at": Utc::now().to_rfc3339(),
                "stage": "started",
                "status": "started",
                "event_id": opp.event_id,
                "arb_type": opp.arb_type.to_string(),
                "account": self.account,
                "data_dir": self.data_dir,
                "account_lock_key": account_lock_key.clone(),
                "baseline_trade_id": baseline_trade_id,
                "execution_route": PAPER_EXECUTION_ROUTE,
                "live_route_compatible": false,
                "order_mode": order_mode,
                "effective_order_type": effective_order_type.clone(),
                "live_order_type": live_order_type.clone(),
                "full_clob_required": config.paper_require_full_clob_quotes,
                "match_live_position_size": config.paper_match_live_position_size,
                "effective_position_size_usd": config.effective_paper_position_size_usd(),
                "config_fingerprint": launch_fingerprint.config_fingerprint.clone(),
                "launch_config_fingerprint": launch_fingerprint.combined_fingerprint.clone(),
                "profit_compatibility_fingerprint": launch_fingerprint.profit_compatibility_fingerprint.clone(),
                "config_field_count": launch_fingerprint.config_field_count,
                "producer_version": env!("CARGO_PKG_VERSION"),
                "producer_executable_sha256": producer_sha256.clone(),
                "external_paper_executable_sha256": adapter_executable_sha256.clone(),
                "execution_profile_sha256": execution_profile_sha256.clone(),
                "payoff_certificate_sha256": payoff_certificate_sha256.clone(),
                "execution_profile": execution_profile.clone(),
                "payoff_certificate": payoff_certificate.value.clone(),
                "planned_basket_units": planned_basket_units,
                "guaranteed_revenue_per_basket_unit": guaranteed_revenue_per_basket_unit,
                "gas_policy_floor_usd": gas_policy_floor_usd,
                "gas_cost_usd": gas_cost_usd,
                "projected_cost_usd": projected_cost_usd,
                "projected_pnl_usd": projected_pnl_usd,
                "projected_roi_pct": projected_roi_pct,
                "leg_count": legs.len(),
                "planned_legs": planned_legs,
            }),
        )
        .with_context(|| {
            format!(
                "paper_attempt_id={attempt_id}: durable attempt-start write failed before submit"
            )
        })?;

        let execution_result: Result<PaperExecutionOutcome> = async {

        self.attempted_legs += legs.len();

        let submissions = if self.use_limit_orders {
            let mut placed_orders: Vec<PlacedPaperOrder> = Vec::with_capacity(legs.len());
            for leg in &legs {
                let placed = match self
                    .place_limit_order_with_retries(leg, final_refresh_started_at, config)
                    .await
                {
                    Ok(placed) => placed,
                    Err(err) => {
                        let order_ids: Vec<i64> =
                            placed_orders.iter().map(|order| order.order_id).collect();
                        self.cancel_limit_orders(&order_ids).await;
                        return Err(err).with_context(|| {
                            format!(
                                "failed to place paper limit order for leg '{} ({})'; cancelled {} already-placed paper limit orders",
                                leg.label,
                                leg.outcome,
                                order_ids.len(),
                            )
                        });
                    }
                };
                info!(
                    "PAPER limit placed: event={} leg='{}' shares={:.4} notional=${:.2} limit={:.4} order_id={}",
                    opp.event_id,
                    leg.label,
                    leg.shares,
                    leg.amount_usd,
                    leg.limit_price,
                    placed.order_id,
                );
                placed_orders.push(placed);
            }

            if let Err(err) = self.await_limit_basket(&placed_orders, config).await {
                let order_ids: Vec<i64> =
                    placed_orders.iter().map(|order| order.order_id).collect();
                self.cancel_limit_orders(&order_ids).await;
                return Err(err).with_context(|| {
                    format!(
                        "paper limit basket incomplete for event {} ({})",
                        opp.event_id, opp.arb_type
                    )
                });
            }
            legs.iter()
                .zip(placed_orders.iter())
                .map(|(leg, placed)| PaperSubmission {
                    kind: PaperSubmissionKind::LimitOrder,
                    id: placed.order_id,
                    market_slug: leg.market_slug.clone(),
                    outcome: leg.outcome.clone(),
                    response_amount_usd: None,
                    response_shares: None,
                })
                .collect::<Vec<_>>()
        } else {
            let mut submissions = Vec::with_capacity(legs.len());
            for leg in &legs {
                submissions.push(
                    self.execute_leg_with_retries(
                        leg,
                        baseline_trade_id,
                        final_refresh_started_at,
                        config,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "external paper basket failed on leg '{} ({})'; basket may now be partially simulated and must be reviewed manually",
                            leg.label, leg.outcome
                        )
                    })?,
                );
            }
            submissions
        };

        let fills = self
            .collect_new_fills(
                baseline_trade_id,
                &legs,
                &final_snapshots,
                &submissions,
                config.paper_trade_history_limit,
            )
            .await?;
        let report = self.analyze_basket_fills(
            fills,
            planned_basket_units,
            guaranteed_revenue_per_basket_unit,
            gas_cost_usd,
        )?;

        self.filled_baskets += 1;
        self.executed_legs += report.fills.len();
        self.conservative_campaign_pnl_usd += report.conservative_campaign_pnl_usd;
        self.unhedged_notional_usd += report.excess_notional_usd;

        for f in &report.fills {
            info!(
                "PAPER fill: {} [{}] shares={:.4} cost=${:.2} fee=${:.4} fee_source=clob_fd avg={:.4} partial={}",
                f.label, f.outcome, f.shares, f.amount_usd, f.fee_usd, f.avg_price, f.is_partial,
            );
        }

        let parity_ok = self.basket_matches_live_parity(&report, config);
        if parity_ok {
            self.parity_accepted_baskets += 1;
            info!(
                "PAPER basket accepted: event={} arb={} legs={} planned_units={:.6} hedged_units={:.6} drift={:.3}% shortfall={:.3}% planned_cost=${:.4} planned_pnl=${:.4} planned_roi={:.2}% hedged_cost=${:.4} hedged_projection=${:.4} hedged_roi={:.2}% conservative_campaign_pnl=${:.4} conservative_campaign_roi={:.2}% unhedged_mark_to_zero=${:.4} fills={}",
                opp.event_id,
                opp.arb_type,
                report.fills.len(),
                report.planned_basket_units,
                report.hedged_basket_units,
                report.unit_drift_pct,
                report.unit_shortfall_pct,
                projected_cost_usd,
                projected_pnl_usd,
                projected_roi_pct,
                report.hedged_cost_usd,
                report.hedged_projection_usd,
                report.hedged_roi_pct,
                report.conservative_campaign_pnl_usd,
                report.conservative_campaign_roi_pct,
                report.excess_notional_usd,
                report.fills.len(),
            );
        } else {
            self.parity_rejected_baskets += 1;
            warn!(
                "PAPER basket rejected for live parity: event={} arb={} legs={} planned_units={:.6} hedged_units={:.6} min_units={:.6} max_units={:.6} drift={:.3}% shortfall={:.3}% tolerance={:.3}% partial={} planned_cost=${:.4} planned_pnl=${:.4} planned_roi={:.2}% hedged_cost=${:.4} hedged_projection=${:.4} hedged_roi={:.2}% conservative_campaign_pnl=${:.4} conservative_campaign_roi={:.2}% unhedged_mark_to_zero=${:.4}",
                opp.event_id,
                opp.arb_type,
                report.fills.len(),
                report.planned_basket_units,
                report.hedged_basket_units,
                report.min_basket_units,
                report.max_basket_units,
                report.unit_drift_pct,
                report.unit_shortfall_pct,
                config.paper_max_share_mismatch_pct,
                report.any_partial,
                projected_cost_usd,
                projected_pnl_usd,
                projected_roi_pct,
                report.hedged_cost_usd,
                report.hedged_projection_usd,
                report.hedged_roi_pct,
                report.conservative_campaign_pnl_usd,
                report.conservative_campaign_roi_pct,
                report.excess_notional_usd,
            );
        }

        Ok(PaperExecutionOutcome {
            fills: report.fills.clone(),
            report: PaperExecutionReport {
                attempt_id: attempt_id.clone(),
                planned_basket_units: report.planned_basket_units,
                hedged_basket_units: report.hedged_basket_units,
                hedged_cost_usd: report.hedged_cost_usd,
                conservative_pnl_usd: report.conservative_campaign_pnl_usd,
                conservative_roi_pct: report.conservative_campaign_roi_pct,
                unhedged_notional_usd: report.excess_notional_usd,
                any_partial: report.any_partial,
                parity_ok,
                fill_count: report.fills.len(),
            },
        })
        }
        .await;

        let execution_result = match sha256_file(&adapter_executable_path) {
            Ok(terminal_sha256) if terminal_sha256 == adapter_executable_sha256 => {
                execution_result
            }
            Ok(terminal_sha256) => Err(anyhow!(
                "external paper adapter executable changed during attempt: before={} after={} path={}",
                adapter_executable_sha256,
                terminal_sha256,
                adapter_executable_path.display(),
            )),
            Err(err) => Err(err.context(
                "re-hashing external paper adapter executable after attempt",
            )),
        };

        match execution_result {
            Ok(outcome) => {
                let report = &outcome.report;
                let (filled_legs, raw_trade_ids) = paper_fill_evidence(&outcome.fills);
                let total_fill_notional_usd = outcome
                    .fills
                    .iter()
                    .map(|fill| fill.amount_usd)
                    .sum::<f64>();
                let total_recomputed_fees_usd =
                    outcome.fills.iter().map(|fill| fill.fee_usd).sum::<f64>();
                let status = if report.parity_ok {
                    "accepted"
                } else {
                    "rejected"
                };
                let mut terminal_record = json!({
                    "schema_version": PAPER_EXECUTION_ATTEMPT_SCHEMA_VERSION,
                    "attempt_id": attempt_id,
                    "recorded_at": Utc::now().to_rfc3339(),
                    "stage": "terminal",
                    "status": status,
                    "event_id": opp.event_id,
                    "arb_type": opp.arb_type.to_string(),
                    "account": self.account,
                    "data_dir": self.data_dir,
                    "account_lock_key": account_lock_key.clone(),
                    "baseline_trade_id": baseline_trade_id,
                    "execution_route": PAPER_EXECUTION_ROUTE,
                    "live_route_compatible": false,
                    "order_mode": order_mode,
                    "effective_order_type": effective_order_type.clone(),
                    "live_order_type": live_order_type.clone(),
                    "full_clob_required": config.paper_require_full_clob_quotes,
                    "match_live_position_size": config.paper_match_live_position_size,
                    "effective_position_size_usd": config.effective_paper_position_size_usd(),
                    "config_fingerprint": launch_fingerprint.config_fingerprint.clone(),
                    "launch_config_fingerprint": launch_fingerprint.combined_fingerprint.clone(),
                    "profit_compatibility_fingerprint": launch_fingerprint.profit_compatibility_fingerprint.clone(),
                    "config_field_count": launch_fingerprint.config_field_count,
                    "producer_version": env!("CARGO_PKG_VERSION"),
                    "producer_executable_sha256": producer_sha256.clone(),
                    "external_paper_executable_sha256": adapter_executable_sha256.clone(),
                    "execution_profile_sha256": execution_profile_sha256.clone(),
                    "payoff_certificate_sha256": payoff_certificate_sha256.clone(),
                    "parity_ok": report.parity_ok,
                    "fill_count": report.fill_count,
                    "planned_basket_units": report.planned_basket_units,
                    "hedged_basket_units": report.hedged_basket_units,
                    "hedged_cost_usd": report.hedged_cost_usd,
                    "conservative_pnl_usd": report.conservative_pnl_usd,
                    "conservative_roi_pct": report.conservative_roi_pct,
                    "unhedged_notional_usd": report.unhedged_notional_usd,
                    "any_partial": report.any_partial,
                });
                let terminal_object = terminal_record
                    .as_object_mut()
                    .expect("paper terminal record is an object");
                terminal_object.insert("raw_trade_count".into(), json!(raw_trade_ids.len()));
                terminal_object.insert("raw_trade_ids".into(), json!(raw_trade_ids));
                terminal_object.insert("filled_legs".into(), json!(filled_legs));
                terminal_object.insert(
                    "total_fill_notional_usd".into(),
                    json!(total_fill_notional_usd),
                );
                terminal_object.insert(
                    "total_recomputed_fees_usd".into(),
                    json!(total_recomputed_fees_usd),
                );
                terminal_object.insert(
                    "guaranteed_revenue_per_basket_unit".into(),
                    json!(guaranteed_revenue_per_basket_unit),
                );
                terminal_object.insert("gas_policy_floor_usd".into(), json!(gas_policy_floor_usd));
                terminal_object.insert("gas_cost_usd".into(), json!(gas_cost_usd));
                append_paper_execution_attempt(
                    config,
                    &terminal_record,
                )
                .with_context(|| {
                    format!(
                        "paper_attempt_id={attempt_id}: durable terminal-{status} write failed after paper execution"
                    )
                })?;
                Ok(outcome.report)
            }
            Err(err) => {
                let error = format!("{err:#}");
                if let Err(journal_err) = append_paper_execution_attempt(
                    config,
                    &json!({
                        "schema_version": PAPER_EXECUTION_ATTEMPT_SCHEMA_VERSION,
                        "attempt_id": attempt_id,
                        "recorded_at": Utc::now().to_rfc3339(),
                        "stage": "terminal",
                        "status": "error",
                        "event_id": opp.event_id,
                        "arb_type": opp.arb_type.to_string(),
                        "account": self.account,
                        "data_dir": self.data_dir,
                        "account_lock_key": account_lock_key.clone(),
                        "baseline_trade_id": baseline_trade_id,
                        "execution_route": PAPER_EXECUTION_ROUTE,
                        "live_route_compatible": false,
                        "order_mode": order_mode,
                        "effective_order_type": effective_order_type.clone(),
                        "live_order_type": live_order_type.clone(),
                        "full_clob_required": config.paper_require_full_clob_quotes,
                        "match_live_position_size": config.paper_match_live_position_size,
                        "effective_position_size_usd": config.effective_paper_position_size_usd(),
                        "config_fingerprint": launch_fingerprint.config_fingerprint.clone(),
                        "launch_config_fingerprint": launch_fingerprint.combined_fingerprint.clone(),
                        "profit_compatibility_fingerprint": launch_fingerprint.profit_compatibility_fingerprint.clone(),
                        "config_field_count": launch_fingerprint.config_field_count,
                        "producer_version": env!("CARGO_PKG_VERSION"),
                        "producer_executable_sha256": producer_sha256.clone(),
                        "external_paper_executable_sha256": adapter_executable_sha256.clone(),
                        "execution_profile_sha256": execution_profile_sha256.clone(),
                        "payoff_certificate_sha256": payoff_certificate_sha256.clone(),
                        "error": error,
                    }),
                ) {
                    return Err(journal_err).with_context(|| {
                        format!(
                            "paper_attempt_id={attempt_id}: durable terminal-error write failed after execution error: {error}"
                        )
                    });
                }
                Err(err).with_context(|| format!("paper_attempt_id={attempt_id}"))
            }
        }
    }

    pub async fn print_summary(&self) {
        info!("{}", "=".repeat(60));
        info!("External paper engine summary");
        info!(
            "Mode:                   {}",
            if self.use_limit_orders {
                "limit-parity"
            } else {
                "market-style"
            }
        );
        info!("Filled baskets:         {}", self.filled_baskets);
        info!("Parity accepted:        {}", self.parity_accepted_baskets);
        info!("Parity rejected:        {}", self.parity_rejected_baskets);
        info!("Attempted legs:         {}", self.attempted_legs);
        info!("Executed legs:          {}", self.executed_legs);
        info!(
            "Conservative campaign PnL:${:.4}",
            self.conservative_campaign_pnl_usd
        );
        info!("Unhedged notional:      ${:.4}", self.unhedged_notional_usd);

        let balance_value = self.run_json(&["balance".to_string()]).await.ok();
        let stats_value = self.run_json(&["stats".to_string()]).await.ok();

        let balance_cash = balance_value.as_ref().and_then(|value| {
            json_find_f64_recursive(
                value,
                &[
                    "cash",
                    "balance",
                    "cash_balance",
                    "available_cash",
                    "usd_balance",
                ],
            )
        });
        let equity = stats_value.as_ref().and_then(|value| {
            json_find_f64_recursive(
                value,
                &[
                    "equity",
                    "portfolio_value",
                    "net_liq",
                    "nav",
                    "account_value",
                ],
            )
        });
        let realized_pnl = stats_value.as_ref().and_then(|value| {
            json_find_f64_recursive(value, &["realized_pnl", "realized", "pnl_realized"])
        });
        let unrealized_pnl = stats_value.as_ref().and_then(|value| {
            json_find_f64_recursive(value, &["unrealized_pnl", "unrealized", "pnl_unrealized"])
        });
        let total_pnl = stats_value
            .as_ref()
            .and_then(|value| json_find_f64_recursive(value, &["total_pnl", "pnl", "net_pnl"]));
        let open_positions = stats_value.as_ref().and_then(|value| {
            json_find_f64_recursive(value, &["open_positions", "positions", "position_count"])
        });
        let total_trades = stats_value.as_ref().and_then(|value| {
            json_find_f64_recursive(value, &["total_trades", "trades", "fill_count", "fills"])
        });
        let win_rate = stats_value.as_ref().and_then(|value| {
            json_find_f64_recursive(value, &["win_rate", "win_pct", "win_percent"])
        });

        if balance_cash.is_some()
            || equity.is_some()
            || realized_pnl.is_some()
            || unrealized_pnl.is_some()
            || total_pnl.is_some()
        {
            info!("Paper account snapshot:");
            if let Some(cash) = balance_cash {
                info!("  Cash / balance:       ${:.2}", cash);
            }
            if let Some(equity) = equity {
                info!("  Equity / NAV:         ${:.2}", equity);
            }
            if let Some(realized) = realized_pnl {
                info!("  Realized PnL:         ${:.2}", realized);
            }
            if let Some(unrealized) = unrealized_pnl {
                info!("  Unrealized PnL:       ${:.2}", unrealized);
            }
            if let Some(total) = total_pnl {
                info!("  Total PnL:            ${:.2}", total);
            }
            if let Some(open_positions) = open_positions {
                info!("  Open positions:       {:.0}", open_positions);
            }
            if let Some(total_trades) = total_trades {
                info!("  Recorded trades:      {:.0}", total_trades);
            }
            if let Some(win_rate) = win_rate {
                info!("  Win rate:             {:.2}%", win_rate);
            }
        } else {
            info!("Paper account snapshot: unavailable (pm-trader balance/stats did not expose recognized fields).");
        }

        info!("{}", "=".repeat(60));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ArbType, Market};
    use chrono::Utc;
    use httpmock::prelude::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn market() -> Market {
        Market {
            question: "Q".into(),
            condition_id: "cond".into(),
            market_slug: "slug".into(),
            clob_token_id_yes: "yes".into(),
            clob_token_id_no: "no".into(),
            gamma_yes_price: 0.4,
            gamma_no_price: 0.6,
            clob_yes_ask: Some(0.4),
            clob_yes_bid: Some(0.39),
            clob_no_ask: Some(0.6),
            clob_no_bid: Some(0.59),
            clob_yes_ask_size: Some(100.0),
            clob_yes_bid_size: None,
            clob_no_ask_size: Some(100.0),
            clob_no_bid_size: None,
            fees_enabled: Some(true),
            taker_fee_rate: Some(0.0),
            maker_fee_rate: Some(0.0),
            clob_taker_fee_bps: Some(0),
            clob_fee_rate: Some(0.0),
            clob_fee_exponent: Some(1),
            order_price_min_tick_size: Some(0.01),
            order_min_size: Some(1.0),
            clob_tick_size: Some(0.01),
            clob_min_order_size: Some(1.0),
            clob_neg_risk: Some(true),
            clob_rfq_enabled: None,
            liquidity: 1000.0,
            closed: false,
        }
    }

    fn opp_ranked() -> ArbitrageOpportunity {
        ArbitrageOpportunity {
            event_title: "Ranked".into(),
            event_id: "r1".into(),
            category: "sports".into(),
            arb_type: ArbType::Ranked,
            markets: vec![market(), market()],
            execution_plan: vec![
                OpportunityLeg {
                    market_index: 0,
                    question: "A".into(),
                    market_slug: "slug-a".into(),
                    condition_id: "c1".into(),
                    token_id: "t1".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 2.0,
                    reference_price: 0.4,
                },
                OpportunityLeg {
                    market_index: 1,
                    question: "B".into(),
                    market_slug: "slug-b".into(),
                    condition_id: "c2".into(),
                    token_id: "t2".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 1.0,
                    reference_price: 0.2,
                },
            ],
            total_cost: 1.0,
            guaranteed_revenue: 1.5,
            gross_profit: 0.5,
            total_fees: 0.0,
            net_profit: 0.5,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 50.0,
            prices_from_clob: true,
            max_executable_size_usd: 25.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        }
    }

    fn opp_yes_family() -> ArbitrageOpportunity {
        let mut first = market();
        first.question = "A".into();
        first.condition_id = "c1".into();
        first.market_slug = "slug-a".into();
        first.clob_token_id_yes = "t1".into();
        first.clob_token_id_no = "n1".into();

        let mut second = market();
        second.question = "B".into();
        second.condition_id = "c2".into();
        second.market_slug = "slug-b".into();
        second.clob_token_id_yes = "t2".into();
        second.clob_token_id_no = "n2".into();

        ArbitrageOpportunity {
            event_title: "Complete YES family".into(),
            event_id: "yes-family-1".into(),
            category: "sports".into(),
            arb_type: ArbType::Yes,
            markets: vec![first, second],
            execution_plan: vec![
                OpportunityLeg {
                    market_index: 0,
                    question: "A".into(),
                    market_slug: "slug-a".into(),
                    condition_id: "c1".into(),
                    token_id: "t1".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 1.0,
                    reference_price: 0.4,
                },
                OpportunityLeg {
                    market_index: 1,
                    question: "B".into(),
                    market_slug: "slug-b".into(),
                    condition_id: "c2".into(),
                    token_id: "t2".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 1.0,
                    reference_price: 0.1,
                },
            ],
            total_cost: 0.5,
            guaranteed_revenue: 1.0,
            gross_profit: 0.5,
            total_fees: 0.0,
            net_profit: 0.5,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 100.0,
            prices_from_clob: true,
            max_executable_size_usd: 25.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        }
    }

    fn opp_binary_bundle() -> ArbitrageOpportunity {
        let mut only_market = market();
        only_market.question = "Binary".into();
        only_market.condition_id = "c1".into();
        only_market.market_slug = "slug-a".into();
        only_market.clob_token_id_yes = "t1".into();
        only_market.clob_token_id_no = "n1".into();

        ArbitrageOpportunity {
            event_title: "Binary bundle".into(),
            event_id: "bundle-1".into(),
            category: "sports".into(),
            arb_type: ArbType::Bundle,
            markets: vec![only_market],
            execution_plan: vec![
                OpportunityLeg {
                    market_index: 0,
                    question: "Binary YES".into(),
                    market_slug: "slug-a".into(),
                    condition_id: "c1".into(),
                    token_id: "t1".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 1.0,
                    reference_price: 0.4,
                },
                OpportunityLeg {
                    market_index: 0,
                    question: "Binary NO".into(),
                    market_slug: "slug-a".into(),
                    condition_id: "c1".into(),
                    token_id: "n1".into(),
                    outcome: OutcomeSide::No,
                    unit_shares: 1.0,
                    reference_price: 0.5,
                },
            ],
            total_cost: 0.9,
            guaranteed_revenue: 1.0,
            gross_profit: 0.1,
            total_fees: 0.0,
            net_profit: 0.1,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 11.11,
            prices_from_clob: true,
            max_executable_size_usd: 25.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn execute_opportunity_rejects_external_token_before_paper_preflight() {
        let mut engine = ExternalPaperEngine {
            command: "pm-trader-should-not-run".into(),
            data_dir: ".pm".into(),
            account: "acct".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: true,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };
        let mut opp = opp_ranked();
        opp.execution_plan[0].token_id = "external:kalshi:abc".into();
        let cfg = Config::from_env();
        let client = HttpClient::new();

        let err = engine
            .execute_opportunity(&opp, &cfg, &client)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("external token id"));
        assert_eq!(engine.attempted_legs, 0);
    }

    #[tokio::test]
    async fn execute_opportunity_rejects_unsupported_payoff_before_adapter_call() {
        let mut engine = ExternalPaperEngine {
            command: "pm-trader-should-not-run".into(),
            data_dir: ".pm".into(),
            account: "acct".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: false,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };
        let error = engine
            .execute_opportunity(&opp_ranked(), &Config::from_env(), &HttpClient::new())
            .await
            .expect_err("ranked payoff must fail before adapter execution");
        let failure = paper_failure_trade_log(&error);
        assert_eq!(failure.status, "pre_submit_rejected");
        assert!(failure.note.contains("payoff_certificate"));
        assert_eq!(engine.attempted_legs, 0);
    }

    #[test]
    fn basket_unit_step_respects_largest_share_requirement() {
        let cfg = Config::from_env();
        let opp = opp_ranked();
        let step = basket_unit_step(&opp.execution_plan, &cfg);
        assert!(step >= cfg.order_size_step_shares);
    }

    #[test]
    fn required_quotes_present_rejects_closed_plan_market() {
        let mut opp = opp_ranked();
        opp.markets[0].closed = true;

        assert!(!required_quotes_present(&opp.markets, &opp.execution_plan));
    }

    #[test]
    fn market_style_order_amount_targets_expected_price_not_slippage_limit() {
        let shares = 10.0;
        let expected_price = 0.10;
        let limit_price = 0.12;

        assert_eq!(
            paper_order_amount_usd(shares, expected_price, limit_price, false),
            1.0
        );
        assert_eq!(
            paper_order_amount_usd(shares, expected_price, limit_price, true),
            1.2
        );
    }

    #[test]
    fn projected_trade_metrics_supports_unequal_ranked_legs() {
        let cfg = Config::from_env();
        let opp = opp_ranked();
        let plan = vec![
            PlanLegSnapshot {
                market: market(),
                raw_ask: 0.4,
                limit_price: 0.4,
            },
            PlanLegSnapshot {
                market: market(),
                raw_ask: 0.2,
                limit_price: 0.2,
            },
        ];
        let (cost, fees, pnl, roi) =
            projected_trade_metrics(&opp, &plan, 2.0, &cfg, 0.0, 1.5).unwrap();
        assert!((cost - 2.0).abs() < 1e-9);
        assert!((fees - 0.0).abs() < 1e-9);
        assert!((pnl - 1.0).abs() < 1e-9);
        assert!((roi - 50.0).abs() < 1e-9);
    }

    #[test]
    fn final_plan_snapshots_follow_execution_leg_order_not_market_index() {
        let legs = vec![
            PaperOrderLeg {
                market_index: 3,
                market_slug: "slug-a".into(),
                token_id: "t1".into(),
                outcome: "yes".into(),
                unit_shares: 2.0,
                shares: 10.0,
                amount_usd: 4.0,
                limit_price: 0.41,
                tick_size: 0.01,
                label: "A".into(),
                min_order_shares: 1.0,
            },
            PaperOrderLeg {
                market_index: 7,
                market_slug: "slug-b".into(),
                token_id: "t2".into(),
                outcome: "yes".into(),
                unit_shares: 1.0,
                shares: 5.0,
                amount_usd: 1.0,
                limit_price: 0.21,
                tick_size: 0.01,
                label: "B".into(),
                min_order_shares: 1.0,
            },
        ];
        let plan_snapshots = vec![
            PlanLegSnapshot {
                market: market(),
                raw_ask: 0.4,
                limit_price: 0.4,
            },
            PlanLegSnapshot {
                market: market(),
                raw_ask: 0.2,
                limit_price: 0.2,
            },
        ];

        let final_snapshots =
            final_plan_snapshots_from_legs(&legs, &plan_snapshots).expect("aligned snapshots");

        assert_eq!(final_snapshots.len(), 2);
        assert_eq!(final_snapshots[0].raw_ask, 0.4);
        assert_eq!(final_snapshots[0].limit_price, 0.41);
        assert_eq!(final_snapshots[1].raw_ask, 0.2);
        assert_eq!(final_snapshots[1].limit_price, 0.21);
    }

    #[test]
    fn json_find_f64_recursive_handles_nested_aliases() {
        let value: Value = serde_json::json!({
            "stats": {
                "portfolio": {
                    "net_liq": "1234.56"
                },
                "pnl": {
                    "realized": 12.5,
                    "unrealized": "-3.25"
                }
            }
        });
        assert_eq!(json_find_f64_recursive(&value, &["net_liq"]), Some(1234.56));
        assert_eq!(
            json_find_f64_recursive(&value, &["realized_pnl", "realized"]),
            Some(12.5)
        );
        assert_eq!(
            json_find_f64_recursive(&value, &["unrealized_pnl", "unrealized"]),
            Some(-3.25)
        );
    }

    #[tokio::test]
    async fn build_limit_legs_uses_batched_depth_books() {
        let server = MockServer::start_async().await;
        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200).json_body(serde_json::json!([
                    {
                        "asset_id": "t1",
                        "asks": [
                            {"price":"0.40","size":"100"},
                            {"price":"0.42","size":"100"}
                        ],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-t1"
                    },
                    {
                        "asset_id": "t2",
                        "asks": [
                            {"price":"0.20","size":"100"},
                            {"price":"0.22","size":"100"}
                        ],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-t2"
                    }
                ]));
            })
            .await;
        let single_book = server
            .mock_async(|when, then| {
                when.method(GET).path("/book");
                then.status(500).body("single book path should be unused");
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.api_timeout_secs = 2;
        cfg.live_slippage_bps = 2500;
        cfg.paper_match_live_position_size = false;
        cfg.paper_trade_position_size_usd = 10.0;
        cfg.external_paper_min_order_usd = 1.0;
        let opp = opp_ranked();
        let plan = vec![
            PlanLegSnapshot {
                market: market(),
                raw_ask: 0.4,
                limit_price: 0.4,
            },
            PlanLegSnapshot {
                market: market(),
                raw_ask: 0.2,
                limit_price: 0.2,
            },
        ];

        let (legs, planned_units) = build_limit_legs(&HttpClient::new(), &cfg, &opp, &plan)
            .await
            .expect("batched paper legs");

        assert_eq!(legs.len(), 2);
        assert!(planned_units > 0.0);
        books.assert_calls_async(1).await;
        single_book.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn build_limit_legs_uses_cutoff_depth_for_limit_price() {
        let server = MockServer::start_async().await;
        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200).json_body(serde_json::json!([
                    {
                        "asset_id": "t1",
                        "asks": [
                            {"price":"0.40","size":"10"},
                            {"price":"0.50","size":"100"}
                        ],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-t1"
                    },
                    {
                        "asset_id": "t2",
                        "asks": [{"price":"0.20","size":"100"}],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-t2"
                    }
                ]));
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.api_timeout_secs = 2;
        cfg.live_slippage_bps = 2500;
        cfg.paper_match_live_position_size = false;
        cfg.paper_trade_position_size_usd = 10.0;
        cfg.external_paper_min_order_usd = 1.0;
        let opp = opp_ranked();
        let plan = vec![
            PlanLegSnapshot {
                market: market(),
                raw_ask: 0.4,
                limit_price: live_style_limit_price(0.4, &market(), &cfg),
            },
            PlanLegSnapshot {
                market: market(),
                raw_ask: 0.2,
                limit_price: live_style_limit_price(0.2, &market(), &cfg),
            },
        ];

        let (legs, _planned_units) = build_limit_legs(&HttpClient::new(), &cfg, &opp, &plan)
            .await
            .expect("paper legs");

        let leg = legs
            .iter()
            .find(|leg| leg.token_id == "t1")
            .expect("t1 leg");
        assert!(leg.shares > 10.0);
        assert_eq!(
            leg.limit_price,
            live_style_limit_price(0.50, &market(), &cfg)
        );
        books.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn refresh_and_validate_rejects_incomplete_fresh_enrichment() {
        let server = MockServer::start_async().await;
        let book_t1 = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "t1");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.40","size":"100"}],"bids":[{"price":"0.39","size":"100"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let book_n1 = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "n1");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[],"bids":[],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let book_t2 = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "t2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.20","size":"100"}],"bids":[{"price":"0.19","size":"100"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let book_n2 = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "n2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[],"bids":[],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let info_1 = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c1");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"mos":1,"mts":0.01,"fd":{"r":0.0,"e":1},"negRisk":true,"accepting_orders":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"seconds_delay":0}"#);
            })
            .await;
        let info_2 = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"mos":1,"mts":0.01,"fd":{"r":0.0,"e":1},"negRisk":true,"accepting_orders":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"seconds_delay":0}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.api_timeout_secs = 2;
        cfg.paper_require_full_clob_quotes = true;
        cfg.min_roi_pct = 0.0;

        let mut opp = opp_ranked();
        opp.prices_from_clob = false;
        opp.markets[0].condition_id = "c1".into();
        opp.markets[0].clob_token_id_yes = "t1".into();
        opp.markets[0].clob_token_id_no = "n1".into();
        opp.markets[1].condition_id = "c2".into();
        opp.markets[1].clob_token_id_yes = "t2".into();
        opp.markets[1].clob_token_id_no = "n2".into();

        let err = refresh_and_validate(&HttpClient::new(), &cfg, &opp)
            .await
            .expect_err("partial enrichment must not become paper evidence");

        assert!(err
            .to_string()
            .contains("fresh CLOB enrichment was incomplete"));
        book_t1.assert_calls_async(1).await;
        book_n1.assert_calls_async(1).await;
        book_t2.assert_calls_async(1).await;
        book_n2.assert_calls_async(1).await;
        info_1.assert_calls_async(1).await;
        info_2.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn paper_pre_submit_fee_refresh_overwrites_stale_scan_metadata() {
        let server = MockServer::start_async().await;
        let fee_1 = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c1");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"c1","fd":{"r":0.02,"e":2}}"#);
            })
            .await;
        let fee_2 = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"c2","fd":{"r":0.03,"e":3}}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.api_timeout_secs = 2;
        let mut opp = opp_ranked();
        opp.markets[0].condition_id = "c1".into();
        opp.markets[1].condition_id = "c2".into();
        let mut snapshots = opp
            .markets
            .iter()
            .cloned()
            .map(|mut market| {
                market.clob_fee_rate = Some(0.99);
                market.clob_fee_exponent = Some(1);
                PlanLegSnapshot {
                    market,
                    raw_ask: 0.4,
                    limit_price: 0.4,
                }
            })
            .collect::<Vec<_>>();

        refresh_plan_fee_schedules(&HttpClient::new(), &cfg, &opp, &mut snapshots)
            .await
            .expect("fresh fee schedules");

        assert_eq!(snapshots[0].market.clob_fee_rate, Some(0.02));
        assert_eq!(snapshots[0].market.clob_fee_exponent, Some(2));
        assert_eq!(snapshots[1].market.clob_fee_rate, Some(0.03));
        assert_eq!(snapshots[1].market.clob_fee_exponent, Some(3));
        fee_1.assert_calls_async(1).await;
        fee_2.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn paper_pre_submit_fee_refresh_fails_closed_instead_of_using_stale_metadata() {
        let server = MockServer::start_async().await;
        let unavailable = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c1");
                then.status(503).body("fee metadata unavailable");
            })
            .await;
        let _fee_2 = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"c2","fd":{"r":0.03,"e":3}}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.api_timeout_secs = 2;
        let mut opp = opp_ranked();
        opp.markets[0].condition_id = "c1".into();
        opp.markets[1].condition_id = "c2".into();
        let mut snapshots = opp
            .markets
            .iter()
            .cloned()
            .map(|mut market| {
                market.clob_fee_rate = Some(0.99);
                market.clob_fee_exponent = Some(1);
                PlanLegSnapshot {
                    market,
                    raw_ask: 0.4,
                    limit_price: 0.4,
                }
            })
            .collect::<Vec<_>>();

        let err = refresh_plan_fee_schedules(&HttpClient::new(), &cfg, &opp, &mut snapshots)
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("paper pre-submit CLOB V2 fee refresh failed"));
        assert_eq!(snapshots[0].market.clob_fee_rate, Some(0.99));
        unavailable.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn execute_opportunity_recomputes_zero_adapter_fees_from_clob_metadata() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("polymarket-paper-adapter-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("fake-pm-trader.log");
        let script_path = dir.join("fake-pm-trader.sh");
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
log="__LOG__"
printf '%s\n' "$*" >> "$log"
cmd=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir|--account)
      shift 2
      ;;
    *)
      cmd="$1"
      shift
      break
      ;;
  esac
done

case "$cmd" in
  history)
    if [[ "${1:-}" == "--limit" && "${2:-}" == "1" ]]; then
      printf '{"ok":true,"data":[]}\n'
    else
      printf '{"ok":true,"data":[{"id":1,"market_slug":"slug-a","outcome":"yes","side":"buy","amount_usd":8.0,"shares":20.0,"fee":0.0,"is_partial":false},{"id":2,"market_slug":"slug-b","outcome":"yes","side":"buy","amount_usd":2.0,"shares":20.0,"fee":0.0,"is_partial":false}]}\n'
    fi
    ;;
  buy)
    if [[ "${1:-}" == "slug-a" ]]; then
      printf '{"ok":true,"data":{"trade":{"id":1,"market_slug":"slug-a","outcome":"yes","side":"buy","amount_usd":8.0,"shares":20.0}}}\n'
    else
      printf '{"ok":true,"data":{"trade":{"id":2,"market_slug":"slug-b","outcome":"yes","side":"buy","amount_usd":2.0,"shares":20.0}}}\n'
    fi
    ;;
  *)
    printf '{"ok":false,"code":"UNEXPECTED","error":"unexpected command"}\n'
    exit 1
    ;;
esac
"#
        .replace("__LOG__", &log_path.to_string_lossy());
        fs::write(&script_path, script).unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();

        let server = MockServer::start_async().await;
        let book_t1 = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "t1");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.40","size":"100"}],"bids":[{"price":"0.39","size":"100"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let book_n1 = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "n1");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.60","size":"100"}],"bids":[{"price":"0.59","size":"100"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let book_t2 = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "t2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.10","size":"100"}],"bids":[{"price":"0.09","size":"100"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let book_n2 = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "n2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.90","size":"100"}],"bids":[{"price":"0.89","size":"100"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let info_1 = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c1");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"c1","t":[{"t":"t1","o":"Yes"},{"t":"n1","o":"No"}],"mos":1,"mts":0.01,"fd":{"r":0.02,"e":2,"to":true},"nr":true,"ao":true,"sd":0,"oas":0}"#);
            })
            .await;
        let info_2 = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"c2","t":[{"t":"t2","o":"Yes"},{"t":"n2","o":"No"}],"mos":1,"mts":0.01,"fd":{"r":0.02,"e":2,"to":true},"nr":true,"ao":true,"sd":0,"oas":0}"#);
            })
            .await;
        let depth_books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200).json_body(serde_json::json!([
                    {
                        "asset_id": "t1",
                        "asks": [{"price":"0.40","size":"100"}],
                        "bids": [{"price":"0.39","size":"100"}],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-t1"
                    },
                    {
                        "asset_id": "t2",
                        "asks": [{"price":"0.10","size":"100"}],
                        "bids": [{"price":"0.09","size":"100"}],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-t2"
                    }
                ]));
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.clob_book_batch_size = 10;
        cfg.max_retries = 1;
        cfg.api_timeout_secs = 2;
        cfg.live_slippage_bps = 0;
        cfg.live_max_refresh_to_submit_ms = 30_000;
        cfg.paper_match_live_position_size = false;
        cfg.paper_trade_position_size_usd = 10.0;
        cfg.external_paper_min_order_usd = 1.0;
        cfg.min_net_profit_usd = 0.01;
        cfg.min_roi_pct = 0.0;
        cfg.paper_require_full_clob_quotes = true;
        cfg.diagnostics_dir = dir.join("diagnostics");

        let mut opp = opp_yes_family();
        opp.prices_from_clob = false;
        opp.markets[0].condition_id = "c1".into();
        opp.markets[0].clob_token_id_yes = "t1".into();
        opp.markets[0].clob_token_id_no = "n1".into();
        opp.markets[1].condition_id = "c2".into();
        opp.markets[1].clob_token_id_yes = "t2".into();
        opp.markets[1].clob_token_id_no = "n2".into();

        let mut engine = ExternalPaperEngine {
            command: script_path.to_string_lossy().to_string(),
            data_dir: dir.join("paper").to_string_lossy().to_string(),
            account: "adapter-proof".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: false,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };

        let report = engine
            .execute_opportunity(&opp, &cfg, &HttpClient::new())
            .await
            .expect("paper adapter execution should trade");

        assert!(report.parity_ok);
        assert_eq!(report.fill_count, 2);
        assert!(!report.attempt_id.is_empty());
        assert!((report.hedged_cost_usd - 10.02628).abs() < 1e-9);
        assert!((report.conservative_pnl_usd - 9.87372).abs() < 1e-9);
        assert!((engine.conservative_campaign_pnl_usd - 9.87372).abs() < 1e-9);
        assert_eq!(engine.filled_baskets, 1);
        assert_eq!(engine.parity_accepted_baskets, 1);
        assert_eq!(engine.attempted_legs, 2);
        assert_eq!(engine.executed_legs, 2);

        let calls = fs::read_to_string(log_path).unwrap();
        assert!(calls.contains("history --limit 1"));
        assert!(calls.contains("buy slug-a yes 8.00 --type fok"));
        assert!(calls.contains("buy slug-b yes 2.00 --type fok"));
        assert!(calls.contains("history --limit 10000"));
        book_t1.assert_calls_async(1).await;
        book_n1.assert_calls_async(1).await;
        book_t2.assert_calls_async(1).await;
        book_n2.assert_calls_async(1).await;
        info_1.assert_calls_async(3).await;
        info_2.assert_calls_async(3).await;
        depth_books.assert_calls_async(1).await;

        let attempt_journal =
            fs::read_to_string(cfg.diagnostics_dir.join(PAPER_EXECUTION_ATTEMPTS_FILE))
                .expect("paper attempt journal");
        let attempt_records = attempt_journal
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("attempt record JSON"))
            .collect::<Vec<_>>();
        assert_eq!(attempt_records.len(), 2);
        assert_eq!(attempt_records[0]["attempt_id"], report.attempt_id);
        assert_eq!(attempt_records[0]["stage"], "started");
        assert_eq!(attempt_records[0]["full_clob_required"], true);
        assert_eq!(attempt_records[0]["execution_route"], PAPER_EXECUTION_ROUTE);
        assert_eq!(attempt_records[0]["live_route_compatible"], false);
        assert_eq!(attempt_records[0]["order_mode"], "market_style");
        assert_eq!(attempt_records[0]["effective_order_type"], "fok");
        assert_eq!(attempt_records[0]["live_order_type"], "fok");
        assert_eq!(attempt_records[0]["match_live_position_size"], false);
        assert_eq!(attempt_records[0]["planned_legs"][0]["unit_shares"], 1.0);
        assert_eq!(
            attempt_records[0]["guaranteed_revenue_per_basket_unit"],
            1.0
        );
        assert_eq!(attempt_records[0]["gas_policy_floor_usd"], 0.1);
        assert_eq!(attempt_records[0]["gas_cost_usd"], 0.1);
        assert_eq!(
            attempt_records[0]["execution_profile"]["fresh_clob_enrichment_complete"],
            true
        );
        for key in [
            "producer_executable_sha256",
            "external_paper_executable_sha256",
            "execution_profile_sha256",
        ] {
            assert_eq!(attempt_records[0][key].as_str().unwrap().len(), 64);
        }
        assert_eq!(attempt_records[1]["attempt_id"], report.attempt_id);
        assert_eq!(attempt_records[1]["stage"], "terminal");
        assert_eq!(attempt_records[1]["status"], "accepted");
        assert_eq!(attempt_records[1]["raw_trade_count"], 2);
        assert_eq!(attempt_records[1]["raw_trade_ids"], json!([1, 2]));
        assert_eq!(
            attempt_records[1]["filled_legs"].as_array().unwrap().len(),
            2
        );
        assert_eq!(
            attempt_records[1]["filled_legs"][0]["raw_trades"][0]["trade_id"],
            1
        );
        assert_eq!(attempt_records[1]["filled_legs"][0]["fee_rate"], 0.02);
        assert_eq!(attempt_records[1]["filled_legs"][0]["fee_exponent"], 2);
        assert_eq!(
            attempt_records[1]["filled_legs"][0]["submission_kind"],
            "market_trade"
        );
        assert_eq!(attempt_records[1]["filled_legs"][0]["submission_id"], 1);
        assert_eq!(
            attempt_records[1]["filled_legs"][0]["recomputed_fee_usd"],
            0.02304
        );
        assert_eq!(attempt_records[1]["total_recomputed_fees_usd"], 0.02628);
        assert_eq!(
            attempt_records[1]["guaranteed_revenue_per_basket_unit"],
            1.0
        );
        assert_eq!(attempt_records[1]["gas_policy_floor_usd"], 0.1);
        assert_eq!(attempt_records[1]["gas_cost_usd"], 0.1);
        assert_eq!(
            attempt_records[1]["execution_profile_sha256"],
            attempt_records[0]["execution_profile_sha256"]
        );
        assert_eq!(
            attempt_records[1]["producer_executable_sha256"],
            attempt_records[0]["producer_executable_sha256"]
        );
    }

    #[tokio::test]
    async fn paper_attempt_errors_reconcile_and_start_write_failure_blocks_submit() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("polymarket-paper-attempt-error-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("fake-pm-trader.log");
        let script_path = dir.join("fake-pm-trader.sh");
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
log="__LOG__"
printf '%s\n' "$*" >> "$log"
cmd=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir|--account) shift 2 ;;
    *) cmd="$1"; shift; break ;;
  esac
done
case "$cmd" in
  history) printf '{"ok":true,"data":[]}\n' ;;
  buy) printf '{"ok":false,"code":"REJECTED","error":"simulated rejection"}\n' ;;
  *) printf '{"ok":false,"code":"UNEXPECTED","error":"unexpected command"}\n' ;;
esac
"#
        .replace("__LOG__", &log_path.to_string_lossy());
        fs::write(&script_path, script).unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();

        let server = MockServer::start_async().await;
        for (token, body) in [
            (
                "t1",
                r#"{"asks":[{"price":"0.40","size":"100"}],"bids":[{"price":"0.39","size":"100"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#,
            ),
            (
                "n1",
                r#"{"asks":[{"price":"0.50","size":"100"}],"bids":[{"price":"0.49","size":"100"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#,
            ),
        ] {
            server
                .mock_async(move |when, then| {
                    when.method(GET)
                        .path("/book")
                        .query_param("token_id", token);
                    then.status(200)
                        .header("content-type", "application/json")
                        .body(body);
                })
                .await;
        }
        server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/c1");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"c1","t":[{"t":"t1","o":"Yes"},{"t":"n1","o":"No"}],"mos":1,"mts":0.01,"fd":{"r":0.02,"e":2},"nr":true,"ao":true,"sd":0,"oas":0}"#);
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200).json_body(json!([
                    {
                        "asset_id": "t1",
                        "asks": [{"price":"0.40","size":"100"}],
                        "bids": [{"price":"0.39","size":"100"}],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-t1"
                    },
                    {
                        "asset_id": "n1",
                        "asks": [{"price":"0.50","size":"100"}],
                        "bids": [{"price":"0.49","size":"100"}],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-n1"
                    }
                ]));
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.api_timeout_secs = 2;
        cfg.live_slippage_bps = 0;
        cfg.live_max_refresh_to_submit_ms = 30_000;
        cfg.paper_match_live_position_size = false;
        cfg.paper_trade_position_size_usd = 10.0;
        cfg.external_paper_min_order_usd = 1.0;
        cfg.min_net_profit_usd = 0.01;
        cfg.min_roi_pct = 0.0;
        cfg.diagnostics_dir = dir.join("diagnostics");
        let mut opp = opp_binary_bundle();
        opp.prices_from_clob = false;
        let mut engine = ExternalPaperEngine {
            command: script_path.to_string_lossy().to_string(),
            data_dir: dir.join("paper").to_string_lossy().to_string(),
            account: "attempt-error".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: false,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };

        let err = engine
            .execute_opportunity(&opp, &cfg, &HttpClient::new())
            .await
            .unwrap_err();
        let records = fs::read_to_string(cfg.diagnostics_dir.join(PAPER_EXECUTION_ATTEMPTS_FILE))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["stage"], "started");
        assert_eq!(records[1]["stage"], "terminal");
        assert_eq!(records[1]["status"], "error");
        assert_eq!(records[0]["attempt_id"], records[1]["attempt_id"]);
        let attempt_id = records[0]["attempt_id"].as_str().unwrap();
        assert!(err.to_string().contains(attempt_id));
        assert!(paper_error_trade_note(&err).contains(&format!(
            "paper_attempt_id={attempt_id}; paper_attempt_status=error"
        )));
        assert!(fs::read_to_string(&log_path)
            .unwrap()
            .contains("buy slug-a yes"));

        fs::write(&log_path, "").unwrap();
        let invalid_diagnostics_dir = dir.join("diagnostics-is-a-file");
        fs::write(&invalid_diagnostics_dir, "not a directory").unwrap();
        cfg.diagnostics_dir = invalid_diagnostics_dir;
        let err = engine
            .execute_opportunity(&opp, &cfg, &HttpClient::new())
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("durable attempt-start write failed before submit"),
            "unexpected error: {err:#}"
        );
        assert!(!fs::read_to_string(&log_path)
            .unwrap()
            .contains("buy slug-a yes"));
    }

    #[tokio::test]
    async fn latest_trade_id_failure_is_not_treated_as_zero_baseline() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("polymarket-paper-history-fail-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let script_path = dir.join("fake-pm-trader.sh");
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
cmd=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir|--account)
      shift 2
      ;;
    *)
      cmd="$1"
      shift
      break
      ;;
  esac
done
if [[ "$cmd" == "history" ]]; then
  printf '{"ok":false,"code":"TRANSIENT","error":"history unavailable"}\n'
  exit 1
fi
printf '{"ok":false,"code":"UNEXPECTED","error":"unexpected command"}\n'
exit 1
"#;
        fs::write(&script_path, script).unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();

        let engine = ExternalPaperEngine {
            command: script_path.to_string_lossy().to_string(),
            data_dir: dir.join("paper").to_string_lossy().to_string(),
            account: "adapter-proof".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: false,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };

        let err = engine.latest_trade_id().await.unwrap_err();

        assert!(err.to_string().contains("history unavailable"));
    }

    #[test]
    fn paper_account_lock_is_exclusive_for_same_account() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("polymarket-paper-lock-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let first = acquire_paper_account_lock(dir.to_str().unwrap(), "account-a").unwrap();
        let error = acquire_paper_account_lock(dir.to_str().unwrap(), "account-a").unwrap_err();
        assert!(error
            .to_string()
            .contains("another paper scanner holds account lock"));
        acquire_paper_account_lock(dir.to_str().unwrap(), "account-b").unwrap();
        drop(first);
        acquire_paper_account_lock(dir.to_str().unwrap(), "account-a").unwrap();
    }

    #[tokio::test]
    async fn paper_engine_owns_account_lock_for_its_full_lifetime() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("polymarket-paper-engine-lock-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let script_path = dir.join("fake-pm-trader.sh");
        fs::write(
            &script_path,
            r#"#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir|--account) shift 2 ;;
    balance) printf '{"ok":true,"data":{"cash":10000}}\n'; exit 0 ;;
    *) shift ;;
  esac
done
printf '{"ok":false,"code":"UNEXPECTED","error":"unexpected command"}\n'
exit 1
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();

        let mut cfg = Config::from_env();
        cfg.external_paper_command = script_path.to_string_lossy().to_string();
        cfg.external_paper_data_dir = dir.join("paper");
        cfg.external_paper_account = "lifetime-lock".into();
        cfg.paper_use_limit_orders = false;

        let first = ExternalPaperEngine::new(&cfg).await.expect("first engine");
        let error = match ExternalPaperEngine::new(&cfg).await {
            Ok(_) => panic!("second engine must not share account"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("another paper scanner holds account lock"));
        drop(first);
        ExternalPaperEngine::new(&cfg)
            .await
            .expect("lock released when engine drops");

        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn fill_attribution_rejects_id_mismatch_and_manual_same_leg_trade() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("polymarket-paper-attribution-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let mode_path = dir.join("mode");
        let script_path = dir.join("fake-pm-trader.sh");
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
if [[ "$(cat '__MODE__')" == "extra" ]]; then
  printf '{"ok":true,"data":[{"id":1,"market_slug":"slug-a","outcome":"yes","side":"buy","amount_usd":8.0,"shares":20.0,"is_partial":false},{"id":2,"market_slug":"slug-a","outcome":"yes","side":"buy","amount_usd":1.0,"shares":2.5,"is_partial":false}]}\n'
else
  printf '{"ok":true,"data":[{"id":1,"market_slug":"slug-a","outcome":"yes","side":"buy","amount_usd":8.0,"shares":20.0,"is_partial":false}]}\n'
fi
"#
        .replace("__MODE__", &mode_path.to_string_lossy());
        fs::write(&script_path, script).unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();
        fs::write(&mode_path, "valid").unwrap();
        let engine = ExternalPaperEngine {
            command: script_path.to_string_lossy().to_string(),
            data_dir: dir.join("paper").to_string_lossy().to_string(),
            account: "attribution".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: false,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };
        let legs = vec![PaperOrderLeg {
            market_index: 0,
            market_slug: "slug-a".into(),
            token_id: "t1".into(),
            outcome: "yes".into(),
            unit_shares: 2.0,
            shares: 20.0,
            amount_usd: 8.0,
            limit_price: 0.4,
            tick_size: 0.01,
            label: "A".into(),
            min_order_shares: 1.0,
        }];
        let snapshots = vec![PlanLegSnapshot {
            market: market(),
            raw_ask: 0.4,
            limit_price: 0.4,
        }];
        let submission = |kind, id| {
            vec![PaperSubmission {
                kind,
                id,
                market_slug: "slug-a".into(),
                outcome: "yes".into(),
                response_amount_usd: (kind == PaperSubmissionKind::MarketTrade).then_some(8.0),
                response_shares: (kind == PaperSubmissionKind::MarketTrade).then_some(20.0),
            }]
        };

        let error = engine
            .collect_new_fills(
                0,
                &legs,
                &snapshots,
                &submission(PaperSubmissionKind::MarketTrade, 99),
                10,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("submission/history id mismatch"));

        fs::write(&mode_path, "extra").unwrap();
        let error = engine
            .collect_new_fills(
                0,
                &legs,
                &snapshots,
                &submission(PaperSubmissionKind::MarketTrade, 1),
                10,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("submission/history id mismatch"));

        fs::write(&mode_path, "valid").unwrap();
        let fills = engine
            .collect_new_fills(
                0,
                &legs,
                &snapshots,
                &submission(PaperSubmissionKind::LimitOrder, 7),
                10,
            )
            .await
            .unwrap();
        assert_eq!(fills[0].submission_id, 7);
        assert_eq!(fills[0].trades[0].trade_id, 1);
    }

    #[tokio::test]
    async fn sync_pending_orders_cancels_and_verifies_existing_gtc_orders() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("polymarket-paper-pending-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("fake-pm-trader.log");
        let state_path = dir.join("orders-list-count");
        let script_path = dir.join("fake-pm-trader.sh");
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
log="__LOG__"
state="__STATE__"
printf '%s\n' "$*" >> "$log"
cmd=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir|--account)
      shift 2
      ;;
    *)
      cmd="$1"
      shift
      break
      ;;
  esac
done
case "$cmd ${1:-}" in
  "orders check")
    printf '{"ok":true,"data":[]}\n'
    ;;
  "orders list")
    count=0
    if [[ -f "$state" ]]; then count="$(cat "$state")"; fi
    next=$((count + 1))
    printf '%s' "$next" > "$state"
    if [[ "$count" == "0" ]]; then
      printf '{"ok":true,"data":[{"id":7}]}\n'
    else
      printf '{"ok":true,"data":[]}\n'
    fi
    ;;
  "orders cancel")
    printf '{"ok":true,"data":{"id":%s}}\n' "${2:-0}"
    ;;
  *)
    printf '{"ok":false,"code":"UNEXPECTED","error":"unexpected command"}\n'
    exit 1
    ;;
esac
"#
        .replace("__LOG__", &log_path.to_string_lossy())
        .replace("__STATE__", &state_path.to_string_lossy());
        fs::write(&script_path, script).unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();

        let engine = ExternalPaperEngine {
            command: script_path.to_string_lossy().to_string(),
            data_dir: dir.join("paper").to_string_lossy().to_string(),
            account: "adapter-proof".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: true,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };

        engine
            .reconcile_pending_orders_exclusive()
            .await
            .expect("pending order cleanup");

        let calls = fs::read_to_string(log_path).unwrap();
        assert!(calls.contains("orders check"));
        assert!(calls.contains("orders list"));
        assert!(calls.contains("orders cancel 7"));
    }

    #[test]
    fn paper_submit_freshness_uses_live_refresh_deadline() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 1;

        let err =
            ensure_paper_submit_fresh(Instant::now() - Duration::from_millis(5), &cfg).unwrap_err();

        assert!(err
            .to_string()
            .contains("LIVE_MAX_REFRESH_TO_SUBMIT_MS=1ms"));
    }

    #[test]
    fn pre_submit_rejection_trade_status_requires_typed_error() {
        let rejection = pre_submit_rejection("final_profit", "fresh edge disappeared");
        let classified = paper_failure_trade_log(&rejection);
        assert_eq!(classified.status, "pre_submit_rejected");
        assert_eq!(
            classified.note,
            "paper_pre_submit_rejection_v1=final_profit; fresh edge disappeared"
        );

        let spoofed =
            anyhow!("paper_pre_submit_rejection_v1=final_profit; attacker-controlled adapter text");
        let classified = paper_failure_trade_log(&spoofed);
        assert_eq!(classified.status, "error");
        assert!(classified.note.starts_with(
            "paper_pre_submit_rejection_v1=final_profit; attacker-controlled adapter text"
        ));
        assert!(classified.note.ends_with("paper_attempt_status=error"));
    }

    #[test]
    fn analyze_basket_fills_uses_basket_units_not_raw_shares() {
        let engine = ExternalPaperEngine {
            command: "pm-trader".into(),
            data_dir: ".pm".into(),
            account: "acct".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: true,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };
        let fills = vec![
            ActualLegFill {
                market_slug: "slug-a".into(),
                outcome: "yes".into(),
                label: "A".into(),
                amount_usd: 1.6,
                fee_usd: 0.0,
                shares: 4.0,
                avg_price: 0.4,
                is_partial: false,
                unit_shares: 2.0,
                fee_rate: 0.0,
                fee_exponent: 1,
                submission_kind: PaperSubmissionKind::MarketTrade,
                submission_id: 1,
                trades: vec![],
            },
            ActualLegFill {
                market_slug: "slug-b".into(),
                outcome: "yes".into(),
                label: "B".into(),
                amount_usd: 0.4,
                fee_usd: 0.0,
                shares: 2.0,
                avg_price: 0.2,
                is_partial: false,
                unit_shares: 1.0,
                fee_rate: 0.0,
                fee_exponent: 1,
                submission_kind: PaperSubmissionKind::MarketTrade,
                submission_id: 2,
                trades: vec![],
            },
        ];
        let report = engine.analyze_basket_fills(fills, 2.0, 1.5, 0.0).unwrap();
        assert!((report.hedged_basket_units - 2.0).abs() < 1e-9);
        assert!((report.hedged_projection_usd - 1.0).abs() < 1e-9);
        assert!((report.conservative_campaign_pnl_usd - 1.0).abs() < 1e-9);
    }

    #[test]
    fn rejected_partial_basket_charges_unhedged_spend_to_campaign_pnl() {
        let engine = ExternalPaperEngine {
            command: "pm-trader".into(),
            data_dir: ".pm".into(),
            account: "acct".into(),
            account_lock: test_paper_account_lock(),
            order_type: "fok".into(),
            limit_order_type: "gtc".into(),
            use_limit_orders: true,
            filled_baskets: 0,
            parity_accepted_baskets: 0,
            parity_rejected_baskets: 0,
            attempted_legs: 0,
            executed_legs: 0,
            conservative_campaign_pnl_usd: 0.0,
            unhedged_notional_usd: 0.0,
        };
        let fills = vec![
            ActualLegFill {
                market_slug: "slug-a".into(),
                outcome: "yes".into(),
                label: "A".into(),
                amount_usd: 1.6,
                fee_usd: 0.0,
                shares: 4.0,
                avg_price: 0.4,
                is_partial: false,
                unit_shares: 2.0,
                fee_rate: 0.0,
                fee_exponent: 1,
                submission_kind: PaperSubmissionKind::MarketTrade,
                submission_id: 1,
                trades: vec![],
            },
            ActualLegFill {
                market_slug: "slug-b".into(),
                outcome: "yes".into(),
                label: "B".into(),
                amount_usd: 0.2,
                fee_usd: 0.0,
                shares: 1.0,
                avg_price: 0.2,
                is_partial: true,
                unit_shares: 1.0,
                fee_rate: 0.0,
                fee_exponent: 1,
                submission_kind: PaperSubmissionKind::MarketTrade,
                submission_id: 2,
                trades: vec![],
            },
        ];

        let report = engine.analyze_basket_fills(fills, 2.0, 1.5, 0.0).unwrap();

        assert!((report.hedged_projection_usd - 0.5).abs() < 1e-9);
        assert!((report.excess_notional_usd - 0.8).abs() < 1e-9);
        assert!((report.conservative_campaign_pnl_usd - (-0.3)).abs() < 1e-9);
    }
}
