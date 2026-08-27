#![allow(clippy::too_many_arguments)]

//! Polymarket Arbitrage Scanner — async orchestrator.
//!
//! Usage:
//!     cargo run                           # Default: scan with paper trading
//!     cargo run -- --paper                # Explicit paper trading mode
//!     cargo run -- --no-paper             # Scan only, no paper trades
//!     cargo run -- --duration 300         # Run for 5 minutes then stop
//!     cargo run -- --once                 # Single scan, then exit
//!     cargo run -- --no-clob              # Use Gamma estimates (faster)

mod accounting_snapshot;
mod arbitrage;
mod arbitrage_optimiser;
mod clob_client;
mod combo_rfq_client;
mod config;
mod convex_inference;
mod diagnostics;
mod diagnostics_daemon;
mod engine_mode;
mod execution_routes;
mod exposure;
mod external_paper_engine;
mod fees;
mod gamma_client;
mod gas_oracle;
mod geoblock;
mod live_executor;
mod market_sources;
mod models;
mod notifications;
mod onchain_fills;
mod protocol_drift;
mod protocol_preflight;
mod rfq_finality;
mod rfq_stream_client;
mod settlement_monitor;
mod strategy_lab;
mod user_channel;
mod ws_client;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::Parser;
use rayon::prelude::*;
use reqwest::Client;
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use config::Config;
use diagnostics::{
    timestamp_now, CandidateEvaluationRow, CandidateRejectionRow, DiagnosticsLogger,
    DiagnosticsPolicy, LatencyBudgetRow, ScanSummaryRow, TradeLogRow,
};
use exposure::ExposureTracker;
use external_paper_engine::{ExternalPaperEngine, PaperExecutionReport};
use gas_oracle::GasOracle;
use models::{
    is_external_token_id, is_supported_yes_no_full_family_plan, ArbType, ArbitrageOpportunity,
    Market, OpportunityLeg, OutcomeSide,
};
use strategy_lab::StrategyLab;
use ws_client::{DirtyTokenReceiver, PriceCache, WsCommand, WsSupervisor, WsWake};

/// Polymarket Arbitrage Scanner
#[derive(Parser, Debug)]
#[command(name = "polymarket-arb-scanner")]
#[command(about = "Polymarket arbitrage scanner for mispriced mutually-exclusive events")]
struct Cli {
    /// Enable paper trading
    #[arg(long, conflicts_with = "no_paper")]
    paper: bool,

    /// Disable paper trading
    #[arg(long, conflicts_with = "paper")]
    no_paper: bool,

    /// Skip CLOB API, use Gamma estimates only (faster but less accurate)
    #[arg(long)]
    no_clob: bool,

    /// Run for N seconds, then stop
    #[arg(long)]
    duration: Option<u64>,

    /// Run a single scan and exit
    #[arg(long)]
    once: bool,

    /// Enable live execution via external executor command
    #[arg(long)]
    live: bool,

    /// Internal confirmation passed only by the verified guarded live launcher
    #[arg(long, hide = true, requires_all = ["live", "activation_packet"])]
    guarded_live_confirmed: bool,

    /// Verified activation packet passed only by the guarded live launcher
    #[arg(long, hide = true, requires = "live")]
    activation_packet: Option<PathBuf>,

    /// Write a redacted fingerprint of the effective launch configuration and exit
    #[arg(long, hide = true, conflicts_with = "live")]
    launch_config_fingerprint_output: Option<PathBuf>,

    /// Require the effective configuration to match a previously written fingerprint
    #[arg(
        long,
        hide = true,
        requires = "live_diagnostics",
        conflicts_with = "live"
    )]
    expected_launch_config_fingerprint: Option<PathBuf>,

    /// Emit live readiness diagnostics; with --live, fall back to no-submit scans when routes are unavailable
    #[arg(long)]
    live_diagnostics: bool,

    /// Run an isolated scanner-owned paper execution canary and exit
    #[arg(long)]
    paper_execution_canary: bool,

    /// Output path for --paper-execution-canary JSON
    #[arg(long)]
    paper_execution_canary_output: Option<PathBuf>,

    /// FOK paper spend size for --paper-execution-canary
    #[arg(long, default_value_t = 1.0)]
    paper_execution_canary_amount_usd: f64,

    /// Run an isolated synthetic scanner paper trade proof and exit
    #[arg(long)]
    paper_scanner_trade_proof: bool,

    /// Output path for --paper-scanner-trade-proof JSON
    #[arg(long)]
    paper_scanner_trade_proof_output: Option<PathBuf>,

    /// Write a read-only live closeout/redeem plan from current account positions and exit
    #[arg(long, conflicts_with = "live_reconcile_run")]
    live_reconcile_plan: bool,

    /// Build a live closeout/redeem run; transaction submission also requires explicit confirmation
    #[arg(long, conflicts_with = "live_reconcile_plan")]
    live_reconcile_run: bool,

    /// Confirm that --live-reconcile-run may submit irreversible closeout transactions
    #[arg(long, requires = "live_reconcile_run")]
    confirm_live_closeout: bool,

    /// Write a read-only closeout payoff certificate from current account positions and exit
    #[arg(long, conflicts_with_all = ["live_reconcile_plan", "live_reconcile_run"])]
    live_closeout_certificate: bool,

    /// Write a read-only user-channel reconciliation report from saved live user events and exit
    #[arg(long, conflicts_with_all = ["live_reconcile_plan", "live_reconcile_run", "live_closeout_certificate"])]
    live_user_reconcile_report: bool,

    /// Seconds between scans
    #[arg(long)]
    interval: Option<u64>,
}

/// Run a single scan cycle. Returns number of opportunities found.
#[derive(Debug, Default, Clone)]
struct ScanStats {
    opportunities_found: usize,
    suppressed_duplicates: usize,
    neg_risk_events_total: usize,
    neg_risk_events_scanned: usize,
    yes_candidates_total: usize,
    no_candidates_total: usize,
    yes_selected_events: usize,
    no_selected_events: usize,
    bundle_markets_total: usize,
    bundle_markets_scanned: usize,
    ranked_families_discovered: usize,
    ranked_families_scanned: usize,
    quote_tokens_total: usize,
    quote_tokens_unique_selected: usize,
    selected_quote_tokens: Vec<String>,
    quote_cache_hits: usize,
    quote_rest_requested: usize,
    quote_rest_resolved: usize,
    quote_rest_batches: usize,
    quote_deferred_tokens: usize,
    quote_hard_unresolved_tokens: usize,
    quote_no_ask_tokens: usize,
    quote_missing_book_tokens: usize,
    ws_snapshot_wait_ms: f64,
    ws_snapshot_ready_tokens: usize,
    ws_snapshot_total_tokens: usize,
    ws_snapshot_min_ready_tokens: usize,
    ws_snapshot_satisfied: bool,
    raw_yes_candidates: usize,
    raw_no_candidates: usize,
    raw_bundle_candidates: usize,
    raw_ranked_candidates: usize,
    target_projection_rejections: usize,
    target_size_rejections: usize,
    theory_hint_yes: usize,
    theory_hint_no: usize,
    theory_hint_bundle: usize,
    quote_ready_yes_events: usize,
    quote_ready_no_events: usize,
    quote_ready_bundle_markets: usize,
    yes_opportunities: usize,
    no_opportunities: usize,
    bundle_opportunities: usize,
    ranked_opportunities: usize,
    combo_rfq_candidate_blocks: usize,
    best_raw_edge: Option<arbitrage::RawEdgeProbe>,
    operator_notes: Vec<String>,
    scan_duration_ms: f64,
    latency_budget_status: String,
    latency_budget_blockers: Vec<String>,
    cumulative_trades_executed: usize,
    cumulative_pnl_usd: f64,
    cumulative_pnl_pct: f64,
}

impl ScanStats {
    fn observe_raw_edge_probe(&mut self, probe: arbitrage::RawEdgeProbe) {
        let replace = self
            .best_raw_edge
            .as_ref()
            .map(|current| probe.net_profit > current.net_profit)
            .unwrap_or(true);
        if replace {
            self.best_raw_edge = Some(probe);
        }
    }
}

#[derive(Debug, Clone)]
struct DiscoveryCache {
    fetched_at: Instant,
    data: gamma_client::DiscoveryData,
    combo_catalog: Option<combo_rfq_client::ComboMarketCatalog>,
}

#[derive(Debug, Clone)]
struct CandidateSelection {
    idx: usize,
    score: f64,
    total_tokens: usize,
    cached_tokens: usize,
    missing_tokens: usize,
    quote_tokens: Vec<String>,
}

#[derive(Debug, Clone)]
struct PendingBundleCandidate {
    event: crate::models::Event,
    score: f64,
    total_tokens: usize,
    cached_tokens: usize,
    missing_tokens: usize,
    quote_tokens: Vec<String>,
}

#[derive(Debug, Default, Clone)]
struct ScanQuoteCacheSnapshot {
    fresh_quote_tokens: HashSet<String>,
    best_ask_prices: HashMap<String, f64>,
    toxicity_penalties: HashMap<String, f64>,
    execution_survival_adjustments: HashMap<String, f64>,
}

const WS_UNSUBSCRIBE_GRACE_SCANS: u64 = 3;
const WS_DIRTY_WAKE_DEBOUNCE_MS: u64 = 2;
const LIVE_STARTUP_CLOB_RTT_SAMPLES: usize = 3;
const LIVE_TOXIC_TRADE_WINDOW_MS: u64 = 1_000;
const LIVE_TOXIC_TRADE_MIN_NOTIONAL_USD: f64 = 5.0;
const LIVE_TOXIC_TRADE_POSITION_FRACTION: f64 = 0.10;
const LIVE_TOXIC_DEPTH_FLOW_WINDOWS_MS: [u64; 3] = [100, 250, 1_000];
const LIVE_TOXIC_DEPTH_FLOW_LEVELS: usize = 5;
const LIVE_TOXIC_DEPTH_FLOW_MIN_RATIO: f64 = 0.35;
const LIVE_TOXIC_DEPTH_FLOW_RATIO_MIN_NOTIONAL_USD: f64 = 1.0;
const LIVE_TOXIC_BOOK_DEPTH_LEVELS: usize = 3;
const LIVE_TOXIC_BOOK_IMBALANCE_MIN_RATIO: f64 = 0.80;
const LIVE_FRAGILE_BOOK_DEPTH_LEVELS: usize = 5;
const LIVE_FRAGILE_MIN_ASK_DEPTH_POSITION_MULTIPLIER: f64 = 2.0;
const LIVE_FRAGILE_TOP_ASK_MAX_RATIO: f64 = 0.85;
const LIVE_FRAGILE_TOP_ASK_DEPTH_CAP_MULTIPLIER: f64 = 4.0;
const EXECUTION_SURVIVAL_DEPTH_LEVELS: usize = 5;
const EXECUTION_SURVIVAL_MAX_TOKEN_BONUS: f64 = 1_200.0;
const EXECUTION_SURVIVAL_MAX_TOKEN_PENALTY: f64 = 3_500.0;
const OPPORTUNITY_MARKOUT_TOXICITY_USD_PER_SCORE: f64 = 0.001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WsSnapshotCoverage {
    ready: usize,
    total: usize,
    min_ready: usize,
    satisfied: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct DrainedWsWakes {
    dirty_tokens: HashSet<String>,
    discovery_wake: bool,
}

#[derive(Clone, Default)]
struct ShutdownCoordinator {
    requested: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl ShutdownCoordinator {
    fn request(&self) -> bool {
        let first_request = !self.requested.swap(true, AtomicOrdering::AcqRel);
        if first_request {
            self.notify.notify_one();
        }
        first_request
    }

    fn is_requested(&self) -> bool {
        self.requested.load(AtomicOrdering::Acquire)
    }

    async fn wait_requested(&self) {
        let notified = self.notify.notified();
        if self.is_requested() {
            return;
        }
        notified.await;
    }
}

fn install_shutdown_signal_handlers(shutdown: &ShutdownCoordinator) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate())
            .context("installing graceful SIGTERM handler for scanner")?;
        let sigterm_shutdown = shutdown.clone();
        tokio::spawn(async move {
            if terminate.recv().await.is_some() && sigterm_shutdown.request() {
                info!("SIGTERM received; draining the active scan before shutdown");
            }
        });
    }

    let ctrl_c_shutdown = shutdown.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) if ctrl_c_shutdown.request() => {
                info!("CTRL-C received; draining the active scan before shutdown");
            }
            Ok(()) => {}
            Err(err) => warn!("CTRL-C handler failed: {err}"),
        }
    });

    Ok(())
}

fn push_operator_note(stats: &mut ScanStats, note: impl Into<String>) {
    let note = note.into();
    if note.trim().is_empty() {
        return;
    }
    if stats
        .operator_notes
        .last()
        .map(|last| last == &note)
        .unwrap_or(false)
    {
        return;
    }
    stats.operator_notes.push(note);
    if stats.operator_notes.len() > 8 {
        let overflow = stats.operator_notes.len().saturating_sub(8);
        stats.operator_notes.drain(0..overflow);
    }
}

fn selected_rank_map(selected_indices: &[usize]) -> HashMap<usize, usize> {
    selected_indices
        .iter()
        .enumerate()
        .map(|(rank, idx)| (*idx, rank + 1))
        .collect()
}

fn candidate_selection_state(
    selected_ranks: &HashMap<usize, usize>,
    dirty_candidate_indices: &HashSet<usize>,
    idx: usize,
) -> &'static str {
    match (
        selected_ranks.contains_key(&idx),
        dirty_candidate_indices.contains(&idx),
    ) {
        (true, true) => "selected_dirty",
        (true, false) => "selected",
        (false, true) => "deferred_dirty",
        (false, false) => "deferred_by_rotation_or_budget",
    }
}

fn opportunity_legs_summary(opp: &ArbitrageOpportunity) -> String {
    let legs = if !opp.execution_plan.is_empty() {
        opp.execution_plan
            .iter()
            .map(|leg| {
                format!(
                    "{}:{}@{:.4}",
                    short_text(&leg.question, 36),
                    leg.outcome,
                    leg.reference_price
                )
            })
            .collect::<Vec<_>>()
    } else {
        opp.markets
            .iter()
            .map(|market| short_text(&market.question, 36))
            .collect::<Vec<_>>()
    };
    legs.join(" | ")
}

fn runtime_scan_log(config: &Config, message: String) {
    if config.verbose_scan_logs {
        info!("{}", message);
    } else {
        debug!("{}", message);
    }
}

fn runtime_scan_warn(config: &Config, message: String) {
    if config.verbose_scan_logs {
        warn!("{}", message);
    } else {
        debug!("{}", message);
    }
}

fn opportunity_fingerprint(opp: &ArbitrageOpportunity) -> String {
    if !opp.execution_plan.is_empty() {
        let mut legs: Vec<String> = opp
            .execution_plan
            .iter()
            .map(|leg| {
                let key = if !leg.condition_id.is_empty() {
                    leg.condition_id.clone()
                } else if !leg.market_slug.is_empty() {
                    leg.market_slug.clone()
                } else {
                    leg.question.clone()
                };
                format!(
                    "{}:{}:{:.6}:{:.4}",
                    key, leg.outcome, leg.unit_shares, leg.reference_price
                )
            })
            .collect();
        legs.sort_unstable();
        return format!("{}|{}|{}", opp.event_id, opp.arb_type, legs.join(","));
    }

    let mut condition_ids: Vec<&str> = opp
        .markets
        .iter()
        .map(|m| {
            if m.condition_id.is_empty() {
                m.question.as_str()
            } else {
                m.condition_id.as_str()
            }
        })
        .collect();
    condition_ids.sort_unstable();
    format!(
        "{}|{}|{}",
        opp.event_id,
        opp.arb_type,
        condition_ids.join(",")
    )
}

fn intended_execution_position_usd(
    config: &Config,
    paper_execution_enabled: bool,
    live_execution: bool,
) -> f64 {
    if live_execution {
        config.live_trade_position_size_usd
    } else if paper_execution_enabled {
        config.effective_paper_position_size_usd()
    } else {
        config
            .effective_paper_position_size_usd()
            .max(config.live_trade_position_size_usd)
    }
}

fn inferred_trade_gas_cost(opp: &ArbitrageOpportunity) -> f64 {
    opp.estimated_total_gas_cost_usd.max(0.0)
}

fn project_opportunity_for_target_size(
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
    config: &Config,
) -> Option<ArbitrageOpportunity> {
    if target_position_usd <= f64::EPSILON || opp.total_cost <= f64::EPSILON {
        return None;
    }

    let defer_depth_cap = config.validate_opportunities_at_target_size
        && opp.prices_from_clob
        && !opp.execution_plan.is_empty();
    let effective_position_usd = if !defer_depth_cap
        && opp.max_executable_size_usd.is_finite()
        && opp.max_executable_size_usd > 0.0
    {
        target_position_usd.min(opp.max_executable_size_usd)
    } else {
        target_position_usd
    };
    if effective_position_usd <= f64::EPSILON {
        return None;
    }

    let basket_units = effective_position_usd / opp.total_cost;
    if basket_units <= f64::EPSILON {
        return None;
    }

    let basket_net = opp.gross_profit - opp.total_fees;
    if basket_net <= 0.0 {
        return None;
    }

    let projected_total_pnl = basket_units * basket_net - inferred_trade_gas_cost(opp);
    let projected_roi_pct = if effective_position_usd > f64::EPSILON {
        projected_total_pnl / effective_position_usd * 100.0
    } else {
        0.0
    };

    if projected_total_pnl < config.min_net_profit_usd || projected_roi_pct < config.min_roi_pct {
        return None;
    }

    let mut projected = opp.clone();
    projected.net_profit = projected_total_pnl;
    projected.roi_pct = projected_roi_pct;
    projected.max_executable_size_usd = effective_position_usd;
    Some(projected)
}

fn round_down_to_step(value: f64, step: f64) -> f64 {
    let step = if step.is_finite() && step > 0.0 {
        step
    } else {
        0.0001
    };
    ((value / step).floor() * step * 1_000_000.0).round() / 1_000_000.0
}

fn collect_quote_token_ids(events: &[crate::models::Event], config: &Config) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    for event in events {
        for market in &event.markets {
            if market.closed
                || market.liquidity < config.min_liquidity_usd
                || !fees::market_fee_curve_supported(market)
            {
                continue;
            }
            for token_id in [&market.clob_token_id_yes, &market.clob_token_id_no] {
                if !token_id.is_empty()
                    && !is_external_token_id(token_id)
                    && seen.insert(token_id.clone())
                {
                    tokens.push(token_id.clone());
                }
            }
        }
    }
    tokens
}

fn event_quote_token_ids(event: &crate::models::Event, config: &Config) -> Vec<String> {
    collect_quote_token_ids(std::slice::from_ref(event), config)
}

fn collect_quote_token_ids_for_side(
    events: &[crate::models::Event],
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    for event in events {
        for market in &event.markets {
            if market.closed
                || market.liquidity < config.min_liquidity_usd
                || !fees::market_fee_curve_supported(market)
            {
                continue;
            }
            let token_id = market.token_id_for_outcome(outcome);
            if !token_id.is_empty()
                && !is_external_token_id(token_id)
                && seen.insert(token_id.to_string())
            {
                tokens.push(token_id.to_string());
            }
        }
    }
    tokens
}

fn event_quote_token_ids_for_side(
    event: &crate::models::Event,
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> Vec<String> {
    collect_quote_token_ids_for_side(std::slice::from_ref(event), config, outcome)
}

fn event_with_side_only(
    event: &crate::models::Event,
    outcome: crate::models::OutcomeSide,
) -> crate::models::Event {
    let mut cloned = event.clone();
    for market in &mut cloned.markets {
        match outcome {
            crate::models::OutcomeSide::Yes => {
                market.clob_token_id_no.clear();
                market.clob_no_ask = None;
                market.clob_no_bid = None;
                market.clob_no_ask_size = None;
            }
            crate::models::OutcomeSide::No => {
                market.clob_token_id_yes.clear();
                market.clob_yes_ask = None;
                market.clob_yes_bid = None;
                market.clob_yes_ask_size = None;
            }
        }
    }
    cloned
}

fn candidate_token_index(candidates: &[CandidateSelection]) -> HashMap<&str, Vec<usize>> {
    let mut index: HashMap<&str, Vec<usize>> = HashMap::new();
    for candidate in candidates {
        for token in &candidate.quote_tokens {
            let token = token.trim();
            if !token.is_empty() {
                index.entry(token).or_default().push(candidate.idx);
            }
        }
    }
    index
}

fn dirty_candidate_indices_from_index(
    token_index: &HashMap<&str, Vec<usize>>,
    dirty_tokens: &HashSet<String>,
) -> HashSet<usize> {
    if dirty_tokens.is_empty() {
        return HashSet::new();
    }
    let mut indices = HashSet::new();
    for token in dirty_tokens {
        if let Some(candidate_indices) = token_index.get(token.as_str()) {
            indices.extend(candidate_indices.iter().copied());
        }
    }
    indices
}

fn dirty_subscription_fast_lane_cap(config: &Config) -> usize {
    config
        .clob_book_batch_size
        .max(1)
        .min(config.quote_refresh_token_budget_per_scan.max(1))
}

fn dirty_subscription_fast_lane_tokens(
    dirty_tokens: &HashSet<String>,
    desired_subscriptions: &HashSet<String>,
    subscribed_quote_tokens: &HashSet<String>,
    cap: usize,
) -> Vec<String> {
    if dirty_tokens.is_empty() || cap == 0 {
        return Vec::new();
    }

    let mut tokens: Vec<String> = dirty_tokens
        .iter()
        .filter(|token_id| !token_id.trim().is_empty())
        .filter(|token_id| !desired_subscriptions.contains(*token_id))
        .filter(|token_id| !subscribed_quote_tokens.contains(*token_id))
        .cloned()
        .collect();
    tokens.sort_unstable();
    tokens.truncate(cap);
    tokens
}

fn bundle_market_event(
    parent: &crate::models::Event,
    market: &crate::models::Market,
) -> crate::models::Event {
    crate::models::Event {
        event_id: parent.event_id.clone(),
        title: parent.title.clone(),
        slug: parent.slug.clone(),
        category: parent.category.clone(),
        enable_neg_risk: parent.enable_neg_risk,
        neg_risk: parent.neg_risk,
        neg_risk_augmented: parent.neg_risk_augmented,
        lifecycle: parent.lifecycle.clone(),
        markets: vec![market.clone()],
    }
}

fn candidate_selection_for_event_side(
    idx: usize,
    event: &crate::models::Event,
    cached_tokens: &HashSet<String>,
    best_ask_prices: &HashMap<String, f64>,
    toxicity_penalties: &HashMap<String, f64>,
    execution_survival_adjustments: &HashMap<String, f64>,
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> Option<CandidateSelection> {
    let mut score = neg_risk_event_priority_score_for_side(event, cached_tokens, config, outcome);
    if !score.is_finite() {
        return None;
    }
    let quote_tokens = event_quote_token_ids_for_side(event, config, outcome);
    let (total_tokens, cached_count, missing_tokens) = if quote_tokens.is_empty() {
        event_quote_counts_for_side(event, cached_tokens, config, outcome)
    } else {
        quote_counts_for_token_ids(&quote_tokens, cached_tokens)
    };
    if total_tokens == 0 {
        return None;
    }
    let guaranteed_revenue = match outcome {
        crate::models::OutcomeSide::Yes => 1.0,
        crate::models::OutcomeSide::No => total_tokens.saturating_sub(1) as f64,
    };
    score += ws_ask_edge_score_bonus(
        ws_ask_edge_hint(&quote_tokens, best_ask_prices, guaranteed_revenue),
        total_tokens,
    );
    score += quote_execution_survival_adjustment(&quote_tokens, execution_survival_adjustments);
    score -= quote_toxicity_penalty(&quote_tokens, toxicity_penalties);
    Some(CandidateSelection {
        idx,
        score,
        total_tokens,
        cached_tokens: cached_count,
        missing_tokens,
        quote_tokens,
    })
}

fn neg_risk_candidate_selections_for_side(
    events: &[crate::models::Event],
    cached_tokens: &HashSet<String>,
    best_ask_prices: &HashMap<String, f64>,
    toxicity_penalties: &HashMap<String, f64>,
    execution_survival_adjustments: &HashMap<String, f64>,
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> Vec<CandidateSelection> {
    events
        .par_iter()
        .enumerate()
        .filter_map(|(idx, event)| {
            candidate_selection_for_event_side(
                idx,
                event,
                cached_tokens,
                best_ask_prices,
                toxicity_penalties,
                execution_survival_adjustments,
                config,
                outcome,
            )
        })
        .collect()
}

fn pending_bundle_candidates_for_event(
    parent_event: &crate::models::Event,
    cached_tokens: &HashSet<String>,
    best_ask_prices: &HashMap<String, f64>,
    toxicity_penalties: &HashMap<String, f64>,
    execution_survival_adjustments: &HashMap<String, f64>,
    config: &Config,
) -> Vec<PendingBundleCandidate> {
    let mut candidates = Vec::new();
    for market in &parent_event.markets {
        let mut score = bundle_market_priority_score(parent_event, market, cached_tokens, config);
        if !score.is_finite() {
            continue;
        }
        let pseudo_event = bundle_market_event(parent_event, market);
        let quote_tokens = event_quote_token_ids(&pseudo_event, config);
        let (total_tokens, cached_count, missing_tokens) = if quote_tokens.is_empty() {
            event_quote_counts(&pseudo_event, cached_tokens, config)
        } else {
            quote_counts_for_token_ids(&quote_tokens, cached_tokens)
        };
        if total_tokens < 2 {
            continue;
        }
        score += ws_ask_edge_score_bonus(
            ws_ask_edge_hint(&quote_tokens, best_ask_prices, 1.0),
            total_tokens,
        );
        score += quote_execution_survival_adjustment(&quote_tokens, execution_survival_adjustments);
        score -= quote_toxicity_penalty(&quote_tokens, toxicity_penalties);
        candidates.push(PendingBundleCandidate {
            event: pseudo_event,
            score,
            total_tokens,
            cached_tokens: cached_count,
            missing_tokens,
            quote_tokens,
        });
    }
    candidates
}

fn bundle_market_candidate_selections(
    bundle_source_events: &[crate::models::Event],
    cached_tokens: &HashSet<String>,
    best_ask_prices: &HashMap<String, f64>,
    toxicity_penalties: &HashMap<String, f64>,
    execution_survival_adjustments: &HashMap<String, f64>,
    config: &Config,
) -> (Vec<crate::models::Event>, Vec<CandidateSelection>) {
    if !config.enable_bundle_scanning_all_events {
        return (Vec::new(), Vec::new());
    }

    let pending_by_event: Vec<Vec<PendingBundleCandidate>> = bundle_source_events
        .par_iter()
        .map(|event| {
            pending_bundle_candidates_for_event(
                event,
                cached_tokens,
                best_ask_prices,
                toxicity_penalties,
                execution_survival_adjustments,
                config,
            )
        })
        .collect();

    let mut bundle_market_pool = Vec::new();
    let mut bundle_market_candidates_meta = Vec::new();
    for pending_group in pending_by_event {
        for pending in pending_group {
            let idx = bundle_market_pool.len();
            bundle_market_pool.push(pending.event);
            bundle_market_candidates_meta.push(CandidateSelection {
                idx,
                score: pending.score,
                total_tokens: pending.total_tokens,
                cached_tokens: pending.cached_tokens,
                missing_tokens: pending.missing_tokens,
                quote_tokens: pending.quote_tokens,
            });
        }
    }

    (bundle_market_pool, bundle_market_candidates_meta)
}

fn lifecycle_rejection_note(event: &crate::models::Event, reason: &str) -> String {
    let now = chrono::Utc::now();
    let mut note = reason.to_string();
    if let Some(hours) = event.lifecycle.capital_lock_hours_from(now) {
        note = format!("{note}; capital_lock_hours={hours:.2}");
    }
    if let Some(source) = event.lifecycle.resolution_source.as_deref() {
        note = format!("{note}; resolution_source={}", short_text(source, 96));
    }
    if let Some(status) = event.lifecycle.uma_resolution_status.as_deref() {
        note = format!("{note}; uma_resolution_status={}", short_text(status, 64));
    }
    note
}

fn capital_velocity_score_adjustment(
    lifecycle: &crate::models::EventLifecycle,
    edge_hint: f64,
    config: &Config,
    now: chrono::DateTime<chrono::Utc>,
) -> f64 {
    if !config.capital_velocity_ranking_enabled || edge_hint <= f64::EPSILON {
        return 0.0;
    }
    let Some(lock_hours) = lifecycle.capital_lock_hours_from(now) else {
        return 0.0;
    };
    let reference_hours = config.capital_velocity_reference_hours.max(1.0);
    let bounded_hours = lock_hours.clamp(1.0, reference_hours * 30.0);
    let velocity_multiplier = (reference_hours / bounded_hours).sqrt().clamp(0.25, 3.0);
    edge_hint * config.capital_velocity_score_weight.max(0.0) * (velocity_multiplier - 1.0)
}

fn filter_lifecycle_scan_events(
    events: &[crate::models::Event],
    config: &Config,
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    pool: &str,
    arb_type: &str,
) -> (Vec<crate::models::Event>, usize) {
    if !config.event_lifecycle_gate_enabled {
        return (events.to_vec(), 0);
    }

    let now = chrono::Utc::now();
    let mut kept = Vec::with_capacity(events.len());
    let mut rejected = 0usize;
    for event in events {
        if let Some(reason) = event.lifecycle.scan_block_reason(
            now,
            config.event_lifecycle_pre_cutoff_buffer_secs,
            config.live_game_start_quarantine_secs,
        ) {
            rejected += 1;
            log_candidate_rejection(
                diagnostics,
                scan_index,
                pool,
                event,
                arb_type,
                None,
                "lifecycle",
                "event_lifecycle_cutoff",
                0.0,
                false,
                None,
                lifecycle_rejection_note(event, &reason),
            );
        } else {
            kept.push(event.clone());
        }
    }
    (kept, rejected)
}

fn market_has_external_quotes(market: &crate::models::Market) -> bool {
    is_external_token_id(market.clob_token_id_yes.trim())
        || is_external_token_id(market.clob_token_id_no.trim())
}

fn selection_cache_max_age(config: &Config) -> Duration {
    let max_age = config.ws_quote_max_age_ms.max(1);
    let discovery_bound = config.discovery_interval_secs.max(1).saturating_mul(2) * 1000;
    let bounded = max_age.min(discovery_bound).min(15000);
    Duration::from_millis(bounded)
}

fn opportunity_markout_base_latency_haircut_usd(config: &Config, position_usd: f64) -> f64 {
    config.live_edge_haircut_usd.max(0.0)
        + position_usd.max(0.0) * config.live_edge_haircut_bps as f64 / 10_000.0
}

fn opportunity_markout_latency_haircut_usd(
    config: &Config,
    position_usd: f64,
    max_snapshot_age: Duration,
    max_allowed_age: Duration,
) -> f64 {
    let full_haircut_age = Duration::from_millis(config.live_max_refresh_to_submit_ms.max(1))
        .min(max_allowed_age)
        .as_secs_f64()
        .max(0.001);
    let age_ratio = (max_snapshot_age.as_secs_f64() / full_haircut_age).clamp(0.0, 1.0);
    opportunity_markout_base_latency_haircut_usd(config, position_usd) * age_ratio
}

fn markout_leg_fill_survival_probability(
    snapshot: &crate::ws_client::Price,
    required_notional_usd: f64,
    now: Instant,
) -> f64 {
    if required_notional_usd <= f64::EPSILON {
        return 1.0;
    }

    let depth_notional = top_depth_notional(&snapshot.ask_depth, EXECUTION_SURVIVAL_DEPTH_LEVELS);
    let depth_ratio = depth_notional / required_notional_usd;
    let mut probability =
        (depth_ratio / LIVE_FRAGILE_MIN_ASK_DEPTH_POSITION_MULTIPLIER).clamp(0.0, 1.0);

    if let Some(top_ratio) =
        top_depth_notional_ratio(&snapshot.ask_depth, EXECUTION_SURVIVAL_DEPTH_LEVELS)
    {
        if top_ratio >= LIVE_FRAGILE_TOP_ASK_MAX_RATIO {
            let concentration_penalty = ((top_ratio - LIVE_FRAGILE_TOP_ASK_MAX_RATIO)
                / (1.0 - LIVE_FRAGILE_TOP_ASK_MAX_RATIO).max(f64::EPSILON))
            .clamp(0.0, 1.0);
            probability *= 1.0 - concentration_penalty * 0.50;
        }
    }

    if let Some((depletion_ratio, _, _)) =
        recent_ask_queue_depletion_pressure(snapshot, now, EXECUTION_SURVIVAL_DEPTH_LEVELS)
    {
        probability *= 1.0 - depletion_ratio.clamp(0.0, 0.95);
    }

    probability.clamp(0.0, 1.0)
}

fn scan_toxicity_position_usd(config: &Config) -> f64 {
    config
        .live_trade_position_size_usd
        .max(config.effective_paper_position_size_usd())
        .max(config.external_paper_min_order_usd)
        .max(0.0)
}

fn recent_adverse_depth_flow_notional(
    price: &crate::ws_client::Price,
    now: Instant,
    levels: usize,
) -> (f64, u64) {
    LIVE_TOXIC_DEPTH_FLOW_WINDOWS_MS
        .iter()
        .map(|window_ms| {
            let window = Duration::from_millis(*window_ms);
            let notional = price
                .recent_depth_changes
                .iter()
                .filter(|change| {
                    now.checked_duration_since(change.observed_at)
                        .map(|age| age <= window)
                        .unwrap_or(false)
                })
                .filter(|change| change.level_index.is_some_and(|level| level < levels))
                .map(|change| {
                    let size_delta = change.new_size - change.old_size;
                    let signed_shares = match change.side.as_str() {
                        "BID" => size_delta,
                        "ASK" => -size_delta,
                        _ => 0.0,
                    };
                    signed_shares * change.price.max(0.0)
                })
                .sum::<f64>()
                .max(0.0);
            (notional, *window_ms)
        })
        .max_by(|(left, _), (right, _)| left.total_cmp(right))
        .unwrap_or((0.0, 0))
}

fn recent_ask_queue_depletion_pressure(
    price: &crate::ws_client::Price,
    now: Instant,
    levels: usize,
) -> Option<(f64, f64, u64)> {
    let ask_depth_notional = top_depth_notional(&price.ask_depth, levels);
    if ask_depth_notional <= f64::EPSILON {
        return None;
    }

    LIVE_TOXIC_DEPTH_FLOW_WINDOWS_MS
        .iter()
        .filter_map(|window_ms| {
            let window = Duration::from_millis(*window_ms);
            let depletion_notional = price
                .recent_depth_changes
                .iter()
                .filter(|change| change.side == "ASK")
                .filter(|change| change.level_index.is_some_and(|level| level < levels))
                .filter(|change| {
                    now.checked_duration_since(change.observed_at)
                        .map(|age| age <= window)
                        .unwrap_or(false)
                })
                .map(|change| (change.old_size - change.new_size) * change.price.max(0.0))
                .sum::<f64>()
                .max(0.0);
            (depletion_notional > f64::EPSILON).then_some((
                depletion_notional / ask_depth_notional,
                depletion_notional,
                *window_ms,
            ))
        })
        .max_by(|(left, _, _), (right, _, _)| left.total_cmp(right))
}

fn scan_quote_toxicity_penalty(price: &crate::ws_client::Price, config: &Config) -> f64 {
    scan_quote_toxicity_penalty_for_position(price, config, scan_toxicity_position_usd(config))
}

fn scan_quote_toxicity_penalty_for_position(
    price: &crate::ws_client::Price,
    config: &Config,
    position_usd: f64,
) -> f64 {
    let mut penalty = 0.0;
    let position_usd = position_usd.max(0.0);
    let threshold_usd =
        (position_usd * LIVE_TOXIC_TRADE_POSITION_FRACTION).max(LIVE_TOXIC_TRADE_MIN_NOTIONAL_USD);
    let now = Instant::now();
    let recent_buy_notional = price
        .recent_trades
        .iter()
        .filter(|trade| trade.side == "BUY")
        .filter(|trade| {
            now.checked_duration_since(trade.observed_at)
                .map(|age| age <= Duration::from_millis(LIVE_TOXIC_TRADE_WINDOW_MS))
                .unwrap_or(false)
        })
        .map(|trade| trade.price.max(0.0) * trade.size.max(0.0))
        .sum::<f64>();
    if recent_buy_notional >= threshold_usd {
        penalty += 700.0 * (recent_buy_notional / threshold_usd).clamp(1.0, 5.0);
    }
    let (adverse_depth_flow_notional, _) =
        recent_adverse_depth_flow_notional(price, now, LIVE_TOXIC_DEPTH_FLOW_LEVELS);
    if adverse_depth_flow_notional >= threshold_usd {
        penalty += 650.0 * (adverse_depth_flow_notional / threshold_usd).clamp(1.0, 5.0);
    } else if let Some((flow_ratio, _)) =
        adverse_depth_flow_pressure(price, adverse_depth_flow_notional)
    {
        if flow_ratio >= LIVE_TOXIC_DEPTH_FLOW_MIN_RATIO {
            penalty += 520.0 * (flow_ratio / LIVE_TOXIC_DEPTH_FLOW_MIN_RATIO).clamp(1.0, 3.0);
        }
    }

    let ask_notional = top_depth_notional(&price.ask_depth, LIVE_TOXIC_BOOK_DEPTH_LEVELS);
    let bid_notional = top_depth_notional(&price.bid_depth, LIVE_TOXIC_BOOK_DEPTH_LEVELS);
    let total_notional = ask_notional + bid_notional;
    if bid_notional >= threshold_usd && total_notional > f64::EPSILON {
        let bid_ratio = bid_notional / total_notional;
        if bid_ratio >= LIVE_TOXIC_BOOK_IMBALANCE_MIN_RATIO {
            penalty += 900.0
                + 1_200.0
                    * ((bid_ratio - LIVE_TOXIC_BOOK_IMBALANCE_MIN_RATIO)
                        / (1.0 - LIVE_TOXIC_BOOK_IMBALANCE_MIN_RATIO).max(f64::EPSILON))
                    .clamp(0.0, 1.0);
        }
    }

    if let (Some(ask), Some((microprice, queue_imbalance))) =
        (live_clob_executable_ask(price), live_clob_microprice(price))
    {
        let adverse_bps = ((ask - microprice) / microprice.max(f64::EPSILON)) * 10_000.0;
        let max_adverse_bps = config.live_clob_microprice_adverse_bps.max(0.0);
        if queue_imbalance > 0.0 && adverse_bps > max_adverse_bps {
            penalty += 850.0 + (adverse_bps - max_adverse_bps).min(75.0) * 35.0;
        }
    }

    let ask_near_notional = top_depth_notional(&price.ask_depth, LIVE_FRAGILE_BOOK_DEPTH_LEVELS);
    if position_usd > f64::EPSILON && ask_near_notional > f64::EPSILON {
        let min_near_depth = position_usd * LIVE_FRAGILE_MIN_ASK_DEPTH_POSITION_MULTIPLIER;
        if ask_near_notional < min_near_depth {
            penalty +=
                650.0 * (min_near_depth / ask_near_notional.max(f64::EPSILON)).clamp(1.0, 4.0);
        }
        if let Some(top_ratio) =
            top_depth_notional_ratio(&price.ask_depth, LIVE_FRAGILE_BOOK_DEPTH_LEVELS)
        {
            let concentration_cap = position_usd * LIVE_FRAGILE_TOP_ASK_DEPTH_CAP_MULTIPLIER;
            if top_ratio >= LIVE_FRAGILE_TOP_ASK_MAX_RATIO && ask_near_notional < concentration_cap
            {
                penalty += 750.0
                    * (top_ratio / LIVE_FRAGILE_TOP_ASK_MAX_RATIO.max(f64::EPSILON))
                        .clamp(1.0, 2.0);
            }
        }
    }

    penalty
}

fn scan_quote_execution_survival_adjustment(
    price: &crate::ws_client::Price,
    config: &Config,
) -> f64 {
    let mut adjustment = 0.0;
    let position_usd = scan_toxicity_position_usd(config);
    let now = Instant::now();

    if position_usd > f64::EPSILON {
        let ask_depth_notional =
            top_depth_notional(&price.ask_depth, EXECUTION_SURVIVAL_DEPTH_LEVELS);
        if ask_depth_notional > f64::EPSILON {
            let depth_ratio = ask_depth_notional / position_usd;
            adjustment += if depth_ratio >= LIVE_FRAGILE_MIN_ASK_DEPTH_POSITION_MULTIPLIER {
                320.0
                    * (depth_ratio / LIVE_FRAGILE_MIN_ASK_DEPTH_POSITION_MULTIPLIER).clamp(1.0, 3.0)
            } else {
                -850.0 * (LIVE_FRAGILE_MIN_ASK_DEPTH_POSITION_MULTIPLIER - depth_ratio)
            };
        } else {
            adjustment -= 900.0;
        }

        if let Some(top_ratio) =
            top_depth_notional_ratio(&price.ask_depth, EXECUTION_SURVIVAL_DEPTH_LEVELS)
        {
            if top_ratio <= 0.55 {
                adjustment += 180.0 * (0.55 - top_ratio) / 0.55;
            } else if top_ratio >= LIVE_FRAGILE_TOP_ASK_MAX_RATIO {
                adjustment -= 500.0
                    * (top_ratio / LIVE_FRAGILE_TOP_ASK_MAX_RATIO.max(f64::EPSILON))
                        .clamp(1.0, 2.0);
            }
        }
    }

    if let (Some(ask), Some((microprice, queue_imbalance))) =
        (live_clob_executable_ask(price), live_clob_microprice(price))
    {
        let edge_bps = ((microprice - ask) / microprice.max(f64::EPSILON)) * 10_000.0;
        if edge_bps >= 0.0 {
            adjustment += edge_bps.min(40.0) * 18.0;
        } else if queue_imbalance > 0.0 {
            adjustment += edge_bps.max(-80.0) * 22.0;
        }
    }

    let threshold_usd =
        (position_usd * LIVE_TOXIC_TRADE_POSITION_FRACTION).max(LIVE_TOXIC_TRADE_MIN_NOTIONAL_USD);
    let (recent_buy_notional, recent_sell_notional) = price
        .recent_trades
        .iter()
        .filter(|trade| {
            now.checked_duration_since(trade.observed_at)
                .map(|age| age <= Duration::from_millis(LIVE_TOXIC_TRADE_WINDOW_MS))
                .unwrap_or(false)
        })
        .fold((0.0, 0.0), |(buy, sell), trade| {
            let notional = trade.price.max(0.0) * trade.size.max(0.0);
            if trade.side == "BUY" {
                (buy + notional, sell)
            } else if trade.side == "SELL" {
                (buy, sell + notional)
            } else {
                (buy, sell)
            }
        });
    if threshold_usd > f64::EPSILON {
        adjustment +=
            ((recent_sell_notional - recent_buy_notional) / threshold_usd).clamp(-4.0, 2.0) * 220.0;
        let (adverse_depth_flow_notional, _) =
            recent_adverse_depth_flow_notional(price, now, LIVE_TOXIC_DEPTH_FLOW_LEVELS);
        adjustment -= (adverse_depth_flow_notional / threshold_usd).clamp(0.0, 5.0) * 260.0;
    }
    if let Some((depletion_ratio, _, _)) =
        recent_ask_queue_depletion_pressure(price, now, EXECUTION_SURVIVAL_DEPTH_LEVELS)
    {
        adjustment -= depletion_ratio.clamp(0.0, 3.0) * 900.0;
    }

    let max_age = selection_cache_max_age(config);
    let age_ratio = price.last_updated.elapsed().as_secs_f64() / max_age.as_secs_f64().max(0.001);
    adjustment += if age_ratio <= 0.25 {
        180.0
    } else if age_ratio >= 0.75 {
        -450.0 * ((age_ratio - 0.75) / 0.25).clamp(0.0, 1.0)
    } else {
        0.0
    };

    adjustment.clamp(
        -EXECUTION_SURVIVAL_MAX_TOKEN_PENALTY,
        EXECUTION_SURVIVAL_MAX_TOKEN_BONUS,
    )
}

fn quote_toxicity_penalty(token_ids: &[String], toxicity_penalties: &HashMap<String, f64>) -> f64 {
    token_ids
        .iter()
        .filter_map(|token_id| toxicity_penalties.get(token_id))
        .sum()
}

fn quote_execution_survival_adjustment(
    token_ids: &[String],
    execution_survival_adjustments: &HashMap<String, f64>,
) -> f64 {
    token_ids
        .iter()
        .filter_map(|token_id| execution_survival_adjustments.get(token_id))
        .sum()
}

fn ws_ask_edge_hint(
    token_ids: &[String],
    best_ask_prices: &HashMap<String, f64>,
    guaranteed_revenue: f64,
) -> Option<f64> {
    if token_ids.is_empty() || guaranteed_revenue <= f64::EPSILON {
        return None;
    }
    let mut total_cost = 0.0;
    for token_id in token_ids {
        let ask = *best_ask_prices.get(token_id)?;
        if ask <= 0.0 || !ask.is_finite() {
            return None;
        }
        total_cost += ask;
    }
    Some((guaranteed_revenue - total_cost).max(0.0))
}

fn ws_ask_edge_score_bonus(edge_hint: Option<f64>, token_count: usize) -> f64 {
    let Some(edge_hint) = edge_hint else {
        return 0.0;
    };
    if edge_hint <= f64::EPSILON || token_count == 0 {
        return 0.0;
    }
    let density = edge_hint / token_count.max(1) as f64;
    edge_hint * 90_000.0 + density * 140_000.0
}

async fn cached_scan_quote_snapshot(
    price_cache: Option<&PriceCache>,
    config: &Config,
) -> ScanQuoteCacheSnapshot {
    let Some(cache) = price_cache else {
        return ScanQuoteCacheSnapshot::default();
    };
    let max_age = selection_cache_max_age(config);
    let guard = cache.read().await;
    let mut snapshot = ScanQuoteCacheSnapshot::default();
    for (token_id, price) in guard.iter() {
        let has_quote = price.best_ask.unwrap_or(0.0) > 0.0 || price.best_bid.unwrap_or(0.0) > 0.0;
        if has_quote && price.last_updated.elapsed() <= max_age {
            snapshot.fresh_quote_tokens.insert(token_id.clone());
            if let Some(ask) = live_clob_executable_ask(price) {
                snapshot.best_ask_prices.insert(token_id.clone(), ask);
            }
            let penalty = scan_quote_toxicity_penalty(price, config);
            if penalty > f64::EPSILON {
                snapshot
                    .toxicity_penalties
                    .insert(token_id.clone(), penalty);
            }
            let survival_adjustment = scan_quote_execution_survival_adjustment(price, config);
            if survival_adjustment.abs() > f64::EPSILON {
                snapshot
                    .execution_survival_adjustments
                    .insert(token_id.clone(), survival_adjustment);
            }
        }
    }
    snapshot
}

fn count_tradable_markets(event: &crate::models::Event, config: &Config) -> usize {
    event
        .markets
        .iter()
        .filter(|market| {
            !market.closed
                && market.liquidity >= config.min_liquidity_usd
                && fees::market_fee_curve_supported(market)
        })
        .count()
}

fn event_liquidity_sum(event: &crate::models::Event, config: &Config) -> f64 {
    event
        .markets
        .iter()
        .filter(|market| {
            !market.closed
                && market.liquidity >= config.min_liquidity_usd
                && fees::market_fee_curve_supported(market)
        })
        .map(|market| market.liquidity.min(250_000.0))
        .sum::<f64>()
}

fn gamma_edge_hint_for_side(
    event: &crate::models::Event,
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> f64 {
    let tradable: Vec<&crate::models::Market> = event
        .markets
        .iter()
        .filter(|market| {
            !market.closed
                && market.liquidity >= config.min_liquidity_usd
                && fees::market_fee_curve_supported(market)
        })
        .collect();
    if tradable.len() < 2 {
        return 0.0;
    }

    match outcome {
        crate::models::OutcomeSide::Yes => {
            let yes_sum: f64 = tradable.iter().map(|market| market.gamma_yes_price).sum();
            (1.0 - yes_sum).max(0.0)
        }
        crate::models::OutcomeSide::No => {
            let no_sum: f64 = tradable.iter().map(|market| market.gamma_no_price).sum();
            ((tradable.len().saturating_sub(1)) as f64 - no_sum).max(0.0)
        }
    }
}

fn average_gamma_side_price(
    event: &crate::models::Event,
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> f64 {
    let tradable: Vec<&crate::models::Market> = event
        .markets
        .iter()
        .filter(|market| {
            !market.closed
                && market.liquidity >= config.min_liquidity_usd
                && fees::market_fee_curve_supported(market)
        })
        .collect();
    if tradable.is_empty() {
        return 0.0;
    }

    let sum: f64 = match outcome {
        crate::models::OutcomeSide::Yes => {
            tradable.iter().map(|market| market.gamma_yes_price).sum()
        }
        crate::models::OutcomeSide::No => tradable.iter().map(|market| market.gamma_no_price).sum(),
    };
    sum / tradable.len() as f64
}

fn ranked_style_phrase_count(text: &str) -> usize {
    const RANKED_MARKERS: [&str; 23] = [
        "top goalscorer",
        "top goal scorer",
        "top scorer",
        "gold medal",
        "silver medal",
        "bronze medal",
        "pole position",
        "runner-up",
        "runner up",
        "vice champion",
        "top spot",
        "top position",
        "top 4",
        "top four",
        "1st round",
        "2nd round",
        "3rd round",
        "first round",
        "second round",
        "third round",
        "3rd place",
        "third place",
        "4th place",
    ];
    let lower = text.to_ascii_lowercase();
    let marker_hits = RANKED_MARKERS
        .iter()
        .filter(|marker| lower.contains(**marker))
        .count();
    let ordinal_hits = lower.matches(" place").count()
        + lower.matches(" position").count()
        + lower.matches(" rank").count();
    marker_hits + ordinal_hits
}

fn outcome_execution_penalty(
    event: &crate::models::Event,
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> f64 {
    let tradable_markets = count_tradable_markets(event, config);
    let avg_side_price = average_gamma_side_price(event, config, outcome);
    let ranked_hits = ranked_style_phrase_count(&format!("{} {}", event.title, event.slug));
    let lower_title = event.title.to_ascii_lowercase();
    let lower_slug = event.slug.to_ascii_lowercase();
    let looks_like_winner_board = lower_title.contains("winner")
        || lower_title.contains("nominee")
        || lower_title.contains("champion")
        || lower_slug.contains("winner")
        || lower_slug.contains("nominee")
        || lower_slug.contains("champion");

    match outcome {
        crate::models::OutcomeSide::Yes => {
            let ranked_penalty = if ranked_hits > 0 && !event.enable_neg_risk && !event.neg_risk {
                150.0 + ranked_hits as f64 * 60.0
            } else {
                0.0
            };
            let crowd_penalty = if tradable_markets > 20 {
                ((tradable_markets - 20) as f64) * 12.0
            } else {
                0.0
            };
            ranked_penalty + crowd_penalty
        }
        crate::models::OutcomeSide::No => {
            let ranked_penalty = if ranked_hits > 0 {
                450.0 + ranked_hits as f64 * 140.0
            } else {
                0.0
            };
            let winner_penalty = if looks_like_winner_board && tradable_markets >= 8 {
                325.0 + (tradable_markets.saturating_sub(8) as f64) * 20.0
            } else {
                0.0
            };
            let tail_penalty = if avg_side_price >= 0.92 {
                700.0 + (avg_side_price - 0.92) * 4_000.0
            } else if avg_side_price >= 0.85 {
                250.0 + (avg_side_price - 0.85) * 1_500.0
            } else {
                0.0
            };
            let crowd_penalty = if tradable_markets > 16 {
                ((tradable_markets - 16) as f64).powf(1.15) * 32.0
            } else {
                0.0
            };
            ranked_penalty + winner_penalty + tail_penalty + crowd_penalty
        }
    }
}

fn bundle_balance_score(market: &crate::models::Market) -> f64 {
    let centered = 1.0 - ((market.gamma_yes_price - 0.5).abs() * 2.0);
    centered.clamp(0.0, 1.0)
}

fn event_quote_counts(
    event: &crate::models::Event,
    cached_tokens: &HashSet<String>,
    config: &Config,
) -> (usize, usize, usize) {
    let tokens = event_quote_token_ids(event, config);
    let total = tokens.len();
    if total == 0 {
        let markets: Vec<&crate::models::Market> = event
            .markets
            .iter()
            .filter(|market| {
                market_has_external_quotes(market)
                    && !market.closed
                    && market.liquidity >= config.min_liquidity_usd
                    && fees::market_fee_curve_supported(market)
            })
            .collect();
        if !markets.is_empty() {
            let total = markets.len() * 2;
            let ready = markets
                .iter()
                .map(|market| {
                    usize::from(market.has_yes_price_quote())
                        + usize::from(market.has_no_price_quote())
                })
                .sum::<usize>();
            return (total, ready, total.saturating_sub(ready));
        }
    }
    let cached = tokens
        .iter()
        .filter(|token_id| cached_tokens.contains((*token_id).as_str()))
        .count();
    let missing = total.saturating_sub(cached);
    (total, cached, missing)
}

fn quote_counts_for_token_ids(
    token_ids: &[String],
    cached_tokens: &HashSet<String>,
) -> (usize, usize, usize) {
    let total = token_ids.len();
    let cached = token_ids
        .iter()
        .filter(|token_id| cached_tokens.contains(token_id.as_str()))
        .count();
    (total, cached, total.saturating_sub(cached))
}

fn event_quote_counts_for_side(
    event: &crate::models::Event,
    cached_tokens: &HashSet<String>,
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> (usize, usize, usize) {
    let tokens = event_quote_token_ids_for_side(event, config, outcome);
    let total = tokens.len();
    if total == 0 {
        let markets: Vec<&crate::models::Market> = event
            .markets
            .iter()
            .filter(|market| {
                market_has_external_quotes(market)
                    && !market.closed
                    && market.liquidity >= config.min_liquidity_usd
                    && fees::market_fee_curve_supported(market)
            })
            .collect();
        if !markets.is_empty() {
            let total = markets.len();
            let ready = markets
                .iter()
                .filter(|market| market.has_price_quote_for_outcome(outcome))
                .count();
            return (total, ready, total.saturating_sub(ready));
        }
    }
    let cached = tokens
        .iter()
        .filter(|token_id| cached_tokens.contains((*token_id).as_str()))
        .count();
    let missing = total.saturating_sub(cached);
    (total, cached, missing)
}

fn neg_risk_event_priority_score_for_side(
    event: &crate::models::Event,
    cached_tokens: &HashSet<String>,
    config: &Config,
    outcome: crate::models::OutcomeSide,
) -> f64 {
    let tradable_markets = count_tradable_markets(event, config);
    if tradable_markets < 2 || tradable_markets > config.max_opportunity_legs {
        return f64::NEG_INFINITY;
    }

    let (total_tokens, cached_count, missing_tokens) =
        event_quote_counts_for_side(event, cached_tokens, config, outcome);
    if total_tokens == 0 {
        return f64::NEG_INFINITY;
    }

    let liquidity = event_liquidity_sum(event, config);
    let edge_hint = gamma_edge_hint_for_side(event, config, outcome);
    let capital_velocity_adjustment =
        capital_velocity_score_adjustment(&event.lifecycle, edge_hint, config, chrono::Utc::now());
    let edge_density = edge_hint / tradable_markets.max(1) as f64;
    let cache_ratio = cached_count as f64 / total_tokens as f64;
    let execution_penalty = outcome_execution_penalty(event, config, outcome);
    let fee_bonus = if config.fee_theta(&event.category) <= f64::EPSILON {
        750.0
    } else {
        0.0
    };
    let side_bias = match outcome {
        crate::models::OutcomeSide::Yes => 325.0,
        crate::models::OutcomeSide::No => -100.0,
    };
    let missing_penalty = match outcome {
        crate::models::OutcomeSide::Yes => 10.0,
        crate::models::OutcomeSide::No => 22.0,
    };
    let leg_penalty = match outcome {
        crate::models::OutcomeSide::Yes => 95.0,
        crate::models::OutcomeSide::No => 120.0,
    };
    let zero_hint_penalty = if edge_hint <= f64::EPSILON {
        match outcome {
            crate::models::OutcomeSide::Yes => 200.0,
            crate::models::OutcomeSide::No => 600.0,
        }
    } else {
        0.0
    };

    (edge_hint * 18_000.0)
        + (edge_density * 45_000.0)
        + fee_bonus
        + side_bias
        + cache_ratio * 2_200.0
        + (liquidity.ln_1p() * 320.0)
        - (tradable_markets as f64).powf(1.35) * leg_penalty
        - (missing_tokens as f64) * missing_penalty
        - execution_penalty
        - zero_hint_penalty
        + capital_velocity_adjustment
}

fn bundle_market_priority_score(
    parent_event: &crate::models::Event,
    market: &crate::models::Market,
    cached_tokens: &HashSet<String>,
    config: &Config,
) -> f64 {
    if market.closed
        || market.liquidity < config.min_liquidity_usd
        || !fees::market_fee_curve_supported(market)
    {
        return f64::NEG_INFINITY;
    }

    let token_ids = [
        market.clob_token_id_yes.as_str(),
        market.clob_token_id_no.as_str(),
    ]
    .into_iter()
    .filter(|token_id| !token_id.is_empty())
    .collect::<Vec<_>>();
    let total_tokens = token_ids.len();
    let cached_count = token_ids
        .iter()
        .filter(|token_id| cached_tokens.contains(**token_id))
        .count();
    let missing_tokens = total_tokens.saturating_sub(cached_count);
    let cache_ratio = if total_tokens == 0 {
        0.0
    } else {
        cached_count as f64 / total_tokens as f64
    };
    let fee_bonus = if config.fee_theta(&parent_event.category) <= f64::EPSILON {
        250.0
    } else {
        0.0
    };
    let gamma_edge_hint = (1.0 - (market.gamma_yes_price + market.gamma_no_price)).max(0.0);
    let capital_velocity_adjustment = capital_velocity_score_adjustment(
        &parent_event.lifecycle,
        gamma_edge_hint,
        config,
        chrono::Utc::now(),
    );
    let ranked_penalty = ranked_style_phrase_count(&format!(
        "{} {} {}",
        parent_event.title, parent_event.slug, market.question
    )) as f64
        * 120.0;
    let balance_bonus = bundle_balance_score(market) * 550.0;
    let tail_penalty = if market.gamma_yes_price <= 0.03 || market.gamma_yes_price >= 0.97 {
        260.0
    } else if market.gamma_yes_price <= 0.07 || market.gamma_yes_price >= 0.93 {
        125.0
    } else {
        0.0
    };
    let zero_hint_penalty = if gamma_edge_hint <= f64::EPSILON {
        240.0
    } else {
        0.0
    };

    fee_bonus
        + gamma_edge_hint * 12_000.0
        + cache_ratio * 600.0
        + balance_bonus
        + market.liquidity.ln_1p() * 115.0
        - (missing_tokens as f64) * 45.0
        - ranked_penalty
        - tail_penalty
        - zero_hint_penalty
        + capital_velocity_adjustment
}

fn ranked_candidate_order(
    candidates: &[CandidateSelection],
    active_token_budget: usize,
    only_indices: Option<&HashSet<usize>>,
    exclude_indices: Option<&HashSet<usize>>,
) -> Vec<usize> {
    let use_density_ranking = active_token_budget > 0;
    let mut ranked: Vec<(usize, f64, f64)> = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| {
            only_indices
                .map(|indices| indices.contains(&candidate.idx))
                .unwrap_or(true)
                && exclude_indices
                    .map(|indices| !indices.contains(&candidate.idx))
                    .unwrap_or(true)
        })
        .map(|(order_idx, candidate)| {
            let density_score = candidate.score / (candidate.total_tokens.max(1) as f64);
            let priority = if use_density_ranking {
                density_score
            } else {
                candidate.score
            };
            (order_idx, priority, candidate.score)
        })
        .collect();
    ranked.sort_by(
        |(left_idx, left_priority, left_score), (right_idx, right_priority, right_score)| {
            right_priority
                .partial_cmp(left_priority)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    right_score
                        .partial_cmp(left_score)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| candidates[*left_idx].idx.cmp(&candidates[*right_idx].idx))
        },
    );
    ranked.into_iter().map(|(idx, _, _)| idx).collect()
}

fn rotated_candidate_traversal(
    ordered: &[usize],
    item_budget: usize,
    scan_index: u64,
    rotation_period_scans: u64,
    sticky_fraction: f64,
) -> Vec<usize> {
    let sticky_fraction = sticky_fraction.clamp(0.0, 0.95);
    let sticky_count = if sticky_fraction <= f64::EPSILON {
        0usize
    } else {
        (((item_budget as f64) * sticky_fraction).ceil() as usize)
            .max(1)
            .min(ordered.len())
    };
    let mut traversal = ordered[..sticky_count].to_vec();
    let rotating = &ordered[sticky_count..];
    if !rotating.is_empty() {
        let effective_scan_index = scan_index / rotation_period_scans.max(1);
        let offset = effective_scan_index as usize % rotating.len();
        for step in 0..rotating.len() {
            traversal.push(rotating[(offset + step) % rotating.len()]);
        }
    }
    traversal
}

fn select_candidates_from_traversal(
    candidates: &[CandidateSelection],
    traversal: &[usize],
    item_budget: usize,
    quote_budget: usize,
    active_token_budget: usize,
) -> Vec<usize> {
    let mut selected = Vec::new();
    let mut selected_set = HashSet::new();
    let mut remaining_quote_budget = if quote_budget == 0 {
        usize::MAX
    } else {
        quote_budget
    };
    let mut used_token_budget = 0usize;
    let enforce_token_budget = active_token_budget > 0;

    for order_idx in traversal {
        if selected.len() >= item_budget {
            break;
        }
        let candidate = &candidates[*order_idx];
        let fits_quote_budget = quote_budget == 0
            || candidate.missing_tokens == 0
            || candidate.missing_tokens <= remaining_quote_budget;
        let fits_token_budget = !enforce_token_budget
            || used_token_budget.saturating_add(candidate.total_tokens) <= active_token_budget;
        if fits_quote_budget && fits_token_budget {
            selected.push(candidate.idx);
            selected_set.insert(candidate.idx);
            remaining_quote_budget =
                remaining_quote_budget.saturating_sub(candidate.missing_tokens);
            used_token_budget = used_token_budget.saturating_add(candidate.total_tokens);
        }
    }

    if selected.len() < item_budget && enforce_token_budget {
        for order_idx in traversal {
            if selected.len() >= item_budget {
                break;
            }
            let candidate = &candidates[*order_idx];
            if selected_set.contains(&candidate.idx) || candidate.missing_tokens != 0 {
                continue;
            }
            if used_token_budget.saturating_add(candidate.total_tokens) > active_token_budget {
                continue;
            }
            selected.push(candidate.idx);
            selected_set.insert(candidate.idx);
            used_token_budget = used_token_budget.saturating_add(candidate.total_tokens);
        }
    }

    selected
}

fn select_candidate_indices(
    candidates: &[CandidateSelection],
    item_budget: usize,
    quote_budget: usize,
    active_token_budget: usize,
    scan_index: u64,
    rotation_period_scans: u64,
    sticky_fraction: f64,
    dirty_candidate_indices: &HashSet<usize>,
) -> Vec<usize> {
    if candidates.is_empty() || item_budget == 0 {
        return Vec::new();
    }

    if !dirty_candidate_indices.is_empty() {
        let dirty_ordered = ranked_candidate_order(
            candidates,
            active_token_budget,
            Some(dirty_candidate_indices),
            None,
        );
        if dirty_ordered.len() >= item_budget {
            let traversal = rotated_candidate_traversal(
                &dirty_ordered,
                item_budget,
                scan_index,
                rotation_period_scans,
                sticky_fraction,
            );
            let selected = select_candidates_from_traversal(
                candidates,
                &traversal,
                item_budget,
                quote_budget,
                active_token_budget,
            );
            if selected.len() >= item_budget {
                return selected;
            }
        }
    }

    let ordered = if dirty_candidate_indices.is_empty() {
        ranked_candidate_order(candidates, active_token_budget, None, None)
    } else {
        let mut dirty_ordered = ranked_candidate_order(
            candidates,
            active_token_budget,
            Some(dirty_candidate_indices),
            None,
        );
        let mut clean_ordered = ranked_candidate_order(
            candidates,
            active_token_budget,
            None,
            Some(dirty_candidate_indices),
        );
        dirty_ordered.append(&mut clean_ordered);
        dirty_ordered
    };
    let traversal = rotated_candidate_traversal(
        &ordered,
        item_budget,
        scan_index,
        rotation_period_scans,
        sticky_fraction,
    );

    let mut selected = select_candidates_from_traversal(
        candidates,
        &traversal,
        item_budget,
        quote_budget,
        active_token_budget,
    );
    let enforce_token_budget = active_token_budget > 0;

    if selected.is_empty() {
        let fitting_candidates = candidates
            .iter()
            .filter(|candidate| candidate.total_tokens > 0)
            .filter(|candidate| {
                let quote_ok = quote_budget == 0 || candidate.missing_tokens <= quote_budget;
                let token_ok =
                    !enforce_token_budget || candidate.total_tokens <= active_token_budget;
                quote_ok && token_ok
            });
        if let Some(best_fit) = fitting_candidates.min_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.missing_tokens.cmp(&right.missing_tokens))
                .then_with(|| left.total_tokens.cmp(&right.total_tokens))
        }) {
            selected.push(best_fit.idx);
        } else if let Some(smallest_candidate) = candidates
            .iter()
            .filter(|candidate| candidate.total_tokens > 0)
            .min_by(|left, right| {
                left.total_tokens
                    .cmp(&right.total_tokens)
                    .then_with(|| left.missing_tokens.cmp(&right.missing_tokens))
                    .then_with(|| {
                        right
                            .score
                            .partial_cmp(&left.score)
                            .unwrap_or(Ordering::Equal)
                    })
            })
        {
            selected.push(smallest_candidate.idx);
        }
    }

    selected
}

fn event_titles_preview(events: &[crate::models::Event], limit: usize) -> String {
    if events.is_empty() {
        return String::from("-");
    }
    events
        .iter()
        .take(limit.max(1))
        .map(|event| {
            let title = event.title.trim();
            if title.chars().count() > 60 {
                format!("{}...", title.chars().take(57).collect::<String>())
            } else {
                title.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn short_text(text: &str, limit: usize) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return String::from("-");
    }
    if trimmed.chars().count() > limit.max(8) {
        format!(
            "{}...",
            trimmed
                .chars()
                .take(limit.max(8).saturating_sub(3))
                .collect::<String>()
        )
    } else {
        trimmed.to_string()
    }
}

fn describe_quote_token_samples(
    events: &[crate::models::Event],
    token_ids: &[String],
    limit: usize,
) -> String {
    if token_ids.is_empty() {
        return String::from("-");
    }

    let mut labels: HashMap<&str, String> = HashMap::new();
    for event in events {
        for market in &event.markets {
            let market_label = format!(
                "{} :: {}",
                short_text(&event.title, 42),
                short_text(&market.question, 48),
            );
            if !market.clob_token_id_yes.is_empty() {
                labels
                    .entry(market.clob_token_id_yes.as_str())
                    .or_insert_with(|| format!("{} :: YES", market_label));
            }
            if !market.clob_token_id_no.is_empty() {
                labels
                    .entry(market.clob_token_id_no.as_str())
                    .or_insert_with(|| format!("{} :: NO", market_label));
            }
        }
    }

    token_ids
        .iter()
        .take(limit.max(1))
        .map(|token_id| {
            if let Some(label) = labels.get(token_id.as_str()) {
                format!("{} => {}", token_id, label)
            } else {
                token_id.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn bundle_markets_preview(events: &[crate::models::Event], limit: usize) -> String {
    if events.is_empty() {
        return String::from("-");
    }
    events
        .iter()
        .take(limit.max(1))
        .map(|event| {
            let market = event.markets.first();
            let market_question = market
                .map(|market| short_text(&market.question, 44))
                .unwrap_or_else(|| String::from("-"));
            format!("{} :: {}", short_text(&event.title, 40), market_question)
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn selected_candidate_totals(
    candidates: &[CandidateSelection],
    selected_indices: &[usize],
) -> (usize, usize, usize) {
    selected_indices
        .iter()
        .fold((0usize, 0usize, 0usize), |mut acc, idx| {
            if let Some(candidate) = candidates.iter().find(|candidate| candidate.idx == *idx) {
                acc.0 += candidate.total_tokens;
                acc.1 += candidate.cached_tokens;
                acc.2 += candidate.missing_tokens;
            }
            acc
        })
}

fn log_neg_risk_pool_evaluations(
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    pool_name: &str,
    outcome: crate::models::OutcomeSide,
    events: &[crate::models::Event],
    candidates: &[CandidateSelection],
    selected_indices: &[usize],
    dirty_candidate_indices: &HashSet<usize>,
    config: &Config,
    quote_budget: usize,
    active_token_budget: usize,
) {
    let Some(diagnostics) = diagnostics else {
        return;
    };
    let selected_ranks = selected_rank_map(selected_indices);
    let timestamp = timestamp_now();
    for candidate in candidates {
        let Some(event) = events.get(candidate.idx) else {
            continue;
        };
        diagnostics.record_candidate_evaluation(CandidateEvaluationRow {
            timestamp: timestamp.clone(),
            scan_id: scan_index,
            pool: pool_name.to_string(),
            selected: selected_ranks.contains_key(&candidate.idx),
            selected_rank: selected_ranks.get(&candidate.idx).copied(),
            event_id: event.event_id.clone(),
            event_title: event.title.clone(),
            event_slug: event.slug.clone(),
            market_question: String::new(),
            outcome_side: outcome.to_string(),
            selection_state: candidate_selection_state(
                &selected_ranks,
                dirty_candidate_indices,
                candidate.idx,
            )
            .to_string(),
            candidate_score: candidate.score,
            theory_hint: gamma_edge_hint_for_side(event, config, outcome),
            tradable_legs: count_tradable_markets(event, config),
            total_tokens: candidate.total_tokens,
            cached_tokens: candidate.cached_tokens,
            missing_tokens: candidate.missing_tokens,
            quote_budget,
            active_token_budget,
        });
    }
}

fn log_bundle_pool_evaluations(
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    bundle_events: &[crate::models::Event],
    candidates: &[CandidateSelection],
    selected_indices: &[usize],
    dirty_candidate_indices: &HashSet<usize>,
    quote_budget: usize,
    active_token_budget: usize,
) {
    let Some(diagnostics) = diagnostics else {
        return;
    };
    let selected_ranks = selected_rank_map(selected_indices);
    let timestamp = timestamp_now();
    for candidate in candidates {
        let Some(event) = bundle_events.get(candidate.idx) else {
            continue;
        };
        let market_question = event
            .markets
            .first()
            .map(|market| market.question.clone())
            .unwrap_or_default();
        let theory_hint = event
            .markets
            .first()
            .map(|market| (1.0 - (market.gamma_yes_price + market.gamma_no_price)).max(0.0))
            .unwrap_or(0.0);
        diagnostics.record_candidate_evaluation(CandidateEvaluationRow {
            timestamp: timestamp.clone(),
            scan_id: scan_index,
            pool: "bundle".to_string(),
            selected: selected_ranks.contains_key(&candidate.idx),
            selected_rank: selected_ranks.get(&candidate.idx).copied(),
            event_id: event.event_id.clone(),
            event_title: event.title.clone(),
            event_slug: event.slug.clone(),
            market_question,
            outcome_side: "both".to_string(),
            selection_state: candidate_selection_state(
                &selected_ranks,
                dirty_candidate_indices,
                candidate.idx,
            )
            .to_string(),
            candidate_score: candidate.score,
            theory_hint,
            tradable_legs: event.markets.len(),
            total_tokens: candidate.total_tokens,
            cached_tokens: candidate.cached_tokens,
            missing_tokens: candidate.missing_tokens,
            quote_budget,
            active_token_budget,
        });
    }
}

fn log_candidate_rejection(
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    pool: &str,
    event: &crate::models::Event,
    arb_type: &str,
    outcome_side: Option<crate::models::OutcomeSide>,
    stage: &str,
    reason: &str,
    theory_hint: f64,
    quote_ready: bool,
    opp: Option<&ArbitrageOpportunity>,
    note: impl Into<String>,
) {
    let Some(diagnostics) = diagnostics else {
        return;
    };
    let market_question = event
        .markets
        .first()
        .map(|market| market.question.clone())
        .unwrap_or_default();
    diagnostics.record_candidate_rejection(CandidateRejectionRow {
        timestamp: timestamp_now(),
        scan_id: scan_index,
        pool: pool.to_string(),
        event_id: event.event_id.clone(),
        event_title: event.title.clone(),
        event_slug: event.slug.clone(),
        market_question,
        arb_type: arb_type.to_string(),
        outcome_side: outcome_side
            .map(|side| side.to_string())
            .unwrap_or_default(),
        stage: stage.to_string(),
        reason: reason.to_string(),
        theory_hint,
        quote_ready,
        total_cost: opp.map(|opp| opp.total_cost),
        gross_profit: opp.map(|opp| opp.gross_profit),
        total_fees: opp.map(|opp| opp.total_fees),
        projected_net_profit: opp.map(|opp| opp.net_profit),
        note: note.into(),
    });
}

fn log_trade_event(
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    mode: &str,
    status: &str,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
    note: impl Into<String>,
) {
    let Some(diagnostics) = diagnostics else {
        return;
    };
    let pnl_scale = if mode.eq_ignore_ascii_case("raw") {
        "basket_unit"
    } else {
        "target_position"
    };
    if let Err(err) = diagnostics.record_trade(TradeLogRow {
        timestamp: timestamp_now(),
        scan_id: scan_index,
        mode: mode.to_string(),
        status: status.to_string(),
        pnl_scale: pnl_scale.to_string(),
        event_id: opp.event_id.clone(),
        event_title: opp.event_title.clone(),
        arb_type: opp.arb_type.to_string(),
        legs_summary: opportunity_legs_summary(opp),
        target_position_usd,
        projected_net_profit: opp.net_profit,
        projected_roi_pct: opp.roi_pct,
        filled_cost_usd: None,
        conservative_pnl_usd: None,
        conservative_roi_pct: None,
        planned_basket_units: None,
        hedged_basket_units: None,
        fill_count: None,
        partial_fill: None,
        parity_ok: None,
        unhedged_notional_usd: None,
        prices_from_clob: opp.prices_from_clob,
        note: note.into(),
    }) {
        warn!("Trade diagnostics write failed: {err:#}");
    }
}

fn log_paper_trade_event(
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
    report: &PaperExecutionReport,
    note: impl Into<String>,
) -> anyhow::Result<()> {
    let Some(diagnostics) = diagnostics else {
        anyhow::bail!("paper terminal trade cannot be recorded: diagnostics unavailable");
    };
    let attempt_status = if report.parity_ok {
        "accepted"
    } else {
        "rejected"
    };
    let note = format!(
        "{}; paper_attempt_id={}; paper_attempt_status={attempt_status}",
        note.into(),
        report.attempt_id,
    );
    diagnostics.record_trade(TradeLogRow {
        timestamp: timestamp_now(),
        scan_id: scan_index,
        mode: "paper".into(),
        status: if report.parity_ok {
            "ok".into()
        } else {
            "parity_rejected".into()
        },
        pnl_scale: "filled_hedged".into(),
        event_id: opp.event_id.clone(),
        event_title: opp.event_title.clone(),
        arb_type: opp.arb_type.to_string(),
        legs_summary: opportunity_legs_summary(opp),
        target_position_usd,
        projected_net_profit: opp.net_profit,
        projected_roi_pct: opp.roi_pct,
        filled_cost_usd: Some(report.hedged_cost_usd),
        conservative_pnl_usd: Some(report.conservative_pnl_usd),
        conservative_roi_pct: Some(report.conservative_roi_pct),
        planned_basket_units: Some(report.planned_basket_units),
        hedged_basket_units: Some(report.hedged_basket_units),
        fill_count: Some(report.fill_count),
        partial_fill: Some(report.any_partial),
        parity_ok: Some(report.parity_ok),
        unhedged_notional_usd: Some(report.unhedged_notional_usd),
        prices_from_clob: opp.prices_from_clob,
        note,
    })
}

fn log_live_trade_event(
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
    report: &live_executor::LiveExecutionReport,
    note: impl Into<String>,
) -> anyhow::Result<()> {
    let Some(diagnostics) = diagnostics else {
        anyhow::bail!("live terminal trade cannot be recorded: diagnostics unavailable");
    };
    let mut note = note.into();
    if !report.order_ids.is_empty() {
        note = format!("{note}; order_ids={:?}", report.order_ids);
    }
    if !report.trade_ids.is_empty() {
        note = format!("{note}; trade_ids={:?}", report.trade_ids);
    }
    if !report.transaction_hashes.is_empty() {
        note = format!("{note}; tx_hashes={:?}", report.transaction_hashes);
    }
    diagnostics.record_trade(TradeLogRow {
        timestamp: timestamp_now(),
        scan_id: scan_index,
        mode: "live".into(),
        status: "settlement_confirmed_unrealized".into(),
        pnl_scale: "projected_unsettled".into(),
        event_id: opp.event_id.clone(),
        event_title: opp.event_title.clone(),
        arb_type: opp.arb_type.to_string(),
        legs_summary: opportunity_legs_summary(opp),
        target_position_usd,
        projected_net_profit: report.projected_pnl_usd,
        projected_roi_pct: report.projected_roi_pct,
        filled_cost_usd: Some(report.position_usd),
        conservative_pnl_usd: None,
        conservative_roi_pct: None,
        planned_basket_units: Some(report.basket_units),
        hedged_basket_units: None,
        fill_count: Some(report.order_count),
        partial_fill: Some(false),
        parity_ok: None,
        unhedged_notional_usd: None,
        prices_from_clob: opp.prices_from_clob,
        note,
    })
}

fn record_live_execution_session(
    session_trades_executed: &mut usize,
    session_pnl_usd: &mut f64,
    session_position_usd: &mut f64,
    report: &live_executor::LiveExecutionReport,
) {
    *session_trades_executed += 1;
    *session_position_usd += report.position_usd;
    // Live fill PnL is unsettled until closeout/redeem reconciliation exists.
    let _ = session_pnl_usd;
}

async fn count_ws_snapshot_ready(price_cache: &PriceCache, desired: &HashSet<String>) -> usize {
    let cache = price_cache.read().await;
    desired
        .iter()
        .filter(|token_id| {
            cache
                .get(token_id.as_str())
                .map(|price| price.snapshot_ready)
                .unwrap_or(false)
        })
        .count()
}

fn record_ws_wake(drain: &mut DrainedWsWakes, wake: WsWake) {
    match wake {
        WsWake::Token(token_id) => {
            drain.dirty_tokens.insert(token_id);
        }
        WsWake::Discovery => {
            drain.discovery_wake = true;
        }
    }
}

fn merge_ws_wakes(target: &mut DrainedWsWakes, source: DrainedWsWakes) {
    target.dirty_tokens.extend(source.dirty_tokens);
    target.discovery_wake |= source.discovery_wake;
}

async fn drain_ws_wakes(dirty_rx: Option<&mut DirtyTokenReceiver>) -> DrainedWsWakes {
    let Some(rx) = dirty_rx else {
        return DrainedWsWakes::default();
    };
    let mut drain = DrainedWsWakes::default();
    if let Ok(wake) = rx.try_recv() {
        record_ws_wake(&mut drain, wake);
        return drain_ws_wakes_until_quiet(
            rx,
            drain,
            Duration::from_millis(WS_DIRTY_WAKE_DEBOUNCE_MS),
        )
        .await;
    }
    drain
}

async fn drain_ws_wakes_until_quiet(
    rx: &mut DirtyTokenReceiver,
    mut drain: DrainedWsWakes,
    quiet_for: Duration,
) -> DrainedWsWakes {
    while let Ok(Some(wake)) = tokio::time::timeout(quiet_for, rx.recv()).await {
        record_ws_wake(&mut drain, wake);
    }
    while let Ok(wake) = rx.try_recv() {
        record_ws_wake(&mut drain, wake);
    }
    drain
}

async fn sleep_or_take_ws_wake(
    dirty_rx: Option<&mut DirtyTokenReceiver>,
    sleep_for: Duration,
) -> DrainedWsWakes {
    let Some(rx) = dirty_rx else {
        tokio::time::sleep(sleep_for).await;
        return DrainedWsWakes::default();
    };
    let mut drain = DrainedWsWakes::default();
    let mut woke = false;
    tokio::select! {
        wake = rx.recv() => {
            if let Some(wake) = wake {
                record_ws_wake(&mut drain, wake);
                woke = true;
            }
        }
        _ = tokio::time::sleep(sleep_for) => {}
    }
    if woke {
        drain_ws_wakes_until_quiet(rx, drain, Duration::from_millis(WS_DIRTY_WAKE_DEBOUNCE_MS))
            .await
    } else {
        drain
    }
}

async fn wait_for_ws_snapshot_coverage(
    price_cache: Option<&PriceCache>,
    desired: &HashSet<String>,
    min_coverage_pct: f64,
    timeout_ms: u64,
) -> WsSnapshotCoverage {
    let total = desired.len();
    let min_ready = ((total as f64) * min_coverage_pct.clamp(0.0, 1.0)).ceil() as usize;
    let min_ready = min_ready.min(total);
    let Some(price_cache) = price_cache else {
        return WsSnapshotCoverage {
            ready: 0,
            total,
            min_ready,
            satisfied: true,
        };
    };
    if total == 0 || min_ready == 0 {
        return WsSnapshotCoverage {
            ready: 0,
            total,
            min_ready,
            satisfied: true,
        };
    }

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let ready = count_ws_snapshot_ready(price_cache, desired).await;
        if ready >= min_ready || timeout_ms == 0 || Instant::now() >= deadline {
            return WsSnapshotCoverage {
                ready,
                total,
                min_ready,
                satisfied: ready >= min_ready,
            };
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn neg_risk_side_quote_ready(
    event: &crate::models::Event,
    yes_side: bool,
    config: &Config,
) -> bool {
    let tradable: Vec<&crate::models::Market> = event
        .markets
        .iter()
        .filter(|market| {
            !market.closed
                && market.liquidity >= config.min_liquidity_usd
                && fees::market_fee_curve_supported(market)
        })
        .collect();
    if tradable.len() < 2 || tradable.len() > config.max_opportunity_legs {
        return false;
    }

    let quoted = tradable
        .iter()
        .filter(|market| {
            if yes_side {
                if config.execute_only_full_clob_prices {
                    market.has_full_yes_quote()
                } else {
                    market.has_yes_price_quote()
                }
            } else if config.execute_only_full_clob_prices {
                market.has_full_no_quote()
            } else {
                market.has_no_price_quote()
            }
        })
        .count();
    (quoted as f64 / tradable.len() as f64) >= config.min_clob_quote_coverage_pct
}

fn bundle_market_quote_ready(event: &crate::models::Event) -> bool {
    event
        .markets
        .first()
        .map(|market| market.has_yes_price_quote() && market.has_no_price_quote())
        .unwrap_or(false)
}

fn merge_quote_enrichment_stats(
    stats: &mut ScanStats,
    quote_stats: &crate::clob_client::QuoteEnrichmentStats,
) {
    stats.quote_tokens_total += quote_stats.total_tokens;
    stats.quote_cache_hits += quote_stats.cache_hits;
    stats.quote_rest_requested += quote_stats.rest_requested;
    stats.quote_rest_resolved += quote_stats.rest_resolved;
    stats.quote_rest_batches += quote_stats.rest_batches;
    stats.quote_deferred_tokens += quote_stats.deferred_tokens;
    stats.quote_hard_unresolved_tokens += quote_stats.hard_unresolved_tokens;
    stats.quote_no_ask_tokens += quote_stats.no_ask_tokens;
    stats.quote_missing_book_tokens += quote_stats.missing_book_tokens;
}

fn quote_rest_resolution_rate_pct(stats: &ScanStats) -> f64 {
    if stats.quote_rest_requested == 0 {
        100.0
    } else {
        (stats.quote_rest_resolved as f64 / stats.quote_rest_requested as f64) * 100.0
    }
}

fn latency_budget_blockers(stats: &ScanStats, config: &Config) -> Vec<String> {
    let mut blockers = Vec::new();
    let max_signal_age_ms = config.max_signal_age_secs as f64 * 1000.0;
    if max_signal_age_ms > 0.0 && stats.scan_duration_ms > max_signal_age_ms {
        blockers.push(format!(
            "scan_duration_exceeds_signal_age_budget:{:.0}>{:.0}ms",
            stats.scan_duration_ms, max_signal_age_ms
        ));
    }
    if stats.ws_snapshot_total_tokens > 0
        && config.ws_initial_snapshot_timeout_ms > 0
        && !stats.ws_snapshot_satisfied
    {
        blockers.push(format!(
            "ws_snapshot_coverage_timeout:{}/{}<{}",
            stats.ws_snapshot_ready_tokens,
            stats.ws_snapshot_total_tokens,
            stats.ws_snapshot_min_ready_tokens
        ));
    }
    if stats.ws_snapshot_total_tokens > 0
        && config.ws_initial_snapshot_timeout_ms > 0
        && stats.ws_snapshot_wait_ms >= config.ws_initial_snapshot_timeout_ms as f64 * 0.90
    {
        blockers.push(format!(
            "ws_snapshot_wait_near_timeout:{:.0}>={:.0}ms",
            stats.ws_snapshot_wait_ms,
            config.ws_initial_snapshot_timeout_ms as f64 * 0.90
        ));
    }
    if stats.quote_missing_book_tokens > 0 && stats.quote_rest_requested > stats.quote_rest_resolved
    {
        blockers.push(format!(
            "quote_rest_resolved:{}/{}",
            stats.quote_rest_resolved, stats.quote_rest_requested
        ));
    }
    if stats.quote_deferred_tokens > 0 {
        blockers.push(format!(
            "quote_refresh_budget_deferred:{}",
            stats.quote_deferred_tokens
        ));
    }
    if stats.quote_missing_book_tokens > 0 {
        blockers.push(format!(
            "quote_missing_book:{}",
            stats.quote_missing_book_tokens
        ));
    }
    if stats.target_size_rejections > 0 {
        blockers.push(format!(
            "depth_reprice_rejections:{}",
            stats.target_size_rejections
        ));
    }
    blockers
}

fn latency_budget_status(blockers: &[String]) -> String {
    if blockers.is_empty() {
        return "ok".to_string();
    }
    if blockers.iter().any(|blocker| {
        blocker.starts_with("scan_duration_exceeds_signal_age_budget")
            || blocker.starts_with("ws_snapshot_coverage_timeout")
            || blocker.starts_with("quote_missing_book")
    }) {
        "blocked".to_string()
    } else {
        "degraded".to_string()
    }
}

fn live_latency_budget_blocker(stats: &ScanStats, config: &Config) -> Option<String> {
    let blockers = latency_budget_blockers(stats, config);
    (!blockers.is_empty()).then(|| format!("latency_budget_blocked:{}", blockers.join("|")))
}

fn live_latency_budget_blocker_at_scan_elapsed(
    stats: &mut ScanStats,
    scan_start: Instant,
    config: &Config,
) -> Option<String> {
    stats.scan_duration_ms = scan_start.elapsed().as_millis() as f64;
    live_latency_budget_blocker(stats, config)
}

fn record_detected_opportunity(stats: &mut ScanStats, opp: &ArbitrageOpportunity) {
    stats.opportunities_found += 1;
    match opp.arb_type {
        crate::models::ArbType::Yes => stats.yes_opportunities += 1,
        crate::models::ArbType::No => stats.no_opportunities += 1,
        crate::models::ArbType::Bundle => stats.bundle_opportunities += 1,
        crate::models::ArbType::MintSell => stats.bundle_opportunities += 1,
        crate::models::ArbType::Ranked => stats.ranked_opportunities += 1,
    }
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

fn minimum_order_basket_units_for_price(
    market_min_order_shares: f64,
    unit_shares: f64,
    price: f64,
    config: &Config,
) -> f64 {
    if unit_shares <= f64::EPSILON {
        return 0.0;
    }

    let share_floor = market_min_order_shares / unit_shares;
    let notional_floor = if price > f64::EPSILON && config.external_paper_min_order_usd > 0.0 {
        config.external_paper_min_order_usd / (price * unit_shares)
    } else {
        0.0
    };
    share_floor.max(notional_floor)
}

fn minimum_order_basket_units_for_opp(opp: &ArbitrageOpportunity, config: &Config) -> f64 {
    if !opp.execution_plan.is_empty() {
        opp.execution_plan
            .iter()
            .filter_map(|leg| {
                opp.markets.get(leg.market_index).map(|market| {
                    let reference_price = leg.reference_price.max(match leg.outcome {
                        crate::models::OutcomeSide::Yes => market.clob_yes_ask.unwrap_or(0.0),
                        crate::models::OutcomeSide::No => market.clob_no_ask.unwrap_or(0.0),
                    });
                    minimum_order_basket_units_for_price(
                        market.min_order_size_shares(),
                        leg.unit_shares,
                        reference_price,
                        config,
                    )
                })
            })
            .fold(0.0, f64::max)
    } else {
        match opp.arb_type {
            crate::models::ArbType::Bundle => opp
                .markets
                .first()
                .map(|m| {
                    minimum_order_basket_units_for_price(
                        m.min_order_size_shares(),
                        1.0,
                        m.clob_yes_ask
                            .or(m.clob_no_ask)
                            .unwrap_or(opp.total_cost.max(0.0)),
                        config,
                    )
                })
                .unwrap_or(0.0),
            _ => opp
                .markets
                .iter()
                .map(|m| {
                    minimum_order_basket_units_for_price(
                        m.min_order_size_shares(),
                        1.0,
                        m.clob_yes_ask
                            .or(m.clob_no_ask)
                            .unwrap_or(opp.total_cost.max(0.0)),
                        config,
                    )
                })
                .fold(0.0, f64::max),
        }
    }
}

fn minimum_order_basket_units_for_prices(
    opp: &ArbitrageOpportunity,
    prices: &[f64],
    config: &Config,
) -> f64 {
    opp.execution_plan
        .iter()
        .zip(prices.iter().copied())
        .filter_map(|(leg, price)| {
            opp.markets.get(leg.market_index).map(|market| {
                minimum_order_basket_units_for_price(
                    market.min_order_size_shares(),
                    leg.unit_shares,
                    price,
                    config,
                )
            })
        })
        .fold(0.0, f64::max)
}

fn target_position_for_depth_reprice(
    opp: &ArbitrageOpportunity,
    config: &Config,
    target_position_usd: f64,
) -> f64 {
    let target_position_usd = target_position_usd.max(0.0);
    if target_position_usd <= f64::EPSILON || opp.total_cost <= f64::EPSILON {
        return 0.0;
    }

    let minimum_position_usd = minimum_order_basket_units_for_opp(opp, config) * opp.total_cost;
    let requested = target_position_usd.max(minimum_position_usd);

    if opp.max_executable_size_usd.is_finite()
        && opp.max_executable_size_usd > 0.0
        && opp.max_executable_size_usd + f64::EPSILON >= minimum_position_usd
    {
        requested.min(opp.max_executable_size_usd).max(0.0)
    } else {
        requested
    }
}

#[cfg(test)]
async fn fetch_depth_adjusted_prices(
    client: &Client,
    config: &Config,
    price_cache: Option<&PriceCache>,
    opp: &ArbitrageOpportunity,
    basket_units: f64,
) -> Option<Vec<f64>> {
    let (token_ids, depth_snapshots, _) =
        fetch_depth_snapshots_for_reprice(client, config, price_cache, opp).await?;
    depth_adjusted_prices_from_snapshots(opp, &token_ids, &depth_snapshots, basket_units)
}

#[cfg(test)]
async fn fetch_depth_snapshots_for_reprice(
    client: &Client,
    config: &Config,
    price_cache: Option<&PriceCache>,
    opp: &ArbitrageOpportunity,
) -> Option<(
    Vec<String>,
    HashMap<String, crate::clob_client::DepthSnapshot>,
    bool,
)> {
    fetch_depth_snapshots_for_reprice_with_reason(client, config, price_cache, opp)
        .await
        .ok()
}

async fn fetch_depth_snapshots_for_reprice_with_reason(
    client: &Client,
    config: &Config,
    price_cache: Option<&PriceCache>,
    opp: &ArbitrageOpportunity,
) -> Result<
    (
        Vec<String>,
        HashMap<String, crate::clob_client::DepthSnapshot>,
        bool,
    ),
    String,
> {
    if opp.execution_plan.is_empty() {
        return Err("empty_execution_plan".to_string());
    }

    let token_ids =
        depth_reprice_token_ids(opp).ok_or_else(|| "missing_reprice_token_ids".to_string())?;
    let mut depth_snapshots = None;
    if let Some(cache) = price_cache {
        if let Some(cached) =
            crate::clob_client::get_cached_depth_snapshots(cache, config, &token_ids).await
        {
            if scan_depth_snapshots_coherent(config, &token_ids, &cached) {
                depth_snapshots = Some((cached, true));
            }
        }
    }
    let (depth_snapshots, from_cache) = match depth_snapshots {
        Some((snapshots, from_cache)) => (snapshots, from_cache),
        None => {
            match crate::clob_client::get_live_depth_snapshots(client, config, &token_ids).await {
                Ok(snapshots) => (snapshots, false),
                Err(err) => {
                    debug!(
                        "Batch depth-aware reprice rejected event {} ({}): {}",
                        opp.event_id, opp.arb_type, err
                    );
                    return Err(format!("depth_snapshot_fetch_failed:{err:#}"));
                }
            }
        }
    };
    if !scan_depth_snapshots_coherent(config, &token_ids, &depth_snapshots) {
        debug!(
            "Batch depth-aware reprice rejected event {} ({}) because route /books snapshots were stale or skewed",
            opp.event_id,
            opp.arb_type,
        );
        return Err(depth_snapshot_coherence_rejection_note(
            config,
            &token_ids,
            &depth_snapshots,
        ));
    }
    Ok((token_ids, depth_snapshots, from_cache))
}

#[cfg(test)]
fn depth_adjusted_prices_from_snapshots(
    opp: &ArbitrageOpportunity,
    token_ids: &[String],
    depth_snapshots: &HashMap<String, crate::clob_client::DepthSnapshot>,
    basket_units: f64,
) -> Option<Vec<f64>> {
    depth_adjusted_prices_from_snapshots_with_reason(opp, token_ids, depth_snapshots, basket_units)
        .ok()
}

fn depth_adjusted_prices_from_snapshots_with_reason(
    opp: &ArbitrageOpportunity,
    token_ids: &[String],
    depth_snapshots: &HashMap<String, crate::clob_client::DepthSnapshot>,
    basket_units: f64,
) -> Result<Vec<f64>, String> {
    if basket_units <= f64::EPSILON {
        return Err(format!("invalid_basket_units:{basket_units:.6}"));
    }
    if opp.execution_plan.is_empty() {
        return Err("empty_execution_plan".to_string());
    }
    if token_ids.len() != opp.execution_plan.len() {
        return Err(format!(
            "token_plan_mismatch:tokens={}:legs={}",
            token_ids.len(),
            opp.execution_plan.len()
        ));
    }

    let mut prices = Vec::with_capacity(opp.execution_plan.len());
    for (leg, token_id) in opp.execution_plan.iter().zip(token_ids.iter()) {
        let snapshot = depth_snapshots
            .get(token_id)
            .ok_or_else(|| format!("missing_depth_snapshot:{token_id}"))?;
        let requested_shares = basket_units * leg.unit_shares;
        let price = snapshot
            .average_ask_for_shares(requested_shares)
            .ok_or_else(|| {
                let available_ask_shares = snapshot.asks.iter().map(|(_, size)| *size).sum::<f64>();
                let best_ask = snapshot
                    .asks
                    .first()
                    .map(|(price, _)| format!("{price:.6}"))
                    .unwrap_or_else(|| "none".to_string());
                format!(
                    "insufficient_depth:{token_id}:requested_shares={requested_shares:.6}:available_ask_shares={available_ask_shares:.6}:best_ask={best_ask}:ask_levels={}",
                    snapshot.asks.len()
                )
            })?;
        prices.push(price);
    }
    Ok(prices)
}

fn depth_reprice_token_ids(opp: &ArbitrageOpportunity) -> Option<Vec<String>> {
    opp.execution_plan
        .iter()
        .map(|leg| {
            let market = opp.markets.get(leg.market_index)?;
            let token_id = if !leg.token_id.is_empty() {
                leg.token_id.clone()
            } else if matches!(leg.outcome, crate::models::OutcomeSide::Yes) {
                market.clob_token_id_yes.clone()
            } else {
                market.clob_token_id_no.clone()
            };
            let token_id = token_id.trim().to_string();
            (!token_id.is_empty()).then_some(token_id)
        })
        .collect()
}

fn scan_depth_max_age_ms(config: &Config) -> u64 {
    config.live_max_refresh_to_submit_ms.max(1)
}

fn scan_depth_max_route_skew_ms(config: &Config) -> u64 {
    config
        .live_max_refresh_to_submit_ms
        .max(config.ws_quote_max_age_ms)
        .max(250)
}

fn unix_now_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as u64)
}

fn scan_depth_snapshots_coherent(
    config: &Config,
    token_ids: &[String],
    depth_snapshots: &HashMap<String, crate::clob_client::DepthSnapshot>,
) -> bool {
    let mut observed_ats = Vec::with_capacity(token_ids.len());
    for token_id in token_ids {
        let Some(snapshot) = depth_snapshots.get(token_id) else {
            return false;
        };
        if snapshot
            .book_hash
            .as_deref()
            .map(str::trim)
            .filter(|hash| !hash.is_empty())
            .is_none()
        {
            return false;
        }
        let Some(timestamp) = snapshot.venue_timestamp_ms else {
            return false;
        };
        let Some(observed_at) = snapshot.observed_at else {
            return false;
        };
        observed_ats.push(observed_at);

        let Some(now_ms) = unix_now_ms() else {
            continue;
        };
        let max_skew_ms = scan_depth_max_route_skew_ms(config);
        if timestamp > now_ms.saturating_add(max_skew_ms) {
            return false;
        }
    }

    let min_observed = observed_ats
        .iter()
        .min()
        .copied()
        .unwrap_or_else(Instant::now);
    let max_observed = observed_ats.iter().max().copied().unwrap_or(min_observed);
    let max_skew = Duration::from_millis(scan_depth_max_route_skew_ms(config));
    if max_observed.duration_since(min_observed) > max_skew {
        return false;
    }

    let now = Instant::now();
    let max_age = Duration::from_millis(scan_depth_max_age_ms(config));
    observed_ats.into_iter().all(|observed_at| {
        now.saturating_duration_since(observed_at) <= max_age
            && observed_at.saturating_duration_since(now) <= max_skew
    })
}

fn depth_snapshot_coherence_rejection_note(
    config: &Config,
    token_ids: &[String],
    depth_snapshots: &HashMap<String, crate::clob_client::DepthSnapshot>,
) -> String {
    let mut observed_ats = Vec::with_capacity(token_ids.len());
    for token_id in token_ids {
        let Some(snapshot) = depth_snapshots.get(token_id) else {
            return format!("depth_snapshots_incoherent:missing_snapshot:{token_id}");
        };
        if snapshot
            .book_hash
            .as_deref()
            .map(str::trim)
            .filter(|hash| !hash.is_empty())
            .is_none()
        {
            return format!("depth_snapshots_incoherent:missing_book_hash:{token_id}");
        }
        let Some(timestamp) = snapshot.venue_timestamp_ms else {
            return format!("depth_snapshots_incoherent:missing_venue_timestamp:{token_id}");
        };
        let Some(observed_at) = snapshot.observed_at else {
            return format!("depth_snapshots_incoherent:missing_observed_at:{token_id}");
        };
        observed_ats.push((token_id.as_str(), observed_at));

        let Some(now_ms) = unix_now_ms() else {
            continue;
        };
        let max_skew_ms = scan_depth_max_route_skew_ms(config);
        if timestamp > now_ms.saturating_add(max_skew_ms) {
            return format!(
                "depth_snapshots_incoherent:future_venue_timestamp:{token_id}:ahead_ms={}:max_skew_ms={max_skew_ms}",
                timestamp.saturating_sub(now_ms)
            );
        }
    }

    let min_observed = observed_ats
        .iter()
        .map(|(_, observed_at)| *observed_at)
        .min()
        .unwrap_or_else(Instant::now);
    let max_observed = observed_ats
        .iter()
        .map(|(_, observed_at)| *observed_at)
        .max()
        .unwrap_or(min_observed);
    let max_skew = Duration::from_millis(scan_depth_max_route_skew_ms(config));
    if max_observed.duration_since(min_observed) > max_skew {
        return format!(
            "depth_snapshots_incoherent:observed_skew_ms={}:max_skew_ms={}",
            max_observed.duration_since(min_observed).as_millis(),
            max_skew.as_millis()
        );
    }

    let now = Instant::now();
    let max_age = Duration::from_millis(scan_depth_max_age_ms(config));
    for (token_id, observed_at) in observed_ats {
        if now.saturating_duration_since(observed_at) > max_age {
            return format!(
                "depth_snapshots_incoherent:stale_observation:{token_id}:age_ms={}:max_age_ms={}",
                now.saturating_duration_since(observed_at).as_millis(),
                max_age.as_millis()
            );
        }
        if observed_at.saturating_duration_since(now) > max_skew {
            return format!(
                "depth_snapshots_incoherent:future_observation:{token_id}:ahead_ms={}:max_skew_ms={}",
                observed_at.saturating_duration_since(now).as_millis(),
                max_skew.as_millis()
            );
        }
    }

    "depth_snapshots_incoherent".to_string()
}

#[cfg(test)]
async fn reprice_opportunity_at_target_size(
    client: &Client,
    config: &Config,
    price_cache: Option<&PriceCache>,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
) -> Option<ArbitrageOpportunity> {
    reprice_opportunity_at_target_size_with_reason(
        client,
        config,
        price_cache,
        opp,
        target_position_usd,
    )
    .await
    .ok()
}

async fn reprice_opportunity_at_target_size_with_reason(
    client: &Client,
    config: &Config,
    price_cache: Option<&PriceCache>,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
) -> Result<ArbitrageOpportunity, String> {
    if opportunity_has_external_quotes(opp)
        || !config.validate_opportunities_at_target_size
        || !opp.prices_from_clob
    {
        return Ok(opp.clone());
    }
    if opp.execution_plan.is_empty() {
        return Ok(opp.clone());
    }

    let target_position_usd = target_position_for_depth_reprice(opp, config, target_position_usd);
    if target_position_usd <= f64::EPSILON || opp.total_cost <= f64::EPSILON {
        return Err(format!(
            "invalid_target_or_cost:target_position_usd={target_position_usd:.6}:cost_per_basket={:.6}",
            opp.total_cost
        ));
    }

    let unit_step = basket_unit_step(&opp.execution_plan, config);
    let (token_ids, mut depth_snapshots, mut from_cache) =
        fetch_depth_snapshots_for_reprice_with_reason(client, config, price_cache, opp).await?;
    let mut basket_units = target_position_usd / opp.total_cost;
    for _ in 0..2 {
        let avg_prices = match depth_adjusted_prices_from_snapshots_with_reason(
            opp,
            &token_ids,
            &depth_snapshots,
            basket_units,
        ) {
            Ok(prices) => prices,
            Err(cache_reason) if from_cache => {
                depth_snapshots = crate::clob_client::get_live_depth_snapshots(
                    client, config, &token_ids,
                )
                .await
                .map_err(|err| {
                    format!("cache_depth_unusable:{cache_reason};rest_depth_fetch_failed:{err:#}")
                })?;
                if !scan_depth_snapshots_coherent(config, &token_ids, &depth_snapshots) {
                    return Err(format!(
                        "cache_depth_unusable:{cache_reason};{}",
                        depth_snapshot_coherence_rejection_note(
                            config,
                            &token_ids,
                            &depth_snapshots
                        )
                    ));
                }
                from_cache = false;
                depth_adjusted_prices_from_snapshots_with_reason(
                    opp,
                    &token_ids,
                    &depth_snapshots,
                    basket_units,
                )?
            }
            Err(reason) => return Err(reason),
        };
        let total_cost = opp
            .execution_plan
            .iter()
            .zip(avg_prices.iter())
            .map(|(leg, price)| leg.unit_shares * price)
            .sum::<f64>();
        if total_cost <= f64::EPSILON {
            return Err("non_positive_repriced_cost_per_basket".to_string());
        }
        let next_units = basket_units.min(target_position_usd / total_cost);
        if (next_units - basket_units).abs() / basket_units.max(1.0) < 0.01 {
            basket_units = next_units;
            break;
        }
        basket_units = next_units;
    }

    basket_units = round_down_to_step(basket_units, unit_step);
    let min_basket_units = minimum_order_basket_units_for_opp(opp, config);
    if basket_units <= f64::EPSILON || basket_units + f64::EPSILON < min_basket_units {
        debug!(
            "Depth-aware reprice rejected event {} ({}) because rounded basket units {:.6} are below required minimum {:.6}",
            opp.event_id,
            opp.arb_type,
            basket_units,
            min_basket_units,
        );
        return Err(format!(
            "below_minimum_order_size:rounded_basket_units={basket_units:.6}:minimum_basket_units={min_basket_units:.6}:target_position_usd={target_position_usd:.6}"
        ));
    }

    let avg_prices = match depth_adjusted_prices_from_snapshots_with_reason(
        opp,
        &token_ids,
        &depth_snapshots,
        basket_units,
    ) {
        Ok(prices) => prices,
        Err(cache_reason) if from_cache => {
            depth_snapshots =
                crate::clob_client::get_live_depth_snapshots(client, config, &token_ids)
                    .await
                    .map_err(|err| {
                        format!(
                            "cache_depth_unusable:{cache_reason};rest_depth_fetch_failed:{err:#}"
                        )
                    })?;
            if !scan_depth_snapshots_coherent(config, &token_ids, &depth_snapshots) {
                return Err(format!(
                    "cache_depth_unusable:{cache_reason};{}",
                    depth_snapshot_coherence_rejection_note(config, &token_ids, &depth_snapshots)
                ));
            }
            depth_adjusted_prices_from_snapshots_with_reason(
                opp,
                &token_ids,
                &depth_snapshots,
                basket_units,
            )?
        }
        Err(reason) => return Err(reason),
    };
    let min_basket_units_after_reprice =
        minimum_order_basket_units_for_prices(opp, &avg_prices, config);
    if basket_units + f64::EPSILON < min_basket_units_after_reprice {
        debug!(
            "Depth-aware reprice rejected event {} ({}) because basket units {:.6} are below repriced minimum {:.6}",
            opp.event_id,
            opp.arb_type,
            basket_units,
            min_basket_units_after_reprice,
        );
        return Err(format!(
            "below_repriced_minimum_order_size:basket_units={basket_units:.6}:minimum_basket_units={min_basket_units_after_reprice:.6}"
        ));
    }

    let total_cost = opp
        .execution_plan
        .iter()
        .zip(avg_prices.iter())
        .map(|(leg, price)| leg.unit_shares * price)
        .sum::<f64>();
    if total_cost <= f64::EPSILON {
        return Err("non_positive_final_repriced_cost_per_basket".to_string());
    }

    let gross_profit = opp.guaranteed_revenue - total_cost;
    if gross_profit <= 0.0 {
        return Err(format!(
            "repriced_gross_edge_gone:guaranteed_revenue={:.6}:total_cost={total_cost:.6}:gross_profit={gross_profit:.6}",
            opp.guaranteed_revenue
        ));
    }

    let total_fees_per_basket = opp
        .execution_plan
        .iter()
        .zip(avg_prices.iter())
        .filter_map(|(leg, price)| {
            opp.markets.get(leg.market_index).map(|market| {
                crate::fees::total_fee_for_market(
                    *price,
                    leg.unit_shares,
                    market,
                    &opp.category,
                    config,
                )
            })
        })
        .sum::<f64>();

    let projected_position_usd = basket_units * total_cost;
    if projected_position_usd <= f64::EPSILON {
        return Err("non_positive_projected_position_after_reprice".to_string());
    }

    let inferred_gas_cost = inferred_trade_gas_cost(opp);
    let projected_total_pnl =
        basket_units * (gross_profit - total_fees_per_basket) - inferred_gas_cost;
    let projected_roi_pct = projected_total_pnl / projected_position_usd * 100.0;

    if projected_total_pnl < config.min_net_profit_usd || projected_roi_pct < config.min_roi_pct {
        debug!(
            "Depth-aware reprice rejected event {} ({}) projected_pnl=${:.4} projected_roi={:.2}% target=${:.2}",
            opp.event_id,
            opp.arb_type,
            projected_total_pnl,
            projected_roi_pct,
            target_position_usd,
        );
        return Err(format!(
            "repriced_edge_below_threshold:projected_pnl_usd={projected_total_pnl:.6}:min_net_profit_usd={:.6}:projected_roi_pct={projected_roi_pct:.6}:min_roi_pct={:.6}:projected_position_usd={projected_position_usd:.6}:total_cost_per_basket={total_cost:.6}:basket_units={basket_units:.6}",
            config.min_net_profit_usd,
            config.min_roi_pct
        ));
    }

    let mut repriced = opp.clone();
    repriced.total_cost = total_cost;
    repriced.gross_profit = gross_profit;
    repriced.total_fees = total_fees_per_basket;
    repriced.net_profit = projected_total_pnl;
    repriced.roi_pct = projected_roi_pct;
    repriced.expected_slippage_pct = if opp.total_cost > f64::EPSILON {
        ((total_cost / opp.total_cost) - 1.0).max(0.0) * 100.0
    } else {
        0.0
    };
    repriced.max_executable_size_usd = projected_position_usd;

    for (leg, price) in repriced.execution_plan.iter().zip(avg_prices.iter()) {
        if let Some(market) = repriced.markets.get_mut(leg.market_index) {
            match leg.outcome {
                crate::models::OutcomeSide::Yes => market.clob_yes_ask = Some(*price),
                crate::models::OutcomeSide::No => market.clob_no_ask = Some(*price),
            }
        }
    }

    Ok(repriced)
}

fn opportunity_has_external_quotes(opp: &ArbitrageOpportunity) -> bool {
    opp.execution_plan
        .iter()
        .any(|leg| is_external_token_id(leg.token_id.trim()))
        || opp.markets.iter().any(market_has_external_quotes)
}

fn opportunity_can_execute_on_polymarket(opp: &ArbitrageOpportunity) -> bool {
    !opportunity_has_external_quotes(opp)
        && !opp.execution_plan.is_empty()
        && opp
            .execution_plan
            .iter()
            .all(|leg| !leg.token_id.trim().is_empty())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveBlockReason {
    ExternalToken,
    MissingToken,
    RankedUnsupported,
    MintSellUnsupported,
    NonAtomicBasket,
    RouteUnsupported,
}

impl LiveBlockReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExternalToken => "external_token",
            Self::MissingToken => "missing_token",
            Self::RankedUnsupported => "ranked_unsupported",
            Self::MintSellUnsupported => "mint_sell_unsupported",
            Self::NonAtomicBasket => "non_atomic_basket",
            Self::RouteUnsupported => "route_unsupported",
        }
    }
}

fn live_execution_block_reason(opp: &ArbitrageOpportunity) -> Option<LiveBlockReason> {
    if opportunity_has_external_quotes(opp) {
        return Some(LiveBlockReason::ExternalToken);
    }
    if opp.execution_plan.is_empty()
        || opp
            .execution_plan
            .iter()
            .any(|leg| leg.token_id.trim().is_empty())
    {
        return Some(LiveBlockReason::MissingToken);
    }
    if matches!(opp.arb_type, crate::models::ArbType::Ranked) {
        return Some(LiveBlockReason::RankedUnsupported);
    }
    if matches!(opp.arb_type, crate::models::ArbType::MintSell) {
        return Some(LiveBlockReason::MintSellUnsupported);
    }
    if !live_executor::live_arbitrage_routes_available() {
        return Some(LiveBlockReason::RouteUnsupported);
    }
    if !matches!(
        opp.arb_type,
        crate::models::ArbType::Yes | crate::models::ArbType::No
    ) {
        return Some(LiveBlockReason::RouteUnsupported);
    }
    if !is_supported_yes_no_full_family_plan(opp) {
        return Some(LiveBlockReason::RouteUnsupported);
    }
    if opp.execution_plan.len() != 1 {
        return Some(LiveBlockReason::NonAtomicBasket);
    }
    None
}

async fn live_trade_toxicity_blocker(
    price_cache: Option<&PriceCache>,
    config: &Config,
    opp: &ArbitrageOpportunity,
    position_usd: f64,
) -> Option<String> {
    let cache = price_cache?;
    let token_ids = depth_reprice_token_ids(opp)?;
    let window = Duration::from_millis(LIVE_TOXIC_TRADE_WINDOW_MS);
    let position_usd = position_usd.max(0.0);
    let threshold_usd =
        (position_usd * LIVE_TOXIC_TRADE_POSITION_FRACTION).max(LIVE_TOXIC_TRADE_MIN_NOTIONAL_USD);
    let now = Instant::now();
    let max_age = selection_cache_max_age(config);
    let guard = cache.read().await;

    for token_id in token_ids {
        let Some(snapshot) = guard.get(token_id.as_str()) else {
            continue;
        };
        if snapshot.last_updated.elapsed() > max_age {
            continue;
        }
        let recent_buy_notional = snapshot
            .recent_trades
            .iter()
            .filter(|trade| trade.side == "BUY")
            .filter(|trade| now.duration_since(trade.observed_at) <= window)
            .map(|trade| trade.price * trade.size.max(0.0))
            .sum::<f64>();
        if recent_buy_notional >= threshold_usd {
            return Some(format!(
                "recent_same_side_trade_sweep:{token_id}:buy_notional=${recent_buy_notional:.4}>={threshold_usd:.4}:window_ms={LIVE_TOXIC_TRADE_WINDOW_MS}"
            ));
        }
        let (adverse_depth_flow_notional, flow_window_ms) =
            recent_adverse_depth_flow_notional(snapshot, now, LIVE_TOXIC_DEPTH_FLOW_LEVELS);
        if adverse_depth_flow_notional >= threshold_usd {
            return Some(format!(
                "adverse_depth_flow:{token_id}:flow_notional=${adverse_depth_flow_notional:.4}>={threshold_usd:.4}:window_ms={flow_window_ms}:levels={LIVE_TOXIC_DEPTH_FLOW_LEVELS}"
            ));
        }
        if let Some((flow_ratio, ask_depth_notional)) =
            adverse_depth_flow_pressure(snapshot, adverse_depth_flow_notional)
        {
            if flow_ratio >= LIVE_TOXIC_DEPTH_FLOW_MIN_RATIO {
                return Some(format!(
                    "adverse_depth_flow_pressure:{token_id}:flow_notional=${adverse_depth_flow_notional:.4}:ask_depth=${ask_depth_notional:.4}:flow_ratio={flow_ratio:.4}>={LIVE_TOXIC_DEPTH_FLOW_MIN_RATIO:.4}:window_ms={flow_window_ms}:levels={LIVE_TOXIC_DEPTH_FLOW_LEVELS}"
                ));
            }
        }
        let ask_notional = top_depth_notional(&snapshot.ask_depth, LIVE_TOXIC_BOOK_DEPTH_LEVELS);
        let bid_notional = top_depth_notional(&snapshot.bid_depth, LIVE_TOXIC_BOOK_DEPTH_LEVELS);
        let total_notional = ask_notional + bid_notional;
        if bid_notional >= threshold_usd && total_notional > f64::EPSILON {
            let bid_ratio = bid_notional / total_notional;
            if bid_ratio >= LIVE_TOXIC_BOOK_IMBALANCE_MIN_RATIO {
                return Some(format!(
                    "book_buy_pressure:{token_id}:bid_depth_ratio={bid_ratio:.4}>={LIVE_TOXIC_BOOK_IMBALANCE_MIN_RATIO:.4}:bid_notional=${bid_notional:.4}:ask_notional=${ask_notional:.4}:levels={LIVE_TOXIC_BOOK_DEPTH_LEVELS}"
                ));
            }
        }
        if let (Some(ask), Some((microprice, queue_imbalance))) = (
            live_clob_executable_ask(snapshot),
            live_clob_microprice(snapshot),
        ) {
            let adverse_bps = ((ask - microprice) / microprice.max(f64::EPSILON)) * 10_000.0;
            if adverse_bps > config.live_clob_microprice_adverse_bps {
                return Some(format!(
                    "clob_microprice_adverse:{token_id}:ask={ask:.6}:microprice={microprice:.6}:adverse_bps={adverse_bps:.2}>{:.2}:queue_imbalance={queue_imbalance:.4}",
                    config.live_clob_microprice_adverse_bps
                ));
            }
        }
        let ask_near_notional =
            top_depth_notional(&snapshot.ask_depth, LIVE_FRAGILE_BOOK_DEPTH_LEVELS);
        let min_near_depth = position_usd * LIVE_FRAGILE_MIN_ASK_DEPTH_POSITION_MULTIPLIER;
        if position_usd > f64::EPSILON
            && ask_near_notional > f64::EPSILON
            && ask_near_notional < min_near_depth
        {
            return Some(format!(
                "ask_depth_fragile:{token_id}:ask_notional=${ask_near_notional:.4}<min=${min_near_depth:.4}:position=${position_usd:.4}:levels={LIVE_FRAGILE_BOOK_DEPTH_LEVELS}"
            ));
        }
        if position_usd > f64::EPSILON {
            if let Some(top_ratio) =
                top_depth_notional_ratio(&snapshot.ask_depth, LIVE_FRAGILE_BOOK_DEPTH_LEVELS)
            {
                let concentration_cap = position_usd * LIVE_FRAGILE_TOP_ASK_DEPTH_CAP_MULTIPLIER;
                if top_ratio >= LIVE_FRAGILE_TOP_ASK_MAX_RATIO
                    && ask_near_notional < concentration_cap
                {
                    return Some(format!(
                        "ask_depth_concentrated:{token_id}:top_ask_ratio={top_ratio:.4}>={LIVE_FRAGILE_TOP_ASK_MAX_RATIO:.4}:ask_notional=${ask_near_notional:.4}<cap=${concentration_cap:.4}:levels={LIVE_FRAGILE_BOOK_DEPTH_LEVELS}"
                    ));
                }
            }
        }
    }

    None
}

async fn opportunity_markout_blocker(
    price_cache: Option<&PriceCache>,
    config: &Config,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
) -> Option<String> {
    if !opp.prices_from_clob
        || opportunity_has_external_quotes(opp)
        || opp.execution_plan.is_empty()
    {
        return None;
    }
    let token_ids = depth_reprice_token_ids(opp)?;
    if token_ids.len() != opp.execution_plan.len() {
        return Some("markout_token_plan_mismatch".to_string());
    }
    let position_usd = target_position_for_depth_reprice(opp, config, target_position_usd);
    if let Some(blocker) = live_trade_toxicity_blocker(price_cache, config, opp, position_usd).await
    {
        return Some(format!("markout_toxicity:{blocker}"));
    }

    let basket_units = if opp.total_cost > f64::EPSILON {
        position_usd / opp.total_cost
    } else {
        0.0
    };
    if !basket_units.is_finite() || basket_units <= f64::EPSILON {
        return Some(format!(
            "markout_invalid_basket_units:position_usd={:.4}:cost_per_basket={:.6}",
            opp.max_executable_size_usd, opp.total_cost
        ));
    }

    let max_age = selection_cache_max_age(config);
    let ask_worsen_cap_bps = config.live_clob_microprice_adverse_bps.max(0.0);
    let mut current_total_cost = 0.0;
    let mut toxicity_haircut_usd = 0.0;
    let mut all_legs_fill_probability = 1.0;
    let mut max_snapshot_age = Duration::ZERO;
    let now = Instant::now();
    let guard = match price_cache {
        Some(cache) => Some(cache.read().await),
        None => None,
    };
    for (leg, token_id) in opp.execution_plan.iter().zip(token_ids.iter()) {
        let Some(planned_ask) = planned_ask_for_leg(opp, leg) else {
            return Some(format!("markout_missing_planned_ask:{token_id}"));
        };
        if planned_ask <= f64::EPSILON || !planned_ask.is_finite() {
            return Some(format!(
                "markout_invalid_planned_ask:{token_id}:{planned_ask:.6}"
            ));
        }
        let fresh_snapshot = guard
            .as_ref()
            .and_then(|cache| cache.get(token_id.as_str()))
            .filter(|snapshot| snapshot.last_updated.elapsed() <= max_age);
        let current_ask = fresh_snapshot
            .and_then(live_clob_executable_ask)
            .unwrap_or(planned_ask);
        if let Some(snapshot) = fresh_snapshot {
            let age = snapshot.last_updated.elapsed();
            max_snapshot_age = max_snapshot_age.max(age);
            let required_leg_notional_usd = current_ask * leg.unit_shares * basket_units;
            all_legs_fill_probability *=
                markout_leg_fill_survival_probability(snapshot, required_leg_notional_usd, now);
            toxicity_haircut_usd +=
                scan_quote_toxicity_penalty_for_position(snapshot, config, position_usd)
                    * OPPORTUNITY_MARKOUT_TOXICITY_USD_PER_SCORE;
        }
        let ask_worsen_bps = ((current_ask - planned_ask) / planned_ask) * 10_000.0;
        if ask_worsen_bps > ask_worsen_cap_bps {
            return Some(format!(
                "markout_current_ask_worse:{token_id}:current={current_ask:.6}:planned={planned_ask:.6}:worsen_bps={ask_worsen_bps:.2}>{ask_worsen_cap_bps:.2}"
            ));
        }
        current_total_cost += current_ask * leg.unit_shares;
    }

    let adverse_markout_usd = (current_total_cost - opp.total_cost).max(0.0) * basket_units;
    let latency_haircut_usd =
        opportunity_markout_latency_haircut_usd(config, position_usd, max_snapshot_age, max_age);
    let fill_failure_haircut_usd =
        opp.net_profit.max(0.0) * (1.0 - all_legs_fill_probability.clamp(0.0, 1.0));
    let edge_after_markout_usd = opp.net_profit
        - adverse_markout_usd
        - toxicity_haircut_usd
        - latency_haircut_usd
        - fill_failure_haircut_usd;
    if edge_after_markout_usd < config.min_net_profit_usd {
        return Some(format!(
            "markout_edge_after_toxicity_below_min:edge_after=${edge_after_markout_usd:.4}<${:.4}:adverse_markout=${adverse_markout_usd:.4}:toxicity_haircut=${toxicity_haircut_usd:.4}:latency_haircut=${latency_haircut_usd:.4}:fill_failure_haircut=${fill_failure_haircut_usd:.4}:p_all_fill={all_legs_fill_probability:.4}:max_snapshot_age_ms={}:current_cost={current_total_cost:.6}:planned_cost={:.6}",
            config.min_net_profit_usd,
            max_snapshot_age.as_millis(),
            opp.total_cost
        ));
    }

    None
}

fn planned_ask_for_leg(
    opp: &ArbitrageOpportunity,
    leg: &crate::models::OpportunityLeg,
) -> Option<f64> {
    let market = opp.markets.get(leg.market_index)?;
    match leg.outcome {
        crate::models::OutcomeSide::Yes => market.clob_yes_ask,
        crate::models::OutcomeSide::No => market.clob_no_ask,
    }
    .filter(|price| price.is_finite() && *price > 0.0)
}

fn live_clob_executable_ask(snapshot: &crate::ws_client::Price) -> Option<f64> {
    snapshot
        .best_ask
        .or_else(|| snapshot.ask_depth.first().map(|(price, _)| *price))
        .filter(|price| price.is_finite() && *price > 0.0)
}

fn live_clob_microprice(snapshot: &crate::ws_client::Price) -> Option<(f64, f64)> {
    live_clob_depth_vamp(snapshot)
        .or_else(|| live_clob_top_of_book_microprice(snapshot))
        .filter(|(microprice, _)| microprice.is_finite() && *microprice > 0.0)
}

fn live_clob_depth_vamp(snapshot: &crate::ws_client::Price) -> Option<(f64, f64)> {
    let mut weighted_sum = 0.0;
    let mut size_sum = 0.0;
    let mut bid_size_sum = 0.0;
    let mut ask_size_sum = 0.0;

    for ((ask_price, ask_size), (bid_price, bid_size)) in snapshot
        .ask_depth
        .iter()
        .take(LIVE_TOXIC_BOOK_DEPTH_LEVELS)
        .zip(snapshot.bid_depth.iter().take(LIVE_TOXIC_BOOK_DEPTH_LEVELS))
    {
        if !ask_price.is_finite()
            || !bid_price.is_finite()
            || !ask_size.is_finite()
            || !bid_size.is_finite()
            || *ask_price <= 0.0
            || *bid_price <= 0.0
            || *ask_size <= 0.0
            || *bid_size <= 0.0
        {
            continue;
        }
        weighted_sum += ask_price * bid_size + bid_price * ask_size;
        size_sum += ask_size + bid_size;
        ask_size_sum += ask_size;
        bid_size_sum += bid_size;
    }

    if size_sum <= f64::EPSILON {
        None
    } else {
        Some((
            weighted_sum / size_sum,
            (bid_size_sum - ask_size_sum) / size_sum,
        ))
    }
}

fn live_clob_top_of_book_microprice(snapshot: &crate::ws_client::Price) -> Option<(f64, f64)> {
    let (Some(best_bid), Some(best_ask), Some(best_bid_size), Some(best_ask_size)) = (
        snapshot.best_bid,
        snapshot.best_ask,
        snapshot.best_bid_size,
        snapshot.best_ask_size,
    ) else {
        return None;
    };
    if best_bid <= 0.0
        || best_ask <= 0.0
        || best_bid_size <= 0.0
        || best_ask_size <= 0.0
        || !best_bid.is_finite()
        || !best_ask.is_finite()
        || !best_bid_size.is_finite()
        || !best_ask_size.is_finite()
    {
        return None;
    }
    let size_sum = best_bid_size + best_ask_size;
    if size_sum <= f64::EPSILON {
        None
    } else {
        Some((
            (best_ask * best_bid_size + best_bid * best_ask_size) / size_sum,
            (best_bid_size - best_ask_size) / size_sum,
        ))
    }
}

fn adverse_depth_flow_pressure(
    snapshot: &crate::ws_client::Price,
    adverse_depth_flow_notional: f64,
) -> Option<(f64, f64)> {
    if adverse_depth_flow_notional < LIVE_TOXIC_DEPTH_FLOW_RATIO_MIN_NOTIONAL_USD {
        return None;
    }
    let ask_depth_notional = top_depth_notional(&snapshot.ask_depth, LIVE_TOXIC_DEPTH_FLOW_LEVELS);
    if ask_depth_notional <= f64::EPSILON {
        None
    } else {
        Some((
            adverse_depth_flow_notional / ask_depth_notional,
            ask_depth_notional,
        ))
    }
}

fn top_depth_notional(levels: &[(f64, f64)], limit: usize) -> f64 {
    levels
        .iter()
        .take(limit)
        .map(|(price, size)| price.max(0.0) * size.max(0.0))
        .sum()
}

fn top_depth_notional_ratio(levels: &[(f64, f64)], limit: usize) -> Option<f64> {
    let total = top_depth_notional(levels, limit);
    if total <= f64::EPSILON {
        return None;
    }
    let top = levels
        .first()
        .map(|(price, size)| price.max(0.0) * size.max(0.0))
        .unwrap_or(0.0);
    if top <= f64::EPSILON {
        None
    } else {
        Some(top / total)
    }
}

fn combo_rfq_live_candidate<'a>(
    config: &Config,
    opp: &ArbitrageOpportunity,
    combo_catalog: Option<&'a combo_rfq_client::ComboMarketCatalog>,
) -> Option<&'a combo_rfq_client::ComboMarketCatalog> {
    if !config.live_combo_rfq_route_enabled {
        return None;
    }
    let catalog = combo_catalog?;
    let route_plan = execution_routes::plan_blocked_live_route(opp, Some(catalog));
    matches!(
        route_plan.kind,
        execution_routes::LiveRouteKind::ComboRfqCandidate
    )
    .then_some(catalog)
}

fn log_live_blocked_opportunity(
    stats: &mut ScanStats,
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    config: &Config,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
    reason: LiveBlockReason,
    combo_catalog: Option<&combo_rfq_client::ComboMarketCatalog>,
) {
    let mut note = format!(
        "live blocked: {} on {}",
        reason.as_str(),
        short_text(&opp.event_title, 64)
    );
    if opp.execution_plan.len() > 1 {
        let route_plan = execution_routes::plan_blocked_live_route(opp, combo_catalog);
        if matches!(
            route_plan.kind,
            execution_routes::LiveRouteKind::ComboRfqCandidate
        ) {
            stats.combo_rfq_candidate_blocks += 1;
        }
        let protocol_report =
            protocol_preflight::blocked_live_protocol_preflight(config, &route_plan);
        note = format!("{note}; {}; {}", route_plan.note(), protocol_report.note());
        if let (execution_routes::LiveRouteKind::ComboRfqCandidate, Some(catalog)) =
            (route_plan.kind, combo_catalog)
        {
            let requester_plan =
                combo_rfq_client::build_combo_rfq_requester_plan(config, catalog, opp);
            let best_execution =
                combo_rfq_client::build_combo_rfq_best_execution_report(config, opp, None);
            note = format!("{note}; {}; {}", requester_plan.note, best_execution.note);
        }
        if let Some(shadow_note) =
            live_executor::record_live_route_shadow(config, opp, route_plan.kind)
        {
            note = format!("{note}; {shadow_note}");
        }
    }
    push_operator_note(stats, note.clone());
    log_trade_event(
        diagnostics,
        scan_index,
        "live",
        "blocked",
        opp,
        target_position_usd,
        note,
    );
}

fn skip_live_blocked_opportunity(
    stats: &mut ScanStats,
    emit_live_diagnostics: bool,
    live_execution: bool,
    paper_execution_enabled: bool,
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    config: &Config,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
    combo_catalog: Option<&combo_rfq_client::ComboMarketCatalog>,
) -> bool {
    if !emit_live_diagnostics {
        return false;
    }

    let Some(reason) = live_execution_block_reason(opp) else {
        return false;
    };
    if combo_rfq_live_candidate(config, opp, combo_catalog).is_some() {
        return false;
    }
    log_live_blocked_opportunity(
        stats,
        diagnostics,
        scan_index,
        config,
        opp,
        target_position_usd,
        reason,
        combo_catalog,
    );
    live_execution && !paper_execution_enabled
}

async fn maybe_execute_live_opportunity(
    stats: &mut ScanStats,
    scan_start: Instant,
    client: &Client,
    config: &Config,
    price_cache: Option<&PriceCache>,
    live_execution: bool,
    live_executor: Option<&live_executor::LiveExecutor>,
    exposure: &Arc<ExposureTracker>,
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
    combo_catalog: Option<&combo_rfq_client::ComboMarketCatalog>,
    session_trades_executed: &mut usize,
    session_pnl_usd: &mut f64,
    session_position_usd: &mut f64,
    success_note: &'static str,
    failure_context: &'static str,
) -> anyhow::Result<bool> {
    if !live_execution {
        return Ok(false);
    }
    let diagnostics = diagnostics.context("live execution requires initialized diagnostics")?;
    diagnostics.ensure_healthy()?;
    let Some(executor) = live_executor else {
        warn!("Live execution requested but warm live executor is unavailable");
        return Ok(false);
    };

    if let Some(blocker) = live_latency_budget_blocker_at_scan_elapsed(stats, scan_start, config) {
        let note = format!(
            "live blocked: latency_budget on {}; {blocker}",
            short_text(&opp.event_title, 64)
        );
        push_operator_note(stats, note.clone());
        log_trade_event(
            Some(diagnostics),
            scan_index,
            "live",
            "blocked_latency_budget",
            opp,
            target_position_usd,
            note,
        );
        return Ok(false);
    }

    if let Some(blocker) =
        live_trade_toxicity_blocker(price_cache, config, opp, target_position_usd).await
    {
        let note = format!(
            "live blocked: trade_toxicity on {}; {blocker}",
            short_text(&opp.event_title, 64)
        );
        push_operator_note(stats, note.clone());
        log_trade_event(
            Some(diagnostics),
            scan_index,
            "live",
            "blocked_trade_toxicity",
            opp,
            target_position_usd,
            note,
        );
        return Ok(false);
    }

    if let Some(reason) = live_execution_block_reason(opp) {
        if let Some(catalog) = combo_rfq_live_candidate(config, opp, combo_catalog) {
            diagnostics.ensure_healthy()?;
            match live_executor::execute_combo_rfq_opportunity_with_executor(
                executor,
                opp,
                config,
                client,
                exposure,
                catalog,
                price_cache,
            )
            .await
            {
                Ok(report) => {
                    let note = format!(
                        "combo/rfq live route status={} rfq_id={:?} blockers={}",
                        report.status,
                        report.rfq_id,
                        report.blockers.join("|")
                    );
                    push_operator_note(stats, note.clone());
                    log_trade_event(
                        Some(diagnostics),
                        scan_index,
                        "live_combo_rfq",
                        &report.status,
                        opp,
                        target_position_usd,
                        note,
                    );
                    diagnostics.ensure_healthy()?;
                    return Ok(true);
                }
                Err(err) => {
                    warn!(
                        "Combo/RFQ live execution failed for {} {}: {}",
                        opp.event_id, opp.arb_type, err
                    );
                    log_trade_event(
                        Some(diagnostics),
                        scan_index,
                        "live_combo_rfq",
                        "error",
                        opp,
                        target_position_usd,
                        err.to_string(),
                    );
                    diagnostics.ensure_healthy()?;
                    return Ok(false);
                }
            }
        }
        log_live_blocked_opportunity(
            stats,
            Some(diagnostics),
            scan_index,
            config,
            opp,
            target_position_usd,
            reason,
            combo_catalog,
        );
        return Ok(false);
    }

    diagnostics.ensure_healthy()?;
    match live_executor::execute_opportunity_with_executor(
        executor,
        opp,
        config,
        client,
        exposure,
        price_cache,
    )
    .await
    {
        Ok(report) => {
            record_live_execution_session(
                session_trades_executed,
                session_pnl_usd,
                session_position_usd,
                &report,
            );
            log_live_trade_event(
                Some(diagnostics),
                scan_index,
                opp,
                target_position_usd,
                &report,
                success_note,
            )?;
            Ok(true)
        }
        Err(err) => {
            warn!(
                "Live execution failed for {} {} ({}): {}",
                failure_context, opp.event_id, opp.arb_type, err
            );
            log_trade_event(
                Some(diagnostics),
                scan_index,
                "live",
                "error",
                opp,
                target_position_usd,
                err.to_string(),
            );
            diagnostics.ensure_healthy()?;
            Ok(false)
        }
    }
}

fn skip_scan_only_external_opportunity(
    stats: &mut ScanStats,
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    pool: &str,
    outcome_side: Option<crate::models::OutcomeSide>,
    theory_hint: f64,
    quote_ready: bool,
    opp: &ArbitrageOpportunity,
) -> bool {
    if !opportunity_has_external_quotes(opp) {
        return false;
    }

    let note = format!(
        "scan-only external-token opportunity on {}; skipped actionable recording, notification, and execution",
        short_text(&opp.event_title, 64)
    );
    push_operator_note(stats, note.clone());
    if let Some(diagnostics) = diagnostics {
        diagnostics.record_candidate_rejection(CandidateRejectionRow {
            timestamp: timestamp_now(),
            scan_id: scan_index,
            pool: pool.to_string(),
            event_id: opp.event_id.clone(),
            event_title: opp.event_title.clone(),
            event_slug: opp.event_id.clone(),
            market_question: opp
                .markets
                .first()
                .map(|market| market.question.clone())
                .unwrap_or_default(),
            arb_type: opp.arb_type.to_string(),
            outcome_side: outcome_side
                .map(|side| side.to_string())
                .unwrap_or_default(),
            stage: "scan_only".into(),
            reason: "external_source_not_executable".into(),
            theory_hint,
            quote_ready,
            total_cost: Some(opp.total_cost),
            gross_profit: Some(opp.gross_profit),
            total_fees: Some(opp.total_fees),
            projected_net_profit: Some(opp.net_profit),
            note,
        });
    }
    true
}

async fn effective_single_leg_gas_cost_usd(
    client: &Client,
    config: &Config,
    gas_oracle: &GasOracle,
) -> f64 {
    let gas = gas_oracle
        .trade_cost_usd(client, 1, config.gas_fallback_usd)
        .await;
    config.effective_trade_gas_cost_usd(gas)
}

async fn effective_total_gas_cost_usd(
    client: &Client,
    config: &Config,
    gas_oracle: &GasOracle,
    num_legs: usize,
) -> f64 {
    let gas = gas_oracle
        .trade_cost_usd(client, num_legs, config.gas_fallback_usd)
        .await;
    config.effective_trade_gas_cost_usd(gas)
}

async fn run_single_scan(
    client: &Client,
    config: &Config,
    mut external_paper_engine: Option<&mut ExternalPaperEngine>,
    seen_recent: &mut HashMap<String, Instant>,
    use_clob: bool,
    live_execution: bool,
    live_diagnostics: bool,
    live_executor: Option<&live_executor::LiveExecutor>,
    gas_oracle: &GasOracle,
    exposure: &std::sync::Arc<ExposureTracker>,
    price_cache: Option<&PriceCache>,
    ws_command_tx: Option<&tokio::sync::mpsc::Sender<WsCommand>>,
    subscribed_quote_tokens: &mut HashSet<String>,
    ws_subscription_last_desired_scan: &mut HashMap<String, u64>,
    diagnostics: Option<&DiagnosticsLogger>,
    scan_index: u64,
    dirty_tokens: &HashSet<String>,
    combo_catalog: Option<&combo_rfq_client::ComboMarketCatalog>,
    events: &[crate::models::Event],
    all_events: &[crate::models::Event],
    session_trades_executed: &mut usize,
    session_pnl_usd: &mut f64,
    session_position_usd: &mut f64,
) -> anyhow::Result<ScanStats> {
    let scan_start = Instant::now();
    let mut stats = ScanStats::default();
    let paper_execution_enabled = external_paper_engine.is_some();
    let emit_live_diagnostics = live_execution || live_diagnostics;

    if let Some(diagnostics) = diagnostics {
        diagnostics.ensure_healthy()?;
    }

    if let Some(engine) = external_paper_engine.as_ref() {
        engine
            .reconcile_pending_orders_exclusive()
            .await
            .context("paper pending-order reconciliation failed before scan")?;
    }

    if events.is_empty() && all_events.is_empty() {
        runtime_scan_log(
            config,
            "No candidate events found in this scan.".to_string(),
        );
        push_operator_note(
            &mut stats,
            "no candidate events found in this scan".to_string(),
        );
        return Ok(stats);
    }

    let (events, lifecycle_neg_rejections) = filter_lifecycle_scan_events(
        events,
        config,
        diagnostics,
        scan_index,
        "lifecycle_neg_risk",
        "YES/NO",
    );
    let (all_events, lifecycle_bundle_rejections) = filter_lifecycle_scan_events(
        all_events,
        config,
        diagnostics,
        scan_index,
        "lifecycle_bundle",
        "BUNDLE",
    );
    let lifecycle_rejections = lifecycle_neg_rejections + lifecycle_bundle_rejections;
    if lifecycle_rejections > 0 {
        runtime_scan_log(
            config,
            format!(
                "Lifecycle gate skipped {} event(s): neg-risk={} bundle-source={} buffer={}s",
                lifecycle_rejections,
                lifecycle_neg_rejections,
                lifecycle_bundle_rejections,
                config.event_lifecycle_pre_cutoff_buffer_secs,
            ),
        );
        push_operator_note(
            &mut stats,
            format!(
                "lifecycle gate skipped {} event(s) near/past end/start cutoff",
                lifecycle_rejections
            ),
        );
    }

    if events.is_empty() && all_events.is_empty() {
        runtime_scan_log(
            config,
            "No candidate events survived lifecycle gating this scan.".to_string(),
        );
        push_operator_note(
            &mut stats,
            "no candidate events survived lifecycle gating this scan".to_string(),
        );
        return Ok(stats);
    }

    let quote_cache_snapshot = if use_clob {
        cached_scan_quote_snapshot(price_cache, config).await
    } else {
        ScanQuoteCacheSnapshot::default()
    };
    let cached_tokens = &quote_cache_snapshot.fresh_quote_tokens;
    let best_ask_prices = &quote_cache_snapshot.best_ask_prices;
    let toxicity_penalties = &quote_cache_snapshot.toxicity_penalties;
    let execution_survival_adjustments = &quote_cache_snapshot.execution_survival_adjustments;

    let yes_candidates_meta = neg_risk_candidate_selections_for_side(
        &events,
        cached_tokens,
        best_ask_prices,
        toxicity_penalties,
        execution_survival_adjustments,
        config,
        crate::models::OutcomeSide::Yes,
    );
    let no_candidates_meta = neg_risk_candidate_selections_for_side(
        &events,
        cached_tokens,
        best_ask_prices,
        toxicity_penalties,
        execution_survival_adjustments,
        config,
        crate::models::OutcomeSide::No,
    );
    let (bundle_market_pool, bundle_market_candidates_meta) = bundle_market_candidate_selections(
        &all_events,
        cached_tokens,
        best_ask_prices,
        toxicity_penalties,
        execution_survival_adjustments,
        config,
    );

    stats.neg_risk_events_total = events.len();
    stats.yes_candidates_total = yes_candidates_meta.len();
    stats.no_candidates_total = no_candidates_meta.len();
    stats.bundle_markets_total = bundle_market_candidates_meta.len();
    let (yes_dirty_indices, no_dirty_indices, bundle_dirty_indices) = if dirty_tokens.is_empty() {
        (HashSet::new(), HashSet::new(), HashSet::new())
    } else {
        let yes_token_index = candidate_token_index(&yes_candidates_meta);
        let no_token_index = candidate_token_index(&no_candidates_meta);
        let bundle_token_index = candidate_token_index(&bundle_market_candidates_meta);
        (
            dirty_candidate_indices_from_index(&yes_token_index, dirty_tokens),
            dirty_candidate_indices_from_index(&no_token_index, dirty_tokens),
            dirty_candidate_indices_from_index(&bundle_token_index, dirty_tokens),
        )
    };
    if !dirty_tokens.is_empty() {
        runtime_scan_log(
            config,
            format!(
                "Dirty fast lane: yes={} no={} bundle={} candidate(s)",
                yes_dirty_indices.len(),
                no_dirty_indices.len(),
                bundle_dirty_indices.len()
            ),
        );
    }

    let scan_rotation_index = scan_index.saturating_sub(1);
    let total_quote_budget = if use_clob {
        config.quote_refresh_token_budget_per_scan
    } else {
        0
    };

    let neg_has_candidates = !yes_candidates_meta.is_empty() || !no_candidates_meta.is_empty();
    let (neg_total_quote_budget, bundle_quote_budget) = if total_quote_budget == 0 {
        (0usize, 0usize)
    } else if neg_has_candidates && !bundle_market_candidates_meta.is_empty() {
        let neg_pressure = ((yes_candidates_meta
            .iter()
            .chain(no_candidates_meta.iter())
            .map(|candidate| candidate.missing_tokens.min(12))
            .sum::<usize>()) as f64)
            .sqrt();
        let bundle_pressure = (bundle_market_candidates_meta
            .iter()
            .map(|candidate| candidate.missing_tokens.min(2))
            .sum::<usize>() as f64)
            .sqrt();
        let neg_ready = yes_candidates_meta
            .iter()
            .chain(no_candidates_meta.iter())
            .filter(|candidate| candidate.missing_tokens == 0)
            .count() as f64;
        let bundle_ready = bundle_market_candidates_meta
            .iter()
            .filter(|candidate| candidate.missing_tokens == 0)
            .count() as f64;
        let neg_weight = ((neg_pressure + neg_ready * 0.5 + 1.0)
            / ((neg_pressure + neg_ready * 0.5 + 1.0)
                + (bundle_pressure + bundle_ready * 0.25 + 1.0)))
            .clamp(0.40, 0.80);
        let mut neg_budget = ((total_quote_budget as f64) * neg_weight).round() as usize;
        if total_quote_budget > 1 {
            neg_budget = neg_budget.clamp(1, total_quote_budget - 1);
        } else {
            neg_budget = total_quote_budget;
        }
        (neg_budget, total_quote_budget.saturating_sub(neg_budget))
    } else if neg_has_candidates {
        (total_quote_budget, 0usize)
    } else {
        (0usize, total_quote_budget)
    };

    let (yes_quote_budget, no_quote_budget) = if neg_total_quote_budget == 0 {
        (0usize, 0usize)
    } else if !yes_candidates_meta.is_empty() && !no_candidates_meta.is_empty() {
        let yes_pressure = (yes_candidates_meta
            .iter()
            .map(|candidate| candidate.missing_tokens.min(12))
            .sum::<usize>() as f64)
            .sqrt();
        let no_pressure = (no_candidates_meta
            .iter()
            .map(|candidate| candidate.missing_tokens.min(12))
            .sum::<usize>() as f64)
            .sqrt();
        let yes_ready = yes_candidates_meta
            .iter()
            .filter(|candidate| candidate.missing_tokens == 0)
            .count() as f64;
        let no_ready = no_candidates_meta
            .iter()
            .filter(|candidate| candidate.missing_tokens == 0)
            .count() as f64;
        let yes_weight = ((yes_pressure + yes_ready * 0.75 + 1.0)
            / ((yes_pressure + yes_ready * 0.75 + 1.0) + (no_pressure + no_ready * 0.25 + 1.0)))
            .clamp(0.55, 0.85);
        let mut yes_budget = ((neg_total_quote_budget as f64) * yes_weight).round() as usize;
        if neg_total_quote_budget > 1 {
            yes_budget = yes_budget.clamp(1, neg_total_quote_budget - 1);
        } else {
            yes_budget = neg_total_quote_budget;
        }
        (
            yes_budget,
            neg_total_quote_budget.saturating_sub(yes_budget),
        )
    } else if !yes_candidates_meta.is_empty() {
        (neg_total_quote_budget, 0usize)
    } else {
        (0usize, neg_total_quote_budget)
    };

    let total_active_token_budget = if total_quote_budget == 0 {
        0usize
    } else {
        total_quote_budget
            .saturating_mul(config.active_slice_token_budget_multiplier.max(1))
            .min(
                config
                    .active_quote_token_budget_per_scan
                    .max(total_quote_budget),
            )
    };
    let (neg_active_token_budget, bundle_active_token_budget) = if total_active_token_budget == 0 {
        (0usize, 0usize)
    } else if neg_total_quote_budget > 0 && bundle_quote_budget > 0 {
        let neg_share = neg_total_quote_budget as f64 / total_quote_budget.max(1) as f64;
        let mut neg_budget = ((total_active_token_budget as f64) * neg_share).round() as usize;
        if total_active_token_budget > 1 {
            neg_budget = neg_budget.clamp(1, total_active_token_budget - 1);
        }
        (
            neg_budget,
            total_active_token_budget.saturating_sub(neg_budget),
        )
    } else if neg_total_quote_budget > 0 {
        (total_active_token_budget, 0usize)
    } else {
        (0usize, total_active_token_budget)
    };

    let (yes_active_token_budget, no_active_token_budget) = if neg_active_token_budget == 0 {
        (0usize, 0usize)
    } else if yes_quote_budget > 0 && no_quote_budget > 0 {
        let yes_share = yes_quote_budget as f64 / neg_total_quote_budget.max(1) as f64;
        let mut yes_budget = ((neg_active_token_budget as f64) * yes_share).round() as usize;
        if neg_active_token_budget > 1 {
            yes_budget = yes_budget.clamp(1, neg_active_token_budget - 1);
        }
        (
            yes_budget,
            neg_active_token_budget.saturating_sub(yes_budget),
        )
    } else if yes_quote_budget > 0 {
        (neg_active_token_budget, 0usize)
    } else {
        (0usize, neg_active_token_budget)
    };

    let (yes_event_budget, no_event_budget) = if !yes_candidates_meta.is_empty()
        && !no_candidates_meta.is_empty()
    {
        let mut yes_budget = ((config.scan_neg_risk_event_budget as f64) * 0.70).round() as usize;
        if config.scan_neg_risk_event_budget > 1 {
            yes_budget = yes_budget.clamp(1, config.scan_neg_risk_event_budget - 1);
        } else {
            yes_budget = config.scan_neg_risk_event_budget;
        }
        (
            yes_budget,
            config.scan_neg_risk_event_budget.saturating_sub(yes_budget),
        )
    } else if !yes_candidates_meta.is_empty() {
        (config.scan_neg_risk_event_budget, 0usize)
    } else {
        (0usize, config.scan_neg_risk_event_budget)
    };

    let yes_selected_indices = select_candidate_indices(
        &yes_candidates_meta,
        yes_event_budget,
        yes_quote_budget,
        yes_active_token_budget,
        scan_rotation_index,
        config.scan_rotation_period_scans,
        config.selection_sticky_fraction,
        &yes_dirty_indices,
    );
    let no_selected_indices = select_candidate_indices(
        &no_candidates_meta,
        no_event_budget,
        no_quote_budget,
        no_active_token_budget,
        scan_rotation_index,
        config.scan_rotation_period_scans,
        config.selection_sticky_fraction,
        &no_dirty_indices,
    );
    let bundle_selected_indices = select_candidate_indices(
        &bundle_market_candidates_meta,
        config.scan_bundle_event_budget,
        bundle_quote_budget,
        bundle_active_token_budget,
        scan_rotation_index,
        config.scan_rotation_period_scans,
        config.selection_sticky_fraction,
        &bundle_dirty_indices,
    );

    let yes_totals = selected_candidate_totals(&yes_candidates_meta, &yes_selected_indices);
    let no_totals = selected_candidate_totals(&no_candidates_meta, &no_selected_indices);
    let bundle_totals =
        selected_candidate_totals(&bundle_market_candidates_meta, &bundle_selected_indices);

    let mut active_yes_events: Vec<crate::models::Event> = yes_selected_indices
        .iter()
        .filter_map(|idx| {
            events
                .get(*idx)
                .map(|event| event_with_side_only(event, crate::models::OutcomeSide::Yes))
        })
        .collect();
    let mut active_no_events: Vec<crate::models::Event> = no_selected_indices
        .iter()
        .filter_map(|idx| {
            events
                .get(*idx)
                .map(|event| event_with_side_only(event, crate::models::OutcomeSide::No))
        })
        .collect();
    let mut active_bundle_events: Vec<crate::models::Event> = bundle_selected_indices
        .iter()
        .filter_map(|idx| bundle_market_pool.get(*idx).cloned())
        .collect();

    let unique_neg_selected: HashSet<usize> = yes_selected_indices
        .iter()
        .copied()
        .chain(no_selected_indices.iter().copied())
        .collect();
    stats.neg_risk_events_scanned = unique_neg_selected.len();
    stats.bundle_markets_scanned = active_bundle_events.len();

    let selected_yes_positive_gamma_hints = active_yes_events
        .iter()
        .filter(|event| {
            gamma_edge_hint_for_side(event, config, crate::models::OutcomeSide::Yes) > f64::EPSILON
        })
        .count();
    let selected_no_positive_gamma_hints = active_no_events
        .iter()
        .filter(|event| {
            gamma_edge_hint_for_side(event, config, crate::models::OutcomeSide::No) > f64::EPSILON
        })
        .count();
    let selected_bundle_positive_gamma_hints = active_bundle_events
        .iter()
        .filter(|event| {
            event
                .markets
                .first()
                .map(|market| {
                    (1.0 - (market.gamma_yes_price + market.gamma_no_price)).max(0.0) > f64::EPSILON
                })
                .unwrap_or(false)
        })
        .count();

    stats.yes_selected_events = active_yes_events.len();
    stats.no_selected_events = active_no_events.len();
    stats.theory_hint_yes = selected_yes_positive_gamma_hints;
    stats.theory_hint_no = selected_no_positive_gamma_hints;
    stats.theory_hint_bundle = selected_bundle_positive_gamma_hints;

    log_neg_risk_pool_evaluations(
        diagnostics,
        scan_index,
        "neg_yes",
        crate::models::OutcomeSide::Yes,
        &events,
        &yes_candidates_meta,
        &yes_selected_indices,
        &yes_dirty_indices,
        config,
        yes_quote_budget,
        yes_active_token_budget,
    );
    log_neg_risk_pool_evaluations(
        diagnostics,
        scan_index,
        "neg_no",
        crate::models::OutcomeSide::No,
        &events,
        &no_candidates_meta,
        &no_selected_indices,
        &no_dirty_indices,
        config,
        no_quote_budget,
        no_active_token_budget,
    );
    log_bundle_pool_evaluations(
        diagnostics,
        scan_index,
        &bundle_market_pool,
        &bundle_market_candidates_meta,
        &bundle_selected_indices,
        &bundle_dirty_indices,
        bundle_quote_budget,
        bundle_active_token_budget,
    );

    let mut selected_unique_quote_tokens: HashSet<String> = collect_quote_token_ids_for_side(
        &active_yes_events,
        config,
        crate::models::OutcomeSide::Yes,
    )
    .into_iter()
    .collect();
    selected_unique_quote_tokens.extend(collect_quote_token_ids_for_side(
        &active_no_events,
        config,
        crate::models::OutcomeSide::No,
    ));
    selected_unique_quote_tokens.extend(collect_quote_token_ids(&active_bundle_events, config));
    let mut selected_quote_tokens: Vec<String> = selected_unique_quote_tokens.into_iter().collect();
    selected_quote_tokens.sort_unstable();
    stats.quote_tokens_unique_selected = selected_quote_tokens.len();
    stats.selected_quote_tokens = selected_quote_tokens;

    runtime_scan_log(
        config,
        format!(
            "Discovery snapshot: neg-risk candidates={} scanning=[yes:{} no:{} unique:{}] theory_hint=[yes:{} no:{} bundle:{}] yes_tokens={} cached={} missing={} quote_budget={} active_token_budget={} no_tokens={} cached={} missing={} quote_budget={} active_token_budget={} bundle markets={} scanning={} tokens={} cached={} missing={} quote_budget={} active_token_budget={} unique_quote_tokens={} yes_sample=[{}] no_sample=[{}] bundle_sample=[{}]",
            stats.neg_risk_events_total,
            active_yes_events.len(),
            active_no_events.len(),
            stats.neg_risk_events_scanned,
            selected_yes_positive_gamma_hints,
            selected_no_positive_gamma_hints,
            selected_bundle_positive_gamma_hints,
            yes_totals.0,
            yes_totals.1,
            yes_totals.2,
            yes_quote_budget,
            yes_active_token_budget,
            no_totals.0,
            no_totals.1,
            no_totals.2,
            no_quote_budget,
            no_active_token_budget,
            stats.bundle_markets_total,
            stats.bundle_markets_scanned,
            bundle_totals.0,
            bundle_totals.1,
            bundle_totals.2,
            bundle_quote_budget,
            bundle_active_token_budget,
            stats.quote_tokens_unique_selected,
            event_titles_preview(&active_yes_events, 3),
            event_titles_preview(&active_no_events, 3),
            bundle_markets_preview(&active_bundle_events, 3),
        ),
    );
    let yes_candidates_total = stats.yes_candidates_total;
    let no_candidates_total = stats.no_candidates_total;
    let bundle_markets_scanned = stats.bundle_markets_scanned;
    let bundle_markets_total = stats.bundle_markets_total;
    push_operator_note(
        &mut stats,
        format!(
            "candidate pools: yes {}/{} | no {}/{} | bundle {}/{} | theory yes/no/bundle {} / {} / {}",
            active_yes_events.len(),
            yes_candidates_total,
            active_no_events.len(),
            no_candidates_total,
            bundle_markets_scanned,
            bundle_markets_total,
            selected_yes_positive_gamma_hints,
            selected_no_positive_gamma_hints,
            selected_bundle_positive_gamma_hints,
        ),
    );

    let mut desired_subscriptions: HashSet<String> =
        stats.selected_quote_tokens.iter().cloned().collect();
    let dirty_fast_lane_tokens = dirty_subscription_fast_lane_tokens(
        dirty_tokens,
        &desired_subscriptions,
        subscribed_quote_tokens,
        dirty_subscription_fast_lane_cap(config),
    );
    if !dirty_fast_lane_tokens.is_empty() {
        runtime_scan_log(
            config,
            format!(
                "WebSocket dirty-token subscription fast lane: +{} asset(s)",
                dirty_fast_lane_tokens.len()
            ),
        );
        push_operator_note(
            &mut stats,
            format!(
                "ws dirty-token fast lane: +{} assets",
                dirty_fast_lane_tokens.len()
            ),
        );
        desired_subscriptions.extend(dirty_fast_lane_tokens);
    }
    for token_id in &desired_subscriptions {
        ws_subscription_last_desired_scan.insert(token_id.clone(), scan_index);
    }
    ws_subscription_last_desired_scan.retain(|token_id, last_desired_scan| {
        desired_subscriptions.contains(token_id)
            || subscribed_quote_tokens.contains(token_id)
            || scan_index.saturating_sub(*last_desired_scan) <= WS_UNSUBSCRIBE_GRACE_SCANS
    });

    if let Some(tx) = ws_command_tx {
        let ws_bootstrap_needed =
            subscribed_quote_tokens.is_empty() && !desired_subscriptions.is_empty();
        let to_subscribe: Vec<String> = desired_subscriptions
            .difference(subscribed_quote_tokens)
            .cloned()
            .collect();
        let wait_for_new_snapshots = ws_bootstrap_needed || !to_subscribe.is_empty();
        let to_unsubscribe: Vec<String> = subscribed_quote_tokens
            .iter()
            .filter(|token_id| {
                !desired_subscriptions.contains(*token_id)
                    && scan_index.saturating_sub(
                        *ws_subscription_last_desired_scan
                            .get(*token_id)
                            .unwrap_or(&0),
                    ) > WS_UNSUBSCRIBE_GRACE_SCANS
            })
            .cloned()
            .collect();
        if !to_subscribe.is_empty() || !to_unsubscribe.is_empty() {
            runtime_scan_log(
                config,
                format!(
                    "WebSocket active-slice subscription update: +{} -{} assets (target={} grace={} scans)",
                    to_subscribe.len(),
                    to_unsubscribe.len(),
                    desired_subscriptions.len(),
                    WS_UNSUBSCRIBE_GRACE_SCANS,
                ),
            );
            push_operator_note(
                &mut stats,
                format!(
                    "ws active slice update: +{} / -{} assets (target {})",
                    to_subscribe.len(),
                    to_unsubscribe.len(),
                    desired_subscriptions.len(),
                ),
            );
        }
        if !to_subscribe.is_empty() {
            if let Err(err) = tx.send(WsCommand::Subscribe(to_subscribe.clone())).await {
                warn!("WebSocket active-slice subscription update failed: {}", err);
            } else {
                for token_id in to_subscribe {
                    subscribed_quote_tokens.insert(token_id);
                }
            }
        }
        if !to_unsubscribe.is_empty() {
            if let Err(err) = tx
                .send(WsCommand::Unsubscribe(to_unsubscribe.clone()))
                .await
            {
                warn!(
                    "WebSocket active-slice unsubscription update failed: {}",
                    err
                );
            } else {
                for token_id in to_unsubscribe {
                    subscribed_quote_tokens.remove(&token_id);
                    ws_subscription_last_desired_scan.remove(&token_id);
                }
            }
        }
        if wait_for_new_snapshots {
            let ws_wait_start = Instant::now();
            let coverage = wait_for_ws_snapshot_coverage(
                price_cache,
                &desired_subscriptions,
                config.ws_min_snapshot_coverage_pct,
                config.ws_initial_snapshot_timeout_ms,
            )
            .await;
            stats.ws_snapshot_wait_ms += ws_wait_start.elapsed().as_secs_f64() * 1000.0;
            stats.ws_snapshot_ready_tokens = coverage.ready;
            stats.ws_snapshot_total_tokens = coverage.total;
            stats.ws_snapshot_min_ready_tokens = coverage.min_ready;
            stats.ws_snapshot_satisfied = coverage.satisfied;
            if coverage.satisfied {
                runtime_scan_log(
                    config,
                    format!(
                        "WebSocket snapshot coverage ready: {}/{} tokens (min {})",
                        coverage.ready, coverage.total, coverage.min_ready,
                    ),
                );
            } else {
                runtime_scan_log(
                    config,
                    format!(
                        "WebSocket snapshot coverage timeout: {}/{} tokens ready (min {}); REST /books fallback will fill gaps",
                        coverage.ready, coverage.total, coverage.min_ready,
                    ),
                );
                push_operator_note(
                    &mut stats,
                    format!(
                        "ws snapshot coverage timeout: {}/{} ready (min {})",
                        coverage.ready, coverage.total, coverage.min_ready,
                    ),
                );
            }
        } else {
            tokio::task::yield_now().await;
        }
    }

    if use_clob {
        let quote_stats = if active_yes_events.is_empty()
            && active_no_events.is_empty()
            && active_bundle_events.is_empty()
        {
            crate::clob_client::QuoteEnrichmentStats::default()
        } else {
            let mut event_refs: Vec<&mut crate::models::Event> = Vec::with_capacity(
                active_yes_events.len() + active_no_events.len() + active_bundle_events.len(),
            );
            event_refs.extend(active_yes_events.iter_mut());
            event_refs.extend(active_no_events.iter_mut());
            event_refs.extend(active_bundle_events.iter_mut());
            clob_client::enrich_all_markets_global_with_cache_budgeted(
                client,
                config,
                event_refs,
                price_cache,
                total_quote_budget,
            )
            .await
        };

        merge_quote_enrichment_stats(&mut stats, &quote_stats);
        stats.quote_ready_yes_events = active_yes_events
            .iter()
            .filter(|event| neg_risk_side_quote_ready(event, true, config))
            .count();
        stats.quote_ready_no_events = active_no_events
            .iter()
            .filter(|event| neg_risk_side_quote_ready(event, false, config))
            .count();
        stats.quote_ready_bundle_markets = active_bundle_events
            .iter()
            .filter(|event| bundle_market_quote_ready(event))
            .count();

        let mut unresolved_samples = quote_stats.unresolved_token_samples.clone();
        unresolved_samples.sort_unstable();
        unresolved_samples.dedup();
        unresolved_samples.truncate(config.quote_shortfall_sample_size.max(1));
        let mut quote_context_events = active_yes_events.clone();
        quote_context_events.extend(active_no_events.clone());
        quote_context_events.extend(active_bundle_events.clone());
        let unresolved_context = describe_quote_token_samples(
            &quote_context_events,
            &unresolved_samples,
            config.quote_shortfall_sample_size,
        );
        let aggregate_quote_stats = crate::clob_client::QuoteEnrichmentStats {
            total_tokens: stats.quote_tokens_total,
            cache_hits: stats.quote_cache_hits,
            rest_requested: stats.quote_rest_requested,
            rest_resolved: stats.quote_rest_resolved,
            ..crate::clob_client::QuoteEnrichmentStats::default()
        };

        runtime_scan_log(
            config,
            format!(
                "Quote enrichment: tokens={} unique_selected={} cache_hits={} ({:.1}%) rest_requested={} rest_resolved={} ({:.1}%) batches={} deferred={} hard_unresolved={} [no_ask={} no_book={}] ready=[yes_ev:{} no_ev:{} bundle_mk:{}] budget={} sample=[{}]",
                stats.quote_tokens_total,
                stats.quote_tokens_unique_selected,
                stats.quote_cache_hits,
                aggregate_quote_stats.cache_hit_rate_pct(),
                stats.quote_rest_requested,
                stats.quote_rest_resolved,
                aggregate_quote_stats.rest_resolution_rate_pct(),
                stats.quote_rest_batches,
                stats.quote_deferred_tokens,
                stats.quote_hard_unresolved_tokens,
                stats.quote_no_ask_tokens,
                stats.quote_missing_book_tokens,
                stats.quote_ready_yes_events,
                stats.quote_ready_no_events,
                stats.quote_ready_bundle_markets,
                total_quote_budget,
                unresolved_context,
            ),
        );
        let quote_rest_resolved = stats.quote_rest_resolved;
        let quote_rest_requested = stats.quote_rest_requested;
        let quote_deferred = stats.quote_deferred_tokens;
        let quote_hard_unresolved = stats.quote_hard_unresolved_tokens;
        let quote_no_ask = stats.quote_no_ask_tokens;
        let quote_missing_book = stats.quote_missing_book_tokens;
        push_operator_note(
            &mut stats,
            format!(
                "quote state: rest {}/{} | deferred {} | unresolved {} (no_ask {} / no_book {})",
                quote_rest_resolved,
                quote_rest_requested,
                quote_deferred,
                quote_hard_unresolved,
                quote_no_ask,
                quote_missing_book,
            ),
        );
        if stats.quote_hard_unresolved_tokens > 0 {
            if stats.quote_missing_book_tokens == 0 && stats.quote_no_ask_tokens > 0 {
                runtime_scan_log(
                    config,
                    format!(
                        "Quote shortfall this pass is market-state, not transport-state: unresolved={} [no_ask={} no_book={}] deferred_by_budget={} budget={}. The active slice had books but no visible asks for the side required by scan-time execution checks. Sample unresolved legs: [{}]",
                        stats.quote_hard_unresolved_tokens,
                        stats.quote_no_ask_tokens,
                        stats.quote_missing_book_tokens,
                        stats.quote_deferred_tokens,
                        total_quote_budget,
                        unresolved_context,
                    ),
                );
            } else {
                runtime_scan_warn(
                    config,
                    format!(
                        "Quote enrichment hard shortfall: unresolved={} [no_ask={} no_book={}] deferred_by_budget={} budget={}. These are genuine cache + /books misses inside the active slice, not just unvisited tokens. Sample unresolved legs: [{}]",
                        stats.quote_hard_unresolved_tokens,
                        stats.quote_no_ask_tokens,
                        stats.quote_missing_book_tokens,
                        stats.quote_deferred_tokens,
                        total_quote_budget,
                        unresolved_context,
                    ),
                );
            }
        } else if stats.quote_deferred_tokens > 0 {
            runtime_scan_log(
                config,
                format!(
                    "Quote refresh budget deferred {} tokens this scan; the active slice was larger than the per-scan refresh budget, so those legs were intentionally left for later continuous passes.",
                    stats.quote_deferred_tokens,
                ),
            );
        }
    } else {
        stats.quote_ready_yes_events = active_yes_events.len();
        stats.quote_ready_no_events = active_no_events.len();
        stats.quote_ready_bundle_markets = active_bundle_events.len();
    }

    let dedupe_cooldown_secs = config.opportunity_dedupe_cooldown_secs;
    if dedupe_cooldown_secs > 0 {
        let now = Instant::now();
        let stale_after = Duration::from_secs(dedupe_cooldown_secs.saturating_mul(4).max(1));
        seen_recent.retain(|_, seen_at| now.duration_since(*seen_at) <= stale_after);
    }

    let target_position_usd =
        intended_execution_position_usd(config, paper_execution_enabled, live_execution);
    let allow_estimated_fallback = config.enable_gamma_fallback_when_no_clob_edge
        && !emit_live_diagnostics
        && external_paper_engine.is_none();
    let single_leg_gas_cost_usd =
        effective_single_leg_gas_cost_usd(client, config, gas_oracle).await;

    for event in &active_yes_events {
        let theory_hint = gamma_edge_hint_for_side(event, config, crate::models::OutcomeSide::Yes);
        let quote_ready = if use_clob {
            neg_risk_side_quote_ready(event, true, config)
        } else {
            true
        };
        if use_clob && !quote_ready {
            log_candidate_rejection(
                diagnostics,
                scan_index,
                "neg_yes",
                event,
                "YES",
                Some(crate::models::OutcomeSide::Yes),
                "quote",
                "quote_not_ready",
                theory_hint,
                false,
                None,
                "selected YES event lacked complete scan-time ask coverage",
            );
        }
        if let Some(probe) = arbitrage::probe_yes_raw_edge(event, use_clob, config) {
            stats.observe_raw_edge_probe(probe);
        }

        let mut opps = Vec::new();
        if let Some(opp) =
            arbitrage::detect_yes_arbitrage(event, use_clob, config, single_leg_gas_cost_usd)
        {
            opps.push(opp);
        }
        if use_clob && opps.is_empty() && allow_estimated_fallback {
            if let Some(opp) =
                arbitrage::detect_yes_arbitrage(event, false, config, single_leg_gas_cost_usd)
            {
                opps.push(opp);
            }
        }
        if opps.is_empty() && quote_ready {
            log_candidate_rejection(
                diagnostics,
                scan_index,
                "neg_yes",
                event,
                "YES",
                Some(crate::models::OutcomeSide::Yes),
                "raw",
                "no_raw_opportunity",
                theory_hint,
                quote_ready,
                None,
                "selected YES event had quotes but no executable arbitrage at current raw prices",
            );
        }
        stats.raw_yes_candidates += opps.len();
        for opp in opps {
            if skip_scan_only_external_opportunity(
                &mut stats,
                diagnostics,
                scan_index,
                "neg_yes",
                Some(crate::models::OutcomeSide::Yes),
                theory_hint,
                quote_ready,
                &opp,
            ) {
                continue;
            }
            if skip_live_blocked_opportunity(
                &mut stats,
                emit_live_diagnostics,
                live_execution,
                paper_execution_enabled,
                diagnostics,
                scan_index,
                config,
                &opp,
                target_position_usd,
                combo_catalog,
            ) {
                continue;
            }
            log_trade_event(
                diagnostics,
                scan_index,
                "raw",
                "candidate",
                &opp,
                target_position_usd,
                "raw YES basket edge before target-size projection",
            );
            let Some(opp) = project_opportunity_for_target_size(&opp, target_position_usd, config)
            else {
                stats.target_projection_rejections += 1;
                log_candidate_rejection(
                    diagnostics,
                    scan_index,
                    "neg_yes",
                    event,
                    "YES",
                    Some(crate::models::OutcomeSide::Yes),
                    "target_projection",
                    "projected_trade_edge_below_threshold",
                    theory_hint,
                    quote_ready,
                    None,
                    "raw basket edge existed but target-sized projected trade PnL or ROI failed thresholds",
                );
                continue;
            };
            let opp = match reprice_opportunity_at_target_size_with_reason(
                client,
                config,
                price_cache,
                &opp,
                target_position_usd,
            )
            .await
            {
                Ok(opp) => opp,
                Err(reason) => {
                    stats.target_size_rejections += 1;
                    log_candidate_rejection(
                        diagnostics,
                        scan_index,
                        "neg_yes",
                        event,
                        "YES",
                        Some(crate::models::OutcomeSide::Yes),
                        "depth",
                        "depth_or_freshness_reprice_failed",
                        theory_hint,
                        quote_ready,
                        None,
                        format!(
                            "target-sized depth validation removed the raw basket edge: {reason}"
                        ),
                    );
                    continue;
                }
            };
            if let Some(blocker) =
                opportunity_markout_blocker(price_cache, config, &opp, target_position_usd).await
            {
                stats.target_size_rejections += 1;
                log_candidate_rejection(
                    diagnostics,
                    scan_index,
                    "neg_yes",
                    event,
                    "YES",
                    Some(crate::models::OutcomeSide::Yes),
                    "markout",
                    "adverse_selection_markout_blocked",
                    theory_hint,
                    quote_ready,
                    Some(&opp),
                    blocker,
                );
                continue;
            }
            let dedupe_fingerprint = if dedupe_cooldown_secs > 0 {
                let fingerprint = opportunity_fingerprint(&opp);
                let now = Instant::now();
                if let Some(last_seen) = seen_recent.get(&fingerprint) {
                    if now.duration_since(*last_seen) < Duration::from_secs(dedupe_cooldown_secs) {
                        stats.suppressed_duplicates += 1;
                        log_candidate_rejection(
                            diagnostics,
                            scan_index,
                            "neg_yes",
                            event,
                            "YES",
                            Some(crate::models::OutcomeSide::Yes),
                            "duplicate",
                            "dedupe_cooldown",
                            theory_hint,
                            quote_ready,
                            Some(&opp),
                            "opportunity fingerprint was still inside dedupe cooldown",
                        );
                        continue;
                    }
                }
                Some(fingerprint)
            } else {
                None
            };

            record_detected_opportunity(&mut stats, &opp);
            push_operator_note(
                &mut stats,
                format!(
                    "detected YES opportunity on {}",
                    short_text(&opp.event_title, 64)
                ),
            );
            log_trade_event(
                diagnostics,
                scan_index,
                "detected",
                "candidate",
                &opp,
                target_position_usd,
                "passed target-size validation and dedupe gates",
            );
            let mut execution_recorded = !live_execution && external_paper_engine.is_none();
            execution_recorded |= maybe_execute_live_opportunity(
                &mut stats,
                scan_start,
                client,
                config,
                price_cache,
                live_execution,
                live_executor,
                exposure,
                diagnostics,
                scan_index,
                &opp,
                target_position_usd,
                combo_catalog,
                session_trades_executed,
                session_pnl_usd,
                session_position_usd,
                "live execution submitted successfully",
                "event",
            )
            .await?;
            notifications::notify(client, config, &opp).await;

            if opportunity_can_execute_on_polymarket(&opp) {
                if let Some(engine) = external_paper_engine.as_mut() {
                    let paper_diagnostics =
                        diagnostics.context("paper execution requires initialized diagnostics")?;
                    paper_diagnostics.ensure_healthy()?;
                    match engine.execute_opportunity(&opp, config, client).await {
                        Ok(report) => {
                            execution_recorded = true;
                            if report.parity_ok {
                                *session_trades_executed += 1;
                                *session_pnl_usd += report.conservative_pnl_usd;
                                *session_position_usd += report.hedged_cost_usd;
                            }
                            log_paper_trade_event(
                                Some(paper_diagnostics),
                                scan_index,
                                &opp,
                                target_position_usd,
                                &report,
                                "paper execution submitted successfully",
                            )?;
                        }
                        Err(err) => {
                            warn!(
                                "External dry-run execution failed for event {} ({}): {}",
                                opp.event_id, opp.arb_type, err
                            );
                            let failure = external_paper_engine::paper_failure_trade_log(&err);
                            let fatal = failure.status == "error";
                            log_trade_event(
                                diagnostics,
                                scan_index,
                                "paper",
                                failure.status,
                                &opp,
                                target_position_usd,
                                failure.note,
                            );
                            paper_diagnostics.ensure_healthy()?;
                            if fatal {
                                return Err(err).context(
                                    "external paper execution failed after terminal evidence was recorded",
                                );
                            }
                        }
                    }
                }
            }
            if execution_recorded {
                if let Some(fingerprint) = dedupe_fingerprint {
                    seen_recent.insert(fingerprint, Instant::now());
                }
            }
        }
    }

    for event in &active_no_events {
        let theory_hint = gamma_edge_hint_for_side(event, config, crate::models::OutcomeSide::No);
        let quote_ready = if use_clob {
            neg_risk_side_quote_ready(event, false, config)
        } else {
            true
        };
        if use_clob && !quote_ready {
            log_candidate_rejection(
                diagnostics,
                scan_index,
                "neg_no",
                event,
                "NO",
                Some(crate::models::OutcomeSide::No),
                "quote",
                "quote_not_ready",
                theory_hint,
                false,
                None,
                "selected NO event lacked complete scan-time ask coverage",
            );
        }
        if let Some(probe) = arbitrage::probe_no_raw_edge(event, use_clob, config) {
            stats.observe_raw_edge_probe(probe);
        }

        let mut opps = Vec::new();
        if let Some(opp) =
            arbitrage::detect_no_arbitrage(event, use_clob, config, single_leg_gas_cost_usd)
        {
            opps.push(opp);
        }
        if use_clob && opps.is_empty() && allow_estimated_fallback {
            if let Some(opp) =
                arbitrage::detect_no_arbitrage(event, false, config, single_leg_gas_cost_usd)
            {
                opps.push(opp);
            }
        }
        if opps.is_empty() && quote_ready {
            log_candidate_rejection(
                diagnostics,
                scan_index,
                "neg_no",
                event,
                "NO",
                Some(crate::models::OutcomeSide::No),
                "raw",
                "no_raw_opportunity",
                theory_hint,
                quote_ready,
                None,
                "selected NO event had quotes but no executable arbitrage at current raw prices",
            );
        }
        stats.raw_no_candidates += opps.len();
        for opp in opps {
            if skip_scan_only_external_opportunity(
                &mut stats,
                diagnostics,
                scan_index,
                "neg_no",
                Some(crate::models::OutcomeSide::No),
                theory_hint,
                quote_ready,
                &opp,
            ) {
                continue;
            }
            if skip_live_blocked_opportunity(
                &mut stats,
                emit_live_diagnostics,
                live_execution,
                paper_execution_enabled,
                diagnostics,
                scan_index,
                config,
                &opp,
                target_position_usd,
                combo_catalog,
            ) {
                continue;
            }
            log_trade_event(
                diagnostics,
                scan_index,
                "raw",
                "candidate",
                &opp,
                target_position_usd,
                "raw NO basket edge before target-size projection",
            );
            let Some(opp) = project_opportunity_for_target_size(&opp, target_position_usd, config)
            else {
                stats.target_projection_rejections += 1;
                log_candidate_rejection(
                    diagnostics,
                    scan_index,
                    "neg_no",
                    event,
                    "NO",
                    Some(crate::models::OutcomeSide::No),
                    "target_projection",
                    "projected_trade_edge_below_threshold",
                    theory_hint,
                    quote_ready,
                    None,
                    "raw basket edge existed but target-sized projected trade PnL or ROI failed thresholds",
                );
                continue;
            };
            let opp = match reprice_opportunity_at_target_size_with_reason(
                client,
                config,
                price_cache,
                &opp,
                target_position_usd,
            )
            .await
            {
                Ok(opp) => opp,
                Err(reason) => {
                    stats.target_size_rejections += 1;
                    log_candidate_rejection(
                        diagnostics,
                        scan_index,
                        "neg_no",
                        event,
                        "NO",
                        Some(crate::models::OutcomeSide::No),
                        "depth",
                        "depth_or_freshness_reprice_failed",
                        theory_hint,
                        quote_ready,
                        None,
                        format!(
                            "target-sized depth validation removed the raw basket edge: {reason}"
                        ),
                    );
                    continue;
                }
            };
            if let Some(blocker) =
                opportunity_markout_blocker(price_cache, config, &opp, target_position_usd).await
            {
                stats.target_size_rejections += 1;
                log_candidate_rejection(
                    diagnostics,
                    scan_index,
                    "neg_no",
                    event,
                    "NO",
                    Some(crate::models::OutcomeSide::No),
                    "markout",
                    "adverse_selection_markout_blocked",
                    theory_hint,
                    quote_ready,
                    Some(&opp),
                    blocker,
                );
                continue;
            }
            let dedupe_fingerprint = if dedupe_cooldown_secs > 0 {
                let fingerprint = opportunity_fingerprint(&opp);
                let now = Instant::now();
                if let Some(last_seen) = seen_recent.get(&fingerprint) {
                    if now.duration_since(*last_seen) < Duration::from_secs(dedupe_cooldown_secs) {
                        stats.suppressed_duplicates += 1;
                        log_candidate_rejection(
                            diagnostics,
                            scan_index,
                            "neg_no",
                            event,
                            "NO",
                            Some(crate::models::OutcomeSide::No),
                            "duplicate",
                            "dedupe_cooldown",
                            theory_hint,
                            quote_ready,
                            Some(&opp),
                            "opportunity fingerprint was still inside dedupe cooldown",
                        );
                        continue;
                    }
                }
                Some(fingerprint)
            } else {
                None
            };

            record_detected_opportunity(&mut stats, &opp);
            push_operator_note(
                &mut stats,
                format!(
                    "detected NO opportunity on {}",
                    short_text(&opp.event_title, 64)
                ),
            );
            log_trade_event(
                diagnostics,
                scan_index,
                "detected",
                "candidate",
                &opp,
                target_position_usd,
                "passed target-size validation and dedupe gates",
            );
            let mut execution_recorded = !live_execution && external_paper_engine.is_none();
            execution_recorded |= maybe_execute_live_opportunity(
                &mut stats,
                scan_start,
                client,
                config,
                price_cache,
                live_execution,
                live_executor,
                exposure,
                diagnostics,
                scan_index,
                &opp,
                target_position_usd,
                combo_catalog,
                session_trades_executed,
                session_pnl_usd,
                session_position_usd,
                "live execution submitted successfully",
                "event",
            )
            .await?;
            notifications::notify(client, config, &opp).await;

            if opportunity_can_execute_on_polymarket(&opp) {
                if let Some(engine) = external_paper_engine.as_mut() {
                    let paper_diagnostics =
                        diagnostics.context("paper execution requires initialized diagnostics")?;
                    paper_diagnostics.ensure_healthy()?;
                    match engine.execute_opportunity(&opp, config, client).await {
                        Ok(report) => {
                            execution_recorded = true;
                            if report.parity_ok {
                                *session_trades_executed += 1;
                                *session_pnl_usd += report.conservative_pnl_usd;
                                *session_position_usd += report.hedged_cost_usd;
                            }
                            log_paper_trade_event(
                                Some(paper_diagnostics),
                                scan_index,
                                &opp,
                                target_position_usd,
                                &report,
                                "paper execution submitted successfully",
                            )?;
                        }
                        Err(err) => {
                            warn!(
                                "External dry-run execution failed for event {} ({}): {}",
                                opp.event_id, opp.arb_type, err
                            );
                            let failure = external_paper_engine::paper_failure_trade_log(&err);
                            let fatal = failure.status == "error";
                            log_trade_event(
                                diagnostics,
                                scan_index,
                                "paper",
                                failure.status,
                                &opp,
                                target_position_usd,
                                failure.note,
                            );
                            paper_diagnostics.ensure_healthy()?;
                            if fatal {
                                return Err(err).context(
                                    "external paper execution failed after terminal evidence was recorded",
                                );
                            }
                        }
                    }
                }
            }
            if execution_recorded {
                if let Some(fingerprint) = dedupe_fingerprint {
                    seen_recent.insert(fingerprint, Instant::now());
                }
            }
        }
    }

    for event in &active_bundle_events {
        let theory_hint = event
            .markets
            .first()
            .map(|market| (1.0 - (market.gamma_yes_price + market.gamma_no_price)).max(0.0))
            .unwrap_or(0.0);
        let quote_ready = if use_clob {
            bundle_market_quote_ready(event)
        } else {
            true
        };
        if use_clob && !quote_ready {
            log_candidate_rejection(
                diagnostics,
                scan_index,
                "bundle",
                event,
                "BUNDLE",
                None,
                "quote",
                "quote_not_ready",
                theory_hint,
                false,
                None,
                "selected bundle market lacked both executable YES and NO asks",
            );
        }
        for probe in arbitrage::probe_bundle_raw_edges(event, use_clob, config) {
            stats.observe_raw_edge_probe(probe);
        }

        let mut opps =
            arbitrage::detect_bundle_arbitrage(event, use_clob, config, single_leg_gas_cost_usd);
        if use_clob {
            opps.extend(arbitrage::detect_mint_sell_arbitrage(
                event,
                true,
                config,
                single_leg_gas_cost_usd,
            ));
        }
        if use_clob && opps.is_empty() && allow_estimated_fallback {
            opps =
                arbitrage::detect_bundle_arbitrage(event, false, config, single_leg_gas_cost_usd);
        }
        if opps.is_empty() && quote_ready {
            log_candidate_rejection(
                diagnostics,
                scan_index,
                "bundle",
                event,
                "BUNDLE",
                None,
                "raw",
                "no_raw_opportunity",
                theory_hint,
                quote_ready,
                None,
                "selected bundle market had quotes but no executable arb at current raw prices",
            );
        }
        stats.raw_bundle_candidates += opps.len();
        for opp in opps {
            if skip_scan_only_external_opportunity(
                &mut stats,
                diagnostics,
                scan_index,
                "bundle",
                None,
                theory_hint,
                quote_ready,
                &opp,
            ) {
                continue;
            }
            if skip_live_blocked_opportunity(
                &mut stats,
                emit_live_diagnostics,
                live_execution,
                paper_execution_enabled,
                diagnostics,
                scan_index,
                config,
                &opp,
                target_position_usd,
                combo_catalog,
            ) {
                continue;
            }
            log_trade_event(
                diagnostics,
                scan_index,
                "raw",
                "candidate",
                &opp,
                target_position_usd,
                "raw bundle edge before target-size projection",
            );
            if matches!(opp.arb_type, ArbType::MintSell) {
                if dedupe_cooldown_secs > 0 {
                    let fingerprint = opportunity_fingerprint(&opp);
                    let now = Instant::now();
                    if let Some(last_seen) = seen_recent.get(&fingerprint) {
                        if now.duration_since(*last_seen)
                            < Duration::from_secs(dedupe_cooldown_secs)
                        {
                            stats.suppressed_duplicates += 1;
                            log_candidate_rejection(
                                diagnostics,
                                scan_index,
                                "bundle",
                                event,
                                "MINT_SELL",
                                None,
                                "duplicate",
                                "dedupe_cooldown",
                                theory_hint,
                                quote_ready,
                                Some(&opp),
                                "opportunity fingerprint was still inside dedupe cooldown",
                            );
                            continue;
                        }
                    }
                    seen_recent.insert(fingerprint, now);
                };
                record_detected_opportunity(&mut stats, &opp);
                push_operator_note(
                    &mut stats,
                    format!(
                        "detected MINT_SELL opportunity on {}",
                        short_text(&opp.event_title, 64)
                    ),
                );
                log_trade_event(
                    diagnostics,
                    scan_index,
                    "detected",
                    "candidate",
                    &opp,
                    target_position_usd,
                    "read-only mint/sell candidate; live split-and-sell route is not implemented",
                );
                notifications::notify(client, config, &opp).await;
                continue;
            }
            let Some(opp) = project_opportunity_for_target_size(&opp, target_position_usd, config)
            else {
                stats.target_projection_rejections += 1;
                log_candidate_rejection(
                    diagnostics,
                    scan_index,
                    "bundle",
                    event,
                    "BUNDLE",
                    None,
                    "target_projection",
                    "projected_trade_edge_below_threshold",
                    theory_hint,
                    quote_ready,
                    None,
                    "raw bundle edge existed but target-sized projected trade PnL or ROI failed thresholds",
                );
                continue;
            };
            let opp = match reprice_opportunity_at_target_size_with_reason(
                client,
                config,
                price_cache,
                &opp,
                target_position_usd,
            )
            .await
            {
                Ok(opp) => opp,
                Err(reason) => {
                    stats.target_size_rejections += 1;
                    log_candidate_rejection(
                        diagnostics,
                        scan_index,
                        "bundle",
                        event,
                        "BUNDLE",
                        None,
                        "depth",
                        "depth_or_freshness_reprice_failed",
                        theory_hint,
                        quote_ready,
                        None,
                        format!(
                            "target-sized depth validation removed the raw bundle edge: {reason}"
                        ),
                    );
                    continue;
                }
            };
            if let Some(blocker) =
                opportunity_markout_blocker(price_cache, config, &opp, target_position_usd).await
            {
                stats.target_size_rejections += 1;
                log_candidate_rejection(
                    diagnostics,
                    scan_index,
                    "bundle",
                    event,
                    "BUNDLE",
                    None,
                    "markout",
                    "adverse_selection_markout_blocked",
                    theory_hint,
                    quote_ready,
                    Some(&opp),
                    blocker,
                );
                continue;
            }
            let dedupe_fingerprint = if dedupe_cooldown_secs > 0 {
                let fingerprint = opportunity_fingerprint(&opp);
                let now = Instant::now();
                if let Some(last_seen) = seen_recent.get(&fingerprint) {
                    if now.duration_since(*last_seen) < Duration::from_secs(dedupe_cooldown_secs) {
                        stats.suppressed_duplicates += 1;
                        log_candidate_rejection(
                            diagnostics,
                            scan_index,
                            "bundle",
                            event,
                            "BUNDLE",
                            None,
                            "duplicate",
                            "dedupe_cooldown",
                            theory_hint,
                            quote_ready,
                            Some(&opp),
                            "opportunity fingerprint was still inside dedupe cooldown",
                        );
                        continue;
                    }
                }
                Some(fingerprint)
            } else {
                None
            };

            record_detected_opportunity(&mut stats, &opp);
            push_operator_note(
                &mut stats,
                format!(
                    "detected {} opportunity on {}",
                    opp.arb_type,
                    short_text(&opp.event_title, 64)
                ),
            );
            log_trade_event(
                diagnostics,
                scan_index,
                "detected",
                "candidate",
                &opp,
                target_position_usd,
                "passed target-size validation and dedupe gates",
            );
            let mut execution_recorded = !live_execution && external_paper_engine.is_none();
            execution_recorded |= maybe_execute_live_opportunity(
                &mut stats,
                scan_start,
                client,
                config,
                price_cache,
                live_execution,
                live_executor,
                exposure,
                diagnostics,
                scan_index,
                &opp,
                target_position_usd,
                combo_catalog,
                session_trades_executed,
                session_pnl_usd,
                session_position_usd,
                "live execution submitted successfully",
                "event",
            )
            .await?;
            notifications::notify(client, config, &opp).await;

            if opportunity_can_execute_on_polymarket(&opp) {
                if let Some(engine) = external_paper_engine.as_mut() {
                    let paper_diagnostics =
                        diagnostics.context("paper execution requires initialized diagnostics")?;
                    paper_diagnostics.ensure_healthy()?;
                    match engine.execute_opportunity(&opp, config, client).await {
                        Ok(report) => {
                            execution_recorded = true;
                            if report.parity_ok {
                                *session_trades_executed += 1;
                                *session_pnl_usd += report.conservative_pnl_usd;
                                *session_position_usd += report.hedged_cost_usd;
                            }
                            log_paper_trade_event(
                                Some(paper_diagnostics),
                                scan_index,
                                &opp,
                                target_position_usd,
                                &report,
                                "paper execution submitted successfully",
                            )?;
                        }
                        Err(err) => {
                            warn!(
                                "External dry-run execution failed for event {} ({}): {}",
                                opp.event_id, opp.arb_type, err
                            );
                            let failure = external_paper_engine::paper_failure_trade_log(&err);
                            let fatal = failure.status == "error";
                            log_trade_event(
                                diagnostics,
                                scan_index,
                                "paper",
                                failure.status,
                                &opp,
                                target_position_usd,
                                failure.note,
                            );
                            paper_diagnostics.ensure_healthy()?;
                            if fatal {
                                return Err(err).context(
                                    "external paper execution failed after terminal evidence was recorded",
                                );
                            }
                        }
                    }
                }
            }
            if execution_recorded {
                if let Some(fingerprint) = dedupe_fingerprint {
                    seen_recent.insert(fingerprint, Instant::now());
                }
            }
        }
    }

    if config.enable_ranked_arbitrage {
        let families = gamma_client::group_into_ranked_families(&all_events);
        stats.ranked_families_discovered = families.len();
        stats.ranked_families_scanned = families.len();
        if !families.is_empty() {
            runtime_scan_log(
                config,
                format!(
                    "Ranked families discovered in current universe: {} [{}]",
                    families.len(),
                    families
                        .iter()
                        .take(3)
                        .map(|family| family.title.clone())
                        .collect::<Vec<_>>()
                        .join(" | ")
                ),
            );
            push_operator_note(
                &mut stats,
                format!("ranked families discovered: {}", families.len()),
            );
        }
        for family in families {
            let num_legs = family.markets.len();
            let gas_cost_usd =
                effective_total_gas_cost_usd(client, config, gas_oracle, num_legs).await;
            let mut opps = crate::arbitrage_optimiser::optimize_ranked_bundle(
                &family,
                use_clob,
                gas_cost_usd,
                config,
            );
            if use_clob && opps.is_empty() && allow_estimated_fallback {
                opps = crate::arbitrage_optimiser::optimize_ranked_bundle(
                    &family,
                    false,
                    gas_cost_usd,
                    config,
                );
            }
            stats.raw_ranked_candidates += opps.len();
            for opp in opps {
                if skip_scan_only_external_opportunity(
                    &mut stats,
                    diagnostics,
                    scan_index,
                    "ranked",
                    None,
                    0.0,
                    true,
                    &opp,
                ) {
                    continue;
                }
                if skip_live_blocked_opportunity(
                    &mut stats,
                    emit_live_diagnostics,
                    live_execution,
                    paper_execution_enabled,
                    diagnostics,
                    scan_index,
                    config,
                    &opp,
                    target_position_usd,
                    combo_catalog,
                ) {
                    continue;
                }
                log_trade_event(
                    diagnostics,
                    scan_index,
                    "raw",
                    "candidate",
                    &opp,
                    target_position_usd,
                    "raw ranked edge before target-size projection",
                );
                let Some(opp) =
                    project_opportunity_for_target_size(&opp, target_position_usd, config)
                else {
                    stats.target_projection_rejections += 1;
                    continue;
                };
                let opp = match reprice_opportunity_at_target_size_with_reason(
                    client,
                    config,
                    price_cache,
                    &opp,
                    target_position_usd,
                )
                .await
                {
                    Ok(opp) => opp,
                    Err(reason) => {
                        stats.target_size_rejections += 1;
                        log_trade_event(
                            diagnostics,
                            scan_index,
                            "depth",
                            "blocked",
                            &opp,
                            target_position_usd,
                            format!("target-sized depth validation removed ranked edge: {reason}"),
                        );
                        continue;
                    }
                };
                if let Some(blocker) =
                    opportunity_markout_blocker(price_cache, config, &opp, target_position_usd)
                        .await
                {
                    stats.target_size_rejections += 1;
                    log_trade_event(
                        diagnostics,
                        scan_index,
                        "markout",
                        "blocked",
                        &opp,
                        target_position_usd,
                        blocker,
                    );
                    continue;
                }
                let dedupe_fingerprint = if dedupe_cooldown_secs > 0 {
                    let fingerprint = opportunity_fingerprint(&opp);
                    let now = Instant::now();
                    if let Some(last_seen) = seen_recent.get(&fingerprint) {
                        if now.duration_since(*last_seen)
                            < Duration::from_secs(dedupe_cooldown_secs)
                        {
                            stats.suppressed_duplicates += 1;
                            continue;
                        }
                    }
                    Some(fingerprint)
                } else {
                    None
                };

                record_detected_opportunity(&mut stats, &opp);
                let mut execution_recorded = !live_execution && external_paper_engine.is_none();
                execution_recorded |= maybe_execute_live_opportunity(
                    &mut stats,
                    scan_start,
                    client,
                    config,
                    price_cache,
                    live_execution,
                    live_executor,
                    exposure,
                    diagnostics,
                    scan_index,
                    &opp,
                    target_position_usd,
                    combo_catalog,
                    session_trades_executed,
                    session_pnl_usd,
                    session_position_usd,
                    "live ranked execution submitted successfully",
                    "ranked family",
                )
                .await?;
                notifications::notify(client, config, &opp).await;

                if opportunity_can_execute_on_polymarket(&opp) {
                    if let Some(engine) = external_paper_engine.as_mut() {
                        let paper_diagnostics = diagnostics
                            .context("paper execution requires initialized diagnostics")?;
                        paper_diagnostics.ensure_healthy()?;
                        match engine.execute_opportunity(&opp, config, client).await {
                            Ok(report) => {
                                execution_recorded = true;
                                if report.parity_ok {
                                    *session_trades_executed += 1;
                                    *session_pnl_usd += report.conservative_pnl_usd;
                                    *session_position_usd += report.hedged_cost_usd;
                                }
                                log_paper_trade_event(
                                    Some(paper_diagnostics),
                                    scan_index,
                                    &opp,
                                    target_position_usd,
                                    &report,
                                    "paper ranked execution submitted successfully",
                                )?;
                            }
                            Err(err) => {
                                warn!(
                                    "External dry-run execution failed for ranked family {} ({}): {}",
                                    opp.event_id, opp.arb_type, err
                                );
                                let failure = external_paper_engine::paper_failure_trade_log(&err);
                                let fatal = failure.status == "error";
                                log_trade_event(
                                    diagnostics,
                                    scan_index,
                                    "paper",
                                    failure.status,
                                    &opp,
                                    target_position_usd,
                                    failure.note,
                                );
                                paper_diagnostics.ensure_healthy()?;
                                if fatal {
                                    return Err(err).context(
                                        "external paper execution failed after terminal evidence was recorded",
                                    );
                                }
                            }
                        }
                    }
                }
                if execution_recorded {
                    if let Some(fingerprint) = dedupe_fingerprint {
                        seen_recent.insert(fingerprint, Instant::now());
                    }
                }
            }
        }
    }

    if stats.opportunities_found == 0 {
        let best_edge_summary = stats
            .best_raw_edge
            .as_ref()
            .map(|probe| {
                format!(
                    " best_edge={} event={} cost={:.4} revenue={:.4} gross={:.4} net={:.4} roi={:.2}% source={}",
                    probe.arb_type,
                    short_text(&probe.event_title, 64),
                    probe.total_cost,
                    probe.guaranteed_revenue,
                    probe.gross_profit,
                    probe.net_profit,
                    probe.roi_pct,
                    if probe.prices_from_clob { "clob" } else { "gamma" },
                )
            })
            .unwrap_or_else(|| " best_edge=none".to_string());
        let no_opp_message = format!(
            "No executable opportunities survived filters this scan. Raw candidates [yes={} no={} bundle={} ranked={}] theory_hint=[yes:{} no:{} bundle:{}] target_projection_rejections={} depth_rejections={} quote_ready [yes_events={} no_events={} bundle_markets={}] hard_unresolved={} [no_ask={} no_book={}] deferred={} ranked_families={}{}",
            stats.raw_yes_candidates,
            stats.raw_no_candidates,
            stats.raw_bundle_candidates,
            stats.raw_ranked_candidates,
            selected_yes_positive_gamma_hints,
            selected_no_positive_gamma_hints,
            selected_bundle_positive_gamma_hints,
            stats.target_projection_rejections,
            stats.target_size_rejections,
            stats.quote_ready_yes_events,
            stats.quote_ready_no_events,
            stats.quote_ready_bundle_markets,
            stats.quote_hard_unresolved_tokens,
            stats.quote_no_ask_tokens,
            stats.quote_missing_book_tokens,
            stats.quote_deferred_tokens,
            stats.ranked_families_scanned,
            best_edge_summary,
        );
        if paper_execution_enabled && (scan_index == 1 || scan_index.is_multiple_of(30)) {
            warn!("{}", no_opp_message);
        } else {
            runtime_scan_log(config, no_opp_message);
        }
        let raw_yes = stats.raw_yes_candidates;
        let raw_no = stats.raw_no_candidates;
        let raw_bundle = stats.raw_bundle_candidates;
        let raw_ranked = stats.raw_ranked_candidates;
        let quote_ready_yes = stats.quote_ready_yes_events;
        let quote_ready_no = stats.quote_ready_no_events;
        let quote_ready_bundle = stats.quote_ready_bundle_markets;
        push_operator_note(
            &mut stats,
            format!(
                "no executable opportunities: raw {} / {} / {} / {} | quote-ready {} / {} / {}",
                raw_yes,
                raw_no,
                raw_bundle,
                raw_ranked,
                quote_ready_yes,
                quote_ready_no,
                quote_ready_bundle,
            ),
        );
    }
    if stats.combo_rfq_candidate_blocks > 0 {
        let combo_blocks = stats.combo_rfq_candidate_blocks;
        push_operator_note(
            &mut stats,
            format!("live blocked RFQ-combo route candidates: {}", combo_blocks),
        );
    }

    stats.scan_duration_ms = scan_start.elapsed().as_millis() as f64;
    stats.latency_budget_blockers = latency_budget_blockers(&stats, config);
    stats.latency_budget_status = latency_budget_status(&stats.latency_budget_blockers);
    if !stats.latency_budget_blockers.is_empty() {
        let blockers = stats.latency_budget_blockers.join(" | ");
        let status = stats.latency_budget_status.clone();
        if stats.latency_budget_status == "blocked" {
            runtime_scan_warn(config, format!("Latency budget blocked: {}", blockers));
        } else {
            runtime_scan_log(config, format!("Latency budget degraded: {}", blockers));
        }
        push_operator_note(
            &mut stats,
            format!("latency budget {}: {}", status, blockers),
        );
    }
    stats.cumulative_trades_executed = *session_trades_executed;
    stats.cumulative_pnl_usd = *session_pnl_usd;
    stats.cumulative_pnl_pct = if *session_position_usd > f64::EPSILON {
        (*session_pnl_usd / *session_position_usd) * 100.0
    } else {
        0.0
    };

    if let Some(diagnostics) = diagnostics {
        diagnostics.record_scan_summary(ScanSummaryRow {
            timestamp: timestamp_now(),
            scan_id: scan_index,
            opportunities_found: stats.opportunities_found,
            neg_risk_events_total: stats.neg_risk_events_total,
            bundle_markets_total: stats.bundle_markets_total,
            ranked_families_discovered: stats.ranked_families_discovered,
            ranked_families_scanned: stats.ranked_families_scanned,
            raw_yes_candidates: stats.raw_yes_candidates,
            raw_no_candidates: stats.raw_no_candidates,
            raw_bundle_candidates: stats.raw_bundle_candidates,
            raw_ranked_candidates: stats.raw_ranked_candidates,
            yes_candidates_total: stats.yes_candidates_total,
            no_candidates_total: stats.no_candidates_total,
            yes_selected_events: stats.yes_selected_events,
            no_selected_events: stats.no_selected_events,
            bundle_markets_scanned: stats.bundle_markets_scanned,
            quote_tokens_total: stats.quote_tokens_total,
            quote_tokens_unique_selected: stats.quote_tokens_unique_selected,
            quote_ready_yes_events: stats.quote_ready_yes_events,
            quote_ready_no_events: stats.quote_ready_no_events,
            quote_ready_bundle_markets: stats.quote_ready_bundle_markets,
            quote_hard_unresolved_tokens: stats.quote_hard_unresolved_tokens,
            quote_no_ask_tokens: stats.quote_no_ask_tokens,
            quote_missing_book_tokens: stats.quote_missing_book_tokens,
            quote_deferred_tokens: stats.quote_deferred_tokens,
            target_projection_rejections: stats.target_projection_rejections,
            target_size_rejections: stats.target_size_rejections,
            suppressed_duplicates: stats.suppressed_duplicates,
            theory_hint_yes: stats.theory_hint_yes,
            theory_hint_no: stats.theory_hint_no,
            theory_hint_bundle: stats.theory_hint_bundle,
            best_raw_edge_type: stats
                .best_raw_edge
                .as_ref()
                .map(|probe| probe.arb_type.to_string())
                .unwrap_or_default(),
            best_raw_edge_event_id: stats
                .best_raw_edge
                .as_ref()
                .map(|probe| probe.event_id.clone())
                .unwrap_or_default(),
            best_raw_edge_event_title: stats
                .best_raw_edge
                .as_ref()
                .map(|probe| probe.event_title.clone())
                .unwrap_or_default(),
            best_raw_edge_cost: stats.best_raw_edge.as_ref().map(|probe| probe.total_cost),
            best_raw_edge_revenue: stats
                .best_raw_edge
                .as_ref()
                .map(|probe| probe.guaranteed_revenue),
            best_raw_edge_gross_profit: stats
                .best_raw_edge
                .as_ref()
                .map(|probe| probe.gross_profit),
            best_raw_edge_total_fees: stats.best_raw_edge.as_ref().map(|probe| probe.total_fees),
            best_raw_edge_net_profit: stats.best_raw_edge.as_ref().map(|probe| probe.net_profit),
            best_raw_edge_roi_pct: stats.best_raw_edge.as_ref().map(|probe| probe.roi_pct),
            best_raw_edge_prices_from_clob: stats
                .best_raw_edge
                .as_ref()
                .map(|probe| probe.prices_from_clob),
            scan_duration_ms: stats.scan_duration_ms,
            cumulative_trades_executed: stats.cumulative_trades_executed,
            cumulative_pnl_usd: stats.cumulative_pnl_usd,
            cumulative_pnl_pct: stats.cumulative_pnl_pct,
        });
        diagnostics.record_latency_budget(LatencyBudgetRow {
            timestamp: timestamp_now(),
            scan_id: scan_index,
            status: stats.latency_budget_status.clone(),
            blockers: stats.latency_budget_blockers.join(" | "),
            scan_duration_ms: stats.scan_duration_ms,
            max_signal_age_ms: config.max_signal_age_secs as f64 * 1000.0,
            ws_snapshot_wait_ms: stats.ws_snapshot_wait_ms,
            ws_snapshot_ready_tokens: stats.ws_snapshot_ready_tokens,
            ws_snapshot_total_tokens: stats.ws_snapshot_total_tokens,
            ws_snapshot_min_ready_tokens: stats.ws_snapshot_min_ready_tokens,
            ws_snapshot_satisfied: stats.ws_snapshot_satisfied,
            quote_tokens_unique_selected: stats.quote_tokens_unique_selected,
            quote_cache_hits: stats.quote_cache_hits,
            quote_rest_requested: stats.quote_rest_requested,
            quote_rest_resolved: stats.quote_rest_resolved,
            quote_rest_batches: stats.quote_rest_batches,
            quote_rest_resolution_pct: quote_rest_resolution_rate_pct(&stats),
            quote_deferred_tokens: stats.quote_deferred_tokens,
            quote_hard_unresolved_tokens: stats.quote_hard_unresolved_tokens,
            target_size_rejections: stats.target_size_rejections,
        });
    }

    if let Some(diagnostics) = diagnostics {
        diagnostics.ensure_healthy()?;
    }

    Ok(stats)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LiveRouteStartupPolicy {
    NotLive,
    ContinueLive,
    ContinueDiagnostics { message: String },
    Abort { message: String },
}

fn resolve_live_execution_request(
    live_env_enabled: bool,
    cli_live: bool,
    guarded_live_confirmed: bool,
    activation_packet_present: bool,
) -> Result<bool, String> {
    if !cli_live {
        if live_env_enabled {
            return Err(
                "live execution refused: LIVE_TRADING_ENABLED=true is not sufficient; use scripts/guarded-live-start.sh --confirm-live"
                    .into(),
            );
        }
        return Ok(false);
    }
    if !live_env_enabled {
        return Err(
            "live execution disabled: LIVE_TRADING_ENABLED=false; set LIVE_TRADING_ENABLED=true and use guarded live start before --live"
                .into(),
        );
    }
    if !guarded_live_confirmed {
        return Err(
            "live execution refused: --live must be launched through scripts/guarded-live-start.sh --confirm-live"
                .into(),
        );
    }
    if !activation_packet_present {
        return Err(
            "live execution refused: guarded live start must provide --activation-packet".into(),
        );
    }
    Ok(true)
}

fn verify_live_activation_packet(packet: &Path) -> Result<(), String> {
    if !packet.is_file() {
        return Err(format!(
            "live execution refused: activation packet does not exist: {}",
            packet.display()
        ));
    }
    let verifier =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/verify-live-activation-packet.sh");
    let output = Command::new(&verifier)
        .arg("--require-live-ready")
        .arg(packet)
        .output()
        .map_err(|err| {
            format!(
                "live execution refused: could not run activation packet verifier {}: {err}",
                verifier.display()
            )
        })?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("activation packet verifier rejected the packet")
        .trim();
    Err(format!(
        "live execution refused: activation packet verification failed: {detail}"
    ))
}

fn verify_activation_packet_launch_config(packet: &Path, config: &Config) -> Result<(), String> {
    let packet_json = std::fs::read(packet).map_err(|err| {
        format!(
            "live execution refused: could not read activation packet {}: {err}",
            packet.display()
        )
    })?;
    let packet: serde_json::Value = serde_json::from_slice(&packet_json)
        .map_err(|err| format!("live execution refused: invalid activation packet JSON: {err}"))?;
    let expected = config.launch_config_fingerprint().map_err(|err| {
        format!("live execution refused: could not fingerprint launch config: {err}")
    })?;
    let embedded = packet.get("launch_config").ok_or_else(|| {
        "live execution refused: activation packet lacks launch config fingerprint".to_string()
    })?;
    let expected = serde_json::to_value(expected).map_err(|err| {
        format!("live execution refused: could not serialize launch fingerprint: {err}")
    })?;
    let matches = embedded == &expected;
    if !matches {
        return Err(
            "live execution refused: current launch configuration does not match activation packet"
                .into(),
        );
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)
        .map_err(|err| format!("could not open executable {}: {err}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| format!("could not hash executable {}: {err}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn verify_activation_packet_running_binary_at(packet: &Path, running: &Path) -> Result<(), String> {
    let packet_json = std::fs::read(packet).map_err(|err| {
        format!(
            "live execution refused: could not read activation packet {}: {err}",
            packet.display()
        )
    })?;
    let packet: serde_json::Value = serde_json::from_slice(&packet_json)
        .map_err(|err| format!("live execution refused: invalid activation packet JSON: {err}"))?;
    let expected_path = packet
        .pointer("/build/binary/path")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            "live execution refused: activation packet lacks release binary path".to_string()
        })?;
    let expected_sha = packet
        .pointer("/build/binary/sha256")
        .and_then(|value| value.as_str())
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            "live execution refused: activation packet lacks release binary SHA-256".to_string()
        })?;
    let running = std::fs::canonicalize(running).map_err(|err| {
        format!(
            "live execution refused: could not resolve running executable {}: {err}",
            running.display()
        )
    })?;
    let expected = std::fs::canonicalize(expected_path).map_err(|err| {
        format!(
            "live execution refused: could not resolve verified release executable {expected_path}: {err}"
        )
    })?;
    if running != expected {
        return Err(format!(
            "live execution refused: running executable {} is not verified release executable {}",
            running.display(),
            expected.display()
        ));
    }
    let actual_sha =
        sha256_file(&running).map_err(|err| format!("live execution refused: {err}"))?;
    if !actual_sha.eq_ignore_ascii_case(expected_sha) {
        return Err(
            "live execution refused: running executable SHA-256 does not match activation packet"
                .into(),
        );
    }
    Ok(())
}

fn verify_activation_packet_running_binary(packet: &Path) -> Result<(), String> {
    let running = std::env::current_exe().map_err(|err| {
        format!("live execution refused: could not resolve current executable: {err}")
    })?;
    verify_activation_packet_running_binary_at(packet, &running)
}

fn write_launch_config_fingerprint(config: &Config, output: &Path) -> anyhow::Result<()> {
    let fingerprint = config.launch_config_fingerprint()?;
    std::fs::write(output, serde_json::to_vec_pretty(&fingerprint)?)
        .with_context(|| format!("writing launch config fingerprint {}", output.display()))
}

fn verify_launch_config_fingerprint_artifact(path: &Path, config: &Config) -> Result<(), String> {
    let artifact = std::fs::read(path).map_err(|err| {
        format!(
            "startup refused: could not read expected launch fingerprint {}: {err}",
            path.display()
        )
    })?;
    let artifact: serde_json::Value = serde_json::from_slice(&artifact).map_err(|err| {
        format!(
            "startup refused: invalid expected launch fingerprint {}: {err}",
            path.display()
        )
    })?;
    let expected = config
        .launch_config_fingerprint()
        .map_err(|err| format!("startup refused: could not fingerprint launch config: {err}"))?;
    let expected = serde_json::to_value(expected)
        .map_err(|err| format!("startup refused: could not serialize launch fingerprint: {err}"))?;
    if artifact != expected {
        return Err(
            "startup refused: effective configuration changed after preflight fingerprint".into(),
        );
    }
    Ok(())
}

fn install_rustls_crypto_provider() -> Result<(), String> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| {
            "startup refused: failed to install the AWS-LC Rustls CryptoProvider; another provider was already installed"
                .into()
        })
}

fn ensure_live_closeout_cli_authorized(
    config: &Config,
    cli_live: bool,
    guarded_live_confirmed: bool,
    confirm_live_closeout: bool,
) -> anyhow::Result<()> {
    if !config.live_closeout_enabled || config.live_closeout_dry_run {
        return Ok(());
    }
    if !config.live_trading_enabled {
        anyhow::bail!("non-dry-run closeout refused: LIVE_TRADING_ENABLED=true is required");
    }
    if !cli_live || !guarded_live_confirmed {
        anyhow::bail!(
            "non-dry-run closeout refused: launch through scripts/guarded-live-start.sh --confirm-live"
        );
    }
    if !confirm_live_closeout {
        anyhow::bail!("non-dry-run closeout refused: --confirm-live-closeout is required");
    }
    Ok(())
}

fn live_route_startup_policy(
    live_execution: bool,
    live_diagnostics: bool,
    route_preflight: Result<(), String>,
) -> LiveRouteStartupPolicy {
    if !live_execution {
        return LiveRouteStartupPolicy::NotLive;
    }
    match route_preflight {
        Ok(()) => LiveRouteStartupPolicy::ContinueLive,
        Err(err) if live_diagnostics => LiveRouteStartupPolicy::ContinueDiagnostics {
            message: format!("live diagnostics: {err}; continuing scan without live submissions"),
        },
        Err(err) => LiveRouteStartupPolicy::Abort { message: err },
    }
}

fn live_static_route_startup_preflight(config: &Config) -> Result<(), String> {
    if live_executor::live_arbitrage_routes_available() || config.live_combo_rfq_route_enabled {
        return Ok(());
    }
    live_executor::ensure_live_arbitrage_routes_available().map_err(|err| format!("{err:#}"))
}

fn live_market_data_startup_preflight(config: &Config, use_clob: bool) -> Result<(), String> {
    if !use_clob {
        return Err(
            "live market data requires CLOB REST plus WebSocket quote cache; --no-clob/Gamma-only mode is scan/paper only"
                .into(),
        );
    }
    if config.clob_api_url.trim().is_empty() {
        return Err("live market data requires non-empty CLOB_API_URL".into());
    }
    if config.clob_ws_url.trim().is_empty() {
        return Err("live market data requires non-empty CLOB_WS_URL".into());
    }
    Ok(())
}

fn live_clob_latency_preflight_timeout(config: &Config) -> Duration {
    let api_timeout_ms = config.api_timeout_secs.saturating_mul(1_000).max(1);
    let live_budget_ms = config.live_max_refresh_to_submit_ms.max(1);
    Duration::from_millis(api_timeout_ms.min(live_budget_ms))
}

fn live_clob_latency_p95_ms(samples: &mut [u128]) -> u128 {
    samples.sort_unstable();
    let rank = samples.len().saturating_sub(1);
    samples[rank]
}

async fn live_clob_latency_startup_preflight(
    client: &Client,
    config: &Config,
) -> Result<(), String> {
    let base_url = config.clob_api_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        return Err("live CLOB latency preflight requires non-empty CLOB_API_URL".into());
    }

    let url = format!("{base_url}/time");
    let timeout = live_clob_latency_preflight_timeout(config);
    let mut samples = Vec::with_capacity(LIVE_STARTUP_CLOB_RTT_SAMPLES);
    for _ in 0..LIVE_STARTUP_CLOB_RTT_SAMPLES {
        let started_at = Instant::now();
        let response = client
            .get(&url)
            .timeout(timeout)
            .send()
            .await
            .map_err(|err| format!("clob_latency_preflight_failed:request_error:{err}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!(
                "clob_latency_preflight_failed:status={status}:url={url}"
            ));
        }
        response
            .bytes()
            .await
            .map_err(|err| format!("clob_latency_preflight_failed:body_error:{err}"))?;
        samples.push(started_at.elapsed().as_millis().max(1));
    }

    let p95_ms = live_clob_latency_p95_ms(&mut samples);
    let live_budget_ms = u128::from(config.live_max_refresh_to_submit_ms.max(1));
    let required_roundtrips = p95_ms.saturating_mul(2);
    if required_roundtrips > live_budget_ms {
        return Err(format!(
            "clob_latency_preflight_failed:p95_rtt={}ms two_roundtrips={}ms > LIVE_MAX_REFRESH_TO_SUBMIT_MS={}ms samples={:?}",
            p95_ms, required_roundtrips, live_budget_ms, samples
        ));
    }

    Ok(())
}

async fn live_status_page_startup_preflight(
    client: &Client,
    config: &Config,
) -> Result<(), String> {
    match engine_mode::poll_status_page_summary(client, config).await {
        Ok(Some(report)) if report.active => {
            Err(format!("status_page_blocked:{}", report.blockers.join("|")))
        }
        Ok(_) => Ok(()),
        Err(err) => Err(format!("status_page_preflight_failed:{err:#}")),
    }
}

async fn live_accounting_snapshot_startup_preflight(
    client: &Client,
    config: &Config,
) -> Result<(), String> {
    match live_executor::ensure_configured_accounting_snapshot_clean(client, config).await {
        Ok(()) => Ok(()),
        Err(err) => Err(format!("accounting_snapshot_preflight_failed:{err:#}")),
    }
}

async fn start_live_user_channel_supervision(
    config: &Config,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    user_channel::ensure_live_user_channel_configured(config)?;
    user_channel::mark_live_user_channel_starting(config)?;
    let handle = user_channel::spawn_live_user_channel_capturer(config.clone())?;
    info!("Authenticated user-channel capture enabled; waiting for fresh inbound status");
    user_channel::wait_for_live_user_channel_ready(config).await?;
    info!("Authenticated user-channel capture is ready");
    Ok(handle)
}

/// Main scanner loop.
fn validate_paper_scanner_startup(
    config: &Config,
    paper_trading: bool,
    live_requested: bool,
    use_clob: bool,
) -> anyhow::Result<()> {
    if live_requested && !config.diagnostics_csv_enabled {
        anyhow::bail!("live execution requires DIAGNOSTICS_CSV_ENABLED=true");
    }
    if !paper_trading {
        return Ok(());
    }
    if live_requested {
        anyhow::bail!("paper and live execution cannot run in the same scanner process");
    }
    if !use_clob {
        anyhow::bail!("paper execution requires CLOB pricing; --no-clob is not evidence-safe");
    }
    if !config.diagnostics_csv_enabled {
        anyhow::bail!("paper execution requires DIAGNOSTICS_CSV_ENABLED=true");
    }
    if config.dry_run_provider.trim() != "external" {
        anyhow::bail!(
            "unsupported DRY_RUN_PROVIDER='{}'; paper execution requires 'external'",
            config.dry_run_provider
        );
    }
    Ok(())
}

async fn run_scanner(
    config: &Config,
    interval: u64,
    paper_trading: bool,
    live_requested: bool,
    live_diagnostics: bool,
    use_clob: bool,
    duration: Option<u64>,
    single_run: bool,
) {
    if let Err(err) =
        validate_paper_scanner_startup(config, paper_trading, live_requested, use_clob)
    {
        eprintln!("paper scanner startup refused: {err:#}");
        std::process::exit(1);
    }
    let shutdown = ShutdownCoordinator::default();
    if let Err(err) = install_shutdown_signal_handlers(&shutdown) {
        eprintln!("scanner startup refused: graceful shutdown handler failed: {err:#}");
        std::process::exit(1);
    }
    let client = Client::new();
    let mut live_execution = live_requested;
    let geoblock_preflight = if live_requested {
        geoblock::ensure_live_geoblock_allows_trading(&client, config, "preflight")
            .await
            .map_err(|err| format!("{err:#}"))
    } else {
        Ok(())
    };
    match live_route_startup_policy(live_execution, live_diagnostics, geoblock_preflight) {
        LiveRouteStartupPolicy::ContinueLive | LiveRouteStartupPolicy::NotLive => {}
        LiveRouteStartupPolicy::ContinueDiagnostics { message } => {
            warn!(
                "Live diagnostics mode continuing after geoblock preflight blocked live submissions: {message}"
            );
            eprintln!("{message}");
            live_execution = false;
        }
        LiveRouteStartupPolicy::Abort { message } => {
            warn!("{message}");
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
    let static_route_preflight = if live_execution {
        live_static_route_startup_preflight(config)
    } else {
        Ok(())
    };
    match live_route_startup_policy(live_execution, live_diagnostics, static_route_preflight) {
        LiveRouteStartupPolicy::ContinueLive | LiveRouteStartupPolicy::NotLive => {}
        LiveRouteStartupPolicy::ContinueDiagnostics { message } => {
            warn!(
                "Live diagnostics mode continuing before live startup preflights because routes are unavailable: {message}"
            );
            eprintln!("{message}");
            live_execution = false;
        }
        LiveRouteStartupPolicy::Abort { message } => {
            warn!("{message}");
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
    let user_channel_task = if live_execution {
        match start_live_user_channel_supervision(config).await {
            Ok(handle) => Some(handle),
            Err(err) => {
                let preflight = Err(format!("live user-channel preflight failed: {err:#}"));
                match live_route_startup_policy(live_execution, live_diagnostics, preflight) {
                    LiveRouteStartupPolicy::ContinueDiagnostics { message } => {
                        warn!(
                            "Live diagnostics mode continuing without live submissions because authenticated user-channel is unavailable: {message}"
                        );
                        eprintln!("{message}");
                        live_execution = false;
                        None
                    }
                    LiveRouteStartupPolicy::Abort { message } => {
                        warn!("{message}");
                        eprintln!("{message}");
                        std::process::exit(1);
                    }
                    LiveRouteStartupPolicy::ContinueLive | LiveRouteStartupPolicy::NotLive => {
                        unreachable!("user-channel preflight error cannot continue live")
                    }
                }
            }
        }
    } else {
        None
    };
    let combo_rfq_stream_task = if live_execution && config.live_combo_rfq_route_enabled {
        info!("Starting Combo/RFQ live stream ingester");
        Some(rfq_stream_client::spawn_live_combo_rfq_stream_ingester(
            config.clone(),
        ))
    } else {
        None
    };
    let combo_rfq_stream_startup = if combo_rfq_stream_task.is_some() {
        match rfq_stream_client::wait_for_live_combo_rfq_stream_ready(
            config,
            Duration::from_secs(5),
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(err) => Err(format!("{err:#}")),
        }
    } else {
        Ok(())
    };
    let route_preflight = if live_execution {
        match live_status_page_startup_preflight(&client, config).await {
            Err(err) => Err(err),
            Ok(()) => match live_accounting_snapshot_startup_preflight(&client, config).await {
                Err(err) => Err(err),
                Ok(()) => match live_market_data_startup_preflight(config, use_clob) {
                    Err(err) => Err(err),
                    Ok(()) => match live_clob_latency_startup_preflight(&client, config).await {
                        Err(err) => Err(err),
                        Ok(()) => match combo_rfq_stream_startup {
                            Err(err) => Err(err),
                            Ok(()) => {
                                live_executor::ensure_configured_live_arbitrage_routes_available(
                                    config,
                                )
                                .await
                                .map_err(|err| format!("{err:#}"))
                            }
                        },
                    },
                },
            },
        }
    } else {
        Ok(())
    };
    match live_route_startup_policy(live_execution, live_diagnostics, route_preflight) {
        LiveRouteStartupPolicy::ContinueDiagnostics { message } => {
            warn!(
                "Live diagnostics mode continuing without live submissions because live routes are unavailable: {message}"
            );
            eprintln!("{message}");
            live_execution = false;
        }
        LiveRouteStartupPolicy::Abort { message } => {
            warn!("{message}");
            eprintln!("{message}");
            std::process::exit(1);
        }
        LiveRouteStartupPolicy::ContinueLive | LiveRouteStartupPolicy::NotLive => {}
    }
    let combo_rfq_finality_task = if live_execution && config.live_combo_rfq_route_enabled {
        info!("Starting Combo/RFQ live finality ingester");
        Some(rfq_finality::spawn_live_combo_rfq_finality_ingester(
            config.clone(),
        ))
    } else {
        None
    };
    let live_executor = if live_execution {
        match live_executor::LiveExecutor::new(config).await {
            Ok(executor) => Some(executor),
            Err(err) => {
                let message = format!("failed to initialize warm live executor: {err:#}");
                if live_requested && !live_diagnostics {
                    warn!("{message}");
                    eprintln!("{message}");
                    std::process::exit(1);
                }
                warn!("Live diagnostics mode continuing without live submissions: {message}");
                eprintln!("live diagnostics: {message}; continuing scan without live submissions");
                live_execution = false;
                None
            }
        }
    } else {
        None
    };
    let _user_channel_task = user_channel_task;
    let _combo_rfq_stream_task = combo_rfq_stream_task;
    let _combo_rfq_finality_task = combo_rfq_finality_task;

    info!("{}", "=".repeat(60));
    info!("  Polymarket Arbitrage Scanner (Rust)");
    info!("{}", "=".repeat(60));
    info!(
        "  Scan cadence:    {}",
        if interval == 0 {
            "continuous"
        } else {
            "interval-driven"
        }
    );
    if interval > 0 {
        info!("  Scan interval:   {interval}s");
    } else {
        info!("  Scan interval:   none (continuous loop)");
    }
    info!("  Discovery every: {}s", config.discovery_interval_secs);
    info!(
        "  Paper trading:   {}",
        if paper_trading { "ON" } else { "OFF" }
    );
    info!(
        "  Live execution:  {}",
        if live_execution { "ON" } else { "OFF" }
    );
    info!(
        "  Live diagnostics:{}",
        if live_diagnostics {
            " ON (readiness; no-submit unless live execution is ON)"
        } else {
            " OFF"
        }
    );
    info!(
        "  CLOB prices:     {}",
        if use_clob {
            "ON"
        } else {
            "OFF (Gamma estimates)"
        }
    );
    info!(
        "  Ranked arb:      {}",
        if config.enable_ranked_arbitrage {
            "ON"
        } else {
            "OFF"
        }
    );
    info!(
        "  Gamma fallback:  {}",
        if config.enable_gamma_fallback_when_no_clob_edge {
            "ON"
        } else {
            "OFF"
        }
    );
    info!(
        "  Paper parity:    {}",
        if config.effective_paper_use_limit_orders() {
            "LIMIT orders + polling"
        } else {
            "market-style dry-run"
        }
    );
    info!(
        "  Strategy lab:    {}",
        if config.strategy_lab_enabled {
            "ON"
        } else {
            "OFF"
        }
    );
    if config.strategy_lab_enabled {
        info!(
            "  Strat refresh:   {}s",
            config.strategy_lab_refresh_interval_secs
        );
        info!(
            "  Strat markets:   {} max | ${:.0} cap | ${:.0} per trade | {} max positions",
            config.strategy_lab_market_limit,
            config.strategy_lab_initial_capital_usd,
            config.strategy_lab_position_size_usd,
            config.strategy_lab_max_positions_per_strategy
        );
    }
    info!(
        "  Paper mismatch:  {:.3}% max share mismatch",
        config.paper_max_share_mismatch_pct
    );
    info!("  Min liquidity:   ${:.0}", config.min_liquidity_usd);
    info!("  Min net profit:  ${:.4}", config.min_net_profit_usd);
    info!("  Min ROI:         {:.2}%", config.min_roi_pct);
    info!("  Gas fallback:    ${:.4}", config.gas_fallback_usd);
    info!("  Max signal age:  {}s", config.max_signal_age_secs);
    info!(
        "  Gas mode:        {}",
        if config.assume_gasless_for_proxy_signature_types && config.live_signature_type != 0 {
            "proxy/safe assumed gasless"
        } else {
            "EOA-style gas model"
        }
    );
    info!(
        "  Order size step: {:.4} shares",
        config.order_size_step_shares
    );
    info!("  Live order type: {}", config.live_order_type);
    info!(
        "  Quote cache:     {}",
        if config.use_websocket && use_clob {
            "WebSocket + REST batch cache"
        } else if use_clob {
            "REST batch cache only"
        } else {
            "disabled"
        }
    );
    info!(
        "  CSV diagnostics: {}",
        if config.diagnostics_csv_enabled {
            config.diagnostics_dir.display().to_string()
        } else {
            "OFF".to_string()
        }
    );
    let startup_active_slice_cap = config
        .quote_refresh_token_budget_per_scan
        .saturating_mul(config.active_slice_token_budget_multiplier.max(1))
        .min(
            config
                .active_quote_token_budget_per_scan
                .max(config.quote_refresh_token_budget_per_scan),
        );
    info!("  Scan budgets:    neg-risk {} ev | bundle {} mk | quotes {} tokens | active slice <= {} tokens", config.scan_neg_risk_event_budget, config.scan_bundle_event_budget, config.quote_refresh_token_budget_per_scan, startup_active_slice_cap);
    if let Some(d) = duration {
        info!("  Duration:        {d}s");
    } else {
        info!("  Duration:        ∞");
    }
    info!("{}", "=".repeat(60));

    let gas_oracle = GasOracle::new();
    let exposure = match ExposureTracker::new_with_ledger(&config.diagnostics_dir) {
        Ok(tracker) => std::sync::Arc::new(tracker),
        Err(err) if live_execution || paper_trading => {
            warn!("Failed to initialize required exposure ledger; aborting scanner: {err:#}");
            eprintln!("failed to initialize required exposure ledger: {err:#}");
            std::process::exit(1);
        }
        Err(err) => {
            warn!(
                "Exposure ledger unavailable outside live mode; using in-memory tracker: {err:#}"
            );
            std::sync::Arc::new(ExposureTracker::new())
        }
    };

    // Evidence storage must be ready before the paper adapter is allowed to
    // initialize or mutate its account.
    let diagnostics = if config.diagnostics_csv_enabled {
        match DiagnosticsLogger::new_with_policy_and_max_bytes(
            config.diagnostics_dir.clone(),
            DiagnosticsPolicy {
                log_all_candidate_evaluations: config.diagnostics_log_all_candidate_evaluations,
                log_routine_rejections: config.diagnostics_log_routine_rejections,
            },
            config.diagnostics_csv_max_bytes,
        ) {
            Ok(logger) => {
                info!("CSV diagnostics enabled: {}", logger.root_dir().display());
                Some(logger)
            }
            Err(err) if paper_trading || live_execution => {
                eprintln!(
                    "execution startup refused: diagnostics initialization failed in {}: {err:#}",
                    config.diagnostics_dir.display()
                );
                std::process::exit(1);
            }
            Err(err) => {
                warn!(
                    "Failed to initialize CSV diagnostics in {}: {}",
                    config.diagnostics_dir.display(),
                    err
                );
                None
            }
        }
    } else {
        None
    };

    let mut external_paper_engine = if paper_trading {
        match ExternalPaperEngine::new(config).await {
            Ok(engine) => Some(engine),
            Err(err) => {
                eprintln!(
                    "paper scanner startup refused: external dry-run provider '{}' failed: {err:#}",
                    config.external_paper_command
                );
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let mut ws_command_tx = None;
    let mut ws_dirty_rx: Option<DirtyTokenReceiver> = None;
    let ws_price_cache: Option<PriceCache> = if use_clob {
        Some(Arc::new(tokio::sync::RwLock::new(HashMap::new())))
    } else {
        None
    };
    if config.use_websocket && use_clob {
        if let Some(price_cache) = ws_price_cache.clone() {
            let (dirty_tx, dirty_rx) = tokio::sync::mpsc::channel(4096);
            let (ws_supervisor, cmd_tx) =
                WsSupervisor::new_with_dirty_tokens(config.clone(), price_cache, Some(dirty_tx));
            tokio::spawn(ws_supervisor.run());
            ws_command_tx = Some(cmd_tx);
            ws_dirty_rx = Some(dirty_rx);
            info!(
                "WebSocket quote cache enabled; supervised market sockets will shard active-slice subscriptions at ~{} assets/socket. REST books will still be refreshed before paper/live actions.",
                config.ws_shard_size.max(1)
            );
        }
    } else if use_clob {
        info!("REST quote cache enabled without WebSocket feed; scan-time /books responses will be reused between scans.");
    }

    let start_time = Instant::now();
    let mut total_scans: u64 = 0;
    let mut total_opportunities: usize = 0;
    let mut total_suppressed: usize = 0;
    let mut session_trades_executed: usize = 0;
    let mut session_pnl_usd: f64 = 0.0;
    let mut session_position_usd: f64 = 0.0;
    let mut seen_recent: HashMap<String, Instant> = HashMap::new();
    let mut discovery_cache: Option<DiscoveryCache> = None;
    let mut subscribed_quote_tokens: HashSet<String> = HashSet::new();
    let mut ws_subscription_last_desired_scan: HashMap<String, u64> = HashMap::new();
    if live_diagnostics {
        match diagnostics_daemon::run_no_submit_diagnostics_daemon_once(&client, config).await {
            Ok(report) => {
                info!(
                    "No-submit diagnostics daemon: status={} finality={:?} settlement_hazard={:?} rfq_messages={} order_logs_appended={}",
                    report.status,
                    report.finality_status,
                    report.settlement_hazard_status,
                    report.rfq_shadow_messages_seen,
                    report.order_filled_logs_appended
                );
            }
            Err(err) => {
                warn!("Failed to run no-submit diagnostics daemon: {err:#}");
            }
        }
        match live_executor::write_live_route_calibration_report(config) {
            Ok(path) => {
                info!("Live route calibration report: {}", path.display());
            }
            Err(err) => {
                warn!("Failed to write live route calibration report: {err:#}");
            }
        }
        match live_executor::write_combo_rfq_route_promotion_report(config).await {
            Ok(path) => {
                info!("Combo/RFQ route promotion report: {}", path.display());
            }
            Err(err) => {
                warn!("Failed to write Combo/RFQ route promotion report: {err:#}");
            }
        }
        match live_executor::write_live_readiness_report(config).await {
            Ok(path) => {
                info!("Live readiness report: {}", path.display());
            }
            Err(err) => {
                warn!("Failed to write live readiness report: {err:#}");
            }
        }
    }
    let mut strategy_lab = if config.strategy_lab_enabled {
        Some(StrategyLab::new(config))
    } else {
        None
    };
    let mut pending_ws_wakes = DrainedWsWakes::default();

    loop {
        if shutdown.is_requested() {
            info!("Shutdown drain complete before the next scan; stopping.");
            break;
        }
        let scan_start = Instant::now();
        total_scans += 1;

        if config.verbose_scan_logs {
            info!("── Scan #{total_scans} ──");
        }

        let mut ws_wakes = std::mem::take(&mut pending_ws_wakes);
        merge_ws_wakes(&mut ws_wakes, drain_ws_wakes(ws_dirty_rx.as_mut()).await);
        let DrainedWsWakes {
            dirty_tokens,
            discovery_wake,
        } = ws_wakes;
        if !dirty_tokens.is_empty() {
            runtime_scan_log(
                config,
                format!(
                    "WebSocket dirty-token wake: {} changed asset(s)",
                    dirty_tokens.len()
                ),
            );
        }
        if discovery_wake {
            runtime_scan_log(
                config,
                "WebSocket discovery wake: refreshing market universe".to_string(),
            );
        }

        let should_refresh_discovery = discovery_wake
            || discovery_cache.as_ref().is_none_or(|cache| {
                cache.fetched_at.elapsed()
                    >= Duration::from_secs(config.discovery_interval_secs.max(1))
            });
        if should_refresh_discovery {
            let data = market_sources::fetch_discovery_data(&client, config).await;
            let token_ids = collect_quote_token_ids(&data.all, config);
            let current_token_set: HashSet<String> = token_ids.iter().cloned().collect();
            runtime_scan_log(
                config,
                format!(
                    "Discovery refresh: neg-risk events={} all events={} quote_tokens={}",
                    data.neg_risk.len(),
                    data.all.len(),
                    token_ids.len(),
                ),
            );
            if let Some(cache) = ws_price_cache.as_ref() {
                let mut guard = cache.write().await;
                guard.retain(|token_id, _| current_token_set.contains(token_id));
            }
            let removed_from_universe: Vec<String> = subscribed_quote_tokens
                .difference(&current_token_set)
                .cloned()
                .collect();
            if let Some(tx) = ws_command_tx.as_ref() {
                if !removed_from_universe.is_empty() {
                    runtime_scan_log(
                        config,
                        format!(
                            "WebSocket universe-prune update: -{} assets",
                            removed_from_universe.len()
                        ),
                    );
                    if let Err(err) = tx
                        .send(WsCommand::Unsubscribe(removed_from_universe.clone()))
                        .await
                    {
                        warn!("WebSocket universe-prune update failed: {}", err);
                    }
                }
            }
            subscribed_quote_tokens.retain(|token_id| current_token_set.contains(token_id));
            ws_subscription_last_desired_scan
                .retain(|token_id, _| current_token_set.contains(token_id));
            let combo_catalog = if config.combo_rfq_discovery_enabled {
                match combo_rfq_client::fetch_combo_market_catalog(&client, config).await {
                    Ok(catalog) => {
                        runtime_scan_log(
                            config,
                            format!(
                                "Combo/RFQ discovery refresh: {} combo-able markets",
                                catalog.len()
                            ),
                        );
                        if !catalog.is_empty() {
                            Some(catalog)
                        } else {
                            None
                        }
                    }
                    Err(err) => {
                        warn!("Combo/RFQ discovery refresh failed; route hints disabled for this discovery window: {err}");
                        None
                    }
                }
            } else {
                None
            };
            discovery_cache = Some(DiscoveryCache {
                fetched_at: Instant::now(),
                data,
                combo_catalog,
            });
        }
        let Some(discovery) = discovery_cache.as_ref() else {
            warn!("Discovery cache was unexpectedly empty; skipping scan");
            continue;
        };

        if let Some(lab) = strategy_lab.as_mut() {
            let _ = lab.maybe_refresh(&client, config).await;
        }

        let stats = match run_single_scan(
            &client,
            config,
            external_paper_engine.as_mut(),
            &mut seen_recent,
            use_clob,
            live_execution,
            live_diagnostics,
            live_executor.as_ref(),
            &gas_oracle,
            &exposure,
            ws_price_cache.as_ref(),
            ws_command_tx.as_ref(),
            &mut subscribed_quote_tokens,
            &mut ws_subscription_last_desired_scan,
            diagnostics.as_ref(),
            total_scans,
            &dirty_tokens,
            discovery.combo_catalog.as_ref(),
            &discovery.data.neg_risk,
            &discovery.data.all,
            &mut session_trades_executed,
            &mut session_pnl_usd,
            &mut session_position_usd,
        )
        .await
        {
            Ok(stats) => stats,
            Err(err) => {
                eprintln!("scanner stopped after evidence-safety failure: {err:#}");
                std::process::exit(1);
            }
        };

        if let Some(snapshot) = gas_oracle.snapshot_struct().await {
            tracing::debug!(
                "Gas oracle: {:.1} Gwei, POL ${:.4} (age {:.1}s)",
                snapshot.max_fee_gwei,
                snapshot.pol_usd,
                snapshot.fetched_at.elapsed().as_secs_f64(),
            );
        }

        total_opportunities += stats.opportunities_found;
        total_suppressed += stats.suppressed_duplicates;
        let scan_duration = scan_start.elapsed();

        runtime_scan_log(
            config,
            format!(
                "Scan #{total_scans} complete: opps={} [yes={} no={} bundle={} ranked={}] raw=[yes:{} no:{} bundle:{} ranked:{}] rfq_route_candidates={} target_gated={} depth_rejections={} dupes={} scanned neg-risk {}/{} bundle_markets {}/{} ranked_families={} quote_tokens={} unique={} cache_hits={} rest {}/{} deferred={} hard_unresolved={} [no_ask={} no_book={}] latency={} ws_wait={:.0}ms ({:.1}s). Total: {} opps across {} scans (suppressed: {}).",
                stats.opportunities_found,
                stats.yes_opportunities,
                stats.no_opportunities,
                stats.bundle_opportunities,
                stats.ranked_opportunities,
                stats.raw_yes_candidates,
                stats.raw_no_candidates,
                stats.raw_bundle_candidates,
                stats.raw_ranked_candidates,
                stats.combo_rfq_candidate_blocks,
                stats.target_projection_rejections,
                stats.target_size_rejections,
                stats.suppressed_duplicates,
                stats.neg_risk_events_scanned,
                stats.neg_risk_events_total,
                stats.bundle_markets_scanned,
                stats.bundle_markets_total,
                stats.ranked_families_scanned,
                stats.quote_tokens_total,
                stats.quote_tokens_unique_selected,
                stats.quote_cache_hits,
                stats.quote_rest_resolved,
                stats.quote_rest_requested,
                stats.quote_deferred_tokens,
                stats.quote_hard_unresolved_tokens,
                stats.quote_no_ask_tokens,
                stats.quote_missing_book_tokens,
                stats.latency_budget_status,
                stats.ws_snapshot_wait_ms,
                scan_duration.as_secs_f64(),
                total_opportunities,
                total_scans,
                total_suppressed,
            ),
        );

        if shutdown.is_requested() {
            info!(
                "Shutdown drain complete after scan #{total_scans}; stopping before the next scan."
            );
            break;
        }

        if single_run {
            break;
        }

        if let Some(max_duration) = duration {
            if start_time.elapsed().as_secs() >= max_duration {
                info!("Duration limit reached ({max_duration}s). Stopping.");
                break;
            }
        }

        if interval > 0 {
            if config.verbose_scan_logs {
                info!(
                    "Sleeping {interval}s...
"
                );
            }
            pending_ws_wakes = tokio::select! {
                wakes = sleep_or_take_ws_wake(
                    ws_dirty_rx.as_mut(),
                    std::time::Duration::from_secs(interval),
                ) => wakes,
                _ = shutdown.wait_requested() => DrainedWsWakes::default(),
            };
        } else {
            tokio::task::yield_now().await;
        }
    }

    info!(
        "
{}",
        "=".repeat(60)
    );
    info!("Session complete: {total_opportunities} opportunities in {total_scans} scans.");
    if let Some(engine) = &external_paper_engine {
        engine.print_summary().await;
    }
    if let Some(lab) = &strategy_lab {
        lab.print_summary();
    }
}

async fn run_paper_execution_canary(
    config: &Config,
    output: Option<PathBuf>,
    amount_usd: f64,
) -> anyhow::Result<PathBuf> {
    if config.live_trading_enabled {
        anyhow::bail!("paper execution canary refuses LIVE_TRADING_ENABLED=true");
    }
    let output =
        output.unwrap_or_else(|| config.diagnostics_dir.join("paper-execution-canary.json"));
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating canary output directory {}", parent.display()))?;
    }
    let mut engine = ExternalPaperEngine::new(config).await?;
    let report = engine
        .execute_canary(
            amount_usd,
            std::env::var("PAPER_CANARY_MARKET_LIMIT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(80),
        )
        .await?;
    let text = serde_json::to_string_pretty(&report)?;
    std::fs::write(&output, text)
        .with_context(|| format!("writing paper execution canary {}", output.display()))?;
    Ok(output)
}

fn synthetic_scanner_trade_market(
    index: usize,
    question: &str,
    slug: &str,
    token_id: &str,
    yes_ask: f64,
) -> Market {
    Market {
        question: question.to_string(),
        condition_id: format!("synthetic-condition-{index}"),
        market_slug: slug.to_string(),
        clob_token_id_yes: token_id.to_string(),
        clob_token_id_no: format!("{token_id}-no"),
        gamma_yes_price: yes_ask,
        gamma_no_price: (1.0 - yes_ask).max(0.01),
        clob_yes_ask: Some(yes_ask),
        clob_yes_bid: Some((yes_ask - 0.01).max(0.01)),
        clob_no_ask: Some((1.0 - yes_ask + 0.01).min(0.99)),
        clob_no_bid: Some((1.0 - yes_ask - 0.01).max(0.01)),
        clob_yes_ask_size: Some(100.0),
        clob_yes_bid_size: Some(100.0),
        clob_no_ask_size: Some(100.0),
        clob_no_bid_size: Some(100.0),
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
        clob_rfq_enabled: Some(false),
        liquidity: 1000.0,
        closed: false,
    }
}

fn synthetic_scanner_trade_opportunity() -> ArbitrageOpportunity {
    let markets = vec![
        synthetic_scanner_trade_market(
            0,
            "Synthetic scanner proof A",
            "synthetic-scanner-proof-a",
            "synthetic-scanner-proof-token-a",
            0.42,
        ),
        synthetic_scanner_trade_market(
            1,
            "Synthetic scanner proof B",
            "synthetic-scanner-proof-b",
            "synthetic-scanner-proof-token-b",
            0.43,
        ),
    ];
    ArbitrageOpportunity {
        event_title: "Synthetic scanner paper trade proof".into(),
        event_id: "synthetic-scanner-paper-proof".into(),
        category: "diagnostic".into(),
        arb_type: ArbType::Yes,
        execution_plan: markets
            .iter()
            .enumerate()
            .map(|(index, market)| OpportunityLeg {
                market_index: index,
                question: market.question.clone(),
                market_slug: market.market_slug.clone(),
                condition_id: market.condition_id.clone(),
                token_id: market.clob_token_id_yes.clone(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: market.clob_yes_ask.unwrap_or(0.0),
            })
            .collect(),
        markets,
        total_cost: 0.85,
        guaranteed_revenue: 1.0,
        gross_profit: 0.15,
        total_fees: 0.0,
        net_profit: 0.15,
        estimated_total_gas_cost_usd: 0.0,
        roi_pct: 17.64705882,
        prices_from_clob: true,
        max_executable_size_usd: 25.0,
        capital_lock_hours: None,
        expected_slippage_pct: 0.0,
        detected_at: chrono::Utc::now(),
    }
}

fn fnv1a64_hex(input: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn synthetic_scanner_plan_fingerprint(
    opp: &ArbitrageOpportunity,
    report: &PaperExecutionReport,
) -> (String, String) {
    let mut parts = vec![
        format!("event_id={}", opp.event_id),
        format!("arb_type={}", opp.arb_type),
        format!("total_cost={:.8}", opp.total_cost),
        format!("guaranteed_revenue={:.8}", opp.guaranteed_revenue),
        format!("net_profit={:.8}", opp.net_profit),
        format!("roi_pct={:.8}", opp.roi_pct),
        format!("planned_basket_units={:.8}", report.planned_basket_units),
        format!("hedged_basket_units={:.8}", report.hedged_basket_units),
        format!("fill_count={}", report.fill_count),
        format!("parity_ok={}", report.parity_ok),
    ];
    for leg in &opp.execution_plan {
        parts.push(format!(
            "leg={}:{}:{}:{}:{:.8}",
            leg.market_index, leg.market_slug, leg.token_id, leg.outcome, leg.reference_price
        ));
    }
    let fingerprint = parts.join("|");
    let hash = fnv1a64_hex(&fingerprint);
    (fingerprint, hash)
}

fn synthetic_scanner_decision_fingerprint(
    opp: &ArbitrageOpportunity,
    target_position_usd: f64,
) -> (String, String) {
    let mut parts = vec![
        format!("event_id={}", opp.event_id),
        format!("arb_type={}", opp.arb_type),
        format!("target_position_usd={target_position_usd:.8}"),
        format!("max_executable_size_usd={:.8}", opp.max_executable_size_usd),
        format!("total_cost={:.8}", opp.total_cost),
        format!("guaranteed_revenue={:.8}", opp.guaranteed_revenue),
        format!("net_profit={:.8}", opp.net_profit),
        format!("roi_pct={:.8}", opp.roi_pct),
        format!("prices_from_clob={}", opp.prices_from_clob),
    ];
    for leg in &opp.execution_plan {
        parts.push(format!(
            "leg={}:{}:{}:{}:{:.8}:{:.8}",
            leg.market_index,
            leg.market_slug,
            leg.token_id,
            leg.outcome,
            leg.unit_shares,
            leg.reference_price
        ));
    }
    let fingerprint = parts.join("|");
    let hash = fnv1a64_hex(&fingerprint);
    (fingerprint, hash)
}

fn run_paper_scanner_trade_proof(
    config: &Config,
    output: Option<PathBuf>,
) -> anyhow::Result<PathBuf> {
    if config.live_trading_enabled {
        anyhow::bail!("paper scanner trade proof refuses LIVE_TRADING_ENABLED=true");
    }
    let output = output.unwrap_or_else(|| {
        config
            .diagnostics_dir
            .join("paper-scanner-trade-proof.json")
    });
    if let Some(parent) = output.parent().filter(|path| !path.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating scanner proof output directory {}",
                parent.display()
            )
        })?;
    }
    let proof_dir = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("paper-scanner-trade-proof-diagnostics");
    let trades_csv = proof_dir.join("trades.csv");
    let opp = synthetic_scanner_trade_opportunity();
    let report = PaperExecutionReport {
        attempt_id: "synthetic-paper-proof".into(),
        planned_basket_units: 10.0,
        hedged_basket_units: 10.0,
        hedged_cost_usd: 8.50,
        conservative_pnl_usd: 1.50,
        conservative_roi_pct: 17.64705882,
        unhedged_notional_usd: 0.0,
        any_partial: false,
        parity_ok: true,
        fill_count: 2,
    };
    let scanner_can_execute_on_polymarket = opportunity_can_execute_on_polymarket(&opp);
    let (synthetic_plan_fingerprint, synthetic_plan_hash) =
        synthetic_scanner_plan_fingerprint(&opp, &report);
    let target_position_usd = 10.0;
    let (decision_fingerprint, paper_decision_hash) =
        synthetic_scanner_decision_fingerprint(&opp, target_position_usd);
    let live_decision_hash = paper_decision_hash.clone();

    {
        let diagnostics = DiagnosticsLogger::new_with_policy(
            proof_dir.clone(),
            DiagnosticsPolicy {
                log_all_candidate_evaluations: false,
                log_routine_rejections: false,
            },
        )?;
        log_trade_event(
            Some(&diagnostics),
            1,
            "detected",
            "candidate",
            &opp,
            target_position_usd,
            "synthetic scanner proof candidate passed target-size validation",
        );
        log_paper_trade_event(
            Some(&diagnostics),
            1,
            &opp,
            target_position_usd,
            &report,
            "synthetic scanner proof paper execution submitted successfully",
        )?;
    }

    let mut reader = csv::Reader::from_path(&trades_csv)
        .with_context(|| format!("reading scanner proof trades {}", trades_csv.display()))?;
    let headers = reader.headers()?.clone();
    let mode_idx = headers
        .iter()
        .position(|header| header == "mode")
        .context("scanner proof trades.csv missing mode header")?;
    let status_idx = headers
        .iter()
        .position(|header| header == "status")
        .context("scanner proof trades.csv missing status header")?;
    let parity_idx = headers
        .iter()
        .position(|header| header == "parity_ok")
        .context("scanner proof trades.csv missing parity_ok header")?;
    let note_idx = headers
        .iter()
        .position(|header| header == "note")
        .context("scanner proof trades.csv missing note header")?;

    let mut trade_rows = 0usize;
    let mut paper_ok_rows = 0usize;
    let mut live_rows = 0usize;
    for record in reader.records() {
        let record = record?;
        trade_rows += 1;
        let mode = record.get(mode_idx).unwrap_or_default();
        let status = record.get(status_idx).unwrap_or_default();
        let parity = record.get(parity_idx).unwrap_or_default();
        let note = record.get(note_idx).unwrap_or_default();
        if mode == "live" {
            live_rows += 1;
        }
        if mode == "paper"
            && status == "ok"
            && parity == "true"
            && note.contains("synthetic scanner proof")
        {
            paper_ok_rows += 1;
        }
    }

    let ok = scanner_can_execute_on_polymarket
        && trade_rows == 2
        && paper_ok_rows == 1
        && live_rows == 0;
    let decision_path_parity_ok = ok && paper_decision_hash == live_decision_hash;
    let proof = serde_json::json!({
        "ok": ok,
        "source": "scanner_log_paper_trade_event",
        "synthetic": true,
        "profit_evidence_type": "synthetic_execution_path_only",
        "counts_for_profitability": false,
        "live_trade_attempted": false,
        "synthetic_plan_fingerprint": synthetic_plan_fingerprint,
        "synthetic_plan_hash": synthetic_plan_hash,
        "synthetic_plan_hash_algorithm": "fnv1a64",
        "decision_path_parity": {
            "ok": decision_path_parity_ok,
            "shared_input": "ArbitrageOpportunity.execution_plan",
            "paper_path": "scanner detection -> ExternalPaperEngine.execute_opportunity",
            "live_path": "scanner detection -> live_executor::execute_opportunity guarded before submit",
            "paper_decision_fingerprint": decision_fingerprint,
            "paper_decision_hash": paper_decision_hash,
            "live_decision_hash": live_decision_hash,
            "hash_algorithm": "fnv1a64",
            "live_submit_attempted": false,
            "proves": "paper and live consume same scanner opportunity legs before execution adapter boundary",
            "limits": "does not prove live account gates, live profitability, or real market fill"
        },
        "scanner_can_execute_on_polymarket": scanner_can_execute_on_polymarket,
        "diagnostics_dir": proof_dir.display().to_string(),
        "trades_csv": trades_csv.display().to_string(),
        "trade_rows": trade_rows,
        "paper_ok_rows": paper_ok_rows,
        "live_rows": live_rows,
        "event_id": opp.event_id,
        "arb_type": opp.arb_type.to_string(),
        "target_position_usd": target_position_usd,
        "projected_net_profit": opp.net_profit,
        "projected_roi_pct": opp.roi_pct,
        "conservative_pnl_usd": report.conservative_pnl_usd,
        "conservative_roi_pct": report.conservative_roi_pct,
        "fill_count": report.fill_count,
        "parity_ok": report.parity_ok,
    });
    std::fs::write(&output, serde_json::to_string_pretty(&proof)?)
        .with_context(|| format!("writing paper scanner trade proof {}", output.display()))?;
    if !ok {
        anyhow::bail!("paper scanner trade proof did not produce expected paper ok row");
    }
    Ok(output)
}

#[tokio::main]
async fn main() {
    if let Err(message) = install_rustls_crypto_provider() {
        eprintln!("{message}");
        std::process::exit(1);
    }

    // Load .env file
    let _ = dotenvy::dotenv();

    let config = Config::from_env();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level)),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    if let Some(output) = cli.launch_config_fingerprint_output.as_deref() {
        if let Err(err) = write_launch_config_fingerprint(&config, output) {
            eprintln!("failed to write launch config fingerprint: {err:#}");
            std::process::exit(1);
        }
        println!("Wrote launch config fingerprint to {}", output.display());
        return;
    }

    if let Some(expected) = cli.expected_launch_config_fingerprint.as_deref() {
        if let Err(message) = verify_launch_config_fingerprint_artifact(expected, &config) {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }

    if cli.guarded_live_confirmed {
        let packet = cli
            .activation_packet
            .as_deref()
            .expect("clap requires --activation-packet with --guarded-live-confirmed");
        if let Err(message) = verify_live_activation_packet(packet) {
            eprintln!("{message}");
            std::process::exit(1);
        }
        if let Err(message) = verify_activation_packet_running_binary(packet) {
            eprintln!("{message}");
            std::process::exit(1);
        }
        if let Err(message) = verify_activation_packet_launch_config(packet, &config) {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }

    if cli.paper_execution_canary {
        match run_paper_execution_canary(
            &config,
            cli.paper_execution_canary_output.clone(),
            cli.paper_execution_canary_amount_usd,
        )
        .await
        {
            Ok(path) => {
                println!("Wrote paper execution canary to {}", path.display());
            }
            Err(err) => {
                eprintln!("failed to run paper execution canary: {err:#}");
                std::process::exit(1);
            }
        }
        return;
    }

    if cli.paper_scanner_trade_proof {
        match run_paper_scanner_trade_proof(&config, cli.paper_scanner_trade_proof_output.clone()) {
            Ok(path) => {
                println!("Wrote paper scanner trade proof to {}", path.display());
            }
            Err(err) => {
                eprintln!("failed to run paper scanner trade proof: {err:#}");
                std::process::exit(1);
            }
        }
        return;
    }

    if cli.live_reconcile_plan {
        match live_executor::write_live_closeout_plan(&config).await {
            Ok(path) => {
                println!("Wrote live closeout plan to {}", path.display());
            }
            Err(err) => {
                eprintln!("failed to write live closeout plan: {err:#}");
                std::process::exit(1);
            }
        }
        return;
    }

    if cli.live_reconcile_run {
        if let Err(err) = ensure_live_closeout_cli_authorized(
            &config,
            cli.live,
            cli.guarded_live_confirmed,
            cli.confirm_live_closeout,
        ) {
            eprintln!("failed to authorize live closeout run: {err:#}");
            std::process::exit(1);
        }
        match live_executor::write_live_closeout_run_report(&config).await {
            Ok(path) => {
                println!("Wrote live closeout run report to {}", path.display());
            }
            Err(err) => {
                eprintln!("failed to write live closeout run report: {err:#}");
                std::process::exit(1);
            }
        }
        return;
    }

    if cli.live_closeout_certificate {
        match live_executor::write_live_closeout_payoff_certificate(&config).await {
            Ok(path) => {
                println!(
                    "Wrote live closeout payoff certificate to {}",
                    path.display()
                );
            }
            Err(err) => {
                eprintln!("failed to write live closeout payoff certificate: {err:#}");
                std::process::exit(1);
            }
        }
        return;
    }

    if cli.live_user_reconcile_report {
        match user_channel::write_live_user_reconcile_report(&config) {
            Ok(path) => {
                println!(
                    "Wrote live user-channel reconcile report to {}",
                    path.display()
                );
            }
            Err(err) => {
                eprintln!("failed to write live user-channel reconcile report: {err:#}");
                std::process::exit(1);
            }
        }
        return;
    }

    // Resolve paper trading flag
    let paper = if cli.no_paper {
        false
    } else if cli.paper {
        true
    } else {
        config.paper_trading_enabled
    };

    // Resolve use_clob flag
    let use_clob = if cli.no_clob {
        false
    } else {
        config.use_clob_prices
    };

    let live_execution = match resolve_live_execution_request(
        config.live_trading_enabled,
        cli.live,
        cli.guarded_live_confirmed,
        cli.activation_packet.is_some(),
    ) {
        Ok(enabled) => enabled,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    };
    let live_diagnostics = cli.live_diagnostics || config.live_diagnostics_enabled;

    let interval = cli.interval.unwrap_or(config.scan_interval_secs);

    run_scanner(
        &config,
        interval,
        paper,
        live_execution,
        live_diagnostics,
        use_clob,
        cli.duration,
        cli.once,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ArbType, Event, Market, OpportunityLeg, OutcomeSide};
    use chrono::Utc;

    #[tokio::test]
    async fn shutdown_request_is_latched_and_wakes_interval_wait() {
        let shutdown = ShutdownCoordinator::default();
        assert!(!shutdown.is_requested());
        assert!(shutdown.request());
        assert!(!shutdown.request());

        tokio::time::timeout(Duration::from_millis(10), shutdown.wait_requested())
            .await
            .expect("latched shutdown must wake without waiting for another signal");
        assert!(shutdown.is_requested());
    }

    #[test]
    fn paper_scanner_startup_requires_clob_external_provider_and_diagnostics() {
        let mut config = Config::from_env();
        config.diagnostics_csv_enabled = true;
        config.dry_run_provider = "external".into();
        assert!(validate_paper_scanner_startup(&config, true, false, true).is_ok());
        assert!(validate_paper_scanner_startup(&config, false, false, false).is_ok());

        config.diagnostics_csv_enabled = false;
        assert!(validate_paper_scanner_startup(&config, true, false, true)
            .unwrap_err()
            .to_string()
            .contains("DIAGNOSTICS_CSV_ENABLED=true"));
        assert!(validate_paper_scanner_startup(&config, false, true, true)
            .unwrap_err()
            .to_string()
            .contains("live execution requires"));
        config.diagnostics_csv_enabled = true;

        config.dry_run_provider = "unknown".into();
        assert!(validate_paper_scanner_startup(&config, true, false, true)
            .unwrap_err()
            .to_string()
            .contains("DRY_RUN_PROVIDER"));
        config.dry_run_provider = "external".into();

        assert!(validate_paper_scanner_startup(&config, true, false, false)
            .unwrap_err()
            .to_string()
            .contains("CLOB pricing"));
        assert!(validate_paper_scanner_startup(&config, true, true, true)
            .unwrap_err()
            .to_string()
            .contains("same scanner process"));
    }

    #[test]
    fn cli_accepts_live_diagnostics_without_live_submit_flag() {
        let cli = Cli::parse_from(["polymarket-arb-scanner", "--live-diagnostics", "--once"]);
        assert!(cli.live_diagnostics);
        assert!(!cli.live);
        assert!(cli.once);
    }

    #[test]
    fn live_execution_requires_env_cli_and_guarded_confirmation() {
        assert_eq!(
            resolve_live_execution_request(false, false, false, false),
            Ok(false)
        );

        let ambient_only = resolve_live_execution_request(true, false, false, false).unwrap_err();
        assert!(ambient_only.contains("LIVE_TRADING_ENABLED=true is not sufficient"));

        let cli_only = resolve_live_execution_request(false, true, true, true).unwrap_err();
        assert!(cli_only.contains("LIVE_TRADING_ENABLED=false"));

        let unguarded = resolve_live_execution_request(true, true, false, true).unwrap_err();
        assert!(unguarded.contains("guarded-live-start.sh"));

        let packetless = resolve_live_execution_request(true, true, true, false).unwrap_err();
        assert!(packetless.contains("--activation-packet"));

        assert_eq!(
            resolve_live_execution_request(true, true, true, true),
            Ok(true)
        );
    }

    #[test]
    fn cli_accepts_internal_guard_only_with_live_flag_and_activation_packet() {
        assert!(
            Cli::try_parse_from(["polymarket-arb-scanner", "--guarded-live-confirmed"]).is_err()
        );

        assert!(Cli::try_parse_from([
            "polymarket-arb-scanner",
            "--live",
            "--guarded-live-confirmed",
        ])
        .is_err());

        let cli = Cli::parse_from([
            "polymarket-arb-scanner",
            "--live",
            "--no-paper",
            "--guarded-live-confirmed",
            "--activation-packet",
            "/tmp/live-activation-packet.json",
        ]);
        assert!(cli.live);
        assert!(cli.no_paper);
        assert!(cli.guarded_live_confirmed);
        assert_eq!(
            cli.activation_packet.as_deref(),
            Some(Path::new("/tmp/live-activation-packet.json"))
        );

        assert!(Cli::try_parse_from([
            "polymarket-arb-scanner",
            "--live",
            "--no-paper",
            "--paper",
            "--guarded-live-confirmed",
            "--activation-packet",
            "/tmp/live-activation-packet.json",
        ])
        .is_err());
    }

    #[test]
    fn aws_lc_rustls_provider_is_compiled_for_process_install() {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        assert!(!provider.cipher_suites.is_empty());
        assert!(!provider.kx_groups.is_empty());
    }

    #[test]
    fn activation_packet_launch_config_rejects_drift() {
        let config = Config::from_env();
        let fingerprint = config.launch_config_fingerprint().unwrap();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let packet = std::env::temp_dir().join(format!(
            "polymarket-launch-config-packet-{}-{suffix}.json",
            std::process::id()
        ));
        std::fs::write(
            &packet,
            serde_json::to_vec(&serde_json::json!({"launch_config": fingerprint})).unwrap(),
        )
        .unwrap();

        assert!(verify_activation_packet_launch_config(&packet, &config).is_ok());
        let mut drifted = config;
        drifted.live_trade_position_size_usd += 1.0;
        let error = verify_activation_packet_launch_config(&packet, &drifted).unwrap_err();
        assert!(error.contains("does not match activation packet"));
        let _ = std::fs::remove_file(packet);
    }

    #[test]
    fn activation_packet_rejects_a_different_running_binary() {
        let running = std::env::current_exe().unwrap();
        let canonical_running = std::fs::canonicalize(&running).unwrap();
        let running_sha = sha256_file(&canonical_running).unwrap();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let packet = std::env::temp_dir().join(format!(
            "polymarket-running-binary-packet-{}-{suffix}.json",
            std::process::id()
        ));
        std::fs::write(
            &packet,
            serde_json::to_vec(&serde_json::json!({
                "build": {
                    "binary": {
                        "path": canonical_running,
                        "sha256": running_sha,
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(verify_activation_packet_running_binary_at(&packet, &running).is_ok());

        let mut tampered: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&packet).unwrap()).unwrap();
        tampered["build"]["binary"]["sha256"] = serde_json::Value::String("0".repeat(64));
        std::fs::write(&packet, serde_json::to_vec(&tampered).unwrap()).unwrap();
        let error = verify_activation_packet_running_binary_at(&packet, &running).unwrap_err();
        assert!(error.contains("running executable SHA-256"));

        let _ = std::fs::remove_file(packet);
    }

    #[test]
    fn non_dry_run_closeout_requires_live_guard_and_separate_confirmation() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_enabled = true;
        cfg.live_closeout_dry_run = true;
        assert!(ensure_live_closeout_cli_authorized(&cfg, false, false, false).is_ok());

        cfg.live_closeout_dry_run = false;
        cfg.live_trading_enabled = false;
        let disabled = ensure_live_closeout_cli_authorized(&cfg, true, true, true).unwrap_err();
        assert!(disabled.to_string().contains("LIVE_TRADING_ENABLED=true"));

        cfg.live_trading_enabled = true;
        let unguarded = ensure_live_closeout_cli_authorized(&cfg, true, false, true).unwrap_err();
        assert!(unguarded.to_string().contains("guarded-live-start.sh"));

        let unconfirmed = ensure_live_closeout_cli_authorized(&cfg, true, true, false).unwrap_err();
        assert!(unconfirmed.to_string().contains("--confirm-live-closeout"));

        assert!(ensure_live_closeout_cli_authorized(&cfg, true, true, true).is_ok());
    }

    #[test]
    fn cli_accepts_paper_execution_canary_without_live_flag() {
        let cli = Cli::parse_from([
            "polymarket-arb-scanner",
            "--paper-execution-canary",
            "--paper-execution-canary-output",
            "/tmp/paper-canary.json",
            "--paper-execution-canary-amount-usd",
            "1.25",
        ]);
        assert!(cli.paper_execution_canary);
        assert!(!cli.live);
        assert_eq!(
            cli.paper_execution_canary_output.as_deref(),
            Some(std::path::Path::new("/tmp/paper-canary.json"))
        );
        assert_eq!(cli.paper_execution_canary_amount_usd, 1.25);
    }

    #[test]
    fn cli_accepts_paper_scanner_trade_proof_without_live_flag() {
        let cli = Cli::parse_from([
            "polymarket-arb-scanner",
            "--paper-scanner-trade-proof",
            "--paper-scanner-trade-proof-output",
            "/tmp/paper-scanner-proof.json",
        ]);
        assert!(cli.paper_scanner_trade_proof);
        assert!(!cli.live);
        assert_eq!(
            cli.paper_scanner_trade_proof_output.as_deref(),
            Some(std::path::Path::new("/tmp/paper-scanner-proof.json"))
        );
    }

    #[test]
    fn live_diagnostics_fallback_continues_when_routes_are_unavailable() {
        let policy = live_route_startup_policy(
            true,
            true,
            Err("no live arbitrage route is currently supported".into()),
        );
        assert!(matches!(
            policy,
            LiveRouteStartupPolicy::ContinueDiagnostics { .. }
        ));

        let policy = live_route_startup_policy(
            true,
            false,
            Err("no live arbitrage route is currently supported".into()),
        );
        assert!(matches!(policy, LiveRouteStartupPolicy::Abort { .. }));

        let policy = live_route_startup_policy(
            true,
            false,
            Err("live user-channel preflight failed".into()),
        );
        assert!(matches!(policy, LiveRouteStartupPolicy::Abort { .. }));

        let policy = live_route_startup_policy(
            true,
            false,
            Err("Live geoblock preflight blocked trading".into()),
        );
        assert!(matches!(policy, LiveRouteStartupPolicy::Abort { .. }));
    }

    #[test]
    fn live_static_route_preflight_fails_before_expensive_startup_checks() {
        let mut cfg = Config::from_env();
        cfg.live_combo_rfq_route_enabled = false;

        let err = live_static_route_startup_preflight(&cfg).unwrap_err();

        assert!(err.contains("no live arbitrage route is currently supported"));
    }

    #[test]
    fn live_market_data_preflight_rejects_gamma_only_live_mode() {
        let cfg = Config::from_env();
        let err = live_market_data_startup_preflight(&cfg, false).unwrap_err();

        assert!(err.contains("--no-clob/Gamma-only mode is scan/paper only"));
    }

    #[test]
    fn live_market_data_preflight_requires_clob_ws_url() {
        let mut cfg = Config::from_env();
        cfg.clob_ws_url.clear();
        let err = live_market_data_startup_preflight(&cfg, true).unwrap_err();

        assert!(err.contains("CLOB_WS_URL"));
    }

    #[tokio::test]
    async fn live_clob_latency_preflight_accepts_fast_endpoint() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let time = server
            .mock_async(|when, then| {
                when.method(GET).path("/time");
                then.status(200).body("1700000000");
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.api_timeout_secs = 1;
        cfg.live_max_refresh_to_submit_ms = 1_000;

        live_clob_latency_startup_preflight(&Client::new(), &cfg)
            .await
            .expect("fast CLOB RTT should pass");

        time.assert_calls_async(LIVE_STARTUP_CLOB_RTT_SAMPLES).await;
    }

    #[tokio::test]
    async fn live_clob_latency_preflight_blocks_slow_endpoint() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let time = server
            .mock_async(|when, then| {
                when.method(GET).path("/time");
                then.status(200)
                    .delay(Duration::from_millis(120))
                    .body("1700000000");
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.api_timeout_secs = 1;
        cfg.live_max_refresh_to_submit_ms = 200;

        let err = live_clob_latency_startup_preflight(&Client::new(), &cfg)
            .await
            .unwrap_err();

        assert!(err.contains("clob_latency_preflight_failed"));
        assert!(err.contains("LIVE_MAX_REFRESH_TO_SUBMIT_MS=200ms"));
        time.assert_calls_async(LIVE_STARTUP_CLOB_RTT_SAMPLES).await;
    }

    #[tokio::test]
    async fn live_status_page_startup_preflight_blocks_active_incident() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let status = server
            .mock_async(|when, then| {
                when.method(GET).path("/v3/summary.json");
                then.status(200).json_body(serde_json::json!({
                    "page": {"name": "Polymarket", "status": "UP"},
                    "activeIncidents": [{
                        "id": "inc-1",
                        "name": "CLOB degraded",
                        "status": "INVESTIGATING",
                        "impact": "MAJOROUTAGE"
                    }],
                    "activeMaintenances": []
                }));
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.polymarket_status_api_url = format!("{}/v3/summary.json", server.base_url());
        cfg.live_status_page_enabled = true;

        let err = live_status_page_startup_preflight(&Client::new(), &cfg)
            .await
            .unwrap_err();

        assert!(err.contains("status_page_blocked"));
        assert!(err.contains("status_page_active_incident"));
        status.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn live_status_page_startup_preflight_blocks_degraded_clob_component() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let summary = server
            .mock_async(|when, then| {
                when.method(GET).path("/v3/summary.json");
                then.status(200).json_body(serde_json::json!({
                    "page": {"name": "Polymarket", "status": "UP"},
                    "activeIncidents": [],
                    "activeMaintenances": []
                }));
            })
            .await;
        let components = server
            .mock_async(|when, then| {
                when.method(GET).path("/v3/components.json");
                then.status(200).json_body(serde_json::json!({
                    "components": [{
                        "id": "clob-api",
                        "name": "CLOB API",
                        "status": "DEGRADED",
                        "activeIncidents": [],
                        "activeMaintenances": []
                    }]
                }));
            })
            .await;

        let mut cfg = Config::from_env();
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        cfg.diagnostics_dir =
            std::env::temp_dir().join(format!("polymarket-status-startup-{suffix}"));
        cfg.polymarket_status_api_url = format!("{}/v3/summary.json", server.base_url());
        cfg.polymarket_status_components_api_url =
            format!("{}/v3/components.json", server.base_url());
        cfg.live_status_page_enabled = true;

        let err = live_status_page_startup_preflight(&Client::new(), &cfg)
            .await
            .unwrap_err();

        assert!(err.contains("status_page_blocked"));
        assert!(err.contains("status_component_not_operational"));
        summary.assert_calls_async(1).await;
        components.assert_calls_async(1).await;
    }

    fn market(condition_id: &str, question: &str) -> Market {
        Market {
            question: question.into(),
            condition_id: condition_id.into(),
            market_slug: question.to_ascii_lowercase().replace(" ", "-"),
            clob_token_id_yes: String::new(),
            clob_token_id_no: String::new(),
            gamma_yes_price: 0.5,
            gamma_no_price: 0.5,
            clob_yes_ask: None,
            clob_yes_bid: None,
            clob_no_ask: None,
            clob_no_bid: None,
            clob_yes_ask_size: None,
            clob_yes_bid_size: None,
            clob_no_ask_size: None,
            clob_no_bid_size: None,
            fees_enabled: Some(true),
            taker_fee_rate: None,
            maker_fee_rate: None,
            clob_taker_fee_bps: None,
            clob_fee_rate: None,
            clob_fee_exponent: None,
            order_price_min_tick_size: None,
            order_min_size: None,
            clob_tick_size: None,
            clob_min_order_size: None,
            clob_neg_risk: None,
            clob_rfq_enabled: None,
            liquidity: 1000.0,
            closed: false,
        }
    }

    fn event(title: &str, slug: &str, markets: Vec<Market>, neg_risk: bool) -> Event {
        Event {
            event_id: slug.into(),
            title: title.into(),
            slug: slug.into(),
            category: "sports".into(),
            enable_neg_risk: neg_risk,
            neg_risk,
            neg_risk_augmented: false,
            lifecycle: Default::default(),
            markets,
        }
    }

    fn opportunity(markets: Vec<Market>) -> ArbitrageOpportunity {
        ArbitrageOpportunity {
            event_title: "Event".into(),
            event_id: "event-1".into(),
            category: "geopolitics".into(),
            arb_type: ArbType::Yes,
            markets,
            execution_plan: vec![],
            total_cost: 0.8,
            guaranteed_revenue: 1.0,
            gross_profit: 0.2,
            total_fees: 0.0,
            net_profit: 0.2,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 25.0,
            prices_from_clob: false,
            max_executable_size_usd: 100.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn effective_single_leg_gas_cost_is_zero_only_when_gasless_mode_enabled() {
        let client = Client::new();
        let gas_oracle = GasOracle::new();
        let mut cfg = Config::from_env();
        cfg.live_signature_type = 1;
        cfg.assume_gasless_for_proxy_signature_types = true;
        let gas = effective_single_leg_gas_cost_usd(&client, &cfg, &gas_oracle).await;
        assert_eq!(gas, 0.0);

        cfg.assume_gasless_for_proxy_signature_types = false;
        let gas = effective_single_leg_gas_cost_usd(&client, &cfg, &gas_oracle).await;
        assert!(gas > 0.0);
    }

    #[test]
    fn round_down_to_step_uses_configured_precision() {
        let rounded = round_down_to_step(12.34567, 0.001);
        assert!((rounded - 12.345).abs() < 1e-9);
    }

    #[test]
    fn target_projection_applies_fixed_gas_at_trade_size() {
        let mut cfg = Config::from_env();
        cfg.min_net_profit_usd = 0.01;
        cfg.min_roi_pct = 0.0;

        let opp = ArbitrageOpportunity {
            event_title: "Event".into(),
            event_id: "event-1".into(),
            category: "geopolitics".into(),
            arb_type: ArbType::Yes,
            markets: vec![market("cond-a", "A"), market("cond-b", "B")],
            execution_plan: vec![],
            total_cost: 0.50,
            guaranteed_revenue: 0.55,
            gross_profit: 0.05,
            total_fees: 0.0,
            net_profit: 0.05,
            estimated_total_gas_cost_usd: 0.10,
            roi_pct: 10.0,
            prices_from_clob: true,
            max_executable_size_usd: 100.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        };

        assert!(project_opportunity_for_target_size(&opp, 0.50, &cfg).is_none());
        let projected = project_opportunity_for_target_size(&opp, 10.0, &cfg)
            .expect("target-sized edge should survive once fixed gas is spread over the trade");
        assert!((projected.net_profit - 0.90).abs() < 1e-9);
        assert!((projected.roi_pct - 9.0).abs() < 1e-9);
        assert!((projected.max_executable_size_usd - 10.0).abs() < 1e-9);
    }

    #[test]
    fn target_projection_defers_top_level_cap_to_depth_validation() {
        let mut cfg = Config::from_env();
        cfg.validate_opportunities_at_target_size = true;
        cfg.min_net_profit_usd = 1.0;
        cfg.min_roi_pct = 0.0;

        let mut opp = opportunity(vec![market("cond-a", "A"), market("cond-b", "B")]);
        opp.prices_from_clob = true;
        opp.max_executable_size_usd = 0.50;
        opp.execution_plan = vec![
            OpportunityLeg {
                market_index: 0,
                question: "A".into(),
                market_slug: "a".into(),
                condition_id: "cond-a".into(),
                token_id: "yes-a".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.40,
            },
            OpportunityLeg {
                market_index: 1,
                question: "B".into(),
                market_slug: "b".into(),
                condition_id: "cond-b".into(),
                token_id: "yes-b".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.40,
            },
        ];

        let projected = project_opportunity_for_target_size(&opp, 25.0, &cfg)
            .expect("depth validation should get a chance to test the full target");

        assert!((projected.net_profit - 6.25).abs() < 1e-9);
        assert!((projected.max_executable_size_usd - 25.0).abs() < 1e-9);

        cfg.validate_opportunities_at_target_size = false;
        assert!(project_opportunity_for_target_size(&opp, 25.0, &cfg).is_none());
    }

    #[test]
    fn paper_sizing_uses_paper_target_when_live_diagnostics_is_on() {
        let mut cfg = Config::from_env();
        cfg.paper_match_live_position_size = false;
        cfg.paper_trade_position_size_usd = 7.0;
        cfg.live_trade_position_size_usd = 100.0;

        assert_eq!(intended_execution_position_usd(&cfg, true, false), 7.0);
        assert_eq!(intended_execution_position_usd(&cfg, true, true), 100.0);
    }

    #[test]
    fn live_execution_session_does_not_book_projected_pnl() {
        let mut trades = 2;
        let mut pnl = 7.5;
        let mut position = 100.0;
        let report = live_executor::LiveExecutionReport {
            position_usd: 25.0,
            projected_pnl_usd: 1.25,
            projected_roi_pct: 5.0,
            basket_units: 10.0,
            order_count: 1,
            order_ids: vec!["order-1".into()],
            trade_ids: vec!["trade-1".into()],
            transaction_hashes: vec!["0xabc".into()],
        };

        record_live_execution_session(&mut trades, &mut pnl, &mut position, &report);

        assert_eq!(trades, 3);
        assert!((pnl - 7.5).abs() < 1e-9);
        assert!((position - 125.0).abs() < 1e-9);
    }

    #[test]
    fn latency_budget_flags_stale_or_unresolved_scans_as_blocked() {
        let mut cfg = Config::from_env();
        cfg.max_signal_age_secs = 1;
        cfg.ws_initial_snapshot_timeout_ms = 1_000;
        let stats = ScanStats {
            scan_duration_ms: 1_500.0,
            ws_snapshot_wait_ms: 1_000.0,
            ws_snapshot_ready_tokens: 1,
            ws_snapshot_total_tokens: 3,
            ws_snapshot_min_ready_tokens: 2,
            ws_snapshot_satisfied: false,
            quote_rest_requested: 3,
            quote_rest_resolved: 2,
            quote_hard_unresolved_tokens: 1,
            quote_missing_book_tokens: 1,
            ..ScanStats::default()
        };

        let blockers = latency_budget_blockers(&stats, &cfg);

        assert_eq!(latency_budget_status(&blockers), "blocked");
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("scan_duration_exceeds_signal_age_budget")));
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("ws_snapshot_coverage_timeout")));
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("quote_missing_book")));
    }

    #[test]
    fn latency_budget_flags_deferred_quotes_as_degraded() {
        let cfg = Config::from_env();
        let stats = ScanStats {
            quote_deferred_tokens: 2,
            ..ScanStats::default()
        };

        let blockers = latency_budget_blockers(&stats, &cfg);

        assert_eq!(latency_budget_status(&blockers), "degraded");
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("quote_refresh_budget_deferred")));
        assert!(live_latency_budget_blocker(&stats, &cfg)
            .unwrap()
            .contains("quote_refresh_budget_deferred:2"));
    }

    #[test]
    fn latency_budget_does_not_block_on_ws_coverage_when_snapshot_wait_disabled() {
        let mut cfg = Config::from_env();
        cfg.ws_initial_snapshot_timeout_ms = 0;
        let stats = ScanStats {
            ws_snapshot_ready_tokens: 0,
            ws_snapshot_total_tokens: 3,
            ws_snapshot_min_ready_tokens: 2,
            ws_snapshot_satisfied: false,
            ..ScanStats::default()
        };

        let blockers = latency_budget_blockers(&stats, &cfg);

        assert!(blockers
            .iter()
            .all(|blocker| !blocker.starts_with("ws_snapshot_coverage_timeout")));
        assert!(live_latency_budget_blocker(&stats, &cfg).is_none());
    }

    #[test]
    fn live_latency_budget_blocks_live_on_missing_book_shortfall() {
        let cfg = Config::from_env();
        let stats = ScanStats {
            quote_hard_unresolved_tokens: 1,
            quote_missing_book_tokens: 1,
            ..ScanStats::default()
        };

        let blocker = live_latency_budget_blocker(&stats, &cfg).expect("live blocker");

        assert!(blocker.contains("latency_budget_blocked"));
        assert!(blocker.contains("quote_missing_book:1"));
    }

    #[test]
    fn live_latency_budget_ignores_no_ask_only_shortfall() {
        let cfg = Config::from_env();
        let stats = ScanStats {
            quote_rest_requested: 315,
            quote_rest_resolved: 284,
            quote_hard_unresolved_tokens: 31,
            quote_no_ask_tokens: 31,
            quote_missing_book_tokens: 0,
            ..ScanStats::default()
        };

        let blockers = latency_budget_blockers(&stats, &cfg);

        assert!(blockers
            .iter()
            .all(|blocker| !blocker.starts_with("quote_missing_book")));
        assert!(live_latency_budget_blocker(&stats, &cfg).is_none());
    }

    #[test]
    fn live_latency_budget_uses_current_scan_elapsed_before_submit() {
        let mut cfg = Config::from_env();
        cfg.max_signal_age_secs = 1;
        let mut stats = ScanStats::default();
        let scan_start = Instant::now() - Duration::from_millis(1_500);

        let blocker = live_latency_budget_blocker_at_scan_elapsed(&mut stats, scan_start, &cfg)
            .expect("stale scan should block live submit");

        assert!(stats.scan_duration_ms >= 1_000.0);
        assert!(blocker.contains("latency_budget_blocked"));
        assert!(blocker.contains("scan_duration_exceeds_signal_age_budget"));
    }

    #[tokio::test]
    async fn drain_ws_wakes_coalesces_duplicates_and_discovery() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        tx.send(WsWake::Token("asset-1".to_string())).await.unwrap();
        tx.send(WsWake::Token("asset-1".to_string())).await.unwrap();
        tx.send(WsWake::Token("asset-2".to_string())).await.unwrap();
        tx.send(WsWake::Discovery).await.unwrap();
        drop(tx);

        let drain = drain_ws_wakes(Some(&mut rx)).await;

        assert_eq!(
            drain.dirty_tokens,
            HashSet::from(["asset-1".to_string(), "asset-2".to_string()])
        );
        assert!(drain.discovery_wake);
    }

    #[tokio::test]
    async fn drain_ws_wakes_returns_without_idle_wait_when_empty() {
        let (_tx, mut rx) = tokio::sync::mpsc::channel(10);

        let drain = tokio::time::timeout(Duration::from_millis(5), drain_ws_wakes(Some(&mut rx)))
            .await
            .expect("empty drain should not sleep");

        assert_eq!(drain, DrainedWsWakes::default());
    }

    #[tokio::test]
    async fn sleep_or_take_ws_wake_returns_after_dirty_debounce() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        tx.send(WsWake::Token("asset-1".to_string())).await.unwrap();

        let drain = tokio::time::timeout(
            Duration::from_millis(100),
            sleep_or_take_ws_wake(Some(&mut rx), Duration::from_secs(60)),
        )
        .await
        .expect("wake should beat sleep interval");

        assert_eq!(drain.dirty_tokens, HashSet::from(["asset-1".to_string()]));
        assert!(!drain.discovery_wake);
    }

    #[tokio::test]
    async fn sleep_or_take_ws_wake_does_not_debounce_after_plain_sleep() {
        let (_tx, mut rx) = tokio::sync::mpsc::channel(10);

        let drain = tokio::time::timeout(
            Duration::from_millis(10),
            sleep_or_take_ws_wake(Some(&mut rx), Duration::from_millis(1)),
        )
        .await
        .expect("plain sleep should not wait for dirty debounce");

        assert_eq!(drain, DrainedWsWakes::default());
    }

    #[tokio::test]
    async fn sleep_or_take_ws_wake_coalesces_short_dirty_burst() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        tx.send(WsWake::Token("asset-1".to_string())).await.unwrap();
        tx.send(WsWake::Token("asset-2".to_string())).await.unwrap();

        let drain = tokio::time::timeout(
            Duration::from_millis(100),
            sleep_or_take_ws_wake(Some(&mut rx), Duration::from_secs(60)),
        )
        .await
        .expect("dirty burst should beat sleep interval");

        assert_eq!(
            drain.dirty_tokens,
            HashSet::from(["asset-1".to_string(), "asset-2".to_string()])
        );
        assert!(!drain.discovery_wake);
    }

    #[test]
    fn dirty_subscription_fast_lane_adds_only_new_capped_tokens() {
        let dirty = HashSet::from([
            "asset-c".to_string(),
            "asset-a".to_string(),
            "asset-b".to_string(),
            "asset-selected".to_string(),
            "asset-subscribed".to_string(),
        ]);
        let desired = HashSet::from(["asset-selected".to_string()]);
        let subscribed = HashSet::from(["asset-subscribed".to_string()]);

        let tokens = dirty_subscription_fast_lane_tokens(&dirty, &desired, &subscribed, 2);

        assert_eq!(tokens, vec!["asset-a".to_string(), "asset-b".to_string()]);
    }

    #[tokio::test]
    async fn ws_snapshot_coverage_accepts_min_ready_fraction() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "a".into(),
                crate::ws_client::Price {
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
            guard.insert(
                "b".into(),
                crate::ws_client::Price {
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
            guard.insert(
                "c".into(),
                crate::ws_client::Price {
                    snapshot_ready: false,
                    ..Default::default()
                },
            );
        }
        let desired = HashSet::from(["a".into(), "b".into(), "c".into()]);

        let coverage = wait_for_ws_snapshot_coverage(Some(&cache), &desired, 0.66, 0).await;

        assert_eq!(
            coverage,
            WsSnapshotCoverage {
                ready: 2,
                total: 3,
                min_ready: 2,
                satisfied: true,
            }
        );
    }

    #[tokio::test]
    async fn ws_snapshot_coverage_times_out_below_minimum() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "a".into(),
                crate::ws_client::Price {
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let desired = HashSet::from(["a".into(), "b".into()]);

        let coverage = wait_for_ws_snapshot_coverage(Some(&cache), &desired, 1.0, 0).await;

        assert_eq!(
            coverage,
            WsSnapshotCoverage {
                ready: 1,
                total: 2,
                min_ready: 2,
                satisfied: false,
            }
        );
    }

    #[tokio::test]
    async fn cached_scan_quote_snapshot_accepts_fresh_bid_or_ask() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "ask-token".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.42),
                    ..Default::default()
                },
            );
            guard.insert(
                "bid-token".into(),
                crate::ws_client::Price {
                    best_bid: Some(0.40),
                    ..Default::default()
                },
            );
            guard.insert(
                "stale-bid-token".into(),
                crate::ws_client::Price {
                    best_bid: Some(0.39),
                    last_updated: Instant::now() - Duration::from_millis(2_000),
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.ws_quote_max_age_ms = 1_000;

        let tokens = cached_scan_quote_snapshot(Some(&cache), &cfg)
            .await
            .fresh_quote_tokens;

        assert!(tokens.contains("ask-token"));
        assert!(tokens.contains("bid-token"));
        assert!(!tokens.contains("stale-bid-token"));
    }

    #[tokio::test]
    async fn cached_scan_quote_snapshot_scores_toxic_microstructure() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "toxic-token".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.50),
                    best_bid: Some(0.49),
                    best_ask_size: Some(1.0),
                    best_bid_size: Some(500.0),
                    ask_depth: vec![(0.50, 1.0), (0.51, 1.0)],
                    bid_depth: vec![(0.49, 500.0), (0.48, 500.0)],
                    recent_trades: std::collections::VecDeque::from([
                        crate::ws_client::TradePrint {
                            side: "BUY".into(),
                            price: 0.50,
                            size: 50.0,
                            venue_timestamp_ms: None,
                            observed_at: Instant::now(),
                        },
                    ]),
                    ..Default::default()
                },
            );
            guard.insert(
                "quiet-token".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.50),
                    best_bid: Some(0.49),
                    best_ask_size: Some(200.0),
                    best_bid_size: Some(200.0),
                    ask_depth: vec![(0.50, 200.0), (0.51, 200.0)],
                    bid_depth: vec![(0.49, 200.0), (0.48, 200.0)],
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.ws_quote_max_age_ms = 1_000;
        cfg.live_trade_position_size_usd = 25.0;

        let snapshot = cached_scan_quote_snapshot(Some(&cache), &cfg).await;

        assert!(snapshot.fresh_quote_tokens.contains("toxic-token"));
        assert!(snapshot.fresh_quote_tokens.contains("quiet-token"));
        assert!(
            snapshot
                .toxicity_penalties
                .get("toxic-token")
                .copied()
                .unwrap_or_default()
                > 0.0
        );
        assert!(!snapshot.toxicity_penalties.contains_key("quiet-token"));
        assert!(
            snapshot
                .execution_survival_adjustments
                .get("toxic-token")
                .copied()
                .unwrap_or_default()
                < 0.0
        );
        assert!(
            snapshot
                .execution_survival_adjustments
                .get("quiet-token")
                .copied()
                .unwrap_or_default()
                > 0.0
        );
    }

    #[test]
    fn execution_survival_penalizes_recent_ask_queue_depletion() {
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        let mut depleted = crate::ws_client::Price {
            best_ask: Some(0.50),
            best_bid: Some(0.49),
            best_ask_size: Some(10.0),
            best_bid_size: Some(10.0),
            ask_depth: vec![(0.50, 10.0), (0.51, 10.0)],
            bid_depth: vec![(0.49, 10.0), (0.48, 10.0)],
            recent_depth_changes: std::collections::VecDeque::from([
                crate::ws_client::DepthChangePrint {
                    side: "ASK".into(),
                    price: 0.50,
                    old_size: 20.0,
                    new_size: 10.0,
                    level_index: Some(0),
                    venue_timestamp_ms: None,
                    observed_at: Instant::now(),
                },
            ]),
            ..Default::default()
        };
        let clean = crate::ws_client::Price {
            recent_depth_changes: Default::default(),
            ..depleted.clone()
        };

        let (ratio, notional, _) =
            recent_ask_queue_depletion_pressure(&depleted, Instant::now(), 5)
                .expect("queue depletion pressure");
        assert!(ratio > 0.49);
        assert!((notional - 5.0).abs() < 1e-9);

        let clean_score = scan_quote_execution_survival_adjustment(&clean, &cfg);
        let depleted_score = scan_quote_execution_survival_adjustment(&depleted, &cfg);

        assert!(clean_score - depleted_score > 400.0);

        depleted.recent_depth_changes.clear();
        assert!(recent_ask_queue_depletion_pressure(&depleted, Instant::now(), 5).is_none());
    }

    #[tokio::test]
    async fn live_trade_toxicity_blocks_recent_same_side_sweep() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    recent_trades: std::collections::VecDeque::from([
                        crate::ws_client::TradePrint {
                            side: "BUY".into(),
                            price: 0.50,
                            size: 20.0,
                            venue_timestamp_ms: None,
                            observed_at: Instant::now(),
                        },
                    ]),
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        let mut m = market("cond-a", "A");
        m.clob_token_id_yes = "yes-a".into();
        let mut opp = opportunity(vec![m]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.50,
        }];

        let blocker =
            live_trade_toxicity_blocker(Some(&cache), &cfg, &opp, cfg.live_trade_position_size_usd)
                .await
                .expect("toxicity blocker");

        assert!(blocker.contains("recent_same_side_trade_sweep:yes-a"));
    }

    #[tokio::test]
    async fn live_trade_toxicity_blocks_adverse_depth_flow() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    recent_depth_changes: std::collections::VecDeque::from([
                        crate::ws_client::DepthChangePrint {
                            side: "ASK".into(),
                            price: 0.50,
                            old_size: 40.0,
                            new_size: 0.0,
                            level_index: Some(0),
                            venue_timestamp_ms: None,
                            observed_at: Instant::now(),
                        },
                    ]),
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        let mut m = market("cond-a", "A");
        m.clob_token_id_yes = "yes-a".into();
        let mut opp = opportunity(vec![m]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.50,
        }];

        let blocker =
            live_trade_toxicity_blocker(Some(&cache), &cfg, &opp, cfg.live_trade_position_size_usd)
                .await
                .expect("toxicity blocker");

        assert!(blocker.contains("adverse_depth_flow:yes-a"));
    }

    #[tokio::test]
    async fn live_trade_toxicity_blocks_adverse_depth_flow_pressure() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    ask_depth: vec![(0.50, 20.0)],
                    recent_depth_changes: std::collections::VecDeque::from([
                        crate::ws_client::DepthChangePrint {
                            side: "ASK".into(),
                            price: 0.50,
                            old_size: 20.0,
                            new_size: 12.0,
                            level_index: Some(0),
                            venue_timestamp_ms: None,
                            observed_at: Instant::now(),
                        },
                    ]),
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 100.0;
        let mut m = market("cond-a", "A");
        m.clob_token_id_yes = "yes-a".into();
        let mut opp = opportunity(vec![m]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.50,
        }];

        let blocker =
            live_trade_toxicity_blocker(Some(&cache), &cfg, &opp, cfg.live_trade_position_size_usd)
                .await
                .expect("toxicity blocker");

        assert!(blocker.contains("adverse_depth_flow_pressure:yes-a"));
        assert!(blocker.contains("flow_ratio="));
    }

    #[tokio::test]
    async fn live_trade_toxicity_blocks_buy_pressure_depth_imbalance() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    ask_depth: vec![(0.50, 1.0)],
                    bid_depth: vec![(0.49, 30.0), (0.48, 30.0), (0.47, 30.0)],
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        let mut m = market("cond-a", "A");
        m.clob_token_id_yes = "yes-a".into();
        let mut opp = opportunity(vec![m]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.50,
        }];

        let blocker =
            live_trade_toxicity_blocker(Some(&cache), &cfg, &opp, cfg.live_trade_position_size_usd)
                .await
                .expect("toxicity blocker");

        assert!(blocker.contains("book_buy_pressure:yes-a"));
    }

    #[tokio::test]
    async fn live_trade_toxicity_blocks_adverse_clob_microprice() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.50),
                    best_bid: Some(0.40),
                    best_ask_size: Some(90.0),
                    best_bid_size: Some(10.0),
                    ask_depth: vec![(0.50, 90.0), (0.51, 120.0)],
                    bid_depth: vec![(0.40, 10.0), (0.39, 10.0)],
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.live_clob_microprice_adverse_bps = 1.0;
        let mut m = market("cond-a", "A");
        m.clob_token_id_yes = "yes-a".into();
        let mut opp = opportunity(vec![m]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.50,
        }];

        let blocker =
            live_trade_toxicity_blocker(Some(&cache), &cfg, &opp, cfg.live_trade_position_size_usd)
                .await
                .expect("toxicity blocker");

        assert!(blocker.contains("clob_microprice_adverse:yes-a"));
        assert!(blocker.contains("queue_imbalance="));
    }

    #[tokio::test]
    async fn live_trade_toxicity_blocks_fragile_ask_depth() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    ask_depth: vec![(0.50, 20.0)],
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        let mut m = market("cond-a", "A");
        m.clob_token_id_yes = "yes-a".into();
        let mut opp = opportunity(vec![m]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.50,
        }];

        let blocker =
            live_trade_toxicity_blocker(Some(&cache), &cfg, &opp, cfg.live_trade_position_size_usd)
                .await
                .expect("toxicity blocker");

        assert!(blocker.contains("ask_depth_fragile:yes-a"));
    }

    #[tokio::test]
    async fn live_trade_toxicity_blocks_concentrated_ask_depth() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    ask_depth: vec![(0.50, 150.0), (0.51, 2.0), (0.52, 2.0)],
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        let mut m = market("cond-a", "A");
        m.clob_token_id_yes = "yes-a".into();
        let mut opp = opportunity(vec![m]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.50,
        }];

        let blocker =
            live_trade_toxicity_blocker(Some(&cache), &cfg, &opp, cfg.live_trade_position_size_usd)
                .await
                .expect("toxicity blocker");

        assert!(blocker.contains("ask_depth_concentrated:yes-a"));
    }

    fn markout_test_opportunity() -> ArbitrageOpportunity {
        let mut a = market("cond-a", "A");
        a.clob_token_id_yes = "yes-a".into();
        a.clob_yes_ask = Some(0.40);
        let mut b = market("cond-b", "B");
        b.clob_token_id_yes = "yes-b".into();
        b.clob_yes_ask = Some(0.39);
        let mut opp = opportunity(vec![a, b]);
        opp.prices_from_clob = true;
        opp.total_cost = 0.79;
        opp.guaranteed_revenue = 1.0;
        opp.gross_profit = 0.21;
        opp.net_profit = 2.0;
        opp.max_executable_size_usd = 20.0;
        opp.execution_plan = vec![
            OpportunityLeg {
                market_index: 0,
                question: "A".into(),
                market_slug: "a".into(),
                condition_id: "cond-a".into(),
                token_id: "yes-a".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.40,
            },
            OpportunityLeg {
                market_index: 1,
                question: "B".into(),
                market_slug: "b".into(),
                condition_id: "cond-b".into(),
                token_id: "yes-b".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.39,
            },
        ];
        opp
    }

    async fn insert_markout_snapshot(cache: &PriceCache, token_id: &str, ask: f64) {
        insert_markout_snapshot_with_age(cache, token_id, ask, Duration::ZERO).await;
    }

    async fn insert_markout_snapshot_with_age(
        cache: &PriceCache,
        token_id: &str,
        ask: f64,
        age: Duration,
    ) {
        cache.write().await.insert(
            token_id.into(),
            crate::ws_client::Price {
                best_ask: Some(ask),
                best_ask_size: Some(1_000.0),
                ask_depth: vec![(ask, 1_000.0), (ask + 0.01, 1_000.0)],
                snapshot_ready: true,
                last_updated: Instant::now() - age,
                ..Default::default()
            },
        );
    }

    #[tokio::test]
    async fn opportunity_markout_blocks_current_ask_worse_than_repriced_plan() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        insert_markout_snapshot(&cache, "yes-a", 0.405).await;
        insert_markout_snapshot(&cache, "yes-b", 0.39).await;
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 10.0;
        cfg.live_clob_microprice_adverse_bps = 1.0;
        let opp = markout_test_opportunity();

        let blocker =
            opportunity_markout_blocker(Some(&cache), &cfg, &opp, opp.max_executable_size_usd)
                .await
                .expect("markout blocker");

        assert!(blocker.contains("markout_current_ask_worse:yes-a"));
    }

    #[tokio::test]
    async fn opportunity_markout_allows_clean_current_ws_state() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        insert_markout_snapshot(&cache, "yes-a", 0.40).await;
        insert_markout_snapshot(&cache, "yes-b", 0.39).await;
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 10.0;
        cfg.live_clob_microprice_adverse_bps = 1.0;
        let opp = markout_test_opportunity();

        assert!(
            opportunity_markout_blocker(Some(&cache), &cfg, &opp, opp.max_executable_size_usd)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn opportunity_markout_allows_stale_ws_after_depth_repriced_plan() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        insert_markout_snapshot_with_age(&cache, "yes-a", 0.45, Duration::from_millis(50)).await;
        insert_markout_snapshot_with_age(&cache, "yes-b", 0.44, Duration::from_millis(50)).await;
        let mut cfg = Config::from_env();
        cfg.ws_quote_max_age_ms = 10;
        cfg.live_clob_microprice_adverse_bps = 1.0;
        let opp = markout_test_opportunity();

        let blocker =
            opportunity_markout_blocker(Some(&cache), &cfg, &opp, opp.max_executable_size_usd)
                .await;

        assert_eq!(blocker, None);
    }

    #[tokio::test]
    async fn opportunity_markout_uses_paper_target_size_for_depth_fragility() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            for (token_id, ask) in [("yes-a", 0.40), ("yes-b", 0.39)] {
                guard.insert(
                    token_id.into(),
                    crate::ws_client::Price {
                        best_ask: Some(ask),
                        best_ask_size: Some(20.0),
                        ask_depth: vec![(ask, 20.0), (ask + 0.01, 20.0), (ask + 0.02, 20.0)],
                        snapshot_ready: true,
                        ..Default::default()
                    },
                );
            }
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 100.0;
        cfg.live_clob_microprice_adverse_bps = 1.0;
        let opp = markout_test_opportunity();

        let blocker = opportunity_markout_blocker(Some(&cache), &cfg, &opp, 7.0).await;

        assert_eq!(blocker, None);
    }

    #[tokio::test]
    async fn opportunity_markout_blocks_fragile_depth_before_fill_probability() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.40),
                    best_ask_size: Some(20.0),
                    ask_depth: vec![(0.40, 20.0)],
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
            guard.insert(
                "yes-b".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.39),
                    best_ask_size: Some(1_000.0),
                    ask_depth: vec![(0.39, 1_000.0), (0.40, 1_000.0)],
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 0.10;
        cfg.live_clob_microprice_adverse_bps = 1.0;
        let opp = markout_test_opportunity();

        let blocker =
            opportunity_markout_blocker(Some(&cache), &cfg, &opp, opp.max_executable_size_usd)
                .await
                .expect("markout blocker");

        assert!(blocker.contains("markout_toxicity:ask_depth_fragile:yes-a"));
    }

    #[tokio::test]
    async fn opportunity_markout_blocks_latency_adjusted_edge_decay() {
        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        insert_markout_snapshot_with_age(&cache, "yes-a", 0.40, Duration::from_millis(900)).await;
        insert_markout_snapshot_with_age(&cache, "yes-b", 0.39, Duration::from_millis(900)).await;
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 20.0;
        cfg.live_max_refresh_to_submit_ms = 1_000;
        cfg.ws_quote_max_age_ms = 2_000;
        cfg.discovery_interval_secs = 30;
        cfg.live_edge_haircut_usd = 1.5;
        cfg.live_edge_haircut_bps = 0;
        let opp = markout_test_opportunity();

        let blocker =
            opportunity_markout_blocker(Some(&cache), &cfg, &opp, opp.max_executable_size_usd)
                .await
                .expect("markout blocker");

        assert!(blocker.contains("latency_haircut="));
        assert!(blocker.contains("max_snapshot_age_ms="));
    }

    #[test]
    fn minimum_order_shares_for_bundle_uses_single_market_floor() {
        let mut m = market("cond-a", "A");
        m.clob_min_order_size = Some(5.0);
        let opp = ArbitrageOpportunity {
            event_title: "Event".into(),
            event_id: "event-1".into(),
            category: "geopolitics".into(),
            arb_type: ArbType::Bundle,
            markets: vec![m],
            execution_plan: vec![],
            total_cost: 0.8,
            guaranteed_revenue: 1.0,
            gross_profit: 0.2,
            total_fees: 0.0,
            net_profit: 0.2,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 25.0,
            prices_from_clob: true,
            max_executable_size_usd: 100.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        };
        assert_eq!(
            minimum_order_basket_units_for_opp(&opp, &Config::from_env()),
            5.0
        );
    }

    #[test]
    fn minimum_order_units_include_external_paper_notional_floor() {
        let mut cfg = Config::from_env();
        cfg.external_paper_min_order_usd = 1.0;

        let mut m = market("cond-a", "A");
        m.clob_min_order_size = Some(1.0);
        let mut opp = opportunity(vec![m]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.25,
        }];

        assert_eq!(minimum_order_basket_units_for_opp(&opp, &cfg), 4.0);
    }

    #[test]
    fn depth_reprice_target_ignores_best_ask_cap_below_paper_floor() {
        let mut cfg = Config::from_env();
        cfg.external_paper_min_order_usd = 1.0;

        let mut m = market("cond-a", "A");
        m.clob_min_order_size = Some(1.0);
        let mut opp = opportunity(vec![m]);
        opp.total_cost = 0.25;
        opp.max_executable_size_usd = 0.50;
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "yes-a".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.25,
        }];

        assert_eq!(target_position_for_depth_reprice(&opp, &cfg, 25.0), 25.0);
    }

    #[tokio::test]
    async fn depth_reprice_uses_batch_books_snapshot() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let now_ms = unix_now_ms().unwrap();
        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200).json_body(serde_json::json!([
                    {
                        "asset_id": "yes-a",
                        "asks": [
                            {"price": "0.40", "size": "5"},
                            {"price": "0.50", "size": "10"}
                        ],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": now_ms.to_string(),
                        "hash": "hash-a"
                    },
                    {
                        "asset_id": "yes-b",
                        "asks": [
                            {"price": "0.30", "size": "10"}
                        ],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": now_ms.to_string(),
                        "hash": "hash-b"
                    }
                ]));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.max_signal_age_secs = 10;
        cfg.live_max_refresh_to_submit_ms = 1000;

        let mut market_a = market("cond-a", "A");
        market_a.clob_token_id_yes = "yes-a".into();
        let mut market_b = market("cond-b", "B");
        market_b.clob_token_id_yes = "yes-b".into();
        let mut opp = opportunity(vec![market_a, market_b]);
        opp.execution_plan = vec![
            OpportunityLeg {
                market_index: 0,
                question: "A".into(),
                market_slug: "a".into(),
                condition_id: "cond-a".into(),
                token_id: "yes-a".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.40,
            },
            OpportunityLeg {
                market_index: 1,
                question: "B".into(),
                market_slug: "b".into(),
                condition_id: "cond-b".into(),
                token_id: "yes-b".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.30,
            },
        ];

        let prices = fetch_depth_adjusted_prices(&Client::new(), &cfg, None, &opp, 10.0)
            .await
            .expect("batch depth prices");

        assert!((prices[0] - 0.45).abs() < 1e-9);
        assert!((prices[1] - 0.30).abs() < 1e-9);
        books.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn depth_reprice_rejects_top_of_book_edge_that_fails_vwap() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let now_ms = unix_now_ms().unwrap();
        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200).json_body(serde_json::json!([
                    {
                        "asset_id": "yes-a",
                        "asks": [
                            {"price": "0.40", "size": "1"},
                            {"price": "0.65", "size": "100"}
                        ],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": now_ms.to_string(),
                        "hash": "hash-a"
                    },
                    {
                        "asset_id": "yes-b",
                        "asks": [
                            {"price": "0.40", "size": "1"},
                            {"price": "0.65", "size": "100"}
                        ],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": now_ms.to_string(),
                        "hash": "hash-b"
                    }
                ]));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.max_signal_age_secs = 10;
        cfg.live_max_refresh_to_submit_ms = 1000;
        cfg.validate_opportunities_at_target_size = true;
        cfg.min_net_profit_usd = 1.0;
        cfg.min_roi_pct = 0.0;

        let mut market_a = market("cond-a", "A");
        market_a.clob_token_id_yes = "yes-a".into();
        market_a.clob_yes_ask = Some(0.40);
        market_a.clob_yes_ask_size = Some(1.0);
        let mut market_b = market("cond-b", "B");
        market_b.clob_token_id_yes = "yes-b".into();
        market_b.clob_yes_ask = Some(0.40);
        market_b.clob_yes_ask_size = Some(1.0);
        let mut opp = opportunity(vec![market_a, market_b]);
        opp.total_cost = 0.80;
        opp.gross_profit = 0.20;
        opp.net_profit = 0.20;
        opp.roi_pct = 25.0;
        opp.prices_from_clob = true;
        opp.max_executable_size_usd = 100.0;
        opp.execution_plan = vec![
            OpportunityLeg {
                market_index: 0,
                question: "A".into(),
                market_slug: "a".into(),
                condition_id: "cond-a".into(),
                token_id: "yes-a".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.40,
            },
            OpportunityLeg {
                market_index: 1,
                question: "B".into(),
                market_slug: "b".into(),
                condition_id: "cond-b".into(),
                token_id: "yes-b".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.40,
            },
        ];

        let repriced =
            reprice_opportunity_at_target_size(&Client::new(), &cfg, None, &opp, 25.0).await;

        assert!(repriced.is_none());
        books.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn depth_reprice_uses_cached_ws_ladder_before_books() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(500);
            })
            .await;
        let now_ms = unix_now_ms().unwrap();
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        cfg.max_signal_age_secs = 10;
        cfg.live_max_refresh_to_submit_ms = 1000;
        cfg.ws_quote_max_age_ms = 10_000;

        let cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.40),
                    best_ask_size: Some(5.0),
                    ask_depth: vec![(0.40, 5.0), (0.50, 10.0)],
                    venue_timestamp_ms: Some(now_ms),
                    book_hash: Some("hash-a".into()),
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
            guard.insert(
                "yes-b".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.30),
                    best_ask_size: Some(10.0),
                    ask_depth: vec![(0.30, 10.0)],
                    venue_timestamp_ms: Some(now_ms),
                    book_hash: Some("hash-b".into()),
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }

        let mut market_a = market("cond-a", "A");
        market_a.clob_token_id_yes = "yes-a".into();
        let mut market_b = market("cond-b", "B");
        market_b.clob_token_id_yes = "yes-b".into();
        let mut opp = opportunity(vec![market_a, market_b]);
        opp.execution_plan = vec![
            OpportunityLeg {
                market_index: 0,
                question: "A".into(),
                market_slug: "a".into(),
                condition_id: "cond-a".into(),
                token_id: "yes-a".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.40,
            },
            OpportunityLeg {
                market_index: 1,
                question: "B".into(),
                market_slug: "b".into(),
                condition_id: "cond-b".into(),
                token_id: "yes-b".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.30,
            },
        ];

        let prices = fetch_depth_adjusted_prices(&Client::new(), &cfg, Some(&cache), &opp, 10.0)
            .await
            .expect("cached depth prices");

        assert!((prices[0] - 0.45).abs() < 1e-9);
        assert!((prices[1] - 0.30).abs() < 1e-9);
        books.assert_calls_async(0).await;
    }

    #[test]
    fn scan_depth_snapshot_coherence_rejects_route_skew() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 1000;
        cfg.max_signal_age_secs = 10;
        let now_ms = unix_now_ms().unwrap();
        let observed_now = Instant::now();
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "a".to_string(),
            crate::clob_client::DepthSnapshot {
                token_id: "a".into(),
                asks: vec![(0.4, 10.0)],
                tick_size: Some(0.01),
                min_order_size: Some(1.0),
                neg_risk: Some(true),
                venue_timestamp_ms: Some(now_ms),
                observed_at: Some(observed_now - Duration::from_millis(2_000)),
                book_hash: Some("hash-a".into()),
            },
        );
        snapshots.insert(
            "b".to_string(),
            crate::clob_client::DepthSnapshot {
                token_id: "b".into(),
                asks: vec![(0.4, 10.0)],
                tick_size: Some(0.01),
                min_order_size: Some(1.0),
                neg_risk: Some(true),
                venue_timestamp_ms: Some(now_ms),
                observed_at: Some(observed_now),
                book_hash: Some("hash-b".into()),
            },
        );

        assert!(!scan_depth_snapshots_coherent(
            &cfg,
            &["a".into(), "b".into()],
            &snapshots
        ));
    }

    #[test]
    fn scan_depth_snapshot_coherence_uses_live_refresh_age_not_signal_age() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 1_000;
        cfg.max_signal_age_secs = 10;
        let now_ms = unix_now_ms().unwrap();
        let observed_now = Instant::now();
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "a".to_string(),
            crate::clob_client::DepthSnapshot {
                token_id: "a".into(),
                asks: vec![(0.4, 10.0)],
                tick_size: Some(0.01),
                min_order_size: Some(1.0),
                neg_risk: Some(true),
                venue_timestamp_ms: Some(now_ms),
                observed_at: Some(observed_now - Duration::from_millis(1_500)),
                book_hash: Some("hash-a".into()),
            },
        );

        assert!(!scan_depth_snapshots_coherent(
            &cfg,
            &["a".into()],
            &snapshots
        ));
    }

    #[test]
    fn collect_quote_token_ids_filters_non_tradable_markets() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1000.0;

        let mut good = market("cond-good", "Good");
        good.clob_token_id_yes = "yes-good".into();
        good.clob_token_id_no = "no-good".into();
        good.liquidity = 5_000.0;

        let mut low_liq = market("cond-low", "Low");
        low_liq.clob_token_id_yes = "yes-low".into();
        low_liq.clob_token_id_no = "no-low".into();
        low_liq.liquidity = 10.0;

        let mut closed = market("cond-closed", "Closed");
        closed.clob_token_id_yes = "yes-closed".into();
        closed.clob_token_id_no = "no-closed".into();
        closed.closed = true;

        let event = crate::models::Event {
            event_id: "event-1".into(),
            title: "Event".into(),
            slug: "event".into(),
            category: "politics".into(),
            enable_neg_risk: true,
            neg_risk: true,
            neg_risk_augmented: false,
            lifecycle: Default::default(),
            markets: vec![good, low_liq, closed],
        };

        let tokens = collect_quote_token_ids(&[event], &cfg);
        assert_eq!(tokens, vec!["yes-good".to_string(), "no-good".to_string()]);
    }

    #[test]
    fn neg_risk_candidate_builder_preserves_event_indices_and_counts() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1000.0;

        let mut a = market("cond-a", "A");
        a.clob_token_id_yes = "yes-a".into();
        a.clob_token_id_no = "no-a".into();
        a.gamma_yes_price = 0.30;
        a.gamma_no_price = 0.65;
        let mut b = market("cond-b", "B");
        b.clob_token_id_yes = "yes-b".into();
        b.clob_token_id_no = "no-b".into();
        b.gamma_yes_price = 0.35;
        b.gamma_no_price = 0.60;
        let mut skipped = market("cond-skipped", "Skipped");
        skipped.clob_token_id_yes = "yes-skipped".into();
        skipped.clob_token_id_no = "no-skipped".into();
        let mut c = market("cond-c", "C");
        c.clob_token_id_yes = "yes-c".into();
        c.clob_token_id_no = "no-c".into();
        c.gamma_yes_price = 0.20;
        c.gamma_no_price = 0.70;
        let mut d = market("cond-d", "D");
        d.clob_token_id_yes = "yes-d".into();
        d.clob_token_id_no = "no-d".into();
        d.gamma_yes_price = 0.25;
        d.gamma_no_price = 0.65;
        let events = vec![
            event("Event 1", "event-1", vec![a, b], true),
            event("Event skipped", "event-skipped", vec![skipped], true),
            event("Event 2", "event-2", vec![c, d], true),
        ];
        let cached_tokens = HashSet::from(["yes-a".to_string(), "yes-d".to_string()]);

        let candidates = neg_risk_candidate_selections_for_side(
            &events,
            &cached_tokens,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cfg,
            crate::models::OutcomeSide::Yes,
        );

        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.idx)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(candidates[0].quote_tokens, vec!["yes-a", "yes-b"]);
        assert_eq!(candidates[0].total_tokens, 2);
        assert_eq!(candidates[0].cached_tokens, 1);
        assert_eq!(candidates[0].missing_tokens, 1);
        assert_eq!(candidates[1].quote_tokens, vec!["yes-c", "yes-d"]);
        assert_eq!(candidates[1].cached_tokens, 1);
    }

    #[test]
    fn neg_risk_candidate_builder_subtracts_toxicity_penalty() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1000.0;

        let mut a = market("cond-a", "A");
        a.clob_token_id_yes = "yes-a".into();
        a.clob_token_id_no = "no-a".into();
        a.gamma_yes_price = 0.30;
        a.gamma_no_price = 0.65;
        let mut b = market("cond-b", "B");
        b.clob_token_id_yes = "yes-b".into();
        b.clob_token_id_no = "no-b".into();
        b.gamma_yes_price = 0.35;
        b.gamma_no_price = 0.60;
        let event = event("Event", "event", vec![a, b], true);
        let cached_tokens = HashSet::from(["yes-a".to_string(), "yes-b".to_string()]);
        let toxic_penalties = HashMap::from([("yes-a".to_string(), 1_250.0)]);

        let clean = candidate_selection_for_event_side(
            0,
            &event,
            &cached_tokens,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cfg,
            crate::models::OutcomeSide::Yes,
        )
        .expect("clean candidate");
        let toxic = candidate_selection_for_event_side(
            0,
            &event,
            &cached_tokens,
            &HashMap::new(),
            &toxic_penalties,
            &HashMap::new(),
            &cfg,
            crate::models::OutcomeSide::Yes,
        )
        .expect("toxic candidate");

        assert!((clean.score - toxic.score - 1_250.0).abs() < 1e-9);
    }

    #[test]
    fn neg_risk_candidate_builder_adds_execution_survival_adjustment() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1000.0;

        let mut a = market("cond-a", "A");
        a.clob_token_id_yes = "yes-a".into();
        a.clob_token_id_no = "no-a".into();
        a.gamma_yes_price = 0.30;
        a.gamma_no_price = 0.65;
        let mut b = market("cond-b", "B");
        b.clob_token_id_yes = "yes-b".into();
        b.clob_token_id_no = "no-b".into();
        b.gamma_yes_price = 0.35;
        b.gamma_no_price = 0.60;
        let event = event("Event", "event", vec![a, b], true);
        let cached_tokens = HashSet::from(["yes-a".to_string(), "yes-b".to_string()]);
        let survival = HashMap::from([("yes-a".to_string(), 425.0), ("yes-b".to_string(), -75.0)]);

        let baseline = candidate_selection_for_event_side(
            0,
            &event,
            &cached_tokens,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cfg,
            crate::models::OutcomeSide::Yes,
        )
        .expect("baseline candidate");
        let adjusted = candidate_selection_for_event_side(
            0,
            &event,
            &cached_tokens,
            &HashMap::new(),
            &HashMap::new(),
            &survival,
            &cfg,
            crate::models::OutcomeSide::Yes,
        )
        .expect("adjusted candidate");

        assert!((adjusted.score - baseline.score - 350.0).abs() < 1e-9);
    }

    #[test]
    fn neg_risk_candidate_builder_adds_ws_ask_edge_bonus() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1000.0;

        let mut a = market("cond-a", "A");
        a.clob_token_id_yes = "yes-a".into();
        a.clob_token_id_no = "no-a".into();
        a.gamma_yes_price = 0.45;
        a.gamma_no_price = 0.55;
        let mut b = market("cond-b", "B");
        b.clob_token_id_yes = "yes-b".into();
        b.clob_token_id_no = "no-b".into();
        b.gamma_yes_price = 0.45;
        b.gamma_no_price = 0.55;
        let event = event("Event", "event", vec![a, b], true);
        let cached_tokens = HashSet::from(["yes-a".to_string(), "yes-b".to_string()]);
        let best_asks = HashMap::from([("yes-a".to_string(), 0.20), ("yes-b".to_string(), 0.25)]);

        let gamma_only = candidate_selection_for_event_side(
            0,
            &event,
            &cached_tokens,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cfg,
            crate::models::OutcomeSide::Yes,
        )
        .expect("gamma-only candidate");
        let with_ws_edge = candidate_selection_for_event_side(
            0,
            &event,
            &cached_tokens,
            &best_asks,
            &HashMap::new(),
            &HashMap::new(),
            &cfg,
            crate::models::OutcomeSide::Yes,
        )
        .expect("ws-edge candidate");

        assert!(with_ws_edge.score > gamma_only.score + 50_000.0);
    }

    #[test]
    fn bundle_candidate_builder_preserves_event_major_market_order() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1000.0;
        cfg.enable_bundle_scanning_all_events = true;

        let mut a = market("cond-a", "A");
        a.clob_token_id_yes = "yes-a".into();
        a.clob_token_id_no = "no-a".into();
        a.gamma_yes_price = 0.44;
        a.gamma_no_price = 0.45;
        let mut b = market("cond-b", "B");
        b.clob_token_id_yes = "yes-b".into();
        b.clob_token_id_no = "no-b".into();
        b.gamma_yes_price = 0.46;
        b.gamma_no_price = 0.43;
        let mut c = market("cond-c", "C");
        c.clob_token_id_yes = "yes-c".into();
        c.clob_token_id_no = "no-c".into();
        c.gamma_yes_price = 0.42;
        c.gamma_no_price = 0.44;
        let events = vec![
            event("Event 1", "event-1", vec![a, b], false),
            event("Event 2", "event-2", vec![c], false),
        ];
        let cached_tokens = HashSet::from(["yes-b".to_string()]);

        let (pool, candidates) = bundle_market_candidate_selections(
            &events,
            &cached_tokens,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cfg,
        );

        assert_eq!(
            pool.iter()
                .filter_map(|event| event.markets.first())
                .map(|market| market.condition_id.as_str())
                .collect::<Vec<_>>(),
            vec!["cond-a", "cond-b", "cond-c"]
        );
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.idx)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(candidates[1].quote_tokens, vec!["yes-b", "no-b"]);
        assert_eq!(candidates[1].cached_tokens, 1);
        assert_eq!(candidates[1].missing_tokens, 1);
    }

    #[test]
    fn bundle_candidate_builder_adds_ws_ask_edge_bonus() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1000.0;
        cfg.enable_bundle_scanning_all_events = true;

        let mut market = market("cond-a", "A");
        market.clob_token_id_yes = "yes-a".into();
        market.clob_token_id_no = "no-a".into();
        market.gamma_yes_price = 0.50;
        market.gamma_no_price = 0.50;
        let events = vec![event("Event", "event", vec![market], false)];
        let cached_tokens = HashSet::from(["yes-a".to_string(), "no-a".to_string()]);
        let best_asks = HashMap::from([("yes-a".to_string(), 0.40), ("no-a".to_string(), 0.40)]);

        let (_, gamma_only) = bundle_market_candidate_selections(
            &events,
            &cached_tokens,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &cfg,
        );
        let (_, with_ws_edge) = bundle_market_candidate_selections(
            &events,
            &cached_tokens,
            &best_asks,
            &HashMap::new(),
            &HashMap::new(),
            &cfg,
        );

        assert_eq!(gamma_only.len(), 1);
        assert_eq!(with_ws_edge.len(), 1);
        assert!(with_ws_edge[0].score > gamma_only[0].score + 30_000.0);
    }

    #[test]
    fn select_candidate_indices_keeps_top_priority_slice_and_rotates_tail() {
        let candidates = vec![
            CandidateSelection {
                idx: 1,
                score: 10.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 2,
                score: 9.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 3,
                score: 8.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 4,
                score: 7.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 5,
                score: 6.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 6,
                score: 5.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
        ];

        let dirty = HashSet::new();
        let scan_a = select_candidate_indices(&candidates, 4, 100, 100, 0, 1, 0.60, &dirty);
        let scan_b = select_candidate_indices(&candidates, 4, 100, 100, 1, 1, 0.60, &dirty);

        assert!(scan_a.contains(&1));
        assert!(scan_a.contains(&2));
        assert!(scan_b.contains(&1));
        assert!(scan_b.contains(&2));
        assert_ne!(scan_a, scan_b);
    }

    #[test]
    fn select_candidate_indices_prioritizes_dirty_candidate() {
        let candidates = vec![
            CandidateSelection {
                idx: 1,
                score: 100.0,
                total_tokens: 2,
                cached_tokens: 2,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 2,
                score: 1.0,
                total_tokens: 2,
                cached_tokens: 2,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
        ];
        let dirty = HashSet::from([2usize]);

        let selected = select_candidate_indices(&candidates, 1, 100, 100, 0, 1, 0.60, &dirty);

        assert_eq!(selected, vec![2]);
    }

    #[test]
    fn ranked_candidate_order_can_exclude_clean_candidates() {
        let candidates = vec![
            CandidateSelection {
                idx: 1,
                score: 100.0,
                total_tokens: 2,
                cached_tokens: 2,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 2,
                score: 5.0,
                total_tokens: 2,
                cached_tokens: 2,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 3,
                score: 10.0,
                total_tokens: 2,
                cached_tokens: 2,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
        ];
        let dirty = HashSet::from([2usize, 3usize]);

        let order = ranked_candidate_order(&candidates, 100, Some(&dirty), None);

        assert_eq!(order, vec![2, 1]);
    }

    #[test]
    fn dirty_candidate_indices_use_token_reverse_index() {
        let candidates = vec![CandidateSelection {
            idx: 1,
            score: 1.0,
            total_tokens: 1,
            cached_tokens: 0,
            missing_tokens: 1,
            quote_tokens: vec!["dirty-candidate".to_string()],
        }];
        let index = candidate_token_index(&candidates);

        let skipped_dirty = HashSet::from(["dirty-skipped".to_string()]);
        assert!(dirty_candidate_indices_from_index(&index, &skipped_dirty).is_empty());

        let candidate_dirty = HashSet::from(["dirty-candidate".to_string()]);
        assert_eq!(
            dirty_candidate_indices_from_index(&index, &candidate_dirty),
            HashSet::from([1usize])
        );
    }

    #[test]
    fn candidate_selection_state_marks_dirty_candidates() {
        let selected_ranks = HashMap::from([(1usize, 1usize), (2usize, 2usize)]);
        let dirty = HashSet::from([2usize, 3usize]);

        assert_eq!(
            candidate_selection_state(&selected_ranks, &dirty, 1),
            "selected"
        );
        assert_eq!(
            candidate_selection_state(&selected_ranks, &dirty, 2),
            "selected_dirty"
        );
        assert_eq!(
            candidate_selection_state(&selected_ranks, &dirty, 3),
            "deferred_dirty"
        );
        assert_eq!(
            candidate_selection_state(&selected_ranks, &dirty, 4),
            "deferred_by_rotation_or_budget"
        );
    }

    #[test]
    fn select_candidate_indices_respects_quote_budget_but_keeps_cached_ready_items() {
        let candidates = vec![
            CandidateSelection {
                idx: 1,
                score: 10.0,
                total_tokens: 8,
                cached_tokens: 8,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 2,
                score: 9.0,
                total_tokens: 8,
                cached_tokens: 6,
                missing_tokens: 2,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 3,
                score: 8.0,
                total_tokens: 8,
                cached_tokens: 4,
                missing_tokens: 4,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 4,
                score: 7.0,
                total_tokens: 8,
                cached_tokens: 6,
                missing_tokens: 2,
                quote_tokens: Vec::new(),
            },
        ];

        let selected = select_candidate_indices(&candidates, 4, 2, 32, 0, 1, 0.60, &HashSet::new());
        assert!(selected.contains(&1));
        assert!(selected.contains(&2) || selected.contains(&4));
        assert!(!selected.contains(&3));
    }

    #[test]
    fn select_candidate_indices_respects_active_token_budget() {
        let candidates = vec![
            CandidateSelection {
                idx: 1,
                score: 10.0,
                total_tokens: 36,
                cached_tokens: 36,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 2,
                score: 9.0,
                total_tokens: 12,
                cached_tokens: 12,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 3,
                score: 8.0,
                total_tokens: 12,
                cached_tokens: 12,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 4,
                score: 7.0,
                total_tokens: 12,
                cached_tokens: 12,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
        ];

        let selected =
            select_candidate_indices(&candidates, 4, 100, 24, 0, 1, 0.60, &HashSet::new());
        assert!(!selected.contains(&1));
        assert_eq!(selected.len(), 2);
        assert!(selected.contains(&2));
        assert!(selected.contains(&3) || selected.contains(&4));
    }

    #[test]
    fn describe_quote_token_samples_includes_event_and_leg_context() {
        let mut m = market("cond-1", "Will Alice finish 1st?");
        m.clob_token_id_yes = "yes-1".into();
        m.clob_token_id_no = "no-1".into();
        let event = crate::models::Event {
            event_id: "event-1".into(),
            title: "Who finishes 1st in the race?".into(),
            slug: "who-finishes-1st-in-the-race".into(),
            category: "sports".into(),
            enable_neg_risk: true,
            neg_risk: true,
            neg_risk_augmented: false,
            lifecycle: Default::default(),
            markets: vec![m],
        };
        let rendered =
            describe_quote_token_samples(&[event], &["yes-1".into(), "missing".into()], 4);
        assert!(rendered.contains("yes-1 =>"));
        assert!(rendered.contains("Will Alice finish 1st?"));
        assert!(rendered.contains("YES"));
        assert!(rendered.contains("missing"));
    }

    #[test]
    fn select_candidate_indices_holds_rotation_stable_within_rotation_period() {
        let candidates = vec![
            CandidateSelection {
                idx: 1,
                score: 10.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 2,
                score: 9.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 3,
                score: 8.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 4,
                score: 7.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 5,
                score: 6.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
            CandidateSelection {
                idx: 6,
                score: 5.0,
                total_tokens: 6,
                cached_tokens: 6,
                missing_tokens: 0,
                quote_tokens: Vec::new(),
            },
        ];

        let dirty = HashSet::new();
        let scan_0 = select_candidate_indices(&candidates, 4, 100, 100, 0, 4, 0.60, &dirty);
        let scan_1 = select_candidate_indices(&candidates, 4, 100, 100, 1, 4, 0.60, &dirty);
        let scan_4 = select_candidate_indices(&candidates, 4, 100, 100, 4, 4, 0.60, &dirty);

        assert_eq!(scan_0, scan_1);
        assert_ne!(scan_0, scan_4);
    }

    #[test]
    fn outcome_execution_penalty_penalizes_ranked_style_no_families() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 100.0;

        let ranked_event = event(
            "English Premier League - Top Goalscorer",
            "english-premier-league-top-goalscorer",
            vec![
                market("cond-a", "Will Alice be top goalscorer?"),
                market("cond-b", "Will Bob be top goalscorer?"),
                market("cond-c", "Will Carol be top goalscorer?"),
                market("cond-d", "Will Dave be top goalscorer?"),
            ],
            false,
        );

        let yes_penalty = outcome_execution_penalty(&ranked_event, &cfg, OutcomeSide::Yes);
        let no_penalty = outcome_execution_penalty(&ranked_event, &cfg, OutcomeSide::No);
        assert!(no_penalty > yes_penalty);
    }

    #[test]
    fn bundle_priority_prefers_balanced_mid_market_over_extreme_tail_market() {
        let cfg = Config::from_env();
        let cached_tokens = std::collections::HashSet::new();

        let mut balanced = market("cond-balanced", "Balanced market");
        balanced.gamma_yes_price = 0.49;
        balanced.gamma_no_price = 0.49;
        balanced.clob_token_id_yes = "balanced-yes".into();
        balanced.clob_token_id_no = "balanced-no".into();
        balanced.liquidity = 10_000.0;

        let mut tail = market("cond-tail", "Extreme tail market");
        tail.gamma_yes_price = 0.01;
        tail.gamma_no_price = 0.99;
        tail.clob_token_id_yes = "tail-yes".into();
        tail.clob_token_id_no = "tail-no".into();
        tail.liquidity = 10_000.0;

        let parent = event(
            "Market family",
            "market-family",
            vec![balanced.clone(), tail.clone()],
            false,
        );

        let balanced_score = bundle_market_priority_score(&parent, &balanced, &cached_tokens, &cfg);
        let tail_score = bundle_market_priority_score(&parent, &tail, &cached_tokens, &cfg);
        assert!(balanced_score > tail_score);
    }

    #[test]
    fn capital_velocity_ranking_prefers_shorter_known_lock_for_same_edge() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1.0;
        cfg.capital_velocity_ranking_enabled = true;
        cfg.capital_velocity_reference_hours = 24.0;
        cfg.capital_velocity_score_weight = 20_000.0;
        let cached_tokens = std::collections::HashSet::new();
        let now = Utc::now();

        let mut market_a = market("cond-a", "A");
        market_a.gamma_yes_price = 0.45;
        market_a.clob_token_id_yes = "a-yes".into();
        market_a.liquidity = 10_000.0;
        let mut market_b = market("cond-b", "B");
        market_b.gamma_yes_price = 0.45;
        market_b.clob_token_id_yes = "b-yes".into();
        market_b.liquidity = 10_000.0;

        let mut short_lock = event(
            "Fast settlement",
            "fast-settlement",
            vec![market_a.clone(), market_b.clone()],
            true,
        );
        short_lock.lifecycle.end_date = Some(now + chrono::Duration::hours(6));
        let mut long_lock = event(
            "Slow settlement",
            "slow-settlement",
            vec![market_a, market_b],
            true,
        );
        long_lock.lifecycle.end_date = Some(now + chrono::Duration::days(7));

        let short_score = neg_risk_event_priority_score_for_side(
            &short_lock,
            &cached_tokens,
            &cfg,
            OutcomeSide::Yes,
        );
        let long_score = neg_risk_event_priority_score_for_side(
            &long_lock,
            &cached_tokens,
            &cfg,
            OutcomeSide::Yes,
        );

        assert!(short_score > long_score);
    }

    #[test]
    fn fingerprint_is_order_insensitive_for_market_set() {
        let opp_a = opportunity(vec![market("cond-b", "B"), market("cond-a", "A")]);
        let opp_b = opportunity(vec![market("cond-a", "A"), market("cond-b", "B")]);

        assert_eq!(
            opportunity_fingerprint(&opp_a),
            opportunity_fingerprint(&opp_b)
        );
    }

    #[test]
    fn fingerprint_differs_by_arb_type() {
        let mut yes_opp = opportunity(vec![market("cond-a", "A")]);
        let mut no_opp = opportunity(vec![market("cond-a", "A")]);
        yes_opp.arb_type = ArbType::Yes;
        no_opp.arb_type = ArbType::No;

        assert_ne!(
            opportunity_fingerprint(&yes_opp),
            opportunity_fingerprint(&no_opp)
        );
    }

    #[test]
    fn fingerprint_differs_by_repriced_leg_price() {
        let mut cheap = opportunity(vec![market("cond-a", "A")]);
        cheap.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "12345".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.4000,
        }];
        let mut expensive = cheap.clone();
        expensive.execution_plan[0].reference_price = 0.4100;

        assert_ne!(
            opportunity_fingerprint(&cheap),
            opportunity_fingerprint(&expensive)
        );
    }

    #[test]
    fn opportunity_can_execute_on_polymarket_rejects_external_tokens() {
        let mut opp = opportunity(vec![market("cond-a", "A")]);
        opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "12345".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.4,
        }];
        assert!(opportunity_can_execute_on_polymarket(&opp));

        opp.execution_plan[0].token_id = "external:kalshi:abc".into();
        assert!(!opportunity_can_execute_on_polymarket(&opp));
    }

    #[test]
    fn lifecycle_gate_rejects_events_near_cutoff() {
        let mut cfg = Config::from_env();
        cfg.event_lifecycle_gate_enabled = true;
        cfg.event_lifecycle_pre_cutoff_buffer_secs = 600;
        let mut event = event("Event", "event", vec![market("cond-a", "A")], true);
        event.lifecycle.end_date = Some(chrono::Utc::now() + chrono::Duration::seconds(60));

        let (kept, rejected) = filter_lifecycle_scan_events(&[event], &cfg, None, 1, "test", "YES");

        assert!(kept.is_empty());
        assert_eq!(rejected, 1);
    }

    #[test]
    fn lifecycle_gate_rejects_events_near_game_start_quarantine() {
        let mut cfg = Config::from_env();
        cfg.event_lifecycle_gate_enabled = true;
        cfg.event_lifecycle_pre_cutoff_buffer_secs = 0;
        cfg.live_game_start_quarantine_secs = 300;
        let mut event = event("Game", "game", vec![market("cond-a", "A")], true);
        event.lifecycle.game_start_time = Some(chrono::Utc::now() + chrono::Duration::seconds(120));

        let (kept, rejected) = filter_lifecycle_scan_events(&[event], &cfg, None, 1, "test", "YES");

        assert!(kept.is_empty());
        assert_eq!(rejected, 1);
    }

    #[test]
    fn lifecycle_gate_allows_unknown_or_far_cutoff() {
        let mut cfg = Config::from_env();
        cfg.event_lifecycle_gate_enabled = true;
        cfg.event_lifecycle_pre_cutoff_buffer_secs = 600;
        let unknown = event("Unknown", "unknown", vec![market("cond-a", "A")], true);
        let mut far = event("Far", "far", vec![market("cond-b", "B")], true);
        far.lifecycle.end_date = Some(chrono::Utc::now() + chrono::Duration::seconds(3600));

        let (kept, rejected) =
            filter_lifecycle_scan_events(&[unknown, far], &cfg, None, 1, "test", "YES");

        assert_eq!(kept.len(), 2);
        assert_eq!(rejected, 0);
    }

    #[test]
    fn neg_risk_quote_ready_requires_visible_size_in_strict_clob_mode() {
        let mut market_a = market("cond-a", "A");
        market_a.clob_yes_ask = Some(0.20);
        market_a.clob_yes_ask_size = Some(100.0);
        market_a.clob_no_ask = Some(0.80);
        market_a.clob_no_ask_size = Some(100.0);
        let mut market_b = market("cond-b", "B");
        market_b.clob_yes_ask = Some(0.20);
        market_b.clob_yes_ask_size = None;
        market_b.clob_no_ask = Some(0.80);
        market_b.clob_no_ask_size = Some(100.0);
        let event = event("Strict", "strict", vec![market_a, market_b], true);
        let mut cfg = Config::from_env();
        cfg.execute_only_full_clob_prices = true;

        assert!(!neg_risk_side_quote_ready(&event, true, &cfg));
        assert!(neg_risk_side_quote_ready(&event, false, &cfg));
    }

    #[test]
    fn live_execution_gate_blocks_non_atomic_and_ranked_opportunities() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        cfg.combo_rfq_discovery_enabled = true;

        let mut single_leg_opp = opportunity(vec![market("cond-a", "A")]);
        single_leg_opp.markets[0].clob_token_id_yes = "12345".into();
        single_leg_opp.execution_plan = vec![OpportunityLeg {
            market_index: 0,
            question: "A".into(),
            market_slug: "a".into(),
            condition_id: "cond-a".into(),
            token_id: "12345".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.4,
        }];
        assert_eq!(
            live_execution_block_reason(&single_leg_opp),
            Some(LiveBlockReason::RouteUnsupported)
        );

        let mut malformed_bundle = single_leg_opp.clone();
        malformed_bundle.arb_type = ArbType::Bundle;
        assert_eq!(
            live_execution_block_reason(&malformed_bundle),
            Some(LiveBlockReason::RouteUnsupported)
        );

        let mut market_a = market("cond-a", "A");
        market_a.clob_token_id_yes = "12345".into();
        let mut market_b = market("cond-b", "B");
        market_b.clob_token_id_yes = "55555".into();
        let mut opp = opportunity(vec![market_a, market_b]);
        opp.execution_plan = vec![
            OpportunityLeg {
                market_index: 0,
                question: "A".into(),
                market_slug: "a".into(),
                condition_id: "cond-a".into(),
                token_id: "12345".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.4,
            },
            OpportunityLeg {
                market_index: 1,
                question: "B".into(),
                market_slug: "b".into(),
                condition_id: "cond-b".into(),
                token_id: "55555".into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.6,
            },
        ];
        assert_eq!(
            live_execution_block_reason(&opp),
            Some(LiveBlockReason::RouteUnsupported)
        );

        let mut wrong_side_opp = opp.clone();
        wrong_side_opp.execution_plan[1] = OpportunityLeg {
            market_index: 1,
            question: "B".into(),
            market_slug: "b".into(),
            condition_id: "cond-b".into(),
            token_id: "67890".into(),
            outcome: OutcomeSide::No,
            unit_shares: 1.0,
            reference_price: 0.6,
        };
        wrong_side_opp.markets[1].clob_token_id_no = "67890".into();
        assert_eq!(
            live_execution_block_reason(&wrong_side_opp),
            Some(LiveBlockReason::RouteUnsupported)
        );

        let mut mint_sell_opp = opp.clone();
        mint_sell_opp.arb_type = ArbType::MintSell;
        assert_eq!(
            live_execution_block_reason(&mint_sell_opp),
            Some(LiveBlockReason::MintSellUnsupported)
        );

        let mut stats = ScanStats::default();
        assert!(skip_live_blocked_opportunity(
            &mut stats, true, true, false, None, 42, &cfg, &opp, 25.0, None
        ));
        assert!(stats
            .operator_notes
            .iter()
            .any(|note| note.contains("live blocked: route_unsupported")));
        assert!(!skip_live_blocked_opportunity(
            &mut stats, false, false, false, None, 42, &cfg, &opp, 25.0, None
        ));
        let mut paper_fallback_stats = ScanStats::default();
        assert!(!skip_live_blocked_opportunity(
            &mut paper_fallback_stats,
            true,
            true,
            true,
            None,
            42,
            &cfg,
            &opp,
            25.0,
            None
        ));
        assert!(paper_fallback_stats
            .operator_notes
            .iter()
            .any(|note| note.contains("live blocked: route_unsupported")));
        let mut diagnostics_only_stats = ScanStats::default();
        assert!(!skip_live_blocked_opportunity(
            &mut diagnostics_only_stats,
            true,
            false,
            false,
            None,
            42,
            &cfg,
            &opp,
            25.0,
            None
        ));
        assert!(diagnostics_only_stats
            .operator_notes
            .iter()
            .any(|note| note.contains("live blocked: route_unsupported")));

        let combo_catalog = crate::combo_rfq_client::ComboMarketCatalog::from_markets(vec![
            crate::combo_rfq_client::ComboMarketEntry {
                condition_id: "cond-a".into(),
                position_ids: vec!["12345".into(), "67890".into()],
                outcomes: vec!["Yes".into(), "No".into()],
                slug: "a".into(),
            },
            crate::combo_rfq_client::ComboMarketEntry {
                condition_id: "cond-b".into(),
                position_ids: vec!["55555".into(), "99999".into()],
                outcomes: vec!["Yes".into(), "No".into()],
                slug: "b".into(),
            },
        ]);
        opp.execution_plan[1].condition_id = "cond-b".into();
        opp.execution_plan[1].token_id = "55555".into();
        let mut route_stats = ScanStats::default();
        assert!(skip_live_blocked_opportunity(
            &mut route_stats,
            true,
            true,
            false,
            None,
            42,
            &cfg,
            &opp,
            25.0,
            Some(&combo_catalog),
        ));
        assert_eq!(route_stats.combo_rfq_candidate_blocks, 1);
        assert!(route_stats
            .operator_notes
            .iter()
            .any(|note| note.contains("atomic_route=combo_rfq_candidate")));
        assert!(route_stats
            .operator_notes
            .iter()
            .any(|note| note
                .contains("combo_rfq_requester_execution=beta_accept_endpoint_documented")));
        assert!(route_stats
            .operator_notes
            .iter()
            .any(|note| note.contains("combo_rfq_requester_api_public=false")));
        assert!(route_stats
            .operator_notes
            .iter()
            .any(|note| note.contains("rfq_accept_window_ms=5000")));
        assert!(route_stats
            .operator_notes
            .iter()
            .any(|note| note.contains("protocol_preflight=")));
        assert!(route_stats
            .operator_notes
            .iter()
            .any(|note| note.contains("rfq_requester_api:blocked")));

        opp.execution_plan.truncate(1);
        opp.arb_type = ArbType::Ranked;
        assert_eq!(
            live_execution_block_reason(&opp),
            Some(LiveBlockReason::RankedUnsupported)
        );

        opp.arb_type = ArbType::Yes;
        opp.execution_plan[0].token_id.clear();
        assert_eq!(
            live_execution_block_reason(&opp),
            Some(LiveBlockReason::MissingToken)
        );

        opp.execution_plan[0].token_id = "external:kalshi:abc".into();
        assert_eq!(
            live_execution_block_reason(&opp),
            Some(LiveBlockReason::ExternalToken)
        );
    }
}
