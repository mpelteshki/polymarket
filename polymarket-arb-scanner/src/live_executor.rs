//! Live execution bridge using Polymarket's official Rust SDK.
//!
//! Safety posture:
//! - refresh prices immediately before trading;
//! - size baskets conservatively from current order-book depth;
//! - round every price to the market tick size;
//! - fail closed on stale edge or incomplete fills;
//! - cancel resting orders on timeout/failure when configured;
//! - set neg-risk metadata on the SDK client before building each order.
//!
//! The executor now follows the same explicit `execution_plan` used by paper mode,
//! so YES/NO/bundle/ranked baskets share one sizing and validation model.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::network::TransactionBuilder;
use alloy::primitives::{keccak256, Bytes};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner as AlloyLocalSigner;
use alloy::signers::Signer as AlloySigner;
use alloy::sol;
use alloy::sol_types::{Eip712Domain, SolStruct};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Kind, LocalSigner as ClobLocalSigner, Normal};
use polymarket_client_sdk_v2::clob::types::request::{
    BalanceAllowanceRequest, OrdersRequest, TradesRequest,
};
use polymarket_client_sdk_v2::clob::types::response::BalanceAllowanceResponse;
use polymarket_client_sdk_v2::clob::types::{
    Amount, AssetType, OrderPayload, OrderStatusType, OrderType, Side, SignatureType, SignedOrder,
    TickSize, TradeStatusType, TraderSide,
};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::contract_config;
use polymarket_client_sdk_v2::data::types::request::PositionsRequest;
use polymarket_client_sdk_v2::data::types::response::Position;
use polymarket_client_sdk_v2::data::Client as DataClient;
use polymarket_client_sdk_v2::types::{Address, Decimal, B256, U256};
use polymarket_client_sdk_v2::PRIVATE_KEY_VAR;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::accounting_snapshot;
use crate::clob_client;
use crate::config::Config;
use crate::engine_mode::{self, EngineMode};
use crate::execution_routes::LiveRouteKind;
use crate::exposure::{append_exposure_ledger_delta, SharedExposureTracker};
use crate::fees;
use crate::gas_oracle::GasOracle;
use crate::geoblock;
use crate::models::{
    is_external_token_id, is_supported_yes_no_full_family_plan, ArbType, ArbitrageOpportunity,
    Market, OpportunityLeg, OutcomeSide,
};
use crate::user_channel;
use crate::ws_client::PriceCache;

const LIVE_EXECUTION_JOURNAL_FILE: &str = "live_execution_journal.jsonl";
pub const LIVE_REALIZED_PNL_FILE: &str = "live_realized_pnl.jsonl";
const LIVE_CLOSEOUT_PLAN_FILE: &str = "live_closeout_plan.json";
const LIVE_CLOSEOUT_RUN_REPORT_FILE: &str = "live_closeout_run_report.json";
const LIVE_CLOSEOUT_PAYOFF_CERTIFICATE_FILE: &str = "live_closeout_payoff_certificate.json";
const LIVE_READINESS_REPORT_FILE: &str = "live_readiness_report.json";
const LIVE_ROUTE_SHADOW_JOURNAL_FILE: &str = "live_route_shadow_journal.jsonl";
const LIVE_ROUTE_REPLAY_JOURNAL_FILE: &str = "live_route_replay_journal.jsonl";
const LIVE_ROUTE_CALIBRATION_REPORT_FILE: &str = "live_route_calibration_report.json";
const COMBO_RFQ_ROUTE_PROMOTION_REPORT_FILE: &str = "combo_rfq_route_promotion_report.json";
const COMBO_RFQ_ROUTE: &str = "combo_rfq_candidate";
const CTF_MERGE_BUNDLE_SHADOW_ROUTE: &str = "ctf_merge_bundle_shadow";
const LIVE_PROCESS_LOCK_OWNER_FILE: &str = "owner.json";
const LIVE_GEOBLOCK_PRE_SUBMIT_ALLOW_TTL: Duration = Duration::from_secs(30);
const COMBO_RFQ_ROUTE_PROMOTION_CACHE_MAX_MS: u64 = 250;
#[cfg(not(test))]
static LIVE_GAS_ORACLE: OnceLock<GasOracle> = OnceLock::new();
static COMBO_RFQ_ROUTE_PROMOTION_CACHE: OnceLock<Mutex<Option<ComboRfqRoutePromotionCacheEntry>>> =
    OnceLock::new();
const CLOB_ORDER_EIP712_NAME: &str = "Polymarket CTF Exchange";
const CLOB_ORDER_EIP712_VERSION_V1: &str = "1";
const CLOB_ORDER_EIP712_VERSION_V2: &str = "2";
const MATCHING_ENGINE_PAUSE: Duration = Duration::from_secs(120);
const RATE_LIMIT_PAUSE: Duration = Duration::from_secs(15);
const TRANSIENT_ENGINE_PAUSE: Duration = Duration::from_secs(30);
const SERVER_CLOCK_SYNC_MAX_WAIT: Duration = Duration::from_millis(1_500);
const SERVER_CLOCK_SYNC_POLL: Duration = Duration::from_millis(25);
const LIVE_SDK_LOT_SIZE_STEP_SHARES: f64 = 0.01;
const LIVE_SDK_LOT_SIZE_SCALE: usize = 2;
const REPORTED_REALIZED_EV_MATCH_TOLERANCE_USD: f64 = 0.01;
const REALIZED_EV_RECOMPUTE_TOLERANCE_USD: f64 = 1e-6;
const STARTUP_POSITIONS_PAGE_LIMIT: i32 = 500;
const STARTUP_POSITIONS_MAX_OFFSET: i32 = 10_000;
const POLYGON_CHAIN_ID: u64 = 137;
const MIN_CLOSEOUT_NATIVE_GAS_WEI: u128 = 10_000_000_000_000_000;
// Source: https://docs.polymarket.com/resources/contracts
const POLYGON_CTF_COLLATERAL_ADAPTER: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";
const POLYGON_NEG_RISK_CTF_COLLATERAL_ADAPTER: &str = "0xadA2005600Dec949baf300f4C6120000bDB6eAab";
// Source: https://docs.polymarket.com/market-makers/combos
const POLYGON_COMBO_ROUTER: &str = "0x12121212006e4CD160D18e3f00711DA5c3372600";
const POLYGON_COMBO_POSITION_MANAGER: &str = "0x006F54F7f9A22e0000CC2AB60031000000ae9fEF";
const POLYMARKET_RELAYER_WALLET_SUBMIT_TO: &str = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07";
const RELAYER_TRANSACTION_POLL_INTERVAL: Duration = Duration::from_secs(2);

sol! {
    struct Call {
        address target;
        uint256 value;
        bytes data;
    }

    struct Batch {
        address wallet;
        uint256 nonce;
        uint256 deadline;
        Call[] calls;
    }

    #[sol(rpc)]
    interface ICtfCollateralAdapter {
        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata partition,
            uint256 amount
        ) external;

        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata indexSets
        ) external;
    }

    #[sol(rpc)]
    interface IERC20Balance {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IERC1155OperatorApproval {
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }

}

#[derive(Debug, Clone)]
struct ServerClock {
    offset_ms: i128,
    uncertainty_ms: i128,
}

#[derive(Debug, Clone)]
struct ServerTimeSample {
    server_secs: i128,
    local_received_ms: i128,
}

impl ServerClock {
    async fn sync(http: &Client, config: &Config) -> Result<Self> {
        let first = sample_server_time(http, config).await?;
        let mut previous = first.clone();
        let sync_budget = SERVER_CLOCK_SYNC_MAX_WAIT.min(Duration::from_millis(
            config.live_max_refresh_to_submit_ms.max(1),
        ));
        let deadline = Instant::now() + sync_budget;
        while Instant::now() < deadline {
            tokio::time::sleep(SERVER_CLOCK_SYNC_POLL).await;
            let next = sample_server_time(http, config).await?;
            if next.server_secs > previous.server_secs {
                let server_boundary_ms = next.server_secs * 1_000;
                let local_boundary_estimate_ms =
                    (previous.local_received_ms + next.local_received_ms) / 2;
                let skipped_secs = (next.server_secs - previous.server_secs - 1).max(0);
                let uncertainty_ms =
                    ((next.local_received_ms - previous.local_received_ms).abs() / 2).max(1)
                        + skipped_secs * 1_000;
                return Ok(Self {
                    offset_ms: server_boundary_ms - local_boundary_estimate_ms,
                    uncertainty_ms,
                });
            }
            previous = next;
        }

        Ok(Self {
            offset_ms: first.server_secs * 1_000 - first.local_received_ms,
            uncertainty_ms: 1_000,
        })
    }

    fn now_ms(&self) -> Result<i128> {
        Ok(local_unix_ms()? + self.offset_ms)
    }
}

fn i128_abs_saturating(value: i128) -> i128 {
    if value == i128::MIN {
        i128::MAX
    } else {
        value.abs()
    }
}

fn ensure_live_server_clock_guard(clock: &ServerClock, config: &Config) -> Result<()> {
    let uncertainty_ms = clock.uncertainty_ms.max(0);
    let max_uncertainty_ms = config.live_max_server_clock_uncertainty_ms.max(1) as i128;
    if uncertainty_ms > max_uncertainty_ms {
        bail!(
            "live server-clock guard blocked: uncertainty={}ms > LIVE_MAX_SERVER_CLOCK_UNCERTAINTY_MS={}ms",
            uncertainty_ms,
            config.live_max_server_clock_uncertainty_ms
        );
    }

    let offset_ms = i128_abs_saturating(clock.offset_ms);
    let max_offset_ms = config.live_max_server_clock_offset_ms.max(1) as i128;
    if offset_ms > max_offset_ms {
        bail!(
            "live server-clock guard blocked: abs_offset={}ms > LIVE_MAX_SERVER_CLOCK_OFFSET_MS={}ms",
            offset_ms,
            config.live_max_server_clock_offset_ms
        );
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct PlanLegSnapshot {
    market: Market,
    raw_ask: f64,
    limit_price: f64,
}

#[derive(Debug, Clone)]
struct LiveOrderLeg {
    market_index: usize,
    condition_id: String,
    token_id: String,
    side: Side,
    price: f64,
    raw_price: f64,
    size: f64,
    unit_shares: f64,
    tick_size: f64,
    question: String,
    outcome: OutcomeSide,
    min_order_shares: f64,
    neg_risk: Option<bool>,
    fee_rate: f64,
    fee_exponent: u32,
    venue_timestamp_ms: Option<u64>,
    venue_age_ms: Option<i64>,
    book_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LiveExecutionReport {
    pub position_usd: f64,
    pub projected_pnl_usd: f64,
    pub projected_roi_pct: f64,
    pub basket_units: f64,
    pub order_count: usize,
    pub order_ids: Vec<String>,
    pub trade_ids: Vec<String>,
    pub transaction_hashes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveRouteSupport {
    pub route: &'static str,
    pub supported: bool,
    pub reason: &'static str,
}

const LIVE_ROUTE_SUPPORT_MATRIX: &[LiveRouteSupport] = &[
    LiveRouteSupport {
        route: "YES/NO full-family neg-risk",
        supported: false,
        reason: "payoff shape can be proven, but live executor has no atomic multi-leg fill and unwind route",
    },
    LiveRouteSupport {
        route: "single-market YES+NO bundle",
        supported: false,
        reason: "standard CTF merge closeout exists, but live entry has no atomic two-leg fill route",
    },
    LiveRouteSupport {
        route: "MintSell split-and-sell",
        supported: false,
        reason: "split-and-sell remains read-only until split plus bid-side sells are atomic",
    },
    LiveRouteSupport {
        route: "Ranked/combinatorial basket",
        supported: false,
        reason: "ranked optimizer is scan/paper only and has no live atomic execution adapter",
    },
    LiveRouteSupport {
        route: "Combo/RFQ",
        supported: false,
        reason: "guarded beta route is available only through explicit Combo/RFQ promotion gates",
    },
];

pub fn live_route_support_matrix() -> &'static [LiveRouteSupport] {
    LIVE_ROUTE_SUPPORT_MATRIX
}

pub fn live_arbitrage_routes_available() -> bool {
    live_route_support_matrix()
        .iter()
        .any(|route| route.supported)
}

pub async fn ensure_configured_live_arbitrage_routes_available(config: &Config) -> Result<()> {
    if live_arbitrage_routes_available() {
        return Ok(());
    }
    if config.live_combo_rfq_route_enabled {
        ensure_combo_rfq_route_promoted(config).await?;
        return Ok(());
    }
    ensure_live_arbitrage_routes_available()
}

pub async fn ensure_combo_rfq_route_promoted(
    config: &Config,
) -> Result<ComboRfqRoutePromotionReport> {
    if let Some(report) = cached_combo_rfq_route_promotion_report(config) {
        return Ok(report);
    }
    let report = build_combo_rfq_route_promotion_report(config).await;
    if report.promoted {
        store_combo_rfq_route_promotion_report(config, report.clone());
        return Ok(report);
    }
    bail!(
        "live Combo/RFQ route is enabled but not promoted; blockers=[{}]",
        report.blockers.join("; ")
    );
}

pub fn ensure_live_arbitrage_routes_available() -> Result<()> {
    if live_arbitrage_routes_available() {
        return Ok(());
    }
    let route_summary = live_route_support_matrix()
        .iter()
        .map(|route| format!("{}: {}", route.route, route.reason))
        .collect::<Vec<_>>()
        .join("; ");
    bail!(
        "live trading requested, but no live arbitrage route is currently supported; route_matrix=[{}]",
        route_summary
    )
}

fn combo_rfq_route_promotion_cache() -> &'static Mutex<Option<ComboRfqRoutePromotionCacheEntry>> {
    COMBO_RFQ_ROUTE_PROMOTION_CACHE.get_or_init(|| Mutex::new(None))
}

fn combo_rfq_route_promotion_cache_ttl(config: &Config) -> Duration {
    Duration::from_millis(
        config
            .live_max_refresh_to_submit_ms
            .clamp(1, COMBO_RFQ_ROUTE_PROMOTION_CACHE_MAX_MS),
    )
}

fn cached_combo_rfq_route_promotion_report(
    config: &Config,
) -> Option<ComboRfqRoutePromotionReport> {
    let key = combo_rfq_route_promotion_cache_key(config);
    let ttl = combo_rfq_route_promotion_cache_ttl(config);
    let guard = combo_rfq_route_promotion_cache().lock().ok()?;
    let entry = guard.as_ref()?;
    if entry.key == key && entry.cached_at.elapsed() <= ttl {
        Some(entry.report.clone())
    } else {
        None
    }
}

fn store_combo_rfq_route_promotion_report(config: &Config, report: ComboRfqRoutePromotionReport) {
    if let Ok(mut guard) = combo_rfq_route_promotion_cache().lock() {
        *guard = Some(ComboRfqRoutePromotionCacheEntry {
            key: combo_rfq_route_promotion_cache_key(config),
            cached_at: Instant::now(),
            report,
        });
    }
}

fn combo_rfq_route_promotion_cache_key(config: &Config) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        config.diagnostics_dir.display(),
        config.live_combo_rfq_route_enabled,
        config.combo_rfq_api_url,
        config.combo_rfq_requester_api_url,
        config.relayer_api_url,
        config.relayer_api_key.len(),
        config.relayer_api_key_address,
        config.combo_rfq_requester_enabled,
        config.combo_rfq_accept_enabled,
        config.combo_rfq_requester_protocol_verified,
        config.combo_rfq_bearer_token.len(),
        config.combo_rfq_participant_id.len(),
        config.combo_rfq_exchange_v3_address,
        config.combo_rfq_finality_max_age_secs,
        config.combo_rfq_finality_min_confirmed_samples,
        config.combo_rfq_stream_enabled,
        config.live_route_calibration_min_samples,
        config.live_route_calibration_max_age_secs,
        config.live_trade_position_size_usd,
        config.live_signature_type,
        config.live_funder_address,
        config.live_closeout_enabled,
        config.live_closeout_dry_run,
        config.polygon_rpc_url,
        config.polygon_finalized_block_max_lag_blocks,
    )
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LiveReadinessState {
    Ready,
    Blocked,
    Unknown,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LiveReadinessCheck {
    pub key: &'static str,
    pub state: LiveReadinessState,
    pub detail: String,
}

impl LiveReadinessCheck {
    fn ready(key: &'static str, detail: impl Into<String>) -> Self {
        Self {
            key,
            state: LiveReadinessState::Ready,
            detail: detail.into(),
        }
    }

    fn blocked(key: &'static str, detail: impl Into<String>) -> Self {
        Self {
            key,
            state: LiveReadinessState::Blocked,
            detail: detail.into(),
        }
    }

    fn unknown(key: &'static str, detail: impl Into<String>) -> Self {
        Self {
            key,
            state: LiveReadinessState::Unknown,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LiveReadinessReport {
    pub generated_at: String,
    pub live_submissions_supported: bool,
    pub account_address: Option<String>,
    pub protocol_drift: crate::protocol_drift::ProtocolDriftReport,
    pub checks: Vec<LiveReadinessCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiveRouteShadowReport {
    pub generated_at: String,
    pub event_id: String,
    pub event_title: String,
    pub route: String,
    pub status: String,
    pub stages: Vec<String>,
    pub basket_units: f64,
    pub gross_edge_usd: f64,
    pub p_both_fill: f64,
    pub p_one_leg_fill: f64,
    pub p_ghost_revert: f64,
    pub orphan_closeout_loss_usd: f64,
    pub settlement_loss_usd: f64,
    pub latency_haircut_usd: f64,
    pub capital_lock_cost_usd: f64,
    pub toxicity_score: f64,
    pub calibrated_replay_samples: usize,
    pub risk_gate_pass: bool,
    pub expected_shadow_ev_usd: f64,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiveRouteReplayRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_id: Option<String>,
    pub generated_at: String,
    pub event_id: String,
    pub route: String,
    pub outcome_label: String,
    #[serde(default)]
    pub realized_ev_usd: Option<f64>,
    #[serde(default)]
    pub toxicity_score: Option<f64>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LiveRouteCalibrationBucket {
    pub route: String,
    pub shadow_samples: usize,
    pub labeled_samples: usize,
    pub realized_ev_samples: usize,
    pub min_required_samples: usize,
    pub p_both_fill_observed: f64,
    pub p_one_leg_fill_observed: f64,
    pub p_ghost_revert_observed: f64,
    pub avg_shadow_ev_usd: Option<f64>,
    pub avg_realized_ev_usd: Option<f64>,
    pub avg_toxicity_score: Option<f64>,
    pub latest_shadow_at: Option<String>,
    pub latest_label_at: Option<String>,
    pub risk_gate_pass: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LiveRouteCalibrationReport {
    pub generated_at: String,
    pub shadow_journal_path: String,
    pub replay_journal_path: String,
    pub shadow_samples: usize,
    pub labeled_replay_samples: usize,
    pub realized_ev_samples: usize,
    pub min_required_samples: usize,
    pub routes: Vec<LiveRouteCalibrationBucket>,
    pub risk_gate_pass: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ComboRfqRoutePromotionReport {
    pub generated_at: String,
    pub route: String,
    pub promoted: bool,
    pub checks: Vec<LiveReadinessCheck>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone)]
struct ComboRfqRoutePromotionCacheEntry {
    key: String,
    cached_at: Instant,
    report: ComboRfqRoutePromotionReport,
}

#[derive(Clone)]
struct LiveExecutionJournal {
    path: PathBuf,
    writer: Arc<Mutex<BufWriter<File>>>,
}

#[derive(Debug, Clone)]
struct LiveProcessLock {
    _inner: Arc<LiveProcessLockInner>,
}

#[derive(Debug)]
struct LiveProcessLockInner {
    path: PathBuf,
}

#[derive(Debug)]
struct LiveCloseoutSafetyPreflight {
    _process_lock: LiveProcessLock,
}

impl Drop for LiveProcessLockInner {
    fn drop(&mut self) {
        if let Err(err) = fs::remove_dir_all(&self.path) {
            warn!(
                "failed to remove live process lock {}: {err}",
                self.path.display()
            );
        }
    }
}

impl LiveProcessLock {
    fn acquire(root_dir: &Path, account_address: Address) -> Result<Self> {
        fs::create_dir_all(root_dir).with_context(|| {
            format!(
                "creating live process lock directory root {}",
                root_dir.display()
            )
        })?;
        let path = root_dir.join(format!(
            "live_execution_{}.lock",
            account_address.to_string().to_ascii_lowercase()
        ));
        match fs::create_dir(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let owner_path = path.join(LIVE_PROCESS_LOCK_OWNER_FILE);
                let owner = fs::read_to_string(&owner_path)
                    .unwrap_or_else(|read_err| format!("owner metadata unavailable: {read_err}"));
                bail!(
                    "another live executor appears to hold account lock {}: {}; stop the other process or remove the stale lock after verifying no live executor is running",
                    path.display(),
                    owner
                );
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("creating live process lock {}", path.display()));
            }
        }

        let owner_path = path.join(LIVE_PROCESS_LOCK_OWNER_FILE);
        let owner = serde_json::json!({
            "pid": std::process::id(),
            "account_address": account_address.to_string(),
            "created_at": Utc::now().to_rfc3339(),
        });
        if let Err(err) = fs::write(&owner_path, serde_json::to_vec_pretty(&owner)?) {
            let _ = fs::remove_dir_all(&path);
            return Err(err).with_context(|| {
                format!("writing live process lock owner {}", owner_path.display())
            });
        }

        Ok(Self {
            _inner: Arc::new(LiveProcessLockInner { path }),
        })
    }

    #[cfg(test)]
    fn path(&self) -> &Path {
        &self._inner.path
    }
}

#[derive(Debug, Clone, Default)]
struct LiveCircuitBreaker {
    paused_until: Arc<Mutex<Option<Instant>>>,
}

#[derive(Debug, Deserialize)]
struct LiveJournalStatusLine {
    execution_id: Option<String>,
    stage: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LiveJournalConditionLine {
    execution_id: Option<String>,
    stage: Option<String>,
    legs: Option<Vec<LiveJournalConditionLeg>>,
}

#[derive(Debug, Deserialize)]
struct LiveJournalConditionLeg {
    condition_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LiveJournalAccountingLine {
    execution_id: Option<String>,
    stage: Option<String>,
    event_id: Option<String>,
    position_usd: Option<f64>,
    actual_fill_cost_usd: Option<f64>,
    entry_fees_usd: Option<f64>,
    entry_gas_cost_usd: Option<f64>,
    actual_entry_cost_usd: Option<f64>,
    projected_pnl_usd: Option<f64>,
    projected_roi_pct: Option<f64>,
    basket_units: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct LiveExecutionAccountingSummary {
    latest_stage: Option<String>,
    event_id: Option<String>,
    position_usd: Option<f64>,
    actual_fill_cost_usd: Option<f64>,
    entry_fees_usd: Option<f64>,
    entry_gas_cost_usd: Option<f64>,
    actual_entry_cost_usd: Option<f64>,
    projected_pnl_usd: Option<f64>,
    projected_roi_pct: Option<f64>,
    basket_units: Option<f64>,
}

impl LiveExecutionAccountingSummary {
    fn entry_cost_basis_usd(&self) -> Option<f64> {
        self.actual_entry_cost_usd.or(self.position_usd)
    }

    fn release_amount_usd(&self) -> Option<f64> {
        self.entry_cost_basis_usd()
            .filter(|amount| amount.is_finite() && *amount > 0.0)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct LiveEntryAccounting {
    actual_fill_cost_usd: f64,
    entry_fees_usd: f64,
    entry_gas_cost_usd: f64,
}

impl LiveEntryAccounting {
    fn actual_entry_cost_usd(self) -> f64 {
        self.actual_fill_cost_usd + self.entry_fees_usd + self.entry_gas_cost_usd
    }
}

#[derive(Debug, Serialize)]
struct LiveRealizedPnlRecord {
    timestamp: String,
    execution_id: Option<String>,
    closeout_action_id: String,
    condition_id: String,
    action: String,
    transaction_hash: String,
    block_number: u64,
    p_usd_balance_before_units: String,
    p_usd_balance_after_units: String,
    p_usd_delta_units: String,
    p_usd_delta_usd: f64,
    allocated_p_usd_delta_usd: f64,
    allocation_ratio: f64,
    projected_position_usd: Option<f64>,
    actual_fill_cost_usd: Option<f64>,
    entry_fees_usd: Option<f64>,
    entry_gas_cost_usd: Option<f64>,
    actual_entry_cost_usd: Option<f64>,
    projected_pnl_usd: Option<f64>,
    projected_roi_pct: Option<f64>,
    closeout_gas_used: u64,
    closeout_effective_gas_price_wei: String,
    closeout_gas_cost_wei: String,
    closeout_gas_cost_pol: f64,
    closeout_gas_cost_usd: f64,
    allocated_closeout_gas_cost_usd: f64,
    realized_pnl_usd_before_closeout_gas: Option<f64>,
    realized_pnl_usd: Option<f64>,
    receipt_total_logs: usize,
    receipt_adapter_logs: usize,
    receipt_collateral_transfer_to_account_logs: usize,
    receipt_ctf_transfer_logs: usize,
}

#[derive(Debug, Deserialize)]
struct LiveRealizedPnlKeyLine {
    execution_id: Option<String>,
    closeout_action_id: Option<String>,
    transaction_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndependentRealizedPnlLine {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    execution_id: Option<String>,
    #[serde(default)]
    closeout_action_id: Option<String>,
    #[serde(default)]
    transaction_hash: Option<String>,
    #[serde(default)]
    block_number: Option<u64>,
    #[serde(default)]
    status_class: Option<String>,
    #[serde(default)]
    realized_ev_usd: Option<f64>,
    #[serde(default)]
    realized_pnl_usd: Option<f64>,
    #[serde(default)]
    allocated_p_usd_delta_usd: Option<f64>,
    #[serde(default)]
    projected_position_usd: Option<f64>,
    #[serde(default)]
    actual_entry_cost_usd: Option<f64>,
    #[serde(default)]
    allocated_closeout_gas_cost_usd: Option<f64>,
    #[serde(default)]
    receipt_total_logs: Option<usize>,
    #[serde(default)]
    receipt_collateral_transfer_to_account_logs: Option<usize>,
    #[serde(default)]
    receipt_ctf_transfer_logs: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ComboRfqRealizedPnlRecord {
    pub timestamp: String,
    pub source: String,
    pub execution_id: Option<String>,
    pub closeout_action_id: String,
    pub condition_id: String,
    pub action: String,
    pub transaction_hash: String,
    pub block_number: Option<u64>,
    pub finality_id: String,
    pub rfq_id: Option<String>,
    pub quote_id: Option<String>,
    pub maker_id: Option<String>,
    pub status: String,
    pub status_class: String,
    pub realized_ev_usd: f64,
    pub expected_edge_usd: Option<f64>,
    pub price: Option<f64>,
    pub qty_decimal: Option<f64>,
    pub order_hash: Option<String>,
    pub token_id: Option<String>,
    pub fee: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct CloseoutReceiptValidation {
    total_logs: usize,
    adapter_logs: usize,
    collateral_transfer_to_account_logs: usize,
    ctf_transfer_logs: usize,
}

#[derive(Debug, Clone)]
struct CloseoutReceiptLogSummary {
    address: Address,
    topics: Vec<B256>,
}

#[derive(Debug, Clone, Copy)]
struct CloseoutGasAccounting {
    gas_used: u64,
    effective_gas_price_wei: u128,
    gas_cost_wei: U256,
    gas_cost_pol: f64,
    gas_cost_usd: f64,
}

#[derive(Debug, Serialize)]
pub struct LiveCloseoutPlan {
    generated_at: String,
    account_address: String,
    open_positions: usize,
    combo_exposure: crate::combo_rfq_client::ComboExposureReport,
    actions: Vec<LiveCloseoutAction>,
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
struct LiveCloseoutAction {
    action: String,
    condition_id: String,
    title: String,
    slug: String,
    negative_risk: bool,
    amount_shares: String,
    yes_asset: Option<String>,
    yes_size: Option<String>,
    no_asset: Option<String>,
    no_size: Option<String>,
    combo_position_id: Option<String>,
    combo_outcome_index: Option<u8>,
    note: String,
}

#[derive(Debug, Serialize)]
pub struct LiveCloseoutRunReport {
    generated_at: String,
    account_address: String,
    combo_exposure: crate::combo_rfq_client::ComboExposureReport,
    dry_run: bool,
    execution_enabled: bool,
    max_actions: usize,
    planned_actions: usize,
    selected_actions: usize,
    skipped_actions: usize,
    actions: Vec<LiveCloseoutRunAction>,
    note: String,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct LiveCloseoutPayoffCertificate {
    generated_at: String,
    account_address: String,
    status: String,
    open_positions: usize,
    planned_actions: usize,
    certified_actions: usize,
    blocked_actions: usize,
    skipped_actions: usize,
    residual_condition_count: usize,
    residual_position_count: usize,
    residual_shares: String,
    unresolved_execution_count: usize,
    combo_exposure_status: String,
    combo_open_count: usize,
    combo_redeemable_count: usize,
    combo_total_cost_usdc: f64,
    deterministic_min_terminal_payout_usd: f64,
    estimated_closeout_gas_usd: Option<f64>,
    closeout_gas_source: String,
    actions: Vec<LiveCloseoutPayoffCertificateAction>,
    blockers: Vec<String>,
    note: String,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
struct LiveCloseoutPayoffCertificateAction {
    action_id: String,
    action: String,
    condition_id: String,
    negative_risk: bool,
    amount_shares: String,
    yes_asset: Option<String>,
    no_asset: Option<String>,
    combo_position_id: Option<String>,
    combo_outcome_index: Option<u8>,
    amount_ctf_units: Option<String>,
    collateral_token: Option<String>,
    target_contract: Option<String>,
    parent_collection_id: String,
    partition: Vec<u8>,
    calldata: Option<String>,
    eth_call_block: String,
    expected_position_delta: String,
    expected_collateral_delta: String,
    expected_pusd_delta_usd: f64,
    deterministic_payout_usd: f64,
    payoff_proof: String,
    execution_preflight_status: String,
    unresolved_execution_ids: Vec<String>,
    blockers: Vec<String>,
    status: String,
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
struct LiveCloseoutRunAction {
    action_id: String,
    action: String,
    kind: String,
    condition_id: String,
    title: String,
    slug: String,
    negative_risk: bool,
    amount_shares: String,
    yes_asset: Option<String>,
    no_asset: Option<String>,
    combo_position_id: Option<String>,
    combo_outcome_index: Option<u8>,
    wallet_type: String,
    target_contract: Option<String>,
    calldata: Option<String>,
    value: String,
    call_preview: LiveCloseoutCallPreview,
    collateral_token: Option<String>,
    amount_ctf_units: Option<String>,
    expected_position_delta: String,
    verification_query: String,
    blockers: Vec<String>,
    unresolved_execution_ids: Vec<String>,
    transaction_hash: Option<String>,
    block_number: Option<u64>,
    reconciled_execution_ids: Vec<String>,
    status: String,
    reason: String,
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
struct LiveCloseoutCallPreview {
    function: String,
    from: Option<String>,
    target_contract: Option<String>,
    collateral_token: Option<String>,
    condition_id: String,
    parent_collection_id: String,
    partition: Vec<u8>,
    amount_ctf_units: Option<String>,
    expected_collateral_delta: String,
    eth_call_block: String,
    eth_call_status: String,
    eth_call_note: String,
}

#[derive(Debug, Clone)]
struct PositionView {
    asset: String,
    condition_id: String,
    size: Decimal,
    title: String,
    slug: String,
    outcome_index: i32,
    redeemable: bool,
    mergeable: bool,
    negative_risk: bool,
}

impl From<&Position> for PositionView {
    fn from(position: &Position) -> Self {
        Self {
            asset: position.asset.to_string(),
            condition_id: position.condition_id.to_string(),
            size: position.size,
            title: position.title.clone(),
            slug: position.slug.clone(),
            outcome_index: position.outcome_index,
            redeemable: position.redeemable,
            mergeable: position.mergeable,
            negative_risk: position.negative_risk,
        }
    }
}

#[derive(Debug, Serialize)]
struct LiveJournalLeg {
    condition_id: String,
    token_id: String,
    question: String,
    outcome: String,
    side: String,
    raw_price: f64,
    limit_price: f64,
    size: f64,
    unit_shares: f64,
    tick_size: f64,
    neg_risk: Option<bool>,
    fee_rate: f64,
    fee_exponent: u32,
    venue_timestamp_ms: Option<u64>,
    venue_age_ms: Option<i64>,
    book_hash: Option<String>,
}

#[derive(Debug, Serialize)]
struct LiveRouteQuoteSnapshotLeg {
    token_id: String,
    book_hash: Option<String>,
    venue_timestamp_ms: Option<u64>,
    venue_age_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
struct LiveRouteQuoteSnapshot {
    refresh_id: String,
    token_ids: Vec<String>,
    venue_timestamp_min_ms: Option<u64>,
    venue_timestamp_max_ms: Option<u64>,
    venue_timestamp_skew_ms: Option<u64>,
    max_venue_age_ms: Option<i64>,
    missing_book_hashes: usize,
    missing_venue_timestamps: usize,
    legs: Vec<LiveRouteQuoteSnapshotLeg>,
}

#[derive(Debug, Serialize)]
struct LiveJournalRecord {
    timestamp: String,
    execution_id: String,
    stage: String,
    event_id: String,
    event_title: String,
    arb_type: String,
    position_usd: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    actual_fill_cost_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entry_fees_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entry_gas_cost_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actual_entry_cost_usd: Option<f64>,
    projected_pnl_usd: f64,
    projected_roi_pct: f64,
    basket_units: f64,
    order_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    expected_order_hashes: Vec<String>,
    trade_ids: Vec<String>,
    transaction_hashes: Vec<String>,
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    route_quote_snapshot: Option<LiveRouteQuoteSnapshot>,
    legs: Vec<LiveJournalLeg>,
}

impl LiveExecutionJournal {
    fn new(root_dir: &Path) -> Result<Self> {
        fs::create_dir_all(root_dir)
            .with_context(|| format!("creating diagnostics directory {}", root_dir.display()))?;
        let path = root_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
        let unresolved = unresolved_journal_executions(&path)?;
        if !unresolved.is_empty() {
            bail!(
                "live execution journal has unresolved exposure/order state in {}: {:?}; reconcile account state, then append a manual_reconciled line for each execution_id or archive the journal before enabling live",
                path.display(),
                unresolved
            );
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("opening live execution journal {}", path.display()))?;
        Ok(Self {
            path,
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn record(&self, record: &LiveJournalRecord) -> Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| anyhow!("live execution journal lock poisoned"))?;
        serde_json::to_writer(&mut *writer, record)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }
}

impl LiveCircuitBreaker {
    fn check(&self) -> Result<()> {
        let mut paused_until = self
            .paused_until
            .lock()
            .map_err(|_| anyhow!("live circuit breaker lock poisoned"))?;
        if let Some(until) = *paused_until {
            let now = Instant::now();
            if until > now {
                bail!(
                    "live circuit breaker paused for {}ms",
                    until.duration_since(now).as_millis()
                );
            }
            *paused_until = None;
        }
        Ok(())
    }

    fn trip_for_error(&self, err: &dyn std::fmt::Display) {
        let message = err.to_string();
        let Some((duration, reason)) = live_error_pause(&message) else {
            return;
        };
        let until = Instant::now() + duration;
        match self.paused_until.lock() {
            Ok(mut paused_until) => {
                if paused_until.map(|old| old < until).unwrap_or(true) {
                    *paused_until = Some(until);
                }
            }
            Err(_) => warn!("live circuit breaker lock poisoned while tripping"),
        }
        warn!(
            "Live circuit breaker paused for {}s after {reason}: {message}",
            duration.as_secs()
        );
    }
}

fn live_error_pause(message: &str) -> Option<(Duration, &'static str)> {
    let observation = engine_mode::classify_engine_mode_observation(
        Utc::now(),
        "live_executor",
        "error_text",
        None,
        None,
        Some(message),
    );
    match observation.mode {
        EngineMode::Restarting | EngineMode::PostOnly | EngineMode::CancelOnly => {
            return Some((MATCHING_ENGINE_PAUSE, "matching-engine pause"));
        }
        EngineMode::Disabled | EngineMode::TransientError => {
            return Some((TRANSIENT_ENGINE_PAUSE, "transient exchange error"));
        }
        EngineMode::RateLimited => return Some((RATE_LIMIT_PAUSE, "rate limit")),
        EngineMode::Normal | EngineMode::Unknown => {}
    }

    let lower = message.to_ascii_lowercase();
    if lower.contains("425")
        || lower.contains("post_only")
        || lower.contains("post-only")
        || lower.contains("cancel_only")
        || lower.contains("cancel-only")
        || lower.contains("cancel only")
        || lower.contains("matching engine")
        || lower.contains("clob final depth")
        || lower.contains("order match delayed")
        || lower.contains("match delayed due to market conditions")
        || lower.contains("market is not yet ready")
        || lower.contains("not yet ready to process new orders")
    {
        return Some((MATCHING_ENGINE_PAUSE, "matching-engine pause"));
    }
    if lower.contains("429") || lower.contains("rate limit") || lower.contains("too many requests")
    {
        return Some((RATE_LIMIT_PAUSE, "rate limit"));
    }
    if lower.contains("503")
        || lower.contains("502")
        || lower.contains("504")
        || lower.contains("timeout")
        || lower.contains("timed out")
    {
        return Some((TRANSIENT_ENGINE_PAUSE, "transient exchange error"));
    }
    None
}

fn ensure_funder_policy(signature_type: u8, funder: Option<Address>) -> Result<()> {
    if signature_type != 0 && funder.is_none() {
        bail!(
            "LIVE_FUNDER_ADDRESS is required when LIVE_SIGNATURE_TYPE={} so startup position reconciliation checks the actual proxy/safe account",
            signature_type
        );
    }
    Ok(())
}

fn journal_stage_is_reconciled(stage: &str) -> bool {
    matches!(
        stage,
        "pre_submit_released" | "submit_rejected_released" | "manual_reconciled"
    )
}

fn unresolved_journal_executions(path: &Path) -> Result<Vec<String>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading live execution journal {}", path.display()))?;
    let mut latest: HashMap<String, String> = HashMap::new();
    for (idx, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: LiveJournalStatusLine = serde_json::from_str(line).with_context(|| {
            format!(
                "live execution journal {} has malformed JSON at line {}",
                path.display(),
                idx + 1
            )
        })?;
        let execution_id = parsed.execution_id.with_context(|| {
            format!(
                "live execution journal {} missing execution_id at line {}",
                path.display(),
                idx + 1
            )
        })?;
        let stage = parsed.stage.with_context(|| {
            format!(
                "live execution journal {} missing stage at line {}",
                path.display(),
                idx + 1
            )
        })?;
        latest.insert(execution_id, stage);
    }

    let mut unresolved: Vec<String> = latest
        .into_iter()
        .filter_map(|(execution_id, stage)| {
            if journal_stage_is_reconciled(&stage) {
                None
            } else {
                Some(format!("{execution_id}:{stage}"))
            }
        })
        .collect();
    unresolved.sort();
    Ok(unresolved)
}

fn unresolved_journal_executions_by_condition(path: &Path) -> Result<HashMap<String, Vec<String>>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading live execution journal {}", path.display()))?;
    let mut latest_stage: HashMap<String, String> = HashMap::new();
    let mut conditions_by_execution: HashMap<String, Vec<String>> = HashMap::new();

    for (idx, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: LiveJournalConditionLine = serde_json::from_str(line).with_context(|| {
            format!(
                "live execution journal {} has malformed JSON at line {}",
                path.display(),
                idx + 1
            )
        })?;
        let execution_id = parsed.execution_id.with_context(|| {
            format!(
                "live execution journal {} missing execution_id at line {}",
                path.display(),
                idx + 1
            )
        })?;
        let stage = parsed.stage.with_context(|| {
            format!(
                "live execution journal {} missing stage at line {}",
                path.display(),
                idx + 1
            )
        })?;
        latest_stage.insert(execution_id.clone(), stage);

        if let Some(legs) = parsed.legs {
            let mut seen = HashSet::new();
            let condition_ids: Vec<String> = legs
                .into_iter()
                .filter_map(|leg| leg.condition_id)
                .map(|condition_id| condition_id.trim().to_string())
                .filter(|condition_id| !condition_id.is_empty())
                .filter(|condition_id| seen.insert(condition_id.clone()))
                .collect();
            if !condition_ids.is_empty() {
                conditions_by_execution.insert(execution_id, condition_ids);
            }
        }
    }

    let mut by_condition: HashMap<String, Vec<String>> = HashMap::new();
    for (execution_id, stage) in latest_stage {
        if journal_stage_is_reconciled(&stage) {
            continue;
        }
        if let Some(condition_ids) = conditions_by_execution.get(&execution_id) {
            for condition_id in condition_ids {
                by_condition
                    .entry(condition_id.clone())
                    .or_default()
                    .push(execution_id.clone());
            }
        }
    }

    for execution_ids in by_condition.values_mut() {
        execution_ids.sort();
        execution_ids.dedup();
    }
    Ok(by_condition)
}

#[derive(Clone)]
pub struct LiveExecutor {
    private_key: String,
    sdk_client: ClobClient<Authenticated<Normal>>,
    account_address: Address,
    submit_lock: Arc<tokio::sync::Mutex<()>>,
    _process_lock: LiveProcessLock,
    journal: LiveExecutionJournal,
    circuit_breaker: LiveCircuitBreaker,
    server_clock: ServerClock,
    geoblock_last_allowed_at: Arc<tokio::sync::Mutex<Option<Instant>>>,
}

impl LiveExecutor {
    pub async fn new(config: &Config) -> Result<Self> {
        let http = Client::new();
        geoblock::ensure_live_geoblock_allows_trading(&http, config, "executor-startup").await?;
        engine_mode::ensure_no_active_new_order_blocker(config)?;
        ensure_no_unresolved_combo_rfq_execution(config)?;
        user_channel::ensure_live_user_channel_configured(config)?;
        let private_key = std::env::var(PRIVATE_KEY_VAR)
            .with_context(|| format!("missing {PRIVATE_KEY_VAR} env var for live execution"))?;
        let signer =
            ClobLocalSigner::from_str(&private_key)?.with_chain_id(Some(config.live_chain_id));
        let funder = parse_live_funder_address(config)?;
        ensure_funder_policy(config.live_signature_type, funder)?;
        let account_address = funder.unwrap_or_else(|| signer.address());
        let process_lock = LiveProcessLock::acquire(&config.diagnostics_dir, account_address)?;
        let journal = LiveExecutionJournal::new(&config.diagnostics_dir)?;
        ensure_live_accounting_snapshot_clean(&http, config, account_address).await?;

        let mut auth_builder = ClobClient::new(&config.clob_api_url, ClobConfig::default())?
            .authentication_builder(&signer)
            .signature_type(signature_type_from_u8(config.live_signature_type)?);

        if let Some(funder) = funder {
            auth_builder = auth_builder.funder(funder);
        }

        let sdk_client = auth_builder.authenticate().await?;
        ensure_live_heartbeats_active(&sdk_client)?;
        ensure_live_account_not_closed_only(&sdk_client).await?;
        let _ = refresh_live_balance_allowance(&sdk_client).await?;
        verify_clean_startup_account(&sdk_client, account_address).await?;
        let server_clock = ServerClock::sync(&http, config).await?;
        ensure_live_server_clock_guard(&server_clock, config)?;
        info!("Live execution journal: {}", journal.path().display());
        Ok(Self {
            private_key,
            sdk_client,
            account_address,
            submit_lock: Arc::new(tokio::sync::Mutex::new(())),
            _process_lock: process_lock,
            journal,
            circuit_breaker: LiveCircuitBreaker::default(),
            server_clock,
            geoblock_last_allowed_at: Arc::new(tokio::sync::Mutex::new(Some(Instant::now()))),
        })
    }

    fn signer(&self, config: &Config) -> Result<impl polymarket_client_sdk_v2::auth::Signer> {
        Ok(ClobLocalSigner::from_str(&self.private_key)?.with_chain_id(Some(config.live_chain_id)))
    }
}

async fn ensure_status_page_allows_live_orders(http: &Client, config: &Config) -> Result<()> {
    if let Some(report) = engine_mode::poll_status_page_summary(http, config).await? {
        if report.active {
            bail!(
                "Polymarket status page blocks live orders: {}",
                report.blockers.join("|")
            );
        }
    }
    engine_mode::ensure_no_active_new_order_blocker(config)
}

fn ensure_no_unresolved_combo_rfq_execution(config: &Config) -> Result<()> {
    let unresolved = crate::combo_rfq_client::unresolved_combo_rfq_execution_records(config)?;
    if unresolved.is_empty() {
        return Ok(());
    }
    let sample = unresolved
        .iter()
        .take(5)
        .map(|record| {
            format!(
                "{}:{}:{:?}:{:?}",
                record.client_request_id, record.status, record.rfq_id, record.quote_id
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    bail!(
        "unresolved Combo/RFQ execution recovery required before live startup: count={} sample={}",
        unresolved.len(),
        sample
    )
}

fn ensure_live_heartbeats_active<K>(sdk_client: &ClobClient<Authenticated<K>>) -> Result<()>
where
    K: Kind,
{
    if !sdk_client.heartbeats_active() {
        bail!("authenticated Polymarket SDK client did not start automatic heartbeats");
    }
    Ok(())
}

fn live_pre_submit_heartbeat_timeout(config: &Config) -> Option<Duration> {
    if config.live_pre_submit_heartbeat_enabled {
        Some(Duration::from_millis(
            config.live_pre_submit_heartbeat_timeout_ms.max(1),
        ))
    } else {
        None
    }
}

async fn ensure_live_pre_submit_heartbeat<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    config: &Config,
) -> Result<()>
where
    K: Kind,
{
    let Some(timeout) = live_pre_submit_heartbeat_timeout(config) else {
        return Ok(());
    };
    let ack = tokio::time::timeout(timeout, sdk_client.post_heartbeat(None))
        .await
        .with_context(|| {
            format!(
                "pre-submit heartbeat timed out after {}ms",
                timeout.as_millis()
            )
        })?
        .context("pre-submit heartbeat request failed")?;
    debug!("Pre-submit heartbeat acknowledged: {}", ack.heartbeat_id);
    Ok(())
}

async fn ensure_live_pre_submit_geoblock(
    http: &Client,
    config: &Config,
    last_allowed_at: &Arc<tokio::sync::Mutex<Option<Instant>>>,
) -> Result<()> {
    let now = Instant::now();
    if last_allowed_at.lock().await.is_some_and(|last_allowed_at| {
        now.saturating_duration_since(last_allowed_at) <= LIVE_GEOBLOCK_PRE_SUBMIT_ALLOW_TTL
    }) {
        return Ok(());
    }

    geoblock::ensure_live_geoblock_allows_trading(http, config, "pre-submit").await?;
    *last_allowed_at.lock().await = Some(Instant::now());
    Ok(())
}

fn parse_live_funder_address(config: &Config) -> Result<Option<Address>> {
    if config.live_funder_address.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            Address::from_str(config.live_funder_address.trim())
                .context("failed to parse LIVE_FUNDER_ADDRESS")?,
        ))
    }
}

pub fn configured_live_account_address(config: &Config) -> Result<Address> {
    let private_key = std::env::var(PRIVATE_KEY_VAR).with_context(|| {
        format!("missing {PRIVATE_KEY_VAR} env var for live account reconciliation")
    })?;
    let signer = ClobLocalSigner::from_str(&private_key)?.with_chain_id(Some(config.live_chain_id));
    let funder = parse_live_funder_address(config)?;
    ensure_funder_policy(config.live_signature_type, funder)?;
    Ok(funder.unwrap_or_else(|| signer.address()))
}

pub async fn ensure_configured_accounting_snapshot_clean(
    http: &Client,
    config: &Config,
) -> Result<()> {
    let account_address = configured_live_account_address(config)?;
    ensure_live_accounting_snapshot_clean(http, config, account_address).await
}

async fn ensure_live_accounting_snapshot_clean(
    http: &Client,
    config: &Config,
    account_address: Address,
) -> Result<()> {
    if !config.live_accounting_snapshot_enabled {
        return Ok(());
    }
    let report = accounting_snapshot::fetch_and_write_accounting_snapshot_report(
        http,
        config,
        account_address,
    )
    .await?;
    if report.blocks_live() {
        bail!(
            "Polymarket accounting snapshot blocks live orders: {}",
            report.blockers.join("|")
        );
    }
    Ok(())
}

async fn build_readiness_sdk_client(config: &Config) -> Result<ClobClient<Authenticated<Normal>>> {
    let private_key = std::env::var(PRIVATE_KEY_VAR)
        .with_context(|| format!("missing {PRIVATE_KEY_VAR} env var for live readiness"))?;
    let signer = ClobLocalSigner::from_str(&private_key)?.with_chain_id(Some(config.live_chain_id));
    let funder = parse_live_funder_address(config)?;
    ensure_funder_policy(config.live_signature_type, funder)?;
    let mut auth_builder = ClobClient::new(&config.clob_api_url, ClobConfig::default())?
        .authentication_builder(&signer)
        .signature_type(signature_type_from_u8(config.live_signature_type)?);
    if let Some(funder) = funder {
        auth_builder = auth_builder.funder(funder);
    }
    auth_builder
        .authenticate()
        .await
        .context("failed to authenticate CLOB SDK client for live readiness")
}

pub async fn build_live_readiness_report(config: &Config) -> LiveReadinessReport {
    let mut checks = Vec::new();
    let protocol_drift = crate::protocol_drift::build_protocol_drift_report(config);
    let combo_rfq_promotion = build_combo_rfq_route_promotion_report(config).await;
    let live_submissions_supported =
        live_arbitrage_routes_available() || combo_rfq_promotion.promoted;
    checks.push(if live_submissions_supported {
        LiveReadinessCheck::ready(
            "live_route_matrix",
            "at_least_one_live_arbitrage_route_supported",
        )
    } else {
        LiveReadinessCheck::blocked(
            "live_route_matrix",
            format!(
                "combo_rfq_route_not_promoted:{}",
                combo_rfq_promotion.blockers.join("|")
            ),
        )
    });
    checks.push(market_data_config_readiness_check(config));
    checks.push(engine_mode_readiness_check(config));
    checks.push(contract_readiness_check(
        config,
        false,
        "standard_contract_config",
    ));
    checks.push(contract_readiness_check(
        config,
        true,
        "neg_risk_contract_config",
    ));
    checks.push(protocol_drift_readiness_check(&protocol_drift));
    checks.push(user_channel_config_readiness_check(config));
    checks.push(user_channel_ready_readiness_check(config));
    checks.push(closeout_execution_readiness_check(config));

    let account_address = match configured_live_account_address(config) {
        Ok(address) => {
            checks.push(LiveReadinessCheck::ready(
                "account_identity",
                format!("account={address}"),
            ));
            Some(address)
        }
        Err(err) => {
            checks.push(LiveReadinessCheck::blocked(
                "account_identity",
                format!("account_unavailable:{err}"),
            ));
            None
        }
    };

    if let Some(account_address) = account_address {
        checks.push(erc1155_operator_approval_readiness_check(config, account_address).await);
        checks.push(accounting_snapshot_readiness_check(config, account_address).await);
        checks.push(native_pol_readiness_check(config, account_address).await);
    } else {
        checks.push(LiveReadinessCheck::blocked(
            "erc1155_operator_approval",
            "account_identity_required_before_erc1155_operator_probe",
        ));
        checks.push(if config.live_accounting_snapshot_enabled {
            LiveReadinessCheck::blocked(
                "accounting_snapshot",
                "account_identity_required_before_accounting_snapshot_probe",
            )
        } else {
            LiveReadinessCheck::unknown(
                "accounting_snapshot",
                "LIVE_ACCOUNTING_SNAPSHOT_ENABLED=false",
            )
        });
        checks.push(LiveReadinessCheck::blocked(
            "native_pol_balance",
            "account_identity_required_before_native_balance_probe",
        ));
    }

    match build_readiness_sdk_client(config).await {
        Ok(sdk_client) => {
            checks.push(LiveReadinessCheck::ready(
                "authenticated_clob_client",
                "authenticated_clob_client_available",
            ));
            checks.push(match ensure_live_heartbeats_active(&sdk_client) {
                Ok(()) => LiveReadinessCheck::ready(
                    "sdk_heartbeats",
                    "authenticated_sdk_heartbeats_active",
                ),
                Err(err) => LiveReadinessCheck::blocked(
                    "sdk_heartbeats",
                    format!("authenticated_sdk_heartbeats_inactive:{err}"),
                ),
            });
            checks.push(
                match ensure_live_account_not_closed_only(&sdk_client).await {
                    Ok(()) => {
                        LiveReadinessCheck::ready("closed_only_status", "account_not_closed_only")
                    }
                    Err(err) => LiveReadinessCheck::blocked(
                        "closed_only_status",
                        format!("closed_only_probe_failed:{err}"),
                    ),
                },
            );
            match refresh_live_balance_allowance(&sdk_client).await {
                Ok(balance_allowance) => {
                    let mut collateral_checks =
                        collateral_readiness_checks(config, &balance_allowance);
                    if let Some(account_address) = account_address {
                        let sdk_v3_ready = collateral_checks.iter().any(|check| {
                            check.key == "exchange_v3_allowance"
                                && check.state == LiveReadinessState::Ready
                        });
                        if !sdk_v3_ready {
                            collateral_checks.retain(|check| check.key != "exchange_v3_allowance");
                            let required =
                                decimal_from_usd(config.live_trade_position_size_usd.max(0.0))
                                    .unwrap_or(Decimal::ZERO);
                            collateral_checks.push(
                                exchange_v3_allowance_rpc_readiness_check(
                                    config,
                                    account_address,
                                    &required,
                                )
                                .await,
                            );
                        }
                    }
                    checks.extend(collateral_checks);
                }
                Err(err) => {
                    checks.push(LiveReadinessCheck::blocked(
                        "pusd_balance",
                        format!("collateral_balance_probe_failed:{err}"),
                    ));
                    checks.push(LiveReadinessCheck::blocked(
                        "pusd_allowance_exchange_v2_standard",
                        format!("collateral_allowance_probe_failed:{err}"),
                    ));
                    checks.push(LiveReadinessCheck::blocked(
                        "pusd_allowance_exchange_v2_neg_risk",
                        format!("collateral_allowance_probe_failed:{err}"),
                    ));
                    checks.push(LiveReadinessCheck::blocked(
                        "exchange_v3_allowance",
                        format!("collateral_allowance_probe_failed:{err}"),
                    ));
                }
            }
            if let Some(account_address) = account_address {
                checks.push(
                    match verify_clean_startup_account(&sdk_client, account_address).await {
                        Ok(()) => LiveReadinessCheck::ready(
                            "clean_startup_account",
                            "no_open_orders_or_positions_detected",
                        ),
                        Err(err) => LiveReadinessCheck::blocked(
                            "clean_startup_account",
                            format!("startup_account_not_clean:{err}"),
                        ),
                    },
                );
            }
        }
        Err(err) => {
            checks.push(LiveReadinessCheck::blocked(
                "authenticated_clob_client",
                format!("authenticated_clob_unavailable:{err}"),
            ));
            checks.push(LiveReadinessCheck::blocked(
                "closed_only_status",
                "authenticated_clob_client_required_before_closed_only_probe",
            ));
            checks.push(LiveReadinessCheck::blocked(
                "pusd_balance",
                "authenticated_clob_client_required_before_balance_probe",
            ));
            checks.push(LiveReadinessCheck::blocked(
                "pusd_allowance_exchange_v2_standard",
                "authenticated_clob_client_required_before_allowance_probe",
            ));
            checks.push(LiveReadinessCheck::blocked(
                "pusd_allowance_exchange_v2_neg_risk",
                "authenticated_clob_client_required_before_allowance_probe",
            ));
            checks.push(LiveReadinessCheck::blocked(
                "exchange_v3_allowance",
                "authenticated_clob_client_required_before_exchange_v3_allowance_probe",
            ));
            checks.push(LiveReadinessCheck::blocked(
                "clean_startup_account",
                "authenticated_clob_client_required_before_open_order_position_probe",
            ));
        }
    }

    LiveReadinessReport {
        generated_at: Utc::now().to_rfc3339(),
        live_submissions_supported,
        account_address: account_address.map(|address| address.to_string()),
        protocol_drift,
        checks,
    }
}

fn engine_mode_readiness_check(config: &Config) -> LiveReadinessCheck {
    match engine_mode::build_engine_mode_report(config) {
        Ok(report) if report.active => LiveReadinessCheck::blocked(
            "clob_engine_mode",
            format!(
                "mode={} blockers={}",
                report.state.mode.as_str(),
                report.blockers.join("|")
            ),
        ),
        Ok(report) if report.state.observations == 0 => {
            LiveReadinessCheck::unknown("clob_engine_mode", "no_engine_mode_observations")
        }
        Ok(report) => LiveReadinessCheck::ready(
            "clob_engine_mode",
            format!(
                "mode={} status={}",
                report.state.mode.as_str(),
                report.status
            ),
        ),
        Err(err) => LiveReadinessCheck::blocked(
            "clob_engine_mode",
            format!("engine_mode_report_unavailable:{err}"),
        ),
    }
}

async fn accounting_snapshot_readiness_check(
    config: &Config,
    account_address: Address,
) -> LiveReadinessCheck {
    if !config.live_accounting_snapshot_enabled {
        return LiveReadinessCheck::unknown(
            "accounting_snapshot",
            "LIVE_ACCOUNTING_SNAPSHOT_ENABLED=false",
        );
    }
    match accounting_snapshot::fetch_and_write_accounting_snapshot_report(
        &Client::new(),
        config,
        account_address,
    )
    .await
    {
        Ok(report) if report.blocks_live() => LiveReadinessCheck::blocked(
            "accounting_snapshot",
            format!(
                "status={} blockers={}",
                report.status,
                report.blockers.join("|")
            ),
        ),
        Ok(report) => LiveReadinessCheck::ready(
            "accounting_snapshot",
            format!(
                "status={} position_rows={} equity_rows={}",
                report.status, report.positions.exposure_rows, report.equity.rows
            ),
        ),
        Err(err) => LiveReadinessCheck::blocked(
            "accounting_snapshot",
            format!("accounting_snapshot_unavailable:{err:#}"),
        ),
    }
}

fn market_data_config_readiness_check(config: &Config) -> LiveReadinessCheck {
    let mut blockers = Vec::new();
    if config.clob_api_url.trim().is_empty() {
        blockers.push("CLOB_API_URL_empty");
    }
    if config.clob_ws_url.trim().is_empty() {
        blockers.push("CLOB_WS_URL_empty");
    }
    if config.ws_quote_max_age_ms > config.live_max_refresh_to_submit_ms {
        blockers.push("WS_QUOTE_MAX_AGE_MS_exceeds_LIVE_MAX_REFRESH_TO_SUBMIT_MS");
    }
    if !blockers.is_empty() {
        return LiveReadinessCheck::blocked(
            "market_data_config",
            format!("market_data_blockers={}", blockers.join("|")),
        );
    }
    LiveReadinessCheck::ready(
        "market_data_config",
        format!(
            "clob_api_url_present clob_ws_url_present ws_quote_max_age_ms={} live_max_refresh_to_submit_ms={}",
            config.ws_quote_max_age_ms, config.live_max_refresh_to_submit_ms
        ),
    )
}

pub async fn write_live_readiness_report(config: &Config) -> Result<PathBuf> {
    let report = build_live_readiness_report(config).await;
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(LIVE_READINESS_REPORT_FILE);
    let body = serde_json::to_string_pretty(&report)?;
    fs::write(&path, body)
        .with_context(|| format!("writing live readiness report {}", path.display()))?;
    Ok(path)
}

pub fn build_live_route_calibration_report(config: &Config) -> Result<LiveRouteCalibrationReport> {
    let shadow_path = config.diagnostics_dir.join(LIVE_ROUTE_SHADOW_JOURNAL_FILE);
    let replay_path = config.diagnostics_dir.join(LIVE_ROUTE_REPLAY_JOURNAL_FILE);
    let realized_pnl_path = config.diagnostics_dir.join(LIVE_REALIZED_PNL_FILE);
    let shadows = read_live_route_shadow_reports(&shadow_path)?;
    let replay_records = read_live_route_replay_records(&replay_path)?;
    let independent_realized_ev = read_independent_realized_ev_by_execution(&realized_pnl_path)?;
    Ok(build_live_route_calibration_report_from_records(
        config,
        &shadow_path,
        &replay_path,
        &shadows,
        &replay_records,
        &independent_realized_ev,
    ))
}

pub fn write_live_route_calibration_report(config: &Config) -> Result<PathBuf> {
    let report = build_live_route_calibration_report(config)?;
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config
        .diagnostics_dir
        .join(LIVE_ROUTE_CALIBRATION_REPORT_FILE);
    let body = serde_json::to_string_pretty(&report)?;
    fs::write(&path, body)
        .with_context(|| format!("writing live route calibration report {}", path.display()))?;
    Ok(path)
}

pub async fn build_combo_rfq_route_promotion_report(
    config: &Config,
) -> ComboRfqRoutePromotionReport {
    let mut checks = Vec::new();
    let requester = crate::combo_rfq_client::combo_rfq_requester_config_report(config);
    checks.push(if requester.blockers.is_empty() {
        LiveReadinessCheck::ready(
            "combo_rfq_requester",
            format!("requester_ready api_url={}", requester.api_url),
        )
    } else {
        LiveReadinessCheck::blocked(
            "combo_rfq_requester",
            format!("requester_blocked:{}", requester.blockers.join("|")),
        )
    });
    checks.push(combo_rfq_requester_protocol_readiness_check(config));
    checks.push(if config.combo_rfq_accept_enabled {
        LiveReadinessCheck::ready("combo_rfq_accept_gate", "COMBO_RFQ_ACCEPT_ENABLED=true")
    } else {
        LiveReadinessCheck::blocked("combo_rfq_accept_gate", "COMBO_RFQ_ACCEPT_ENABLED=false")
    });
    let protocol_drift = crate::protocol_drift::build_protocol_drift_report(config);
    checks.push(protocol_drift_readiness_check(&protocol_drift));
    checks.push(
        match crate::combo_rfq_client::unresolved_combo_rfq_execution_records(config) {
            Ok(records) if records.is_empty() => LiveReadinessCheck::ready(
                "combo_rfq_execution_recovery",
                "no_unresolved_combo_rfq_execution_records",
            ),
            Ok(records) => LiveReadinessCheck::blocked(
                "combo_rfq_execution_recovery",
                format!("unresolved_combo_rfq_execution_records={}", records.len()),
            ),
            Err(err) => LiveReadinessCheck::blocked(
                "combo_rfq_execution_recovery",
                format!("execution_recovery_journal_unreadable:{err}"),
            ),
        },
    );
    let combo_exposure =
        crate::combo_rfq_client::fetch_live_combo_exposure_report(&Client::new(), config).await;
    checks.push(combo_rfq_account_exposure_readiness_check(&combo_exposure));
    checks.push(match user_channel::ensure_live_user_channel_ready(config) {
        Ok(()) => LiveReadinessCheck::ready(
            "user_channel_ready",
            "fresh_authenticated_user_channel_status",
        ),
        Err(err) => LiveReadinessCheck::blocked(
            "user_channel_ready",
            format!("user_channel_not_ready:{err}"),
        ),
    });
    checks.push(match build_live_route_calibration_report(config) {
        Ok(report) => match report
            .routes
            .iter()
            .find(|bucket| bucket.route == COMBO_RFQ_ROUTE)
        {
            Some(bucket) if bucket.risk_gate_pass => LiveReadinessCheck::ready(
                "combo_rfq_replay_calibration",
                format!("labeled_samples={}", bucket.labeled_samples),
            ),
            Some(bucket) => LiveReadinessCheck::blocked(
                "combo_rfq_replay_calibration",
                format!(
                    "calibration_failed samples={} blockers={}",
                    bucket.labeled_samples,
                    bucket.blockers.join("|")
                ),
            ),
            None => LiveReadinessCheck::blocked(
                "combo_rfq_replay_calibration",
                "missing_combo_rfq_calibration_bucket",
            ),
        },
        Err(err) => LiveReadinessCheck::blocked(
            "combo_rfq_replay_calibration",
            format!("calibration_unavailable:{err}"),
        ),
    });
    checks.push(
        match crate::combo_rfq_client::build_combo_rfq_maker_scorecard(config) {
            Ok(scorecard) if scorecard.records_seen == 0 => LiveReadinessCheck::blocked(
                "combo_rfq_maker_scorecard",
                "missing_maker_score_samples",
            ),
            Ok(scorecard) => {
                let unready_makers = scorecard
                    .makers
                    .iter()
                    .filter(|maker| maker.status != "pass")
                    .count();
                if scorecard.maker_count == 0 {
                    LiveReadinessCheck::blocked(
                        "combo_rfq_maker_scorecard",
                        format!(
                            "missing_scored_makers records_seen={}",
                            scorecard.records_seen
                        ),
                    )
                } else if unready_makers > 0 {
                    LiveReadinessCheck::blocked(
                        "combo_rfq_maker_scorecard",
                        format!(
                            "unready_makers={} records_seen={}",
                            unready_makers, scorecard.records_seen
                        ),
                    )
                } else {
                    LiveReadinessCheck::ready(
                        "combo_rfq_maker_scorecard",
                        format!(
                            "records_seen={} makers={}",
                            scorecard.records_seen, scorecard.maker_count
                        ),
                    )
                }
            }
            Err(err) => LiveReadinessCheck::blocked(
                "combo_rfq_maker_scorecard",
                format!("maker_scorecard_unavailable:{err}"),
            ),
        },
    );
    checks.push(combo_rfq_settlement_hazard_promotion_readiness_check(
        config,
    ));
    checks.push(combo_rfq_exchange_v3_allowance_promotion_readiness_check(config).await);
    checks.push(combo_rfq_erc1155_operator_approval_promotion_readiness_check(config).await);
    checks
        .push(combo_rfq_position_manager_operator_approval_promotion_readiness_check(config).await);
    checks.push(combo_rfq_closeout_execution_promotion_readiness_check(
        config,
    ));
    checks.push(combo_rfq_finalized_block_promotion_readiness_check(config).await);
    checks.push(
        match crate::rfq_finality::write_combo_rfq_finality_report(config)
            .and_then(|_| crate::rfq_finality::build_combo_rfq_finality_report(config))
        {
            Ok(report) if report.blockers.is_empty() => LiveReadinessCheck::ready(
                "rfq_finality_stream",
                format!(
                    "terminal_records={} confirmed_records={} realized_terminal_records={}",
                    report.terminal_records,
                    report.confirmed_records,
                    report.realized_terminal_records
                ),
            ),
            Ok(report) => LiveReadinessCheck::blocked(
                "rfq_finality_stream",
                format!(
                    "finality_blocked records={} blockers={}",
                    report.records_seen,
                    report.blockers.join("|")
                ),
            ),
            Err(err) => LiveReadinessCheck::blocked(
                "rfq_finality_stream",
                format!("finality_report_unavailable:{err}"),
            ),
        },
    );
    checks.push(
        match crate::rfq_stream_client::ensure_live_combo_rfq_stream_ready(config) {
            Ok(()) => LiveReadinessCheck::ready(
                "rfq_stream_client",
                "fresh_same_process_rfq_stream_status",
            ),
            Err(err) => {
                LiveReadinessCheck::blocked("rfq_stream_client", format!("stream_not_ready:{err}"))
            }
        },
    );
    checks.push(if !config.live_combo_rfq_route_enabled {
        LiveReadinessCheck::blocked(
            "live_route_support_code",
            "LIVE_COMBO_RFQ_ROUTE_ENABLED=false",
        )
    } else {
        LiveReadinessCheck::ready(
            "live_route_support_code",
            "LIVE_COMBO_RFQ_ROUTE_ENABLED=true; route gated by Combo/RFQ promotion report",
        )
    });

    let blockers: Vec<String> = checks
        .iter()
        .filter(|check| !matches!(check.state, LiveReadinessState::Ready))
        .map(|check| format!("{}:{}", check.key, check.detail))
        .collect();
    ComboRfqRoutePromotionReport {
        generated_at: Utc::now().to_rfc3339(),
        route: COMBO_RFQ_ROUTE.to_string(),
        promoted: blockers.is_empty(),
        checks,
        blockers,
    }
}

fn combo_rfq_requester_protocol_readiness_check(config: &Config) -> LiveReadinessCheck {
    if config.combo_rfq_requester_protocol_verified {
        return LiveReadinessCheck::ready(
            "combo_rfq_requester_protocol",
            "COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED=true; beta requester create/query/accept endpoint flow verified",
        );
    }
    LiveReadinessCheck::blocked(
        "combo_rfq_requester_protocol",
        "COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED=false; Polymarket Combo requester API is beta, so live promotion requires explicit operator verification of create/query/accept/finality flow",
    )
}

fn combo_rfq_account_exposure_readiness_check(
    report: &crate::combo_rfq_client::ComboExposureReport,
) -> LiveReadinessCheck {
    if report.status == "clean" && report.open_combo_count == 0 {
        return LiveReadinessCheck::ready(
            "combo_rfq_account_exposure",
            format!(
                "open_combo_count=0 activity_count={}",
                report.activity.activity_count
            ),
        );
    }
    if report.open_combo_count > 0 {
        return LiveReadinessCheck::blocked(
            "combo_rfq_account_exposure",
            format!(
                "open_combo_exposure count={} entry_cost_usdc={:.6} total_cost_usdc={:.6}",
                report.open_combo_count, report.total_entry_cost_usdc, report.total_cost_usdc
            ),
        );
    }
    if report.redeemable_combo_count > 0 {
        return LiveReadinessCheck::blocked(
            "combo_rfq_account_exposure",
            format!(
                "redeemable_combo_exposure count={} total_cost_usdc={:.6}; run closeout planning and Relayer redeem before new Combo/RFQ exposure",
                report.redeemable_combo_count, report.total_cost_usdc
            ),
        );
    }
    LiveReadinessCheck::blocked(
        "combo_rfq_account_exposure",
        format!(
            "combo_exposure_unavailable status={} error={}",
            report.status,
            report.error.clone().unwrap_or_else(|| "none".into())
        ),
    )
}

async fn ensure_combo_rfq_account_exposure_clean(config: &Config, http: &Client) -> Result<()> {
    let report = crate::combo_rfq_client::fetch_live_combo_exposure_report(http, config).await;
    ensure_combo_rfq_account_exposure_report_clean(&report)
}

fn combo_rfq_settlement_hazard_promotion_readiness_check(config: &Config) -> LiveReadinessCheck {
    match crate::settlement_monitor::build_settlement_hazard_report(config) {
        Ok(report) if report.blockers.is_empty() => LiveReadinessCheck::ready(
            "settlement_revert_hazard",
            format!(
                "recent_receipts={} failed_receipts={} revert_rate={:.4}",
                report.recent_records, report.failed_receipts, report.revert_rate
            ),
        ),
        Ok(report) => LiveReadinessCheck::blocked(
            "settlement_revert_hazard",
            format!(
                "settlement_hazard_blocked recent_receipts={} failed_receipts={} revert_rate={:.4} blockers={}",
                report.recent_records,
                report.failed_receipts,
                report.revert_rate,
                report.blockers.join("|")
            ),
        ),
        Err(err) => LiveReadinessCheck::blocked(
            "settlement_revert_hazard",
            format!("settlement_hazard_report_unavailable:{err}"),
        ),
    }
}

fn combo_rfq_closeout_execution_promotion_readiness_check(config: &Config) -> LiveReadinessCheck {
    if !config.live_closeout_enabled {
        return LiveReadinessCheck::blocked(
            "combo_rfq_closeout_execution",
            "LIVE_CLOSEOUT_ENABLED=false; Combo/RFQ promotion requires an executable closeout/redeem path",
        );
    }
    if config.live_closeout_dry_run {
        return LiveReadinessCheck::blocked(
            "combo_rfq_closeout_execution",
            "LIVE_CLOSEOUT_DRY_RUN=true; Combo/RFQ promotion requires non-dry-run closeout execution",
        );
    }
    let wallet_type = closeout_wallet_type(config);
    if wallet_type == "EOA" {
        return LiveReadinessCheck::ready(
            "combo_rfq_closeout_execution",
            "combo_router_eoa_redeem_executor_ready; PositionManager approval, eth_call preflight, receipt logs, finality, PnL, and exposure release are enforced before accounting reconciliation",
        );
    }
    if wallet_type == "DEPOSIT" && config.live_signature_type == 3 {
        let blockers = deposit_wallet_relayer_config_blockers(config);
        if blockers.is_empty() {
            return LiveReadinessCheck::ready(
                "combo_rfq_closeout_execution",
                "combo_router_deposit_wallet_relayer_executor_ready; Relayer WALLET submit, confirmation polling, receipt logs, finality, PnL, and exposure release are enforced before accounting reconciliation",
            );
        }
        return LiveReadinessCheck::blocked(
            "combo_rfq_closeout_execution",
            format!(
                "deposit_wallet_relayer_config_blocked:{}",
                blockers.join("|")
            ),
        );
    }
    LiveReadinessCheck::blocked(
        "combo_rfq_closeout_execution",
        format!(
            "closeout_wallet_type={wallet_type}; wallet-specific Relayer closeout path required for automatic Combo redeem; use LIVE_SIGNATURE_TYPE=0 EOA direct Router closeout or LIVE_SIGNATURE_TYPE=3 Deposit Wallet Relayer closeout"
        ),
    )
}

async fn combo_rfq_finalized_block_promotion_readiness_check(
    config: &Config,
) -> LiveReadinessCheck {
    if !config.onchain_order_filled_collector_enabled {
        return LiveReadinessCheck::blocked(
            "polygon_finalized_block",
            "ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED=false; finalized block gate requires live on-chain fill collection",
        );
    }
    if config.polygon_rpc_url.trim().is_empty() {
        return LiveReadinessCheck::blocked("polygon_finalized_block", "POLYGON_RPC_URL_empty");
    }

    let http = Client::new();
    let latest = match fetch_polygon_block_number_by_tag(&http, config, "latest").await {
        Ok(block) => block,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "polygon_finalized_block",
                format!("latest_block_unavailable:{err}"),
            )
        }
    };
    let finalized = match fetch_polygon_block_number_by_tag(&http, config, "finalized").await {
        Ok(block) => block,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "polygon_finalized_block",
                format!("finalized_block_unavailable:{err}"),
            )
        }
    };
    if finalized > latest {
        return LiveReadinessCheck::blocked(
            "polygon_finalized_block",
            format!("finalized_block_ahead_of_latest finalized={finalized} latest={latest}"),
        );
    }

    let lag = latest.saturating_sub(finalized);
    let max_lag = config.polygon_finalized_block_max_lag_blocks.max(1);
    if lag > max_lag {
        return LiveReadinessCheck::blocked(
            "polygon_finalized_block",
            format!(
                "finalized_block_lag_blocks={lag}>{max_lag} latest={latest} finalized={finalized}"
            ),
        );
    }

    LiveReadinessCheck::ready(
        "polygon_finalized_block",
        format!(
            "latest_block={latest} finalized_block={finalized} lag_blocks={lag} max_lag_blocks={max_lag}"
        ),
    )
}

async fn fetch_polygon_block_number_by_tag(
    http: &Client,
    config: &Config,
    tag: &str,
) -> Result<u64> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [tag, false],
    });
    let response = http
        .post(config.polygon_rpc_url.trim())
        .json(&request)
        .send()
        .await
        .with_context(|| format!("sending eth_getBlockByNumber({tag}) request"))?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .with_context(|| format!("parsing eth_getBlockByNumber({tag}) status={status}"))?;
    if !status.is_success() {
        bail!("eth_getBlockByNumber({tag})_http_status:{status}");
    }
    if let Some(error) = body.get("error") {
        bail!("eth_getBlockByNumber({tag})_rpc_error:{error}");
    }
    let Some(result) = body.get("result") else {
        bail!("eth_getBlockByNumber({tag})_missing_result");
    };
    if result.is_null() {
        bail!("eth_getBlockByNumber({tag})_null_result");
    }
    parse_rpc_u64_quantity(result.get("number"))
        .with_context(|| format!("eth_getBlockByNumber({tag}) missing/invalid number: {body}"))
}

fn parse_rpc_u64_quantity(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            let hex = trimmed
                .strip_prefix("0x")
                .or_else(|| trimmed.strip_prefix("0X"))
                .unwrap_or(trimmed);
            u64::from_str_radix(hex, 16).ok()
        }
        Some(Value::Number(number)) => number.as_u64(),
        _ => None,
    }
}

async fn combo_rfq_erc1155_operator_approval_promotion_readiness_check(
    config: &Config,
) -> LiveReadinessCheck {
    let exchange = match combo_rfq_exchange_v3_spender(config) {
        Ok(exchange) => exchange,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "exchange_v3_erc1155_operator_approval",
                format!("exchange_v3_spender_unavailable:{err}"),
            )
        }
    };
    let account_address = match configured_live_account_address(config) {
        Ok(account_address) => account_address,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "exchange_v3_erc1155_operator_approval",
                format!("account_unavailable_for_erc1155_operator_probe:{err}"),
            )
        }
    };
    erc1155_operator_approval_rpc_readiness_check(config, account_address, exchange).await
}

async fn combo_rfq_position_manager_operator_approval_promotion_readiness_check(
    config: &Config,
) -> LiveReadinessCheck {
    let position_manager = match combo_position_manager_address(config.live_chain_id) {
        Some(position_manager) => position_manager,
        None => {
            return LiveReadinessCheck::blocked(
                "combo_position_manager_erc1155_operator_approval",
                format!(
                    "missing_combo_position_manager_config chain_id={}",
                    config.live_chain_id
                ),
            )
        }
    };
    let account_address = match configured_live_account_address(config) {
        Ok(account_address) => account_address,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "combo_position_manager_erc1155_operator_approval",
                format!("account_unavailable_for_position_manager_operator_probe:{err}"),
            )
        }
    };
    let (approved, conditional_tokens) = match erc1155_operator_approval_rpc_probe(
        config,
        account_address,
        position_manager,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "combo_position_manager_erc1155_operator_approval",
                format!("position_manager_operator_probe_failed:{err:#}"),
            )
        }
    };
    if approved {
        LiveReadinessCheck::ready(
            "combo_position_manager_erc1155_operator_approval",
            format!(
                "isApprovedForAll=true account={} operator={} conditional_tokens={} source=polygon_rpc",
                account_address, position_manager, conditional_tokens
            ),
        )
    } else {
        LiveReadinessCheck::blocked(
            "combo_position_manager_erc1155_operator_approval",
            format!(
                "isApprovedForAll=false account={} operator={} conditional_tokens={} source=polygon_rpc",
                account_address, position_manager, conditional_tokens
            ),
        )
    }
}

async fn erc1155_operator_approval_readiness_check(
    config: &Config,
    account_address: Address,
) -> LiveReadinessCheck {
    let mut operators = Vec::new();
    for neg_risk in [false, true] {
        let label = if neg_risk {
            "exchange_v2_neg_risk"
        } else {
            "exchange_v2_standard"
        };
        let contract = match contract_config(config.live_chain_id, neg_risk) {
            Some(contract) => contract,
            None => {
                return LiveReadinessCheck::blocked(
                    "erc1155_operator_approval",
                    format!(
                        "missing_sdk_contract_config chain_id={} neg_risk={} for ERC1155 operator approval probe",
                        config.live_chain_id, neg_risk
                    ),
                )
            }
        };
        let Some(operator) = contract.exchange_v2 else {
            return LiveReadinessCheck::blocked(
                "erc1155_operator_approval",
                format!(
                    "missing_exchange_v2_operator chain_id={} neg_risk={}",
                    config.live_chain_id, neg_risk
                ),
            );
        };
        if !operators
            .iter()
            .any(|(_, existing): &(&'static str, Address)| *existing == operator)
        {
            operators.push((label, operator));
        }
    }

    let mut approved = Vec::new();
    let mut missing = Vec::new();
    for (label, operator) in operators {
        match erc1155_operator_approval_rpc_probe(config, account_address, operator).await {
            Ok((true, conditional_tokens)) => approved.push(format!(
                "{label}:operator={operator}:conditional_tokens={conditional_tokens}:source=polygon_rpc"
            )),
            Ok((false, conditional_tokens)) => missing.push(format!(
                "{label}:operator={operator}:conditional_tokens={conditional_tokens}:source=polygon_rpc"
            )),
            Err(err) => {
                return LiveReadinessCheck::blocked(
                    "erc1155_operator_approval",
                    format!("erc1155_operator_approval_probe_failed:{err:#}"),
                )
            }
        }
    }

    if missing.is_empty() {
        LiveReadinessCheck::ready(
            "erc1155_operator_approval",
            format!("isApprovedForAll=true {}", approved.join("|")),
        )
    } else {
        LiveReadinessCheck::blocked(
            "erc1155_operator_approval",
            format!("isApprovedForAll=false {}", missing.join("|")),
        )
    }
}

async fn erc1155_operator_approval_rpc_readiness_check(
    config: &Config,
    account_address: Address,
    exchange: Address,
) -> LiveReadinessCheck {
    let (approved, conditional_tokens) =
        match erc1155_operator_approval_rpc_probe(config, account_address, exchange).await {
            Ok(result) => result,
            Err(err) => {
                return LiveReadinessCheck::blocked(
                    "exchange_v3_erc1155_operator_approval",
                    format!("erc1155_operator_approval_probe_failed:{err:#}"),
                )
            }
        };
    if approved {
        LiveReadinessCheck::ready(
            "exchange_v3_erc1155_operator_approval",
            format!(
                "isApprovedForAll=true account={} operator={} conditional_tokens={} source=polygon_rpc",
                account_address, exchange, conditional_tokens
            ),
        )
    } else {
        LiveReadinessCheck::blocked(
            "exchange_v3_erc1155_operator_approval",
            format!(
                "isApprovedForAll=false account={} operator={} conditional_tokens={} source=polygon_rpc",
                account_address, exchange, conditional_tokens
            ),
        )
    }
}

async fn erc1155_operator_approval_rpc_probe(
    config: &Config,
    account_address: Address,
    operator: Address,
) -> Result<(bool, Address)> {
    let contract = contract_config(config.live_chain_id, false).ok_or_else(|| {
        anyhow!(
            "missing_sdk_contract_config chain_id={} for ERC1155 operator approval probe",
            config.live_chain_id
        )
    })?;
    let rpc_url = config.polygon_rpc_url.trim();
    if rpc_url.is_empty() {
        bail!("POLYGON_RPC_URL is required before ERC1155 operator approval probe");
    }
    let provider = ProviderBuilder::new()
        .connect(rpc_url)
        .await
        .context("erc1155_operator_rpc_connect_failed")?;
    let conditional_tokens = IERC1155OperatorApproval::new(contract.conditional_tokens, provider);
    let approved = conditional_tokens
        .isApprovedForAll(account_address, operator)
        .call()
        .await
        .context("erc1155_operator_approval_probe_failed")?;
    Ok((approved, contract.conditional_tokens))
}

fn ensure_combo_rfq_account_exposure_report_clean(
    report: &crate::combo_rfq_client::ComboExposureReport,
) -> Result<()> {
    let check = combo_rfq_account_exposure_readiness_check(report);
    if matches!(check.state, LiveReadinessState::Ready) {
        Ok(())
    } else {
        bail!("Combo/RFQ account exposure check blocked: {}", check.detail)
    }
}

pub async fn write_combo_rfq_route_promotion_report(config: &Config) -> Result<PathBuf> {
    let report = build_combo_rfq_route_promotion_report(config).await;
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config
        .diagnostics_dir
        .join(COMBO_RFQ_ROUTE_PROMOTION_REPORT_FILE);
    let body = serde_json::to_string_pretty(&report)?;
    fs::write(&path, body).with_context(|| {
        format!(
            "writing Combo/RFQ route promotion report {}",
            path.display()
        )
    })?;
    Ok(path)
}

#[cfg(test)]
pub fn append_live_route_replay_record(
    config: &Config,
    record: &LiveRouteReplayRecord,
) -> Result<PathBuf> {
    append_live_route_replay_records_deduped(config, std::slice::from_ref(record))?;
    Ok(config.diagnostics_dir.join(LIVE_ROUTE_REPLAY_JOURNAL_FILE))
}

pub fn append_live_route_replay_records_deduped(
    config: &Config,
    records: &[LiveRouteReplayRecord],
) -> Result<usize> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(LIVE_ROUTE_REPLAY_JOURNAL_FILE);
    let mut existing_keys: HashSet<String> = read_live_route_replay_records(&path)?
        .iter()
        .map(live_route_replay_record_key)
        .collect();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening live route replay journal {}", path.display()))?;
    let mut written = 0usize;
    for record in records {
        let key = live_route_replay_record_key(record);
        if !existing_keys.insert(key) {
            continue;
        }
        serde_json::to_writer(&mut file, record)
            .with_context(|| format!("serializing live route replay record {}", path.display()))?;
        writeln!(file)
            .with_context(|| format!("writing live route replay journal {}", path.display()))?;
        written += 1;
    }
    Ok(written)
}

fn build_live_route_calibration_report_from_records(
    config: &Config,
    shadow_path: &Path,
    replay_path: &Path,
    shadows: &[LiveRouteShadowReport],
    replay_records: &[LiveRouteReplayRecord],
    independent_realized_ev: &HashMap<String, f64>,
) -> LiveRouteCalibrationReport {
    let min_required_samples = config.live_route_calibration_min_samples;
    let mut route_names: Vec<String> = shadows
        .iter()
        .map(|report| report.route.clone())
        .chain(replay_records.iter().map(|record| record.route.clone()))
        .collect();
    route_names.sort();
    route_names.dedup();

    let mut blockers = Vec::new();
    if shadows.is_empty() && replay_records.is_empty() {
        push_unique_blocker(&mut blockers, "no_route_calibration_data");
    }
    if !shadows.is_empty() && replay_records.is_empty() {
        push_unique_blocker(&mut blockers, "missing_finality_labels");
    }

    let mut routes = Vec::new();
    for route in route_names {
        let route_shadows: Vec<&LiveRouteShadowReport> = shadows
            .iter()
            .filter(|report| report.route == route)
            .collect();
        let route_replays: Vec<&LiveRouteReplayRecord> = replay_records
            .iter()
            .filter(|record| record.route == route)
            .collect();
        let bucket = build_live_route_calibration_bucket(
            config,
            &route,
            min_required_samples,
            &route_shadows,
            &route_replays,
            independent_realized_ev,
        );
        for blocker in &bucket.blockers {
            push_unique_blocker(&mut blockers, format!("{}:{}", bucket.route, blocker));
        }
        routes.push(bucket);
    }

    let risk_gate_pass = !routes.is_empty() && routes.iter().all(|bucket| bucket.risk_gate_pass);
    let recent_labeled_replay_samples = routes.iter().map(|bucket| bucket.labeled_samples).sum();
    let realized_ev_samples = routes.iter().map(|bucket| bucket.realized_ev_samples).sum();

    LiveRouteCalibrationReport {
        generated_at: Utc::now().to_rfc3339(),
        shadow_journal_path: shadow_path.display().to_string(),
        replay_journal_path: replay_path.display().to_string(),
        shadow_samples: shadows.len(),
        labeled_replay_samples: recent_labeled_replay_samples,
        realized_ev_samples,
        min_required_samples,
        routes,
        risk_gate_pass,
        blockers,
    }
}

fn build_live_route_calibration_bucket(
    config: &Config,
    route: &str,
    min_required_samples: usize,
    shadows: &[&LiveRouteShadowReport],
    replay_records: &[&LiveRouteReplayRecord],
    independent_realized_ev: &HashMap<String, f64>,
) -> LiveRouteCalibrationBucket {
    let now = Utc::now();
    let max_age_secs = config.live_route_calibration_max_age_secs.max(1);
    let mut recent_replay_records = Vec::new();
    let mut stale_label_count = 0usize;
    let mut future_label_count = 0usize;
    let mut missing_label_timestamp_count = 0usize;
    for record in replay_records {
        match parse_rfc3339_utc(&record.generated_at) {
            Some(timestamp) if timestamp > now + chrono::Duration::seconds(5) => {
                future_label_count += 1;
            }
            Some(timestamp) => {
                let age_secs = now.signed_duration_since(timestamp).num_seconds().max(0) as u64;
                if age_secs > max_age_secs {
                    stale_label_count += 1;
                } else {
                    recent_replay_records.push(*record);
                }
            }
            None => missing_label_timestamp_count += 1,
        }
    }

    let labeled_samples = recent_replay_records.len();
    let denominator = labeled_samples.max(1) as f64;
    let both_count = recent_replay_records
        .iter()
        .filter(|record| replay_label_kind(&record.outcome_label) == ReplayLabelKind::Both)
        .count();
    let one_leg_count = recent_replay_records
        .iter()
        .filter(|record| replay_label_kind(&record.outcome_label) == ReplayLabelKind::OneLeg)
        .count();
    let ghost_count = recent_replay_records
        .iter()
        .filter(|record| replay_label_kind(&record.outcome_label) == ReplayLabelKind::Ghost)
        .count();
    let unknown_labels: Vec<String> = recent_replay_records
        .iter()
        .filter(|record| replay_label_kind(&record.outcome_label) == ReplayLabelKind::Unknown)
        .map(|record| record.outcome_label.clone())
        .collect();

    let avg_shadow_ev_usd = average_f64(shadows.iter().map(|report| report.expected_shadow_ev_usd));
    let mut realized_execution_ids = HashSet::new();
    let mut realized_ev_values = Vec::new();
    let mut realized_ev_mismatch_count = 0usize;
    for record in &recent_replay_records {
        let Some(execution_id) = replay_execution_id(record) else {
            continue;
        };
        let Some(independent_ev) = independent_realized_ev.get(execution_id).copied() else {
            continue;
        };
        if record.realized_ev_usd.is_some_and(|reported| {
            reported.is_finite()
                && (reported - independent_ev).abs() > REPORTED_REALIZED_EV_MATCH_TOLERANCE_USD
        }) {
            realized_ev_mismatch_count += 1;
        }
        if !realized_execution_ids.insert(execution_id.to_string()) {
            continue;
        }
        realized_ev_values.push(independent_ev);
    }
    let realized_ev_samples = realized_ev_values.len();
    let avg_realized_ev_usd = average_f64(realized_ev_values.iter().copied());
    let invalid_reported_realized_ev_count = recent_replay_records
        .iter()
        .filter_map(|record| record.realized_ev_usd)
        .filter(|value| !value.is_finite())
        .count();
    let avg_toxicity_score = average_f64(
        recent_replay_records
            .iter()
            .filter_map(|record| record.toxicity_score)
            .chain(shadows.iter().map(|report| report.toxicity_score)),
    );
    let latest_shadow_at = latest_string(shadows.iter().map(|report| report.generated_at.as_str()));
    let latest_label_at = latest_string(
        replay_records
            .iter()
            .map(|record| record.generated_at.as_str()),
    );

    let p_both_fill_observed = both_count as f64 / denominator;
    let p_one_leg_fill_observed = one_leg_count as f64 / denominator;
    let p_ghost_revert_observed = ghost_count as f64 / denominator;

    let mut blockers = Vec::new();
    if !shadows.is_empty() && replay_records.is_empty() {
        push_unique_blocker(&mut blockers, "shadow_reports_unlabeled");
    }
    if stale_label_count > 0 {
        push_unique_blocker(
            &mut blockers,
            format!("stale_labeled_samples:{stale_label_count}>{max_age_secs}s"),
        );
    }
    if future_label_count > 0 {
        push_unique_blocker(
            &mut blockers,
            format!("future_labeled_samples:{future_label_count}"),
        );
    }
    if missing_label_timestamp_count > 0 {
        push_unique_blocker(
            &mut blockers,
            format!("missing_labeled_sample_timestamps:{missing_label_timestamp_count}"),
        );
    }
    if labeled_samples == 0 {
        push_unique_blocker(&mut blockers, "missing_finality_labels");
    }
    if labeled_samples < min_required_samples {
        push_unique_blocker(
            &mut blockers,
            format!("insufficient_labeled_samples:{labeled_samples}/{min_required_samples}"),
        );
    }
    if realized_ev_samples < min_required_samples {
        push_unique_blocker(
            &mut blockers,
            format!(
                "insufficient_realized_ev_samples:{realized_ev_samples}/{min_required_samples}"
            ),
        );
    }
    if invalid_reported_realized_ev_count > 0 {
        push_unique_blocker(
            &mut blockers,
            format!("invalid_realized_ev_labels:{invalid_reported_realized_ev_count}"),
        );
    }
    if realized_ev_mismatch_count > 0 {
        push_unique_blocker(
            &mut blockers,
            format!("realized_ev_mismatch_labels:{realized_ev_mismatch_count}"),
        );
    }
    if !unknown_labels.is_empty() {
        let mut unique = unknown_labels;
        unique.sort();
        unique.dedup();
        push_unique_blocker(
            &mut blockers,
            format!("unknown_outcome_labels:{}", unique.join("|")),
        );
    }
    if avg_realized_ev_usd.is_none() {
        push_unique_blocker(&mut blockers, "missing_realized_ev_labels");
    }
    if p_one_leg_fill_observed > 0.005 {
        push_unique_blocker(
            &mut blockers,
            format!("one_leg_fill_rate_too_high:{p_one_leg_fill_observed:.4}"),
        );
    }
    if p_ghost_revert_observed > 0.001 {
        push_unique_blocker(
            &mut blockers,
            format!("ghost_revert_rate_too_high:{p_ghost_revert_observed:.4}"),
        );
    }
    if avg_realized_ev_usd
        .map(|ev| ev <= config.min_net_profit_usd)
        .unwrap_or(false)
    {
        push_unique_blocker(
            &mut blockers,
            format!(
                "realized_ev_below_min_profit:{:.4}<={:.4}",
                avg_realized_ev_usd.unwrap_or_default(),
                config.min_net_profit_usd
            ),
        );
    }
    if avg_toxicity_score
        .map(|score| score > 0.25)
        .unwrap_or(false)
    {
        push_unique_blocker(
            &mut blockers,
            format!(
                "toxicity_score_too_high:{:.4}",
                avg_toxicity_score.unwrap_or_default()
            ),
        );
    }

    LiveRouteCalibrationBucket {
        route: route.to_string(),
        shadow_samples: shadows.len(),
        labeled_samples,
        realized_ev_samples,
        min_required_samples,
        p_both_fill_observed,
        p_one_leg_fill_observed,
        p_ghost_revert_observed,
        avg_shadow_ev_usd,
        avg_realized_ev_usd,
        avg_toxicity_score,
        latest_shadow_at,
        latest_label_at,
        risk_gate_pass: blockers.is_empty(),
        blockers,
    }
}

fn read_live_route_shadow_reports(path: &Path) -> Result<Vec<LiveRouteShadowReport>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)
        .with_context(|| format!("opening live route shadow journal {}", path.display()))?;
    let mut reports = Vec::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "reading live route shadow journal {} line {}",
                path.display(),
                idx + 1
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let report: LiveRouteShadowReport = serde_json::from_str(&line).with_context(|| {
            format!(
                "live route shadow journal {} has malformed JSON at line {}",
                path.display(),
                idx + 1
            )
        })?;
        reports.push(report);
    }
    Ok(reports)
}

fn read_live_route_replay_records(path: &Path) -> Result<Vec<LiveRouteReplayRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)
        .with_context(|| format!("opening live route replay journal {}", path.display()))?;
    let mut records = Vec::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "reading live route replay journal {} line {}",
                path.display(),
                idx + 1
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let record: LiveRouteReplayRecord = serde_json::from_str(&line).with_context(|| {
            format!(
                "live route replay journal {} has malformed JSON at line {}",
                path.display(),
                idx + 1
            )
        })?;
        records.push(record);
    }
    Ok(records)
}

fn read_independent_realized_ev_by_execution(path: &Path) -> Result<HashMap<String, f64>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let file = File::open(path)
        .with_context(|| format!("opening live realized PnL ledger {}", path.display()))?;
    let mut realized_by_execution = HashMap::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "reading live realized PnL ledger {} line {}",
                path.display(),
                idx + 1
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let record: IndependentRealizedPnlLine =
            serde_json::from_str(&line).with_context(|| {
                format!(
                    "live realized PnL ledger {} has malformed JSON at line {}",
                    path.display(),
                    idx + 1
                )
            })?;
        let Some(execution_id) = record
            .execution_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let realized_ev = match record.source.as_deref() {
            Some("combo_closeout_router") => {
                let has_closeout_proof = record
                    .closeout_action_id
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                    && record
                        .transaction_hash
                        .as_deref()
                        .is_some_and(|value| !value.trim().is_empty())
                    && record.block_number.is_some_and(|value| value > 0)
                    && record.status_class.as_deref() == Some("closeout_confirmed");
                if !has_closeout_proof {
                    bail!(
                        "live realized PnL ledger {} line {} lacks confirmed closeout proof",
                        path.display(),
                        idx + 1
                    );
                }
                Some(record.realized_ev_usd.ok_or_else(|| {
                    anyhow!(
                        "live realized PnL ledger {} line {} is missing independently derived realized_ev_usd",
                        path.display(),
                        idx + 1
                    )
                })?)
            }
            None => {
                let Some(reported_realized_pnl) = record.realized_pnl_usd else {
                    continue;
                };
                let has_closeout_proof = record
                    .closeout_action_id
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                    && record
                        .transaction_hash
                        .as_deref()
                        .is_some_and(|value| !value.trim().is_empty())
                    && record.block_number.is_some_and(|value| value > 0)
                    && record.receipt_total_logs.is_some_and(|value| value > 0)
                    && record
                        .receipt_collateral_transfer_to_account_logs
                        .unwrap_or_default()
                        > 0
                    && record.receipt_ctf_transfer_logs.unwrap_or_default() > 0;
                if !has_closeout_proof {
                    bail!(
                        "live realized PnL ledger {} line {} lacks receipt-derived closeout proof",
                        path.display(),
                        idx + 1
                    );
                }
                let allocated_payout = record.allocated_p_usd_delta_usd.ok_or_else(|| {
                    anyhow!(
                        "live realized PnL ledger {} line {} lacks allocated payout",
                        path.display(),
                        idx + 1
                    )
                })?;
                let entry_cost = record
                    .actual_entry_cost_usd
                    .or(record.projected_position_usd)
                    .ok_or_else(|| {
                        anyhow!(
                            "live realized PnL ledger {} line {} lacks entry cost basis",
                            path.display(),
                            idx + 1
                        )
                    })?;
                let closeout_gas = record.allocated_closeout_gas_cost_usd.ok_or_else(|| {
                    anyhow!(
                        "live realized PnL ledger {} line {} lacks allocated closeout gas",
                        path.display(),
                        idx + 1
                    )
                })?;
                if !allocated_payout.is_finite()
                    || !entry_cost.is_finite()
                    || !closeout_gas.is_finite()
                    || allocated_payout < 0.0
                    || entry_cost < 0.0
                    || closeout_gas < 0.0
                {
                    bail!(
                        "live realized PnL ledger {} line {} has invalid payout/cost/gas values",
                        path.display(),
                        idx + 1
                    );
                }
                let recomputed = allocated_payout - entry_cost - closeout_gas;
                if !reported_realized_pnl.is_finite()
                    || (reported_realized_pnl - recomputed).abs()
                        > REALIZED_EV_RECOMPUTE_TOLERANCE_USD
                {
                    bail!(
                        "live realized PnL ledger {} line {} realized PnL does not recompute",
                        path.display(),
                        idx + 1
                    );
                }
                Some(recomputed)
            }
            // RFQ finality copies provider-supplied realizedEvUsd. It is not an
            // independent PnL measurement and cannot satisfy calibration.
            Some(_) => None,
        };
        let Some(realized_ev) = realized_ev else {
            continue;
        };
        if !realized_ev.is_finite() {
            bail!(
                "live realized PnL ledger {} line {} has non-finite realized PnL",
                path.display(),
                idx + 1
            );
        }
        *realized_by_execution
            .entry(execution_id.to_string())
            .or_insert(0.0) += realized_ev;
    }
    Ok(realized_by_execution)
}

fn replay_execution_id(record: &LiveRouteReplayRecord) -> Option<&str> {
    record.notes.iter().find_map(|note| {
        note.strip_prefix("execution_id=")
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })
}

fn live_route_replay_record_key(record: &LiveRouteReplayRecord) -> String {
    record.label_id.clone().unwrap_or_else(|| {
        format!(
            "{}:{}:{}",
            record.route, record.event_id, record.outcome_label
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayLabelKind {
    Both,
    OneLeg,
    Ghost,
    Terminal,
    Unknown,
}

fn replay_label_kind(label: &str) -> ReplayLabelKind {
    match label.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "both_confirmed" | "merge_success" => ReplayLabelKind::Both,
        "one_leg_confirmed" | "residual_inventory" => ReplayLabelKind::OneLeg,
        "matched_then_failed" | "ghost_revert" => ReplayLabelKind::Ghost,
        "timeout" | "book_stale" | "price_moved" => ReplayLabelKind::Terminal,
        _ => ReplayLabelKind::Unknown,
    }
}

fn average_f64(values: impl IntoIterator<Item = f64>) -> Option<f64> {
    let mut total = 0.0;
    let mut count = 0usize;
    for value in values {
        if value.is_finite() {
            total += value;
            count += 1;
        }
    }
    if count == 0 {
        None
    } else {
        Some(total / count as f64)
    }
}

fn latest_string<'a>(values: impl IntoIterator<Item = &'a str>) -> Option<String> {
    values
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .max()
        .map(str::to_string)
}

fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn push_unique_blocker(blockers: &mut Vec<String>, blocker: impl Into<String>) {
    let blocker = blocker.into();
    if !blockers.contains(&blocker) {
        blockers.push(blocker);
    }
}

fn live_route_calibration_bucket(
    config: &Config,
    route: &str,
) -> Result<Option<LiveRouteCalibrationBucket>> {
    let report = build_live_route_calibration_report(config)?;
    Ok(report
        .routes
        .into_iter()
        .find(|bucket| bucket.route == route))
}

pub fn record_live_route_shadow(
    config: &Config,
    opp: &ArbitrageOpportunity,
    route_kind: LiveRouteKind,
) -> Option<String> {
    let report = build_live_route_shadow_report(config, opp, route_kind)?;
    if let Err(err) = append_live_route_shadow_report(config, &report) {
        warn!("failed to append live route shadow report: {err:#}");
    }
    Some(format!(
        "shadow_route={} shadow_status={} risk_gate_pass={} p_both_fill={:.3} p_one_leg_fill={:.3} p_ghost_revert={:.3} toxicity_score={:.3} orphan_loss_usd={:.4} shadow_ev_usd={:.4} shadow_blockers={}",
        report.route,
        report.status,
        report.risk_gate_pass,
        report.p_both_fill,
        report.p_one_leg_fill,
        report.p_ghost_revert,
        report.toxicity_score,
        report.orphan_closeout_loss_usd,
        report.expected_shadow_ev_usd,
        if report.blockers.is_empty() {
            "none".to_string()
        } else {
            report.blockers.join("|")
        }
    ))
}

fn append_live_route_shadow_report(config: &Config, report: &LiveRouteShadowReport) -> Result<()> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(LIVE_ROUTE_SHADOW_JOURNAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening live route shadow journal {}", path.display()))?;
    serde_json::to_writer(&mut file, report)
        .with_context(|| format!("serializing live route shadow report {}", path.display()))?;
    writeln!(file).with_context(|| format!("writing live route shadow journal {}", path.display()))
}

fn build_live_route_shadow_report(
    config: &Config,
    opp: &ArbitrageOpportunity,
    route_kind: LiveRouteKind,
) -> Option<LiveRouteShadowReport> {
    if !matches!(route_kind, LiveRouteKind::CtfMergeBundleCandidate) {
        return None;
    }
    let mut stages = vec![
        "planned".to_string(),
        "priced".to_string(),
        "orphan_risk_evaluated".to_string(),
        "blocked_no_submit".to_string(),
    ];
    let mut blockers = Vec::new();
    if !matches!(opp.arb_type, ArbType::Bundle) {
        blockers.push("not_bundle_route".to_string());
    }
    if opp.execution_plan.len() != 2 {
        blockers.push(format!(
            "expected_two_legs_got_{}",
            opp.execution_plan.len()
        ));
    }

    let mut condition_ids = HashSet::new();
    let mut yes_capacity = None;
    let mut no_capacity = None;
    let mut any_neg_risk = false;
    for leg in &opp.execution_plan {
        condition_ids.insert(leg.condition_id.trim().to_string());
        let market = match plan_market(&opp.markets, leg) {
            Ok(market) => market,
            Err(err) => {
                blockers.push(format!("missing_market:{err}"));
                continue;
            }
        };
        any_neg_risk |= market.clob_neg_risk == Some(true);
        match leg.outcome {
            OutcomeSide::Yes => yes_capacity = market.clob_yes_ask_size,
            OutcomeSide::No => no_capacity = market.clob_no_ask_size,
        }
    }
    if condition_ids.len() != 1 {
        blockers.push("legs_not_same_condition".to_string());
    }
    if any_neg_risk {
        blockers.push("negative_risk_merge_shadow_not_supported".to_string());
    }

    let basket_units = if opp.total_cost > f64::EPSILON {
        (config.live_trade_position_size_usd / opp.total_cost).max(0.0)
    } else {
        0.0
    };
    if basket_units <= f64::EPSILON {
        blockers.push("invalid_shadow_basket_units".to_string());
    }
    let yes_fill = yes_capacity
        .map(|size| (size / basket_units.max(f64::EPSILON)).clamp(0.0, 1.0))
        .unwrap_or(0.0);
    let no_fill = no_capacity
        .map(|size| (size / basket_units.max(f64::EPSILON)).clamp(0.0, 1.0))
        .unwrap_or(0.0);
    if yes_capacity.is_none() || no_capacity.is_none() {
        blockers.push("missing_visible_ask_size".to_string());
    }
    let mut p_both_fill = yes_fill * no_fill;
    let mut p_one_leg_fill = (yes_fill * (1.0 - no_fill)) + (no_fill * (1.0 - yes_fill));
    let gross_edge_usd = (opp.gross_profit - opp.total_fees).max(0.0) * basket_units;
    let latency_haircut_usd = live_edge_haircut_usd(config.live_trade_position_size_usd, config);
    let orphan_closeout_loss_usd =
        config.live_trade_position_size_usd * (config.live_slippage_bps as f64 / 10_000.0 + 0.02);
    let settlement_loss_usd = config.live_trade_position_size_usd;
    let mut p_ghost_revert = 0.01;
    let toxicity_score = shadow_toxicity_score(yes_fill, no_fill, opp.expected_slippage_pct);
    let mut calibrated_replay_samples = 0usize;
    let mut calibration_gate_pass = false;
    match live_route_calibration_bucket(config, CTF_MERGE_BUNDLE_SHADOW_ROUTE) {
        Ok(Some(bucket)) => {
            calibrated_replay_samples = bucket.labeled_samples;
            if bucket.labeled_samples > 0 {
                p_both_fill = bucket.p_both_fill_observed;
                p_one_leg_fill = bucket.p_one_leg_fill_observed;
                p_ghost_revert = bucket.p_ghost_revert_observed;
            }
            calibration_gate_pass = bucket.risk_gate_pass;
            if !bucket.risk_gate_pass {
                blockers.push("route_calibration_gate_failed".to_string());
            }
            for blocker in &bucket.blockers {
                blockers.push(format!("route_calibration:{blocker}"));
            }
        }
        Ok(None) => blockers.push("missing_route_calibration_bucket".to_string()),
        Err(err) => blockers.push(format!("route_calibration_unavailable:{err}")),
    }
    let lock_hours = opp
        .capital_lock_hours
        .filter(|hours| hours.is_finite() && *hours >= 0.0)
        .unwrap_or(config.capital_velocity_reference_hours);
    let capital_lock_cost_usd =
        config.live_trade_position_size_usd * (lock_hours / (24.0 * 365.0)) * 0.10;
    let expected_shadow_ev_usd = p_both_fill * gross_edge_usd
        - p_one_leg_fill * orphan_closeout_loss_usd
        - p_ghost_revert * settlement_loss_usd
        - latency_haircut_usd
        - capital_lock_cost_usd;
    if calibrated_replay_samples == 0 {
        blockers.push("execution_risk_uncalibrated".to_string());
    }
    if toxicity_score > 0.25 {
        blockers.push(format!("toxicity_score_too_high:{toxicity_score:.3}"));
    }
    if expected_shadow_ev_usd <= 0.0 {
        blockers.push("non_positive_shadow_ev".to_string());
    }
    let risk_gate_pass = blockers.is_empty()
        && calibration_gate_pass
        && calibrated_replay_samples >= config.live_route_calibration_min_samples
        && expected_shadow_ev_usd > config.min_net_profit_usd
        && toxicity_score <= 0.25
        && p_ghost_revert <= 0.001;

    Some(LiveRouteShadowReport {
        generated_at: Utc::now().to_rfc3339(),
        event_id: opp.event_id.clone(),
        event_title: opp.event_title.clone(),
        route: CTF_MERGE_BUNDLE_SHADOW_ROUTE.to_string(),
        status: "blocked_no_submit".to_string(),
        stages: std::mem::take(&mut stages),
        basket_units,
        gross_edge_usd,
        p_both_fill,
        p_one_leg_fill,
        p_ghost_revert,
        orphan_closeout_loss_usd,
        settlement_loss_usd,
        latency_haircut_usd,
        capital_lock_cost_usd,
        toxicity_score,
        calibrated_replay_samples,
        risk_gate_pass,
        expected_shadow_ev_usd,
        blockers,
    })
}

fn shadow_toxicity_score(yes_fill: f64, no_fill: f64, expected_slippage_pct: f64) -> f64 {
    let imbalance = (yes_fill - no_fill).abs().clamp(0.0, 1.0);
    let slippage = (expected_slippage_pct.max(0.0) / 100.0).clamp(0.0, 1.0);
    (imbalance * 0.7 + slippage * 0.3).clamp(0.0, 1.0)
}

async fn verify_clean_startup_account<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    account_address: Address,
) -> Result<()>
where
    K: Kind,
{
    verify_clean_account(sdk_client, account_address, "startup").await
}

async fn verify_clean_pre_submit_account<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    account_address: Address,
) -> Result<()>
where
    K: Kind,
{
    verify_clean_account(sdk_client, account_address, "pre-submit").await
}

async fn verify_clean_account<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    account_address: Address,
    phase: &str,
) -> Result<()>
where
    K: Kind,
{
    let open_orders = sdk_client
        .orders(&OrdersRequest::default(), None)
        .await
        .with_context(|| {
            format!("failed to reconcile authenticated open orders before live {phase}")
        })?;
    let order_samples: Vec<String> = open_orders
        .data
        .iter()
        .take(3)
        .map(|order| {
            format!(
                "{} {} {} size={} matched={} price={}",
                order.id,
                order.side,
                order.asset_id,
                order.original_size,
                order.size_matched,
                order.price
            )
        })
        .collect();

    let data_client = DataClient::default();
    let positions = fetch_account_positions(&data_client, account_address, phase).await?;
    let position_samples: Vec<String> = positions
        .iter()
        .take(3)
        .map(|position| {
            format!(
                "{} {} size={} current_value={}",
                position.asset, position.outcome, position.size, position.current_value
            )
        })
        .collect();

    ensure_clean_account_state(
        phase,
        open_orders.data.len(),
        positions.len(),
        account_address,
        &order_samples,
        &position_samples,
    )
}

async fn fetch_account_positions(
    data_client: &DataClient,
    account_address: Address,
    phase: &str,
) -> Result<Vec<Position>> {
    let mut positions = Vec::new();
    let mut offset = 0;

    loop {
        let request = PositionsRequest::builder()
            .user(account_address)
            .size_threshold(Decimal::ZERO)
            .limit(STARTUP_POSITIONS_PAGE_LIMIT)?
            .offset(offset)?
            .build();
        let mut page = data_client.positions(&request).await.with_context(|| {
            format!(
                "failed to reconcile current positions before live {phase} at offset {}",
                offset
            )
        })?;
        let page_len = page.len();
        positions.append(&mut page);

        match next_startup_positions_offset(offset, page_len, STARTUP_POSITIONS_PAGE_LIMIT)? {
            Some(next_offset) => offset = next_offset,
            None => break,
        }
    }

    Ok(positions)
}

pub async fn write_live_closeout_plan(config: &Config) -> Result<PathBuf> {
    let plan = fetch_live_closeout_plan(config, "closeout-plan").await?;

    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(LIVE_CLOSEOUT_PLAN_FILE);
    let body = serde_json::to_string_pretty(&plan)?;
    fs::write(&path, body)
        .with_context(|| format!("writing live closeout plan {}", path.display()))?;
    Ok(path)
}

pub async fn write_live_closeout_run_report(config: &Config) -> Result<PathBuf> {
    let plan = fetch_live_closeout_plan(config, "closeout-run").await?;
    let journal_path = config.diagnostics_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
    let unresolved_by_condition = unresolved_journal_executions_by_condition(&journal_path)?;
    let report =
        execute_or_build_live_closeout_run_report(config, &plan, &unresolved_by_condition).await?;

    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(LIVE_CLOSEOUT_RUN_REPORT_FILE);
    let body = serde_json::to_string_pretty(&report)?;
    fs::write(&path, body)
        .with_context(|| format!("writing live closeout run report {}", path.display()))?;
    Ok(path)
}

pub async fn write_live_closeout_payoff_certificate(config: &Config) -> Result<PathBuf> {
    let certificate = fetch_live_closeout_payoff_certificate(config).await?;

    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config
        .diagnostics_dir
        .join(LIVE_CLOSEOUT_PAYOFF_CERTIFICATE_FILE);
    let body = serde_json::to_string_pretty(&certificate)?;
    fs::write(&path, body).with_context(|| {
        format!(
            "writing live closeout payoff certificate {}",
            path.display()
        )
    })?;
    Ok(path)
}

async fn fetch_live_closeout_plan(config: &Config, phase: &str) -> Result<LiveCloseoutPlan> {
    let account_address = configured_live_account_address(config)?;
    let data_client = DataClient::default();
    let positions = fetch_account_positions(&data_client, account_address, phase).await?;
    let position_views: Vec<PositionView> = positions.iter().map(PositionView::from).collect();
    let combo_exposure =
        crate::combo_rfq_client::fetch_live_combo_exposure_report(&Client::new(), config).await;
    Ok(build_live_closeout_plan_with_combo_exposure(
        account_address,
        &position_views,
        combo_exposure,
    ))
}

async fn fetch_live_closeout_payoff_certificate(
    config: &Config,
) -> Result<LiveCloseoutPayoffCertificate> {
    let account_address = configured_live_account_address(config)?;
    let data_client = DataClient::default();
    let positions =
        fetch_account_positions(&data_client, account_address, "closeout-certificate").await?;
    let position_views: Vec<PositionView> = positions.iter().map(PositionView::from).collect();
    let combo_exposure =
        crate::combo_rfq_client::fetch_live_combo_exposure_report(&Client::new(), config).await;
    let plan = build_live_closeout_plan_with_combo_exposure(
        account_address,
        &position_views,
        combo_exposure,
    );
    let journal_path = config.diagnostics_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
    let unresolved_by_condition = unresolved_journal_executions_by_condition(&journal_path)?;
    let mut report = build_live_closeout_run_report(config, &plan, &unresolved_by_condition)?;
    enrich_closeout_run_report_eth_calls(config, &mut report).await;
    Ok(build_live_closeout_payoff_certificate(
        &plan,
        &position_views,
        &report,
    ))
}

#[cfg(test)]
fn build_live_closeout_plan(
    account_address: Address,
    positions: &[PositionView],
) -> LiveCloseoutPlan {
    build_live_closeout_plan_with_combo_exposure(
        account_address,
        positions,
        unchecked_combo_exposure_report(),
    )
}

fn build_live_closeout_plan_with_combo_exposure(
    account_address: Address,
    positions: &[PositionView],
    combo_exposure: crate::combo_rfq_client::ComboExposureReport,
) -> LiveCloseoutPlan {
    let mut by_condition: HashMap<String, Vec<&PositionView>> = HashMap::new();
    for position in positions {
        if position.size <= Decimal::ZERO {
            continue;
        }
        by_condition
            .entry(position.condition_id.clone())
            .or_default()
            .push(position);
    }

    let mut actions = Vec::new();
    for (condition_id, condition_positions) in by_condition {
        let negative_risk = condition_positions
            .iter()
            .any(|position| position.negative_risk);
        let title = condition_positions
            .first()
            .map(|position| position.title.clone())
            .unwrap_or_default();
        let slug = condition_positions
            .first()
            .map(|position| position.slug.clone())
            .unwrap_or_default();
        let yes = condition_positions
            .iter()
            .find(|position| position.outcome_index == 0)
            .copied();
        let no = condition_positions
            .iter()
            .find(|position| position.outcome_index == 1)
            .copied();

        if condition_positions
            .iter()
            .any(|position| position.redeemable)
        {
            let amount = yes.map(|position| position.size).unwrap_or(Decimal::ZERO)
                + no.map(|position| position.size).unwrap_or(Decimal::ZERO);
            actions.push(LiveCloseoutAction {
                action: if negative_risk {
                    "neg_risk_redeem_review".into()
                } else {
                    "redeem_resolved".into()
                },
                condition_id,
                title,
                slug,
                negative_risk,
                amount_shares: amount.to_string(),
                yes_asset: yes.map(|position| position.asset.clone()),
                yes_size: yes.map(|position| position.size.to_string()),
                no_asset: no.map(|position| position.asset.clone()),
                no_size: no.map(|position| position.size.to_string()),
                combo_position_id: None,
                combo_outcome_index: None,
                note: if negative_risk {
                    "resolved negative-risk position; use the NegRisk adapter redemption path and verify amounts before releasing exposure".into()
                } else {
                    "resolved standard binary position; redeem both index sets, verify position removal, then release exposure".into()
                },
            });
            continue;
        }

        let Some(yes) = yes else {
            continue;
        };
        let Some(no) = no else {
            continue;
        };
        if !yes.mergeable && !no.mergeable {
            continue;
        }
        let amount = yes.size.min(no.size);
        if amount <= Decimal::ZERO {
            continue;
        }
        actions.push(LiveCloseoutAction {
            action: if negative_risk {
                "neg_risk_merge_review".into()
            } else {
                "merge_full_set".into()
            },
            condition_id,
            title,
            slug,
            negative_risk,
            amount_shares: amount.to_string(),
            yes_asset: Some(yes.asset.clone()),
            yes_size: Some(yes.size.to_string()),
            no_asset: Some(no.asset.clone()),
            no_size: Some(no.size.to_string()),
            combo_position_id: None,
            combo_outcome_index: None,
            note: if negative_risk {
                "matched negative-risk YES/NO inventory; verify the correct adapter path before merging or releasing exposure".into()
            } else {
                "matched standard YES/NO inventory; merge this full-set amount, verify position removal, then release exposure".into()
            },
        });
    }

    append_combo_closeout_actions(&mut actions, &combo_exposure);

    actions.sort_by(|left, right| {
        left.condition_id
            .cmp(&right.condition_id)
            .then_with(|| left.action.cmp(&right.action))
    });

    LiveCloseoutPlan {
        generated_at: Utc::now().to_rfc3339(),
        account_address: account_address.to_string(),
        open_positions: positions.len(),
        combo_exposure,
        actions,
    }
}

fn append_combo_closeout_actions(
    actions: &mut Vec<LiveCloseoutAction>,
    combo_exposure: &crate::combo_rfq_client::ComboExposureReport,
) {
    for combo in &combo_exposure.combos {
        if !combo_position_is_redeemable_win(combo) {
            continue;
        }
        actions.push(LiveCloseoutAction {
            action: "combo_redeem_resolved_win_review".into(),
            condition_id: combo.combo_condition_id.clone(),
            title: combo
                .status
                .as_deref()
                .map(|status| format!("Resolved Combo {status}"))
                .unwrap_or_else(|| "Resolved Combo".into()),
            slug: combo
                .combo_position_id
                .as_deref()
                .map(|position_id| format!("combo-position-{position_id}"))
                .unwrap_or_else(|| format!("combo-condition-{}", combo.combo_condition_id)),
            negative_risk: false,
            amount_shares: combo
                .shares_balance
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("0")
                .to_string(),
            yes_asset: None,
            yes_size: None,
            no_asset: None,
            no_size: None,
            combo_position_id: combo.combo_position_id.clone(),
            combo_outcome_index: combo.combo_outcome_index,
            note: "resolved winning Combo position; redeem uses Polymarket Combo Router directly for EOA closeout or Relayer /submit for Deposit Wallet closeout after PositionManager approval".into(),
        });
    }
}

fn combo_position_is_redeemable_win(combo: &crate::combo_rfq_client::ComboPositionView) -> bool {
    matches!(
        combo
            .status
            .as_deref()
            .map(|status| status.trim().to_ascii_lowercase().replace([' ', '-'], "_"))
            .as_deref(),
        Some("resolved_win") | Some("resolved_winning") | Some("redeemable")
    )
}

#[cfg(test)]
fn unchecked_combo_exposure_report() -> crate::combo_rfq_client::ComboExposureReport {
    crate::combo_rfq_client::ComboExposureReport {
        status: "not_checked".into(),
        ..crate::combo_rfq_client::ComboExposureReport::default()
    }
}

fn build_live_closeout_run_report(
    config: &Config,
    plan: &LiveCloseoutPlan,
    unresolved_by_condition: &HashMap<String, Vec<String>>,
) -> Result<LiveCloseoutRunReport> {
    let max_actions = config.live_closeout_max_actions_per_run.max(1);
    let planned_actions = plan.actions.len();
    let actions: Vec<LiveCloseoutRunAction> = plan
        .actions
        .iter()
        .take(max_actions)
        .map(|action| build_live_closeout_run_action(config, action, unresolved_by_condition))
        .collect();
    let selected_actions = actions.len();
    let skipped_actions = planned_actions.saturating_sub(selected_actions);
    let dry_run = config.live_closeout_dry_run || !config.live_closeout_enabled;
    let note = if dry_run && config.live_closeout_enabled {
        "dry-run is enabled; no transactions were sent and no exposure was released"
    } else if config.live_closeout_enabled {
        "non-dry-run closeout is enabled; ready standard actions may send transactions"
    } else {
        "closeout execution is disabled; this report is advisory only and no exposure was released"
    };

    Ok(LiveCloseoutRunReport {
        generated_at: Utc::now().to_rfc3339(),
        account_address: plan.account_address.clone(),
        combo_exposure: plan.combo_exposure.clone(),
        dry_run,
        execution_enabled: config.live_closeout_enabled,
        max_actions,
        planned_actions,
        selected_actions,
        skipped_actions,
        actions,
        note: note.into(),
    })
}

fn build_live_closeout_payoff_certificate(
    plan: &LiveCloseoutPlan,
    positions: &[PositionView],
    report: &LiveCloseoutRunReport,
) -> LiveCloseoutPayoffCertificate {
    let plan_actions_by_id: HashMap<String, &LiveCloseoutAction> = plan
        .actions
        .iter()
        .map(|action| (closeout_action_id(action), action))
        .collect();
    let plan_actions_by_condition: HashMap<String, &LiveCloseoutAction> = plan
        .actions
        .iter()
        .map(|action| (action.condition_id.clone(), action))
        .collect();
    let actions: Vec<LiveCloseoutPayoffCertificateAction> = report
        .actions
        .iter()
        .map(|action| {
            build_live_closeout_payoff_certificate_action(
                plan_actions_by_id.get(&action.action_id).copied(),
                action,
            )
        })
        .collect();
    let certified_actions = actions
        .iter()
        .filter(|action| action.status == "certified")
        .count();
    let blocked_actions = actions.len().saturating_sub(certified_actions);
    let unresolved_execution_count = actions
        .iter()
        .flat_map(|action| action.unresolved_execution_ids.iter())
        .collect::<HashSet<_>>()
        .len();
    let deterministic_min_terminal_payout_usd = actions
        .iter()
        .map(|action| action.deterministic_payout_usd.max(0.0))
        .sum();
    let (residual_condition_count, residual_position_count, residual_shares) =
        closeout_certificate_residuals(positions, &plan_actions_by_condition);
    let estimated_closeout_gas_usd = None;
    let closeout_gas_source = if report.planned_actions == 0 {
        "not_required"
    } else {
        "not_quoted_no_fallback"
    }
    .to_string();

    let mut blockers = Vec::new();
    if plan.combo_exposure.open_combo_count > 0 {
        blockers.push(format!(
            "combo_open_position_closeout_not_certified open_combo_count={} total_cost_usdc={:.6}",
            plan.combo_exposure.open_combo_count, plan.combo_exposure.total_cost_usdc
        ));
    }
    if plan.combo_exposure.status != "clean" {
        blockers.push(format!(
            "combo_exposure_not_cleanly_verified status={}",
            plan.combo_exposure.status
        ));
    }
    if report.skipped_actions > 0 {
        blockers.push(format!(
            "closeout_actions_skipped_by_run_limit={}",
            report.skipped_actions
        ));
    }
    if residual_position_count > 0 {
        blockers.push(format!(
            "residual_open_positions_not_certified positions={} conditions={} shares={}",
            residual_position_count, residual_condition_count, residual_shares
        ));
    }
    if unresolved_execution_count > 0 {
        blockers.push(format!(
            "unresolved_execution_journal_entries={unresolved_execution_count}"
        ));
    }
    if blocked_actions > 0 {
        blockers.push(format!("uncertified_closeout_actions={blocked_actions}"));
    }
    if report.planned_actions > 0 && estimated_closeout_gas_usd.is_none() {
        blockers.push(
            "closeout_gas_quote_unavailable_no_fallback_used; profitability cannot be certified"
                .into(),
        );
    }

    let status = if blockers.is_empty() {
        if plan.open_positions == 0 {
            "clean_no_open_positions"
        } else {
            "certified"
        }
    } else {
        "blocked"
    };
    let note = if blockers.is_empty() {
        "all open positions have a proof-carrying deterministic payoff certificate"
    } else {
        "certificate is read-only and fail-closed; deterministic payout excludes unquoted gas, unsupported Combo/RFQ closeout, and any residual inventory"
    };

    LiveCloseoutPayoffCertificate {
        generated_at: Utc::now().to_rfc3339(),
        account_address: report.account_address.clone(),
        status: status.into(),
        open_positions: plan.open_positions,
        planned_actions: report.planned_actions,
        certified_actions,
        blocked_actions,
        skipped_actions: report.skipped_actions,
        residual_condition_count,
        residual_position_count,
        residual_shares,
        unresolved_execution_count,
        combo_exposure_status: plan.combo_exposure.status.clone(),
        combo_open_count: plan.combo_exposure.open_combo_count,
        combo_redeemable_count: plan.combo_exposure.redeemable_combo_count,
        combo_total_cost_usdc: plan.combo_exposure.total_cost_usdc,
        deterministic_min_terminal_payout_usd,
        estimated_closeout_gas_usd,
        closeout_gas_source,
        actions,
        blockers,
        note: note.into(),
    }
}

fn build_live_closeout_payoff_certificate_action(
    plan_action: Option<&LiveCloseoutAction>,
    action: &LiveCloseoutRunAction,
) -> LiveCloseoutPayoffCertificateAction {
    let mut blockers = Vec::new();
    let mut deterministic_payout_usd = 0.0;
    let mut payoff_proof = "not_proven".to_string();

    match (action.action.as_str(), plan_action) {
        ("merge_full_set", Some(plan_action)) if !plan_action.negative_risk => {
            let yes = plan_action
                .yes_size
                .as_deref()
                .and_then(|value| Decimal::from_str(value).ok());
            let no = plan_action
                .no_size
                .as_deref()
                .and_then(|value| Decimal::from_str(value).ok());
            let amount = Decimal::from_str(&plan_action.amount_shares).ok();
            match (yes, no, amount) {
                (Some(yes), Some(no), Some(amount)) if yes == no && amount > Decimal::ZERO => {
                    deterministic_payout_usd = decimal_to_f64(&amount);
                    payoff_proof =
                        "standard_binary_full_set_merge_pays_one_pusd_unit_per_share".into();
                }
                (Some(yes), Some(no), _) if yes != no => blockers.push(format!(
                    "unbalanced_yes_no_inventory yes_size={yes} no_size={no}"
                )),
                _ => blockers.push("merge_payoff_missing_balanced_yes_no_amounts".into()),
            }
        }
        ("merge_full_set", Some(_)) => {
            blockers.push("negative_risk_merge_payoff_not_certified_by_standard_path".into());
        }
        ("redeem_resolved", _) => {
            blockers.push(
                "resolved_redemption_payout_requires_oracle_result_not_certified_here".into(),
            );
        }
        ("combo_redeem_resolved_win_review", Some(plan_action)) => {
            match Decimal::from_str(&plan_action.amount_shares) {
                Ok(amount) if amount > Decimal::ZERO => {
                    deterministic_payout_usd = decimal_to_f64(&amount);
                    payoff_proof = "resolved_winning_combo_pays_one_usdc_unit_per_share".into();
                }
                _ => blockers.push("combo_redeem_amount_missing_or_non_positive".into()),
            }
            if plan_action.combo_position_id.is_none() {
                blockers.push("combo_redeem_position_id_missing".into());
            }
            if plan_action.combo_outcome_index.is_none() {
                blockers.push("combo_redeem_outcome_index_unresolved_from_public_catalog".into());
            }
        }
        ("neg_risk_merge_review" | "neg_risk_redeem_review", _) => {
            blockers.push("negative_risk_closeout_requires_adapter_specific_certificate".into());
        }
        (_, None) => blockers.push("matching_closeout_plan_action_missing".into()),
        _ => blockers.push("unsupported_closeout_action_for_payoff_certificate".into()),
    }

    if closeout_action_supports_eth_call(action) && action.call_preview.eth_call_status != "ok" {
        blockers.push(format!(
            "eth_call_preflight_not_ok status={} note={}",
            action.call_preview.eth_call_status,
            truncate_report_text(&action.call_preview.eth_call_note, 180)
        ));
    }
    if closeout_action_supports_eth_call(action) && action.calldata.is_none() {
        blockers.push("closeout_calldata_missing".into());
    }
    if closeout_action_supports_eth_call(action) && action.target_contract.is_none() {
        blockers.push("closeout_target_contract_missing".into());
    }
    if !action.unresolved_execution_ids.is_empty() {
        blockers.push(format!(
            "unresolved_execution_journal_entries={}",
            action.unresolved_execution_ids.len()
        ));
    }
    for blocker in &action.blockers {
        push_unique_blocker(&mut blockers, blocker.clone());
    }

    let status = if blockers.is_empty() {
        "certified"
    } else if deterministic_payout_usd > 0.0 {
        "payoff_shape_proven_preflight_blocked"
    } else {
        "blocked"
    };

    LiveCloseoutPayoffCertificateAction {
        action_id: action.action_id.clone(),
        action: action.action.clone(),
        condition_id: action.condition_id.clone(),
        negative_risk: action.negative_risk,
        amount_shares: action.amount_shares.clone(),
        yes_asset: action.yes_asset.clone(),
        no_asset: action.no_asset.clone(),
        combo_position_id: action.combo_position_id.clone(),
        combo_outcome_index: action.combo_outcome_index,
        amount_ctf_units: action.amount_ctf_units.clone(),
        collateral_token: action.collateral_token.clone(),
        target_contract: action.target_contract.clone(),
        parent_collection_id: action.call_preview.parent_collection_id.clone(),
        partition: action.call_preview.partition.clone(),
        calldata: action.calldata.clone(),
        eth_call_block: action.call_preview.eth_call_block.clone(),
        expected_position_delta: action.expected_position_delta.clone(),
        expected_collateral_delta: action.call_preview.expected_collateral_delta.clone(),
        expected_pusd_delta_usd: deterministic_payout_usd,
        deterministic_payout_usd,
        payoff_proof,
        execution_preflight_status: action.call_preview.eth_call_status.clone(),
        unresolved_execution_ids: action.unresolved_execution_ids.clone(),
        blockers,
        status: status.into(),
    }
}

fn closeout_certificate_residuals(
    positions: &[PositionView],
    plan_actions_by_condition: &HashMap<String, &LiveCloseoutAction>,
) -> (usize, usize, String) {
    let mut by_condition: HashMap<String, Vec<&PositionView>> = HashMap::new();
    for position in positions {
        if position.size > Decimal::ZERO {
            by_condition
                .entry(position.condition_id.clone())
                .or_default()
                .push(position);
        }
    }

    let mut residual_condition_count = 0;
    let mut residual_position_count = 0;
    let mut residual_shares = Decimal::ZERO;
    for (condition_id, condition_positions) in by_condition {
        let action = plan_actions_by_condition.get(&condition_id).copied();
        if !closeout_action_certifies_position_group(action, &condition_positions) {
            residual_condition_count += 1;
            residual_position_count += condition_positions.len();
            for position in condition_positions {
                residual_shares += position.size;
            }
        }
    }

    (
        residual_condition_count,
        residual_position_count,
        residual_shares.to_string(),
    )
}

fn closeout_action_certifies_position_group(
    action: Option<&LiveCloseoutAction>,
    positions: &[&PositionView],
) -> bool {
    let Some(action) = action else {
        return false;
    };
    if action.negative_risk
        || positions
            .iter()
            .any(|position| !matches!(position.outcome_index, 0 | 1))
    {
        return false;
    }
    match action.action.as_str() {
        "merge_full_set" => {
            let yes = action
                .yes_size
                .as_deref()
                .and_then(|value| Decimal::from_str(value).ok())
                .unwrap_or(Decimal::ZERO);
            let no = action
                .no_size
                .as_deref()
                .and_then(|value| Decimal::from_str(value).ok())
                .unwrap_or(Decimal::ZERO);
            yes > Decimal::ZERO && yes == no
        }
        "redeem_resolved" => positions.iter().all(|position| position.redeemable),
        _ => false,
    }
}

async fn execute_or_build_live_closeout_run_report(
    config: &Config,
    plan: &LiveCloseoutPlan,
    unresolved_by_condition: &HashMap<String, Vec<String>>,
) -> Result<LiveCloseoutRunReport> {
    let mut report = build_live_closeout_run_report(config, plan, unresolved_by_condition)?;
    enrich_closeout_run_report_eth_calls(config, &mut report).await;
    if report.dry_run || !config.live_closeout_enabled {
        return Ok(report);
    }

    let account_address = Address::from_str(&report.account_address).with_context(|| {
        format!(
            "invalid report account address '{}'",
            report.account_address
        )
    })?;
    let _safety_preflight = prepare_non_dry_run_closeout_execution(config, account_address)?;
    for action in &mut report.actions {
        if action.status != "ready" {
            continue;
        }
        match execute_closeout_action(config, account_address, action).await {
            Ok(result) => {
                action.transaction_hash = Some(result.transaction_hash);
                action.block_number = Some(result.block_number);
                action.reconciled_execution_ids = result.reconciled_execution_ids;
                action.status = "executed".into();
                action.reason = "closeout executed, position state verified, and matching unresolved journal entries were reconciled".into();
            }
            Err(err) => {
                action.status = "failed".into();
                action.reason = err.to_string();
            }
        }
    }
    report.note = "non-dry-run closeout attempted ready actions; inspect each action status".into();
    Ok(report)
}

fn prepare_non_dry_run_closeout_execution(
    config: &Config,
    account_address: Address,
) -> Result<LiveCloseoutSafetyPreflight> {
    let process_lock = LiveProcessLock::acquire(&config.diagnostics_dir, account_address)
        .context("failed to acquire live closeout account process lock")?;
    user_channel::ensure_live_user_channel_ready(config)
        .context("non-dry-run closeout requires a ready authenticated user-channel tripwire")?;
    Ok(LiveCloseoutSafetyPreflight {
        _process_lock: process_lock,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EthCallOutcome {
    status: String,
    note: String,
}

#[derive(Debug, Deserialize)]
struct EthCallRpcResponse {
    result: Option<Value>,
    error: Option<EthCallRpcError>,
}

#[derive(Debug, Deserialize)]
struct EthCallRpcError {
    code: Option<i64>,
    message: Option<String>,
    data: Option<Value>,
}

#[derive(Debug, Clone)]
struct DepositWalletRelayerConfig {
    api_url: String,
    api_key: String,
    api_key_address: Address,
}

#[derive(Debug, Clone, Serialize)]
struct RelayerCallJson {
    target: String,
    value: String,
    data: String,
}

#[derive(Debug, Deserialize)]
struct RelayerNonceResponse {
    nonce: Value,
}

#[derive(Debug, Deserialize)]
struct RelayerTransactionResponse {
    #[serde(default, alias = "transactionID", alias = "transaction_id")]
    transaction_id: Option<String>,
    #[serde(default, alias = "transactionHash", alias = "transaction_hash")]
    transaction_hash: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default, alias = "errorMsg", alias = "error_msg")]
    error_msg: Option<String>,
    #[serde(default)]
    error: Option<Value>,
}

async fn enrich_closeout_run_report_eth_calls(config: &Config, report: &mut LiveCloseoutRunReport) {
    let account_address = match configured_live_account_address(config) {
        Ok(address) => address,
        Err(err) => {
            for action in report
                .actions
                .iter_mut()
                .filter(|action| closeout_action_supports_eth_call(action))
            {
                let outcome = EthCallOutcome {
                    status: "not_checked_missing_live_account".into(),
                    note: format!(
                        "{PRIVATE_KEY_VAR} is required to populate the eth_call from account: {}",
                        truncate_report_text(&err.to_string(), 180)
                    ),
                };
                apply_eth_call_outcome(action, outcome);
            }
            return;
        }
    };
    let http = Client::new();
    for action in report
        .actions
        .iter_mut()
        .filter(|action| closeout_action_supports_eth_call(action))
    {
        action.call_preview.from = Some(account_address.to_string());
        let outcome =
            simulate_closeout_eth_call_with_rpc_url(&http, config, action, account_address).await;
        apply_eth_call_outcome(action, outcome);
    }
}

fn closeout_action_supports_eth_call(action: &LiveCloseoutRunAction) -> bool {
    (matches!(action.action.as_str(), "merge_full_set" | "redeem_resolved")
        || (action.action == "combo_redeem_resolved_win_review" && action.wallet_type == "EOA"))
        && !action.negative_risk
}

async fn simulate_closeout_eth_call_with_rpc_url(
    http: &Client,
    config: &Config,
    action: &LiveCloseoutRunAction,
    from: Address,
) -> EthCallOutcome {
    let rpc_url = config.polygon_rpc_url.trim();
    if rpc_url.is_empty() {
        return EthCallOutcome {
            status: "not_checked_missing_polygon_rpc_url".into(),
            note: "POLYGON_RPC_URL is required before an eth_call simulation can run".into(),
        };
    }
    let Some(to) = action.target_contract.as_deref() else {
        return EthCallOutcome {
            status: "not_checked_missing_target_contract".into(),
            note: "target contract is missing; cannot simulate eth_call".into(),
        };
    };
    let Some(data) = action.calldata.as_deref() else {
        return EthCallOutcome {
            status: "not_checked_missing_calldata".into(),
            note: "ABI calldata could not be built; cannot simulate eth_call".into(),
        };
    };

    let block = action.call_preview.eth_call_block.as_str();
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {
                "from": from.to_string(),
                "to": to,
                "data": data,
            },
            block,
        ],
    });

    let response = match http.post(rpc_url).json(&request).send().await {
        Ok(response) => response,
        Err(err) => {
            return EthCallOutcome {
                status: "error".into(),
                note: format!(
                    "eth_call request failed: {}",
                    truncate_report_text(&err.to_string(), 220)
                ),
            };
        }
    };
    let response = match response.error_for_status() {
        Ok(response) => response,
        Err(err) => {
            return EthCallOutcome {
                status: "error".into(),
                note: format!(
                    "eth_call HTTP failure: {}",
                    truncate_report_text(&err.to_string(), 220)
                ),
            };
        }
    };
    let parsed = match response.json::<EthCallRpcResponse>().await {
        Ok(parsed) => parsed,
        Err(err) => {
            return EthCallOutcome {
                status: "error".into(),
                note: format!(
                    "eth_call response was not valid JSON-RPC: {}",
                    truncate_report_text(&err.to_string(), 220)
                ),
            };
        }
    };

    if let Some(error) = parsed.error {
        let message = error.message.unwrap_or_else(|| "unknown RPC error".into());
        let status = if message.to_ascii_lowercase().contains("revert") {
            "reverted"
        } else {
            "error"
        };
        let data = error
            .data
            .as_ref()
            .map(|value| format!(" data={}", truncate_report_text(&value.to_string(), 160)))
            .unwrap_or_default();
        return EthCallOutcome {
            status: status.into(),
            note: format!(
                "eth_call failed code={} message={}{}",
                error
                    .code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "unknown".into()),
                truncate_report_text(&message, 180),
                data
            ),
        };
    }

    if let Some(result) = parsed.result {
        return EthCallOutcome {
            status: "ok".into(),
            note: format!(
                "eth_call {} succeeded with result {}",
                block,
                truncate_report_text(&result.to_string(), 160)
            ),
        };
    }

    EthCallOutcome {
        status: "error".into(),
        note: "eth_call response did not include result or error".into(),
    }
}

fn apply_eth_call_outcome(action: &mut LiveCloseoutRunAction, outcome: EthCallOutcome) {
    let failed = outcome.status != "ok";
    action.call_preview.eth_call_status = outcome.status;
    action.call_preview.eth_call_note = outcome.note;
    if failed && action.status == "ready" {
        let blocker = format!(
            "eth_call preflight did not succeed: {} ({})",
            action.call_preview.eth_call_status, action.call_preview.eth_call_note
        );
        if !action.blockers.iter().any(|existing| existing == &blocker) {
            action.blockers.push(blocker);
        }
        action.status = "blocked".into();
        action.reason = "closeout candidate is blocked by failed eth_call preflight".into();
    }
}

#[derive(Debug, Clone)]
struct CloseoutExecutionResult {
    transaction_hash: String,
    block_number: u64,
    reconciled_execution_ids: Vec<String>,
}

async fn execute_closeout_action(
    config: &Config,
    account_address: Address,
    action: &LiveCloseoutRunAction,
) -> Result<CloseoutExecutionResult> {
    if action.action == "combo_redeem_resolved_win_review" {
        execute_combo_redeem_closeout_action(config, account_address, action).await
    } else {
        execute_standard_closeout_action(config, account_address, action).await
    }
}

async fn execute_combo_redeem_closeout_action(
    config: &Config,
    account_address: Address,
    action: &LiveCloseoutRunAction,
) -> Result<CloseoutExecutionResult> {
    if action.negative_risk {
        bail!("Combo Router redeem closeout cannot run for negative-risk positions");
    }
    let wallet_type = closeout_wallet_type(config);
    if wallet_type == "DEPOSIT" && config.live_signature_type == 3 {
        return execute_combo_redeem_closeout_action_via_deposit_relayer(
            config,
            account_address,
            action,
        )
        .await;
    }
    if wallet_type != "EOA" {
        bail!(
            "Combo Router redeem closeout requires an EOA closeout wallet; configured wallet_type={}",
            wallet_type
        );
    }
    if action.call_preview.eth_call_status != "ok" {
        bail!(
            "Combo Router redeem closeout requires successful eth_call preflight, got {}: {}",
            action.call_preview.eth_call_status,
            action.call_preview.eth_call_note
        );
    }

    let contract_cfg = contract_config(config.live_chain_id, false).with_context(|| {
        format!(
            "missing Polymarket standard contract config for chain_id={}",
            config.live_chain_id
        )
    })?;
    let target_contract = action
        .target_contract
        .as_deref()
        .ok_or_else(|| anyhow!("Combo redeem action missing Router target"))?;
    let target_contract = Address::from_str(target_contract)
        .with_context(|| format!("invalid Combo Router target '{target_contract}'"))?;
    let position_manager =
        combo_position_manager_address(config.live_chain_id).with_context(|| {
            format!(
                "missing Combo PositionManager contract config for chain_id={}",
                config.live_chain_id
            )
        })?;
    let outcome_index = action
        .combo_outcome_index
        .ok_or_else(|| anyhow!("Combo redeem action missing outcome index"))?;
    let amount_units = action
        .amount_ctf_units
        .as_deref()
        .ok_or_else(|| anyhow!("Combo redeem action missing amount units"))?;
    let amount_units = U256::from_str_radix(amount_units, 10)
        .with_context(|| format!("invalid Combo redeem amount units '{amount_units}'"))?;
    let condition_word = combo_redeem_condition_id_abi_word_from_run_action(action)?;
    let calldata =
        encode_combo_redeem_calldata_bytes(condition_word, U256::from(outcome_index), amount_units);

    let private_key = std::env::var(PRIVATE_KEY_VAR)
        .context("POLYMARKET_PRIVATE_KEY is required for Combo closeout")?;
    let signer =
        AlloyLocalSigner::from_str(&private_key)?.with_chain_id(Some(config.live_chain_id));
    let rpc_url = config.polygon_rpc_url.trim();
    if rpc_url.is_empty() {
        bail!("POLYGON_RPC_URL is required for Combo closeout execution");
    }
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect(rpc_url)
        .await
        .context("failed to connect Polygon RPC provider for Combo closeout")?;
    let native_balance = provider
        .get_balance(account_address)
        .await
        .context("failed to read native POL balance before Combo closeout")?;
    ensure_closeout_native_gas_balance(native_balance)?;

    let collateral = IERC20Balance::new(contract_cfg.collateral, provider.clone());
    let p_usd_balance_before = collateral
        .balanceOf(account_address)
        .call()
        .await
        .context("failed to read pUSD balance before Combo closeout")?;

    let tx = provider
        .transaction_request()
        .with_from(account_address)
        .with_to(target_contract)
        .with_input(Bytes::from(calldata));
    let pending_tx = provider
        .send_transaction(tx)
        .await
        .context("Combo Router redeem transaction failed to send")?;
    let transaction_hash = pending_tx.tx_hash().to_string();
    let receipt = pending_tx
        .get_receipt()
        .await
        .context("Combo Router redeem receipt unavailable")?;
    if !receipt.status() {
        bail!("Combo Router redeem transaction reverted");
    }
    let block_number = receipt
        .block_number
        .ok_or_else(|| anyhow!("Combo Router redeem receipt missing block number"))?;
    let receipt_logs = closeout_receipt_log_summaries(receipt.logs());
    let _receipt_validation = validate_combo_redeem_receipt_logs(
        &receipt_logs,
        target_contract,
        contract_cfg.collateral,
        position_manager,
        account_address,
    )?;
    let gas_accounting = closeout_gas_accounting(
        &Client::new(),
        config,
        receipt.gas_used,
        receipt.effective_gas_price,
    )
    .await;

    let p_usd_balance_after = collateral
        .balanceOf(account_address)
        .call()
        .await
        .context("failed to read pUSD balance after Combo closeout")?;
    ensure_closeout_p_usd_delta(
        action,
        amount_units,
        p_usd_balance_before,
        p_usd_balance_after,
    )?;
    verify_closeout_position_state(config, account_address, action).await?;
    wait_for_closeout_receipt_finalized(config, block_number).await?;
    let reconciliation_execution_ids = closeout_reconciliation_execution_ids(action);
    append_combo_closeout_realized_pnl_records(
        config,
        action,
        &transaction_hash,
        block_number,
        p_usd_balance_before,
        p_usd_balance_after,
        gas_accounting,
        &reconciliation_execution_ids,
    )?;
    let reconciled_execution_ids =
        append_closeout_manual_reconciliations(config, action, &transaction_hash)?;
    let released_exposures =
        append_closeout_exposure_releases(config, &reconciliation_execution_ids)?;
    if released_exposures < reconciliation_execution_ids.len() {
        warn!(
            "Combo closeout exposure release wrote {} of {} expected release records",
            released_exposures,
            reconciliation_execution_ids.len()
        );
    }

    Ok(CloseoutExecutionResult {
        transaction_hash,
        block_number,
        reconciled_execution_ids,
    })
}

async fn execute_combo_redeem_closeout_action_via_deposit_relayer(
    config: &Config,
    account_address: Address,
    action: &LiveCloseoutRunAction,
) -> Result<CloseoutExecutionResult> {
    let relayer = deposit_wallet_relayer_config(config)?;
    let contract_cfg = contract_config(config.live_chain_id, false).with_context(|| {
        format!(
            "missing Polymarket standard contract config for chain_id={}",
            config.live_chain_id
        )
    })?;
    let target_contract = action
        .target_contract
        .as_deref()
        .ok_or_else(|| anyhow!("Combo redeem action missing Router target"))?;
    let target_contract = Address::from_str(target_contract)
        .with_context(|| format!("invalid Combo Router target '{target_contract}'"))?;
    let position_manager =
        combo_position_manager_address(config.live_chain_id).with_context(|| {
            format!(
                "missing Combo PositionManager contract config for chain_id={}",
                config.live_chain_id
            )
        })?;
    let outcome_index = action
        .combo_outcome_index
        .ok_or_else(|| anyhow!("Combo redeem action missing outcome index"))?;
    let amount_units = action
        .amount_ctf_units
        .as_deref()
        .ok_or_else(|| anyhow!("Combo redeem action missing amount units"))?;
    let amount_units = U256::from_str_radix(amount_units, 10)
        .with_context(|| format!("invalid Combo redeem amount units '{amount_units}'"))?;
    let condition_word = combo_redeem_condition_id_abi_word_from_run_action(action)?;
    let calldata =
        encode_combo_redeem_calldata_bytes(condition_word, U256::from(outcome_index), amount_units);
    let calldata_hex = format!("0x{}", hex_encode_lower(&calldata));

    let private_key = std::env::var(PRIVATE_KEY_VAR)
        .context("POLYMARKET_PRIVATE_KEY is required for Deposit Wallet Relayer closeout")?;
    let signer =
        AlloyLocalSigner::from_str(&private_key)?.with_chain_id(Some(config.live_chain_id));
    let http = Client::new();
    let rpc_url = config.polygon_rpc_url.trim();
    if rpc_url.is_empty() {
        bail!("POLYGON_RPC_URL is required for Deposit Wallet Relayer closeout verification");
    }
    let provider = ProviderBuilder::new()
        .connect(rpc_url)
        .await
        .context("failed to connect Polygon RPC provider for Deposit Wallet closeout")?;
    let collateral = IERC20Balance::new(contract_cfg.collateral, provider.clone());
    let p_usd_balance_before = collateral
        .balanceOf(account_address)
        .call()
        .await
        .context("failed to read pUSD balance before Deposit Wallet closeout")?;

    let relayer_call = RelayerCallJson {
        target: target_contract.to_string(),
        value: "0".into(),
        data: calldata_hex,
    };
    let nonce = fetch_deposit_wallet_relayer_nonce(&http, &relayer, config).await?;
    let deadline = relayer_deadline(config)?;
    let signature = sign_deposit_wallet_batch(
        &signer,
        config,
        account_address,
        nonce,
        U256::from(deadline),
        std::slice::from_ref(&relayer_call),
    )
    .await?;
    let submitted = submit_deposit_wallet_relayer_batch(
        &http,
        &relayer,
        account_address,
        nonce,
        deadline,
        signature,
        vec![relayer_call],
        format!("Redeem resolved Combo closeout {}", action.action_id),
    )
    .await?;
    let confirmed =
        poll_deposit_wallet_relayer_transaction(&http, &relayer, config, &submitted).await?;
    let transaction_hash = confirmed
        .transaction_hash
        .as_deref()
        .map(str::trim)
        .filter(|hash| !hash.is_empty())
        .ok_or_else(|| anyhow!("Relayer confirmed Combo redeem without transaction_hash"))?;
    let tx_hash = B256::from_str(transaction_hash)
        .with_context(|| format!("invalid Relayer transaction hash '{transaction_hash}'"))?;
    let receipt_deadline =
        Instant::now() + Duration::from_secs(config.live_closeout_confirm_timeout_secs);
    let receipt = loop {
        match provider
            .get_transaction_receipt(tx_hash)
            .await
            .context("failed to fetch Deposit Wallet Relayer transaction receipt")?
        {
            Some(receipt) => break receipt,
            None if Instant::now() >= receipt_deadline => {
                bail!(
                    "Deposit Wallet Relayer transaction receipt unavailable before timeout: {}",
                    transaction_hash
                );
            }
            None => tokio::time::sleep(RELAYER_TRANSACTION_POLL_INTERVAL).await,
        }
    };
    if !receipt.status() {
        bail!("Deposit Wallet Relayer Combo redeem transaction reverted");
    }
    let block_number = receipt
        .block_number
        .ok_or_else(|| anyhow!("Deposit Wallet Relayer receipt missing block number"))?;
    let receipt_logs = closeout_receipt_log_summaries(receipt.logs());
    let _receipt_validation = validate_combo_redeem_receipt_logs(
        &receipt_logs,
        target_contract,
        contract_cfg.collateral,
        position_manager,
        account_address,
    )?;
    let gas_accounting =
        closeout_gas_accounting(&http, config, receipt.gas_used, receipt.effective_gas_price).await;
    let p_usd_balance_after = collateral
        .balanceOf(account_address)
        .call()
        .await
        .context("failed to read pUSD balance after Deposit Wallet closeout")?;
    ensure_closeout_p_usd_delta(
        action,
        amount_units,
        p_usd_balance_before,
        p_usd_balance_after,
    )?;
    verify_closeout_position_state(config, account_address, action).await?;
    wait_for_closeout_receipt_finalized(config, block_number).await?;
    let reconciliation_execution_ids = closeout_reconciliation_execution_ids(action);
    append_combo_closeout_realized_pnl_records(
        config,
        action,
        transaction_hash,
        block_number,
        p_usd_balance_before,
        p_usd_balance_after,
        gas_accounting,
        &reconciliation_execution_ids,
    )?;
    let reconciled_execution_ids =
        append_closeout_manual_reconciliations(config, action, transaction_hash)?;
    let released_exposures =
        append_closeout_exposure_releases(config, &reconciliation_execution_ids)?;
    if released_exposures < reconciliation_execution_ids.len() {
        warn!(
            "Deposit Wallet Combo closeout exposure release wrote {} of {} expected release records",
            released_exposures,
            reconciliation_execution_ids.len()
        );
    }

    Ok(CloseoutExecutionResult {
        transaction_hash: transaction_hash.to_string(),
        block_number,
        reconciled_execution_ids,
    })
}

fn relayer_deadline(config: &Config) -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX_EPOCH")?
        .as_secs();
    now.checked_add(config.relayer_wallet_deadline_secs)
        .context("Relayer wallet deadline overflow")
}

async fn fetch_deposit_wallet_relayer_nonce(
    http: &Client,
    relayer: &DepositWalletRelayerConfig,
    config: &Config,
) -> Result<U256> {
    let url = format!("{}/v1/account/transactions/params", relayer.api_url);
    let response = http
        .get(&url)
        .header("RELAYER_API_KEY", &relayer.api_key)
        .header(
            "RELAYER_API_KEY_ADDRESS",
            relayer.api_key_address.to_string(),
        )
        .query(&[
            ("address", relayer.api_key_address.to_string()),
            ("type", "WALLET".to_string()),
        ])
        .timeout(Duration::from_secs(config.api_timeout_secs.max(1)))
        .send()
        .await
        .context("Relayer nonce request failed")?;
    let parsed: RelayerNonceResponse = relayer_json_response(response, "Relayer nonce").await?;
    u256_from_json_value(&parsed.nonce, "Relayer nonce")
}

async fn sign_deposit_wallet_batch(
    signer: &AlloyLocalSigner,
    config: &Config,
    wallet: Address,
    nonce: U256,
    deadline: U256,
    calls: &[RelayerCallJson],
) -> Result<String> {
    let calls = calls
        .iter()
        .map(relayer_call_json_to_eip712_call)
        .collect::<Result<Vec<_>>>()?;
    let batch = Batch {
        wallet,
        nonce,
        deadline,
        calls,
    };
    let domain = Eip712Domain {
        name: Some(Cow::Borrowed("DepositWallet")),
        version: Some(Cow::Borrowed("1")),
        chain_id: Some(U256::from(config.live_chain_id)),
        verifying_contract: Some(wallet),
        ..Eip712Domain::default()
    };
    let hash = batch.eip712_signing_hash(&domain);
    let signature = signer
        .sign_hash(&hash)
        .await
        .context("failed to sign Deposit Wallet Relayer batch")?;
    Ok(signature.to_string())
}

fn relayer_call_json_to_eip712_call(call: &RelayerCallJson) -> Result<Call> {
    let target = Address::from_str(&call.target)
        .with_context(|| format!("invalid Relayer call target '{}'", call.target))?;
    let value = U256::from_str_radix(call.value.trim(), 10)
        .with_context(|| format!("invalid Relayer call value '{}'", call.value))?;
    let data = Bytes::from(decode_hex_string(&call.data)?);
    Ok(Call {
        target,
        value,
        data,
    })
}

async fn submit_deposit_wallet_relayer_batch(
    http: &Client,
    relayer: &DepositWalletRelayerConfig,
    wallet: Address,
    nonce: U256,
    deadline: u64,
    signature: String,
    calls: Vec<RelayerCallJson>,
    metadata: String,
) -> Result<RelayerTransactionResponse> {
    let url = format!("{}/submit", relayer.api_url);
    let body = serde_json::json!({
        "type": "WALLET",
        "from": relayer.api_key_address.to_string(),
        "to": POLYMARKET_RELAYER_WALLET_SUBMIT_TO,
        "nonce": nonce.to_string(),
        "signature": signature,
        "metadata": metadata,
        "depositWalletParams": {
            "depositWallet": wallet.to_string(),
            "deadline": deadline.to_string(),
            "calls": calls,
        }
    });
    let response = http
        .post(&url)
        .header("Content-Type", "application/json")
        .header("RELAYER_API_KEY", &relayer.api_key)
        .header(
            "RELAYER_API_KEY_ADDRESS",
            relayer.api_key_address.to_string(),
        )
        .json(&body)
        .send()
        .await
        .context("Relayer submit request failed")?;
    relayer_json_response(response, "Relayer submit").await
}

async fn poll_deposit_wallet_relayer_transaction(
    http: &Client,
    relayer: &DepositWalletRelayerConfig,
    config: &Config,
    submitted: &RelayerTransactionResponse,
) -> Result<RelayerTransactionResponse> {
    let transaction_id = submitted
        .transaction_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| anyhow!("Relayer submit response missing transaction_id"))?;
    let deadline = Instant::now() + Duration::from_secs(config.live_closeout_confirm_timeout_secs);
    loop {
        let url = format!(
            "{}/v1/account/transactions/{}",
            relayer.api_url, transaction_id
        );
        let response = http
            .get(&url)
            .header("RELAYER_API_KEY", &relayer.api_key)
            .header(
                "RELAYER_API_KEY_ADDRESS",
                relayer.api_key_address.to_string(),
            )
            .timeout(Duration::from_secs(config.api_timeout_secs.max(1)))
            .send()
            .await
            .context("Relayer transaction poll request failed")?;
        let parsed: RelayerTransactionResponse =
            relayer_json_response(response, "Relayer transaction poll").await?;
        match parsed.state.as_deref().map(str::trim) {
            Some("STATE_CONFIRMED") => return Ok(parsed),
            Some("STATE_FAILED" | "STATE_INVALID") => {
                let state = parsed.state.as_deref().unwrap_or_default().to_string();
                let error_detail = parsed
                    .error_msg
                    .clone()
                    .or_else(|| parsed.error.as_ref().map(Value::to_string));
                bail!(
                    "Relayer transaction terminal failure state={} error={:?}",
                    state,
                    error_detail
                );
            }
            _ if Instant::now() >= deadline => {
                bail!(
                    "Relayer transaction did not confirm before timeout: id={} state={:?}",
                    transaction_id,
                    parsed.state
                );
            }
            _ => tokio::time::sleep(RELAYER_TRANSACTION_POLL_INTERVAL).await,
        }
    }
}

async fn relayer_json_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
    context_label: &str,
) -> Result<T> {
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("{context_label} response body unavailable"))?;
    if !status.is_success() {
        bail!(
            "{} failed with status {} body={}",
            context_label,
            status,
            truncate_report_text(&body, 256)
        );
    }
    serde_json::from_str(&body).with_context(|| {
        format!(
            "{} response was not valid JSON: {}",
            context_label,
            truncate_report_text(&body, 256)
        )
    })
}

fn u256_from_json_value(value: &Value, label: &str) -> Result<U256> {
    match value {
        Value::String(raw) => parse_u256_string(raw, label),
        Value::Number(number) => {
            let raw = number.to_string();
            parse_u256_string(&raw, label)
        }
        _ => bail!("{label} was not a string or number: {value}"),
    }
}

fn parse_u256_string(raw: &str, label: &str) -> Result<U256> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("{label} empty");
    }
    if let Some(hex) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
        U256::from_str_radix(hex, 16).with_context(|| format!("invalid {label} hex '{raw}'"))
    } else {
        U256::from_str_radix(raw, 10).with_context(|| format!("invalid {label} decimal '{raw}'"))
    }
}

fn decode_hex_string(raw: &str) -> Result<Vec<u8>> {
    let raw = raw.trim();
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    if !hex.len().is_multiple_of(2) {
        bail!("hex string has odd length: {raw}");
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for idx in (0..hex.len()).step_by(2) {
        bytes.push(
            u8::from_str_radix(&hex[idx..idx + 2], 16)
                .with_context(|| format!("invalid hex string '{raw}'"))?,
        );
    }
    Ok(bytes)
}

async fn execute_standard_closeout_action(
    config: &Config,
    account_address: Address,
    action: &LiveCloseoutRunAction,
) -> Result<CloseoutExecutionResult> {
    if action.negative_risk {
        bail!("negative-risk closeout is not supported by the standard pUSD adapter executor");
    }
    if closeout_wallet_type(config) != "EOA" {
        bail!("standard pUSD adapter closeout execution requires an EOA closeout wallet");
    }
    if action.call_preview.eth_call_status != "ok" {
        bail!(
            "standard pUSD adapter closeout requires successful eth_call preflight, got {}: {}",
            action.call_preview.eth_call_status,
            action.call_preview.eth_call_note
        );
    }

    let contract_cfg = contract_config(config.live_chain_id, false).with_context(|| {
        format!(
            "missing Polymarket standard contract config for chain_id={}",
            config.live_chain_id
        )
    })?;
    let target_contract = action
        .target_contract
        .as_deref()
        .ok_or_else(|| anyhow!("closeout action missing pUSD collateral adapter target"))?;
    let target_contract = Address::from_str(target_contract)
        .with_context(|| format!("invalid pUSD collateral adapter target '{target_contract}'"))?;
    let condition_id = B256::from_str(&action.condition_id)
        .with_context(|| format!("invalid condition id '{}'", action.condition_id))?;
    let amount_units = action
        .amount_ctf_units
        .as_deref()
        .ok_or_else(|| anyhow!("closeout action missing CTF amount units"))?;
    let amount_units = U256::from_str_radix(amount_units, 10)
        .with_context(|| format!("invalid CTF amount units '{amount_units}'"))?;
    let private_key = std::env::var(PRIVATE_KEY_VAR)
        .context("POLYMARKET_PRIVATE_KEY is required for closeout")?;
    let signer =
        AlloyLocalSigner::from_str(&private_key)?.with_chain_id(Some(config.live_chain_id));
    let rpc_url = config.polygon_rpc_url.trim();
    if rpc_url.is_empty() {
        bail!("POLYGON_RPC_URL is required for closeout execution");
    }
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect(rpc_url)
        .await
        .context("failed to connect Polygon RPC provider for closeout")?;
    let native_balance = provider
        .get_balance(account_address)
        .await
        .context("failed to read native POL balance before closeout")?;
    ensure_closeout_native_gas_balance(native_balance)?;
    let collateral = IERC20Balance::new(contract_cfg.collateral, provider.clone());
    let adapter = ICtfCollateralAdapter::new(target_contract, provider);
    let parent_collection_id = B256::default();
    let partition = vec![U256::from(1u8), U256::from(2u8)];
    let p_usd_balance_before = collateral
        .balanceOf(account_address)
        .call()
        .await
        .context("failed to read pUSD balance before closeout")?;

    let (transaction_hash, block_number, receipt_validation, gas_accounting) =
        match action.action.as_str() {
            "merge_full_set" => {
                let pending_tx = adapter
                    .mergePositions(
                        contract_cfg.collateral,
                        parent_collection_id,
                        condition_id,
                        partition.clone(),
                        amount_units,
                    )
                    .send()
                    .await
                    .context("standard pUSD adapter merge transaction failed to send")?;
                let transaction_hash = pending_tx.tx_hash().to_string();
                let receipt = pending_tx
                    .get_receipt()
                    .await
                    .context("standard pUSD adapter merge receipt unavailable")?;
                if !receipt.status() {
                    bail!("standard pUSD adapter merge transaction reverted");
                }
                let block_number = receipt.block_number.ok_or_else(|| {
                    anyhow!("standard pUSD adapter merge receipt missing block number")
                })?;
                let receipt_logs = closeout_receipt_log_summaries(receipt.logs());
                let receipt_validation = validate_closeout_receipt_logs(
                    &receipt_logs,
                    target_contract,
                    contract_cfg.collateral,
                    contract_cfg.conditional_tokens,
                    account_address,
                )?;
                let gas_accounting = closeout_gas_accounting(
                    &Client::new(),
                    config,
                    receipt.gas_used,
                    receipt.effective_gas_price,
                )
                .await;
                (
                    transaction_hash,
                    block_number,
                    receipt_validation,
                    gas_accounting,
                )
            }
            "redeem_resolved" => {
                let pending_tx = adapter
                    .redeemPositions(
                        contract_cfg.collateral,
                        parent_collection_id,
                        condition_id,
                        partition,
                    )
                    .send()
                    .await
                    .context("standard pUSD adapter redeem transaction failed to send")?;
                let transaction_hash = pending_tx.tx_hash().to_string();
                let receipt = pending_tx
                    .get_receipt()
                    .await
                    .context("standard pUSD adapter redeem receipt unavailable")?;
                if !receipt.status() {
                    bail!("standard pUSD adapter redeem transaction reverted");
                }
                let block_number = receipt.block_number.ok_or_else(|| {
                    anyhow!("standard pUSD adapter redeem receipt missing block number")
                })?;
                let receipt_logs = closeout_receipt_log_summaries(receipt.logs());
                let receipt_validation = validate_closeout_receipt_logs(
                    &receipt_logs,
                    target_contract,
                    contract_cfg.collateral,
                    contract_cfg.conditional_tokens,
                    account_address,
                )?;
                let gas_accounting = closeout_gas_accounting(
                    &Client::new(),
                    config,
                    receipt.gas_used,
                    receipt.effective_gas_price,
                )
                .await;
                (
                    transaction_hash,
                    block_number,
                    receipt_validation,
                    gas_accounting,
                )
            }
            other => bail!("unsupported closeout action for pUSD adapter executor: {other}"),
        };

    let p_usd_balance_after = collateral
        .balanceOf(account_address)
        .call()
        .await
        .context("failed to read pUSD balance after closeout")?;
    ensure_closeout_p_usd_delta(
        action,
        amount_units,
        p_usd_balance_before,
        p_usd_balance_after,
    )?;
    verify_closeout_position_state(config, account_address, action).await?;
    wait_for_closeout_receipt_finalized(config, block_number).await?;
    let reconciliation_execution_ids = closeout_reconciliation_execution_ids(action);
    append_closeout_realized_pnl_records(
        config,
        action,
        &transaction_hash,
        block_number,
        p_usd_balance_before,
        p_usd_balance_after,
        receipt_validation,
        gas_accounting,
        &reconciliation_execution_ids,
    )?;
    let reconciled_execution_ids =
        append_closeout_manual_reconciliations(config, action, &transaction_hash)?;
    let released_exposures =
        append_closeout_exposure_releases(config, &reconciliation_execution_ids)?;
    if released_exposures < reconciliation_execution_ids.len() {
        warn!(
            "closeout exposure release wrote {} of {} expected release records",
            released_exposures,
            reconciliation_execution_ids.len()
        );
    }

    Ok(CloseoutExecutionResult {
        transaction_hash,
        block_number,
        reconciled_execution_ids,
    })
}

fn closeout_receipt_log_summaries<T>(logs: &[T]) -> Vec<CloseoutReceiptLogSummary>
where
    T: AsRef<alloy::primitives::Log>,
{
    logs.iter()
        .map(|log| {
            let log = log.as_ref();
            CloseoutReceiptLogSummary {
                address: log.address,
                topics: log.topics().to_vec(),
            }
        })
        .collect()
}

fn validate_closeout_receipt_logs(
    logs: &[CloseoutReceiptLogSummary],
    adapter: Address,
    collateral: Address,
    conditional_tokens: Address,
    account: Address,
) -> Result<CloseoutReceiptValidation> {
    let erc20_transfer = event_topic("Transfer(address,address,uint256)");
    let erc1155_transfer_single =
        event_topic("TransferSingle(address,address,address,uint256,uint256)");
    let erc1155_transfer_batch =
        event_topic("TransferBatch(address,address,address,uint256[],uint256[])");

    let mut validation = CloseoutReceiptValidation {
        total_logs: logs.len(),
        adapter_logs: 0,
        collateral_transfer_to_account_logs: 0,
        ctf_transfer_logs: 0,
    };

    for log in logs {
        if log.address == adapter {
            validation.adapter_logs += 1;
        }
        let Some(topic0) = log.topics.first() else {
            continue;
        };
        if log.address == collateral
            && *topic0 == erc20_transfer
            && log
                .topics
                .get(2)
                .map(|topic| indexed_topic_matches_address(topic, account))
                .unwrap_or(false)
        {
            validation.collateral_transfer_to_account_logs += 1;
        }
        if log.address == conditional_tokens
            && (*topic0 == erc1155_transfer_single || *topic0 == erc1155_transfer_batch)
            && (log
                .topics
                .get(2)
                .map(|topic| indexed_topic_matches_address(topic, account))
                .unwrap_or(false)
                || log
                    .topics
                    .get(3)
                    .map(|topic| indexed_topic_matches_address(topic, account))
                    .unwrap_or(false))
        {
            validation.ctf_transfer_logs += 1;
        }
    }

    if validation.collateral_transfer_to_account_logs == 0 {
        bail!(
            "closeout receipt did not include a pUSD Transfer to account {}; refusing reconciliation",
            account
        );
    }
    if validation.ctf_transfer_logs == 0 {
        bail!(
            "closeout receipt did not include a CTF TransferSingle/TransferBatch involving account {}; refusing reconciliation",
            account
        );
    }

    Ok(validation)
}

fn validate_combo_redeem_receipt_logs(
    logs: &[CloseoutReceiptLogSummary],
    router: Address,
    collateral: Address,
    position_manager: Address,
    account: Address,
) -> Result<CloseoutReceiptValidation> {
    let erc20_transfer = event_topic("Transfer(address,address,uint256)");
    let erc1155_transfer_single =
        event_topic("TransferSingle(address,address,address,uint256,uint256)");
    let erc1155_transfer_batch =
        event_topic("TransferBatch(address,address,address,uint256[],uint256[])");

    let mut validation = CloseoutReceiptValidation {
        total_logs: logs.len(),
        adapter_logs: 0,
        collateral_transfer_to_account_logs: 0,
        ctf_transfer_logs: 0,
    };

    for log in logs {
        if log.address == router {
            validation.adapter_logs += 1;
        }
        let Some(topic0) = log.topics.first() else {
            continue;
        };
        if log.address == collateral
            && *topic0 == erc20_transfer
            && log
                .topics
                .get(2)
                .map(|topic| indexed_topic_matches_address(topic, account))
                .unwrap_or(false)
        {
            validation.collateral_transfer_to_account_logs += 1;
        }
        if log.address == position_manager
            && (*topic0 == erc1155_transfer_single || *topic0 == erc1155_transfer_batch)
            && (log
                .topics
                .get(2)
                .map(|topic| indexed_topic_matches_address(topic, account))
                .unwrap_or(false)
                || log
                    .topics
                    .get(3)
                    .map(|topic| indexed_topic_matches_address(topic, account))
                    .unwrap_or(false))
        {
            validation.ctf_transfer_logs += 1;
        }
    }

    if validation.collateral_transfer_to_account_logs == 0 {
        bail!(
            "Combo redeem receipt did not include a pUSD Transfer to account {}; refusing reconciliation",
            account
        );
    }
    if validation.ctf_transfer_logs == 0 {
        bail!(
            "Combo redeem receipt did not include a PositionManager TransferSingle/TransferBatch involving account {}; refusing reconciliation",
            account
        );
    }

    Ok(validation)
}

fn ensure_closeout_p_usd_delta(
    action: &LiveCloseoutRunAction,
    amount_units: U256,
    balance_before: U256,
    balance_after: U256,
) -> Result<U256> {
    if balance_after <= balance_before {
        bail!(
            "closeout did not increase pUSD balance: before={} after={}",
            balance_before,
            balance_after
        );
    }
    let delta = balance_after - balance_before;
    match action.action.as_str() {
        "merge_full_set" if delta < amount_units => bail!(
            "merge closeout pUSD delta {} was below expected full-set amount {}",
            delta,
            amount_units
        ),
        "redeem_resolved" if delta == U256::ZERO => {
            bail!("redeem closeout produced zero pUSD delta")
        }
        "combo_redeem_resolved_win_review" if delta == U256::ZERO => {
            bail!("Combo redeem closeout produced zero pUSD delta")
        }
        _ => Ok(delta),
    }
}

async fn closeout_gas_accounting(
    http: &Client,
    config: &Config,
    gas_used: u64,
    effective_gas_price_wei: u128,
) -> CloseoutGasAccounting {
    let gas_cost_wei = U256::from(gas_used) * U256::from(effective_gas_price_wei);
    let gas_cost_pol = wei_to_native_f64(gas_cost_wei);
    let gas_cost_usd = GasOracle::new()
        .native_pol_cost_usd(http, gas_cost_pol, config.gas_fallback_usd.max(0.0))
        .await;
    CloseoutGasAccounting {
        gas_used,
        effective_gas_price_wei,
        gas_cost_wei,
        gas_cost_pol,
        gas_cost_usd,
    }
}

fn event_topic(signature: &str) -> B256 {
    keccak256(signature.as_bytes())
}

fn indexed_topic_matches_address(topic: &B256, address: Address) -> bool {
    &topic.as_slice()[12..] == address.as_slice()
}

#[cfg(test)]
fn indexed_address_topic(address: Address) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    B256::from(bytes)
}

async fn verify_closeout_position_state(
    config: &Config,
    account_address: Address,
    action: &LiveCloseoutRunAction,
) -> Result<()> {
    if action.action == "combo_redeem_resolved_win_review" {
        return verify_combo_closeout_position_state(config, action).await;
    }

    let timeout = Duration::from_secs(config.live_closeout_confirm_timeout_secs);
    let deadline = Instant::now() + timeout;
    let data_client = DataClient::default();

    loop {
        let positions =
            fetch_account_positions(&data_client, account_address, "closeout-confirm").await?;
        let remaining: Vec<PositionView> = positions
            .iter()
            .map(PositionView::from)
            .filter(|position| {
                position.condition_id == action.condition_id && position.size > Decimal::ZERO
            })
            .collect();
        let confirmed = match action.action.as_str() {
            "merge_full_set" => remaining.is_empty(),
            "redeem_resolved" => !remaining.iter().any(|position| position.redeemable),
            _ => false,
        };
        if confirmed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let samples: Vec<String> = remaining
                .iter()
                .take(3)
                .map(|position| {
                    format!(
                        "asset={} outcome_index={} size={} redeemable={}",
                        position.asset, position.outcome_index, position.size, position.redeemable
                    )
                })
                .collect();
            bail!(
                "closeout position verification timed out after {}s for condition {} remaining={:?}",
                config.live_closeout_confirm_timeout_secs,
                action.condition_id,
                samples
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn verify_combo_closeout_position_state(
    config: &Config,
    action: &LiveCloseoutRunAction,
) -> Result<()> {
    let timeout = Duration::from_secs(config.live_closeout_confirm_timeout_secs);
    let deadline = Instant::now() + timeout;
    let http = Client::new();

    loop {
        let report = crate::combo_rfq_client::fetch_live_combo_exposure_report(&http, config).await;
        let remaining: Vec<&crate::combo_rfq_client::ComboPositionView> = report
            .combos
            .iter()
            .filter(|combo| combo_position_matches_closeout_action(combo, action))
            .filter(|combo| combo_position_is_redeemable_win(combo))
            .filter(|combo| combo_position_remaining_shares(combo) > Decimal::ZERO)
            .collect();
        if remaining.is_empty() && report.status != "error" {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let samples: Vec<String> = remaining
                .iter()
                .take(3)
                .map(|combo| {
                    format!(
                        "combo_position_id={:?} condition_id={} outcome_index={:?} status={:?} shares_balance={:?}",
                        combo.combo_position_id,
                        combo.combo_condition_id,
                        combo.combo_outcome_index,
                        combo.status,
                        combo.shares_balance
                    )
                })
                .collect();
            bail!(
                "Combo closeout position verification timed out after {}s for action {} report_status={} error={:?} remaining={:?}",
                config.live_closeout_confirm_timeout_secs,
                action.action_id,
                report.status,
                report.error,
                samples
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn combo_position_matches_closeout_action(
    combo: &crate::combo_rfq_client::ComboPositionView,
    action: &LiveCloseoutRunAction,
) -> bool {
    let position_matches = action
        .combo_position_id
        .as_deref()
        .map(str::trim)
        .filter(|position_id| !position_id.is_empty())
        .and_then(|position_id| {
            combo
                .combo_position_id
                .as_deref()
                .map(str::trim)
                .map(|combo_position_id| combo_position_id == position_id)
        })
        .unwrap_or(false);
    position_matches
        || combo
            .combo_condition_id
            .trim()
            .eq_ignore_ascii_case(&action.condition_id)
            && combo.combo_outcome_index == action.combo_outcome_index
}

fn combo_position_remaining_shares(combo: &crate::combo_rfq_client::ComboPositionView) -> Decimal {
    combo
        .shares_balance
        .as_deref()
        .and_then(|shares| Decimal::from_str(shares.trim()).ok())
        .unwrap_or(Decimal::ZERO)
}

async fn wait_for_closeout_receipt_finalized(
    config: &Config,
    receipt_block_number: u64,
) -> Result<u64> {
    if config.polygon_rpc_url.trim().is_empty() {
        bail!("POLYGON_RPC_URL is required to finalize closeout accounting");
    }
    let timeout = Duration::from_secs(config.live_closeout_confirm_timeout_secs);
    let deadline = Instant::now() + timeout;
    let http = Client::new();

    loop {
        let finalized = fetch_polygon_block_number_by_tag(&http, config, "finalized")
            .await
            .context("failed to read finalized Polygon block before closeout accounting release")?;
        if finalized >= receipt_block_number {
            return Ok(finalized);
        }
        if Instant::now() >= deadline {
            bail!(
                "closeout receipt not finalized before accounting release: receipt_block={} finalized_block={} timeout_secs={}",
                receipt_block_number,
                finalized,
                config.live_closeout_confirm_timeout_secs,
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn append_closeout_manual_reconciliations(
    config: &Config,
    action: &LiveCloseoutRunAction,
    transaction_hash: &str,
) -> Result<Vec<String>> {
    let reconciliation_execution_ids = closeout_reconciliation_execution_ids(action);
    if reconciliation_execution_ids.is_empty() {
        return Ok(Vec::new());
    }
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
    let mut writer = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening live execution journal {}", path.display()))?;
    let mut reconciled = Vec::new();
    for execution_id in reconciliation_execution_ids {
        let line = serde_json::json!({
            "timestamp": Utc::now().to_rfc3339(),
            "execution_id": execution_id,
            "stage": "manual_reconciled",
            "condition_id": action.condition_id,
            "closeout_action_id": action.action_id,
            "closeout_transaction_hash": transaction_hash,
        });
        writeln!(writer, "{line}")
            .with_context(|| format!("writing live execution journal {}", path.display()))?;
        reconciled.push(execution_id.clone());
    }
    writer
        .flush()
        .with_context(|| format!("flushing live execution journal {}", path.display()))?;
    Ok(reconciled)
}

fn closeout_reconciliation_execution_ids(action: &LiveCloseoutRunAction) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut ids = Vec::new();
    for execution_id in &action.unresolved_execution_ids {
        if seen.insert(execution_id.clone()) {
            ids.push(execution_id.clone());
        }
    }
    ids
}

fn append_closeout_realized_pnl_records(
    config: &Config,
    action: &LiveCloseoutRunAction,
    transaction_hash: &str,
    block_number: u64,
    p_usd_balance_before: U256,
    p_usd_balance_after: U256,
    receipt_validation: CloseoutReceiptValidation,
    gas_accounting: CloseoutGasAccounting,
    reconciliation_execution_ids: &[String],
) -> Result<()> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let journal_path = config.diagnostics_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
    let realized_path = config.diagnostics_dir.join(LIVE_REALIZED_PNL_FILE);
    let summaries =
        live_execution_accounting_summaries(&journal_path, reconciliation_execution_ids)?;
    let existing_keys = existing_realized_pnl_keys(&realized_path)?;
    let p_usd_delta = p_usd_balance_after - p_usd_balance_before;
    let p_usd_delta_usd = ctf_units_to_usd_f64(p_usd_delta);
    let allocations = closeout_pnl_allocations(reconciliation_execution_ids, &summaries);

    let mut writer = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&realized_path)
        .with_context(|| {
            format!(
                "opening live realized PnL ledger {}",
                realized_path.display()
            )
        })?;

    for (execution_id, allocation_ratio, summary) in allocations {
        let key = realized_pnl_key(execution_id.as_deref(), &action.action_id, transaction_hash);
        if existing_keys.contains(&key) {
            continue;
        }
        let allocated_p_usd_delta_usd = p_usd_delta_usd * allocation_ratio;
        let allocated_closeout_gas_cost_usd = gas_accounting.gas_cost_usd * allocation_ratio;
        let entry_cost_basis_usd = summary
            .as_ref()
            .and_then(LiveExecutionAccountingSummary::entry_cost_basis_usd);
        let realized_pnl_usd_before_closeout_gas =
            entry_cost_basis_usd.map(|entry_cost_usd| allocated_p_usd_delta_usd - entry_cost_usd);
        let realized_pnl_usd =
            realized_pnl_usd_before_closeout_gas.map(|pnl| pnl - allocated_closeout_gas_cost_usd);
        let record = LiveRealizedPnlRecord {
            timestamp: Utc::now().to_rfc3339(),
            execution_id,
            closeout_action_id: action.action_id.clone(),
            condition_id: action.condition_id.clone(),
            action: action.action.clone(),
            transaction_hash: transaction_hash.to_string(),
            block_number,
            p_usd_balance_before_units: p_usd_balance_before.to_string(),
            p_usd_balance_after_units: p_usd_balance_after.to_string(),
            p_usd_delta_units: p_usd_delta.to_string(),
            p_usd_delta_usd,
            allocated_p_usd_delta_usd,
            allocation_ratio,
            projected_position_usd: summary.as_ref().and_then(|summary| summary.position_usd),
            actual_fill_cost_usd: summary
                .as_ref()
                .and_then(|summary| summary.actual_fill_cost_usd),
            entry_fees_usd: summary.as_ref().and_then(|summary| summary.entry_fees_usd),
            entry_gas_cost_usd: summary
                .as_ref()
                .and_then(|summary| summary.entry_gas_cost_usd),
            actual_entry_cost_usd: summary
                .as_ref()
                .and_then(|summary| summary.actual_entry_cost_usd),
            projected_pnl_usd: summary
                .as_ref()
                .and_then(|summary| summary.projected_pnl_usd),
            projected_roi_pct: summary
                .as_ref()
                .and_then(|summary| summary.projected_roi_pct),
            closeout_gas_used: gas_accounting.gas_used,
            closeout_effective_gas_price_wei: gas_accounting.effective_gas_price_wei.to_string(),
            closeout_gas_cost_wei: gas_accounting.gas_cost_wei.to_string(),
            closeout_gas_cost_pol: gas_accounting.gas_cost_pol,
            closeout_gas_cost_usd: gas_accounting.gas_cost_usd,
            allocated_closeout_gas_cost_usd,
            realized_pnl_usd_before_closeout_gas,
            realized_pnl_usd,
            receipt_total_logs: receipt_validation.total_logs,
            receipt_adapter_logs: receipt_validation.adapter_logs,
            receipt_collateral_transfer_to_account_logs: receipt_validation
                .collateral_transfer_to_account_logs,
            receipt_ctf_transfer_logs: receipt_validation.ctf_transfer_logs,
        };
        serde_json::to_writer(&mut writer, &record).with_context(|| {
            format!(
                "writing live realized PnL ledger {}",
                realized_path.display()
            )
        })?;
        writer.write_all(b"\n").with_context(|| {
            format!(
                "writing live realized PnL ledger {}",
                realized_path.display()
            )
        })?;
    }
    writer.flush().with_context(|| {
        format!(
            "flushing live realized PnL ledger {}",
            realized_path.display()
        )
    })?;
    Ok(())
}

pub fn append_combo_rfq_realized_pnl_record(
    config: &Config,
    record: &ComboRfqRealizedPnlRecord,
) -> Result<bool> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let realized_path = config.diagnostics_dir.join(LIVE_REALIZED_PNL_FILE);
    let existing_keys = existing_realized_pnl_keys(&realized_path)?;
    let key = realized_pnl_key(
        record.execution_id.as_deref(),
        &record.closeout_action_id,
        &record.transaction_hash,
    );
    if existing_keys.contains(&key) {
        return Ok(false);
    }

    let mut writer = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&realized_path)
        .with_context(|| {
            format!(
                "opening live realized PnL ledger {}",
                realized_path.display()
            )
        })?;
    serde_json::to_writer(&mut writer, record).with_context(|| {
        format!(
            "writing Combo/RFQ realized PnL ledger {}",
            realized_path.display()
        )
    })?;
    writer.write_all(b"\n").with_context(|| {
        format!(
            "writing Combo/RFQ realized PnL ledger {}",
            realized_path.display()
        )
    })?;
    writer.flush().with_context(|| {
        format!(
            "flushing Combo/RFQ realized PnL ledger {}",
            realized_path.display()
        )
    })?;
    Ok(true)
}

fn append_combo_closeout_realized_pnl_records(
    config: &Config,
    action: &LiveCloseoutRunAction,
    transaction_hash: &str,
    block_number: u64,
    p_usd_balance_before: U256,
    p_usd_balance_after: U256,
    gas_accounting: CloseoutGasAccounting,
    reconciliation_execution_ids: &[String],
) -> Result<()> {
    let journal_path = config.diagnostics_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
    let summaries =
        live_execution_accounting_summaries(&journal_path, reconciliation_execution_ids)?;
    let p_usd_delta = p_usd_balance_after - p_usd_balance_before;
    let p_usd_delta_usd = ctf_units_to_usd_f64(p_usd_delta);
    let action_qty = Decimal::from_str(&action.amount_shares)
        .ok()
        .map(|value| decimal_to_f64(&value))
        .filter(|qty: &f64| qty.is_finite() && *qty > 0.0);

    for (execution_id, allocation_ratio, summary) in
        closeout_pnl_allocations(reconciliation_execution_ids, &summaries)
    {
        let allocated_payout_usd = p_usd_delta_usd * allocation_ratio;
        let allocated_closeout_gas_usd = gas_accounting.gas_cost_usd * allocation_ratio;
        let entry_cost_basis_usd = summary
            .as_ref()
            .and_then(LiveExecutionAccountingSummary::entry_cost_basis_usd);
        let realized_ev_usd = entry_cost_basis_usd
            .map(|entry_cost| allocated_payout_usd - entry_cost - allocated_closeout_gas_usd)
            .unwrap_or(allocated_payout_usd - allocated_closeout_gas_usd);
        let qty_decimal = action_qty.map(|qty| qty * allocation_ratio);
        let price = match (
            summary
                .as_ref()
                .and_then(|summary| summary.actual_fill_cost_usd),
            qty_decimal,
        ) {
            (Some(cost), Some(qty)) if qty > f64::EPSILON => Some(cost / qty),
            _ => None,
        };
        let record = ComboRfqRealizedPnlRecord {
            timestamp: Utc::now().to_rfc3339(),
            source: "combo_closeout_router".into(),
            execution_id,
            closeout_action_id: action.action_id.clone(),
            condition_id: action.condition_id.clone(),
            action: action.action.clone(),
            transaction_hash: transaction_hash.to_string(),
            block_number: Some(block_number),
            finality_id: format!("combo_closeout:{}:{transaction_hash}", action.action_id),
            rfq_id: None,
            quote_id: None,
            maker_id: None,
            status: "combo_redeem_closeout_confirmed".into(),
            status_class: "closeout_confirmed".into(),
            realized_ev_usd,
            expected_edge_usd: summary
                .as_ref()
                .and_then(|summary| summary.projected_pnl_usd),
            price,
            qty_decimal,
            order_hash: None,
            token_id: action.combo_position_id.clone(),
            fee: None,
        };
        append_combo_rfq_realized_pnl_record(config, &record)?;
    }

    Ok(())
}

fn append_closeout_exposure_releases(
    config: &Config,
    reconciliation_execution_ids: &[String],
) -> Result<usize> {
    if reconciliation_execution_ids.is_empty() {
        return Ok(0);
    }
    let journal_path = config.diagnostics_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
    let summaries =
        live_execution_accounting_summaries(&journal_path, reconciliation_execution_ids)?;
    let mut released = 0usize;
    for execution_id in reconciliation_execution_ids {
        let Some(summary) = summaries.get(execution_id) else {
            warn!(
                "closeout exposure release skipped for execution_id={} because no accounting summary was found",
                execution_id
            );
            continue;
        };
        let Some(event_id) = summary
            .event_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
        else {
            warn!(
                "closeout exposure release skipped for execution_id={} because event_id is missing",
                execution_id
            );
            continue;
        };
        let Some(amount_usd) = summary.release_amount_usd() else {
            warn!(
                "closeout exposure release skipped for execution_id={} event_id={} because release amount is unavailable",
                execution_id, event_id
            );
            continue;
        };
        append_exposure_ledger_delta(
            &config.diagnostics_dir,
            event_id,
            -amount_usd,
            "released",
            "live_closeout",
        )
        .with_context(|| {
            format!(
                "writing closeout exposure release for execution_id={} event_id={}",
                execution_id, event_id
            )
        })?;
        released += 1;
    }
    Ok(released)
}

fn live_execution_accounting_summaries(
    path: &Path,
    execution_ids: &[String],
) -> Result<HashMap<String, LiveExecutionAccountingSummary>> {
    if execution_ids.is_empty() || !path.exists() {
        return Ok(HashMap::new());
    }
    let wanted: HashSet<&str> = execution_ids.iter().map(String::as_str).collect();
    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading live execution journal {}", path.display()))?;
    let mut summaries = HashMap::new();
    for (line_index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: LiveJournalAccountingLine = serde_json::from_str(line).with_context(|| {
            format!(
                "parsing live execution journal {} line {}",
                path.display(),
                line_index + 1
            )
        })?;
        let Some(execution_id) = parsed.execution_id else {
            continue;
        };
        if !wanted.contains(execution_id.as_str()) {
            continue;
        }
        if parsed.stage.as_deref() == Some("manual_reconciled") {
            continue;
        }
        let summary = summaries
            .entry(execution_id)
            .or_insert_with(LiveExecutionAccountingSummary::default);
        if let Some(stage) = parsed.stage {
            summary.latest_stage = Some(stage);
        }
        if parsed.event_id.is_some() {
            summary.event_id = parsed.event_id;
        }
        if parsed.position_usd.is_some() {
            summary.position_usd = parsed.position_usd;
        }
        if parsed.actual_fill_cost_usd.is_some() {
            summary.actual_fill_cost_usd = parsed.actual_fill_cost_usd;
        }
        if parsed.entry_fees_usd.is_some() {
            summary.entry_fees_usd = parsed.entry_fees_usd;
        }
        if parsed.entry_gas_cost_usd.is_some() {
            summary.entry_gas_cost_usd = parsed.entry_gas_cost_usd;
        }
        if parsed.actual_entry_cost_usd.is_some() {
            summary.actual_entry_cost_usd = parsed.actual_entry_cost_usd;
        }
        if parsed.projected_pnl_usd.is_some() {
            summary.projected_pnl_usd = parsed.projected_pnl_usd;
        }
        if parsed.projected_roi_pct.is_some() {
            summary.projected_roi_pct = parsed.projected_roi_pct;
        }
        if parsed.basket_units.is_some() {
            summary.basket_units = parsed.basket_units;
        }
    }
    Ok(summaries)
}

fn closeout_pnl_allocations(
    reconciliation_execution_ids: &[String],
    summaries: &HashMap<String, LiveExecutionAccountingSummary>,
) -> Vec<(Option<String>, f64, Option<LiveExecutionAccountingSummary>)> {
    if reconciliation_execution_ids.is_empty() {
        return vec![(None, 1.0, None)];
    }
    let positive_weight_sum: f64 = reconciliation_execution_ids
        .iter()
        .filter_map(|execution_id| summaries.get(execution_id))
        .filter_map(LiveExecutionAccountingSummary::entry_cost_basis_usd)
        .filter(|position_usd| *position_usd > 0.0)
        .sum();
    let equal_ratio = 1.0 / reconciliation_execution_ids.len() as f64;

    reconciliation_execution_ids
        .iter()
        .map(|execution_id| {
            let summary = summaries.get(execution_id).cloned();
            let allocation_ratio = if positive_weight_sum > f64::EPSILON {
                summary
                    .as_ref()
                    .and_then(LiveExecutionAccountingSummary::entry_cost_basis_usd)
                    .filter(|position_usd| *position_usd > 0.0)
                    .map(|position_usd| position_usd / positive_weight_sum)
                    .unwrap_or(0.0)
            } else {
                equal_ratio
            };
            (Some(execution_id.clone()), allocation_ratio, summary)
        })
        .collect()
}

fn existing_realized_pnl_keys(path: &Path) -> Result<HashSet<String>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading live realized PnL ledger {}", path.display()))?;
    let mut keys = HashSet::new();
    for (line_index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: LiveRealizedPnlKeyLine = serde_json::from_str(line).with_context(|| {
            format!(
                "parsing live realized PnL ledger {} line {}",
                path.display(),
                line_index + 1
            )
        })?;
        let key = realized_pnl_key(
            parsed.execution_id.as_deref(),
            parsed.closeout_action_id.as_deref().unwrap_or_default(),
            parsed.transaction_hash.as_deref().unwrap_or_default(),
        );
        keys.insert(key);
    }
    Ok(keys)
}

fn realized_pnl_key(
    execution_id: Option<&str>,
    closeout_action_id: &str,
    transaction_hash: &str,
) -> String {
    format!(
        "{}|{}|{}",
        execution_id.unwrap_or("<unlinked>"),
        closeout_action_id,
        transaction_hash
    )
}

fn ctf_units_to_usd_f64(units: U256) -> f64 {
    units.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000.0
}

fn wei_to_native_f64(units: U256) -> f64 {
    units.to_string().parse::<f64>().unwrap_or(0.0) / 1_000_000_000_000_000_000.0
}

fn closeout_native_gas_floor_wei() -> U256 {
    U256::from(MIN_CLOSEOUT_NATIVE_GAS_WEI)
}

fn ensure_closeout_native_gas_balance(balance_wei: U256) -> Result<()> {
    let minimum = closeout_native_gas_floor_wei();
    if balance_wei < minimum {
        bail!(
            "standard closeout requires at least {:.6} native POL for gas; balance={:.6} POL",
            wei_to_native_f64(minimum),
            wei_to_native_f64(balance_wei),
        );
    }
    Ok(())
}

fn closeout_action_id(action: &LiveCloseoutAction) -> String {
    format!("{}:{}", action.action, action.condition_id)
}

fn build_live_closeout_run_action(
    config: &Config,
    action: &LiveCloseoutAction,
    unresolved_by_condition: &HashMap<String, Vec<String>>,
) -> LiveCloseoutRunAction {
    let action_id = closeout_action_id(action);
    let wallet_type = closeout_wallet_type(config);
    let standard_config = if closeout_action_is_combo_redeem(action) {
        None
    } else {
        contract_config(config.live_chain_id, false)
    };
    let collateral_token = standard_config.map(|cfg| cfg.collateral.to_string());
    let target_contract =
        closeout_target_contract_address(config.live_chain_id, action).map(|addr| addr.to_string());
    let kind = closeout_action_kind(&action.action);
    let expected_position_delta = closeout_expected_position_delta(action);
    let amount_ctf_units = closeout_action_ctf_units(action).ok();
    let calldata = closeout_action_calldata(
        action,
        collateral_token.as_deref(),
        amount_ctf_units.as_deref(),
    )
    .ok();
    let call_preview = closeout_call_preview(
        config,
        action,
        kind.as_str(),
        target_contract.clone(),
        collateral_token.clone(),
        amount_ctf_units.clone(),
    );
    let account_for_query = configured_live_account_address(config)
        .map(|address| address.to_string())
        .unwrap_or_else(|_| "<configured-live-account>".into());
    let verification_query = if closeout_action_is_combo_redeem(action) {
        format!("/v1/positions/combos?user={account_for_query}&status=RESOLVED_WIN&limit=100")
    } else {
        format!(
            "/positions?user={account_for_query}&sizeThreshold=0&limit={STARTUP_POSITIONS_PAGE_LIMIT}&conditionId={}",
            action.condition_id,
        )
    };
    let blockers = closeout_action_blockers(config, action, target_contract.as_deref());
    let (status, reason) = match action.action.as_str() {
        "merge_full_set" | "redeem_resolved" if blockers.is_empty() => (
            "ready",
            "standard closeout action is ready for SDK execution",
        ),
        "merge_full_set" | "redeem_resolved" if config.live_closeout_dry_run => (
            "dry_run_candidate",
            "standard closeout candidate; dry-run report only",
        ),
        "merge_full_set" | "redeem_resolved" => (
            "blocked",
            "standard closeout candidate is blocked by one or more execution preconditions",
        ),
        "neg_risk_merge_review" => (
            "review_only",
            "negative-risk merge requires adapter-specific execution and manual verification",
        ),
        "neg_risk_redeem_review" => (
            "review_only",
            "negative-risk redemption requires adapter-specific execution and manual verification",
        ),
        "combo_redeem_resolved_win_review" if blockers.is_empty() => (
            "ready",
            "resolved winning Combo redeem action is ready for EOA Router execution",
        ),
        "combo_redeem_resolved_win_review" if config.live_closeout_dry_run => (
            "dry_run_candidate",
            "resolved winning Combo redeem candidate; dry-run report only",
        ),
        "combo_redeem_resolved_win_review" => (
            "blocked",
            "resolved winning Combo redeem candidate is blocked by one or more execution preconditions",
        ),
        _ => (
            "unsupported",
            "unknown closeout action type; no automatic execution path is available",
        ),
    };

    LiveCloseoutRunAction {
        action_id,
        action: action.action.clone(),
        kind,
        condition_id: action.condition_id.clone(),
        title: action.title.clone(),
        slug: action.slug.clone(),
        negative_risk: action.negative_risk,
        amount_shares: action.amount_shares.clone(),
        yes_asset: action.yes_asset.clone(),
        no_asset: action.no_asset.clone(),
        combo_position_id: action.combo_position_id.clone(),
        combo_outcome_index: action.combo_outcome_index,
        wallet_type,
        target_contract,
        calldata,
        value: "0".into(),
        call_preview,
        collateral_token,
        amount_ctf_units,
        expected_position_delta,
        verification_query,
        blockers,
        unresolved_execution_ids: unresolved_by_condition
            .get(&action.condition_id)
            .cloned()
            .unwrap_or_default(),
        transaction_hash: None,
        block_number: None,
        reconciled_execution_ids: Vec::new(),
        status: status.into(),
        reason: reason.into(),
    }
}

fn closeout_action_kind(action: &str) -> String {
    match action {
        "merge_full_set" | "neg_risk_merge_review" => "merge_positions",
        "redeem_resolved" | "neg_risk_redeem_review" => "redeem_positions",
        "combo_redeem_resolved_win_review" => "combo_redeem_positions",
        _ => "unsupported",
    }
    .into()
}

fn closeout_action_is_combo_redeem(action: &LiveCloseoutAction) -> bool {
    action.action == "combo_redeem_resolved_win_review"
}

fn closeout_target_contract_address(chain_id: u64, action: &LiveCloseoutAction) -> Option<Address> {
    if closeout_action_is_combo_redeem(action) {
        return match chain_id {
            POLYGON_CHAIN_ID => Address::from_str(POLYGON_COMBO_ROUTER).ok(),
            _ => None,
        };
    }
    closeout_collateral_adapter_address(chain_id, action.negative_risk)
}

fn combo_position_manager_address(chain_id: u64) -> Option<Address> {
    match chain_id {
        POLYGON_CHAIN_ID => Address::from_str(POLYGON_COMBO_POSITION_MANAGER).ok(),
        _ => None,
    }
}

fn closeout_collateral_adapter_address(chain_id: u64, negative_risk: bool) -> Option<Address> {
    let address = match (chain_id, negative_risk) {
        (POLYGON_CHAIN_ID, false) => POLYGON_CTF_COLLATERAL_ADAPTER,
        (POLYGON_CHAIN_ID, true) => POLYGON_NEG_RISK_CTF_COLLATERAL_ADAPTER,
        _ => return None,
    };
    Address::from_str(address).ok()
}

fn closeout_action_calldata(
    action: &LiveCloseoutAction,
    collateral_token: Option<&str>,
    amount_ctf_units: Option<&str>,
) -> Result<String> {
    let parent_collection_id = B256::default();
    let partition = [U256::from(1u8), U256::from(2u8)];

    match action.action.as_str() {
        "merge_full_set" => {
            let condition_id = B256::from_str(&action.condition_id).with_context(|| {
                format!("invalid closeout condition id '{}'", action.condition_id)
            })?;
            let collateral_token = parse_closeout_collateral_token(collateral_token)?;
            let amount_ctf_units = amount_ctf_units
                .ok_or_else(|| anyhow!("missing CTF amount units for closeout calldata"))?;
            let amount = U256::from_str_radix(amount_ctf_units, 10)
                .with_context(|| format!("invalid CTF amount units '{amount_ctf_units}'"))?;
            Ok(encode_merge_positions_calldata(
                collateral_token,
                parent_collection_id,
                condition_id,
                &partition,
                amount,
            ))
        }
        "redeem_resolved" => {
            let condition_id = B256::from_str(&action.condition_id).with_context(|| {
                format!("invalid closeout condition id '{}'", action.condition_id)
            })?;
            let collateral_token = parse_closeout_collateral_token(collateral_token)?;
            Ok(encode_redeem_positions_calldata(
                collateral_token,
                parent_collection_id,
                condition_id,
                &partition,
            ))
        }
        "combo_redeem_resolved_win_review" => {
            let outcome_index = action
                .combo_outcome_index
                .ok_or_else(|| anyhow!("missing Combo outcome index for redeem calldata"))?;
            let amount_ctf_units = amount_ctf_units
                .ok_or_else(|| anyhow!("missing Combo amount units for redeem calldata"))?;
            let amount = U256::from_str_radix(amount_ctf_units, 10)
                .with_context(|| format!("invalid Combo amount units '{amount_ctf_units}'"))?;
            let condition_word = combo_redeem_condition_id_abi_word(action)?;
            Ok(encode_combo_redeem_calldata(
                condition_word,
                U256::from(outcome_index),
                amount,
            ))
        }
        other => bail!("closeout action has no standard calldata encoder: {other}"),
    }
}

fn parse_closeout_collateral_token(collateral_token: Option<&str>) -> Result<Address> {
    let collateral_token = collateral_token
        .ok_or_else(|| anyhow!("missing collateral token for closeout calldata"))?;
    Address::from_str(collateral_token)
        .with_context(|| format!("invalid closeout collateral token '{collateral_token}'"))
}

fn encode_merge_positions_calldata(
    collateral_token: Address,
    parent_collection_id: B256,
    condition_id: B256,
    partition: &[U256],
    amount: U256,
) -> String {
    let mut bytes = Vec::with_capacity(4 + 32 * (5 + 1 + partition.len()));
    bytes.extend_from_slice(&abi_selector(
        "mergePositions(address,bytes32,bytes32,uint256[],uint256)",
    ));
    push_abi_address_word(&mut bytes, collateral_token);
    push_abi_b256_word(&mut bytes, parent_collection_id);
    push_abi_b256_word(&mut bytes, condition_id);
    push_abi_u256_word(&mut bytes, U256::from(32usize * 5));
    push_abi_u256_word(&mut bytes, amount);
    push_abi_u256_array(&mut bytes, partition);
    format!("0x{}", hex_encode_lower(&bytes))
}

fn encode_redeem_positions_calldata(
    collateral_token: Address,
    parent_collection_id: B256,
    condition_id: B256,
    index_sets: &[U256],
) -> String {
    let mut bytes = Vec::with_capacity(4 + 32 * (4 + 1 + index_sets.len()));
    bytes.extend_from_slice(&abi_selector(
        "redeemPositions(address,bytes32,bytes32,uint256[])",
    ));
    push_abi_address_word(&mut bytes, collateral_token);
    push_abi_b256_word(&mut bytes, parent_collection_id);
    push_abi_b256_word(&mut bytes, condition_id);
    push_abi_u256_word(&mut bytes, U256::from(32usize * 4));
    push_abi_u256_array(&mut bytes, index_sets);
    format!("0x{}", hex_encode_lower(&bytes))
}

fn encode_combo_redeem_calldata(
    condition_id_word: [u8; 32],
    outcome_index: U256,
    amount: U256,
) -> String {
    format!(
        "0x{}",
        hex_encode_lower(&encode_combo_redeem_calldata_bytes(
            condition_id_word,
            outcome_index,
            amount,
        ))
    )
}

fn encode_combo_redeem_calldata_bytes(
    condition_id_word: [u8; 32],
    outcome_index: U256,
    amount: U256,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + 32 * 3);
    bytes.extend_from_slice(&abi_selector("redeem(bytes31,uint256,uint256)"));
    bytes.extend_from_slice(&condition_id_word);
    push_abi_u256_word(&mut bytes, outcome_index);
    push_abi_u256_word(&mut bytes, amount);
    bytes
}

fn combo_redeem_condition_id_abi_word(action: &LiveCloseoutAction) -> Result<[u8; 32]> {
    if let Some(position_id) = action
        .combo_position_id
        .as_deref()
        .map(str::trim)
        .filter(|position_id| !position_id.is_empty())
    {
        return combo_redeem_condition_id_abi_word_from_position_id(
            position_id,
            action.combo_outcome_index,
        );
    }
    parse_combo_condition_id_abi_word(&action.condition_id)
}

fn combo_redeem_condition_id_abi_word_from_run_action(
    action: &LiveCloseoutRunAction,
) -> Result<[u8; 32]> {
    if let Some(position_id) = action
        .combo_position_id
        .as_deref()
        .map(str::trim)
        .filter(|position_id| !position_id.is_empty())
    {
        return combo_redeem_condition_id_abi_word_from_position_id(
            position_id,
            action.combo_outcome_index,
        );
    }
    parse_combo_condition_id_abi_word(&action.condition_id)
}

fn combo_redeem_condition_id_abi_word_from_position_id(
    position_id: &str,
    expected_outcome_index: Option<u8>,
) -> Result<[u8; 32]> {
    let token_id = U256::from_str_radix(position_id, 10)
        .with_context(|| format!("invalid Combo position id '{position_id}'"))?;
    let mut word = token_id.to_be_bytes::<32>();
    let observed_outcome_index = word[31];
    if !matches!(observed_outcome_index, 0 | 1) {
        bail!(
            "Combo position id low byte must be 0 or 1 for Router redeem, got {observed_outcome_index}"
        );
    }
    if let Some(expected) = expected_outcome_index {
        if observed_outcome_index != expected {
            bail!(
                "Combo position id outcome byte {observed_outcome_index} did not match catalog outcome index {expected}"
            );
        }
    }
    word[31] = 0;
    Ok(word)
}

fn parse_combo_condition_id_abi_word(condition_id: &str) -> Result<[u8; 32]> {
    let condition_id = condition_id.trim();
    let hex = condition_id
        .strip_prefix("0x")
        .or_else(|| condition_id.strip_prefix("0X"))
        .unwrap_or(condition_id);
    if !hex.len().is_multiple_of(2) {
        bail!("Combo condition id hex has odd length: {condition_id}");
    }
    let mut raw = Vec::with_capacity(hex.len() / 2);
    for idx in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[idx..idx + 2], 16)
            .with_context(|| format!("invalid Combo condition id hex '{condition_id}'"))?;
        raw.push(byte);
    }
    match raw.len() {
        31 => {
            let mut word = [0u8; 32];
            word[..31].copy_from_slice(&raw);
            Ok(word)
        }
        32 if raw[31] == 0 => {
            let mut word = [0u8; 32];
            word.copy_from_slice(&raw);
            Ok(word)
        }
        32 => bail!(
            "Combo condition id must be bytes31 or an ABI-padded bytes31 word; got 32 bytes with non-zero low byte"
        ),
        other => bail!("Combo condition id must be 31 bytes, got {other} bytes"),
    }
}

fn abi_selector(signature: &str) -> [u8; 4] {
    let hash = keccak256(signature.as_bytes());
    [hash[0], hash[1], hash[2], hash[3]]
}

fn push_abi_address_word(bytes: &mut Vec<u8>, address: Address) {
    bytes.extend_from_slice(&[0u8; 12]);
    bytes.extend_from_slice(address.as_slice());
}

fn push_abi_b256_word(bytes: &mut Vec<u8>, value: B256) {
    bytes.extend_from_slice(value.as_slice());
}

fn push_abi_u256_word(bytes: &mut Vec<u8>, value: U256) {
    bytes.extend_from_slice(&value.to_be_bytes::<32>());
}

fn push_abi_u256_array(bytes: &mut Vec<u8>, values: &[U256]) {
    push_abi_u256_word(bytes, U256::from(values.len()));
    for value in values {
        push_abi_u256_word(bytes, *value);
    }
}

fn hex_encode_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn truncate_report_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated: String = text.chars().take(max_chars.saturating_sub(3)).collect();
    truncated.push_str("...");
    truncated
}

fn closeout_wallet_type(config: &Config) -> String {
    match (
        config.live_signature_type,
        config.live_funder_address.trim().is_empty(),
    ) {
        (0, true) => "EOA",
        (0, false) => "DEPOSIT",
        (1, _) => "PROXY",
        (2, _) => "SAFE",
        (3, _) => "DEPOSIT",
        _ => "unknown",
    }
    .into()
}

fn deposit_wallet_relayer_config(config: &Config) -> Result<DepositWalletRelayerConfig> {
    if config.live_signature_type != 3 {
        bail!(
            "LIVE_SIGNATURE_TYPE={} but Deposit Wallet Relayer closeout requires 3",
            config.live_signature_type
        );
    }
    let api_url = config.relayer_api_url.trim();
    if api_url.is_empty() {
        bail!("RELAYER_API_URL_empty");
    }
    let api_key = config.relayer_api_key.trim();
    if api_key.is_empty() {
        bail!("RELAYER_API_KEY_empty");
    }
    let api_key_address = config.relayer_api_key_address.trim();
    if api_key_address.is_empty() {
        bail!("RELAYER_API_KEY_ADDRESS_empty");
    }
    let api_key_address =
        Address::from_str(api_key_address).context("RELAYER_API_KEY_ADDRESS_invalid")?;
    Ok(DepositWalletRelayerConfig {
        api_url: api_url.trim_end_matches('/').to_string(),
        api_key: api_key.to_string(),
        api_key_address,
    })
}

fn deposit_wallet_relayer_config_blockers(config: &Config) -> Vec<String> {
    match deposit_wallet_relayer_config(config) {
        Ok(_) => Vec::new(),
        Err(err) => vec![err.to_string()],
    }
}

fn closeout_expected_position_delta(action: &LiveCloseoutAction) -> String {
    match action.action.as_str() {
        "merge_full_set" | "neg_risk_merge_review" => format!(
            "burn {} YES and {} NO shares up to {} full-set shares",
            action.yes_size.as_deref().unwrap_or("unknown"),
            action.no_size.as_deref().unwrap_or("unknown"),
            action.amount_shares,
        ),
        "redeem_resolved" | "neg_risk_redeem_review" => format!(
            "burn redeemable winning shares for condition {}",
            action.condition_id
        ),
        "combo_redeem_resolved_win_review" => format!(
            "burn resolved winning Combo position {} for condition {}",
            action
                .combo_position_id
                .as_deref()
                .unwrap_or("<unknown-combo-position>"),
            action.condition_id
        ),
        _ => "no supported position delta".into(),
    }
}

fn closeout_call_preview(
    config: &Config,
    action: &LiveCloseoutAction,
    kind: &str,
    target_contract: Option<String>,
    collateral_token: Option<String>,
    amount_ctf_units: Option<String>,
) -> LiveCloseoutCallPreview {
    let partition = closeout_partition(action);
    let expected_collateral_delta = closeout_expected_collateral_delta(action);
    let eth_call_status: String =
        if closeout_action_is_combo_redeem(action) && closeout_wallet_type(config) != "EOA" {
            "relayer_submit_required".into()
        } else if closeout_action_is_combo_redeem(action) {
            if config.polygon_rpc_url.trim().is_empty() {
                "not_checked_missing_polygon_rpc_url".into()
            } else {
                "not_checked_report_only".into()
            }
        } else if !matches!(action.action.as_str(), "merge_full_set" | "redeem_resolved") {
            "not_supported".into()
        } else if config.polygon_rpc_url.trim().is_empty() {
            "not_checked_missing_polygon_rpc_url".into()
        } else {
            "not_checked_report_only".into()
        };
    let eth_call_note = match eth_call_status.as_str() {
        "relayer_submit_required" => {
            format!(
                "Combo redeem for this wallet type requires Polymarket Relayer /submit with PositionManager {} approval; no direct EOA Router eth_call is authoritative",
                POLYGON_COMBO_POSITION_MANAGER
            )
        }
        "not_supported" => "no automatic transaction path exists for this action".into(),
        "not_checked_missing_polygon_rpc_url" => {
            "POLYGON_RPC_URL is required before an eth_call simulation can run".into()
        }
        _ => {
            "sync report preview only; live closeout run simulates this calldata before any transaction submission".into()
        }
    };

    LiveCloseoutCallPreview {
        function: kind.to_string(),
        from: configured_live_account_address(config)
            .ok()
            .map(|address| address.to_string()),
        target_contract,
        collateral_token,
        condition_id: action.condition_id.clone(),
        parent_collection_id: "0x0000000000000000000000000000000000000000000000000000000000000000"
            .into(),
        partition,
        amount_ctf_units,
        expected_collateral_delta,
        eth_call_block: "latest".into(),
        eth_call_status,
        eth_call_note,
    }
}

fn closeout_partition(action: &LiveCloseoutAction) -> Vec<u8> {
    match action.action.as_str() {
        "merge_full_set"
        | "redeem_resolved"
        | "neg_risk_merge_review"
        | "neg_risk_redeem_review" => {
            vec![1, 2]
        }
        "combo_redeem_resolved_win_review" => action
            .combo_outcome_index
            .map(|idx| vec![idx])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn closeout_expected_collateral_delta(action: &LiveCloseoutAction) -> String {
    match action.action.as_str() {
        "merge_full_set" | "neg_risk_merge_review" => {
            format!(
                "increase collateral by approximately {} full-set shares before fees/gas",
                action.amount_shares
            )
        }
        "redeem_resolved" | "neg_risk_redeem_review" => {
            "increase collateral by resolved payout amount; exact payout depends on oracle result"
                .into()
        }
        "combo_redeem_resolved_win_review" => {
            format!(
                "increase USDC by up to {} resolved winning Combo shares before fees/gas",
                action.amount_shares
            )
        }
        _ => "no supported collateral delta".into(),
    }
}

fn decimal_shares_to_ctf_units(value: Decimal) -> Result<U256> {
    if value <= Decimal::ZERO {
        bail!("closeout amount must be positive, got {value}");
    }
    let scaled = value * Decimal::from(1_000_000u64);
    let truncated = scaled.trunc();
    if truncated <= Decimal::ZERO {
        bail!("closeout amount rounds below one CTF unit: {value}");
    }
    U256::from_str_radix(&truncated.to_string(), 10)
        .with_context(|| format!("failed to convert closeout amount {value} to CTF units"))
}

fn closeout_action_ctf_units(action: &LiveCloseoutAction) -> Result<String> {
    let amount = Decimal::from_str(&action.amount_shares)
        .with_context(|| format!("invalid closeout amount '{}'", action.amount_shares))?;
    Ok(decimal_shares_to_ctf_units(amount)?.to_string())
}

fn closeout_action_blockers(
    config: &Config,
    action: &LiveCloseoutAction,
    target_contract: Option<&str>,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if !config.live_closeout_enabled {
        blockers.push("LIVE_CLOSEOUT_ENABLED=false; report is advisory only".into());
    }
    if config.live_closeout_dry_run {
        blockers.push("LIVE_CLOSEOUT_DRY_RUN=true; no transaction submission is allowed".into());
    }
    if target_contract.is_none() {
        blockers.push(format!(
            "missing target contract config for LIVE_CHAIN_ID={} negative_risk={}",
            config.live_chain_id, action.negative_risk
        ));
    }
    if closeout_action_is_combo_redeem(action) {
        if action.combo_position_id.is_none() {
            blockers.push("Combo redeem action missing combo_position_id".into());
        }
        if action.combo_outcome_index.is_none() {
            blockers
                .push("Combo redeem action missing outcome index from public Combo catalog".into());
        }
        if closeout_action_ctf_units(action).is_err() {
            blockers
                .push("Combo redeem amount cannot be converted to positive 6-decimal units".into());
        }
        if let Err(err) = combo_redeem_condition_id_abi_word(action) {
            blockers.push(format!(
                "Combo redeem Router condition id unavailable: {err}"
            ));
        }
        let wallet_type = closeout_wallet_type(config);
        if wallet_type == "DEPOSIT" && config.live_signature_type == 3 {
            blockers.extend(
                deposit_wallet_relayer_config_blockers(config)
                    .into_iter()
                    .map(|blocker| format!("deposit_wallet_relayer_config_blocked:{blocker}")),
            );
        } else if wallet_type != "EOA" {
            blockers.push(format!(
                "closeout_wallet_type={wallet_type}; wallet-specific Relayer closeout path required for automatic Combo redeem"
            ));
        }
        return blockers;
    }
    if closeout_wallet_type(config) != "EOA" {
        blockers.push(
            "non-dry-run standard closeout requires an EOA closeout wallet; proxy/safe closeout needs a wallet-specific Relayer path"
                .into(),
        );
    }
    if action.negative_risk {
        blockers.push(
            "negative-risk closeout remains review-only; standard CTF path is not valid".into(),
        );
    }
    if matches!(
        action.action.as_str(),
        "merge_full_set" | "neg_risk_merge_review"
    ) {
        match (&action.yes_size, &action.no_size) {
            (Some(yes), Some(no)) => {
                let yes = Decimal::from_str(yes).unwrap_or(Decimal::ZERO);
                let no = Decimal::from_str(no).unwrap_or(Decimal::ZERO);
                if yes != no {
                    blockers.push(
                        "YES/NO sizes differ; execution must explicitly handle residual inventory"
                            .into(),
                    );
                }
            }
            _ => blockers.push("merge action missing YES or NO size metadata".into()),
        }
    }
    if closeout_action_ctf_units(action).is_err() {
        blockers.push("closeout amount cannot be converted to positive 6-decimal CTF units".into());
    }
    if !matches!(action.action.as_str(), "merge_full_set" | "redeem_resolved") {
        blockers.push("closeout action has no standard SDK execution path".into());
    }
    blockers
}

fn next_startup_positions_offset(
    current_offset: i32,
    page_len: usize,
    page_limit: i32,
) -> Result<Option<i32>> {
    if page_limit <= 0 {
        bail!("startup position page limit must be positive, got {page_limit}");
    }
    if page_len < page_limit as usize {
        return Ok(None);
    }
    let next_offset = current_offset
        .checked_add(page_limit)
        .context("startup position pagination offset overflow")?;
    if next_offset > STARTUP_POSITIONS_MAX_OFFSET {
        bail!(
            "current positions exceed exhaustive startup reconciliation window: next_offset={} max_offset={}",
            next_offset,
            STARTUP_POSITIONS_MAX_OFFSET,
        );
    }
    Ok(Some(next_offset))
}

fn ensure_clean_account_state(
    phase: &str,
    open_order_count: usize,
    position_count: usize,
    account_address: Address,
    order_samples: &[String],
    position_samples: &[String],
) -> Result<()> {
    if open_order_count > 0 || position_count > 0 {
        bail!(
            "live {phase} requires a clean account for {}; found {} open order(s) {:?} and {} current position(s) {:?}; reconcile/cancel/close them before enabling live",
            account_address,
            open_order_count,
            order_samples,
            position_count,
            position_samples,
        );
    }
    Ok(())
}

fn live_execution_id(opp: &ArbitrageOpportunity) -> String {
    format!(
        "{}:{}:{}",
        Utc::now().timestamp_millis(),
        opp.event_id,
        opp.arb_type
    )
}

fn route_quote_snapshot(
    execution_id: &str,
    legs: &[LiveOrderLeg],
) -> Option<LiveRouteQuoteSnapshot> {
    if legs.is_empty() {
        return None;
    }

    let token_ids = legs
        .iter()
        .map(|leg| leg.token_id.clone())
        .collect::<Vec<_>>();
    let venue_timestamps = legs
        .iter()
        .filter_map(|leg| leg.venue_timestamp_ms)
        .collect::<Vec<_>>();
    let venue_timestamp_min_ms = venue_timestamps.iter().min().copied();
    let venue_timestamp_max_ms = venue_timestamps.iter().max().copied();
    let venue_timestamp_skew_ms = venue_timestamp_min_ms
        .zip(venue_timestamp_max_ms)
        .map(|(min, max)| max.saturating_sub(min));
    let max_venue_age_ms = legs.iter().filter_map(|leg| leg.venue_age_ms).max();
    let missing_book_hashes = legs.iter().filter(|leg| leg.book_hash.is_none()).count();
    let missing_venue_timestamps = legs
        .iter()
        .filter(|leg| leg.venue_timestamp_ms.is_none())
        .count();

    Some(LiveRouteQuoteSnapshot {
        refresh_id: route_quote_refresh_id(execution_id, legs),
        token_ids,
        venue_timestamp_min_ms,
        venue_timestamp_max_ms,
        venue_timestamp_skew_ms,
        max_venue_age_ms,
        missing_book_hashes,
        missing_venue_timestamps,
        legs: legs
            .iter()
            .map(|leg| LiveRouteQuoteSnapshotLeg {
                token_id: leg.token_id.clone(),
                book_hash: leg.book_hash.clone(),
                venue_timestamp_ms: leg.venue_timestamp_ms,
                venue_age_ms: leg.venue_age_ms,
            })
            .collect(),
    })
}

fn ensure_final_route_quote_coherent(legs: &[LiveOrderLeg], config: &Config) -> Result<()> {
    let snapshot = route_quote_snapshot("pre-submit", legs)
        .context("live execution aborted: final route quote snapshot unavailable")?;
    if snapshot.missing_book_hashes > 0 {
        bail!(
            "live execution aborted: final route quote coherence missing_book_hashes={}",
            snapshot.missing_book_hashes
        );
    }
    if snapshot.missing_venue_timestamps > 0 {
        bail!(
            "live execution aborted: final route quote coherence missing_venue_timestamps={}",
            snapshot.missing_venue_timestamps
        );
    }
    let missing_venue_ages = legs.iter().filter(|leg| leg.venue_age_ms.is_none()).count();
    if missing_venue_ages > 0 {
        bail!(
            "live execution aborted: final route quote coherence missing_venue_ages={missing_venue_ages}"
        );
    }
    let max_ms = config.live_max_refresh_to_submit_ms.max(1);
    if let Some(age_ms) = snapshot.max_venue_age_ms {
        if age_ms > max_ms as i64 {
            bail!(
                "live execution aborted: final route quote coherence max_venue_age_ms={age_ms} > LIVE_MAX_REFRESH_TO_SUBMIT_MS={max_ms}ms"
            );
        }
    }
    if let Some(skew_ms) = snapshot.venue_timestamp_skew_ms {
        if skew_ms > max_ms {
            bail!(
                "live execution aborted: final route quote coherence venue_timestamp_skew_ms={skew_ms} > LIVE_MAX_REFRESH_TO_SUBMIT_MS={max_ms}ms"
            );
        }
    }
    Ok(())
}

fn route_quote_refresh_id(execution_id: &str, legs: &[LiveOrderLeg]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for part in std::iter::once(execution_id).chain(legs.iter().flat_map(|leg| {
        [
            leg.token_id.as_str(),
            leg.book_hash.as_deref().unwrap_or(""),
        ]
    })) {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= u64::from(b'|');
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for leg in legs {
        for byte in leg
            .venue_timestamp_ms
            .map(|value| value.to_string())
            .unwrap_or_default()
            .as_bytes()
        {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= u64::from(b'|');
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("route-final-books:{hash:016x}")
}

#[cfg(test)]
fn live_journal_record(
    execution_id: &str,
    stage: &str,
    opp: &ArbitrageOpportunity,
    legs: &[LiveOrderLeg],
    position_usd: f64,
    entry_accounting: Option<LiveEntryAccounting>,
    projected_pnl_usd: f64,
    projected_roi_pct: f64,
    basket_units: f64,
    order_ids: &[String],
    trade_ids: &[String],
    transaction_hashes: &[String],
    error: Option<String>,
) -> LiveJournalRecord {
    live_journal_record_with_expected_order_hashes(
        execution_id,
        stage,
        opp,
        legs,
        position_usd,
        entry_accounting,
        projected_pnl_usd,
        projected_roi_pct,
        basket_units,
        order_ids,
        &[],
        trade_ids,
        transaction_hashes,
        error,
    )
}

#[allow(clippy::too_many_arguments)]
fn live_journal_record_with_expected_order_hashes(
    execution_id: &str,
    stage: &str,
    opp: &ArbitrageOpportunity,
    legs: &[LiveOrderLeg],
    position_usd: f64,
    entry_accounting: Option<LiveEntryAccounting>,
    projected_pnl_usd: f64,
    projected_roi_pct: f64,
    basket_units: f64,
    order_ids: &[String],
    expected_order_hashes: &[String],
    trade_ids: &[String],
    transaction_hashes: &[String],
    error: Option<String>,
) -> LiveJournalRecord {
    LiveJournalRecord {
        timestamp: Utc::now().to_rfc3339(),
        execution_id: execution_id.to_string(),
        stage: stage.to_string(),
        event_id: opp.event_id.clone(),
        event_title: opp.event_title.clone(),
        arb_type: opp.arb_type.to_string(),
        position_usd,
        actual_fill_cost_usd: entry_accounting.map(|accounting| accounting.actual_fill_cost_usd),
        entry_fees_usd: entry_accounting.map(|accounting| accounting.entry_fees_usd),
        entry_gas_cost_usd: entry_accounting.map(|accounting| accounting.entry_gas_cost_usd),
        actual_entry_cost_usd: entry_accounting
            .map(|accounting| accounting.actual_entry_cost_usd()),
        projected_pnl_usd,
        projected_roi_pct,
        basket_units,
        order_ids: order_ids.to_vec(),
        expected_order_hashes: expected_order_hashes.to_vec(),
        trade_ids: trade_ids.to_vec(),
        transaction_hashes: transaction_hashes.to_vec(),
        error,
        route_quote_snapshot: route_quote_snapshot(execution_id, legs),
        legs: legs
            .iter()
            .map(|leg| LiveJournalLeg {
                condition_id: leg.condition_id.clone(),
                token_id: leg.token_id.clone(),
                question: leg.question.clone(),
                outcome: leg.outcome.to_string(),
                side: leg.side.to_string(),
                raw_price: leg.raw_price,
                limit_price: leg.price,
                size: leg.size,
                unit_shares: leg.unit_shares,
                tick_size: leg.tick_size,
                neg_risk: leg.neg_risk,
                fee_rate: leg.fee_rate,
                fee_exponent: leg.fee_exponent,
                venue_timestamp_ms: leg.venue_timestamp_ms,
                venue_age_ms: leg.venue_age_ms,
                book_hash: leg.book_hash.clone(),
            })
            .collect(),
    }
}

#[allow(clippy::too_many_arguments)]
fn record_live_journal_with_expected_order_hashes(
    executor: &LiveExecutor,
    execution_id: &str,
    stage: &str,
    opp: &ArbitrageOpportunity,
    legs: &[LiveOrderLeg],
    position_usd: f64,
    entry_accounting: Option<LiveEntryAccounting>,
    projected_pnl_usd: f64,
    projected_roi_pct: f64,
    basket_units: f64,
    order_ids: &[String],
    expected_order_hashes: &[String],
    trade_ids: &[String],
    transaction_hashes: &[String],
    error: Option<String>,
) -> Result<()> {
    let record = live_journal_record_with_expected_order_hashes(
        execution_id,
        stage,
        opp,
        legs,
        position_usd,
        entry_accounting,
        projected_pnl_usd,
        projected_roi_pct,
        basket_units,
        order_ids,
        expected_order_hashes,
        trade_ids,
        transaction_hashes,
        error,
    );
    executor.journal.record(&record)
}

#[allow(clippy::too_many_arguments)]
fn warn_live_journal_failure_with_expected_order_hashes(
    executor: &LiveExecutor,
    execution_id: &str,
    stage: &str,
    opp: &ArbitrageOpportunity,
    legs: &[LiveOrderLeg],
    position_usd: f64,
    entry_accounting: Option<LiveEntryAccounting>,
    projected_pnl_usd: f64,
    projected_roi_pct: f64,
    basket_units: f64,
    order_ids: &[String],
    expected_order_hashes: &[String],
    trade_ids: &[String],
    transaction_hashes: &[String],
    error: Option<String>,
) {
    if let Err(err) = record_live_journal_with_expected_order_hashes(
        executor,
        execution_id,
        stage,
        opp,
        legs,
        position_usd,
        entry_accounting,
        projected_pnl_usd,
        projected_roi_pct,
        basket_units,
        order_ids,
        expected_order_hashes,
        trade_ids,
        transaction_hashes,
        error,
    ) {
        warn!("failed to append live execution journal record: {err}");
    }
}

fn expected_order_hashes_for_signed_orders(
    config: &Config,
    legs: &[LiveOrderLeg],
    signed_orders: &[SignedOrder],
) -> Result<Vec<String>> {
    signed_orders
        .iter()
        .enumerate()
        .map(|(idx, signed)| {
            let neg_risk = legs.get(idx).and_then(|leg| leg.neg_risk).unwrap_or(false);
            expected_order_hash_for_signed_order(config, signed, neg_risk)
                .with_context(|| format!("computing expected order hash for leg #{}", idx + 1))
        })
        .collect()
}

fn expected_order_hash_for_signed_order(
    config: &Config,
    signed: &SignedOrder,
    neg_risk: bool,
) -> Result<String> {
    let contracts = contract_config(config.live_chain_id, neg_risk).ok_or_else(|| {
        anyhow!(
            "missing SDK contract config for chain_id={} neg_risk={neg_risk}",
            config.live_chain_id
        )
    })?;
    let domain = match &signed.payload {
        OrderPayload::V2(_) => Eip712Domain {
            name: Some(Cow::Borrowed(CLOB_ORDER_EIP712_NAME)),
            version: Some(Cow::Borrowed(CLOB_ORDER_EIP712_VERSION_V2)),
            chain_id: Some(U256::from(config.live_chain_id)),
            verifying_contract: Some(contracts.exchange_v2.ok_or_else(|| {
                anyhow!(
                    "missing exchange_v2 contract config for chain_id={} neg_risk={neg_risk}",
                    config.live_chain_id
                )
            })?),
            ..Eip712Domain::default()
        },
        OrderPayload::V1(_) => Eip712Domain {
            name: Some(Cow::Borrowed(CLOB_ORDER_EIP712_NAME)),
            version: Some(Cow::Borrowed(CLOB_ORDER_EIP712_VERSION_V1)),
            chain_id: Some(U256::from(config.live_chain_id)),
            verifying_contract: Some(contracts.exchange),
            ..Eip712Domain::default()
        },
        _ => bail!("unsupported non-exhaustive SDK order payload variant"),
    };
    let hash = match &signed.payload {
        OrderPayload::V2(payload) => payload.order.eip712_signing_hash(&domain),
        OrderPayload::V1(payload) => payload.order.eip712_signing_hash(&domain),
        _ => bail!("unsupported non-exhaustive SDK order payload variant"),
    };
    Ok(format!("{hash:#x}"))
}

fn signature_type_from_u8(value: u8) -> Result<SignatureType> {
    match value {
        0 => Ok(SignatureType::Eoa),
        1 => Ok(SignatureType::Proxy),
        2 => Ok(SignatureType::GnosisSafe),
        3 => Ok(SignatureType::Poly1271),
        _ => bail!("unsupported LIVE_SIGNATURE_TYPE={value}, expected 0|1|2|3"),
    }
}

fn decimal_to_f64(value: &Decimal) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0)
}

fn decimal_from_usd(value: f64) -> Result<Decimal> {
    if !value.is_finite() || value < 0.0 {
        bail!("USD amount must be finite and non-negative, got {value}");
    }
    Decimal::from_str(&format!("{value:.6}")).context("failed to convert USD amount to Decimal")
}

fn parse_allowance_decimal(raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw.trim())
        .with_context(|| format!("failed to parse allowance value '{}'", raw.trim()))
}

fn live_collateral_spender(config: &Config, is_neg_risk: bool) -> Result<Address> {
    contract_config(config.live_chain_id, is_neg_risk)
        .with_context(|| {
            format!(
                "missing Polymarket contract config for chain_id={} neg_risk={}",
                config.live_chain_id, is_neg_risk
            )
        })?
        .exchange_v2
        .with_context(|| {
            format!(
                "missing Polymarket CLOB V2 exchange config for chain_id={} neg_risk={}",
                config.live_chain_id, is_neg_risk
            )
        })
}

fn required_collateral_spend_by_exchange(
    config: &Config,
    legs: &[LiveOrderLeg],
) -> Result<HashMap<Address, Decimal>> {
    let mut required = HashMap::new();
    for leg in legs {
        if !matches!(leg.side, Side::Buy) {
            continue;
        }
        let is_neg_risk = matches!(leg.neg_risk, Some(true));
        let exchange = live_collateral_spender(config, is_neg_risk)?;
        let fee_usd =
            fees::total_fee_with_curve(leg.price, leg.size, leg.fee_rate, leg.fee_exponent);
        let spend = decimal_from_usd(leg.price * leg.size + fee_usd)?;
        *required.entry(exchange).or_insert(Decimal::ZERO) += spend;
    }
    Ok(required)
}

fn ensure_balance_allowance_covers(
    response: &BalanceAllowanceResponse,
    required_by_exchange: &HashMap<Address, Decimal>,
) -> Result<()> {
    let total_required = required_by_exchange
        .values()
        .copied()
        .fold(Decimal::ZERO, |acc, value| acc + value);
    if response.balance < total_required {
        bail!(
            "insufficient collateral balance for live submit: balance={} required={}",
            response.balance,
            total_required,
        );
    }

    for (exchange, required) in required_by_exchange {
        let Some(raw_allowance) = response.allowances.get(exchange) else {
            bail!("missing collateral allowance for exchange {exchange}");
        };
        let allowance = parse_allowance_decimal(raw_allowance)?;
        if allowance < *required {
            bail!(
                "insufficient collateral allowance for exchange {}: allowance={} required={}",
                exchange,
                allowance,
                required,
            );
        }
    }

    Ok(())
}

async fn ensure_live_account_not_closed_only<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
) -> Result<()>
where
    K: Kind,
{
    let status = sdk_client
        .closed_only_mode()
        .await
        .context("failed to check closed-only mode before live trading")?;
    if status.closed_only {
        bail!("live execution disabled: authenticated account is in closed-only mode");
    }
    Ok(())
}

async fn refresh_live_balance_allowance<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
) -> Result<BalanceAllowanceResponse>
where
    K: Kind,
{
    let request = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Collateral)
        .build();
    sdk_client
        .update_balance_allowance(request.clone())
        .await
        .context("failed to refresh live collateral balance/allowance")?;
    sdk_client
        .balance_allowance(request)
        .await
        .context("failed to fetch live collateral balance/allowance")
}

fn contract_readiness_check(
    config: &Config,
    neg_risk: bool,
    key: &'static str,
) -> LiveReadinessCheck {
    match contract_config(config.live_chain_id, neg_risk) {
        Some(contract) if contract.exchange_v2.is_some() => LiveReadinessCheck::ready(
            key,
            format!(
                "collateral={} conditional_tokens={} exchange_v2={}",
                contract.collateral,
                contract.conditional_tokens,
                contract.exchange_v2.expect("checked exchange_v2 presence")
            ),
        ),
        Some(_) => LiveReadinessCheck::blocked(
            key,
            format!(
                "sdk_contract_config_missing_exchange_v2 chain_id={} neg_risk={neg_risk}",
                config.live_chain_id
            ),
        ),
        None => LiveReadinessCheck::blocked(
            key,
            format!(
                "missing_sdk_contract_config chain_id={} neg_risk={neg_risk}",
                config.live_chain_id
            ),
        ),
    }
}

fn protocol_drift_readiness_check(
    report: &crate::protocol_drift::ProtocolDriftReport,
) -> LiveReadinessCheck {
    match report.status.as_str() {
        "ready" => LiveReadinessCheck::ready(
            "protocol_drift",
            format!(
                "no_protocol_drift_detected source_count={}",
                report.source_urls.len()
            ),
        ),
        "blocked" => LiveReadinessCheck::blocked(
            "protocol_drift",
            format!("protocol_drift_blockers={}", report.blockers.join("|")),
        ),
        _ => LiveReadinessCheck::unknown(
            "protocol_drift",
            format!(
                "protocol_drift_unknown checks={}",
                report
                    .checks
                    .iter()
                    .filter(|check| check.state == "unknown")
                    .map(|check| check.key.as_str())
                    .collect::<Vec<_>>()
                    .join("|")
            ),
        ),
    }
}

fn user_channel_config_readiness_check(config: &Config) -> LiveReadinessCheck {
    match user_channel::ensure_live_user_channel_configured(config) {
        Ok(()) => LiveReadinessCheck::ready(
            "user_channel_config",
            "authenticated_user_channel_configured",
        ),
        Err(err) => LiveReadinessCheck::blocked(
            "user_channel_config",
            format!("user_channel_not_configured:{err}"),
        ),
    }
}

fn user_channel_ready_readiness_check(config: &Config) -> LiveReadinessCheck {
    match user_channel::ensure_live_user_channel_ready(config) {
        Ok(()) => LiveReadinessCheck::ready(
            "user_channel_ready",
            "fresh_authenticated_user_channel_status",
        ),
        Err(err) => LiveReadinessCheck::blocked(
            "user_channel_ready",
            format!("user_channel_not_ready:{err}"),
        ),
    }
}

fn closeout_execution_readiness_check(config: &Config) -> LiveReadinessCheck {
    if !config.live_closeout_enabled {
        return LiveReadinessCheck::blocked(
            "closeout_execution",
            "LIVE_CLOSEOUT_ENABLED=false; closeout remains read-only",
        );
    }
    if config.live_closeout_dry_run {
        return LiveReadinessCheck::unknown(
            "closeout_execution",
            "LIVE_CLOSEOUT_DRY_RUN=true; execution readiness requires non-dry-run action preflight",
        );
    }
    LiveReadinessCheck::ready(
        "closeout_execution",
        "non_dry_run_closeout_enabled; per-action eth_call and user-channel tripwire are enforced before transaction submission",
    )
}

async fn native_pol_readiness_check(
    config: &Config,
    account_address: Address,
) -> LiveReadinessCheck {
    let rpc_url = config.polygon_rpc_url.trim();
    if rpc_url.is_empty() {
        return LiveReadinessCheck::blocked(
            "native_pol_balance",
            "POLYGON_RPC_URL is required before native POL balance probe",
        );
    }
    let provider = match ProviderBuilder::new().connect(rpc_url).await {
        Ok(provider) => provider,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "native_pol_balance",
                format!("native_rpc_connect_failed:{err}"),
            );
        }
    };
    let balance = match provider.get_balance(account_address).await {
        Ok(balance) => balance,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "native_pol_balance",
                format!("native_balance_probe_failed:{err}"),
            );
        }
    };
    match ensure_closeout_native_gas_balance(balance) {
        Ok(()) => LiveReadinessCheck::ready(
            "native_pol_balance",
            format!("balance_pol={:.6}", wei_to_native_f64(balance)),
        ),
        Err(err) => LiveReadinessCheck::blocked("native_pol_balance", err.to_string()),
    }
}

fn collateral_readiness_checks(
    config: &Config,
    response: &BalanceAllowanceResponse,
) -> Vec<LiveReadinessCheck> {
    let required = match decimal_from_usd(config.live_trade_position_size_usd.max(0.0)) {
        Ok(required) => required,
        Err(err) => {
            return vec![
                LiveReadinessCheck::blocked(
                    "pusd_balance",
                    format!("invalid_live_trade_position_size:{err}"),
                ),
                LiveReadinessCheck::blocked(
                    "pusd_allowance_exchange_v2_standard",
                    format!("invalid_live_trade_position_size:{err}"),
                ),
                LiveReadinessCheck::blocked(
                    "pusd_allowance_exchange_v2_neg_risk",
                    format!("invalid_live_trade_position_size:{err}"),
                ),
            ];
        }
    };
    let mut checks = Vec::new();
    if response.balance < required {
        checks.push(LiveReadinessCheck::blocked(
            "pusd_balance",
            format!("balance={} required={required}", response.balance),
        ));
    } else {
        checks.push(LiveReadinessCheck::ready(
            "pusd_balance",
            format!("balance={} required={required}", response.balance),
        ));
    }
    checks.push(allowance_readiness_check(
        config,
        response,
        false,
        "pusd_allowance_exchange_v2_standard",
        &required,
    ));
    checks.push(allowance_readiness_check(
        config,
        response,
        true,
        "pusd_allowance_exchange_v2_neg_risk",
        &required,
    ));
    checks.push(exchange_v3_allowance_readiness_check(
        config, response, &required,
    ));
    checks
}

fn ensure_readiness_checks_ready(context: &str, checks: &[LiveReadinessCheck]) -> Result<()> {
    let blockers: Vec<String> = checks
        .iter()
        .filter(|check| check.state != LiveReadinessState::Ready)
        .map(|check| format!("{}:{}", check.key, check.detail))
        .collect();
    if blockers.is_empty() {
        Ok(())
    } else {
        bail!("{context} blocked: {}", blockers.join("; "))
    }
}

fn combo_rfq_exchange_v3_spender(config: &Config) -> Result<Address> {
    let raw = config.combo_rfq_exchange_v3_address.trim();
    if raw.is_empty() {
        bail!("COMBO_RFQ_EXCHANGE_V3_ADDRESS_empty");
    }
    Address::from_str(raw).with_context(|| format!("COMBO_RFQ_EXCHANGE_V3_ADDRESS_invalid:{raw}"))
}

fn decimal_collateral_to_units(value: Decimal) -> Result<U256> {
    if value < Decimal::ZERO {
        bail!("collateral amount must be non-negative, got {value}");
    }
    let scaled = value * Decimal::from(1_000_000u64);
    let truncated = scaled.trunc();
    U256::from_str_radix(&truncated.to_string(), 10)
        .with_context(|| format!("failed to convert collateral amount {value} to raw units"))
}

fn format_collateral_units(units: U256) -> String {
    let raw = units.to_string();
    if raw.len() > 24 {
        return format!("raw_units={raw}");
    }
    format!("{:.6}", ctf_units_to_usd_f64(units))
}

async fn combo_rfq_exchange_v3_allowance_promotion_readiness_check(
    config: &Config,
) -> LiveReadinessCheck {
    let required = match decimal_from_usd(config.live_trade_position_size_usd.max(0.0)) {
        Ok(required) => required,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "exchange_v3_allowance",
                format!("invalid_live_trade_position_size:{err}"),
            )
        }
    };
    let account_address = match configured_live_account_address(config) {
        Ok(account_address) => account_address,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "exchange_v3_allowance",
                format!("account_unavailable_for_exchange_v3_allowance_probe:{err}"),
            )
        }
    };
    exchange_v3_allowance_rpc_readiness_check(config, account_address, &required).await
}

async fn exchange_v3_allowance_rpc_readiness_check(
    config: &Config,
    account_address: Address,
    required: &Decimal,
) -> LiveReadinessCheck {
    let exchange = match combo_rfq_exchange_v3_spender(config) {
        Ok(exchange) => exchange,
        Err(err) => return LiveReadinessCheck::blocked("exchange_v3_allowance", err.to_string()),
    };
    let contract = match contract_config(config.live_chain_id, false) {
        Some(contract) => contract,
        None => {
            return LiveReadinessCheck::blocked(
                "exchange_v3_allowance",
                format!(
                    "missing_sdk_contract_config chain_id={} for collateral allowance probe",
                    config.live_chain_id
                ),
            )
        }
    };
    let rpc_url = config.polygon_rpc_url.trim();
    if rpc_url.is_empty() {
        return LiveReadinessCheck::blocked(
            "exchange_v3_allowance",
            "POLYGON_RPC_URL is required before exchange_v3 allowance probe",
        );
    }
    let provider = match ProviderBuilder::new().connect(rpc_url).await {
        Ok(provider) => provider,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "exchange_v3_allowance",
                format!("exchange_v3_rpc_connect_failed:{err}"),
            )
        }
    };
    let required_units = match decimal_collateral_to_units(*required) {
        Ok(required_units) => required_units,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "exchange_v3_allowance",
                format!("invalid_required_collateral:{err}"),
            )
        }
    };
    let collateral = IERC20Balance::new(contract.collateral, provider);
    let allowance_units = match collateral.allowance(account_address, exchange).call().await {
        Ok(allowance_units) => allowance_units,
        Err(err) => {
            return LiveReadinessCheck::blocked(
                "exchange_v3_allowance",
                format!("exchange_v3_allowance_probe_failed:{err}"),
            )
        }
    };
    if allowance_units < required_units {
        LiveReadinessCheck::blocked(
            "exchange_v3_allowance",
            format!(
                "allowance={} required={} exchange={} source=polygon_rpc",
                format_collateral_units(allowance_units),
                required,
                exchange
            ),
        )
    } else {
        LiveReadinessCheck::ready(
            "exchange_v3_allowance",
            format!(
                "allowance={} required={} exchange={} source=polygon_rpc",
                format_collateral_units(allowance_units),
                required,
                exchange
            ),
        )
    }
}

fn exchange_v3_allowance_readiness_check(
    config: &Config,
    response: &BalanceAllowanceResponse,
    required: &Decimal,
) -> LiveReadinessCheck {
    let exchange = match combo_rfq_exchange_v3_spender(config) {
        Ok(exchange) => exchange,
        Err(err) => return LiveReadinessCheck::blocked("exchange_v3_allowance", err.to_string()),
    };
    let Some(raw_allowance) = response.allowances.get(&exchange) else {
        return LiveReadinessCheck::blocked(
            "exchange_v3_allowance",
            format!("missing_collateral_allowance exchange={exchange} required={required}"),
        );
    };
    let allowance = match parse_allowance_decimal(raw_allowance) {
        Ok(allowance) => allowance,
        Err(err) => return LiveReadinessCheck::blocked("exchange_v3_allowance", err.to_string()),
    };
    if allowance < *required {
        LiveReadinessCheck::blocked(
            "exchange_v3_allowance",
            format!("allowance={allowance} required={required} exchange={exchange}"),
        )
    } else {
        LiveReadinessCheck::ready(
            "exchange_v3_allowance",
            format!("allowance={allowance} required={required} exchange={exchange}"),
        )
    }
}

fn allowance_readiness_check(
    config: &Config,
    response: &BalanceAllowanceResponse,
    neg_risk: bool,
    key: &'static str,
    required: &Decimal,
) -> LiveReadinessCheck {
    let exchange = match live_collateral_spender(config, neg_risk) {
        Ok(exchange) => exchange,
        Err(err) => return LiveReadinessCheck::blocked(key, err.to_string()),
    };
    let Some(raw_allowance) = response.allowances.get(&exchange) else {
        return LiveReadinessCheck::blocked(
            key,
            format!("missing_collateral_allowance exchange={exchange} required={required}"),
        );
    };
    let allowance = match parse_allowance_decimal(raw_allowance) {
        Ok(allowance) => allowance,
        Err(err) => return LiveReadinessCheck::blocked(key, err.to_string()),
    };
    if allowance < *required {
        LiveReadinessCheck::blocked(
            key,
            format!("allowance={allowance} required={required} exchange={exchange}"),
        )
    } else {
        LiveReadinessCheck::ready(
            key,
            format!("allowance={allowance} required={required} exchange={exchange}"),
        )
    }
}

async fn ensure_live_account_funding<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    config: &Config,
    legs: &[LiveOrderLeg],
) -> Result<()>
where
    K: Kind,
{
    ensure_live_account_not_closed_only(sdk_client).await?;
    let required_by_exchange = required_collateral_spend_by_exchange(config, legs)?;
    if required_by_exchange.is_empty() {
        return Ok(());
    }
    let response = refresh_live_balance_allowance(sdk_client).await?;
    ensure_balance_allowance_covers(&response, &required_by_exchange)
}

async fn ensure_combo_rfq_live_account_funding<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    config: &Config,
    account_address: Address,
) -> Result<()>
where
    K: Kind,
{
    ensure_live_account_not_closed_only(sdk_client).await?;
    let response = refresh_live_balance_allowance(sdk_client).await?;
    let mut checks = collateral_readiness_checks(config, &response);
    let sdk_v3_ready = checks.iter().any(|check| {
        check.key == "exchange_v3_allowance" && check.state == LiveReadinessState::Ready
    });
    if !sdk_v3_ready {
        checks.retain(|check| check.key != "exchange_v3_allowance");
        let required = decimal_from_usd(config.live_trade_position_size_usd.max(0.0))?;
        checks.push(
            exchange_v3_allowance_rpc_readiness_check(config, account_address, &required).await,
        );
    }
    ensure_readiness_checks_ready("Combo/RFQ pre-submit collateral", &checks)
}

async fn ensure_combo_rfq_pre_submit_account_guard<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    config: &Config,
    http: &Client,
    account_address: Address,
) -> Result<()>
where
    K: Kind,
{
    ensure_combo_rfq_live_account_funding(sdk_client, config, account_address).await?;
    verify_clean_pre_submit_account(sdk_client, account_address).await?;
    ensure_combo_rfq_account_exposure_clean(config, http).await?;
    ensure_live_pre_submit_heartbeat(sdk_client, config).await
}

fn order_type_from_config(value: &str) -> Result<OrderType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "fok" => Ok(OrderType::FOK),
        "gtc" => Ok(OrderType::GTC),
        "fak" => Ok(OrderType::FAK),
        "gtd" => bail!("LIVE_ORDER_TYPE=gtd is not supported by this executor because no per-order expiry is configured; use gtc|fok|fak"),
        other => bail!("unsupported LIVE_ORDER_TYPE='{other}', expected gtc|fok|fak"),
    }
}

fn live_basket_order_type_from_config(value: &str) -> Result<OrderType> {
    let order_type = order_type_from_config(value)?;
    if !matches!(order_type, OrderType::FOK) {
        bail!(
            "LIVE_ORDER_TYPE must be fok for live arbitrage baskets; gtc can rest and fak can partially fill"
        );
    }
    Ok(order_type)
}

fn tick_size_from_f64(value: f64) -> Option<TickSize> {
    let scaled = (value * 10_000.0).round() as i64;
    match scaled {
        1000 => Some(TickSize::Tenth),
        100 => Some(TickSize::Hundredth),
        10 => Some(TickSize::Thousandth),
        1 => Some(TickSize::TenThousandth),
        _ => None,
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

fn live_order_size_step_shares(config: &Config) -> f64 {
    config
        .order_size_step_shares
        .max(LIVE_SDK_LOT_SIZE_STEP_SHARES)
}

fn format_live_order_size(size: f64) -> String {
    let rounded = round_down_to_step(size, LIVE_SDK_LOT_SIZE_STEP_SHARES);
    format!("{rounded:.precision$}", precision = LIVE_SDK_LOT_SIZE_SCALE)
}

fn apply_slippage(price: f64, slippage_bps: u32, tick_size: f64) -> f64 {
    let adjusted = price * (1.0 + slippage_bps as f64 / 10_000.0);
    clob_client::round_up_to_tick(adjusted.min(0.99), tick_size)
}

fn parse_clob_server_time_secs(raw: &str) -> Result<i128> {
    let trimmed = raw.trim().trim_matches('"');
    let server_secs = trimmed.parse::<i128>().with_context(|| {
        format!(
            "CLOB server time response was not a Unix timestamp in seconds: {}",
            trimmed.chars().take(64).collect::<String>()
        )
    })?;
    if server_secs <= 0 {
        bail!("CLOB server time response was non-positive: {server_secs}");
    }
    Ok(server_secs)
}

async fn sample_server_time(http: &Client, config: &Config) -> Result<ServerTimeSample> {
    let url = format!("{}/time", config.clob_api_url.trim_end_matches('/'));
    let timeout = Duration::from_millis(
        config
            .api_timeout_secs
            .saturating_mul(1_000)
            .max(1)
            .min(config.live_max_refresh_to_submit_ms.max(1)),
    );
    let response = http
        .get(&url)
        .timeout(timeout)
        .send()
        .await
        .context("CLOB server time request failed")?;
    let status = response.status();
    let final_url = response.url().as_str().to_string();
    if final_url != url {
        bail!(
            "CLOB server time endpoint redirected from {} to {}; set CLOB_API_URL to the canonical production host",
            url,
            final_url
        );
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "CLOB server time failed with status {} body={}",
            status,
            body.chars().take(256).collect::<String>()
        );
    }

    let raw = response
        .text()
        .await
        .context("CLOB server time response body failed")?;
    Ok(ServerTimeSample {
        server_secs: parse_clob_server_time_secs(&raw)?,
        local_received_ms: local_unix_ms()?,
    })
}

fn local_unix_ms() -> Result<i128> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?;
    Ok(elapsed.as_millis() as i128)
}

fn ensure_signal_fresh(opp: &ArbitrageOpportunity, config: &Config) -> Result<()> {
    let age_secs = (Utc::now() - opp.detected_at).num_seconds();
    if !config.signal_is_fresh(age_secs) {
        bail!(
            "signal too old for live execution: age={}s > MAX_SIGNAL_AGE_SECONDS={}s",
            age_secs,
            config.max_signal_age_secs
        );
    }
    Ok(())
}

fn ensure_submit_fresh(final_refresh_started_at: Instant, config: &Config) -> Result<()> {
    let age = final_refresh_started_at.elapsed();
    let max_age = Duration::from_millis(config.live_max_refresh_to_submit_ms.max(1));
    if age > max_age {
        bail!(
            "live execution aborted before submit: final quote refresh age={}ms > LIVE_MAX_REFRESH_TO_SUBMIT_MS={}ms",
            age.as_millis(),
            config.live_max_refresh_to_submit_ms,
        );
    }
    Ok(())
}

async fn ensure_ws_causal_watermark_not_newer(
    price_cache: Option<&PriceCache>,
    legs: &[LiveOrderLeg],
    final_refresh_started_at: Instant,
) -> Result<()> {
    let Some(price_cache) = price_cache else {
        bail!("live execution aborted by causal watermark: price_cache_unavailable");
    };
    let cache = price_cache.read().await;
    for leg in legs {
        let Some(snapshot) = cache.get(leg.token_id.as_str()) else {
            bail!(
                "live execution aborted by causal watermark: missing_ws_snapshot:{}",
                leg.token_id
            );
        };
        let Some(ws_ts) = snapshot.venue_timestamp_ms else {
            bail!(
                "live execution aborted by causal watermark: missing_ws_venue_timestamp:{}",
                leg.token_id
            );
        };
        let Some(rest_ts) = leg.venue_timestamp_ms else {
            bail!(
                "live execution aborted by causal watermark: missing_rest_depth_timestamp:{}",
                leg.token_id
            );
        };
        if ws_ts > rest_ts {
            bail!(
                "live execution aborted by causal watermark: token={} ws_venue_timestamp_ms={} > rest_depth_timestamp_ms={}",
                leg.token_id,
                ws_ts,
                rest_ts
            );
        }
        if snapshot.book_hash.is_some()
            && leg.book_hash.is_some()
            && snapshot.book_hash != leg.book_hash
        {
            let same_timestamp = matches!(
                (snapshot.venue_timestamp_ms, leg.venue_timestamp_ms),
                (Some(snapshot_ts), Some(leg_ts)) if snapshot_ts == leg_ts
            );
            let changed_after_refresh = snapshot
                .last_updated
                .checked_duration_since(final_refresh_started_at)
                .is_some();
            if same_timestamp {
                bail!(
                    "live execution aborted by causal watermark: token={} same timestamp but ws_book_hash={:?} != rest_depth_book_hash={:?}",
                    leg.token_id,
                    snapshot.book_hash,
                    leg.book_hash
                );
            }
            if changed_after_refresh {
                bail!(
                    "live execution aborted by causal watermark: token={} ws_book_hash={:?} changed after final REST depth hash={:?}",
                    leg.token_id,
                    snapshot.book_hash,
                    leg.book_hash
                );
            }
        }
        for trade in &snapshot.recent_trades {
            let trade_after_rest_timestamp =
                match (trade.venue_timestamp_ms, leg.venue_timestamp_ms) {
                    (Some(trade_ts), Some(rest_ts)) => trade_ts > rest_ts,
                    _ => false,
                };
            let trade_after_refresh_started = trade
                .observed_at
                .checked_duration_since(final_refresh_started_at)
                .is_some();
            if trade_after_rest_timestamp || trade_after_refresh_started {
                bail!(
                    "live execution aborted by causal watermark: token={} trade_print side={} observed_after_final_refresh={} venue_timestamp_ms={:?} rest_depth_timestamp_ms={:?}",
                    leg.token_id,
                    trade.side,
                    trade_after_refresh_started,
                    trade.venue_timestamp_ms,
                    leg.venue_timestamp_ms
                );
            }
        }
    }
    Ok(())
}

fn ensure_final_depth_fresh(
    snapshot: &clob_client::DepthSnapshot,
    server_clock: &ServerClock,
    config: &Config,
) -> Result<i64> {
    let venue_timestamp_ms = snapshot.venue_timestamp_ms.with_context(|| {
        format!(
            "CLOB final depth /books missing venue timestamp for token {}",
            snapshot.token_id
        )
    })? as i128;
    let age_ms = server_clock.now_ms()? - venue_timestamp_ms;
    let uncertainty_ms = server_clock.uncertainty_ms.max(0);
    let max_age_ms = config.live_max_refresh_to_submit_ms.max(1) as i128;
    if uncertainty_ms * 2 >= max_age_ms {
        bail!(
            "CLOB final depth /books freshness unavailable for token {}: clock_uncertainty={}ms consumes LIVE_MAX_REFRESH_TO_SUBMIT_MS={}ms",
            snapshot.token_id,
            uncertainty_ms,
            config.live_max_refresh_to_submit_ms,
        );
    }
    let stale_age_ms = age_ms + uncertainty_ms;
    let future_age_ms = age_ms - uncertainty_ms;
    if stale_age_ms > max_age_ms {
        bail!(
            "CLOB final depth /books stale for token {}: conservative_age={}ms estimated_age={}ms clock_uncertainty={}ms > LIVE_MAX_REFRESH_TO_SUBMIT_MS={}ms",
            snapshot.token_id,
            stale_age_ms,
            age_ms,
            uncertainty_ms,
            config.live_max_refresh_to_submit_ms,
        );
    }
    if future_age_ms < -max_age_ms {
        bail!(
            "CLOB final depth /books future timestamp for token {}: conservative_age={}ms estimated_age={}ms clock_uncertainty={}ms < -LIVE_MAX_REFRESH_TO_SUBMIT_MS={}ms",
            snapshot.token_id,
            future_age_ms,
            age_ms,
            uncertainty_ms,
            config.live_max_refresh_to_submit_ms,
        );
    }
    Ok(age_ms.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
}

fn ensure_final_depth_rules_match(
    depth: &clob_client::DepthSnapshot,
    market: &Market,
    leg_question: &str,
) -> Result<()> {
    let final_tick_size = depth
        .tick_size
        .with_context(|| format!("CLOB final depth /books missing tick size for {leg_question}"))?;
    let planned_tick_size = market.tick_size();
    if (final_tick_size - planned_tick_size).abs() > 1e-12 {
        bail!(
            "CLOB final depth /books tick size drift for '{}': final={:.6} planned={:.6}",
            leg_question,
            final_tick_size,
            planned_tick_size,
        );
    }

    let final_min_order = depth.min_order_size.with_context(|| {
        format!("CLOB final depth /books missing minimum order size for {leg_question}")
    })?;
    let planned_min_order = market.min_order_size_shares();
    if (final_min_order - planned_min_order).abs() > 1e-9 {
        bail!(
            "CLOB final depth /books minimum order size drift for '{}': final={:.6} planned={:.6}",
            leg_question,
            final_min_order,
            planned_min_order,
        );
    }

    let final_neg_risk = depth.neg_risk.with_context(|| {
        format!("CLOB final depth /books missing neg-risk flag for {leg_question}")
    })?;
    if market.clob_neg_risk != Some(final_neg_risk) {
        bail!(
            "CLOB final depth /books neg-risk drift for '{}': final={} planned={:?}",
            leg_question,
            final_neg_risk,
            market.clob_neg_risk,
        );
    }

    Ok(())
}

fn reject_external_token_opportunity(opp: &ArbitrageOpportunity) -> Result<()> {
    for leg in &opp.execution_plan {
        let token_id = leg.token_id.trim();
        if is_external_token_id(token_id) {
            bail!(
                "live execution refuses external token id '{}' for event {} ({})",
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
                    "live execution refuses external token id '{}' for event {} ({})",
                    token_id,
                    opp.event_id,
                    opp.arb_type
                );
            }
        }
    }
    Ok(())
}

fn live_total_gas_cost_usd(entry_gas_cost_usd: f64, estimated_closeout_gas_usd: f64) -> f64 {
    entry_gas_cost_usd.max(0.0) + estimated_closeout_gas_usd.max(0.0)
}

fn live_gas_oracle() -> GasOracle {
    #[cfg(not(test))]
    {
        LIVE_GAS_ORACLE.get_or_init(GasOracle::new).clone()
    }
    #[cfg(test)]
    {
        GasOracle::new()
    }
}

async fn required_live_trade_gas_cost_usd(
    http: &Client,
    config: &Config,
    num_legs: usize,
    context: &str,
) -> Result<f64> {
    if config.assume_gasless_for_proxy_signature_types && config.live_signature_type != 0 {
        return Ok(0.0);
    }

    let estimate = live_gas_oracle()
        .trade_cost_estimate_usd(http, num_legs, config.gas_fallback_usd)
        .await;
    if !estimate.source.is_fresh_oracle_backed() {
        bail!(
            "live {context} gas estimate requires a fresh Polygon gas/POL oracle; source={} legs={} fallback_usd_per_leg={:.4}",
            estimate.source.as_str(),
            estimate.legs,
            config.gas_fallback_usd,
        );
    }
    Ok(config.effective_trade_gas_cost_usd(estimate.cost_usd))
}

async fn estimated_live_closeout_gas_cost_usd(http: &Client, config: &Config) -> Result<f64> {
    required_live_trade_gas_cost_usd(http, config, 1, "closeout").await
}

async fn combo_rfq_live_gas_costs_usd(
    http: &Client,
    config: &Config,
    num_legs: usize,
) -> Result<(f64, f64)> {
    tokio::try_join!(
        required_live_trade_gas_cost_usd(http, config, num_legs, "Combo/RFQ entry"),
        estimated_live_closeout_gas_cost_usd(http, config)
    )
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

fn plan_leg_has_full_quote(market: &Market, outcome: OutcomeSide) -> bool {
    match outcome {
        OutcomeSide::Yes => market.has_full_yes_quote(),
        OutcomeSide::No => market.has_full_no_quote(),
    }
}

fn required_quotes_present(markets: &[Market], plan: &[OpportunityLeg]) -> bool {
    plan.iter().all(|leg| {
        plan_market(markets, leg)
            .map(|market| !market.closed && plan_leg_has_full_quote(market, leg.outcome))
            .unwrap_or(false)
    })
}

fn planned_condition_tokens(
    markets: &[Market],
    opp: &ArbitrageOpportunity,
) -> Result<Vec<(String, String)>> {
    let mut planned = Vec::with_capacity(opp.execution_plan.len());
    for leg in &opp.execution_plan {
        let market = plan_market(markets, leg)?;
        let condition_id = market.condition_id.trim();
        if condition_id.is_empty() {
            bail!(
                "live execution requires a condition id for planned market '{}'",
                market.question
            );
        }
        let token_id = if leg.token_id.trim().is_empty() {
            market.token_id_for_outcome(leg.outcome).trim()
        } else {
            leg.token_id.trim()
        };
        if token_id.is_empty() {
            bail!(
                "live execution requires a token id for planned market '{}'",
                market.question
            );
        }
        planned.push((condition_id.to_string(), token_id.to_string()));
    }
    Ok(planned)
}

fn ensure_live_basket_atomicity_supported(opp: &ArbitrageOpportunity) -> Result<()> {
    if !matches!(opp.arb_type, ArbType::Yes | ArbType::No) {
        bail!(
            "live execution refuses unsupported arbitrage route {} without an explicit live executor",
            opp.arb_type
        );
    }
    if !is_supported_yes_no_full_family_plan(opp) {
        bail!(
            "live execution refuses malformed {} route; expected a complete YES/NO full-family plan with matching side tokens",
            opp.arb_type
        );
    }
    if opp.execution_plan.len() > 1 {
        bail!(
            "live execution refuses {}-leg arbitrage basket without atomic basket fill or unwind support",
            opp.execution_plan.len()
        );
    }
    Ok(())
}

fn ensure_single_live_clob_order_submit(signed_order_count: usize) -> Result<()> {
    if signed_order_count != 1 {
        bail!(
            "live CLOB submit refuses {} signed orders because POST /orders is not an atomic basket fill",
            signed_order_count
        );
    }
    Ok(())
}

fn ensure_yes_no_neg_risk_metadata(opp: &ArbitrageOpportunity, markets: &[Market]) -> Result<()> {
    if !matches!(opp.arb_type, ArbType::Yes | ArbType::No) {
        return Ok(());
    }

    for leg in &opp.execution_plan {
        let market = plan_market(markets, leg)?;
        if market.clob_neg_risk != Some(true) {
            bail!(
                "live execution refuses {} basket because refreshed market '{}' lacks CLOB neg-risk confirmation",
                opp.arb_type,
                market.question,
            );
        }
    }
    Ok(())
}

fn clear_cached_neg_risk_metadata(markets: &mut [Market]) {
    for market in markets {
        market.clob_neg_risk = None;
    }
}

fn basket_unit_step(plan: &[OpportunityLeg], config: &Config) -> f64 {
    let share_step = live_order_size_step_shares(config);
    plan.iter()
        .filter_map(|leg| {
            if leg.unit_shares > f64::EPSILON {
                Some(share_step / leg.unit_shares)
            } else {
                None
            }
        })
        .fold(share_step, f64::max)
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
            let limit_price = apply_slippage(raw_ask, config.live_slippage_bps, market.tick_size());
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
        total_fees_usd +=
            fees::total_fee_from_clob_metadata(snapshot.limit_price, shares, &snapshot.market)
                .with_context(|| {
                    format!(
                        "live projected fee missing authoritative CLOB fd.r/fd.e for '{}'",
                        leg.question
                    )
                })?;
    }

    let projected_pnl_usd = basket_units * opp.guaranteed_revenue
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

fn projected_trade_metrics_for_legs(
    opp: &ArbitrageOpportunity,
    markets: &[Market],
    legs: &[LiveOrderLeg],
    basket_units: f64,
    _config: &Config,
    inferred_gas_cost_usd: f64,
) -> Result<(f64, f64, f64, f64)> {
    if basket_units <= f64::EPSILON {
        bail!("basket units must be positive");
    }

    let mut total_cost_usd = 0.0;
    let mut total_fees_usd = 0.0;
    for leg in legs {
        markets.get(leg.market_index).ok_or_else(|| {
            anyhow!(
                "live leg references missing market index {}",
                leg.market_index
            )
        })?;
        total_cost_usd += leg.price * leg.size;
        total_fees_usd += live_leg_fee_usd(leg);
    }

    let projected_pnl_usd = basket_units * opp.guaranteed_revenue
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

fn live_leg_fee_usd(leg: &LiveOrderLeg) -> f64 {
    live_fill_fee_usd(leg, leg.price, leg.size)
}

fn live_fill_fee_usd(leg: &LiveOrderLeg, price: f64, size: f64) -> f64 {
    fees::total_fee_with_curve(price, size, leg.fee_rate, leg.fee_exponent)
}

fn add_authenticated_fill_accounting(
    accounting: &mut LiveEntryAccounting,
    leg: &LiveOrderLeg,
    price: f64,
    size: f64,
) {
    accounting.actual_fill_cost_usd += price * size;
    accounting.entry_fees_usd += live_fill_fee_usd(leg, price, size);
}

async fn refresh_live_leg_fee_schedules(
    http: &Client,
    config: &Config,
    legs: &mut [LiveOrderLeg],
) -> Result<()> {
    let condition_ids: Vec<String> = legs.iter().map(|leg| leg.condition_id.clone()).collect();
    let fee_schedules = clob_client::get_live_fee_schedules(http, config, &condition_ids)
        .await
        .context("live execution requires fresh CLOB V2 fd.r/fd.e for every planned market")?;
    for leg in legs {
        let schedule = fee_schedules.get(&leg.condition_id).with_context(|| {
            format!(
                "CLOB V2 fee metadata missing planned condition {}",
                leg.condition_id
            )
        })?;
        leg.fee_rate = schedule.rate;
        leg.fee_exponent = schedule.exponent;
    }
    Ok(())
}

fn signed_order_price_size(leg: &LiveOrderLeg) -> Result<(Decimal, Decimal, f64, f64)> {
    let price_text = clob_client::format_price_for_tick(leg.price, leg.tick_size);
    let price = Decimal::from_str(&price_text).with_context(|| {
        format!(
            "invalid signed limit price for '{}': {price_text}",
            leg.question
        )
    })?;
    let size_text = format_live_order_size(leg.size);
    let size = Decimal::from_str(&size_text).with_context(|| {
        format!(
            "invalid signed order size for '{}': {size_text}",
            leg.question
        )
    })?;
    let price_f64 = decimal_to_f64(&price);
    let size_f64 = decimal_to_f64(&size);
    if !(0.0..=1.0).contains(&price_f64) || price_f64 <= 0.0 {
        bail!(
            "live pre-sign fixed-point simulation rejected '{}': signed_price={price} outside (0,1]",
            leg.question
        );
    }
    if size_f64 <= f64::EPSILON {
        bail!(
            "live pre-sign fixed-point simulation rejected '{}': signed_size={size} is non-positive",
            leg.question
        );
    }
    if size_f64 + f64::EPSILON < leg.min_order_shares {
        bail!(
            "live pre-sign fixed-point simulation rejected '{}': signed_size={size_f64:.6} < min_order_shares={:.6}",
            leg.question,
            leg.min_order_shares
        );
    }
    Ok((price, size, price_f64, size_f64))
}

fn normalize_legs_to_signed_order_values(
    legs: &mut [LiveOrderLeg],
    config: &Config,
) -> Result<f64> {
    if legs.is_empty() {
        bail!("live pre-sign fixed-point simulation requires at least one leg");
    }

    let mut signed_units = Vec::with_capacity(legs.len());
    for leg in legs.iter_mut() {
        if leg.unit_shares <= f64::EPSILON {
            bail!(
                "live pre-sign fixed-point simulation rejected '{}': unit_shares={:.6}",
                leg.question,
                leg.unit_shares
            );
        }
        let (_signed_price, _signed_size, price_f64, size_f64) = signed_order_price_size(leg)?;
        if (price_f64 - leg.price).abs() > f64::EPSILON
            || (size_f64 - leg.size).abs() > f64::EPSILON
        {
            debug!(
                "Live pre-sign fixed-point normalization: '{}' price {:.8}->{:.8} size {:.8}->{:.8}",
                leg.question, leg.price, price_f64, leg.size, size_f64
            );
        }
        leg.price = price_f64;
        leg.size = size_f64;
        signed_units.push(size_f64 / leg.unit_shares);
    }

    let min_units = signed_units.iter().copied().fold(f64::INFINITY, f64::min);
    let max_units = signed_units
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    if !min_units.is_finite() || min_units <= f64::EPSILON {
        bail!("live pre-sign fixed-point simulation reduced basket units to zero");
    }

    let max_unit_skew = live_order_size_step_shares(config).max(LIVE_SDK_LOT_SIZE_STEP_SHARES);
    if legs.len() > 1 && max_units - min_units > max_unit_skew + f64::EPSILON {
        bail!(
            "live pre-sign fixed-point simulation rejected basket unit skew: min_units={min_units:.6} max_units={max_units:.6} max_skew={max_unit_skew:.6}"
        );
    }

    Ok(min_units)
}

fn live_edge_haircut_usd(position_usd: f64, config: &Config) -> f64 {
    let fixed = config.live_edge_haircut_usd.max(0.0);
    let proportional = position_usd.max(0.0) * config.live_edge_haircut_bps as f64 / 10_000.0;
    fixed + proportional
}

fn ensure_live_edge_survives_haircut(
    position_usd: f64,
    projected_pnl_usd: f64,
    config: &Config,
) -> Result<(f64, f64, f64)> {
    if position_usd <= f64::EPSILON {
        bail!("live execution cannot apply edge haircut to non-positive position size");
    }
    let haircut_usd = live_edge_haircut_usd(position_usd, config);
    let adjusted_pnl_usd = projected_pnl_usd - haircut_usd;
    let adjusted_roi_pct = adjusted_pnl_usd / position_usd * 100.0;

    if adjusted_pnl_usd < config.min_net_profit_usd || adjusted_roi_pct < config.min_roi_pct {
        bail!(
            "live execution aborted after final haircut: projected_cost=${position_usd:.4} projected_pnl=${projected_pnl_usd:.4} haircut=${haircut_usd:.4} adjusted_pnl=${adjusted_pnl_usd:.4} adjusted_roi={adjusted_roi_pct:.2}% min_net=${:.4} min_roi={:.2}%",
            config.min_net_profit_usd,
            config.min_roi_pct,
        );
    }

    Ok((haircut_usd, adjusted_pnl_usd, adjusted_roi_pct))
}

async fn refresh_and_validate(
    http: &Client,
    config: &Config,
    opp: &ArbitrageOpportunity,
) -> Result<(Vec<Market>, Vec<PlanLegSnapshot>)> {
    reject_external_token_opportunity(opp)?;
    ensure_signal_fresh(opp, config)?;
    if opp.execution_plan.is_empty() {
        bail!("live execution requires a non-empty execution plan");
    }
    if config.execute_only_full_clob_prices && !opp.prices_from_clob {
        bail!("live execution gate: refusing opportunity not fully priced from live CLOB quotes");
    }

    let mut markets = opp.markets.clone();
    clear_cached_neg_risk_metadata(&mut markets);
    let _ = clob_client::enrich_event_markets(http, config, &mut markets).await;
    let planned_condition_tokens = planned_condition_tokens(&markets, opp)?;
    clob_client::verify_live_orderable_markets(http, config, &planned_condition_tokens)
        .await
        .context(
            "live execution requires fresh orderable CLOB market metadata for every planned market",
        )?;
    if !required_quotes_present(&markets, &opp.execution_plan) {
        bail!("live execution requires executable quotes with visible size for every leg");
    }
    if markets
        .iter()
        .any(|market| !fees::market_fee_curve_supported(market))
    {
        bail!("live execution refuses markets with unsupported fee curves");
    }
    ensure_yes_no_neg_risk_metadata(opp, &markets)?;

    let plan_snapshots = refreshed_plan_snapshots(&markets, opp, config)?;
    let (_cost, _fees, scalable_unit_edge, roi_pct) =
        projected_trade_metrics(opp, &plan_snapshots, 1.0, config, 0.0)?;

    if scalable_unit_edge <= 0.0 || roi_pct < config.min_roi_pct {
        bail!(
            "stale arb: refreshed_unit_edge_ex_gas=${scalable_unit_edge:.4} roi={roi_pct:.2}% min_roi={:.2}%",
            config.min_roi_pct,
        );
    }

    info!(
        "Price refresh OK: event={} arb={} refreshed_unit_edge_ex_gas=${scalable_unit_edge:.4} refreshed_roi={roi_pct:.2}%",
        opp.event_id,
        opp.arb_type,
    );

    Ok((markets, plan_snapshots))
}

async fn build_legs(
    http: &Client,
    config: &Config,
    opp: &ArbitrageOpportunity,
    plan_snapshots: &[PlanLegSnapshot],
    server_clock: &ServerClock,
) -> Result<(Vec<LiveOrderLeg>, f64)> {
    if plan_snapshots.len() != opp.execution_plan.len() {
        bail!("plan snapshot length mismatch");
    }

    let executable_cap =
        if opp.max_executable_size_usd.is_finite() && opp.max_executable_size_usd > 0.0 {
            opp.max_executable_size_usd
        } else {
            config.live_trade_position_size_usd
        };
    let requested_position_usd = config.live_trade_position_size_usd.min(executable_cap);
    let min_leg_usd = config.live_min_leg_size_usd.max(0.0);
    if requested_position_usd <= f64::EPSILON {
        bail!("non-positive live position size after executable-size cap");
    }

    let unit_step = basket_unit_step(&opp.execution_plan, config);
    let mut max_basket_units = f64::INFINITY;
    let mut total_cost_per_basket = 0.0;
    let mut token_ids = Vec::new();

    for (leg, snapshot) in opp.execution_plan.iter().zip(plan_snapshots.iter()) {
        if leg.unit_shares <= f64::EPSILON {
            bail!(
                "execution plan contains non-positive unit_shares for '{}'",
                leg.question
            );
        }
        let token_id = if !leg.token_id.is_empty() {
            leg.token_id.clone()
        } else {
            match leg.outcome {
                OutcomeSide::Yes => snapshot.market.clob_token_id_yes.clone(),
                OutcomeSide::No => snapshot.market.clob_token_id_no.clone(),
            }
        };
        if token_id.is_empty() {
            bail!("missing token id for leg '{}'", leg.question);
        }

        total_cost_per_basket += snapshot.limit_price * leg.unit_shares;
        token_ids.push(token_id);
    }

    let depth_snapshots =
        match clob_client::get_live_depth_snapshots(http, config, &token_ids).await {
            Ok(snapshots) => snapshots,
            Err(err) => {
                // Final-depth fetch is live-critical; preserve status text for breaker classification.
                if let Some((_, _)) = live_error_pause(&err.to_string()) {
                    warn!("Live final depth fetch failed; circuit breaker will pause live: {err}");
                }
                return Err(err);
            }
        };
    let mut quoted_depths = Vec::new();

    for ((leg, snapshot), token_id) in opp
        .execution_plan
        .iter()
        .zip(plan_snapshots.iter())
        .zip(token_ids.iter())
    {
        let depth = depth_snapshots
            .get(token_id)
            .with_context(|| format!("missing live depth snapshot for leg '{}'", leg.question))?;
        let venue_age_ms = ensure_final_depth_fresh(depth, server_clock, config)?;
        ensure_final_depth_rules_match(depth, &snapshot.market, &leg.question)?;
        let available_shares = depth.available_shares_at_price(snapshot.limit_price);

        let min_order_shares = snapshot.market.min_order_size_shares();
        let required_depth_usd = min_leg_usd.max(min_order_shares * snapshot.limit_price);
        let available_limit_notional = available_shares * snapshot.limit_price;
        if available_limit_notional + f64::EPSILON < required_depth_usd {
            bail!(
                "insufficient executable depth for leg '{}': available=${available_limit_notional:.2} < required=${required_depth_usd:.2}",
                leg.question,
            );
        }

        let max_units_for_leg = available_shares / leg.unit_shares;
        max_basket_units = max_basket_units.min(max_units_for_leg);
        quoted_depths.push((
            token_id.clone(),
            depth.clone(),
            available_limit_notional,
            venue_age_ms,
        ));
    }

    if total_cost_per_basket <= f64::EPSILON {
        bail!("non-positive live basket cost");
    }

    let mut basket_units = round_down_to_step(
        (requested_position_usd / total_cost_per_basket).min(max_basket_units),
        unit_step,
    );
    if basket_units <= f64::EPSILON {
        bail!("calculated live basket size is non-positive");
    }

    let mut adjusted_total_cost_per_basket = 0.0;
    for ((leg, snapshot), (_, depth, _, _)) in opp
        .execution_plan
        .iter()
        .zip(plan_snapshots.iter())
        .zip(quoted_depths.iter())
    {
        let price = depth
            .cutoff_ask_for_shares(basket_units * leg.unit_shares)
            .with_context(|| {
                format!(
                    "missing depth-aware cutoff ask for live leg '{}' at requested size",
                    leg.question
                )
            })?;
        adjusted_total_cost_per_basket +=
            apply_slippage(price, config.live_slippage_bps, snapshot.market.tick_size())
                * leg.unit_shares;
    }

    if adjusted_total_cost_per_basket > f64::EPSILON {
        let adjusted_target_units = requested_position_usd / adjusted_total_cost_per_basket;
        basket_units = round_down_to_step(adjusted_target_units.min(max_basket_units), unit_step);
    }
    if basket_units <= f64::EPSILON {
        bail!("calculated live basket size is non-positive after depth-aware repricing");
    }

    // Normalize through per-leg share rounding so ranked baskets remain hedgeable after step constraints.
    let share_step = live_order_size_step_shares(config);
    let normalized_units = opp
        .execution_plan
        .iter()
        .map(|leg| {
            let rounded_shares = round_down_to_step(basket_units * leg.unit_shares, share_step);
            rounded_shares / leg.unit_shares.max(0.000_000_1)
        })
        .fold(basket_units, f64::min);
    basket_units = round_down_to_step(normalized_units, unit_step);
    if basket_units <= f64::EPSILON {
        bail!("share-step normalization reduced live basket size to zero");
    }

    let mut legs = Vec::new();
    for ((leg, snapshot), (token_id, depth, available_usd, venue_age_ms)) in opp
        .execution_plan
        .iter()
        .zip(plan_snapshots.iter())
        .zip(quoted_depths.into_iter())
    {
        let reference_price = depth
            .average_ask_for_shares(basket_units * leg.unit_shares)
            .with_context(|| {
                format!(
                    "missing depth-aware average ask for live leg '{}' at final size",
                    leg.question
                )
            })?;
        let cutoff_price = depth
            .cutoff_ask_for_shares(basket_units * leg.unit_shares)
            .with_context(|| {
                format!(
                    "missing depth-aware cutoff ask for live leg '{}' at final size",
                    leg.question
                )
            })?;
        let limit_price = apply_slippage(
            cutoff_price,
            config.live_slippage_bps,
            snapshot.market.tick_size(),
        );
        let size = round_down_to_step(basket_units * leg.unit_shares, share_step);
        let leg_notional = limit_price * size;
        let min_order_shares = snapshot.market.min_order_size_shares();
        if size + f64::EPSILON < min_order_shares {
            bail!(
                "leg '{}' would be {:.4} shares, below market minimum {:.4} shares",
                leg.question,
                size,
                min_order_shares,
            );
        }
        if leg_notional + f64::EPSILON < min_leg_usd {
            bail!(
                "leg '{}' would be ${leg_notional:.2}, below LIVE_MIN_LEG_SIZE_USD=${:.2}",
                leg.question,
                min_leg_usd,
            );
        }

        let fee_schedule =
            fees::verified_clob_fee_schedule(&snapshot.market).with_context(|| {
                format!(
                    "live leg '{}' is missing authoritative CLOB fd.r/fd.e fee metadata",
                    leg.question
                )
            })?;

        info!(
            "Live leg: '{}' {} avg_ask={:.4} cutoff_ask={:.4} limit={:.4} tick={:.4} depth_usd={:.2} size={:.4} venue_age_ms={}",
            leg.question,
            leg.outcome,
            reference_price,
            cutoff_price,
            limit_price,
            snapshot.market.tick_size(),
            available_usd,
            size,
            venue_age_ms,
        );

        legs.push(LiveOrderLeg {
            market_index: leg.market_index,
            condition_id: leg.condition_id.clone(),
            token_id,
            side: Side::Buy,
            price: limit_price,
            raw_price: snapshot.raw_ask,
            size,
            unit_shares: leg.unit_shares,
            tick_size: snapshot.market.tick_size(),
            question: leg.question.clone(),
            outcome: leg.outcome,
            min_order_shares,
            neg_risk: snapshot.market.clob_neg_risk,
            fee_rate: fee_schedule.rate,
            fee_exponent: fee_schedule.exponent,
            venue_timestamp_ms: depth.venue_timestamp_ms,
            venue_age_ms: Some(venue_age_ms),
            book_hash: depth.book_hash.clone(),
        });
    }

    Ok((legs, basket_units))
}

async fn cancel_open_orders<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    order_ids: &[String],
    circuit_breaker: &LiveCircuitBreaker,
) where
    K: Kind,
{
    if order_ids.is_empty() {
        return;
    }

    let refs: Vec<&str> = order_ids.iter().map(String::as_str).collect();
    if let Err(err) = sdk_client.cancel_orders(&refs).await {
        circuit_breaker.trip_for_error(&err);
        warn!("batch cancel failed: {err}");
        for oid in order_ids {
            if let Err(e) = sdk_client.cancel_order(oid).await {
                circuit_breaker.trip_for_error(&e);
                warn!("cancel failed for order {oid}: {e}");
            }
        }
    }
}

async fn maybe_cancel_all_on_kill_switch<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    config: &Config,
    circuit_breaker: &LiveCircuitBreaker,
    reason: &str,
) where
    K: Kind,
{
    if !config.live_cancel_all_on_kill_switch {
        debug!("Live kill switch cancel-all disabled; reason={reason}");
        return;
    }

    match sdk_client.cancel_all_orders().await {
        Ok(response) => {
            let canceled = response.canceled.len();
            if response.not_canceled.is_empty() {
                warn!(
                    "Live kill switch cancel-all completed: reason={} canceled_orders={}",
                    reason, canceled
                );
            } else {
                let err = anyhow!(
                    "live kill switch cancel-all incomplete: reason={} canceled_orders={} not_canceled={:?}",
                    reason,
                    canceled,
                    response.not_canceled
                );
                circuit_breaker.trip_for_error(&err);
                warn!("{err:#}");
            }
        }
        Err(err) => {
            let err = anyhow!("live kill switch cancel-all failed: reason={reason}: {err}");
            circuit_breaker.trip_for_error(&err);
            warn!("{err:#}");
        }
    }
}

fn normalize_status_value(raw: &str) -> String {
    let mut out = String::new();
    let mut last_was_separator = false;
    for ch in raw.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
            last_was_separator = false;
        } else if !last_was_separator && !out.is_empty() {
            out.push('_');
            last_was_separator = true;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}

fn normalized_status_is_confirmed(value: &str) -> bool {
    matches!(
        value,
        "CONFIRMED" | "ORDER_CONFIRMED" | "TRADE_CONFIRMED" | "SETTLEMENT_CONFIRMED" | "SETTLED"
    )
}

fn normalized_order_status_is_immediate_fill(value: &str) -> bool {
    normalized_status_is_confirmed(value)
        || matches!(
            value,
            "MATCHED" | "ORDER_MATCHED" | "FILLED" | "FULLY_FILLED" | "ORDER_FILLED"
        )
}

fn normalized_status_is_terminal_failure(value: &str) -> bool {
    matches!(
        value,
        "CANCELED"
            | "CANCELLED"
            | "CANCEL"
            | "EXPIRED"
            | "EXPIRE"
            | "UNMATCHED"
            | "INVALID"
            | "FAILED"
            | "REJECTED"
            | "REJECT"
    ) || value.ends_with("_CANCELED")
        || value.ends_with("_CANCELLED")
        || value.ends_with("_EXPIRED")
        || value.ends_with("_UNMATCHED")
        || value.ends_with("_INVALID")
        || value.ends_with("_FAILED")
        || value.ends_with("_REJECTED")
        || value.starts_with("CANCELED_")
        || value.starts_with("CANCELLED_")
        || value.starts_with("EXPIRED_")
        || value.starts_with("UNMATCHED_")
        || value.starts_with("INVALID_")
        || value.starts_with("FAILED_")
        || value.starts_with("REJECTED_")
}

fn normalized_status_is_matched_unconfirmed(value: &str) -> bool {
    matches!(
        value,
        "MATCHED" | "ORDER_MATCHED" | "MATCHED_UNCONFIRMED" | "MINED" | "ORDER_MINED" | "RETRYING"
    )
}

fn order_status_is_confirmed(status: &OrderStatusType) -> bool {
    matches!(status, OrderStatusType::Unknown(raw) if normalized_status_is_confirmed(&normalize_status_value(raw)))
}

fn fok_post_order_status_is_immediate_fill(status: &OrderStatusType) -> bool {
    match status {
        OrderStatusType::Matched => true,
        OrderStatusType::Unknown(raw) => {
            normalized_order_status_is_immediate_fill(&normalize_status_value(raw))
        }
        _ => false,
    }
}

fn ensure_fok_market_price_not_above_limit(
    question: &str,
    calculated_price: Decimal,
    limit_price: Decimal,
) -> Result<()> {
    if calculated_price > limit_price {
        bail!(
            "live FOK market-price oracle rejected leg '{}': calculated_market_price={} > signed_limit_price={}",
            question,
            calculated_price,
            limit_price
        );
    }

    Ok(())
}

async fn ensure_sdk_fok_market_price_oracle<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    legs: &[LiveOrderLeg],
    order_type: &OrderType,
) -> Result<()>
where
    K: Kind,
{
    if !matches!(order_type, OrderType::FOK) {
        bail!("live FOK market-price oracle requires LIVE_ORDER_TYPE=fok");
    }

    for leg in legs {
        let token_id = U256::from_str(&leg.token_id)
            .with_context(|| format!("invalid token id for FOK oracle: {}", leg.token_id))?;
        let size = Decimal::from_str(&format_live_order_size(leg.size))
            .with_context(|| format!("invalid live order size for FOK oracle: {}", leg.size))?;
        let amount = Amount::shares(size)
            .with_context(|| format!("invalid share amount for FOK oracle: {}", leg.question))?;
        let limit_price = Decimal::from_str(&clob_client::format_price_for_tick(
            leg.price,
            leg.tick_size,
        ))
        .with_context(|| {
            format!(
                "invalid signed limit price for FOK oracle: {}",
                leg.question
            )
        })?;
        let calculated_price = sdk_client
            .calculate_market_price(token_id, leg.side, amount, OrderType::FOK)
            .await
            .with_context(|| {
                format!(
                    "live FOK market-price oracle could not price leg '{}'",
                    leg.question
                )
            })?;

        ensure_fok_market_price_not_above_limit(&leg.question, calculated_price, limit_price)?;
        debug!(
            "Live FOK market-price oracle accepted '{}' size={} calculated_market_price={} signed_limit_price={}",
            leg.question,
            size,
            calculated_price,
            limit_price
        );
    }

    Ok(())
}

fn order_status_is_terminal_failure(status: &OrderStatusType) -> bool {
    match status {
        OrderStatusType::Canceled | OrderStatusType::Unmatched => true,
        OrderStatusType::Unknown(raw) => {
            normalized_status_is_terminal_failure(&normalize_status_value(raw))
        }
        _ => false,
    }
}

fn order_status_is_matched_unconfirmed(status: &OrderStatusType) -> bool {
    if order_status_is_confirmed(status) || order_status_is_terminal_failure(status) {
        return false;
    }
    match status {
        OrderStatusType::Matched => true,
        OrderStatusType::Unknown(raw) => {
            normalized_status_is_matched_unconfirmed(&normalize_status_value(raw))
        }
        _ => false,
    }
}

fn trade_status_is_confirmed(status: &TradeStatusType) -> bool {
    matches!(status, TradeStatusType::Confirmed)
        || matches!(status, TradeStatusType::Unknown(raw) if normalized_status_is_confirmed(&normalize_status_value(raw)))
}

fn trade_status_is_terminal_failure(status: &TradeStatusType) -> bool {
    matches!(status, TradeStatusType::Failed)
        || matches!(status, TradeStatusType::Unknown(raw) if normalized_status_is_terminal_failure(&normalize_status_value(raw)))
}

fn trade_status_is_matched_unconfirmed(status: &TradeStatusType) -> bool {
    !trade_status_is_confirmed(status)
        && !trade_status_is_terminal_failure(status)
        && match status {
            TradeStatusType::Matched | TradeStatusType::Mined | TradeStatusType::Retrying => true,
            TradeStatusType::Unknown(raw) => {
                normalized_status_is_matched_unconfirmed(&normalize_status_value(raw))
            }
            _ => false,
        }
}

async fn await_order_fills<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    config: &Config,
    order_ids: &[String],
    opp: &ArbitrageOpportunity,
    circuit_breaker: &LiveCircuitBreaker,
) -> Result<()>
where
    K: Kind,
{
    let timeout = Duration::from_secs(config.live_fill_poll_timeout_secs);
    let poll_interval = Duration::from_millis(config.live_fill_poll_interval_ms.max(100));
    let deadline = Instant::now() + timeout;
    let mut pending: Vec<String> = order_ids.to_vec();
    let mut matched_unconfirmed = Vec::new();

    while !pending.is_empty() && Instant::now() < deadline {
        if let Err(err) = user_channel::ensure_live_user_channel_ready(config) {
            circuit_breaker.trip_for_error(&err);
            return Err(err);
        }
        match user_channel::live_user_channel_fill_status(config, &pending, &[]) {
            Ok(status) => {
                if !status.failed_order_ids.is_empty() {
                    bail!(
                        "user-channel reported failed live orders before REST poll: {:?}",
                        status.failed_order_ids
                    );
                }
                if !status.confirmed_order_ids.is_empty() {
                    pending.retain(|order_id| !status.confirmed_order_ids.contains(order_id));
                    if pending.is_empty() {
                        info!(
                            "Settlement confirmed from user channel: event={} orders={:?}",
                            opp.event_id, order_ids
                        );
                        return Ok(());
                    }
                }
            }
            Err(err) => {
                circuit_breaker.trip_for_error(&err);
                return Err(err).context("reading user-channel fill status before order poll");
            }
        }
        let status =
            user_channel::wait_for_live_user_channel_fill_status(&pending, &[], poll_interval)
                .await;
        if !status.failed_order_ids.is_empty() {
            bail!(
                "user-channel reported failed live orders before REST poll: {:?}",
                status.failed_order_ids
            );
        }
        if !status.confirmed_order_ids.is_empty() {
            pending.retain(|order_id| !status.confirmed_order_ids.contains(order_id));
            if pending.is_empty() {
                info!(
                    "Settlement confirmed from user-channel event bus: event={} orders={:?}",
                    opp.event_id, order_ids
                );
                return Ok(());
            }
        }
        if let Err(err) = user_channel::ensure_live_user_channel_ready(config) {
            circuit_breaker.trip_for_error(&err);
            return Err(err);
        }
        let mut still_pending = Vec::new();
        matched_unconfirmed.clear();

        for order_id in &pending {
            let order = match sdk_client.order(order_id).await {
                Ok(order) => order,
                Err(err) => {
                    circuit_breaker.trip_for_error(&err);
                    warn!("Order status fetch error for {order_id}: {err}");
                    still_pending.push(order_id.clone());
                    continue;
                }
            };

            let original_size = decimal_to_f64(&order.original_size);
            let size_matched = decimal_to_f64(&order.size_matched);
            let remaining = (original_size - size_matched).max(0.0);

            if order_status_is_confirmed(&order.status) {
                if remaining > 0.000001 {
                    bail!(
                        "order {order_id} reported CONFIRMED but still has {:.6} remaining shares",
                        remaining
                    );
                }
                info!(
                    "Settlement confirmed: order={order_id} event={} status={:?} size={size_matched:.4}",
                    opp.event_id, order.status
                );
            } else if order_status_is_terminal_failure(&order.status) {
                bail!(
                    "order {order_id} reached terminal status {:?} without confirmed fill",
                    order.status
                );
            } else {
                if order_status_is_matched_unconfirmed(&order.status) {
                    if remaining > 0.000001 {
                        bail!(
                            "order {order_id} reported {:?} but still has {:.6} remaining shares",
                            order.status,
                            remaining
                        );
                    }
                    matched_unconfirmed.push(order_id.clone());
                    warn!(
                        "Order matched but not settlement-confirmed: order={order_id} event={} status={:?} matched={size_matched:.4}",
                        opp.event_id, order.status
                    );
                } else if size_matched > 0.0 {
                    warn!(
                        "Order status pending confirmation: order={order_id} event={} status={:?} matched={size_matched:.4} remaining={remaining:.4}",
                        opp.event_id, order.status
                    );
                }
                still_pending.push(order_id.clone());
            }
        }

        pending = still_pending;
    }

    if !pending.is_empty() {
        if !matched_unconfirmed.is_empty() {
            bail!(
                "fill settlement confirmation timed out after {}s with matched_unconfirmed orders: {:?}",
                config.live_fill_poll_timeout_secs,
                matched_unconfirmed
            );
        }
        bail!(
            "fill confirmation timed out after {}s for orders: {:?}",
            config.live_fill_poll_timeout_secs,
            pending
        );
    }

    Ok(())
}

async fn await_trade_confirmations<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    config: &Config,
    trade_ids: &[String],
    opp: &ArbitrageOpportunity,
    circuit_breaker: &LiveCircuitBreaker,
) -> Result<Vec<String>>
where
    K: Kind,
{
    let timeout = Duration::from_secs(config.live_fill_poll_timeout_secs);
    let poll_interval = Duration::from_millis(config.live_fill_poll_interval_ms.max(100));
    let deadline = Instant::now() + timeout;
    let mut pending: Vec<String> = trade_ids.to_vec();
    let mut matched_unconfirmed = Vec::new();
    let mut transaction_hashes = Vec::new();

    while !pending.is_empty() && Instant::now() < deadline {
        if let Err(err) = user_channel::ensure_live_user_channel_ready(config) {
            circuit_breaker.trip_for_error(&err);
            return Err(err);
        }
        match user_channel::live_user_channel_fill_status(config, &[], &pending) {
            Ok(status) => {
                if !status.failed_trade_ids.is_empty() {
                    bail!(
                        "user-channel reported failed live trades before REST poll: {:?}",
                        status.failed_trade_ids
                    );
                }
                if !status.confirmed_trade_ids.is_empty() {
                    for hash in status.transaction_hashes {
                        append_unique_transaction_hash(&mut transaction_hashes, hash);
                    }
                    pending.retain(|trade_id| !status.confirmed_trade_ids.contains(trade_id));
                    if pending.is_empty() {
                        info!(
                            "Trade settlement confirmed from user channel: event={} trades={:?}",
                            opp.event_id, trade_ids
                        );
                        return Ok(transaction_hashes);
                    }
                }
            }
            Err(err) => {
                circuit_breaker.trip_for_error(&err);
                return Err(err).context("reading user-channel fill status before trade poll");
            }
        }
        let status =
            user_channel::wait_for_live_user_channel_fill_status(&[], &pending, poll_interval)
                .await;
        if !status.failed_trade_ids.is_empty() {
            bail!(
                "user-channel reported failed live trades before REST poll: {:?}",
                status.failed_trade_ids
            );
        }
        if !status.confirmed_trade_ids.is_empty() {
            for hash in status.transaction_hashes {
                append_unique_transaction_hash(&mut transaction_hashes, hash);
            }
            pending.retain(|trade_id| !status.confirmed_trade_ids.contains(trade_id));
            if pending.is_empty() {
                info!(
                    "Trade settlement confirmed from user-channel event bus: event={} trades={:?}",
                    opp.event_id, trade_ids
                );
                return Ok(transaction_hashes);
            }
        }
        if let Err(err) = user_channel::ensure_live_user_channel_ready(config) {
            circuit_breaker.trip_for_error(&err);
            return Err(err);
        }
        let mut still_pending = Vec::new();
        matched_unconfirmed.clear();

        for trade_id in &pending {
            let request = TradesRequest::builder().id(trade_id.clone()).build();
            let page = match sdk_client.trades(&request, None).await {
                Ok(page) => page,
                Err(err) => {
                    circuit_breaker.trip_for_error(&err);
                    warn!("Trade status fetch error for {trade_id}: {err}");
                    still_pending.push(trade_id.clone());
                    continue;
                }
            };

            let Some(trade) = page.data.iter().find(|trade| trade.id == *trade_id) else {
                warn!(
                    "Trade status pending: trade={trade_id} event={} not found",
                    opp.event_id
                );
                still_pending.push(trade_id.clone());
                continue;
            };

            if trade_status_is_confirmed(&trade.status) {
                append_unique_transaction_hash(&mut transaction_hashes, trade.transaction_hash);
                info!(
                    "Trade settlement confirmed: trade={trade_id} event={} status={:?} size={} tx={}",
                    opp.event_id,
                    trade.status,
                    trade.size,
                    trade.transaction_hash
                );
            } else if trade_status_is_terminal_failure(&trade.status) {
                bail!(
                    "trade {trade_id} reached terminal status {:?} without confirmed settlement: error={:?}",
                    trade.status,
                    trade.error_msg
                );
            } else {
                if trade_status_is_matched_unconfirmed(&trade.status) {
                    matched_unconfirmed.push(trade_id.clone());
                }
                warn!(
                    "Trade status pending confirmation: trade={trade_id} event={} status={:?} tx={} error={:?}",
                    opp.event_id,
                    trade.status,
                    trade.transaction_hash,
                    trade.error_msg
                );
                still_pending.push(trade_id.clone());
            }
        }

        pending = still_pending;
    }

    if !pending.is_empty() {
        if !matched_unconfirmed.is_empty() {
            bail!(
                "trade settlement confirmation timed out after {}s with matched_unconfirmed trades: {:?}",
                config.live_fill_poll_timeout_secs,
                matched_unconfirmed
            );
        }
        bail!(
            "trade confirmation timed out after {}s for trades: {:?}",
            config.live_fill_poll_timeout_secs,
            pending
        );
    }

    Ok(transaction_hashes)
}

fn append_unique_transaction_hash(transaction_hashes: &mut Vec<String>, hash: impl ToString) {
    let hash = hash.to_string();
    let hash = hash.trim();
    if hash.is_empty() {
        return;
    }
    if !transaction_hashes
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(hash))
    {
        transaction_hashes.push(hash.to_string());
    }
}

fn append_unique_transaction_hashes<T: ToString>(
    transaction_hashes: &mut Vec<String>,
    hashes: impl IntoIterator<Item = T>,
) {
    for hash in hashes {
        append_unique_transaction_hash(transaction_hashes, hash);
    }
}

fn validate_live_order_fill(
    order_id: &str,
    leg: &LiveOrderLeg,
    actual_token_id: &str,
    actual_side: &Side,
    original_size: f64,
    size_matched: f64,
    price: f64,
) -> Result<()> {
    if actual_token_id != leg.token_id {
        bail!(
            "confirmed order {order_id} token mismatch: actual={} expected={}",
            actual_token_id,
            leg.token_id,
        );
    }
    if actual_side != &leg.side {
        bail!(
            "confirmed order {order_id} side mismatch: actual={} expected={}",
            actual_side,
            leg.side,
        );
    }
    let remaining = (original_size - size_matched).max(0.0);
    if remaining > 0.000001 {
        bail!(
            "confirmed order {order_id} has {:.6} remaining shares after fill reconciliation",
            remaining
        );
    }
    if (size_matched - leg.size).abs() > 0.000001 {
        bail!(
            "confirmed order {order_id} fill size mismatch: actual={:.6} expected={:.6}",
            size_matched,
            leg.size,
        );
    }
    if price > leg.price + 0.000001 {
        bail!(
            "confirmed order {order_id} price exceeds planned limit: actual={:.6} expected_limit={:.6}",
            price,
            leg.price,
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_authenticated_taker_trade(
    order_id: &str,
    leg: &LiveOrderLeg,
    trade_id: &str,
    taker_order_id: &str,
    actual_token_id: &str,
    actual_side: &Side,
    trader_side: &TraderSide,
    status: &TradeStatusType,
    size: f64,
    price: f64,
) -> Result<()> {
    if !trade_status_is_confirmed(status) {
        bail!(
            "authenticated trade {trade_id} for order {order_id} is not confirmed: status={status:?}"
        );
    }
    if trader_side != &TraderSide::Taker || taker_order_id != order_id {
        bail!(
            "authenticated trade {trade_id} does not prove taker execution for order {order_id}: trader_side={trader_side:?} taker_order_id={taker_order_id}"
        );
    }
    if actual_token_id != leg.token_id || actual_side != &leg.side {
        bail!(
            "authenticated trade {trade_id} economic mismatch for order {order_id}: token={} expected={} side={} expected_side={}",
            actual_token_id,
            leg.token_id,
            actual_side,
            leg.side,
        );
    }
    if !size.is_finite() || size <= f64::EPSILON {
        bail!("authenticated trade {trade_id} for order {order_id} has invalid size={size}");
    }
    if !price.is_finite() || price <= 0.0 || price > leg.price + 0.000001 {
        bail!(
            "authenticated trade {trade_id} for order {order_id} has invalid price={price:.6} expected_limit={:.6}",
            leg.price,
        );
    }
    Ok(())
}

async fn verify_live_order_fills<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    legs: &[LiveOrderLeg],
    markets: &[Market],
    order_ids: &[String],
    opp: &ArbitrageOpportunity,
    entry_gas_cost_usd: f64,
    circuit_breaker: &LiveCircuitBreaker,
) -> Result<LiveEntryAccounting>
where
    K: Kind,
{
    if legs.len() != order_ids.len() {
        bail!(
            "cannot reconcile live fills: legs={} order_ids={}",
            legs.len(),
            order_ids.len()
        );
    }

    let mut accounting = LiveEntryAccounting {
        entry_gas_cost_usd: entry_gas_cost_usd.max(0.0),
        ..LiveEntryAccounting::default()
    };
    for (leg, order_id) in legs.iter().zip(order_ids.iter()) {
        let order = match sdk_client.order(order_id).await {
            Ok(order) => order,
            Err(err) => {
                circuit_breaker.trip_for_error(&err);
                return Err(err)
                    .with_context(|| format!("failed to fetch confirmed order {order_id}"));
            }
        };
        let actual_size = decimal_to_f64(&order.size_matched);
        let order_price = decimal_to_f64(&order.price);
        validate_live_order_fill(
            order_id,
            leg,
            &order.asset_id.to_string(),
            &order.side,
            decimal_to_f64(&order.original_size),
            actual_size,
            order_price,
        )?;
        markets.get(leg.market_index).ok_or_else(|| {
            anyhow!(
                "confirmed live fill references missing market index {}",
                leg.market_index
            )
        })?;
        if order.associate_trades.is_empty() {
            bail!(
                "confirmed order {order_id} has no authenticated associated trades; exact live fee accounting unavailable"
            );
        }

        let mut associated_trade_ids = HashSet::new();
        let mut authenticated_size = 0.0;
        for trade_id in &order.associate_trades {
            let trade_id = trade_id.trim();
            if trade_id.is_empty() || !associated_trade_ids.insert(trade_id.to_string()) {
                bail!("confirmed order {order_id} has empty or duplicate authenticated trade id");
            }
            let request = TradesRequest::builder().id(trade_id.to_string()).build();
            let page = match sdk_client.trades(&request, None).await {
                Ok(page) => page,
                Err(err) => {
                    circuit_breaker.trip_for_error(&err);
                    return Err(err).with_context(|| {
                        format!(
                            "failed to fetch authenticated trade {trade_id} for order {order_id}"
                        )
                    });
                }
            };
            let trade = page
                .data
                .iter()
                .find(|trade| trade.id == trade_id)
                .with_context(|| {
                    format!("authenticated trade {trade_id} missing for confirmed order {order_id}")
                })?;
            let trade_size = decimal_to_f64(&trade.size);
            let trade_price = decimal_to_f64(&trade.price);
            validate_authenticated_taker_trade(
                order_id,
                leg,
                trade_id,
                &trade.taker_order_id,
                &trade.asset_id.to_string(),
                &trade.side,
                &trade.trader_side,
                &trade.status,
                trade_size,
                trade_price,
            )?;
            add_authenticated_fill_accounting(&mut accounting, leg, trade_price, trade_size);
            authenticated_size += trade_size;
        }
        if (authenticated_size - actual_size).abs() > 0.000001 {
            bail!(
                "confirmed order {order_id} authenticated trade size mismatch: trades={authenticated_size:.6} order_size_matched={actual_size:.6}"
            );
        }
        info!(
            "Live fill reconciled from authenticated trades: event={} order={} token={} size_matched={} limit_price={} fee_rate={} fee_exponent={}",
            opp.event_id,
            order_id,
            order.asset_id,
            order.size_matched,
            order.price,
            leg.fee_rate,
            leg.fee_exponent,
        );
    }

    Ok(accounting)
}

async fn await_fills<K>(
    sdk_client: &ClobClient<Authenticated<K>>,
    config: &Config,
    order_ids: &[String],
    trade_ids: &[String],
    opp: &ArbitrageOpportunity,
    circuit_breaker: &LiveCircuitBreaker,
) -> Result<Vec<String>>
where
    K: Kind,
{
    if !trade_ids.is_empty() {
        await_trade_confirmations(sdk_client, config, trade_ids, opp, circuit_breaker).await
    } else {
        warn!(
            "No trade ids returned for event={}; falling back to order-status polling",
            opp.event_id
        );
        await_order_fills(sdk_client, config, order_ids, opp, circuit_breaker).await?;
        Ok(Vec::new())
    }
}

#[cfg(test)]
pub async fn execute_opportunity(
    opp: &ArbitrageOpportunity,
    config: &Config,
    http: &Client,
    exposure: &SharedExposureTracker,
) -> Result<LiveExecutionReport> {
    reject_external_token_opportunity(opp)?;
    ensure_live_arbitrage_routes_available()?;
    let executor = LiveExecutor::new(config).await?;
    execute_opportunity_with_executor(&executor, opp, config, http, exposure, None).await
}

pub async fn execute_combo_rfq_opportunity_with_executor(
    executor: &LiveExecutor,
    opp: &ArbitrageOpportunity,
    config: &Config,
    http: &Client,
    exposure: &SharedExposureTracker,
    catalog: &crate::combo_rfq_client::ComboMarketCatalog,
    price_cache: Option<&PriceCache>,
) -> Result<crate::combo_rfq_client::ComboRfqExecutionReport> {
    reject_external_token_opportunity(opp)?;
    ensure_combo_rfq_route_promoted(config).await?;
    user_channel::ensure_live_user_channel_ready(config)?;
    ensure_signal_fresh(opp, config)?;
    executor.circuit_breaker.check()?;
    ensure_status_page_allows_live_orders(http, config).await?;

    let requester_plan =
        crate::combo_rfq_client::build_combo_rfq_requester_plan(config, catalog, opp);
    if !requester_plan.blockers.is_empty() {
        bail!(
            "Combo/RFQ requester plan blocked before live submit: {}",
            requester_plan.blockers.join("; ")
        );
    }

    let position_usd = config.live_trade_position_size_usd;
    if position_usd <= f64::EPSILON {
        bail!("LIVE_TRADE_POSITION_SIZE_USD must be positive for Combo/RFQ live route");
    }

    let combo_rfq_leg_count = requester_plan
        .request
        .as_ref()
        .map(|request| request.legs.len())
        .unwrap_or_else(|| opp.execution_plan.len());
    let (fresh_entry_gas_usd, fresh_closeout_gas_usd) =
        combo_rfq_live_gas_costs_usd(http, config, combo_rfq_leg_count).await?;
    let mut execution_config = config.clone();
    execution_config.gas_fallback_usd = fresh_closeout_gas_usd.max(config.gas_fallback_usd);
    let mut execution_opp = opp.clone();
    execution_opp.estimated_total_gas_cost_usd = execution_opp
        .estimated_total_gas_cost_usd
        .max(fresh_entry_gas_usd);

    let _submit_guard = executor.submit_lock.lock().await;
    if let Err(err) =
        ensure_live_pre_submit_geoblock(http, config, &executor.geoblock_last_allowed_at).await
    {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    user_channel::ensure_live_user_channel_ready(config)?;
    engine_mode::ensure_no_active_new_order_blocker(config)?;
    if let Err(err) = ensure_combo_rfq_pre_submit_account_guard(
        &executor.sdk_client,
        config,
        http,
        executor.account_address,
    )
    .await
    {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    exposure
        .check_and_reserve_with_total(
            &opp.event_id,
            position_usd,
            config.live_max_event_exposure_usd,
            config.live_max_total_exposure_usd,
        )
        .await
        .with_context(|| {
            format!(
                "exposure cap reached for Combo/RFQ event={} total_cap=${:.2}",
                opp.event_id, config.live_max_total_exposure_usd
            )
        })?;

    let report =
        match crate::combo_rfq_client::run_combo_rfq_execution_state_machine_with_price_cache(
            http,
            &execution_config,
            catalog,
            &execution_opp,
            price_cache,
        )
        .await
        {
            Ok(report) => report,
            Err(err) => {
                exposure.release(&opp.event_id, position_usd).await;
                executor.circuit_breaker.trip_for_error(&err);
                return Err(err).context("Combo/RFQ execution failed before accepted state");
            }
        };

    if combo_rfq_execution_report_was_accepted(&report) {
        warn!(
            "Combo/RFQ accepted; exposure retained pending finality/manual review: event={} status={} rfq_id={:?} blockers={}",
            opp.event_id,
            report.status,
            report.rfq_id,
            report.blockers.join("|")
        );
        Ok(report)
    } else if combo_rfq_execution_report_retains_exposure(report.accept_outcome, &report.blockers) {
        warn!(
            "Combo/RFQ exposure retained pending finality/manual review: event={} status={} blockers={}",
            opp.event_id,
            report.status,
            report.blockers.join("|")
        );
        let err = combo_rfq_retained_exposure_error(&opp.event_id, &report);
        executor.circuit_breaker.trip_for_error(&err);
        Err(err)
    } else {
        exposure.release(&opp.event_id, position_usd).await;
        bail!(
            "Combo/RFQ execution did not reach accepted state: status={} blockers={}",
            report.status,
            report.blockers.join("; ")
        )
    }
}

fn combo_rfq_execution_report_was_accepted(
    report: &crate::combo_rfq_client::ComboRfqExecutionReport,
) -> bool {
    matches!(
        report.accept_outcome,
        Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::Accepted)
    )
}

fn combo_rfq_retained_exposure_error(
    event_id: &str,
    report: &crate::combo_rfq_client::ComboRfqExecutionReport,
) -> anyhow::Error {
    anyhow::anyhow!(
        "Combo/RFQ exposure retained pending finality/manual review event={} status={} rfq_id={:?} blockers={}",
        event_id,
        report.status,
        report.rfq_id,
        report.blockers.join("; ")
    )
}

fn combo_rfq_execution_report_retains_exposure(
    accept_outcome: Option<crate::combo_rfq_client::ComboRfqAcceptOutcome>,
    blockers: &[String],
) -> bool {
    matches!(
        accept_outcome,
        Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::Accepted)
            | Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::Unknown)
    ) || blockers
        .iter()
        .any(|blocker| blocker == "exposure_must_remain_reserved_until_finality_or_manual_review")
}

pub async fn execute_opportunity_with_executor(
    executor: &LiveExecutor,
    opp: &ArbitrageOpportunity,
    config: &Config,
    http: &Client,
    exposure: &SharedExposureTracker,
    price_cache: Option<&PriceCache>,
) -> Result<LiveExecutionReport> {
    reject_external_token_opportunity(opp)?;
    ensure_live_arbitrage_routes_available()?;
    user_channel::ensure_live_user_channel_ready(config)?;
    ensure_signal_fresh(opp, config)?;
    if opp.execution_plan.is_empty() {
        bail!("live execution requires a non-empty execution plan");
    }
    ensure_live_basket_atomicity_supported(opp)?;
    if opp.execution_plan.len() > config.max_batchable_legs() {
        bail!(
            "basket has {} legs but the configured/exchange-safe batch limit is {}",
            opp.execution_plan.len(),
            config.max_batchable_legs()
        );
    }
    executor.circuit_breaker.check()?;
    user_channel::ensure_live_user_channel_ready(config)?;

    let sdk_client = &executor.sdk_client;
    let signer = executor.signer(config)?;

    debug!(
        "Startup CLOB server clock uncertainty={}ms; refreshing before live execution",
        executor.server_clock.uncertainty_ms
    );
    let server_clock = match ServerClock::sync(http, config).await {
        Ok(clock) => clock,
        Err(err) => {
            executor.circuit_breaker.trip_for_error(&err);
            return Err(err);
        }
    };
    if let Err(err) = ensure_live_server_clock_guard(&server_clock, config) {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    let final_refresh_started_at = Instant::now();
    let (fresh_markets, plan_snapshots) = refresh_and_validate(http, config, opp).await?;
    let (mut legs, _pre_normalized_basket_units) =
        match build_legs(http, config, opp, &plan_snapshots, &server_clock).await {
            Ok(result) => result,
            Err(err) => {
                executor.circuit_breaker.trip_for_error(&err);
                return Err(err);
            }
        };
    if let Err(err) = ensure_final_route_quote_coherent(&legs, config) {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    if let Err(err) = refresh_live_leg_fee_schedules(http, config, &mut legs).await {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    let basket_units = match normalize_legs_to_signed_order_values(&mut legs, config) {
        Ok(signed_units) => signed_units,
        Err(err) => {
            executor.circuit_breaker.trip_for_error(&err);
            return Err(err);
        }
    };
    let entry_gas_cost_usd =
        required_live_trade_gas_cost_usd(http, config, opp.execution_plan.len(), "entry").await?;
    let estimated_closeout_gas_usd = estimated_live_closeout_gas_cost_usd(http, config).await?;
    let projected_live_gas_cost_usd =
        live_total_gas_cost_usd(entry_gas_cost_usd, estimated_closeout_gas_usd);
    let (position_usd, _projected_fees_usd, projected_pnl_usd, projected_roi_pct) =
        projected_trade_metrics_for_legs(
            opp,
            &fresh_markets,
            &legs,
            basket_units,
            config,
            projected_live_gas_cost_usd,
        )?;
    let (edge_haircut_usd, adjusted_pnl_usd, adjusted_roi_pct) =
        ensure_live_edge_survives_haircut(position_usd, projected_pnl_usd, config)?;
    debug!(
        "Live final edge gate: event={} arb={} projected_pnl=${projected_pnl_usd:.4} projected_roi={projected_roi_pct:.2}% entry_gas=${:.4} estimated_closeout_gas=${:.4} haircut=${edge_haircut_usd:.4} adjusted_pnl=${adjusted_pnl_usd:.4} adjusted_roi={adjusted_roi_pct:.2}%",
        opp.event_id,
        opp.arb_type,
        entry_gas_cost_usd,
        estimated_closeout_gas_usd,
    );
    if let Err(err) = ensure_live_account_funding(sdk_client, config, &legs).await {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }

    let order_type = live_basket_order_type_from_config(&config.live_order_type)?;

    let mut signed_orders = Vec::new();
    for leg in &legs {
        let token_id = U256::from_str(&leg.token_id)
            .with_context(|| format!("invalid token id: {}", leg.token_id))?;

        if leg.size + f64::EPSILON < leg.min_order_shares {
            bail!(
                "calculated size for '{}' is below market minimum: shares={:.4} min_shares={:.4}",
                leg.question,
                leg.size,
                leg.min_order_shares
            );
        }

        if let Some(neg_risk) = leg.neg_risk {
            sdk_client.set_neg_risk(token_id, neg_risk);
        }
        if let Some(tick_size) = tick_size_from_f64(leg.tick_size) {
            sdk_client.set_tick_size(token_id, tick_size);
        }
        let (price, size, _, _) = signed_order_price_size(leg)?;

        let signable = sdk_client
            .limit_order()
            .token_id(token_id)
            .price(price)
            .size(size)
            .side(leg.side)
            .order_type(order_type.clone())
            .build()
            .await?;

        let signed = sdk_client.sign(&signer, signable).await?;
        signed_orders.push(signed);
    }
    ensure_single_live_clob_order_submit(signed_orders.len())?;
    let expected_order_hashes =
        expected_order_hashes_for_signed_orders(config, &legs, &signed_orders)?;

    if let Err(err) = ensure_sdk_fok_market_price_oracle(sdk_client, &legs, &order_type).await {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    ensure_submit_fresh(final_refresh_started_at, config)?;

    let _submit_guard = executor.submit_lock.lock().await;
    if let Err(err) =
        ensure_live_pre_submit_geoblock(http, config, &executor.geoblock_last_allowed_at).await
    {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    ensure_ws_causal_watermark_not_newer(price_cache, &legs, final_refresh_started_at).await?;
    if let Err(err) = verify_clean_pre_submit_account(sdk_client, executor.account_address).await {
        executor.circuit_breaker.trip_for_error(&err);
        maybe_cancel_all_on_kill_switch(
            sdk_client,
            config,
            &executor.circuit_breaker,
            "pre_submit_account_not_clean",
        )
        .await;
        return Err(err);
    }
    if let Err(err) = ensure_live_pre_submit_heartbeat(sdk_client, config).await {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    ensure_submit_fresh(final_refresh_started_at, config)?;
    executor.circuit_breaker.check()?;
    if let Err(err) = ensure_status_page_allows_live_orders(http, config).await {
        executor.circuit_breaker.trip_for_error(&err);
        return Err(err);
    }
    user_channel::ensure_live_user_channel_ready(config)?;
    ensure_submit_fresh(final_refresh_started_at, config)?;

    exposure
        .check_and_reserve_with_total(
            &opp.event_id,
            position_usd,
            config.live_max_event_exposure_usd,
            config.live_max_total_exposure_usd,
        )
        .await
        .with_context(|| {
            format!(
                "exposure cap reached for event={} total_cap=${:.2} - skipping",
                opp.event_id, config.live_max_total_exposure_usd
            )
        })?;

    let execution_id = live_execution_id(opp);
    if let Err(err) = record_live_journal_with_expected_order_hashes(
        executor,
        &execution_id,
        "submit_intent",
        opp,
        &legs,
        position_usd,
        None,
        projected_pnl_usd,
        projected_roi_pct,
        basket_units,
        &[],
        &expected_order_hashes,
        &[],
        &[],
        None,
    ) {
        exposure.release(&opp.event_id, position_usd).await;
        return Err(err).context("failed to persist live submit intent before order submission");
    }

    if let Err(err) = ensure_submit_fresh(final_refresh_started_at, config) {
        exposure.release(&opp.event_id, position_usd).await;
        warn_live_journal_failure_with_expected_order_hashes(
            executor,
            &execution_id,
            "pre_submit_released",
            opp,
            &legs,
            position_usd,
            None,
            projected_pnl_usd,
            projected_roi_pct,
            basket_units,
            &[],
            &expected_order_hashes,
            &[],
            &[],
            Some(err.to_string()),
        );
        return Err(err);
    }
    if let Err(err) = user_channel::ensure_live_user_channel_ready(config) {
        exposure.release(&opp.event_id, position_usd).await;
        warn_live_journal_failure_with_expected_order_hashes(
            executor,
            &execution_id,
            "pre_submit_released",
            opp,
            &legs,
            position_usd,
            None,
            projected_pnl_usd,
            projected_roi_pct,
            basket_units,
            &[],
            &expected_order_hashes,
            &[],
            &[],
            Some(err.to_string()),
        );
        return Err(err);
    }
    if let Err(err) = engine_mode::ensure_no_active_new_order_blocker(config) {
        exposure.release(&opp.event_id, position_usd).await;
        warn_live_journal_failure_with_expected_order_hashes(
            executor,
            &execution_id,
            "pre_submit_released",
            opp,
            &legs,
            position_usd,
            None,
            projected_pnl_usd,
            projected_roi_pct,
            basket_units,
            &[],
            &expected_order_hashes,
            &[],
            &[],
            Some(err.to_string()),
        );
        return Err(err);
    }
    let responses = match sdk_client.post_orders(signed_orders).await {
        Ok(responses) => responses,
        Err(err) => {
            if let Err(observe_err) =
                engine_mode::observe_error_text(config, "live_executor", "sdk_post_orders", &err)
            {
                warn!("Failed to record CLOB engine-mode observation: {observe_err:#}");
            }
            executor.circuit_breaker.trip_for_error(&err);
            warn!(
                "batch order submission state unknown for event={}; exposure reservation retained for reconciliation",
                opp.event_id
            );
            warn_live_journal_failure_with_expected_order_hashes(
                executor,
                &execution_id,
                "submit_unknown",
                opp,
                &legs,
                position_usd,
                None,
                projected_pnl_usd,
                projected_roi_pct,
                basket_units,
                &[],
                &expected_order_hashes,
                &[],
                &[],
                Some(err.to_string()),
            );
            bail!("batch order submission state unknown (submit_unknown): {err}");
        }
    };
    if let Err(err) = engine_mode::observe_http_response(
        config,
        "live_executor",
        "sdk_post_orders",
        200,
        None,
        None,
    ) {
        warn!("Failed to record CLOB engine-mode success observation: {err:#}");
    }

    let mut order_ids = Vec::new();
    let mut trade_ids = Vec::new();
    let mut transaction_hashes = Vec::new();
    let mut cleanup_ids = Vec::new();
    let mut rejected = Vec::new();
    for (idx, resp) in responses.iter().enumerate() {
        let order_id = resp.order_id.trim().to_string();
        if !order_id.is_empty() {
            cleanup_ids.push(order_id.clone());
        }

        let has_error = resp
            .error_msg
            .as_ref()
            .map(|msg| !msg.trim().is_empty())
            .unwrap_or(false);

        if resp.success
            && !order_id.is_empty()
            && !has_error
            && fok_post_order_status_is_immediate_fill(&resp.status)
        {
            order_ids.push(order_id);
            trade_ids.extend(
                resp.trade_ids
                    .iter()
                    .map(|id| id.trim().to_string())
                    .filter(|id| !id.is_empty()),
            );
            append_unique_transaction_hashes(
                &mut transaction_hashes,
                resp.transaction_hashes.iter(),
            );
        } else {
            rejected.push(format!(
                "leg#{}: success={} order_id='{}' status={:?} error={:?}",
                idx + 1,
                resp.success,
                resp.order_id,
                resp.status,
                resp.error_msg
            ));
        }
    }

    if !rejected.is_empty() || order_ids.len() != legs.len() {
        let rejected_msg = format!("{rejected:?}");
        executor.circuit_breaker.trip_for_error(&rejected_msg);
        if !cleanup_ids.is_empty() {
            cancel_open_orders(sdk_client, &cleanup_ids, &executor.circuit_breaker).await;
            warn!(
                "batch response was incomplete/rejected after creating {} order id(s); exposure reservation retained for manual review",
                cleanup_ids.len()
            );
            if let Err(journal_err) = record_live_journal_with_expected_order_hashes(
                executor,
                &execution_id,
                "submit_incomplete_retained",
                opp,
                &legs,
                position_usd,
                None,
                projected_pnl_usd,
                projected_roi_pct,
                basket_units,
                &cleanup_ids,
                &expected_order_hashes,
                &trade_ids,
                &transaction_hashes,
                Some(rejected_msg.clone()),
            ) {
                warn!(
                    "failed to append live execution journal after incomplete submit; exposure retained: {journal_err}"
                );
                bail!(
                    "batch order rejected/incomplete and journal append failed after order id(s) existed: submit_error={rejected_msg}; journal_error={journal_err}"
                );
            }
        } else {
            exposure.release(&opp.event_id, position_usd).await;
            warn_live_journal_failure_with_expected_order_hashes(
                executor,
                &execution_id,
                "submit_rejected_released",
                opp,
                &legs,
                position_usd,
                None,
                projected_pnl_usd,
                projected_roi_pct,
                basket_units,
                &[],
                &expected_order_hashes,
                &[],
                &[],
                Some(rejected_msg.clone()),
            );
        }
        bail!("batch order rejected or incomplete: {:?}", rejected);
    }

    if let Err(err) = record_live_journal_with_expected_order_hashes(
        executor,
        &execution_id,
        "submitted",
        opp,
        &legs,
        position_usd,
        None,
        projected_pnl_usd,
        projected_roi_pct,
        basket_units,
        &order_ids,
        &expected_order_hashes,
        &trade_ids,
        &transaction_hashes,
        None,
    ) {
        cancel_open_orders(sdk_client, &order_ids, &executor.circuit_breaker).await;
        warn!(
            "failed to append submitted live execution journal after accepted order id(s); exposure retained: {err}"
        );
        bail!("submitted live orders but failed to persist order/trade ids to journal: {err}");
    }

    info!(
        "Batch submitted: event={} arb={} legs={} basket_units={:.4} order_ids={:?} trade_ids={:?} tx_hashes={:?}",
        opp.event_id,
        opp.arb_type,
        legs.len(),
        basket_units,
        order_ids,
        trade_ids,
        transaction_hashes,
    );

    for (idx, leg) in legs.iter().enumerate() {
        debug!(
            "  Leg #{} [{} {}]: raw={:.4} limit={:.4} size={:.4} unit_shares={:.4} tick={:.4} min_order_shares={:.4} neg_risk={:?}",
            idx + 1,
            leg.question,
            leg.outcome,
            leg.raw_price,
            leg.price,
            leg.size,
            leg.unit_shares,
            leg.tick_size,
            leg.min_order_shares,
            leg.neg_risk,
        );
    }

    let fill_result: Result<(Vec<String>, LiveEntryAccounting)> = async {
        let confirmed_transaction_hashes = await_fills(
            sdk_client,
            config,
            &order_ids,
            &trade_ids,
            opp,
            &executor.circuit_breaker,
        )
        .await?;
        let entry_accounting = verify_live_order_fills(
            sdk_client,
            &legs,
            &fresh_markets,
            &order_ids,
            opp,
            entry_gas_cost_usd,
            &executor.circuit_breaker,
        )
        .await?;
        Ok((confirmed_transaction_hashes, entry_accounting))
    }
    .await;

    let (confirmed_transaction_hashes, entry_accounting) = match fill_result {
        Ok(result) => result,
        Err(err) => {
            executor.circuit_breaker.trip_for_error(&err);
            warn!(
                "fill confirmation failed for event={}: {err}. {}",
                opp.event_id,
                if config.live_cancel_on_fill_timeout {
                    "Attempting to cancel any resting orders."
                } else {
                    "Automatic cancel disabled; manual review required."
                }
            );
            if config.live_cancel_on_fill_timeout {
                cancel_open_orders(sdk_client, &order_ids, &executor.circuit_breaker).await;
            }
            warn!(
            "exposure reservation retained because one or more live orders may already have filled"
        );
            if let Err(journal_err) = record_live_journal_with_expected_order_hashes(
                executor,
                &execution_id,
                "fill_failed_retained",
                opp,
                &legs,
                position_usd,
                None,
                projected_pnl_usd,
                projected_roi_pct,
                basket_units,
                &order_ids,
                &expected_order_hashes,
                &trade_ids,
                &transaction_hashes,
                Some(err.to_string()),
            ) {
                warn!(
                "failed to append fill failure to live execution journal; exposure retained: {journal_err}"
            );
                return Err(err).context(format!(
                    "also failed to persist fill failure journal record: {journal_err}"
                ));
            }
            return Err(err);
        }
    };

    append_unique_transaction_hashes(
        &mut transaction_hashes,
        confirmed_transaction_hashes.iter().map(String::as_str),
    );

    if let Err(err) = record_live_journal_with_expected_order_hashes(
        executor,
        &execution_id,
        "fill_confirmed_exposure_retained",
        opp,
        &legs,
        position_usd,
        Some(entry_accounting),
        projected_pnl_usd,
        projected_roi_pct,
        basket_units,
        &order_ids,
        &expected_order_hashes,
        &trade_ids,
        &transaction_hashes,
        None,
    ) {
        warn!(
            "live fill confirmed but failed to append confirmed-fill journal record; exposure retained: {err}"
        );
        bail!("live fill confirmed but failed to persist confirmed-fill journal record: {err}");
    }

    info!(
        "Live orders settlement-confirmed; exposure retained until closeout/redeem reconciliation: event={} arb={} legs={} basket_units={:.4} position_usd=${position_usd:.2}",
        opp.event_id,
        opp.arb_type,
        legs.len(),
        basket_units,
    );

    Ok(LiveExecutionReport {
        position_usd,
        projected_pnl_usd,
        projected_roi_pct,
        basket_units,
        order_count: order_ids.len(),
        order_ids,
        trade_ids,
        transaction_hashes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OutcomeSide;
    use httpmock::prelude::*;

    #[test]
    fn slippage_price_respects_tick_size() {
        let price = apply_slippage(0.503, 25, 0.01);
        assert!((price - 0.51).abs() < 1e-9);
    }

    #[test]
    fn round_down_shares_uses_step() {
        let shares = round_down_to_step(12.34987, 0.0001);
        assert!((shares - 12.3498).abs() < 1e-9);
    }

    #[test]
    fn live_order_size_format_matches_sdk_lot_scale() {
        assert_eq!(format_live_order_size(12.34987), "12.34");
        assert_eq!(format_live_order_size(0.019), "0.01");
    }

    #[test]
    fn pre_sign_simulation_normalizes_signed_price_and_size() {
        let cfg = Config::from_env();
        let mut legs = vec![live_order_leg_for_test()];
        legs[0].price = 0.4041;
        legs[0].size = 3.019;

        let signed_units = normalize_legs_to_signed_order_values(&mut legs, &cfg).unwrap();

        assert!((legs[0].price - 0.41).abs() < 1e-9);
        assert!((legs[0].size - 3.01).abs() < 1e-9);
        assert!((signed_units - 3.01).abs() < 1e-9);
    }

    #[test]
    fn pre_sign_simulation_rejects_size_rounding_below_minimum() {
        let cfg = Config::from_env();
        let mut legs = vec![live_order_leg_for_test()];
        legs[0].size = 0.019;
        legs[0].min_order_shares = 0.02;

        let err = normalize_legs_to_signed_order_values(&mut legs, &cfg).unwrap_err();

        assert!(err.to_string().contains("signed_size=0.010000"));
        assert!(err.to_string().contains("min_order_shares=0.020000"));
    }

    #[test]
    fn pre_sign_simulation_rejects_cross_leg_unit_skew() {
        let cfg = Config::from_env();
        let mut first = live_order_leg_for_test();
        first.token_id = "1".into();
        first.size = 3.00;
        let mut second = live_order_leg_for_test();
        second.token_id = "2".into();
        second.size = 2.98;

        let err = normalize_legs_to_signed_order_values(&mut [first, second], &cfg).unwrap_err();

        assert!(err.to_string().contains("basket unit skew"));
    }

    #[test]
    fn order_type_parsing_accepts_immediate_types() {
        assert!(matches!(
            order_type_from_config("fok").unwrap(),
            OrderType::FOK
        ));
        assert!(matches!(
            order_type_from_config("fak").unwrap(),
            OrderType::FAK
        ));
    }

    #[test]
    fn empty_live_order_type_defaults_to_fok() {
        assert!(matches!(
            order_type_from_config("").unwrap(),
            OrderType::FOK
        ));
    }

    #[test]
    fn combo_rfq_execution_report_retains_exposure_for_recovery_states() {
        assert!(combo_rfq_execution_report_retains_exposure(
            Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::Accepted),
            &[]
        ));
        assert!(combo_rfq_execution_report_retains_exposure(
            Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::Unknown),
            &[]
        ));
        assert!(combo_rfq_execution_report_retains_exposure(
            Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::Unknown),
            &[]
        ));
        assert!(combo_rfq_execution_report_retains_exposure(
            None,
            &["exposure_must_remain_reserved_until_finality_or_manual_review".to_string()]
        ));
        assert!(!combo_rfq_execution_report_retains_exposure(None, &[]));
        assert!(!combo_rfq_execution_report_retains_exposure(
            Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::RejectedProven),
            &[]
        ));
        assert!(!combo_rfq_execution_report_retains_exposure(
            None,
            &["quote_notional_mismatch".to_string()]
        ));
    }

    #[test]
    fn combo_rfq_accepted_report_has_success_submit_semantics() {
        let mut report = crate::combo_rfq_client::ComboRfqExecutionReport {
            status: "accepted_pending_finality".into(),
            accept_outcome: Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::Accepted),
            request: None,
            rfq_id: Some("rfq-1".into()),
            quote_response: None,
            best_execution: crate::combo_rfq_client::ComboRfqBestExecutionReport {
                status: "ready_to_accept".into(),
                quotes_seen: 1,
                quotes_eligible: 1,
                selected_quote: None,
                maker_scorecard: crate::combo_rfq_client::ComboRfqMakerScorecard {
                    status: "ready".into(),
                    journal_path: String::new(),
                    records_seen: 0,
                    maker_count: 0,
                    min_terminal_samples: 0,
                    makers: Vec::new(),
                    error: None,
                },
                requester_ready: true,
                accept_enabled: true,
                edge_gate_pass: true,
                last_look_gate_pass: true,
                accept_gate_pass: true,
                blockers: Vec::new(),
                note: String::new(),
            },
            pre_accept_markout: None,
            accept_request: None,
            accept_response: None,
            blockers: vec!["exposure_must_remain_reserved_until_finality_or_manual_review".into()],
            steps: Vec::new(),
            note: String::new(),
        };

        assert!(combo_rfq_execution_report_was_accepted(&report));

        report.accept_outcome = Some(crate::combo_rfq_client::ComboRfqAcceptOutcome::Unknown);
        assert!(!combo_rfq_execution_report_was_accepted(&report));

        let err = combo_rfq_retained_exposure_error("event-1", &report);
        let text = err.to_string();

        assert!(text.contains("exposure retained"));
        assert!(text.contains("rfq-1"));
    }

    #[tokio::test]
    async fn sample_server_time_accepts_direct_canonical_response() {
        let server = MockServer::start_async().await;
        let time = server
            .mock_async(|when, then| {
                when.method(GET).path("/time");
                then.status(200).body("1700000000");
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.api_timeout_secs = 2;

        let sample = sample_server_time(&Client::new(), &cfg).await.unwrap();

        assert_eq!(sample.server_secs, 1_700_000_000);
        time.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn sample_server_time_rejects_redirected_host() {
        let source = MockServer::start_async().await;
        let target = MockServer::start_async().await;
        let source_time = source
            .mock_async(|when, then| {
                when.method(GET).path("/time");
                then.status(302)
                    .header("location", format!("{}/time", target.base_url()))
                    .body("");
            })
            .await;
        let target_time = target
            .mock_async(|when, then| {
                when.method(GET).path("/time");
                then.status(200).body("1700000000");
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = source.base_url();
        cfg.api_timeout_secs = 2;

        let err = sample_server_time(&Client::new(), &cfg).await.unwrap_err();

        assert!(err.to_string().contains("redirected"));
        source_time.assert_calls_async(1).await;
        target_time.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn sample_server_time_uses_live_freshness_timeout() {
        let server = MockServer::start_async().await;
        let _time = server
            .mock_async(|when, then| {
                when.method(GET).path("/time");
                then.status(200)
                    .delay(Duration::from_millis(25))
                    .body("1700000000");
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.api_timeout_secs = 2;
        cfg.live_max_refresh_to_submit_ms = 1;

        let err = sample_server_time(&Client::new(), &cfg).await.unwrap_err();

        assert!(err.to_string().contains("CLOB server time request failed"));
    }

    #[tokio::test]
    async fn server_clock_sync_respects_live_refresh_budget() {
        let server = MockServer::start_async().await;
        let _time = server
            .mock_async(|when, then| {
                when.method(GET).path("/time");
                then.status(200).body("1700000000");
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.api_timeout_secs = 2;
        cfg.live_max_refresh_to_submit_ms = 25;

        let started = Instant::now();
        let clock = ServerClock::sync(&Client::new(), &cfg).await.unwrap();

        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(clock.uncertainty_ms, 1_000);
    }

    #[test]
    fn live_server_clock_guard_blocks_excess_uncertainty_or_offset() {
        let mut cfg = Config::from_env();
        cfg.live_max_server_clock_uncertainty_ms = 250;
        cfg.live_max_server_clock_offset_ms = 5_000;

        assert!(ensure_live_server_clock_guard(
            &ServerClock {
                offset_ms: 500,
                uncertainty_ms: 100,
            },
            &cfg,
        )
        .is_ok());

        let err = ensure_live_server_clock_guard(
            &ServerClock {
                offset_ms: 500,
                uncertainty_ms: 251,
            },
            &cfg,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("LIVE_MAX_SERVER_CLOCK_UNCERTAINTY_MS"));

        let err = ensure_live_server_clock_guard(
            &ServerClock {
                offset_ms: -5_001,
                uncertainty_ms: 100,
            },
            &cfg,
        )
        .unwrap_err();
        assert!(err.to_string().contains("LIVE_MAX_SERVER_CLOCK_OFFSET_MS"));
    }

    #[test]
    fn live_basket_order_type_rejects_resting_or_partial_types() {
        assert!(live_basket_order_type_from_config("fok").is_ok());
        assert!(live_basket_order_type_from_config("gtc").is_err());
        assert!(live_basket_order_type_from_config("fak").is_err());
    }

    #[test]
    fn live_heartbeat_guard_uses_sdk_heartbeats_feature() {
        let _guard: fn(&ClobClient<Authenticated<Normal>>) -> Result<()> =
            ensure_live_heartbeats_active::<Normal>;
    }

    #[test]
    fn pre_submit_heartbeat_timeout_tracks_config() {
        let mut cfg = Config::from_env();
        cfg.live_pre_submit_heartbeat_enabled = true;
        cfg.live_pre_submit_heartbeat_timeout_ms = 250;
        assert_eq!(
            live_pre_submit_heartbeat_timeout(&cfg),
            Some(Duration::from_millis(250))
        );

        cfg.live_pre_submit_heartbeat_enabled = false;
        assert_eq!(live_pre_submit_heartbeat_timeout(&cfg), None);
    }

    #[tokio::test]
    async fn pre_submit_geoblock_uses_fresh_allow_cache() {
        let cfg = Config::from_env();
        let cache = Arc::new(tokio::sync::Mutex::new(Some(Instant::now())));
        let client = Client::builder()
            .no_proxy()
            .resolve("polymarket.com", "127.0.0.1:9".parse().unwrap())
            .build()
            .unwrap();

        ensure_live_pre_submit_geoblock(&client, &cfg, &cache)
            .await
            .unwrap();
    }

    #[test]
    fn live_edge_haircut_combines_fixed_and_cost_proportional_buffers() {
        let mut cfg = Config::from_env();
        cfg.live_edge_haircut_usd = 0.05;
        cfg.live_edge_haircut_bps = 25;

        let haircut = live_edge_haircut_usd(100.0, &cfg);

        assert!((haircut - 0.30).abs() < 1e-9);
    }

    #[test]
    fn live_edge_haircut_rejects_marginal_final_edge() {
        let mut cfg = Config::from_env();
        cfg.min_net_profit_usd = 1.0;
        cfg.min_roi_pct = 0.0;
        cfg.live_edge_haircut_usd = 0.05;
        cfg.live_edge_haircut_bps = 0;

        let err = ensure_live_edge_survives_haircut(100.0, 1.04, &cfg).unwrap_err();

        assert!(err.to_string().contains("final haircut"));
        assert!(err.to_string().contains("adjusted_pnl=$0.9900"));
    }

    #[test]
    fn live_edge_haircut_accepts_edge_after_buffer() {
        let mut cfg = Config::from_env();
        cfg.min_net_profit_usd = 1.0;
        cfg.min_roi_pct = 1.0;
        cfg.live_edge_haircut_usd = 0.05;
        cfg.live_edge_haircut_bps = 0;

        let (haircut, adjusted_pnl, adjusted_roi) =
            ensure_live_edge_survives_haircut(100.0, 2.05, &cfg).unwrap();

        assert!((haircut - 0.05).abs() < 1e-9);
        assert!((adjusted_pnl - 2.0).abs() < 1e-9);
        assert!((adjusted_roi - 2.0).abs() < 1e-9);
    }

    #[test]
    fn projected_metrics_for_legs_use_fresh_v2_fee_schedule() {
        let cfg = Config::from_env();
        let mut opp = executable_opp("1");
        opp.guaranteed_revenue = 1.0;
        opp.markets[0].clob_fee_rate = Some(0.0);
        let mut leg = live_order_leg_for_test();
        leg.price = 0.50;
        leg.size = 10.0;
        leg.fee_rate = 0.02;
        leg.fee_exponent = 2;

        let (_cost, fees, pnl, roi) =
            projected_trade_metrics_for_legs(&opp, &opp.markets, &[leg], 10.0, &cfg, 0.0).unwrap();

        assert!((fees - 0.0125).abs() < 1e-9);
        assert!((pnl - 4.9875).abs() < 1e-9);
        assert!((roi - 99.75).abs() < 1e-9);
    }

    #[tokio::test]
    async fn live_leg_fee_refresh_uses_compact_v2_metadata_not_legacy_fee_rate() {
        let server = MockServer::start_async().await;
        let market = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/C");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"C","t":[{"t":"1","o":"Yes"},{"t":"2","o":"No"}],"mts":0.01,"mos":1,"fd":{"r":0.02,"e":2,"to":true}}"#);
            })
            .await;
        let legacy = server
            .mock_async(|when, then| {
                when.method(GET).path("/fee-rate");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"base_fee":1000}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.max_retries = 1;
        let mut legs = vec![live_order_leg_for_test()];

        refresh_live_leg_fee_schedules(&Client::new(), &cfg, &mut legs)
            .await
            .expect("fresh V2 schedule");

        assert_eq!(legs[0].fee_rate, 0.02);
        assert_eq!(legs[0].fee_exponent, 2);
        market.assert_calls_async(1).await;
        legacy.assert_calls_async(0).await;
    }

    #[test]
    fn projected_metrics_use_pre_sign_normalized_values() {
        let cfg = Config::from_env();
        let mut opp = executable_opp("1");
        opp.guaranteed_revenue = 1.0;
        let mut leg = live_order_leg_for_test();
        leg.price = 0.4041;
        leg.size = 3.019;
        leg.fee_rate = 0.0;
        leg.fee_exponent = 2;
        let mut legs = vec![leg];
        let signed_units = normalize_legs_to_signed_order_values(&mut legs, &cfg).unwrap();

        let (cost, fees, pnl, roi) =
            projected_trade_metrics_for_legs(&opp, &opp.markets, &legs, signed_units, &cfg, 0.0)
                .unwrap();

        assert!((signed_units - 3.01).abs() < 1e-9);
        assert!((cost - 1.2341).abs() < 1e-9);
        assert_eq!(fees, 0.0);
        assert!((pnl - 1.7759).abs() < 1e-9);
        assert!((roi - (1.7759 / 1.2341 * 100.0)).abs() < 1e-9);
    }

    #[test]
    fn live_total_gas_cost_includes_estimated_closeout_gas() {
        assert!((live_total_gas_cost_usd(0.12, 0.03) - 0.15).abs() < 1e-9);
        assert!((live_total_gas_cost_usd(0.12, -1.0) - 0.12).abs() < 1e-9);
    }

    #[tokio::test]
    async fn live_required_gas_cost_blocks_fallback_oracle_source() {
        let client = Client::builder()
            .no_proxy()
            .resolve(
                "gasstation.polygon.technology",
                "127.0.0.1:9".parse().unwrap(),
            )
            .build()
            .unwrap();
        let cfg = Config::from_env();

        let err = required_live_trade_gas_cost_usd(&client, &cfg, 2, "entry")
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("requires a fresh Polygon gas/POL oracle"));
        assert!(err.to_string().contains("source=fallback"));
    }

    #[tokio::test]
    async fn live_required_gas_cost_allows_configured_gasless_proxy_mode() {
        let client = Client::builder()
            .no_proxy()
            .resolve(
                "gasstation.polygon.technology",
                "127.0.0.1:9".parse().unwrap(),
            )
            .build()
            .unwrap();
        let mut cfg = Config::from_env();
        cfg.assume_gasless_for_proxy_signature_types = true;
        cfg.live_signature_type = 1;

        let gas = required_live_trade_gas_cost_usd(&client, &cfg, 2, "entry")
            .await
            .unwrap();

        assert_eq!(gas, 0.0);
    }

    #[tokio::test]
    async fn combo_rfq_live_gas_costs_use_gasless_proxy_mode_without_network() {
        let client = Client::builder()
            .no_proxy()
            .resolve(
                "gasstation.polygon.technology",
                "127.0.0.1:9".parse().unwrap(),
            )
            .build()
            .unwrap();
        let mut cfg = Config::from_env();
        cfg.assume_gasless_for_proxy_signature_types = true;
        cfg.live_signature_type = 1;

        let (entry, closeout) = combo_rfq_live_gas_costs_usd(&client, &cfg, 2)
            .await
            .unwrap();

        assert_eq!(entry, 0.0);
        assert_eq!(closeout, 0.0);
    }

    #[test]
    fn live_route_support_matrix_fails_closed_when_empty() {
        assert!(!live_arbitrage_routes_available());
        let err = ensure_live_arbitrage_routes_available().unwrap_err();
        assert!(err
            .to_string()
            .contains("no live arbitrage route is currently supported"));
        assert!(err.to_string().contains("Combo/RFQ"));
    }

    #[tokio::test]
    async fn configured_live_routes_require_combo_rfq_promotion_when_flagged() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("configured-routes-combo-rfq-promotion");
        cfg.live_combo_rfq_route_enabled = true;

        let err = ensure_configured_live_arbitrage_routes_available(&cfg)
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("live Combo/RFQ route is enabled but not promoted"));
        assert!(err.to_string().contains("combo_rfq_closeout_execution"));
    }

    #[test]
    fn matched_order_status_is_not_final_live_success() {
        assert!(order_status_is_matched_unconfirmed(
            &OrderStatusType::Matched
        ));
        assert!(!order_status_is_confirmed(&OrderStatusType::Matched));
        assert!(!order_status_is_confirmed(&OrderStatusType::Unknown(
            "MINED".into()
        )));
        assert!(order_status_is_confirmed(&OrderStatusType::Unknown(
            "CONFIRMED".into()
        )));
        assert!(!order_status_is_confirmed(&OrderStatusType::Unknown(
            "UNCONFIRMED".into()
        )));
        assert!(!order_status_is_confirmed(&OrderStatusType::Unknown(
            "MATCHED_UNCONFIRMED".into()
        )));
        assert!(order_status_is_matched_unconfirmed(
            &OrderStatusType::Unknown("MATCHED_UNCONFIRMED".into())
        ));
        assert!(!order_status_is_matched_unconfirmed(
            &OrderStatusType::Unknown("UNMATCHED".into())
        ));
    }

    #[test]
    fn fok_post_order_status_gate_requires_immediate_fill_status() {
        assert!(fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Matched
        ));
        assert!(fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unknown("filled".into())
        ));
        assert!(fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unknown("trade_confirmed".into())
        ));
        assert!(fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unknown("fully-filled".into())
        ));

        assert!(!fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Live
        ));
        assert!(!fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Delayed
        ));
        assert!(!fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unmatched
        ));
        assert!(!fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unknown("accepted_pending".into())
        ));
        assert!(!fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unknown("unfilled".into())
        ));
        assert!(!fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unknown("partially_filled".into())
        ));
        assert!(!fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unknown("matched_unconfirmed".into())
        ));
        assert!(!fok_post_order_status_is_immediate_fill(
            &OrderStatusType::Unknown("unconfirmed".into())
        ));
    }

    #[test]
    fn fok_market_price_oracle_rejects_engine_price_above_signed_limit() {
        let limit = Decimal::from_str("0.51").unwrap();

        ensure_fok_market_price_not_above_limit("leg", Decimal::from_str("0.51").unwrap(), limit)
            .unwrap();
        ensure_fok_market_price_not_above_limit("leg", Decimal::from_str("0.50").unwrap(), limit)
            .unwrap();

        let err = ensure_fok_market_price_not_above_limit(
            "leg",
            Decimal::from_str("0.52").unwrap(),
            limit,
        )
        .unwrap_err();
        assert!(err.to_string().contains("market-price oracle rejected"));
    }

    #[test]
    fn terminal_failure_statuses_are_not_pending_success() {
        assert!(order_status_is_terminal_failure(&OrderStatusType::Canceled));
        assert!(order_status_is_terminal_failure(&OrderStatusType::Unknown(
            "FAILED".into()
        )));
        assert!(!order_status_is_terminal_failure(
            &OrderStatusType::Unknown("CONFIRMED".into())
        ));
    }

    fn temp_live_journal_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "polymarket-arb-scanner-live-journal-{name}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn live_process_lock_rejects_second_holder_and_releases_on_drop() {
        let dir = temp_live_journal_dir("process-lock");
        let account = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();

        let first = LiveProcessLock::acquire(&dir, account).unwrap();
        let lock_path = first.path().to_path_buf();
        assert!(lock_path.exists());
        let err = LiveProcessLock::acquire(&dir, account).unwrap_err();
        assert!(err
            .to_string()
            .contains("another live executor appears to hold account lock"));
        assert!(err.to_string().contains("account_address"));

        drop(first);
        assert!(!lock_path.exists());
        let second = LiveProcessLock::acquire(&dir, account).unwrap();
        assert!(second.path().exists());
    }

    #[test]
    fn closeout_safety_preflight_releases_lock_when_user_channel_not_ready() {
        let dir = temp_live_journal_dir("closeout-preflight-lock");
        let account = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.live_user_ws_enabled = false;

        let err = prepare_non_dry_run_closeout_execution(&cfg, account).unwrap_err();

        assert!(err.to_string().contains("user-channel tripwire"));
        let lock_path = dir.join(format!(
            "live_execution_{}.lock",
            account.to_string().to_ascii_lowercase()
        ));
        assert!(!lock_path.exists());
        assert!(LiveProcessLock::acquire(&dir, account).is_ok());
    }

    fn depth_snapshot_with_timestamp(venue_timestamp_ms: u64) -> clob_client::DepthSnapshot {
        clob_client::DepthSnapshot {
            token_id: "T".into(),
            asks: vec![(0.40, 10.0)],
            tick_size: Some(0.01),
            min_order_size: Some(1.0),
            neg_risk: Some(true),
            observed_at: Some(std::time::Instant::now()),
            venue_timestamp_ms: Some(venue_timestamp_ms),
            book_hash: Some("hash".into()),
        }
    }

    fn live_order_leg_for_test() -> LiveOrderLeg {
        LiveOrderLeg {
            market_index: 0,
            condition_id: "C".into(),
            token_id: "1".into(),
            side: Side::Buy,
            price: 0.41,
            raw_price: 0.40,
            size: 3.0,
            unit_shares: 1.0,
            tick_size: 0.01,
            question: "Q".into(),
            outcome: OutcomeSide::Yes,
            min_order_shares: 1.0,
            neg_risk: Some(true),
            fee_rate: 0.0,
            fee_exponent: 2,
            venue_timestamp_ms: Some(1_700_000_002_000),
            venue_age_ms: Some(87),
            book_hash: Some("h-a".into()),
        }
    }

    #[test]
    fn final_route_quote_coherence_accepts_complete_fresh_books() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 1_000;
        let mut second = live_order_leg_for_test();
        second.token_id = "2".into();
        second.venue_timestamp_ms = Some(1_700_000_002_500);
        second.venue_age_ms = Some(99);
        second.book_hash = Some("h-b".into());

        ensure_final_route_quote_coherent(&[live_order_leg_for_test(), second], &cfg).unwrap();
    }

    #[test]
    fn final_route_quote_coherence_rejects_missing_metadata_and_skew() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 100;

        let mut missing_hash = live_order_leg_for_test();
        missing_hash.book_hash = None;
        let err = ensure_final_route_quote_coherent(&[missing_hash], &cfg).unwrap_err();
        assert!(err.to_string().contains("missing_book_hashes=1"));

        let mut missing_timestamp = live_order_leg_for_test();
        missing_timestamp.venue_timestamp_ms = None;
        let err = ensure_final_route_quote_coherent(&[missing_timestamp], &cfg).unwrap_err();
        assert!(err.to_string().contains("missing_venue_timestamps=1"));

        let mut skewed = live_order_leg_for_test();
        skewed.token_id = "2".into();
        skewed.venue_timestamp_ms = Some(1_700_000_002_101);
        skewed.book_hash = Some("h-b".into());
        let err = ensure_final_route_quote_coherent(&[live_order_leg_for_test(), skewed], &cfg)
            .unwrap_err();
        assert!(err.to_string().contains("venue_timestamp_skew_ms=101"));
    }

    fn causal_watermark_cache(price: crate::ws_client::Price) -> PriceCache {
        Arc::new(tokio::sync::RwLock::new(HashMap::from([(
            "1".to_string(),
            price,
        )])))
    }

    #[tokio::test]
    async fn causal_watermark_allows_older_matching_ws_snapshot() {
        let leg = live_order_leg_for_test();
        let price = crate::ws_client::Price {
            venue_timestamp_ms: Some(1_700_000_001_000),
            book_hash: Some("h-a".into()),
            ..Default::default()
        };
        let cache = causal_watermark_cache(price);

        ensure_ws_causal_watermark_not_newer(Some(&cache), &[leg], Instant::now())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn causal_watermark_blocks_missing_cache_snapshot_or_timestamp() {
        let leg = live_order_leg_for_test();

        let err =
            ensure_ws_causal_watermark_not_newer(None, std::slice::from_ref(&leg), Instant::now())
                .await
                .unwrap_err();
        assert!(err.to_string().contains("price_cache_unavailable"));

        let empty_cache: PriceCache = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let err = ensure_ws_causal_watermark_not_newer(
            Some(&empty_cache),
            std::slice::from_ref(&leg),
            Instant::now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("missing_ws_snapshot:1"));

        let missing_ts_cache = causal_watermark_cache(crate::ws_client::Price {
            venue_timestamp_ms: None,
            book_hash: Some("h-a".into()),
            ..Default::default()
        });
        let err = ensure_ws_causal_watermark_not_newer(
            Some(&missing_ts_cache),
            std::slice::from_ref(&leg),
            Instant::now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("missing_ws_venue_timestamp:1"));

        let mut leg_without_rest_ts = leg.clone();
        leg_without_rest_ts.venue_timestamp_ms = None;
        let cache = causal_watermark_cache(crate::ws_client::Price {
            venue_timestamp_ms: Some(1_700_000_001_000),
            book_hash: Some("h-a".into()),
            ..Default::default()
        });
        let err = ensure_ws_causal_watermark_not_newer(
            Some(&cache),
            &[leg_without_rest_ts],
            Instant::now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("missing_rest_depth_timestamp:1"));
    }

    #[tokio::test]
    async fn causal_watermark_blocks_newer_ws_timestamp() {
        let leg = live_order_leg_for_test();
        let price = crate::ws_client::Price {
            venue_timestamp_ms: Some(1_700_000_003_000),
            book_hash: Some("h-a".into()),
            ..Default::default()
        };
        let cache = causal_watermark_cache(price);

        let err = ensure_ws_causal_watermark_not_newer(Some(&cache), &[leg], Instant::now())
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("causal watermark"));
        assert!(message.contains("ws_venue_timestamp_ms"));
    }

    #[tokio::test]
    async fn causal_watermark_blocks_same_timestamp_hash_mismatch() {
        let leg = live_order_leg_for_test();
        let price = crate::ws_client::Price {
            venue_timestamp_ms: leg.venue_timestamp_ms,
            book_hash: Some("h-b".into()),
            last_updated: Instant::now() - Duration::from_secs(1),
            ..Default::default()
        };
        let cache = causal_watermark_cache(price);

        let err = ensure_ws_causal_watermark_not_newer(Some(&cache), &[leg], Instant::now())
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("causal watermark"));
        assert!(message.contains("same timestamp"));
    }

    #[tokio::test]
    async fn causal_watermark_blocks_post_refresh_book_or_trade() {
        let leg = live_order_leg_for_test();
        let final_refresh_started_at = Instant::now() - Duration::from_millis(1);
        let price = crate::ws_client::Price {
            venue_timestamp_ms: Some(1_700_000_001_000),
            book_hash: Some("h-b".into()),
            ..Default::default()
        };
        let cache = causal_watermark_cache(price);

        let err = ensure_ws_causal_watermark_not_newer(
            Some(&cache),
            std::slice::from_ref(&leg),
            final_refresh_started_at,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("ws_book_hash"));

        let mut trade_price = crate::ws_client::Price {
            venue_timestamp_ms: Some(1_700_000_001_000),
            book_hash: Some("h-a".into()),
            ..Default::default()
        };
        trade_price
            .recent_trades
            .push_back(crate::ws_client::TradePrint {
                side: "BUY".into(),
                price: 0.41,
                size: 10.0,
                venue_timestamp_ms: Some(1_700_000_003_000),
                observed_at: Instant::now(),
            });
        let trade_cache = causal_watermark_cache(trade_price);

        let err = ensure_ws_causal_watermark_not_newer(
            Some(&trade_cache),
            &[leg],
            final_refresh_started_at,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("trade_print"));
    }

    fn market_for_shadow_test(condition_id: &str, question: &str) -> Market {
        Market {
            question: question.into(),
            condition_id: condition_id.into(),
            market_slug: question.to_ascii_lowercase().replace(' ', "-"),
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
            fees_enabled: Some(false),
            taker_fee_rate: None,
            maker_fee_rate: None,
            clob_taker_fee_bps: None,
            clob_fee_rate: Some(0.0),
            clob_fee_exponent: None,
            order_price_min_tick_size: Some(0.01),
            order_min_size: Some(1.0),
            clob_tick_size: Some(0.01),
            clob_min_order_size: Some(1.0),
            clob_neg_risk: Some(false),
            clob_rfq_enabled: None,
            liquidity: 10_000.0,
            closed: false,
        }
    }

    fn same_condition_bundle_opp_for_shadow_test() -> ArbitrageOpportunity {
        let mut yes = market_for_shadow_test("cond-a", "Yes leg");
        yes.clob_yes_ask = Some(0.49);
        yes.clob_yes_ask_size = Some(50.0);
        yes.clob_token_id_yes = "yes-token".into();
        yes.clob_neg_risk = Some(false);
        let mut no = market_for_shadow_test("cond-a", "No leg");
        no.question = "No leg".into();
        no.clob_no_ask = Some(0.49);
        no.clob_no_ask_size = Some(50.0);
        no.clob_token_id_no = "no-token".into();

        ArbitrageOpportunity {
            event_title: "Bundle".into(),
            event_id: "event-1".into(),
            category: "test".into(),
            arb_type: ArbType::Bundle,
            markets: vec![yes, no],
            execution_plan: vec![
                OpportunityLeg {
                    market_index: 0,
                    question: "Yes leg".into(),
                    market_slug: "yes".into(),
                    condition_id: "cond-a".into(),
                    token_id: "yes-token".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 1.0,
                    reference_price: 0.49,
                },
                OpportunityLeg {
                    market_index: 1,
                    question: "No leg".into(),
                    market_slug: "no".into(),
                    condition_id: "cond-a".into(),
                    token_id: "no-token".into(),
                    outcome: OutcomeSide::No,
                    unit_shares: 1.0,
                    reference_price: 0.49,
                },
            ],
            total_cost: 0.98,
            guaranteed_revenue: 1.0,
            gross_profit: 0.02,
            total_fees: 0.0,
            net_profit: 0.02,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 2.0,
            prices_from_clob: true,
            max_executable_size_usd: 10.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        }
    }

    fn shadow_report_for_calibration_test(event_id: &str) -> LiveRouteShadowReport {
        LiveRouteShadowReport {
            generated_at: "2026-01-01T00:00:00Z".into(),
            event_id: event_id.into(),
            event_title: "Calibration event".into(),
            route: CTF_MERGE_BUNDLE_SHADOW_ROUTE.into(),
            status: "blocked_no_submit".into(),
            stages: vec![
                "planned".into(),
                "priced".into(),
                "orphan_risk_evaluated".into(),
                "blocked_no_submit".into(),
            ],
            basket_units: 10.0,
            gross_edge_usd: 2.0,
            p_both_fill: 0.99,
            p_one_leg_fill: 0.01,
            p_ghost_revert: 0.01,
            orphan_closeout_loss_usd: 1.0,
            settlement_loss_usd: 10.0,
            latency_haircut_usd: 0.01,
            capital_lock_cost_usd: 0.01,
            toxicity_score: 0.05,
            calibrated_replay_samples: 0,
            risk_gate_pass: false,
            expected_shadow_ev_usd: 1.25,
            blockers: vec!["execution_risk_uncalibrated".into()],
        }
    }

    fn append_independent_realized_ev(config: &Config, execution_id: &str, realized_ev_usd: f64) {
        let path = config.diagnostics_dir.join(LIVE_REALIZED_PNL_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        serde_json::to_writer(
            &mut file,
            &serde_json::json!({
                "source": "combo_closeout_router",
                "execution_id": execution_id,
                "closeout_action_id": format!("closeout-{execution_id}"),
                "transaction_hash": format!("0x{execution_id}"),
                "block_number": 1,
                "status_class": "closeout_confirmed",
                "realized_ev_usd": realized_ev_usd,
            }),
        )
        .unwrap();
        writeln!(file).unwrap();
    }

    #[test]
    fn live_journal_blocks_unresolved_startup_state() {
        let dir = temp_live_journal_dir("dirty");
        let path = dir.join(LIVE_EXECUTION_JOURNAL_FILE);
        std::fs::write(
            &path,
            r#"{"execution_id":"exec-1","stage":"submit_unknown"}"#,
        )
        .unwrap();

        let err = match LiveExecutionJournal::new(&dir) {
            Ok(_) => panic!("dirty journal should block live startup"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("unresolved exposure/order state"));
        assert!(err.to_string().contains("exec-1:submit_unknown"));
    }

    #[test]
    fn live_journal_allows_manual_reconciled_state() {
        let dir = temp_live_journal_dir("clean");
        let path = dir.join(LIVE_EXECUTION_JOURNAL_FILE);
        std::fs::write(
            &path,
            concat!(
                r#"{"execution_id":"exec-1","stage":"submit_unknown"}"#,
                "\n",
                r#"{"execution_id":"exec-1","stage":"manual_reconciled"}"#,
                "\n"
            ),
        )
        .unwrap();

        assert!(LiveExecutionJournal::new(&dir).is_ok());
    }

    #[test]
    fn live_journal_record_writes_jsonl() {
        let dir = temp_live_journal_dir("write");
        let journal = LiveExecutionJournal::new(&dir).unwrap();
        let opp = executable_opp("1");
        let leg = live_order_leg_for_test();

        let record = live_journal_record(
            "exec-2",
            "submit_intent",
            &opp,
            &[leg],
            1.23,
            Some(LiveEntryAccounting {
                actual_fill_cost_usd: 1.20,
                entry_fees_usd: 0.02,
                entry_gas_cost_usd: 0.01,
            }),
            0.45,
            3.0,
            3.0,
            &[],
            &["trade-1".into()],
            &["0xabc".into()],
            None,
        );
        journal.record(&record).unwrap();

        let body = std::fs::read_to_string(dir.join(LIVE_EXECUTION_JOURNAL_FILE)).unwrap();
        assert!(body.contains(r#""execution_id":"exec-2""#));
        assert!(body.contains(r#""stage":"submit_intent""#));
        assert!(body.contains(r#""condition_id":"C""#));
        assert!(body.contains(r#""token_id":"1""#));
        assert!(body.contains(r#""venue_timestamp_ms":1700000002000"#));
        assert!(body.contains(r#""venue_age_ms":87"#));
        assert!(body.contains(r#""book_hash":"h-a""#));
        assert!(body.contains(r#""route_quote_snapshot":"#));
        assert!(body.contains(r#""refresh_id":"route-final-books:"#));
        assert!(body.contains(r#""token_ids":["1"]"#));
        assert!(body.contains(r#""venue_timestamp_min_ms":1700000002000"#));
        assert!(body.contains(r#""venue_timestamp_max_ms":1700000002000"#));
        assert!(body.contains(r#""venue_timestamp_skew_ms":0"#));
        assert!(body.contains(r#""max_venue_age_ms":87"#));
        assert!(body.contains(r#""missing_book_hashes":0"#));
        assert!(body.contains(r#""missing_venue_timestamps":0"#));
        assert!(body.contains(r#""actual_fill_cost_usd":1.2"#));
        assert!(body.contains(r#""entry_fees_usd":0.02"#));
        assert!(body.contains(r#""entry_gas_cost_usd":0.01"#));
        assert!(body.contains(r#""actual_entry_cost_usd":1.23"#));
        assert!(body.contains(r#""trade_ids":["trade-1"]"#));
        assert!(body.contains(r#""transaction_hashes":["0xabc"]"#));
    }

    #[test]
    fn live_journal_record_writes_expected_order_hashes() {
        let dir = temp_live_journal_dir("expected-order-hashes");
        let journal = LiveExecutionJournal::new(&dir).unwrap();
        let opp = executable_opp("1");
        let leg = live_order_leg_for_test();

        let record = live_journal_record_with_expected_order_hashes(
            "exec-hash",
            "submit_unknown",
            &opp,
            &[leg],
            1.23,
            None,
            0.45,
            3.0,
            3.0,
            &[],
            &["0xexpected".into()],
            &[],
            &[],
            Some("transport outcome unknown".into()),
        );
        journal.record(&record).unwrap();

        let body = std::fs::read_to_string(dir.join(LIVE_EXECUTION_JOURNAL_FILE)).unwrap();
        assert!(body.contains(r#""stage":"submit_unknown""#));
        assert!(body.contains(r#""expected_order_hashes":["0xexpected"]"#));
        assert!(body.contains(r#""error":"transport outcome unknown""#));
    }

    #[test]
    fn unresolved_journal_executions_by_condition_tracks_latest_stage() {
        let dir = temp_live_journal_dir("conditions");
        let path = dir.join(LIVE_EXECUTION_JOURNAL_FILE);
        std::fs::write(
            &path,
            concat!(
                r#"{"execution_id":"exec-1","stage":"fill_confirmed_exposure_retained","legs":[{"condition_id":"C1"}]}"#,
                "\n",
                r#"{"execution_id":"exec-2","stage":"fill_confirmed_exposure_retained","legs":[{"condition_id":"C1"},{"condition_id":"C2"}]}"#,
                "\n",
                r#"{"execution_id":"exec-2","stage":"manual_reconciled","legs":[{"condition_id":"C1"},{"condition_id":"C2"}]}"#,
                "\n"
            ),
        )
        .unwrap();

        let unresolved = unresolved_journal_executions_by_condition(&path).unwrap();

        assert_eq!(unresolved.get("C1").unwrap(), &vec!["exec-1".to_string()]);
        assert!(!unresolved.contains_key("C2"));
    }

    #[test]
    fn live_fill_reconciliation_accepts_exact_confirmed_order() {
        let leg = live_order_leg_for_test();

        validate_live_order_fill("order-1", &leg, "1", &Side::Buy, 3.0, 3.0, 0.41).unwrap();
    }

    #[test]
    fn live_fill_reconciliation_rejects_partial_or_wrong_fill() {
        let leg = live_order_leg_for_test();

        assert!(
            validate_live_order_fill("order-1", &leg, "2", &Side::Buy, 3.0, 3.0, 0.41)
                .unwrap_err()
                .to_string()
                .contains("token mismatch")
        );
        assert!(
            validate_live_order_fill("order-1", &leg, "1", &Side::Buy, 3.0, 2.99, 0.41)
                .unwrap_err()
                .to_string()
                .contains("remaining shares")
        );
        assert!(
            validate_live_order_fill("order-1", &leg, "1", &Side::Buy, 3.0, 3.0, 0.42)
                .unwrap_err()
                .to_string()
                .contains("price exceeds planned limit")
        );
    }

    #[test]
    fn authenticated_trade_reconciliation_requires_confirmed_taker_economics() {
        let mut leg = live_order_leg_for_test();
        leg.price = 0.40;
        leg.size = 20.0;
        leg.fee_rate = 0.02;
        leg.fee_exponent = 2;

        validate_authenticated_taker_trade(
            "order-1",
            &leg,
            "trade-1",
            "order-1",
            "1",
            &Side::Buy,
            &TraderSide::Taker,
            &TradeStatusType::Confirmed,
            20.0,
            0.40,
        )
        .unwrap();

        let err = validate_authenticated_taker_trade(
            "order-1",
            &leg,
            "trade-1",
            "other-order",
            "1",
            &Side::Buy,
            &TraderSide::Maker,
            &TradeStatusType::Confirmed,
            20.0,
            0.40,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not prove taker execution"));

        let mut accounting = LiveEntryAccounting::default();
        add_authenticated_fill_accounting(&mut accounting, &leg, 0.40, 20.0);
        assert!((accounting.actual_fill_cost_usd - 8.0).abs() < 1e-9);
        assert!((accounting.entry_fees_usd - 0.02304).abs() < 1e-9);
    }

    #[test]
    fn transaction_hash_merge_trims_dedupes_and_preserves_order() {
        let mut hashes = vec!["0xabc".to_string()];

        append_unique_transaction_hash(&mut hashes, " 0xABC ");
        append_unique_transaction_hash(&mut hashes, "");
        append_unique_transaction_hashes(&mut hashes, ["0xdef", " 0x123 ", "0xDEF"]);

        assert_eq!(hashes, vec!["0xabc", "0xdef", "0x123"]);
    }

    #[test]
    fn balance_allowance_preflight_requires_balance_and_exchange_allowance() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        let mut leg = live_order_leg_for_test();
        let required = required_collateral_spend_by_exchange(&cfg, &[leg.clone()]).unwrap();
        let exchange = contract_config(137, true).unwrap().exchange_v2.unwrap();
        assert_eq!(required.get(&exchange).unwrap().to_string(), "1.230000");
        leg.fee_rate = 0.02;
        leg.fee_exponent = 2;
        let required_with_fee = required_collateral_spend_by_exchange(&cfg, &[leg]).unwrap();
        assert_eq!(
            required_with_fee.get(&exchange).unwrap().to_string(),
            "1.233510"
        );

        let response = BalanceAllowanceResponse::builder()
            .balance(Decimal::from_str("2.00").unwrap())
            .allowances(HashMap::from([(exchange, "2.00".into())]))
            .build();
        ensure_balance_allowance_covers(&response, &required).unwrap();

        let low_balance = BalanceAllowanceResponse::builder()
            .balance(Decimal::from_str("1.00").unwrap())
            .allowances(HashMap::from([(exchange, "2.00".into())]))
            .build();
        assert!(ensure_balance_allowance_covers(&low_balance, &required)
            .unwrap_err()
            .to_string()
            .contains("insufficient collateral balance"));

        let low_allowance = BalanceAllowanceResponse::builder()
            .balance(Decimal::from_str("2.00").unwrap())
            .allowances(HashMap::from([(exchange, "1.00".into())]))
            .build();
        assert!(ensure_balance_allowance_covers(&low_allowance, &required)
            .unwrap_err()
            .to_string()
            .contains("insufficient collateral allowance"));
    }

    #[test]
    fn collateral_readiness_checks_report_balance_and_allowance_states() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        cfg.live_trade_position_size_usd = 25.0;
        cfg.combo_rfq_exchange_v3_address = "0x0000000000000000000000000000000000000003".into();
        let standard_exchange = contract_config(137, false).unwrap().exchange_v2.unwrap();
        let neg_risk_exchange = contract_config(137, true).unwrap().exchange_v2.unwrap();
        let exchange_v3 = Address::from_str(&cfg.combo_rfq_exchange_v3_address).unwrap();
        let response = BalanceAllowanceResponse::builder()
            .balance(Decimal::from_str("30.00").unwrap())
            .allowances(HashMap::from([
                (standard_exchange, "30.00".into()),
                (neg_risk_exchange, "10.00".into()),
                (exchange_v3, "30.00".into()),
            ]))
            .build();

        let checks = collateral_readiness_checks(&cfg, &response);

        assert_eq!(
            checks
                .iter()
                .find(|check| check.key == "pusd_balance")
                .map(|check| check.state),
            Some(LiveReadinessState::Ready)
        );
        assert_eq!(
            checks
                .iter()
                .find(|check| check.key == "pusd_allowance_exchange_v2_standard")
                .map(|check| check.state),
            Some(LiveReadinessState::Ready)
        );
        assert_eq!(
            checks
                .iter()
                .find(|check| check.key == "pusd_allowance_exchange_v2_neg_risk")
                .map(|check| check.state),
            Some(LiveReadinessState::Blocked)
        );
        assert_eq!(
            checks
                .iter()
                .find(|check| check.key == "exchange_v3_allowance")
                .map(|check| check.state),
            Some(LiveReadinessState::Ready)
        );
    }

    #[test]
    fn readiness_guard_blocks_unknown_and_blocked_states() {
        let checks = vec![
            LiveReadinessCheck::ready("ready_probe", "ok"),
            LiveReadinessCheck::unknown("unknown_probe", "no_recent_sample"),
            LiveReadinessCheck::blocked("blocked_probe", "insufficient_allowance"),
        ];

        let err = ensure_readiness_checks_ready("pre-submit guard", &checks).unwrap_err();
        let message = err.to_string();

        assert!(message.contains("pre-submit guard blocked"));
        assert!(message.contains("unknown_probe:no_recent_sample"));
        assert!(message.contains("blocked_probe:insufficient_allowance"));
        assert!(!message.contains("ready_probe:ok"));
    }

    #[test]
    fn market_data_config_readiness_requires_clob_rest_and_ws() {
        let mut cfg = Config::from_env();
        cfg.clob_api_url.clear();
        cfg.clob_ws_url.clear();

        let check = market_data_config_readiness_check(&cfg);

        assert_eq!(check.state, LiveReadinessState::Blocked);
        assert!(check.detail.contains("CLOB_API_URL_empty"));
        assert!(check.detail.contains("CLOB_WS_URL_empty"));
    }

    #[test]
    fn market_data_config_readiness_blocks_stale_ws_window() {
        let mut cfg = Config::from_env();
        cfg.ws_quote_max_age_ms = cfg.live_max_refresh_to_submit_ms + 1;

        let check = market_data_config_readiness_check(&cfg);

        assert_eq!(check.state, LiveReadinessState::Blocked);
        assert!(check
            .detail
            .contains("WS_QUOTE_MAX_AGE_MS_exceeds_LIVE_MAX_REFRESH_TO_SUBMIT_MS"));
    }

    #[test]
    fn combo_rfq_collateral_guard_requires_exchange_v3_allowance() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        cfg.live_trade_position_size_usd = 25.0;
        cfg.combo_rfq_exchange_v3_address = "0x0000000000000000000000000000000000000003".into();
        let standard_exchange = contract_config(137, false).unwrap().exchange_v2.unwrap();
        let neg_risk_exchange = contract_config(137, true).unwrap().exchange_v2.unwrap();
        let response = BalanceAllowanceResponse::builder()
            .balance(Decimal::from_str("30.00").unwrap())
            .allowances(HashMap::from([
                (standard_exchange, "30.00".into()),
                (neg_risk_exchange, "30.00".into()),
            ]))
            .build();
        let checks = collateral_readiness_checks(&cfg, &response);

        let err =
            ensure_readiness_checks_ready("Combo/RFQ pre-submit collateral", &checks).unwrap_err();
        let message = err.to_string();

        assert!(message.contains("Combo/RFQ pre-submit collateral blocked"));
        assert!(message.contains("exchange_v3_allowance"));
    }

    #[tokio::test]
    async fn exchange_v3_allowance_rpc_probe_reads_onchain_allowance() {
        let server = MockServer::start_async().await;
        let allowance_units = format!("0x{:064x}", 30_000_000u64);
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes("\"method\":\"eth_call\"");
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": allowance_units
                }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        cfg.polygon_rpc_url = server.base_url();
        cfg.combo_rfq_exchange_v3_address = "0x0000000000000000000000000000000000000003".into();
        let account = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let required = Decimal::from_str("25.00").unwrap();

        let check = exchange_v3_allowance_rpc_readiness_check(&cfg, account, &required).await;

        assert_eq!(check.state, LiveReadinessState::Ready, "{}", check.detail);
        assert!(check.detail.contains("allowance=30.000000"));
        assert!(check.detail.contains("source=polygon_rpc"));
        rpc.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn erc1155_operator_approval_rpc_probe_reads_onchain_approval() {
        let server = MockServer::start_async().await;
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes("\"method\":\"eth_call\"");
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": format!("0x{:064x}", 1u64)
                }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        cfg.polygon_rpc_url = server.base_url();
        let account = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let exchange = Address::from_str("0x0000000000000000000000000000000000000003").unwrap();

        let check = erc1155_operator_approval_rpc_readiness_check(&cfg, account, exchange).await;

        assert_eq!(check.state, LiveReadinessState::Ready, "{}", check.detail);
        assert!(check.detail.contains("isApprovedForAll=true"));
        assert!(check.detail.contains("source=polygon_rpc"));
        rpc.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn combo_position_manager_operator_approval_checks_position_manager() {
        let server = MockServer::start_async().await;
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes("\"method\":\"eth_call\"");
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": format!("0x{:064x}", 1u64)
                }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        cfg.polygon_rpc_url = server.base_url();
        cfg.live_signature_type = 3;
        cfg.live_funder_address = "0x0000000000000000000000000000000000000001".into();
        std::env::set_var(
            PRIVATE_KEY_VAR,
            "0000000000000000000000000000000000000000000000000000000000000001",
        );

        let check =
            combo_rfq_position_manager_operator_approval_promotion_readiness_check(&cfg).await;

        assert_eq!(
            check.key,
            "combo_position_manager_erc1155_operator_approval"
        );
        assert_eq!(check.state, LiveReadinessState::Ready, "{}", check.detail);
        assert!(check.detail.contains("isApprovedForAll=true"));
        assert!(check.detail.contains(POLYGON_COMBO_POSITION_MANAGER));
        rpc.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn erc1155_operator_approval_readiness_checks_exchange_v2_operators() {
        let server = MockServer::start_async().await;
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes("\"method\":\"eth_call\"");
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": format!("0x{:064x}", 1u64)
                }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        cfg.polygon_rpc_url = server.base_url();
        let account = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();

        let check = erc1155_operator_approval_readiness_check(&cfg, account).await;

        assert_eq!(check.key, "erc1155_operator_approval");
        assert_eq!(check.state, LiveReadinessState::Ready, "{}", check.detail);
        assert!(check.detail.contains("isApprovedForAll=true"));
        assert!(check.detail.contains("exchange_v2_standard"));
        assert!(check.detail.contains("source=polygon_rpc"));
        assert!(rpc.calls_async().await >= 1);
    }

    #[test]
    fn shadow_route_report_blocks_same_condition_bundle_without_submit() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("shadow-no-calibration");
        cfg.live_trade_position_size_usd = 10.0;
        cfg.live_slippage_bps = 10;
        let opp = same_condition_bundle_opp_for_shadow_test();

        let report =
            build_live_route_shadow_report(&cfg, &opp, LiveRouteKind::CtfMergeBundleCandidate)
                .expect("shadow report");

        assert_eq!(report.status, "blocked_no_submit");
        assert!(report.stages.contains(&"orphan_risk_evaluated".to_string()));
        assert!(report.p_both_fill > 0.99);
        assert_eq!(report.calibrated_replay_samples, 0);
        assert!(!report.risk_gate_pass);
        assert!(report
            .blockers
            .contains(&"execution_risk_uncalibrated".to_string()));
        assert!(report.p_ghost_revert > 0.0);
        assert!(report.expected_shadow_ev_usd.is_finite());
        assert!(!live_arbitrage_routes_available());
    }

    #[test]
    fn shadow_route_report_uses_opportunity_capital_lock_hours() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("shadow-lock-hours");
        cfg.live_trade_position_size_usd = 10.0;
        cfg.live_slippage_bps = 10;
        cfg.capital_velocity_reference_hours = 240.0;
        let mut opp = same_condition_bundle_opp_for_shadow_test();
        opp.capital_lock_hours = Some(6.0);

        let report =
            build_live_route_shadow_report(&cfg, &opp, LiveRouteKind::CtfMergeBundleCandidate)
                .expect("shadow report");

        let expected = cfg.live_trade_position_size_usd * (6.0 / (24.0 * 365.0)) * 0.10;
        assert!((report.capital_lock_cost_usd - expected).abs() < 1e-9);
        let fallback = cfg.live_trade_position_size_usd
            * (cfg.capital_velocity_reference_hours / (24.0 * 365.0))
            * 0.10;
        assert!(report.capital_lock_cost_usd < fallback);
    }

    #[test]
    fn shadow_route_report_uses_passing_replay_calibration_probabilities() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("shadow-with-calibration");
        cfg.live_trade_position_size_usd = 10.0;
        cfg.live_slippage_bps = 10;
        cfg.live_route_calibration_min_samples = 3;
        cfg.min_net_profit_usd = 0.05;
        for idx in 0..3 {
            let execution_id = format!("event-{idx}");
            append_independent_realized_ev(&cfg, &execution_id, 1.50);
            append_live_route_replay_record(
                &cfg,
                &LiveRouteReplayRecord {
                    label_id: None,
                    generated_at: Utc::now().to_rfc3339(),
                    event_id: execution_id.clone(),
                    route: CTF_MERGE_BUNDLE_SHADOW_ROUTE.into(),
                    outcome_label: "both_confirmed".into(),
                    realized_ev_usd: Some(1.50),
                    toxicity_score: Some(0.10),
                    notes: vec![format!("execution_id={execution_id}")],
                },
            )
            .unwrap();
        }
        let opp = same_condition_bundle_opp_for_shadow_test();

        let report =
            build_live_route_shadow_report(&cfg, &opp, LiveRouteKind::CtfMergeBundleCandidate)
                .expect("shadow report");

        assert_eq!(report.calibrated_replay_samples, 3);
        assert_eq!(report.p_both_fill, 1.0);
        assert_eq!(report.p_one_leg_fill, 0.0);
        assert_eq!(report.p_ghost_revert, 0.0);
        assert!(report.risk_gate_pass);
        assert!(report.blockers.is_empty());
        assert!(report.expected_shadow_ev_usd > cfg.min_net_profit_usd);
    }

    #[test]
    fn shadow_toxicity_score_penalizes_imbalanced_fill_capacity() {
        let balanced = shadow_toxicity_score(1.0, 1.0, 0.0);
        let imbalanced = shadow_toxicity_score(1.0, 0.2, 0.0);

        assert!(balanced < 0.01);
        assert!(imbalanced > 0.5);
    }

    #[test]
    fn live_route_calibration_report_fails_closed_when_shadow_journal_is_unlabeled() {
        let dir = temp_live_journal_dir("route-calibration-unlabeled");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_route_calibration_min_samples = 2;
        let shadow = shadow_report_for_calibration_test("event-1");
        append_live_route_shadow_report(&cfg, &shadow).unwrap();

        let report = build_live_route_calibration_report(&cfg).unwrap();

        assert_eq!(report.shadow_samples, 1);
        assert_eq!(report.labeled_replay_samples, 0);
        assert!(!report.risk_gate_pass);
        assert!(report
            .blockers
            .contains(&"missing_finality_labels".to_string()));
        let bucket = report
            .routes
            .iter()
            .find(|bucket| bucket.route == "ctf_merge_bundle_shadow")
            .unwrap();
        assert_eq!(bucket.shadow_samples, 1);
        assert_eq!(bucket.labeled_samples, 0);
        assert_eq!(bucket.realized_ev_samples, 0);
        assert!(!bucket.risk_gate_pass);
        assert!(bucket
            .blockers
            .contains(&"shadow_reports_unlabeled".to_string()));
        assert!(bucket
            .blockers
            .contains(&"missing_realized_ev_labels".to_string()));
    }

    #[test]
    fn live_route_calibration_passes_with_enough_clean_profitable_replay_labels() {
        let dir = temp_live_journal_dir("route-calibration-clean");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_route_calibration_min_samples = 3;
        cfg.min_net_profit_usd = 1.0;
        for idx in 0..3 {
            let execution_id = format!("event-{idx}");
            append_independent_realized_ev(&cfg, &execution_id, 1.50);
            append_live_route_replay_record(
                &cfg,
                &LiveRouteReplayRecord {
                    label_id: None,
                    generated_at: Utc::now().to_rfc3339(),
                    event_id: execution_id.clone(),
                    route: "ctf_merge_bundle_shadow".into(),
                    outcome_label: "both_confirmed".into(),
                    realized_ev_usd: Some(1.50),
                    toxicity_score: Some(0.10),
                    notes: vec![format!("execution_id={execution_id}")],
                },
            )
            .unwrap();
        }

        let path = write_live_route_calibration_report(&cfg).unwrap();
        let report = build_live_route_calibration_report(&cfg).unwrap();

        assert!(path.exists());
        assert_eq!(report.labeled_replay_samples, 3);
        assert!(report.risk_gate_pass);
        let bucket = &report.routes[0];
        assert!(bucket.risk_gate_pass);
        assert_eq!(bucket.realized_ev_samples, 3);
        assert_eq!(bucket.p_both_fill_observed, 1.0);
        assert_eq!(bucket.p_one_leg_fill_observed, 0.0);
        assert_eq!(bucket.p_ghost_revert_observed, 0.0);
        assert_eq!(bucket.avg_realized_ev_usd, Some(1.50));
        assert!(bucket.blockers.is_empty());
    }

    #[test]
    fn live_route_calibration_requires_enough_independent_realized_ev_samples() {
        let dir = temp_live_journal_dir("route-calibration-sparse-realized-ev");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_route_calibration_min_samples = 100;
        cfg.min_net_profit_usd = 1.0;
        for idx in 0..100 {
            let execution_id = format!("execution-{idx}");
            if idx == 0 {
                append_independent_realized_ev(&cfg, &execution_id, 2.0);
            }
            append_live_route_replay_record(
                &cfg,
                &LiveRouteReplayRecord {
                    label_id: Some(format!("label-{idx}")),
                    generated_at: Utc::now().to_rfc3339(),
                    event_id: format!("event-{idx}"),
                    route: COMBO_RFQ_ROUTE.into(),
                    outcome_label: "both_confirmed".into(),
                    realized_ev_usd: (idx == 0).then_some(2.0),
                    toxicity_score: Some(0.05),
                    notes: vec![format!("execution_id={execution_id}")],
                },
            )
            .unwrap();
        }

        let report = build_live_route_calibration_report(&cfg).unwrap();
        let bucket = &report.routes[0];

        assert_eq!(bucket.labeled_samples, 100);
        assert_eq!(bucket.realized_ev_samples, 1);
        assert!(!bucket.risk_gate_pass);
        assert!(bucket
            .blockers
            .contains(&"insufficient_realized_ev_samples:1/100".to_string()));
    }

    #[test]
    fn live_route_calibration_does_not_trust_replay_reported_ev() {
        let dir = temp_live_journal_dir("route-calibration-untrusted-realized-ev");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_route_calibration_min_samples = 1;
        cfg.min_net_profit_usd = 1.0;
        append_live_route_replay_record(
            &cfg,
            &LiveRouteReplayRecord {
                label_id: Some("label-external-ev".into()),
                generated_at: Utc::now().to_rfc3339(),
                event_id: "event-external-ev".into(),
                route: COMBO_RFQ_ROUTE.into(),
                outcome_label: "both_confirmed".into(),
                realized_ev_usd: Some(10_000.0),
                toxicity_score: Some(0.05),
                notes: vec!["execution_id=execution-without-ledger-pnl".into()],
            },
        )
        .unwrap();

        let report = build_live_route_calibration_report(&cfg).unwrap();
        let bucket = &report.routes[0];

        assert_eq!(bucket.realized_ev_samples, 0);
        assert_eq!(bucket.avg_realized_ev_usd, None);
        assert!(!bucket.risk_gate_pass);
        assert!(bucket
            .blockers
            .contains(&"insufficient_realized_ev_samples:0/1".to_string()));
    }

    #[test]
    fn live_route_calibration_blocks_reported_ev_mismatch() {
        let dir = temp_live_journal_dir("route-calibration-realized-ev-mismatch");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_route_calibration_min_samples = 1;
        cfg.min_net_profit_usd = 1.0;
        append_independent_realized_ev(&cfg, "execution-1", 2.0);
        append_live_route_replay_record(
            &cfg,
            &LiveRouteReplayRecord {
                label_id: Some("label-mismatch".into()),
                generated_at: Utc::now().to_rfc3339(),
                event_id: "event-mismatch".into(),
                route: COMBO_RFQ_ROUTE.into(),
                outcome_label: "both_confirmed".into(),
                realized_ev_usd: Some(3.0),
                toxicity_score: Some(0.05),
                notes: vec!["execution_id=execution-1".into()],
            },
        )
        .unwrap();

        let report = build_live_route_calibration_report(&cfg).unwrap();
        let bucket = &report.routes[0];

        assert_eq!(bucket.realized_ev_samples, 1);
        assert_eq!(bucket.avg_realized_ev_usd, Some(2.0));
        assert!(!bucket.risk_gate_pass);
        assert!(bucket
            .blockers
            .contains(&"realized_ev_mismatch_labels:1".to_string()));
    }

    #[test]
    fn live_route_calibration_rejects_malformed_independent_pnl() {
        let dir = temp_live_journal_dir("route-calibration-malformed-realized-ev");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        fs::write(
            dir.join(LIVE_REALIZED_PNL_FILE),
            r#"{"source":"combo_closeout_router","execution_id":"execution-1","realized_ev_usd":"not-a-number"}"#,
        )
        .unwrap();

        let err = build_live_route_calibration_report(&cfg).unwrap_err();

        assert!(err.to_string().contains("malformed JSON"));
    }

    #[test]
    fn live_route_calibration_rejects_unproven_legacy_pnl() {
        let dir = temp_live_journal_dir("route-calibration-unproven-legacy-pnl");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        fs::write(
            dir.join(LIVE_REALIZED_PNL_FILE),
            r#"{"execution_id":"execution-1","realized_pnl_usd":2.0}"#,
        )
        .unwrap();

        let err = build_live_route_calibration_report(&cfg).unwrap_err();

        assert!(err
            .to_string()
            .contains("lacks receipt-derived closeout proof"));
    }

    #[test]
    fn live_route_calibration_recomputes_receipt_derived_legacy_pnl() {
        let dir = temp_live_journal_dir("route-calibration-valid-legacy-pnl");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.live_route_calibration_min_samples = 1;
        cfg.min_net_profit_usd = 1.0;
        fs::write(
            dir.join(LIVE_REALIZED_PNL_FILE),
            concat!(
                r#"{"execution_id":"execution-1","closeout_action_id":"action-1","transaction_hash":"0xabc","block_number":1,"allocated_p_usd_delta_usd":4.0,"actual_entry_cost_usd":1.5,"allocated_closeout_gas_cost_usd":0.5,"realized_pnl_usd":2.0,"receipt_total_logs":2,"receipt_adapter_logs":0,"receipt_collateral_transfer_to_account_logs":1,"receipt_ctf_transfer_logs":1}"#,
                "\n"
            ),
        )
        .unwrap();
        append_live_route_replay_record(
            &cfg,
            &LiveRouteReplayRecord {
                label_id: Some("label-1".into()),
                generated_at: Utc::now().to_rfc3339(),
                event_id: "event-1".into(),
                route: COMBO_RFQ_ROUTE.into(),
                outcome_label: "both_confirmed".into(),
                realized_ev_usd: Some(2.0),
                toxicity_score: Some(0.05),
                notes: vec!["execution_id=execution-1".into()],
            },
        )
        .unwrap();

        let report = build_live_route_calibration_report(&cfg).unwrap();
        let bucket = &report.routes[0];

        assert_eq!(bucket.realized_ev_samples, 1);
        assert_eq!(bucket.avg_realized_ev_usd, Some(2.0));
        assert!(bucket.risk_gate_pass);
    }

    #[test]
    fn live_route_calibration_ignores_stale_replay_labels_for_gate() {
        let dir = temp_live_journal_dir("route-calibration-stale-labels");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_route_calibration_min_samples = 2;
        cfg.live_route_calibration_max_age_secs = 1;
        cfg.min_net_profit_usd = 1.0;
        for idx in 0..2 {
            append_live_route_replay_record(
                &cfg,
                &LiveRouteReplayRecord {
                    label_id: None,
                    generated_at: "2020-01-01T00:00:00Z".into(),
                    event_id: format!("event-{idx}"),
                    route: "ctf_merge_bundle_shadow".into(),
                    outcome_label: "both_confirmed".into(),
                    realized_ev_usd: Some(1.50),
                    toxicity_score: Some(0.10),
                    notes: Vec::new(),
                },
            )
            .unwrap();
        }

        let report = build_live_route_calibration_report(&cfg).unwrap();

        assert_eq!(report.labeled_replay_samples, 0);
        assert!(!report.risk_gate_pass);
        let bucket = &report.routes[0];
        assert_eq!(bucket.labeled_samples, 0);
        assert!(bucket
            .blockers
            .iter()
            .any(|blocker| blocker.starts_with("stale_labeled_samples:")));
        assert!(bucket
            .blockers
            .contains(&"missing_finality_labels".to_string()));
    }

    #[tokio::test]
    async fn combo_rfq_route_promotion_report_fails_closed_by_default() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("combo-rfq-promotion-default");

        let report = build_combo_rfq_route_promotion_report(&cfg).await;

        assert_eq!(report.route, COMBO_RFQ_ROUTE);
        assert!(!report.promoted);
        assert!(report
            .checks
            .iter()
            .any(|check| check.key == "combo_rfq_requester"
                && check.state == LiveReadinessState::Blocked));
        assert!(report
            .checks
            .iter()
            .any(|check| check.key == "combo_rfq_accept_gate"
                && check.state == LiveReadinessState::Blocked));
        assert!(report.checks.iter().any(|check| {
            check.key == "combo_rfq_closeout_execution"
                && check.state == LiveReadinessState::Blocked
        }));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("missing_combo_rfq_calibration_bucket")));
    }

    #[tokio::test]
    async fn live_readiness_names_combo_rfq_route_blockers_when_route_flag_is_off() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("live-readiness-combo-rfq-blockers");
        cfg.live_combo_rfq_route_enabled = false;

        let report = build_live_readiness_report(&cfg).await;

        assert!(!report.live_submissions_supported);
        let route_matrix = report
            .checks
            .iter()
            .find(|check| check.key == "live_route_matrix")
            .expect("live route matrix check");
        assert_eq!(route_matrix.state, LiveReadinessState::Blocked);
        assert!(route_matrix.detail.contains("combo_rfq_route_not_promoted"));
        assert!(route_matrix.detail.contains("live_route_support_code"));
        assert!(!route_matrix
            .detail
            .contains("no_live_arbitrage_routes_supported"));
    }

    #[test]
    fn combo_rfq_promotion_cache_reuses_promoted_report_for_same_submit_window() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("combo-rfq-promotion-cache");
        cfg.live_max_refresh_to_submit_ms = 20;
        let report = ComboRfqRoutePromotionReport {
            generated_at: Utc::now().to_rfc3339(),
            route: COMBO_RFQ_ROUTE.into(),
            promoted: true,
            checks: vec![LiveReadinessCheck::ready("cache_test", "ready")],
            blockers: Vec::new(),
        };
        if let Ok(mut guard) = combo_rfq_route_promotion_cache().lock() {
            *guard = None;
        }

        store_combo_rfq_route_promotion_report(&cfg, report.clone());

        assert_eq!(cached_combo_rfq_route_promotion_report(&cfg), Some(report));

        let mut changed_cfg = cfg.clone();
        changed_cfg.live_trade_position_size_usd += 1.0;
        assert!(cached_combo_rfq_route_promotion_report(&changed_cfg).is_none());
        assert_eq!(
            combo_rfq_route_promotion_cache_ttl(&cfg),
            Duration::from_millis(20)
        );

        if let Ok(mut guard) = combo_rfq_route_promotion_cache().lock() {
            *guard = None;
        }
    }

    #[tokio::test]
    async fn combo_rfq_route_promotion_requires_finality_and_allowance_even_after_calibration() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("combo-rfq-promotion-calibrated");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_route_calibration_min_samples = 2;
        cfg.min_net_profit_usd = 1.0;
        for idx in 0..2 {
            let execution_id = format!("rfq-event-{idx}");
            append_independent_realized_ev(&cfg, &execution_id, 2.0);
            append_live_route_replay_record(
                &cfg,
                &LiveRouteReplayRecord {
                    label_id: Some(format!("rfq-label-{idx}")),
                    generated_at: Utc::now().to_rfc3339(),
                    event_id: execution_id.clone(),
                    route: COMBO_RFQ_ROUTE.into(),
                    outcome_label: "both_confirmed".into(),
                    realized_ev_usd: Some(2.0),
                    toxicity_score: Some(0.05),
                    notes: vec![format!("execution_id={execution_id}")],
                },
            )
            .unwrap();
        }

        let report = build_combo_rfq_route_promotion_report(&cfg).await;

        assert!(!report.promoted);
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.key == "combo_rfq_requester")
                .map(|check| check.state),
            Some(LiveReadinessState::Ready)
        );
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.key == "combo_rfq_replay_calibration")
                .map(|check| check.state),
            Some(LiveReadinessState::Ready)
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("exchange_v3_allowance")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("exchange_v3_erc1155_operator_approval")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("rfq_finality_stream")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("rfq_stream_client:stream_not_ready")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_rfq_closeout_execution")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("live_route_support_code")));
    }

    #[tokio::test]
    async fn combo_rfq_route_promotion_ingests_finality_events_before_checking() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("combo-rfq-promotion-ingests-finality");
        fs::create_dir_all(&cfg.diagnostics_dir).unwrap();
        fs::write(
            cfg.diagnostics_dir
                .join(crate::rfq_finality::COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-ingest","quoteId":"quote-ingest","makerId":"maker-ingest","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-reject","rfqId":"rfq-ingest","quoteId":"quote-ingest","makerId":"maker-ingest","status":"quote_done_away","realizedEvUsd":-0.1}"#,
                "\n"
            ),
        )
        .unwrap();

        let report = build_combo_rfq_route_promotion_report(&cfg).await;

        assert!(!report.promoted);
        let journal = fs::read_to_string(
            cfg.diagnostics_dir
                .join(crate::rfq_finality::COMBO_RFQ_FINALITY_JOURNAL_FILE),
        )
        .unwrap();
        assert!(journal.contains("rfq-ingest"));
        let finality_check = report
            .checks
            .iter()
            .find(|check| check.key == "rfq_finality_stream")
            .expect("rfq finality readiness check");
        assert!(finality_check.detail.contains("records=2"));
    }

    #[tokio::test]
    async fn combo_rfq_route_promotion_support_code_requires_explicit_route_flag() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("combo-rfq-promotion-flagged");
        cfg.live_combo_rfq_route_enabled = true;
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_route_calibration_min_samples = 1;
        cfg.min_net_profit_usd = 1.0;
        append_independent_realized_ev(&cfg, "rfq-event", 2.0);
        append_live_route_replay_record(
            &cfg,
            &LiveRouteReplayRecord {
                label_id: Some("rfq-label".into()),
                generated_at: Utc::now().to_rfc3339(),
                event_id: "rfq-event".into(),
                route: COMBO_RFQ_ROUTE.into(),
                outcome_label: "both_confirmed".into(),
                realized_ev_usd: Some(2.0),
                toxicity_score: Some(0.05),
                notes: vec!["execution_id=rfq-event".into()],
            },
        )
        .unwrap();

        let report = build_combo_rfq_route_promotion_report(&cfg).await;

        assert!(!report.promoted);
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.key == "live_route_support_code")
                .map(|check| check.state),
            Some(LiveReadinessState::Ready)
        );
        assert!(!report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("live_route_support_code")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_rfq_closeout_execution")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("exchange_v3_allowance")));
    }

    #[tokio::test]
    async fn combo_rfq_route_promotion_blocks_unverified_requester_protocol() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("combo-rfq-requester-protocol");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();

        let report = build_combo_rfq_route_promotion_report(&cfg).await;

        let check = report
            .checks
            .iter()
            .find(|check| check.key == "combo_rfq_requester_protocol")
            .expect("requester protocol readiness check");
        assert_eq!(check.state, LiveReadinessState::Blocked);
        assert!(check
            .detail
            .contains("COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED=false"));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_rfq_requester_protocol")));
    }

    #[tokio::test]
    async fn combo_rfq_route_promotion_allows_verified_requester_protocol_gate() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("combo-rfq-requester-protocol-verified");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_protocol_verified = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();

        let report = build_combo_rfq_route_promotion_report(&cfg).await;

        let check = report
            .checks
            .iter()
            .find(|check| check.key == "combo_rfq_requester_protocol")
            .expect("requester protocol readiness check");
        assert_eq!(check.state, LiveReadinessState::Ready);
        assert!(!report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_rfq_requester_protocol")));
        assert!(!report.promoted);
    }

    #[test]
    fn combo_rfq_closeout_promotion_check_requires_non_dry_run_wallet_path() {
        let mut cfg = Config::from_env();

        let disabled = combo_rfq_closeout_execution_promotion_readiness_check(&cfg);
        assert_eq!(disabled.state, LiveReadinessState::Blocked);
        assert!(disabled.detail.contains("LIVE_CLOSEOUT_ENABLED=false"));

        cfg.live_closeout_enabled = true;
        cfg.live_closeout_dry_run = true;
        let dry_run = combo_rfq_closeout_execution_promotion_readiness_check(&cfg);
        assert_eq!(dry_run.state, LiveReadinessState::Blocked);
        assert!(dry_run.detail.contains("LIVE_CLOSEOUT_DRY_RUN=true"));

        cfg.live_closeout_dry_run = false;
        cfg.live_signature_type = 1;
        let proxy = combo_rfq_closeout_execution_promotion_readiness_check(&cfg);
        assert_eq!(proxy.state, LiveReadinessState::Blocked);
        assert!(proxy.detail.contains("closeout_wallet_type=PROXY"));

        cfg.live_signature_type = 0;
        cfg.live_funder_address.clear();
        let combo_closeout = combo_rfq_closeout_execution_promotion_readiness_check(&cfg);
        assert_eq!(combo_closeout.state, LiveReadinessState::Ready);
        assert!(combo_closeout
            .detail
            .contains("combo_router_eoa_redeem_executor_ready"));

        cfg.live_signature_type = 3;
        cfg.live_funder_address = "0x0000000000000000000000000000000000000001".into();
        cfg.relayer_api_key.clear();
        cfg.relayer_api_key_address.clear();
        let deposit_missing = combo_rfq_closeout_execution_promotion_readiness_check(&cfg);
        assert_eq!(deposit_missing.state, LiveReadinessState::Blocked);
        assert!(deposit_missing
            .detail
            .contains("deposit_wallet_relayer_config_blocked"));

        cfg.relayer_api_key = "relayer-key".into();
        cfg.relayer_api_key_address = "0x0000000000000000000000000000000000000002".into();
        let deposit_ready = combo_rfq_closeout_execution_promotion_readiness_check(&cfg);
        assert_eq!(deposit_ready.state, LiveReadinessState::Ready);
        assert!(deposit_ready
            .detail
            .contains("combo_router_deposit_wallet_relayer_executor_ready"));
    }

    #[test]
    fn top_level_closeout_readiness_has_reachable_ready_state() {
        let mut cfg = Config::from_env();

        let disabled = closeout_execution_readiness_check(&cfg);
        assert_eq!(disabled.state, LiveReadinessState::Blocked);

        cfg.live_closeout_enabled = true;
        cfg.live_closeout_dry_run = true;
        let dry_run = closeout_execution_readiness_check(&cfg);
        assert_eq!(dry_run.state, LiveReadinessState::Unknown);

        cfg.live_closeout_dry_run = false;
        let enabled = closeout_execution_readiness_check(&cfg);
        assert_eq!(enabled.state, LiveReadinessState::Ready);
        assert!(enabled.detail.contains("per-action eth_call"));
    }

    #[test]
    fn live_signature_type_supports_poly1271_deposit_wallet() {
        assert_eq!(signature_type_from_u8(3).unwrap(), SignatureType::Poly1271);
        let err = signature_type_from_u8(4).unwrap_err();
        assert!(err.to_string().contains("expected 0|1|2|3"));
    }

    #[tokio::test]
    async fn combo_rfq_finalized_block_gate_blocks_when_collector_disabled() {
        let cfg = Config::from_env();

        let check = combo_rfq_finalized_block_promotion_readiness_check(&cfg).await;

        assert_eq!(check.key, "polygon_finalized_block");
        assert_eq!(check.state, LiveReadinessState::Blocked);
        assert!(check
            .detail
            .contains("ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED=false"));
    }

    #[tokio::test]
    async fn combo_rfq_finalized_block_gate_accepts_fresh_finalized_head() {
        let server = MockServer::start_async().await;
        let latest = server
            .mock_async(|when, then| {
                when.method(POST).body_includes(r#""latest""#);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0x100"}
                }));
            })
            .await;
        let finalized = server
            .mock_async(|when, then| {
                when.method(POST).body_includes(r#""finalized""#);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0xf0"}
                }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.onchain_order_filled_collector_enabled = true;
        cfg.polygon_rpc_url = server.base_url();
        cfg.polygon_finalized_block_max_lag_blocks = 32;

        let check = combo_rfq_finalized_block_promotion_readiness_check(&cfg).await;

        assert_eq!(check.state, LiveReadinessState::Ready);
        assert!(check.detail.contains("latest_block=256"));
        assert!(check.detail.contains("finalized_block=240"));
        latest.assert_calls_async(1).await;
        finalized.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn combo_rfq_finalized_block_gate_blocks_stale_finalized_head() {
        let server = MockServer::start_async().await;
        let latest = server
            .mock_async(|when, then| {
                when.method(POST).body_includes(r#""latest""#);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0x200"}
                }));
            })
            .await;
        let finalized = server
            .mock_async(|when, then| {
                when.method(POST).body_includes(r#""finalized""#);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0x100"}
                }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.onchain_order_filled_collector_enabled = true;
        cfg.polygon_rpc_url = server.base_url();
        cfg.polygon_finalized_block_max_lag_blocks = 10;

        let check = combo_rfq_finalized_block_promotion_readiness_check(&cfg).await;

        assert_eq!(check.state, LiveReadinessState::Blocked);
        assert!(check.detail.contains("finalized_block_lag_blocks=256>10"));
        latest.assert_calls_async(1).await;
        finalized.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn combo_rfq_route_promotion_blocks_unresolved_execution_recovery() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_live_journal_dir("combo-rfq-promotion-unresolved");
        cfg.live_combo_rfq_route_enabled = true;
        crate::combo_rfq_client::append_combo_rfq_execution_journal_record(
            &cfg,
            &crate::combo_rfq_client::ComboRfqExecutionJournalRecord {
                generated_at: Utc::now().to_rfc3339(),
                event_id: "event-1".into(),
                stage: "accept_quote".into(),
                status: "accept_state_unknown".into(),
                client_request_id: "client-1".into(),
                rfq_id: Some("rfq-1".into()),
                quote_id: Some("quote-1".into()),
                maker_id: Some("maker-1".into()),
                request: None,
                selected_quote: None,
                accept_request: None,
                response: Some(serde_json::json!({"status":"unknown"})),
                error: Some("timeout".into()),
                blockers: vec!["rfq_accept_state_unknown".into()],
                note: "test unresolved accept".into(),
            },
        )
        .unwrap();

        let report = build_combo_rfq_route_promotion_report(&cfg).await;

        assert!(!report.promoted);
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.key == "combo_rfq_execution_recovery")
                .map(|check| check.state),
            Some(LiveReadinessState::Blocked)
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("unresolved_combo_rfq_execution_records=1")));
    }

    #[test]
    fn combo_rfq_account_exposure_check_blocks_open_positions() {
        let report = crate::combo_rfq_client::ComboExposureReport {
            status: "open_combo_exposure".into(),
            open_combo_count: 2,
            total_entry_cost_usdc: 42.0,
            total_cost_usdc: 45.0,
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };

        let check = combo_rfq_account_exposure_readiness_check(&report);

        assert_eq!(check.state, LiveReadinessState::Blocked);
        assert_eq!(check.key, "combo_rfq_account_exposure");
        assert!(check.detail.contains("open_combo_exposure count=2"));
    }

    #[test]
    fn combo_rfq_account_exposure_check_accepts_clean_report() {
        let report = crate::combo_rfq_client::ComboExposureReport {
            status: "clean".into(),
            open_combo_count: 0,
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };

        let check = combo_rfq_account_exposure_readiness_check(&report);

        assert_eq!(check.state, LiveReadinessState::Ready);
        assert_eq!(check.key, "combo_rfq_account_exposure");
    }

    #[test]
    fn combo_rfq_account_exposure_guard_rejects_open_or_unavailable_report() {
        let open = crate::combo_rfq_client::ComboExposureReport {
            status: "open_combo_exposure".into(),
            open_combo_count: 1,
            total_entry_cost_usdc: 12.0,
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };
        let unavailable = crate::combo_rfq_client::ComboExposureReport {
            status: "error".into(),
            error: Some("network".into()),
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };

        assert!(ensure_combo_rfq_account_exposure_report_clean(&open)
            .unwrap_err()
            .to_string()
            .contains("open_combo_exposure"));
        assert!(ensure_combo_rfq_account_exposure_report_clean(&unavailable)
            .unwrap_err()
            .to_string()
            .contains("combo_exposure_unavailable"));
    }

    #[test]
    fn trade_status_finality_uses_trade_lifecycle() {
        assert!(trade_status_is_confirmed(&TradeStatusType::Confirmed));
        assert!(!trade_status_is_confirmed(&TradeStatusType::Matched));
        assert!(trade_status_is_matched_unconfirmed(
            &TradeStatusType::Matched
        ));
        assert!(trade_status_is_matched_unconfirmed(&TradeStatusType::Mined));
        assert!(trade_status_is_terminal_failure(&TradeStatusType::Failed));
        assert!(trade_status_is_confirmed(&TradeStatusType::Unknown(
            "CONFIRMED".into()
        )));
        assert!(!trade_status_is_confirmed(&TradeStatusType::Unknown(
            "UNCONFIRMED".into()
        )));
        assert!(trade_status_is_matched_unconfirmed(
            &TradeStatusType::Unknown("MATCHED_UNCONFIRMED".into())
        ));
        assert!(trade_status_is_terminal_failure(&TradeStatusType::Unknown(
            "FAILED_ONCHAIN".into()
        )));
        assert!(!trade_status_is_matched_unconfirmed(
            &TradeStatusType::Unknown("UNKNOWN_STATUS".into())
        ));
    }

    #[test]
    fn live_error_pause_classifies_exchange_backpressure() {
        assert_eq!(
            live_error_pause("HTTP 425 matching engine restarting")
                .unwrap()
                .0,
            MATCHING_ENGINE_PAUSE
        );
        assert_eq!(
            live_error_pause("503 post_only_mode").unwrap().0,
            MATCHING_ENGINE_PAUSE
        );
        assert_eq!(
            live_error_pause("429 Too Many Requests").unwrap().0,
            RATE_LIMIT_PAUSE
        );
        assert_eq!(
            live_error_pause("order match delayed due to market conditions")
                .unwrap()
                .0,
            MATCHING_ENGINE_PAUSE
        );
        assert_eq!(
            live_error_pause("the market is not yet ready to process new orders")
                .unwrap()
                .0,
            MATCHING_ENGINE_PAUSE
        );
        assert_eq!(
            live_error_pause("status timeout while polling").unwrap().0,
            TRANSIENT_ENGINE_PAUSE
        );
        assert!(live_error_pause("insufficient balance").is_none());
    }

    #[test]
    fn live_circuit_breaker_pauses_and_clears_after_deadline() {
        let breaker = LiveCircuitBreaker::default();

        breaker.trip_for_error(&"HTTP 425");
        assert!(breaker.check().is_err());

        {
            let mut paused_until = breaker.paused_until.lock().unwrap();
            *paused_until = Some(Instant::now() - Duration::from_millis(1));
        }

        assert!(breaker.check().is_ok());
        assert!(breaker.paused_until.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn submit_lock_serializes_local_submit_window() {
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        let first_guard = lock.lock().await;
        let second_lock = lock.clone();
        let second = tokio::spawn(async move {
            let _guard = second_lock.lock().await;
            true
        });

        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!second.is_finished());

        drop(first_guard);
        assert!(second.await.unwrap());
    }

    #[test]
    fn clean_pre_submit_account_reports_phase() {
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();

        let err = ensure_clean_account_state("pre-submit", 1, 0, address, &["order-1".into()], &[])
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("live pre-submit requires a clean account"));
        assert!(err.to_string().contains("1 open order"));
    }

    fn position_view(
        condition_id: &str,
        outcome_index: i32,
        size: &str,
        redeemable: bool,
        mergeable: bool,
        negative_risk: bool,
    ) -> PositionView {
        PositionView {
            asset: format!("asset-{condition_id}-{outcome_index}"),
            condition_id: condition_id.into(),
            size: Decimal::from_str(size).unwrap(),
            title: format!("Market {condition_id}"),
            slug: format!("market-{condition_id}"),
            outcome_index,
            redeemable,
            mergeable,
            negative_risk,
        }
    }

    fn combo_position_view(
        condition_id: &str,
        position_id: &str,
        outcome_index: Option<u8>,
        shares_balance: &str,
        status: &str,
    ) -> crate::combo_rfq_client::ComboPositionView {
        crate::combo_rfq_client::ComboPositionView {
            combo_condition_id: condition_id.into(),
            combo_position_id: Some(position_id.into()),
            combo_outcome_index: outcome_index,
            status: Some(status.into()),
            shares_balance: Some(shares_balance.into()),
            entry_cost_usdc: Some(1.0),
            total_cost_usdc: Some(1.0),
            realized_payout_usdc: None,
            legs_total: Some(2),
            legs_pending: Some(0),
            legs: Vec::new(),
        }
    }

    #[test]
    fn closeout_plan_detects_standard_merge_pair() {
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("C", 0, "3.50", false, true, false),
            position_view("C", 1, "2.00", false, true, false),
        ];

        let plan = build_live_closeout_plan(address, &positions);

        assert_eq!(plan.open_positions, 2);
        assert_eq!(plan.combo_exposure.status, "not_checked");
        assert_eq!(plan.actions.len(), 1);
        assert_eq!(plan.actions[0].action, "merge_full_set");
        assert_eq!(plan.actions[0].amount_shares, "2.00");
        assert!(!plan.actions[0].negative_risk);
    }

    #[test]
    fn closeout_run_report_includes_combo_exposure_summary() {
        let cfg = Config::from_env();
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let combo_exposure = crate::combo_rfq_client::ComboExposureReport {
            user: Some(address.to_string()),
            open_combo_count: 1,
            total_entry_cost_usdc: 11.0,
            total_cost_usdc: 12.5,
            status: "open_combo_exposure".into(),
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };
        let plan =
            build_live_closeout_plan_with_combo_exposure(address, &[], combo_exposure.clone());

        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        assert_eq!(plan.combo_exposure.status, "open_combo_exposure");
        assert_eq!(report.combo_exposure.open_combo_count, 1);
        assert_eq!(report.combo_exposure.redeemable_combo_count, 0);
        assert_eq!(report.combo_exposure.status, "open_combo_exposure");
        assert!((report.combo_exposure.total_cost_usdc - 12.5).abs() < f64::EPSILON);
    }

    #[test]
    fn closeout_plan_includes_resolved_combo_redeem_review_action() {
        let mut cfg = Config::from_env();
        cfg.polygon_rpc_url = "https://polygon-rpc.invalid".into();
        let condition_id = "0x00000000000000000000000000000000000000000000000000000000000000cb";
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let combo_exposure = crate::combo_rfq_client::ComboExposureReport {
            user: Some(address.to_string()),
            redeemable_combo_count: 1,
            total_cost_usdc: 3.25,
            status: "redeemable_combo_exposure".into(),
            combos: vec![combo_position_view(
                condition_id,
                "1565002850659932464461040283313300477127768157985983788601518182528477822977",
                Some(1),
                "3.250000",
                "RESOLVED_WIN",
            )],
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };

        let plan = build_live_closeout_plan_with_combo_exposure(address, &[], combo_exposure);
        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        assert_eq!(plan.actions.len(), 1);
        assert_eq!(plan.actions[0].action, "combo_redeem_resolved_win_review");
        assert_eq!(
            plan.actions[0].combo_position_id.as_deref(),
            Some("1565002850659932464461040283313300477127768157985983788601518182528477822977")
        );
        assert_eq!(plan.actions[0].combo_outcome_index, Some(1));
        let action = &report.actions[0];
        assert_eq!(action.kind, "combo_redeem_positions");
        assert_eq!(action.status, "dry_run_candidate");
        assert_eq!(
            action.target_contract.as_deref(),
            Some(POLYGON_COMBO_ROUTER)
        );
        assert_eq!(action.amount_ctf_units.as_deref(), Some("3250000"));
        assert_eq!(action.call_preview.partition, vec![1]);
        assert_eq!(
            action.call_preview.eth_call_status,
            "not_checked_report_only"
        );
        assert!(action
            .blockers
            .iter()
            .any(|blocker| blocker.contains("LIVE_CLOSEOUT_ENABLED=false")));
        assert!(action.verification_query.contains("/v1/positions/combos"));
    }

    #[test]
    fn closeout_certificate_proves_resolved_combo_shape_but_blocks_preflight() {
        let cfg = Config::from_env();
        let condition_id = "0x00000000000000000000000000000000000000000000000000000000000000cc";
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let combo_exposure = crate::combo_rfq_client::ComboExposureReport {
            user: Some(address.to_string()),
            redeemable_combo_count: 1,
            status: "redeemable_combo_exposure".into(),
            combos: vec![combo_position_view(
                condition_id,
                "1565002850659932464461040283313300477127768157985983788601518182528477822976",
                Some(0),
                "4.000000",
                "RESOLVED_WIN",
            )],
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };
        let plan = build_live_closeout_plan_with_combo_exposure(address, &[], combo_exposure);
        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        let certificate = build_live_closeout_payoff_certificate(&plan, &[], &report);

        assert_eq!(certificate.status, "blocked");
        assert_eq!(certificate.combo_redeemable_count, 1);
        assert!((certificate.deterministic_min_terminal_payout_usd - 4.0).abs() < f64::EPSILON);
        assert_eq!(
            certificate.actions[0].payoff_proof,
            "resolved_winning_combo_pays_one_usdc_unit_per_share"
        );
        assert_eq!(
            certificate.actions[0].status,
            "payoff_shape_proven_preflight_blocked"
        );
        assert!(certificate.actions[0]
            .blockers
            .iter()
            .any(|blocker| blocker.contains("LIVE_CLOSEOUT_ENABLED=false")));
        assert!(certificate.actions[0]
            .blockers
            .iter()
            .any(|blocker| blocker.contains("eth_call_preflight_not_ok")));
    }

    #[test]
    fn closeout_payoff_certificate_proves_standard_merge_but_blocks_unquoted_gas() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_enabled = true;
        cfg.live_closeout_dry_run = false;
        cfg.polygon_rpc_url.clear();
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("C", 0, "2.00", false, true, false),
            position_view("C", 1, "2.00", false, true, false),
        ];
        let combo_exposure = crate::combo_rfq_client::ComboExposureReport {
            user: Some(address.to_string()),
            status: "clean".into(),
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };
        let plan =
            build_live_closeout_plan_with_combo_exposure(address, &positions, combo_exposure);
        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        let certificate = build_live_closeout_payoff_certificate(&plan, &positions, &report);

        assert_eq!(certificate.status, "blocked");
        assert_eq!(certificate.residual_position_count, 0);
        assert_eq!(certificate.closeout_gas_source, "not_quoted_no_fallback");
        assert!(certificate.estimated_closeout_gas_usd.is_none());
        assert!((certificate.deterministic_min_terminal_payout_usd - 2.0).abs() < f64::EPSILON);
        assert!(certificate
            .blockers
            .iter()
            .any(|blocker| blocker.contains("closeout_gas_quote_unavailable_no_fallback_used")));
        assert_eq!(certificate.actions.len(), 1);
        assert_eq!(
            certificate.actions[0].payoff_proof,
            "standard_binary_full_set_merge_pays_one_pusd_unit_per_share"
        );
        assert_eq!(
            certificate.actions[0].yes_asset.as_deref(),
            Some("asset-C-0")
        );
        assert_eq!(
            certificate.actions[0].no_asset.as_deref(),
            Some("asset-C-1")
        );
        assert_eq!(
            certificate.actions[0].amount_ctf_units.as_deref(),
            Some("2000000")
        );
        assert_eq!(certificate.actions[0].partition, vec![1, 2]);
        assert_eq!(certificate.actions[0].eth_call_block, "latest");
        assert!((certificate.actions[0].expected_pusd_delta_usd - 2.0).abs() < f64::EPSILON);
        assert_eq!(
            certificate.actions[0].status,
            "payoff_shape_proven_preflight_blocked"
        );
        assert!(certificate.actions[0]
            .blockers
            .iter()
            .any(|blocker| blocker.contains("eth_call_preflight_not_ok")));
    }

    #[test]
    fn closeout_payoff_certificate_blocks_combo_exposure() {
        let cfg = Config::from_env();
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let combo_exposure = crate::combo_rfq_client::ComboExposureReport {
            user: Some(address.to_string()),
            open_combo_count: 1,
            total_cost_usdc: 12.5,
            status: "open_combo_exposure".into(),
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };
        let plan = build_live_closeout_plan_with_combo_exposure(address, &[], combo_exposure);
        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        let certificate = build_live_closeout_payoff_certificate(&plan, &[], &report);

        assert_eq!(certificate.status, "blocked");
        assert_eq!(certificate.combo_open_count, 1);
        assert_eq!(certificate.closeout_gas_source, "not_required");
        assert!(certificate
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_open_position_closeout_not_certified")));
    }

    #[test]
    fn closeout_payoff_certificate_flags_single_sided_residual_inventory() {
        let cfg = Config::from_env();
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![position_view("C", 0, "3.00", false, true, false)];
        let combo_exposure = crate::combo_rfq_client::ComboExposureReport {
            user: Some(address.to_string()),
            status: "clean".into(),
            ..crate::combo_rfq_client::ComboExposureReport::default()
        };
        let plan =
            build_live_closeout_plan_with_combo_exposure(address, &positions, combo_exposure);
        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        let certificate = build_live_closeout_payoff_certificate(&plan, &positions, &report);

        assert_eq!(plan.actions.len(), 0);
        assert_eq!(certificate.status, "blocked");
        assert_eq!(certificate.residual_condition_count, 1);
        assert_eq!(certificate.residual_position_count, 1);
        assert_eq!(certificate.residual_shares, "3.00");
        assert!(certificate
            .blockers
            .iter()
            .any(|blocker| blocker.contains("residual_open_positions_not_certified")));
    }

    #[test]
    fn closeout_plan_keeps_negative_risk_redeem_in_review() {
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![position_view("C", 0, "1.25", true, false, true)];

        let plan = build_live_closeout_plan(address, &positions);

        assert_eq!(plan.actions.len(), 1);
        assert_eq!(plan.actions[0].action, "neg_risk_redeem_review");
        assert_eq!(plan.actions[0].amount_shares, "1.25");
        assert!(plan.actions[0].negative_risk);
    }

    #[test]
    fn closeout_run_report_marks_standard_actions_ready_and_neg_risk_review_only() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_dry_run = true;
        cfg.polygon_rpc_url = "https://polygon-rpc.invalid".into();
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("A", 0, "2.00", false, true, false),
            position_view("A", 1, "2.00", false, true, false),
            position_view("B", 0, "1.25", true, false, true),
        ];
        let plan = build_live_closeout_plan(address, &positions);

        let mut unresolved = HashMap::new();
        unresolved.insert("A".into(), vec!["exec-1".into()]);

        let report = build_live_closeout_run_report(&cfg, &plan, &unresolved).unwrap();

        assert!(report.dry_run);
        assert_eq!(report.planned_actions, 2);
        assert_eq!(report.selected_actions, 2);
        assert_eq!(report.skipped_actions, 0);
        assert_eq!(report.actions[0].action_id, "merge_full_set:A");
        assert_eq!(report.actions[0].action, "merge_full_set");
        assert_eq!(report.actions[0].kind, "merge_positions");
        assert_eq!(report.actions[0].wallet_type, "EOA");
        assert_eq!(
            report.actions[0].target_contract.as_deref(),
            Some(POLYGON_CTF_COLLATERAL_ADAPTER)
        );
        assert!(report.actions[0].collateral_token.is_some());
        assert!(report.actions[0].calldata.is_none());
        assert_eq!(report.actions[0].value, "0");
        assert_eq!(report.actions[0].call_preview.function, "merge_positions");
        assert_eq!(report.actions[0].call_preview.partition, vec![1, 2]);
        assert_eq!(
            report.actions[0].call_preview.amount_ctf_units.as_deref(),
            Some("2000000")
        );
        assert_eq!(
            report.actions[0].call_preview.eth_call_status,
            "not_checked_report_only"
        );
        assert!(report.actions[0]
            .call_preview
            .expected_collateral_delta
            .contains("2.00"));
        assert_eq!(
            report.actions[0].amount_ctf_units.as_deref(),
            Some("2000000")
        );
        assert!(report.actions[0].transaction_hash.is_none());
        assert!(report.actions[0].block_number.is_none());
        assert!(report.actions[0].reconciled_execution_ids.is_empty());
        assert!(report.actions[0]
            .expected_position_delta
            .contains("full-set shares"));
        assert!(report.actions[0]
            .verification_query
            .contains("conditionId=A"));
        assert!(report.actions[0]
            .blockers
            .iter()
            .any(|blocker| blocker.contains("LIVE_CLOSEOUT_ENABLED=false")));
        assert_eq!(report.actions[0].status, "dry_run_candidate");
        assert_eq!(report.actions[0].unresolved_execution_ids, vec!["exec-1"]);
        assert_eq!(report.actions[1].action, "neg_risk_redeem_review");
        assert_eq!(report.actions[1].status, "review_only");
    }

    #[test]
    fn closeout_run_report_encodes_standard_merge_calldata() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_dry_run = true;
        let condition_id = "0x000000000000000000000000000000000000000000000000000000000000000a";
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view(condition_id, 0, "2.00", false, true, false),
            position_view(condition_id, 1, "2.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);

        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        let action = &report.actions[0];
        let calldata = action.calldata.as_deref().expect("merge calldata");
        assert_eq!(
            action.target_contract.as_deref(),
            Some(POLYGON_CTF_COLLATERAL_ADAPTER)
        );
        assert_ne!(
            action.target_contract,
            contract_config(cfg.live_chain_id, false)
                .map(|contract| contract.conditional_tokens.to_string())
        );
        let expected_selector = format!(
            "0x{}",
            hex_encode_lower(&abi_selector(
                "mergePositions(address,bytes32,bytes32,uint256[],uint256)"
            ))
        );
        let mut amount_word = Vec::new();
        push_abi_u256_word(&mut amount_word, U256::from(2_000_000u64));
        assert!(calldata.starts_with(&expected_selector));
        assert!(calldata.contains(condition_id.trim_start_matches("0x")));
        assert!(calldata.contains(&hex_encode_lower(&amount_word)));
        assert_eq!(calldata.len(), 2 + (4 + 32 * 8) * 2);
        assert_eq!(
            action.call_preview.parent_collection_id,
            B256::default().to_string()
        );
        assert_eq!(action.call_preview.eth_call_block, "latest");
    }

    #[test]
    fn combo_redeem_calldata_uses_router_bytes31_condition_from_position_id() {
        let position_id =
            "1565002850659932464461040283313300477127768157985983788601518182528477822977";
        let token_id = U256::from_str_radix(position_id, 10).unwrap();
        let mut expected_condition_word = token_id.to_be_bytes::<32>();
        assert_eq!(expected_condition_word[31], 1);
        expected_condition_word[31] = 0;

        let condition_word =
            combo_redeem_condition_id_abi_word_from_position_id(position_id, Some(1)).unwrap();
        assert_eq!(condition_word, expected_condition_word);

        let calldata =
            encode_combo_redeem_calldata(condition_word, U256::from(1u8), U256::from(4_000_000u64));
        assert!(calldata.starts_with("0xd217a3cc"));
        assert!(calldata.contains(&hex_encode_lower(&expected_condition_word)));
        assert_eq!(calldata.len(), 2 + (4 + 32 * 3) * 2);
        let mismatch =
            combo_redeem_condition_id_abi_word_from_position_id(position_id, Some(0)).unwrap_err();
        assert!(mismatch
            .to_string()
            .contains("did not match catalog outcome index"));
    }

    #[test]
    fn deposit_wallet_relayer_batch_uses_docs_eip712_type_names() {
        let call_type = Call::eip712_encode_type();
        let batch_type = Batch::eip712_encode_type();
        assert_eq!(call_type, "Call(address target,uint256 value,bytes data)");
        assert!(batch_type.contains("Batch("));
        assert!(batch_type.contains("Call[] calls"));
        assert!(batch_type.contains("Call(address target,uint256 value,bytes data)"));

        let call = RelayerCallJson {
            target: POLYGON_COMBO_ROUTER.into(),
            value: "0".into(),
            data: "0x1234".into(),
        };
        let eip712_call = relayer_call_json_to_eip712_call(&call).unwrap();
        assert_eq!(
            eip712_call.target,
            Address::from_str(POLYGON_COMBO_ROUTER).unwrap()
        );
        assert_eq!(eip712_call.value, U256::ZERO);
        assert_eq!(eip712_call.data.as_ref(), &[0x12, 0x34]);
    }

    #[tokio::test]
    async fn deposit_wallet_relayer_nonce_request_uses_wallet_params_and_headers() {
        let server = MockServer::start_async().await;
        let api_key_address =
            Address::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let nonce = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/account/transactions/params")
                    .query_param("address", api_key_address.to_string())
                    .query_param("type", "WALLET")
                    .header("RELAYER_API_KEY", "relayer-key")
                    .header("RELAYER_API_KEY_ADDRESS", api_key_address.to_string());
                then.status(200)
                    .json_body(serde_json::json!({ "nonce": "42" }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.api_timeout_secs = 2;
        let relayer = DepositWalletRelayerConfig {
            api_url: server.base_url(),
            api_key: "relayer-key".into(),
            api_key_address,
        };

        let parsed = fetch_deposit_wallet_relayer_nonce(&Client::new(), &relayer, &cfg)
            .await
            .unwrap();

        assert_eq!(parsed, U256::from(42u64));
        nonce.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn deposit_wallet_relayer_submit_posts_docs_wallet_batch_payload() {
        let server = MockServer::start_async().await;
        let api_key_address =
            Address::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let wallet = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let submit = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/submit")
                    .header("content-type", "application/json")
                    .header("RELAYER_API_KEY", "relayer-key")
                    .header("RELAYER_API_KEY_ADDRESS", api_key_address.to_string())
                    .body_includes(r#""type":"WALLET""#)
                    .body_includes(format!(r#""from":"{}""#, api_key_address))
                    .body_includes(format!(r#""to":"{}""#, POLYMARKET_RELAYER_WALLET_SUBMIT_TO))
                    .body_includes(r#""nonce":"7""#)
                    .body_includes(r#""signature":"0xsig""#)
                    .body_includes(r#""metadata":"redeem combo""#)
                    .body_includes(format!(r#""depositWallet":"{}""#, wallet))
                    .body_includes(r#""deadline":"1800000000""#)
                    .body_includes(format!(r#""target":"{}""#, POLYGON_COMBO_ROUTER))
                    .body_includes(r#""value":"0""#)
                    .body_includes(r#""data":"0x1234""#);
                then.status(200).json_body(serde_json::json!({
                    "transaction_id": "tx-1",
                    "state": "STATE_NEW"
                }));
            })
            .await;
        let relayer = DepositWalletRelayerConfig {
            api_url: server.base_url(),
            api_key: "relayer-key".into(),
            api_key_address,
        };
        let calls = vec![RelayerCallJson {
            target: POLYGON_COMBO_ROUTER.into(),
            value: "0".into(),
            data: "0x1234".into(),
        }];

        let response = submit_deposit_wallet_relayer_batch(
            &Client::new(),
            &relayer,
            wallet,
            U256::from(7u64),
            1_800_000_000,
            "0xsig".into(),
            calls,
            "redeem combo".into(),
        )
        .await
        .unwrap();

        assert_eq!(response.transaction_id.as_deref(), Some("tx-1"));
        assert_eq!(response.state.as_deref(), Some("STATE_NEW"));
        submit.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn deposit_wallet_relayer_poll_accepts_confirmed_transaction_hash() {
        let server = MockServer::start_async().await;
        let api_key_address =
            Address::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let poll = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/account/transactions/tx-1")
                    .header("RELAYER_API_KEY", "relayer-key")
                    .header("RELAYER_API_KEY_ADDRESS", api_key_address.to_string());
                then.status(200).json_body(serde_json::json!({
                    "transaction_id": "tx-1",
                    "transaction_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                    "state": "STATE_CONFIRMED"
                }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.api_timeout_secs = 2;
        cfg.live_closeout_confirm_timeout_secs = 1;
        let relayer = DepositWalletRelayerConfig {
            api_url: server.base_url(),
            api_key: "relayer-key".into(),
            api_key_address,
        };
        let submitted = RelayerTransactionResponse {
            transaction_id: Some("tx-1".into()),
            transaction_hash: None,
            state: Some("STATE_NEW".into()),
            error_msg: None,
            error: None,
        };

        let confirmed =
            poll_deposit_wallet_relayer_transaction(&Client::new(), &relayer, &cfg, &submitted)
                .await
                .unwrap();

        assert_eq!(confirmed.state.as_deref(), Some("STATE_CONFIRMED"));
        assert_eq!(
            confirmed.transaction_hash.as_deref(),
            Some("0x1111111111111111111111111111111111111111111111111111111111111111")
        );
        poll.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn deposit_wallet_relayer_poll_blocks_terminal_failure() {
        let server = MockServer::start_async().await;
        let api_key_address =
            Address::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let poll = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/account/transactions/tx-1")
                    .header("RELAYER_API_KEY", "relayer-key")
                    .header("RELAYER_API_KEY_ADDRESS", api_key_address.to_string());
                then.status(200).json_body(serde_json::json!({
                    "transaction_id": "tx-1",
                    "state": "STATE_FAILED",
                    "error_msg": "wallet signature rejected"
                }));
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.api_timeout_secs = 2;
        cfg.live_closeout_confirm_timeout_secs = 1;
        let relayer = DepositWalletRelayerConfig {
            api_url: server.base_url(),
            api_key: "relayer-key".into(),
            api_key_address,
        };
        let submitted = RelayerTransactionResponse {
            transaction_id: Some("tx-1".into()),
            transaction_hash: None,
            state: Some("STATE_NEW".into()),
            error_msg: None,
            error: None,
        };

        let err =
            poll_deposit_wallet_relayer_transaction(&Client::new(), &relayer, &cfg, &submitted)
                .await
                .unwrap_err();

        assert!(err.to_string().contains("STATE_FAILED"));
        assert!(err.to_string().contains("wallet signature rejected"));
        poll.assert_calls_async(1).await;
    }

    #[test]
    fn closeout_receipt_validation_requires_pusd_transfer_to_account() {
        let account = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let other = Address::from_str("0x0000000000000000000000000000000000000002").unwrap();
        let adapter = Address::from_str(POLYGON_CTF_COLLATERAL_ADAPTER).unwrap();
        let contract_cfg = contract_config(POLYGON_CHAIN_ID, false).unwrap();
        let ctf_transfer = CloseoutReceiptLogSummary {
            address: contract_cfg.conditional_tokens,
            topics: vec![
                event_topic("TransferBatch(address,address,address,uint256[],uint256[])"),
                indexed_address_topic(adapter),
                indexed_address_topic(account),
                indexed_address_topic(Address::ZERO),
            ],
        };
        let wrong_collateral_transfer = CloseoutReceiptLogSummary {
            address: contract_cfg.collateral,
            topics: vec![
                event_topic("Transfer(address,address,uint256)"),
                indexed_address_topic(adapter),
                indexed_address_topic(other),
            ],
        };

        let err = validate_closeout_receipt_logs(
            &[ctf_transfer, wrong_collateral_transfer],
            adapter,
            contract_cfg.collateral,
            contract_cfg.conditional_tokens,
            account,
        )
        .unwrap_err();

        assert!(err.to_string().contains("pUSD Transfer to account"));
    }

    #[test]
    fn closeout_receipt_validation_accepts_pusd_and_ctf_logs() {
        let account = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let adapter = Address::from_str(POLYGON_CTF_COLLATERAL_ADAPTER).unwrap();
        let contract_cfg = contract_config(POLYGON_CHAIN_ID, false).unwrap();
        let logs = vec![
            CloseoutReceiptLogSummary {
                address: contract_cfg.collateral,
                topics: vec![
                    event_topic("Transfer(address,address,uint256)"),
                    indexed_address_topic(adapter),
                    indexed_address_topic(account),
                ],
            },
            CloseoutReceiptLogSummary {
                address: contract_cfg.conditional_tokens,
                topics: vec![
                    event_topic("TransferSingle(address,address,address,uint256,uint256)"),
                    indexed_address_topic(adapter),
                    indexed_address_topic(account),
                    indexed_address_topic(Address::ZERO),
                ],
            },
        ];

        let validation = validate_closeout_receipt_logs(
            &logs,
            adapter,
            contract_cfg.collateral,
            contract_cfg.conditional_tokens,
            account,
        )
        .unwrap();

        assert_eq!(validation.total_logs, 2);
        assert_eq!(validation.collateral_transfer_to_account_logs, 1);
        assert_eq!(validation.ctf_transfer_logs, 1);
    }

    #[test]
    fn closeout_p_usd_delta_requires_expected_merge_amount() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_dry_run = true;
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("A", 0, "2.00", false, true, false),
            position_view("A", 1, "2.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);
        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();
        let action = &report.actions[0];

        assert!(ensure_closeout_p_usd_delta(
            action,
            U256::from(2_000_000u64),
            U256::from(10_000_000u64),
            U256::from(11_999_999u64),
        )
        .unwrap_err()
        .to_string()
        .contains("below expected"));
        assert_eq!(
            ensure_closeout_p_usd_delta(
                action,
                U256::from(2_000_000u64),
                U256::from(10_000_000u64),
                U256::from(12_000_000u64),
            )
            .unwrap(),
            U256::from(2_000_000u64)
        );
    }

    #[tokio::test]
    async fn closeout_accounting_waits_for_finalized_receipt_block() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let finalized = server
            .mock_async(|when, then| {
                when.method(POST).body_includes(r#""finalized""#);
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x7b"}}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.polygon_rpc_url = server.base_url();
        cfg.live_closeout_confirm_timeout_secs = 1;

        let block = wait_for_closeout_receipt_finalized(&cfg, 123)
            .await
            .unwrap();

        assert_eq!(block, 123);
        finalized.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn closeout_accounting_blocks_before_finalized_receipt_block() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;
        let finalized = server
            .mock_async(|when, then| {
                when.method(POST).body_includes(r#""finalized""#);
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x7a"}}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.polygon_rpc_url = server.base_url();
        cfg.live_closeout_confirm_timeout_secs = 0;

        let err = wait_for_closeout_receipt_finalized(&cfg, 123)
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("closeout receipt not finalized before accounting release"));
        finalized.assert_calls_async(1).await;
    }

    #[test]
    fn closeout_realized_pnl_ledger_allocates_delta_by_execution_cost() {
        let dir = temp_live_journal_dir("realized-pnl");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.live_closeout_dry_run = true;
        let journal_path = dir.join(LIVE_EXECUTION_JOURNAL_FILE);
        std::fs::write(
            &journal_path,
            concat!(
                r#"{"execution_id":"exec-1","stage":"fill_confirmed_exposure_retained","position_usd":1.50,"actual_fill_cost_usd":1.52,"entry_fees_usd":0.04,"entry_gas_cost_usd":0.04,"actual_entry_cost_usd":1.60,"projected_pnl_usd":0.50,"projected_roi_pct":33.3333,"basket_units":2.0}"#,
                "\n",
                r#"{"execution_id":"exec-2","stage":"fill_confirmed_exposure_retained","position_usd":0.50,"actual_fill_cost_usd":0.38,"entry_fees_usd":0.01,"entry_gas_cost_usd":0.01,"actual_entry_cost_usd":0.40,"projected_pnl_usd":0.10,"projected_roi_pct":20.0,"basket_units":1.0}"#,
                "\n"
            ),
        )
        .unwrap();
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("A", 0, "2.00", false, true, false),
            position_view("A", 1, "2.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);
        let mut unresolved = HashMap::new();
        unresolved.insert("A".into(), vec!["exec-1".into(), "exec-2".into()]);
        let report = build_live_closeout_run_report(&cfg, &plan, &unresolved).unwrap();
        let action = &report.actions[0];

        append_closeout_realized_pnl_records(
            &cfg,
            action,
            "0xtx",
            123,
            U256::from(10_000_000u64),
            U256::from(12_000_000u64),
            CloseoutReceiptValidation {
                total_logs: 2,
                adapter_logs: 0,
                collateral_transfer_to_account_logs: 1,
                ctf_transfer_logs: 1,
            },
            closeout_gas_accounting_for_test(0.20),
            &closeout_reconciliation_execution_ids(action),
        )
        .unwrap();

        let ledger = std::fs::read_to_string(dir.join(LIVE_REALIZED_PNL_FILE)).unwrap();
        let records: Vec<Value> = ledger
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["execution_id"], "exec-1");
        assert_eq!(records[0]["p_usd_delta_units"], "2000000");
        assert!((records[0]["allocation_ratio"].as_f64().unwrap() - 0.80).abs() < 1e-9);
        assert!((records[0]["actual_fill_cost_usd"].as_f64().unwrap() - 1.52).abs() < 1e-9);
        assert!((records[0]["entry_fees_usd"].as_f64().unwrap() - 0.04).abs() < 1e-9);
        assert!((records[0]["entry_gas_cost_usd"].as_f64().unwrap() - 0.04).abs() < 1e-9);
        assert!((records[0]["actual_entry_cost_usd"].as_f64().unwrap() - 1.60).abs() < 1e-9);
        assert!(
            (records[0]["realized_pnl_usd_before_closeout_gas"]
                .as_f64()
                .unwrap()
                - 0.0)
                .abs()
                < 1e-9
        );
        assert!((records[0]["closeout_gas_cost_usd"].as_f64().unwrap() - 0.20).abs() < 1e-9);
        assert!(
            (records[0]["allocated_closeout_gas_cost_usd"]
                .as_f64()
                .unwrap()
                - 0.16)
                .abs()
                < 1e-9
        );
        assert!((records[0]["realized_pnl_usd"].as_f64().unwrap() + 0.16).abs() < 1e-9);
        assert_eq!(records[1]["execution_id"], "exec-2");
        assert!((records[1]["allocation_ratio"].as_f64().unwrap() - 0.20).abs() < 1e-9);
        assert!((records[1]["realized_pnl_usd"].as_f64().unwrap() + 0.04).abs() < 1e-9);

        append_closeout_realized_pnl_records(
            &cfg,
            action,
            "0xtx",
            123,
            U256::from(10_000_000u64),
            U256::from(12_000_000u64),
            CloseoutReceiptValidation {
                total_logs: 2,
                adapter_logs: 0,
                collateral_transfer_to_account_logs: 1,
                ctf_transfer_logs: 1,
            },
            closeout_gas_accounting_for_test(0.20),
            &closeout_reconciliation_execution_ids(action),
        )
        .unwrap();
        let deduped_ledger = std::fs::read_to_string(dir.join(LIVE_REALIZED_PNL_FILE)).unwrap();
        assert_eq!(deduped_ledger.lines().count(), 2);
    }

    #[tokio::test]
    async fn closeout_exposure_release_uses_execution_event_and_cost_basis() {
        let dir = temp_live_journal_dir("closeout-exposure-release");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        let journal_path = dir.join(LIVE_EXECUTION_JOURNAL_FILE);
        std::fs::write(
            &journal_path,
            concat!(
                r#"{"execution_id":"exec-1","stage":"fill_confirmed_exposure_retained","event_id":"event-1","position_usd":1.50,"actual_entry_cost_usd":1.60}"#,
                "\n",
                r#"{"execution_id":"exec-2","stage":"fill_confirmed_exposure_retained","event_id":"event-2","position_usd":0.50,"actual_entry_cost_usd":0.40}"#,
                "\n"
            ),
        )
        .unwrap();
        append_exposure_ledger_delta(&dir, "event-1", 1.60, "reserved", "test").unwrap();
        append_exposure_ledger_delta(&dir, "event-2", 0.40, "reserved", "test").unwrap();
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("A", 0, "2.00", false, true, false),
            position_view("A", 1, "2.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);
        let mut unresolved = HashMap::new();
        unresolved.insert("A".into(), vec!["exec-1".into(), "exec-2".into()]);
        let report = build_live_closeout_run_report(&cfg, &plan, &unresolved).unwrap();
        let action = &report.actions[0];

        let released =
            append_closeout_exposure_releases(&cfg, &closeout_reconciliation_execution_ids(action))
                .unwrap();

        assert_eq!(released, 2);
        let exposure = crate::exposure::ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(exposure.current("event-1").await, 0.0);
        assert_eq!(exposure.current("event-2").await, 0.0);
        let ledger = std::fs::read_to_string(dir.join("live_exposure_ledger.jsonl")).unwrap();
        assert!(ledger.contains(r#""source":"live_closeout""#));
        assert!(ledger.contains(r#""delta_usd":-1.6"#));
        assert!(ledger.contains(r#""delta_usd":-0.4"#));
    }

    fn closeout_gas_accounting_for_test(gas_cost_usd: f64) -> CloseoutGasAccounting {
        CloseoutGasAccounting {
            gas_used: 100_000,
            effective_gas_price_wei: 30_000_000_000,
            gas_cost_wei: U256::from(3_000_000_000_000_000u64),
            gas_cost_pol: 0.003,
            gas_cost_usd,
        }
    }

    #[tokio::test]
    async fn closeout_eth_call_preflight_records_success() {
        let server = MockServer::start_async().await;
        let condition_id = "0x000000000000000000000000000000000000000000000000000000000000000b";
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view(condition_id, 0, "2.00", false, true, false),
            position_view(condition_id, 1, "2.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);
        let mut cfg = Config::from_env();
        cfg.polygon_rpc_url = server.base_url();
        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();
        let action = &report.actions[0];
        let calldata = action.calldata.as_deref().unwrap();
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes("\"method\":\"eth_call\"")
                    .body_includes(calldata);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": "0x"
                }));
            })
            .await;

        let outcome =
            simulate_closeout_eth_call_with_rpc_url(&Client::new(), &cfg, action, address).await;

        assert_eq!(outcome.status, "ok");
        assert!(outcome.note.contains("succeeded"));
        rpc.assert_calls_async(1).await;
    }

    #[test]
    fn failed_eth_call_preflight_blocks_ready_closeout_action() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_enabled = true;
        cfg.live_closeout_dry_run = false;
        let condition_id = "0x000000000000000000000000000000000000000000000000000000000000000c";
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view(condition_id, 0, "2.00", false, true, false),
            position_view(condition_id, 1, "2.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);
        let mut report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        assert_eq!(report.actions[0].status, "ready");
        apply_eth_call_outcome(
            &mut report.actions[0],
            EthCallOutcome {
                status: "reverted".into(),
                note: "eth_call failed code=-32000 message=execution reverted".into(),
            },
        );

        assert_eq!(report.actions[0].status, "blocked");
        assert_eq!(
            report.actions[0].reason,
            "closeout candidate is blocked by failed eth_call preflight"
        );
        assert!(report.actions[0]
            .blockers
            .iter()
            .any(|blocker| blocker.contains("eth_call preflight did not succeed")));
    }

    #[test]
    fn closeout_run_report_respects_max_actions_per_run() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_dry_run = true;
        cfg.live_closeout_max_actions_per_run = 1;
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("A", 0, "2.00", false, true, false),
            position_view("A", 1, "2.00", false, true, false),
            position_view("B", 0, "3.00", false, true, false),
            position_view("B", 1, "3.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);

        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        assert_eq!(report.planned_actions, 2);
        assert_eq!(report.selected_actions, 1);
        assert_eq!(report.skipped_actions, 1);
        assert_eq!(report.max_actions, 1);
    }

    #[test]
    fn closeout_run_report_marks_non_dry_run_standard_actions_ready() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_enabled = true;
        cfg.live_closeout_dry_run = false;
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("A", 0, "2.00", false, true, false),
            position_view("A", 1, "2.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);

        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        assert!(!report.dry_run);
        assert_eq!(report.actions.len(), 1);
        assert_eq!(report.actions[0].status, "ready");
        assert!(report.actions[0].blockers.is_empty());
        assert_eq!(
            report.actions[0].amount_ctf_units.as_deref(),
            Some("2000000")
        );
    }

    #[test]
    fn closeout_run_report_blocks_non_eoa_execution() {
        let mut cfg = Config::from_env();
        cfg.live_closeout_enabled = true;
        cfg.live_closeout_dry_run = false;
        cfg.live_signature_type = 2;
        let address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = vec![
            position_view("A", 0, "2.00", false, true, false),
            position_view("A", 1, "2.00", false, true, false),
        ];
        let plan = build_live_closeout_plan(address, &positions);

        let report = build_live_closeout_run_report(&cfg, &plan, &HashMap::new()).unwrap();

        assert_eq!(report.actions[0].status, "blocked");
        assert!(report.actions[0]
            .blockers
            .iter()
            .any(|blocker| blocker.contains("requires an EOA closeout wallet")));
    }

    #[test]
    fn closeout_amount_conversion_uses_six_decimal_ctf_units() {
        assert_eq!(
            decimal_shares_to_ctf_units(Decimal::from_str("1.234567").unwrap()).unwrap(),
            U256::from(1_234_567u64)
        );
        assert_eq!(
            decimal_shares_to_ctf_units(Decimal::from_str("1.2345678").unwrap()).unwrap(),
            U256::from(1_234_567u64)
        );
        assert!(decimal_shares_to_ctf_units(Decimal::ZERO).is_err());
    }

    #[test]
    fn closeout_native_gas_balance_requires_minimum_pol() {
        let minimum = closeout_native_gas_floor_wei();
        let err = ensure_closeout_native_gas_balance(minimum - U256::from(1u8)).unwrap_err();

        assert!(err.to_string().contains("native POL for gas"));
        assert!(ensure_closeout_native_gas_balance(minimum).is_ok());
    }

    #[test]
    fn startup_position_pagination_continues_until_short_page() {
        assert_eq!(
            next_startup_positions_offset(
                0,
                STARTUP_POSITIONS_PAGE_LIMIT as usize,
                STARTUP_POSITIONS_PAGE_LIMIT
            )
            .unwrap(),
            Some(STARTUP_POSITIONS_PAGE_LIMIT)
        );
        assert_eq!(
            next_startup_positions_offset(
                STARTUP_POSITIONS_PAGE_LIMIT,
                1,
                STARTUP_POSITIONS_PAGE_LIMIT
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn startup_position_pagination_fails_when_not_exhaustive() {
        let err = next_startup_positions_offset(
            STARTUP_POSITIONS_MAX_OFFSET,
            STARTUP_POSITIONS_PAGE_LIMIT as usize,
            STARTUP_POSITIONS_PAGE_LIMIT,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("exceed exhaustive startup reconciliation window"));
    }

    #[test]
    fn non_eoa_live_requires_explicit_funder_for_reconciliation() {
        let funder = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();

        assert!(ensure_funder_policy(0, None).is_ok());
        assert!(ensure_funder_policy(1, Some(funder)).is_ok());

        let err = ensure_funder_policy(2, None).unwrap_err();
        assert!(err.to_string().contains("LIVE_FUNDER_ADDRESS is required"));
    }

    #[test]
    fn order_type_parsing_rejects_gtd_without_expiry_support() {
        assert!(order_type_from_config("gtd").is_err());
    }

    #[test]
    fn tick_size_mapping_accepts_supported_increments() {
        assert!(tick_size_from_f64(0.1).is_some());
        assert!(tick_size_from_f64(0.01).is_some());
        assert!(tick_size_from_f64(0.001).is_some());
        assert!(tick_size_from_f64(0.0001).is_some());
    }

    #[test]
    fn basket_unit_step_scales_for_ranked_baskets() {
        let cfg = Config::from_env();
        let step = basket_unit_step(
            &[
                OpportunityLeg {
                    market_index: 0,
                    question: "A".into(),
                    market_slug: "a".into(),
                    condition_id: "c1".into(),
                    token_id: "1".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 2.0,
                    reference_price: 0.3,
                },
                OpportunityLeg {
                    market_index: 1,
                    question: "B".into(),
                    market_slug: "b".into(),
                    condition_id: "c2".into(),
                    token_id: "2".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 0.5,
                    reference_price: 0.4,
                },
            ],
            &cfg,
        );
        assert!(step >= LIVE_SDK_LOT_SIZE_STEP_SHARES / 0.5);
    }

    fn executable_opp(token_id: &str) -> ArbitrageOpportunity {
        ArbitrageOpportunity {
            event_title: "E".into(),
            event_id: "1".into(),
            category: "sports".into(),
            arb_type: ArbType::Bundle,
            markets: vec![Market {
                question: "Q".into(),
                condition_id: "C".into(),
                market_slug: "q".into(),
                clob_token_id_yes: "Y".into(),
                clob_token_id_no: "N".into(),
                gamma_yes_price: 0.5,
                gamma_no_price: 0.5,
                clob_yes_ask: Some(0.4),
                clob_yes_bid: Some(0.39),
                clob_no_ask: Some(0.6),
                clob_no_bid: Some(0.59),
                clob_yes_ask_size: Some(10.0),
                clob_yes_bid_size: None,
                clob_no_ask_size: Some(10.0),
                clob_no_bid_size: None,
                fees_enabled: Some(true),
                taker_fee_rate: None,
                maker_fee_rate: None,
                clob_taker_fee_bps: None,
                clob_fee_rate: None,
                clob_fee_exponent: None,
                order_price_min_tick_size: Some(0.01),
                order_min_size: Some(1.0),
                clob_tick_size: Some(0.01),
                clob_min_order_size: Some(1.0),
                clob_neg_risk: Some(true),
                clob_rfq_enabled: None,
                liquidity: 1000.0,
                closed: false,
            }],
            execution_plan: vec![OpportunityLeg {
                market_index: 0,
                question: "Q".into(),
                market_slug: "q".into(),
                condition_id: "C".into(),
                token_id: token_id.into(),
                outcome: OutcomeSide::Yes,
                unit_shares: 1.0,
                reference_price: 0.4,
            }],
            total_cost: 0.8,
            guaranteed_revenue: 1.0,
            gross_profit: 0.2,
            total_fees: 0.0,
            net_profit: 0.19,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 10.0,
            prices_from_clob: true,
            max_executable_size_usd: 10.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn live_execution_refuses_multi_leg_baskets_without_atomic_support() {
        let mut opp = executable_opp("1");

        let mut unsupported_route = opp.clone();
        unsupported_route.arb_type = ArbType::Bundle;
        let err = ensure_live_basket_atomicity_supported(&unsupported_route).unwrap_err();
        assert!(err
            .to_string()
            .contains("unsupported arbitrage route BUNDLE"));

        let mut malformed_yes = executable_opp("Y");
        malformed_yes.arb_type = ArbType::Yes;
        let err = ensure_live_basket_atomicity_supported(&malformed_yes).unwrap_err();
        assert!(err.to_string().contains("malformed YES route"));

        opp.execution_plan[0].token_id = "Y".into();
        opp.arb_type = ArbType::Yes;
        let mut second_market = opp.markets[0].clone();
        second_market.question = "Q2".into();
        second_market.market_slug = "q2".into();
        second_market.condition_id = "C2".into();
        second_market.clob_token_id_yes = "Y2".into();
        second_market.clob_token_id_no = "N2".into();
        opp.markets.push(second_market);
        opp.execution_plan.push(OpportunityLeg {
            market_index: 1,
            question: "Q2".into(),
            market_slug: "q2".into(),
            condition_id: "C2".into(),
            token_id: "Y2".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.4,
        });

        let err = ensure_live_basket_atomicity_supported(&opp).unwrap_err();
        assert!(err.to_string().contains("without atomic basket fill"));
    }

    #[test]
    fn live_submit_refuses_multi_order_clob_batch_as_non_atomic() {
        ensure_single_live_clob_order_submit(1).unwrap();

        let err = ensure_single_live_clob_order_submit(2).unwrap_err();

        assert!(err
            .to_string()
            .contains("POST /orders is not an atomic basket fill"));
    }

    #[test]
    fn yes_no_live_execution_requires_clob_neg_risk_confirmation() {
        let mut opp = executable_opp("1");
        opp.arb_type = ArbType::Yes;
        assert!(ensure_yes_no_neg_risk_metadata(&opp, &opp.markets).is_ok());

        let mut missing = opp.clone();
        missing.markets[0].clob_neg_risk = None;
        assert!(ensure_yes_no_neg_risk_metadata(&missing, &missing.markets).is_err());

        let mut bundle = missing;
        bundle.arb_type = ArbType::Bundle;
        assert!(ensure_yes_no_neg_risk_metadata(&bundle, &bundle.markets).is_ok());
    }

    #[test]
    fn live_refresh_clears_gamma_seeded_neg_risk_before_clob_enrichment() {
        let opp = executable_opp("1");
        let mut markets = opp.markets.clone();
        assert_eq!(markets[0].clob_neg_risk, Some(true));

        clear_cached_neg_risk_metadata(&mut markets);

        assert_eq!(markets[0].clob_neg_risk, None);
    }

    #[tokio::test]
    async fn execute_opportunity_rejects_external_token_before_live_setup() {
        let cfg = Config::from_env();
        let client = Client::new();
        let exposure = std::sync::Arc::new(crate::exposure::ExposureTracker::new());
        let err = execute_opportunity(
            &executable_opp("external:kalshi:abc"),
            &cfg,
            &client,
            &exposure,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("external token id"));
    }

    #[test]
    fn required_quotes_present_follows_execution_plan() {
        let market = Market {
            question: "Q".into(),
            condition_id: "C".into(),
            market_slug: "q".into(),
            clob_token_id_yes: "Y".into(),
            clob_token_id_no: "N".into(),
            gamma_yes_price: 0.5,
            gamma_no_price: 0.5,
            clob_yes_ask: Some(0.4),
            clob_yes_bid: Some(0.39),
            clob_no_ask: Some(0.6),
            clob_no_bid: Some(0.59),
            clob_yes_ask_size: Some(10.0),
            clob_yes_bid_size: None,
            clob_no_ask_size: None,
            clob_no_bid_size: None,
            fees_enabled: Some(true),
            taker_fee_rate: None,
            maker_fee_rate: None,
            clob_taker_fee_bps: None,
            clob_fee_rate: None,
            clob_fee_exponent: None,
            order_price_min_tick_size: Some(0.01),
            order_min_size: Some(1.0),
            clob_tick_size: Some(0.01),
            clob_min_order_size: Some(1.0),
            clob_neg_risk: Some(true),
            clob_rfq_enabled: None,
            liquidity: 1000.0,
            closed: false,
        };
        let plan = vec![OpportunityLeg {
            market_index: 0,
            question: "Q".into(),
            market_slug: "q".into(),
            condition_id: "C".into(),
            token_id: "Y".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.4,
        }];
        assert!(required_quotes_present(
            std::slice::from_ref(&market),
            &plan
        ));

        let no_plan = vec![OpportunityLeg {
            outcome: OutcomeSide::No,
            token_id: "N".into(),
            ..plan[0].clone()
        }];
        assert!(!required_quotes_present(&[market], &no_plan));
    }

    #[test]
    fn required_quotes_present_rejects_closed_plan_market() {
        let mut market = executable_opp("Y").markets.remove(0);
        market.closed = true;
        let plan = vec![OpportunityLeg {
            market_index: 0,
            question: "Q".into(),
            market_slug: "q".into(),
            condition_id: "C".into(),
            token_id: "Y".into(),
            outcome: OutcomeSide::Yes,
            unit_shares: 1.0,
            reference_price: 0.4,
        }];

        assert!(!required_quotes_present(&[market], &plan));
    }

    #[tokio::test]
    async fn refresh_and_validate_is_scoped_to_execution_plan_quotes() {
        let server = MockServer::start_async().await;
        let yes = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "Y");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.40","size":"10"}],"bids":[{"price":"0.39","size":"10"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let no = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "N");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"bids":[{"price":"0.59","size":"10"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/C");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"C","t":[{"t":"Y","o":"Yes"},{"t":"N","o":"No"}],"mts":0.01,"mos":1,"fd":{"r":0.02,"e":2,"to":true},"nr":true,"rfqe":true,"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0,"gst":null}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.api_timeout_secs = 2;
        cfg.max_retries = 1;
        cfg.min_roi_pct = 0.0;
        let client = Client::new();
        let opp = executable_opp("Y");

        let (_markets, snapshots) = refresh_and_validate(&client, &cfg, &opp)
            .await
            .expect("YES plan leg should not require unused NO ask");

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].raw_ask, 0.40);
        yes.assert_calls_async(1).await;
        no.assert_calls_async(1).await;
        info.assert_calls_async(2).await;
    }

    #[tokio::test]
    async fn refresh_and_validate_rejects_delayed_market_info() {
        let server = MockServer::start_async().await;
        let yes = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "Y");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.40","size":"10"}],"bids":[{"price":"0.39","size":"10"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let no = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "N");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.60","size":"10"}],"bids":[{"price":"0.59","size":"10"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/C");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"mos":1,"mts":0.01,"negRisk":true,"accepting_orders":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"seconds_delay":0.5,"fd":{"r":0.02,"e":2}}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.api_timeout_secs = 2;
        cfg.max_retries = 1;
        let client = Client::new();
        let opp = executable_opp("Y");

        let err = refresh_and_validate(&client, &cfg, &opp).await.unwrap_err();

        let detail = format!("{err:#}");
        assert!(detail.contains("seconds_delay"), "{detail}");
        yes.assert_calls_async(1).await;
        no.assert_calls_async(1).await;
        info.assert_calls_async(2).await;
    }

    #[tokio::test]
    async fn refresh_and_validate_rejects_missing_market_info() {
        let server = MockServer::start_async().await;
        let yes = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "Y");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.40","size":"10"}],"bids":[{"price":"0.39","size":"10"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let no = server
            .mock_async(|when, then| {
                when.method(GET).path("/book").query_param("token_id", "N");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.60","size":"10"}],"bids":[{"price":"0.59","size":"10"}],"tick_size":"0.01","min_order_size":"1","neg_risk":true}"#);
            })
            .await;
        let info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/C");
                then.status(503).body("unavailable");
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.clob_api_url = server.base_url();
        cfg.api_timeout_secs = 2;
        cfg.max_retries = 1;
        let client = Client::new();
        let opp = executable_opp("Y");

        let err = refresh_and_validate(&client, &cfg, &opp).await.unwrap_err();

        assert!(err
            .to_string()
            .contains("fresh orderable CLOB market metadata"));
        yes.assert_calls_async(1).await;
        no.assert_calls_async(1).await;
        info.assert_calls_async(2).await;
    }

    #[test]
    fn stale_signal_is_rejected() {
        let mut cfg = Config::from_env();
        cfg.max_signal_age_secs = 5;
        let opp = ArbitrageOpportunity {
            event_title: "E".into(),
            event_id: "1".into(),
            category: "sports".into(),
            arb_type: ArbType::Bundle,
            markets: vec![],
            execution_plan: vec![],
            total_cost: 0.8,
            guaranteed_revenue: 1.0,
            gross_profit: 0.2,
            total_fees: 0.0,
            net_profit: 0.19,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 10.0,
            prices_from_clob: true,
            max_executable_size_usd: 10.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: chrono::Utc::now() - chrono::Duration::seconds(30),
        };
        assert!(ensure_signal_fresh(&opp, &cfg).is_err());
    }

    #[test]
    fn final_depth_freshness_accepts_current_server_time() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 1_000;
        let server_clock = ServerClock {
            offset_ms: 0,
            uncertainty_ms: 0,
        };
        let snapshot = depth_snapshot_with_timestamp(local_unix_ms().unwrap() as u64);

        let age = ensure_final_depth_fresh(&snapshot, &server_clock, &cfg).unwrap();

        assert!((0..=1_000).contains(&age));
    }

    #[test]
    fn final_depth_freshness_rejects_stale_books() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 50;
        let server_clock = ServerClock {
            offset_ms: 0,
            uncertainty_ms: 0,
        };
        let snapshot = depth_snapshot_with_timestamp((local_unix_ms().unwrap() - 500) as u64);

        let err = ensure_final_depth_fresh(&snapshot, &server_clock, &cfg).unwrap_err();

        assert!(err.to_string().contains("CLOB final depth /books stale"));
    }

    #[test]
    fn final_depth_freshness_rejects_future_books() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 50;
        let server_clock = ServerClock {
            offset_ms: 0,
            uncertainty_ms: 0,
        };
        let snapshot = depth_snapshot_with_timestamp((local_unix_ms().unwrap() + 500) as u64);

        let err = ensure_final_depth_fresh(&snapshot, &server_clock, &cfg).unwrap_err();

        assert!(err
            .to_string()
            .contains("CLOB final depth /books future timestamp"));
    }

    #[test]
    fn final_depth_freshness_uses_clock_uncertainty() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 100;
        let server_clock = ServerClock {
            offset_ms: 0,
            uncertainty_ms: 50,
        };
        let snapshot = depth_snapshot_with_timestamp((local_unix_ms().unwrap() - 75) as u64);

        let err = ensure_final_depth_fresh(&snapshot, &server_clock, &cfg).unwrap_err();

        assert!(err.to_string().contains("clock_uncertainty=50ms"));
    }

    #[test]
    fn final_depth_rules_reject_exchange_metadata_drift() {
        let market = executable_opp("1").markets.remove(0);
        let mut snapshot = depth_snapshot_with_timestamp(local_unix_ms().unwrap() as u64);
        snapshot.tick_size = Some(0.001);

        let err = ensure_final_depth_rules_match(&snapshot, &market, "Q").unwrap_err();

        assert!(err
            .to_string()
            .contains("CLOB final depth /books tick size drift"));
    }

    #[test]
    fn stale_final_refresh_is_rejected_before_submit() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 50;
        let started = Instant::now() - Duration::from_millis(75);

        let err = ensure_submit_fresh(started, &cfg).unwrap_err();
        assert!(err.to_string().contains("aborted before submit"));
        assert!(err
            .to_string()
            .contains("LIVE_MAX_REFRESH_TO_SUBMIT_MS=50ms"));
    }
}
