//! Centralized configuration for the arbitrage scanner.
//!
//! All settings can be overridden via environment variables.
//! Create a `.env` file in the project root for local development.

use std::collections::HashMap;
use std::path::PathBuf;

use alloy::primitives::keccak256;
use serde::Serialize;
use serde_json::{Map, Value};

const DEFAULT_PREDICTION_MARKET_SOURCES: &str = "polymarket,kalshi,manifold,seer,sxbet";
const DEFAULT_CLOB_BOOK_BATCH_SIZE: u64 = 150;
const DEFAULT_CLOB_BOOK_BATCH_PAUSE_MS: u64 = 0;
pub const DEFAULT_DIAGNOSTICS_CSV_MAX_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL: &str =
    "wss://combos-rfq-gateway-quoter.polymarket.sh/ws/rfq";
pub const COMBO_RFQ_GATEWAY_WSS_URLS: &[&str] = &[
    DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL,
    "wss://combos-rfq-gateway-quoter.polymarket.com/ws/rfq",
];

/// Scanner configuration loaded from environment variables.
#[derive(Debug, Clone, Serialize)]
pub struct Config {
    // API endpoints
    pub gamma_api_url: String,
    pub clob_api_url: String,
    pub polymarket_data_api_url: String,
    pub polymarket_status_api_url: String,
    pub polymarket_status_components_api_url: String,
    pub combo_rfq_api_url: String,
    pub combo_rfq_requester_api_url: String,
    pub relayer_api_url: String,
    pub relayer_api_key: String,
    pub relayer_api_key_address: String,
    pub relayer_wallet_deadline_secs: u64,
    pub combo_rfq_requester_enabled: bool,
    pub combo_rfq_accept_enabled: bool,
    pub combo_rfq_requester_protocol_verified: bool,
    pub combo_rfq_bearer_token: String,
    pub combo_rfq_participant_id: String,
    pub combo_rfq_quote_max_age_ms: u64,
    pub combo_rfq_microprice_adverse_bps: f64,
    pub combo_rfq_markout_race_score_horizon_ms: u64,
    pub combo_rfq_markout_race_min_samples: usize,
    pub combo_rfq_markout_race_max_age_secs: u64,
    pub combo_rfq_markout_race_max_adverse_bps: f64,
    pub combo_rfq_exchange_v3_address: String,
    pub combo_rfq_finality_max_age_secs: u64,
    pub combo_rfq_finality_min_confirmed_samples: usize,
    pub combo_rfq_counterparty_min_settlement_samples: usize,
    pub combo_rfq_stream_enabled: bool,
    pub combo_rfq_gateway_wss_url: String,
    pub combo_rfq_grpc_url: String,
    pub combo_rfq_stream_bearer_token: String,
    pub combo_rfq_stream_reconnect_backoff_ms: u64,
    pub kalshi_api_url: String,
    pub manifold_api_url: String,
    pub predictit_api_url: String,
    pub limitless_api_url: String,
    pub seer_api_url: String,
    pub sxbet_api_url: String,
    pub sxbet_base_token: String,
    pub betdex_api_url: String,
    pub betdex_auth_token: String,
    pub prediction_market_sources: Vec<String>,

    // Scanner behaviour
    pub scan_interval_secs: u64,
    pub max_events_to_fetch: u64,
    pub min_liquidity_usd: f64,
    pub min_net_profit_usd: f64,
    pub min_roi_pct: f64,
    /// How often to refresh event discovery data from Gamma (seconds).
    pub discovery_interval_secs: u64,
    pub api_timeout_secs: u64,
    pub use_clob_prices: bool,
    pub use_websocket: bool,
    pub clob_ws_url: String,
    pub clob_user_ws_url: String,
    /// Target maximum token subscriptions per WebSocket connection.
    pub ws_shard_size: usize,
    /// Maximum time to wait for initial WebSocket book snapshots after subscribing; 0 starts REST fallback immediately.
    pub ws_initial_snapshot_timeout_ms: u64,
    /// Minimum fraction of desired active-slice tokens that should have snapshot-ready WS cache.
    pub ws_min_snapshot_coverage_pct: f64,
    /// Maximum age of a WebSocket quote snapshot before REST fallback is required (milliseconds).
    pub ws_quote_max_age_ms: u64,
    /// Close and reconnect a market-data shard if subscribed assets receive no market events for this long.
    pub ws_market_data_silence_timeout_ms: u64,
    /// Minimum fraction of legs that must have live CLOB quotes in CLOB mode.
    pub min_clob_quote_coverage_pct: f64,
    /// If true, re-run detection with Gamma estimates when CLOB-mode finds no edge.
    pub enable_gamma_fallback_when_no_clob_edge: bool,
    /// If true, only opportunities priced fully from fresh CLOB quotes may be executed.
    pub execute_only_full_clob_prices: bool,
    /// Maximum signal age allowed for paper/live execution.
    pub max_signal_age_secs: u64,
    /// If true, skip candidate events whose end/start lifecycle cutoff is near or already passed.
    pub event_lifecycle_gate_enabled: bool,
    /// Stop scanning an event this many seconds before its known end/start lifecycle cutoff.
    pub event_lifecycle_pre_cutoff_buffer_secs: u64,
    /// Enable ranked-family arbitrage optimization.
    pub enable_ranked_arbitrage: bool,
    /// If true, scan non-neg-risk binary markets for bundle (YES+NO) arbitrage too.
    pub enable_bundle_scanning_all_events: bool,
    /// If true, fetch the public Combo/RFQ market catalog and annotate non-atomic opportunities.
    pub combo_rfq_discovery_enabled: bool,
    /// Maximum combo-able markets to keep in the read-only RFQ route catalog.
    pub combo_rfq_max_markets: usize,
    pub allow_augmented_neg_risk: bool,
    /// Maximum number of in-flight REST requests for per-event validation paths.
    pub clob_max_concurrency: usize,
    /// Maximum number of token ids to batch into a single POST /books request.
    pub clob_book_batch_size: usize,
    /// Multiplier that caps how large the continuously-scanned active slice may grow
    /// relative to the per-scan REST refresh budget.
    pub active_slice_token_budget_multiplier: usize,
    /// Pause inserted between scan-time POST /books batches to reduce 429 bursts.
    pub clob_book_batch_pause_ms: u64,
    /// Maximum number of unresolved token ids to refresh from REST /books per scan.
    /// Remaining tokens stay cache-only and rotate into later scans.
    pub quote_refresh_token_budget_per_scan: usize,
    /// Soft cap on total quote tokens kept in the active scan slice / WS watchlist.
    /// This prevents a warmed cache from exploding the tracked universe far beyond the
    /// per-scan refresh budget.
    pub active_quote_token_budget_per_scan: usize,
    /// Maximum number of neg-risk events actively scanned per cycle. A sticky head of
    /// top-priority events is always scanned; the remainder rotates to widen coverage.
    pub scan_neg_risk_event_budget: usize,
    /// Maximum number of non-neg-risk events actively scanned for bundle arbs per cycle.
    pub scan_bundle_event_budget: usize,
    /// Number of unresolved token previews to include in warning logs.
    pub quote_shortfall_sample_size: usize,
    pub opportunity_dedupe_cooldown_secs: u64,
    /// Number of scans to keep the rotating tail stable before moving to a new slice.
    pub scan_rotation_period_scans: u64,
    /// Fraction of each candidate budget kept sticky at the head of the ranking.
    /// The remainder rotates continuously to widen coverage across the universe.
    pub selection_sticky_fraction: f64,
    /// If true, candidate ranking favors faster capital return from known lifecycle end dates.
    pub capital_velocity_ranking_enabled: bool,
    /// Reference lock duration used by capital-velocity ranking.
    pub capital_velocity_reference_hours: f64,
    /// Score weight applied to edge hints when capital lock differs from the reference.
    pub capital_velocity_score_weight: f64,
    /// Maximum number of legs allowed for YES/NO opportunities.
    /// Very large baskets are hard to execute reliably in practice.
    pub max_opportunity_legs: usize,

    // Fee model: fallback theta values by category.
    // Used only when per-market fee metadata is absent.
    pub fee_theta_by_category: HashMap<String, f64>,
    pub fee_theta_default: f64,

    // Gas cost estimation
    /// Fallback gas cost per arb trade in USD, used when the live Polygon gas
    /// oracle or POL/USD price feed is unavailable.
    pub gas_fallback_usd: f64,
    /// If true, treat proxy / safe signature types as gasless for per-trade ROI filtering.
    pub assume_gasless_for_proxy_signature_types: bool,

    // Paper trading
    pub paper_trading_enabled: bool,
    pub paper_trade_position_size_usd: f64,
    /// If true, the paper engine uses LIVE_TRADE_POSITION_SIZE_USD by default.
    pub paper_match_live_position_size: bool,
    /// If true, paper execution is allowed only when every leg has live CLOB quotes.
    pub paper_require_full_clob_quotes: bool,
    /// If true, external paper execution uses limit orders + polling to mirror live trading.
    pub paper_use_limit_orders: bool,
    pub paper_trade_history_limit: usize,
    /// Maximum tolerated basket-share mismatch in paper mode before a fill is
    /// treated as non-representative of live execution (percent, e.g. 0.5 = 0.5%).
    pub paper_max_share_mismatch_pct: f64,
    pub dry_run_provider: String,
    pub external_paper_command: String,
    pub external_paper_data_dir: PathBuf,
    pub external_paper_account: String,
    pub external_paper_init_balance_usd: f64,
    /// Market-style fallback order behavior used when PAPER_USE_LIMIT_ORDERS=false (fok|fak).
    pub external_paper_order_type: String,
    /// Limit-order behavior used in parity mode (typically gtc).
    pub external_paper_limit_order_type: String,
    pub external_paper_min_order_usd: f64,

    // Strategy lab / parallel paper research
    pub strategy_lab_enabled: bool,
    pub strategy_lab_refresh_interval_secs: u64,
    pub strategy_lab_market_limit: usize,
    pub strategy_lab_initial_capital_usd: f64,
    pub strategy_lab_position_size_usd: f64,
    pub strategy_lab_max_positions_per_strategy: usize,
    pub strategy_lab_candidate_cap_per_strategy: usize,

    // Live execution
    pub live_trading_enabled: bool,
    /// If true, emit live-route readiness diagnostics while keeping live submissions disabled.
    pub live_diagnostics_enabled: bool,
    /// Hard opt-in for the Combo/RFQ live route. The route still requires the
    /// full promotion report to pass before startup permits live submissions.
    pub live_combo_rfq_route_enabled: bool,
    /// Minimum labeled replay samples required before a live route calibration can pass.
    pub live_route_calibration_min_samples: usize,
    /// Maximum age for replay labels used by live route calibration gates.
    pub live_route_calibration_max_age_secs: u64,
    pub live_trade_position_size_usd: f64,
    pub live_chain_id: u64,
    pub live_signature_type: u8,
    pub live_funder_address: String,
    /// Slippage allowance added to each leg's limit price (basis points, 1 bp = 0.01%).
    pub live_slippage_bps: u32,
    /// Block live taker buys when the executable ask is this many bps above WS microprice.
    pub live_clob_microprice_adverse_bps: f64,
    /// How long to wait for a fill confirmation before declaring failure (seconds).
    pub live_fill_poll_timeout_secs: u64,
    /// How often to poll order status during fill confirmation (milliseconds).
    pub live_fill_poll_interval_ms: u64,
    /// Maximum time allowed between final quote refresh start and live submit (milliseconds).
    pub live_max_refresh_to_submit_ms: u64,
    /// Maximum tolerated uncertainty in the CLOB server-clock sync before live trading blocks.
    pub live_max_server_clock_uncertainty_ms: u64,
    /// Maximum tolerated absolute local-vs-CLOB clock offset before live trading blocks.
    pub live_max_server_clock_offset_ms: u64,
    /// If true, no-submit diagnostics polls the public Polymarket status page and feeds
    /// active incidents/maintenance into the live engine-mode blocker.
    pub live_status_page_enabled: bool,
    /// Block live ahead of scheduled status-page maintenance this many seconds before start.
    pub live_status_page_maintenance_prehalt_secs: u64,
    /// If true, live startup fetches the Data API accounting snapshot ZIP and fails closed
    /// when it cannot prove the account has no retained position rows.
    pub live_accounting_snapshot_enabled: bool,
    /// Maximum non-dust position rows allowed in the accounting snapshot before live blocks.
    pub live_accounting_snapshot_max_position_rows: usize,
    /// Block live taker submission when CLOB market info reports a game start inside this window.
    pub live_game_start_quarantine_secs: u64,
    /// Minimum leg size in USD; legs below this are skipped (liquidity guard).
    pub live_min_leg_size_usd: f64,
    /// Maximum total exposure per event across all open live positions (USD).
    pub live_max_event_exposure_usd: f64,
    /// Maximum total retained exposure across all live positions in this process (USD).
    pub live_max_total_exposure_usd: f64,
    /// If true, cancel any still-open orders when fill polling times out or a leg fails.
    pub live_cancel_on_fill_timeout: bool,
    /// If true, pre-submit kill-switch hazards may cancel every open order for the account.
    pub live_cancel_all_on_kill_switch: bool,
    /// Live order type used by the official SDK for basket legs. Live arbitrage execution requires FOK.
    pub live_order_type: String,
    /// If true, run an authenticated user-channel websocket capturer alongside live mode.
    pub live_user_ws_enabled: bool,
    /// If true, require a fresh authenticated heartbeat ACK before live order submission.
    pub live_pre_submit_heartbeat_enabled: bool,
    /// Maximum time to wait for the pre-submit heartbeat ACK.
    pub live_pre_submit_heartbeat_timeout_ms: u64,
    /// Fixed safety haircut subtracted from live projected PnL before submit gating.
    pub live_edge_haircut_usd: f64,
    /// Cost-proportional safety haircut subtracted from live projected PnL before submit gating.
    pub live_edge_haircut_bps: u32,
    /// If true, the closeout runner may process account closeout actions.
    /// Current runner output is read-only unless a future executor is explicitly implemented.
    pub live_closeout_enabled: bool,
    /// If true, closeout runs only write an execution-intent report and never send transactions.
    pub live_closeout_dry_run: bool,
    /// Maximum closeout actions selected by a single closeout run.
    pub live_closeout_max_actions_per_run: usize,
    /// Maximum time to wait for post-closeout position confirmation once execution exists.
    pub live_closeout_confirm_timeout_secs: u64,
    /// Polygon RPC endpoint reserved for on-chain closeout/redeem execution.
    pub polygon_rpc_url: String,
    /// Maximum tolerated latest-minus-finalized block lag for live on-chain finality gates.
    pub polygon_finalized_block_max_lag_blocks: u64,
    /// If true, diagnostics fetch recent Exchange V3 OrderFilled logs into the RFQ finality inbox.
    pub onchain_order_filled_collector_enabled: bool,
    /// Recent Polygon block lookback used by the bounded OrderFilled diagnostics collector.
    pub onchain_order_filled_collector_lookback_blocks: u64,
    /// If true, live route promotion requires recent settlement receipt hazard diagnostics.
    pub settlement_monitor_enabled: bool,
    /// Minimum recent settlement receipts required before the revert hazard gate can pass.
    pub settlement_revert_hazard_min_samples: usize,
    /// Maximum tolerated failed/reverted settlement receipt rate.
    pub settlement_revert_hazard_max_rate: f64,
    /// Maximum age for receipt samples used by the settlement hazard gate.
    pub settlement_receipt_max_age_secs: u64,
    /// Share-quantity rounding step applied to live/paper basket sizing.
    pub order_size_step_shares: f64,

    /// If true, reprice candidate opportunities at the actual intended basket size before notifying or executing.
    pub validate_opportunities_at_target_size: bool,

    // Notifications
    pub webhook_url: String,

    // Logging / operator UX
    pub log_level: String,
    pub verbose_scan_logs: bool,
    pub diagnostics_csv_enabled: bool,
    pub diagnostics_dir: PathBuf,
    /// Maximum size of each rolling high-volume diagnostics CSV generation.
    pub diagnostics_csv_max_bytes: u64,
    pub diagnostics_log_all_candidate_evaluations: bool,
    pub diagnostics_log_routine_rejections: bool,

    // Retry / resilience
    pub max_retries: u32,
    pub retry_backoff_base_ms: u64,
}

const DIRECT_LIVE_IDENTITY_ENVS: &[&str] = &[
    "BETDEX_AUTH_TOKEN",
    "CLOB_API_KEY",
    "CLOB_PASSPHRASE",
    "CLOB_PASS_PHRASE",
    "CLOB_SECRET",
    "COMBO_RFQ_BEARER_TOKEN",
    "COMBO_RFQ_PARTICIPANT_ID",
    "COMBO_RFQ_STREAM_BEARER_TOKEN",
    "LIVE_FUNDER_ADDRESS",
    "LIVE_SIGNER_ADDRESS",
    "POLYGON_RPC_URL",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_PASSPHRASE",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_PRIVATE_KEY",
    "RELAYER_API_KEY",
    "RELAYER_API_KEY_ADDRESS",
    "WEBHOOK_URL",
];

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DirectLiveIdentityFingerprint {
    pub name: &'static str,
    pub present: bool,
}

/// Redacted, Config-derived subset used to compare paper execution evidence with
/// the settings that would govern a guarded live launch. Runtime producer and
/// adapter hashes, plus per-attempt freshness proofs, are recorded separately by
/// the paper execution journal.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PaperLiveProfileConfig {
    pub schema_version: u32,
    pub execution_route: &'static str,
    pub order_mode: &'static str,
    pub effective_order_type: String,
    pub live_order_type: String,
    pub paper_use_limit_orders_requested: bool,
    pub effective_paper_use_limit_orders: bool,
    pub full_clob_required: bool,
    pub match_live_position_size: bool,
    pub effective_position_size_usd: f64,
    pub live_position_size_usd: f64,
    pub paper_max_share_mismatch_pct: f64,
    pub min_net_profit_usd: f64,
    pub min_roi_pct: f64,
    pub max_signal_age_secs: u64,
    pub gas_fallback_usd: f64,
    pub assume_gasless_for_proxy_signature_types: bool,
    pub live_signature_type: u8,
    pub order_size_step_shares: f64,
    pub validate_opportunities_at_target_size: bool,
    pub execute_only_full_clob_prices: bool,
    pub live_slippage_bps: u32,
    pub live_edge_haircut_usd: f64,
    pub live_edge_haircut_bps: u32,
    pub live_min_leg_size_usd: f64,
    pub live_max_refresh_to_submit_ms: u64,
    pub clob_api_url: String,
    pub gamma_api_url: String,
    pub external_paper_command: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LaunchConfigFingerprint {
    pub schema_version: u32,
    pub algorithm: &'static str,
    pub config_field_count: usize,
    pub config_fingerprint: String,
    pub direct_live_identity_fingerprint: String,
    pub combined_fingerprint: String,
    pub profit_compatibility_fingerprint: String,
    pub direct_live_identities: Vec<DirectLiveIdentityFingerprint>,
    pub paper_live_profile_config: PaperLiveProfileConfig,
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let mut entries: Vec<_> = values.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut canonical = Map::new();
            for (key, value) in entries {
                canonical.insert(key, canonical_json(value));
            }
            Value::Object(canonical)
        }
        scalar => scalar,
    }
}

fn domain_hash(domain: &str, payload: &[u8]) -> String {
    let mut bytes = Vec::with_capacity(domain.len() + payload.len() + 1);
    bytes.extend_from_slice(domain.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(payload);
    format!("{:#x}", keccak256(bytes))
}

impl Config {
    pub fn profit_compatibility_fingerprint(&self) -> Result<String, serde_json::Error> {
        let mut normalized = self.clone();

        // Operational mode differs by construction between evidence collection,
        // no-submit operator diagnostics, and guarded live startup.
        normalized.paper_trading_enabled = false;
        normalized.live_trading_enabled = false;
        normalized.live_diagnostics_enabled = false;
        normalized.strategy_lab_enabled = false;
        normalized.live_combo_rfq_route_enabled = false;
        normalized.combo_rfq_requester_enabled = false;
        normalized.combo_rfq_accept_enabled = false;
        normalized.combo_rfq_requester_protocol_verified = false;
        normalized.combo_rfq_stream_enabled = false;
        normalized.live_user_ws_enabled = false;
        normalized.onchain_order_filled_collector_enabled = false;
        normalized.settlement_monitor_enabled = false;
        normalized.live_closeout_enabled = false;
        normalized.live_closeout_dry_run = false;

        // Identity/credential values are bound separately without disclosure.
        normalized.relayer_api_key.clear();
        normalized.relayer_api_key_address.clear();
        normalized.combo_rfq_bearer_token.clear();
        normalized.combo_rfq_participant_id.clear();
        normalized.combo_rfq_stream_bearer_token.clear();
        normalized.betdex_auth_token.clear();
        normalized.live_funder_address.clear();
        normalized.polygon_rpc_url.clear();
        normalized.webhook_url.clear();

        // Paths, account labels, adapter identity, and logging are attested by
        // their dedicated artifacts and do not change trading economics.
        normalized.external_paper_command.clear();
        normalized.external_paper_data_dir = PathBuf::new();
        normalized.external_paper_account.clear();
        normalized.diagnostics_dir = PathBuf::new();
        normalized.log_level.clear();
        normalized.verbose_scan_logs = false;
        normalized.diagnostics_csv_enabled = false;
        normalized.diagnostics_csv_max_bytes = 0;
        normalized.diagnostics_log_all_candidate_evaluations = false;
        normalized.diagnostics_log_routine_rejections = false;

        let canonical = canonical_json(serde_json::to_value(&normalized)?);
        let payload = serde_json::to_vec(&canonical)?;
        Ok(domain_hash("polymarket-profit-compatibility-v1", &payload))
    }

    pub fn paper_live_profile_config(&self) -> PaperLiveProfileConfig {
        let effective_paper_use_limit_orders = self.effective_paper_use_limit_orders();
        PaperLiveProfileConfig {
            schema_version: 1,
            execution_route: "legged_clob_paper",
            order_mode: if effective_paper_use_limit_orders {
                "limit"
            } else {
                "market_style"
            },
            effective_order_type: if effective_paper_use_limit_orders {
                self.external_paper_limit_order_type
                    .trim()
                    .to_ascii_lowercase()
            } else {
                self.external_paper_order_type.trim().to_ascii_lowercase()
            },
            live_order_type: self.live_order_type.trim().to_ascii_lowercase(),
            paper_use_limit_orders_requested: self.paper_use_limit_orders,
            effective_paper_use_limit_orders,
            full_clob_required: self.paper_require_full_clob_quotes,
            match_live_position_size: self.paper_match_live_position_size,
            effective_position_size_usd: self.effective_paper_position_size_usd(),
            live_position_size_usd: self.live_trade_position_size_usd,
            paper_max_share_mismatch_pct: self.paper_max_share_mismatch_pct,
            min_net_profit_usd: self.min_net_profit_usd,
            min_roi_pct: self.min_roi_pct,
            max_signal_age_secs: self.max_signal_age_secs,
            gas_fallback_usd: self.gas_fallback_usd,
            assume_gasless_for_proxy_signature_types: self.assume_gasless_for_proxy_signature_types,
            live_signature_type: self.live_signature_type,
            order_size_step_shares: self.order_size_step_shares,
            validate_opportunities_at_target_size: self.validate_opportunities_at_target_size,
            execute_only_full_clob_prices: self.execute_only_full_clob_prices,
            live_slippage_bps: self.live_slippage_bps,
            live_edge_haircut_usd: self.live_edge_haircut_usd,
            live_edge_haircut_bps: self.live_edge_haircut_bps,
            live_min_leg_size_usd: self.live_min_leg_size_usd,
            live_max_refresh_to_submit_ms: self.live_max_refresh_to_submit_ms,
            clob_api_url: self.clob_api_url.clone(),
            gamma_api_url: self.gamma_api_url.clone(),
            external_paper_command: self.external_paper_command.clone(),
        }
    }

    pub fn launch_config_fingerprint(&self) -> Result<LaunchConfigFingerprint, serde_json::Error> {
        let mut normalized_config = self.clone();
        // The operator proof is generated with live submission disabled, while the guarded
        // launcher separately requires it enabled. Keep the field present but normalize this
        // single activation toggle so the fingerprint binds every other effective setting.
        normalized_config.live_trading_enabled = false;
        let canonical_config = canonical_json(serde_json::to_value(&normalized_config)?);
        let config_field_count = canonical_config
            .as_object()
            .map(|fields| fields.len())
            .unwrap_or_default();
        let config_bytes = serde_json::to_vec(&canonical_config)?;
        let config_fingerprint = domain_hash("polymarket-launch-config-v1", &config_bytes);

        let mut direct_live_identities = Vec::with_capacity(DIRECT_LIVE_IDENTITY_ENVS.len());
        let mut internal_identity_hashes = Vec::with_capacity(DIRECT_LIVE_IDENTITY_ENVS.len());
        for name in DIRECT_LIVE_IDENTITY_ENVS {
            let value = std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty());
            let value_payload = match value.as_deref() {
                Some(value) => format!("{name}\0present\0{value}"),
                None => format!("{name}\0absent"),
            };
            internal_identity_hashes.push((
                *name,
                value.is_some(),
                domain_hash(
                    "polymarket-direct-live-identity-v1",
                    value_payload.as_bytes(),
                ),
            ));
            direct_live_identities.push(DirectLiveIdentityFingerprint {
                name,
                present: value.is_some(),
            });
        }
        let identity_bytes = serde_json::to_vec(&internal_identity_hashes)?;
        let direct_live_identity_fingerprint =
            domain_hash("polymarket-direct-live-identities-v1", &identity_bytes);
        let combined_payload = format!("{config_fingerprint}\0{direct_live_identity_fingerprint}");
        let combined_fingerprint = domain_hash(
            "polymarket-launch-fingerprint-v1",
            combined_payload.as_bytes(),
        );
        let profit_compatibility_fingerprint = self.profit_compatibility_fingerprint()?;

        Ok(LaunchConfigFingerprint {
            schema_version: 1,
            algorithm: "keccak256-domain-separated-v1",
            config_field_count,
            config_fingerprint,
            direct_live_identity_fingerprint,
            combined_fingerprint,
            profit_compatibility_fingerprint,
            direct_live_identities,
            paper_live_profile_config: self.paper_live_profile_config(),
        })
    }

    /// Load configuration from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        let mut fee_theta: HashMap<String, f64> = HashMap::new();
        fee_theta.insert("crypto".into(), 0.070);
        fee_theta.insert("economics".into(), 0.050);
        fee_theta.insert("culture".into(), 0.050);
        fee_theta.insert("weather".into(), 0.050);
        fee_theta.insert("finance".into(), 0.040);
        fee_theta.insert("politics".into(), 0.040);
        fee_theta.insert("mentions".into(), 0.040);
        fee_theta.insert("tech".into(), 0.040);
        fee_theta.insert("sports".into(), 0.030);
        fee_theta.insert("other".into(), 0.050);
        fee_theta.insert("general".into(), 0.050);
        fee_theta.insert("other-general".into(), 0.050);
        fee_theta.insert("geopolitics".into(), 0.000);
        fee_theta.insert("world-events".into(), 0.000);

        let quote_refresh_token_budget_per_scan =
            env_u64("QUOTE_REFRESH_TOKEN_BUDGET_PER_SCAN", 320).max(1) as usize;
        let active_quote_token_budget_per_scan = env_u64(
            "ACTIVE_QUOTE_TOKEN_BUDGET_PER_SCAN",
            quote_refresh_token_budget_per_scan.saturating_mul(2) as u64,
        )
        .max(quote_refresh_token_budget_per_scan as u64)
            as usize;
        let scan_rotation_period_scans = env_u64("SCAN_ROTATION_PERIOD_SCANS", 4).max(1);
        let selection_sticky_fraction = env_f64("SELECTION_STICKY_FRACTION", 0.35).clamp(0.0, 0.95);
        let combo_rfq_grpc_url = env_str("COMBO_RFQ_GRPC_URL", "");
        let combo_rfq_gateway_wss_default = if combo_rfq_grpc_url.trim().is_empty() {
            DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL
        } else {
            combo_rfq_grpc_url.as_str()
        };
        let combo_rfq_gateway_wss_url =
            env_str("COMBO_RFQ_GATEWAY_WSS_URL", combo_rfq_gateway_wss_default);

        Self {
            gamma_api_url: env_str("GAMMA_API_URL", "https://gamma-api.polymarket.com"),
            clob_api_url: env_str("CLOB_API_URL", "https://clob.polymarket.com"),
            polymarket_data_api_url: env_str(
                "POLYMARKET_DATA_API_URL",
                "https://data-api.polymarket.com",
            ),
            polymarket_status_api_url: env_str(
                "POLYMARKET_STATUS_API_URL",
                "https://status.polymarket.com/v3/summary.json",
            ),
            polymarket_status_components_api_url: env_str(
                "POLYMARKET_STATUS_COMPONENTS_API_URL",
                "https://status.polymarket.com/v3/components.json",
            ),
            combo_rfq_api_url: env_str("COMBO_RFQ_API_URL", "https://combos-rfq-api.polymarket.sh"),
            combo_rfq_requester_api_url: env_str(
                "COMBO_RFQ_REQUESTER_API_URL",
                "https://api.polymarket.us",
            ),
            relayer_api_url: env_str("RELAYER_API_URL", "https://relayer-v2.polymarket.com"),
            relayer_api_key: env_str("RELAYER_API_KEY", ""),
            relayer_api_key_address: env_str("RELAYER_API_KEY_ADDRESS", ""),
            relayer_wallet_deadline_secs: env_u64("RELAYER_WALLET_DEADLINE_SECONDS", 300).max(30),
            combo_rfq_requester_enabled: env_bool("COMBO_RFQ_REQUESTER_ENABLED", false),
            combo_rfq_accept_enabled: env_bool("COMBO_RFQ_ACCEPT_ENABLED", false),
            combo_rfq_requester_protocol_verified: env_bool(
                "COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED",
                false,
            ),
            combo_rfq_bearer_token: env_str("COMBO_RFQ_BEARER_TOKEN", ""),
            combo_rfq_participant_id: env_str("COMBO_RFQ_PARTICIPANT_ID", ""),
            combo_rfq_quote_max_age_ms: env_u64("COMBO_RFQ_QUOTE_MAX_AGE_MS", 400).max(1),
            combo_rfq_microprice_adverse_bps: env_f64("COMBO_RFQ_MICROPRICE_ADVERSE_BPS", 1.0)
                .max(0.0),
            combo_rfq_markout_race_score_horizon_ms: env_u64(
                "COMBO_RFQ_MARKOUT_RACE_SCORE_HORIZON_MS",
                250,
            )
            .max(1),
            combo_rfq_markout_race_min_samples: env_u64("COMBO_RFQ_MARKOUT_RACE_MIN_SAMPLES", 3)
                .max(1) as usize,
            combo_rfq_markout_race_max_age_secs: env_u64(
                "COMBO_RFQ_MARKOUT_RACE_MAX_AGE_SECS",
                3_600,
            )
            .max(1),
            combo_rfq_markout_race_max_adverse_bps: env_f64(
                "COMBO_RFQ_MARKOUT_RACE_MAX_ADVERSE_BPS",
                1.0,
            )
            .max(0.0),
            combo_rfq_exchange_v3_address: env_str(
                "COMBO_RFQ_EXCHANGE_V3_ADDRESS",
                "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa",
            ),
            combo_rfq_finality_max_age_secs: env_u64("COMBO_RFQ_FINALITY_MAX_AGE_SECS", 300).max(1),
            combo_rfq_finality_min_confirmed_samples: env_u64(
                "COMBO_RFQ_FINALITY_MIN_CONFIRMED_SAMPLES",
                3,
            )
            .max(1) as usize,
            combo_rfq_counterparty_min_settlement_samples: env_u64(
                "COMBO_RFQ_COUNTERPARTY_MIN_SETTLEMENT_SAMPLES",
                3,
            )
            .max(1) as usize,
            combo_rfq_stream_enabled: env_bool("COMBO_RFQ_STREAM_ENABLED", false),
            combo_rfq_gateway_wss_url,
            combo_rfq_grpc_url,
            combo_rfq_stream_bearer_token: env_str("COMBO_RFQ_STREAM_BEARER_TOKEN", ""),
            combo_rfq_stream_reconnect_backoff_ms: env_u64(
                "COMBO_RFQ_STREAM_RECONNECT_BACKOFF_MS",
                1_000,
            )
            .max(100),
            kalshi_api_url: env_str(
                "KALSHI_API_URL",
                "https://external-api.kalshi.com/trade-api/v2",
            ),
            manifold_api_url: env_str("MANIFOLD_API_URL", "https://api.manifold.markets/v0"),
            predictit_api_url: env_str(
                "PREDICTIT_API_URL",
                "https://www.predictit.org/api/marketdata/all/",
            ),
            limitless_api_url: env_str("LIMITLESS_API_URL", "https://api.limitless.exchange"),
            seer_api_url: env_str("SEER_API_URL", "https://app.seer.pm/.netlify/functions"),
            sxbet_api_url: env_str("SXBET_API_URL", "https://api.sx.bet"),
            sxbet_base_token: env_str(
                "SXBET_BASE_TOKEN",
                "0x6629Ce1Cf35Cc1329ebB4F63202F3f197b3F050B",
            ),
            betdex_api_url: env_str("BETDEX_API_URL", "https://prod.api.btdx.io"),
            betdex_auth_token: env_str("BETDEX_AUTH_TOKEN", ""),
            prediction_market_sources: env_list(
                "PREDICTION_MARKET_SOURCES",
                DEFAULT_PREDICTION_MARKET_SOURCES,
            ),

            scan_interval_secs: env_u64("SCAN_INTERVAL_SECONDS", 1),
            max_events_to_fetch: env_u64("MAX_EVENTS_TO_FETCH", 500),
            min_liquidity_usd: env_f64("MIN_LIQUIDITY_USD", 1000.0),
            min_net_profit_usd: env_f64("MIN_NET_PROFIT_USD", 1.0),
            min_roi_pct: env_f64("MIN_ROI_PCT", 1.0),
            discovery_interval_secs: env_u64("DISCOVERY_INTERVAL_SECONDS", 30),
            api_timeout_secs: env_u64("API_TIMEOUT_SECONDS", 15),
            use_clob_prices: env_bool("USE_CLOB_PRICES", true),
            use_websocket: env_bool("USE_WEBSOCKET", true),
            clob_ws_url: env_str(
                "CLOB_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ),
            clob_user_ws_url: env_str(
                "CLOB_USER_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/user",
            ),
            ws_shard_size: env_u64("WS_SHARD_SIZE", 200).max(1) as usize,
            ws_initial_snapshot_timeout_ms: env_u64("WS_INITIAL_SNAPSHOT_TIMEOUT_MS", 0),
            ws_min_snapshot_coverage_pct: env_f64("WS_MIN_SNAPSHOT_COVERAGE_PCT", 0.90)
                .clamp(0.0, 1.0),
            ws_quote_max_age_ms: env_u64("WS_QUOTE_MAX_AGE_MS", 1000),
            ws_market_data_silence_timeout_ms: env_u64("WS_MARKET_DATA_SILENCE_TIMEOUT_MS", 2_500),
            min_clob_quote_coverage_pct: env_f64("MIN_CLOB_QUOTE_COVERAGE_PCT", 1.00)
                .clamp(0.0, 1.0),
            enable_gamma_fallback_when_no_clob_edge: env_bool(
                "ENABLE_GAMMA_FALLBACK_WHEN_NO_CLOB_EDGE",
                false,
            ),
            execute_only_full_clob_prices: env_bool("EXECUTE_ONLY_FULL_CLOB_PRICES", true),
            max_signal_age_secs: env_u64("MAX_SIGNAL_AGE_SECONDS", 5),
            event_lifecycle_gate_enabled: env_bool("EVENT_LIFECYCLE_GATE_ENABLED", true),
            event_lifecycle_pre_cutoff_buffer_secs: env_u64(
                "EVENT_LIFECYCLE_PRE_CUTOFF_BUFFER_SECONDS",
                300,
            ),
            enable_ranked_arbitrage: env_bool("ENABLE_RANKED_ARBITRAGE", false),
            enable_bundle_scanning_all_events: env_bool("ENABLE_BUNDLE_SCANNING_ALL_EVENTS", true),
            combo_rfq_discovery_enabled: env_bool("COMBO_RFQ_DISCOVERY_ENABLED", true),
            combo_rfq_max_markets: env_u64("COMBO_RFQ_MAX_MARKETS", 500).max(1) as usize,
            allow_augmented_neg_risk: env_bool("ALLOW_AUGMENTED_NEG_RISK", false),
            clob_max_concurrency: env_u64("CLOB_MAX_CONCURRENCY", 4).max(1) as usize,
            clob_book_batch_size: env_u64("CLOB_BOOK_BATCH_SIZE", DEFAULT_CLOB_BOOK_BATCH_SIZE)
                .max(1) as usize,
            active_slice_token_budget_multiplier: env_u64("ACTIVE_SLICE_TOKEN_BUDGET_MULTIPLIER", 2)
                .max(1) as usize,
            clob_book_batch_pause_ms: env_u64(
                "CLOB_BOOK_BATCH_PAUSE_MS",
                DEFAULT_CLOB_BOOK_BATCH_PAUSE_MS,
            ),
            quote_refresh_token_budget_per_scan,
            active_quote_token_budget_per_scan,
            scan_neg_risk_event_budget: env_u64("SCAN_NEG_RISK_EVENT_BUDGET", 96).max(1) as usize,
            scan_bundle_event_budget: env_u64("SCAN_BUNDLE_EVENT_BUDGET", 240).max(1) as usize,
            quote_shortfall_sample_size: env_u64("QUOTE_SHORTFALL_SAMPLE_SIZE", 8).max(1) as usize,
            opportunity_dedupe_cooldown_secs: env_u64("OPPORTUNITY_DEDUPE_COOLDOWN_SECONDS", 300),
            scan_rotation_period_scans,
            selection_sticky_fraction,
            capital_velocity_ranking_enabled: env_bool("CAPITAL_VELOCITY_RANKING_ENABLED", true),
            capital_velocity_reference_hours: env_f64("CAPITAL_VELOCITY_REFERENCE_HOURS", 24.0)
                .max(1.0),
            capital_velocity_score_weight: env_f64("CAPITAL_VELOCITY_SCORE_WEIGHT", 20_000.0)
                .max(0.0),
            max_opportunity_legs: env_u64("MAX_OPPORTUNITY_LEGS", 15).max(2) as usize,

            fee_theta_by_category: fee_theta,
            fee_theta_default: env_f64("FEE_THETA_DEFAULT", 0.050),

            gas_fallback_usd: env_f64("GAS_FALLBACK_USD", 0.05),
            assume_gasless_for_proxy_signature_types: env_bool(
                "ASSUME_GASLESS_FOR_PROXY_SIGNATURE_TYPES",
                false,
            ),

            paper_trading_enabled: env_bool("PAPER_TRADING_ENABLED", true),
            paper_trade_position_size_usd: env_f64("PAPER_TRADE_POSITION_SIZE_USD", 25.0),
            paper_match_live_position_size: env_bool("PAPER_MATCH_LIVE_POSITION_SIZE", true),
            paper_require_full_clob_quotes: env_bool("PAPER_REQUIRE_FULL_CLOB_QUOTES", true),
            paper_use_limit_orders: env_bool("PAPER_USE_LIMIT_ORDERS", true),
            paper_trade_history_limit: env_u64("PAPER_TRADE_HISTORY_LIMIT", 10_000) as usize,
            paper_max_share_mismatch_pct: env_f64("PAPER_MAX_SHARE_MISMATCH_PCT", 0.50).max(0.0),
            dry_run_provider: env_str("DRY_RUN_PROVIDER", "external"),
            external_paper_command: env_str("EXTERNAL_PAPER_COMMAND", "pm-trader"),
            external_paper_data_dir: PathBuf::from(env_str(
                "EXTERNAL_PAPER_DATA_DIR",
                ".pm-trader-scanner",
            )),
            external_paper_account: env_str("EXTERNAL_PAPER_ACCOUNT", "arb-scanner"),
            external_paper_init_balance_usd: env_f64("EXTERNAL_PAPER_INIT_BALANCE_USD", 10_000.0),
            external_paper_order_type: env_str("EXTERNAL_PAPER_ORDER_TYPE", "fok"),
            external_paper_limit_order_type: env_str("EXTERNAL_PAPER_LIMIT_ORDER_TYPE", "gtc"),
            external_paper_min_order_usd: env_f64("EXTERNAL_PAPER_MIN_ORDER_USD", 1.0),

            strategy_lab_enabled: env_bool("STRATEGY_LAB_ENABLED", true),
            strategy_lab_refresh_interval_secs: env_u64(
                "STRATEGY_LAB_REFRESH_INTERVAL_SECONDS",
                30,
            )
            .max(1),
            strategy_lab_market_limit: env_u64("STRATEGY_LAB_MARKET_LIMIT", 500).max(1) as usize,
            strategy_lab_initial_capital_usd: env_f64("STRATEGY_LAB_INITIAL_CAPITAL_USD", 10_000.0)
                .max(100.0),
            strategy_lab_position_size_usd: env_f64("STRATEGY_LAB_POSITION_SIZE_USD", 25.0)
                .max(1.0),
            strategy_lab_max_positions_per_strategy: env_u64(
                "STRATEGY_LAB_MAX_POSITIONS_PER_STRATEGY",
                4,
            )
            .max(1) as usize,
            strategy_lab_candidate_cap_per_strategy: env_u64(
                "STRATEGY_LAB_CANDIDATE_CAP_PER_STRATEGY",
                24,
            )
            .max(1) as usize,

            live_trading_enabled: env_bool("LIVE_TRADING_ENABLED", false),
            live_diagnostics_enabled: env_bool("LIVE_DIAGNOSTICS_ENABLED", false),
            live_combo_rfq_route_enabled: env_bool("LIVE_COMBO_RFQ_ROUTE_ENABLED", false),
            live_route_calibration_min_samples: env_u64("LIVE_ROUTE_CALIBRATION_MIN_SAMPLES", 100)
                .max(1) as usize,
            live_route_calibration_max_age_secs: env_u64(
                "LIVE_ROUTE_CALIBRATION_MAX_AGE_SECS",
                300,
            )
            .max(1),
            live_trade_position_size_usd: env_f64("LIVE_TRADE_POSITION_SIZE_USD", 25.0),
            live_chain_id: env_u64("LIVE_CHAIN_ID", 137),
            live_signature_type: env_signature_type(),
            live_funder_address: env_str("LIVE_FUNDER_ADDRESS", ""),
            live_slippage_bps: env_u64("LIVE_SLIPPAGE_BPS", 10) as u32,
            live_clob_microprice_adverse_bps: env_f64("LIVE_CLOB_MICROPRICE_ADVERSE_BPS", 1.0)
                .max(0.0),
            live_fill_poll_timeout_secs: env_u64("LIVE_FILL_POLL_TIMEOUT_SECONDS", 30),
            live_fill_poll_interval_ms: env_u64("LIVE_FILL_POLL_INTERVAL_MS", 500),
            live_max_refresh_to_submit_ms: env_u64("LIVE_MAX_REFRESH_TO_SUBMIT_MS", 1000).max(1),
            live_max_server_clock_uncertainty_ms: env_u64(
                "LIVE_MAX_SERVER_CLOCK_UNCERTAINTY_MS",
                250,
            )
            .max(1),
            live_max_server_clock_offset_ms: env_u64("LIVE_MAX_SERVER_CLOCK_OFFSET_MS", 5_000)
                .max(1),
            live_status_page_enabled: env_bool("LIVE_STATUS_PAGE_ENABLED", true),
            live_status_page_maintenance_prehalt_secs: env_u64(
                "LIVE_STATUS_PAGE_MAINTENANCE_PREHALT_SECS",
                1_800,
            ),
            live_accounting_snapshot_enabled: env_bool("LIVE_ACCOUNTING_SNAPSHOT_ENABLED", true),
            live_accounting_snapshot_max_position_rows: env_u64(
                "LIVE_ACCOUNTING_SNAPSHOT_MAX_POSITION_ROWS",
                0,
            ) as usize,
            live_game_start_quarantine_secs: env_u64("LIVE_GAME_START_QUARANTINE_SECS", 300),
            live_min_leg_size_usd: env_f64("LIVE_MIN_LEG_SIZE_USD", 1.0),
            live_max_event_exposure_usd: env_f64("LIVE_MAX_EVENT_EXPOSURE_USD", 200.0),
            live_max_total_exposure_usd: env_f64("LIVE_MAX_TOTAL_EXPOSURE_USD", 1_000.0),
            live_cancel_on_fill_timeout: env_bool("LIVE_CANCEL_ON_FILL_TIMEOUT", true),
            live_cancel_all_on_kill_switch: env_bool("LIVE_CANCEL_ALL_ON_KILL_SWITCH", false),
            live_order_type: env_str("LIVE_ORDER_TYPE", "fok"),
            live_user_ws_enabled: env_bool("LIVE_USER_WS_ENABLED", false),
            live_pre_submit_heartbeat_enabled: env_bool("LIVE_PRE_SUBMIT_HEARTBEAT_ENABLED", true),
            live_pre_submit_heartbeat_timeout_ms: env_u64(
                "LIVE_PRE_SUBMIT_HEARTBEAT_TIMEOUT_MS",
                750,
            )
            .max(1),
            live_edge_haircut_usd: env_f64("LIVE_EDGE_HAIRCUT_USD", 0.01).max(0.0),
            live_edge_haircut_bps: env_u64("LIVE_EDGE_HAIRCUT_BPS", 5) as u32,
            live_closeout_enabled: env_bool("LIVE_CLOSEOUT_ENABLED", false),
            live_closeout_dry_run: env_bool("LIVE_CLOSEOUT_DRY_RUN", true),
            live_closeout_max_actions_per_run: env_u64("LIVE_CLOSEOUT_MAX_ACTIONS_PER_RUN", 10)
                .max(1) as usize,
            live_closeout_confirm_timeout_secs: env_u64(
                "LIVE_CLOSEOUT_CONFIRM_TIMEOUT_SECONDS",
                120,
            )
            .max(1),
            polygon_rpc_url: env_str("POLYGON_RPC_URL", "https://polygon-rpc.com"),
            polygon_finalized_block_max_lag_blocks: env_u64(
                "POLYGON_FINALIZED_BLOCK_MAX_LAG_BLOCKS",
                512,
            )
            .max(1),
            onchain_order_filled_collector_enabled: env_bool(
                "ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED",
                false,
            ),
            onchain_order_filled_collector_lookback_blocks: env_u64(
                "ONCHAIN_ORDER_FILLED_COLLECTOR_LOOKBACK_BLOCKS",
                512,
            )
            .max(1),
            settlement_monitor_enabled: env_bool("SETTLEMENT_MONITOR_ENABLED", false),
            settlement_revert_hazard_min_samples: env_u64(
                "SETTLEMENT_REVERT_HAZARD_MIN_SAMPLES",
                10,
            )
            .max(1) as usize,
            settlement_revert_hazard_max_rate: env_f64("SETTLEMENT_REVERT_HAZARD_MAX_RATE", 0.0)
                .clamp(0.0, 1.0),
            settlement_receipt_max_age_secs: env_u64("SETTLEMENT_RECEIPT_MAX_AGE_SECONDS", 86_400)
                .max(1),
            order_size_step_shares: env_f64("ORDER_SIZE_STEP_SHARES", 0.0001).max(0.0001),

            validate_opportunities_at_target_size: env_bool(
                "VALIDATE_OPPORTUNITIES_AT_TARGET_SIZE",
                true,
            ),

            webhook_url: env_str("WEBHOOK_URL", ""),
            log_level: env_str("LOG_LEVEL", "info"),
            verbose_scan_logs: env_bool("VERBOSE_SCAN_LOGS", false),
            diagnostics_csv_enabled: env_bool("DIAGNOSTICS_CSV_ENABLED", true),
            diagnostics_dir: PathBuf::from(env_str("DIAGNOSTICS_DIR", "runtime_diagnostics")),
            diagnostics_csv_max_bytes: env_u64(
                "DIAGNOSTICS_CSV_MAX_BYTES",
                DEFAULT_DIAGNOSTICS_CSV_MAX_BYTES,
            )
            .max(1),
            diagnostics_log_all_candidate_evaluations: env_bool(
                "DIAGNOSTICS_LOG_ALL_CANDIDATE_EVALUATIONS",
                false,
            ),
            diagnostics_log_routine_rejections: env_bool(
                "DIAGNOSTICS_LOG_ROUTINE_REJECTIONS",
                false,
            ),
            max_retries: env_u64("MAX_RETRIES", 3) as u32,
            retry_backoff_base_ms: env_u64("RETRY_BACKOFF_BASE_MS", 1000),
        }
    }

    /// Get the fallback fee coefficient (Theta) for a given market category.
    pub fn fee_theta(&self, category: &str) -> f64 {
        let normalized = category.to_lowercase().replace(' ', "-");
        self.fee_theta_by_category
            .get(&normalized)
            .copied()
            .or_else(|| {
                self.fee_theta_by_category
                    .get(&normalized.replace('-', ""))
                    .copied()
            })
            .unwrap_or(self.fee_theta_default)
    }

    /// Paper sizing defaults to live sizing when requested, to maximize parity.
    pub fn effective_paper_position_size_usd(&self) -> f64 {
        if self.paper_match_live_position_size {
            self.live_trade_position_size_usd
        } else {
            self.paper_trade_position_size_usd
        }
    }

    pub fn effective_paper_use_limit_orders(&self) -> bool {
        self.paper_use_limit_orders
            && matches!(
                self.live_order_type.trim().to_ascii_lowercase().as_str(),
                "gtc"
            )
    }

    pub fn max_batchable_legs(&self) -> usize {
        self.max_opportunity_legs.clamp(2, 15)
    }

    pub fn effective_trade_gas_cost_usd(&self, gas_cost_usd: f64) -> f64 {
        if self.assume_gasless_for_proxy_signature_types && self.live_signature_type != 0 {
            0.0
        } else {
            gas_cost_usd
        }
    }

    pub fn signal_is_fresh(&self, signal_age_secs: i64) -> bool {
        signal_age_secs >= 0 && signal_age_secs <= self.max_signal_age_secs as i64
    }

    pub fn market_source_enabled(&self, source: &str) -> bool {
        let source = source.to_ascii_lowercase();
        self.prediction_market_sources
            .iter()
            .any(|item| item == "all" || item == "*" || item == &source)
    }
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_signature_type() -> u8 {
    normalize_signature_type(env_u64("LIVE_SIGNATURE_TYPE", 0))
}

fn normalize_signature_type(value: u64) -> u8 {
    match value {
        value @ 0..=3 => value as u8,
        _ => u8::MAX,
    }
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(default)
}

fn env_list(key: &str, default: &str) -> Vec<String> {
    env_str(key, default)
        .split(',')
        .map(|part| part.trim().to_ascii_lowercase())
        .filter(|part| !part.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_config_fingerprint_is_deterministic_sensitive_and_redacted() {
        let cfg = Config::from_env();
        let first = cfg.launch_config_fingerprint().unwrap();
        let second = cfg.launch_config_fingerprint().unwrap();
        assert_eq!(first, second);
        assert!(first.config_field_count > 100);
        assert_eq!(
            first.paper_live_profile_config,
            cfg.paper_live_profile_config()
        );
        let compatibility = first.profit_compatibility_fingerprint.clone();
        macro_rules! assert_profit_drift {
            ($change:expr) => {{
                let mut drifted = cfg.clone();
                $change(&mut drifted);
                assert_ne!(
                    compatibility,
                    drifted.profit_compatibility_fingerprint().unwrap()
                );
            }};
        }
        assert_profit_drift!(|config: &mut Config| config.live_clob_microprice_adverse_bps += 0.1);
        assert_profit_drift!(|config: &mut Config| config.event_lifecycle_gate_enabled =
            !config.event_lifecycle_gate_enabled);
        assert_profit_drift!(|config: &mut Config| config.min_liquidity_usd += 1.0);
        assert_profit_drift!(|config: &mut Config| config.fee_theta_default += 0.001);
        assert_profit_drift!(|config: &mut Config| {
            config.fee_theta_by_category.insert("crypto".into(), 0.123);
        });
        assert_profit_drift!(|config: &mut Config| config.ws_quote_max_age_ms += 1);
        assert_profit_drift!(|config: &mut Config| config.gas_fallback_usd += 0.01);
        let mut identity_only = cfg.clone();
        identity_only.polygon_rpc_url = "https://private-rpc.invalid/token".into();
        assert_eq!(
            compatibility,
            identity_only.profit_compatibility_fingerprint().unwrap(),
            "RPC identity is launch-bound separately and must not invalidate paper economics"
        );
        assert_eq!(first.paper_live_profile_config.order_mode, "market_style");
        assert_eq!(first.paper_live_profile_config.effective_order_type, "fok");
        assert_eq!(first.paper_live_profile_config.live_order_type, "fok");
        assert!(first.paper_live_profile_config.full_clob_required);
        assert!(first.paper_live_profile_config.match_live_position_size);
        assert_eq!(
            first.paper_live_profile_config.effective_position_size_usd,
            first.paper_live_profile_config.live_position_size_usd
        );
        assert_eq!(
            first
                .direct_live_identities
                .iter()
                .map(|identity| identity.name)
                .collect::<Vec<_>>(),
            DIRECT_LIVE_IDENTITY_ENVS
        );
        for name in [
            "CLOB_API_KEY",
            "CLOB_SECRET",
            "CLOB_PASS_PHRASE",
            "CLOB_PASSPHRASE",
            "LIVE_SIGNER_ADDRESS",
        ] {
            assert!(first
                .direct_live_identities
                .iter()
                .any(|identity| identity.name == name));
        }

        let mut changed = cfg.clone();
        changed.scan_interval_secs = changed.scan_interval_secs.saturating_add(1);
        assert_ne!(
            first.combined_fingerprint,
            changed
                .launch_config_fingerprint()
                .unwrap()
                .combined_fingerprint
        );

        let mut live_enabled = cfg.clone();
        live_enabled.live_trading_enabled = true;
        assert_eq!(
            first.combined_fingerprint,
            live_enabled
                .launch_config_fingerprint()
                .unwrap()
                .combined_fingerprint
        );

        let mut secret = cfg;
        secret.relayer_api_key = "must-not-appear-in-fingerprint-artifact".into();
        let artifact = serde_json::to_string(&secret.launch_config_fingerprint().unwrap()).unwrap();
        assert!(!artifact.contains("must-not-appear-in-fingerprint-artifact"));
    }

    #[test]
    fn test_fee_theta_lookup() {
        let cfg = Config::from_env();
        assert!((cfg.fee_theta("crypto") - 0.070).abs() < f64::EPSILON);
        assert!((cfg.fee_theta("geopolitics") - 0.0).abs() < f64::EPSILON);
        assert!((cfg.fee_theta("sports") - 0.030).abs() < f64::EPSILON);
        assert!((cfg.fee_theta("other general") - 0.050).abs() < f64::EPSILON);
        assert!((cfg.fee_theta("unknown-category") - cfg.fee_theta_default).abs() < f64::EPSILON);
    }

    #[test]
    fn test_effective_paper_position_size_defaults_to_live() {
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 42.0;
        cfg.paper_trade_position_size_usd = 7.0;
        cfg.paper_match_live_position_size = true;
        assert_eq!(cfg.effective_paper_position_size_usd(), 42.0);

        cfg.paper_match_live_position_size = false;
        assert_eq!(cfg.effective_paper_position_size_usd(), 7.0);
    }

    #[test]
    fn test_min_roi_default_matches_example_config() {
        let cfg = Config::from_env();
        assert!((cfg.min_roi_pct - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_min_net_profit_default_matches_example_config() {
        let cfg = Config::from_env();
        assert!((cfg.min_net_profit_usd - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_paper_limit_orders_default_to_live_parity() {
        let cfg = Config::from_env();
        assert!(cfg.paper_use_limit_orders);
        assert_eq!(cfg.external_paper_limit_order_type, "gtc");
        assert!(!cfg.effective_paper_use_limit_orders());
    }

    #[test]
    fn test_paper_share_mismatch_tolerance_is_non_negative() {
        let cfg = Config::from_env();
        assert!(cfg.paper_max_share_mismatch_pct >= 0.0);
    }

    #[test]
    fn test_effective_paper_limit_mode_tracks_live_order_type() {
        let mut cfg = Config::from_env();
        cfg.paper_use_limit_orders = true;
        cfg.live_order_type = "gtc".into();
        assert!(cfg.effective_paper_use_limit_orders());
        cfg.live_order_type = "gtd".into();
        assert!(!cfg.effective_paper_use_limit_orders());
        cfg.live_order_type = "fok".into();
        assert!(!cfg.effective_paper_use_limit_orders());
    }

    #[test]
    fn test_max_batchable_legs_never_exceeds_exchange_limit() {
        let mut cfg = Config::from_env();
        cfg.max_opportunity_legs = 32;
        assert_eq!(cfg.max_batchable_legs(), 15);
    }
    #[test]
    fn test_scan_defaults_use_ws_wakes_with_bounded_idle_polling() {
        let cfg = Config::from_env();
        assert_eq!(cfg.scan_interval_secs, 1);
        assert!(cfg.discovery_interval_secs <= 60);
        assert!(cfg.quote_refresh_token_budget_per_scan >= cfg.clob_book_batch_size);
        assert!(cfg.active_slice_token_budget_multiplier >= 1);
        assert_eq!(
            cfg.combo_rfq_api_url,
            "https://combos-rfq-api.polymarket.sh"
        );
        assert!(cfg.combo_rfq_discovery_enabled);
        assert_eq!(cfg.combo_rfq_max_markets, 500);
        assert!(cfg.capital_velocity_ranking_enabled);
        assert_eq!(cfg.capital_velocity_reference_hours, 24.0);
        assert_eq!(cfg.capital_velocity_score_weight, 20_000.0);
    }

    #[test]
    fn test_market_style_external_paper_defaults_to_fok() {
        let cfg = Config::from_env();
        assert_eq!(cfg.external_paper_order_type, "fok");
        assert_eq!(cfg.live_order_type, "fok");
    }

    #[test]
    fn test_operator_defaults_use_dashboard_logs_and_csv_diagnostics() {
        let cfg = Config::from_env();
        assert!(!cfg.verbose_scan_logs);
        assert!(cfg.diagnostics_csv_enabled);
        assert_eq!(cfg.diagnostics_dir, PathBuf::from("runtime_diagnostics"));
        assert_eq!(
            cfg.diagnostics_csv_max_bytes,
            DEFAULT_DIAGNOSTICS_CSV_MAX_BYTES
        );
        assert!(!cfg.diagnostics_log_all_candidate_evaluations);
        assert!(!cfg.diagnostics_log_routine_rejections);
        assert_eq!(
            cfg.polymarket_data_api_url,
            "https://data-api.polymarket.com"
        );
        assert_eq!(
            cfg.polymarket_status_api_url,
            "https://status.polymarket.com/v3/summary.json"
        );
        assert_eq!(
            cfg.polymarket_status_components_api_url,
            "https://status.polymarket.com/v3/components.json"
        );
    }

    #[test]
    fn test_selection_sticky_fraction_default_is_balanced() {
        let cfg = Config::from_env();
        assert!(cfg.selection_sticky_fraction >= 0.0);
        assert!(cfg.selection_sticky_fraction <= 0.95);
        assert!((cfg.selection_sticky_fraction - 0.35).abs() < f64::EPSILON);
    }

    #[test]
    fn test_signal_freshness_window_defaults() {
        let cfg = Config::from_env();
        assert_eq!(cfg.max_signal_age_secs, 5);
        assert_eq!(cfg.ws_quote_max_age_ms, 1000);
        assert_eq!(cfg.live_max_refresh_to_submit_ms, 1000);
        assert_eq!(cfg.live_max_server_clock_uncertainty_ms, 250);
        assert_eq!(cfg.live_max_server_clock_offset_ms, 5_000);
        assert!(cfg.live_status_page_enabled);
        assert_eq!(cfg.live_status_page_maintenance_prehalt_secs, 1_800);
        assert!(cfg.live_accounting_snapshot_enabled);
        assert_eq!(cfg.live_accounting_snapshot_max_position_rows, 0);
        assert_eq!(cfg.live_game_start_quarantine_secs, 300);
        assert_eq!(cfg.live_max_total_exposure_usd, 1_000.0);
        assert!(cfg.signal_is_fresh(0));
        assert!(!cfg.signal_is_fresh(6));
    }

    #[test]
    fn test_live_closeout_defaults_are_dry_run_only() {
        let cfg = Config::from_env();
        assert_eq!(cfg.clob_api_url, "https://clob.polymarket.com");
        assert_eq!(cfg.combo_rfq_requester_api_url, "https://api.polymarket.us");
        assert_eq!(cfg.relayer_api_url, "https://relayer-v2.polymarket.com");
        assert!(cfg.relayer_api_key.is_empty());
        assert!(cfg.relayer_api_key_address.is_empty());
        assert_eq!(cfg.relayer_wallet_deadline_secs, 300);
        assert!(!cfg.combo_rfq_requester_enabled);
        assert!(!cfg.combo_rfq_accept_enabled);
        assert!(!cfg.combo_rfq_requester_protocol_verified);
        assert!(cfg.combo_rfq_bearer_token.is_empty());
        assert!(cfg.combo_rfq_participant_id.is_empty());
        assert_eq!(cfg.combo_rfq_quote_max_age_ms, 400);
        assert_eq!(cfg.combo_rfq_microprice_adverse_bps, 1.0);
        assert_eq!(cfg.combo_rfq_markout_race_score_horizon_ms, 250);
        assert_eq!(cfg.combo_rfq_markout_race_min_samples, 3);
        assert_eq!(cfg.combo_rfq_markout_race_max_age_secs, 3_600);
        assert_eq!(cfg.combo_rfq_markout_race_max_adverse_bps, 1.0);
        assert_eq!(
            cfg.combo_rfq_exchange_v3_address,
            "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"
        );
        assert_eq!(cfg.combo_rfq_finality_max_age_secs, 300);
        assert_eq!(cfg.combo_rfq_finality_min_confirmed_samples, 3);
        assert_eq!(cfg.combo_rfq_counterparty_min_settlement_samples, 3);
        assert!(!cfg.combo_rfq_stream_enabled);
        assert_eq!(
            cfg.combo_rfq_gateway_wss_url,
            DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL
        );
        assert!(cfg.combo_rfq_grpc_url.is_empty());
        assert!(cfg.combo_rfq_stream_bearer_token.is_empty());
        assert_eq!(cfg.combo_rfq_stream_reconnect_backoff_ms, 1_000);
        assert_eq!(cfg.live_edge_haircut_usd, 0.01);
        assert_eq!(cfg.live_edge_haircut_bps, 5);
        assert_eq!(cfg.live_clob_microprice_adverse_bps, 1.0);
        assert!(cfg.live_cancel_on_fill_timeout);
        assert!(!cfg.live_cancel_all_on_kill_switch);
        assert!(!cfg.live_diagnostics_enabled);
        assert!(!cfg.live_combo_rfq_route_enabled);
        assert_eq!(cfg.live_route_calibration_min_samples, 100);
        assert_eq!(cfg.live_route_calibration_max_age_secs, 300);
        assert!(!cfg.live_user_ws_enabled);
        assert!(cfg.live_pre_submit_heartbeat_enabled);
        assert_eq!(cfg.live_pre_submit_heartbeat_timeout_ms, 750);
        assert!(!cfg.live_closeout_enabled);
        assert!(cfg.live_closeout_dry_run);
        assert_eq!(cfg.live_closeout_max_actions_per_run, 10);
        assert_eq!(cfg.live_closeout_confirm_timeout_secs, 120);
        assert_eq!(
            cfg.polygon_rpc_url,
            std::env::var("POLYGON_RPC_URL")
                .unwrap_or_else(|_| "https://polygon-rpc.com".to_string())
        );
        assert_eq!(cfg.polygon_finalized_block_max_lag_blocks, 512);
        assert!(!cfg.onchain_order_filled_collector_enabled);
        assert_eq!(cfg.onchain_order_filled_collector_lookback_blocks, 512);
        assert!(!cfg.settlement_monitor_enabled);
        assert_eq!(cfg.settlement_revert_hazard_min_samples, 10);
        assert_eq!(cfg.settlement_revert_hazard_max_rate, 0.0);
        assert_eq!(cfg.settlement_receipt_max_age_secs, 86_400);
        assert_eq!(
            cfg.clob_user_ws_url,
            "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        );
    }

    #[test]
    fn test_proxy_signature_types_can_assume_zero_trade_gas() {
        let mut cfg = Config::from_env();
        cfg.assume_gasless_for_proxy_signature_types = true;
        cfg.live_signature_type = 1;
        assert_eq!(cfg.effective_trade_gas_cost_usd(1.23), 0.0);

        cfg.live_signature_type = 0;
        assert_eq!(cfg.effective_trade_gas_cost_usd(1.23), 1.23);
    }

    #[test]
    fn invalid_signature_types_remain_invalid_instead_of_wrapping() {
        assert_eq!(normalize_signature_type(0), 0);
        assert_eq!(normalize_signature_type(3), 3);
        assert_eq!(normalize_signature_type(4), u8::MAX);
        assert_eq!(normalize_signature_type(256), u8::MAX);
    }

    #[test]
    fn test_strategy_lab_defaults_enable_parallel_paper_tests() {
        let cfg = Config::from_env();
        assert!(cfg.strategy_lab_enabled);
        assert!(cfg.strategy_lab_refresh_interval_secs >= 1);
        assert!(cfg.strategy_lab_market_limit >= 1);
        assert!(cfg.strategy_lab_initial_capital_usd >= 100.0);
        assert!(cfg.strategy_lab_position_size_usd >= 1.0);
        assert!(cfg.strategy_lab_max_positions_per_strategy >= 1);
        assert!(cfg.strategy_lab_candidate_cap_per_strategy >= 1);
    }

    #[test]
    fn test_order_size_step_defaults_to_subshare_precision() {
        let cfg = Config::from_env();
        assert!(cfg.order_size_step_shares > 0.0);
        assert!(cfg.validate_opportunities_at_target_size);
        assert!(cfg.ws_quote_max_age_ms >= 1);
        assert_eq!(cfg.ws_market_data_silence_timeout_ms, 2_500);
        assert!(cfg.clob_book_batch_size >= 1);
        assert!(cfg.quote_refresh_token_budget_per_scan >= 1);
        assert!(cfg.active_quote_token_budget_per_scan >= cfg.quote_refresh_token_budget_per_scan);
        assert!(cfg.scan_neg_risk_event_budget >= 1);
        assert!(cfg.scan_bundle_event_budget >= 1);
        assert!(cfg.quote_shortfall_sample_size >= 1);
        assert!(cfg.scan_rotation_period_scans >= 1);
    }

    #[test]
    fn default_clob_book_batching_is_latency_oriented() {
        assert_eq!(DEFAULT_CLOB_BOOK_BATCH_SIZE, 150);
        assert_eq!(DEFAULT_CLOB_BOOK_BATCH_PAUSE_MS, 0);
    }

    #[test]
    fn test_prediction_market_sources_default_to_public_adapters() {
        let sources: Vec<&str> = DEFAULT_PREDICTION_MARKET_SOURCES.split(',').collect();
        assert_eq!(
            sources,
            vec!["polymarket", "kalshi", "manifold", "seer", "sxbet"]
        );
        assert!(!sources.contains(&"limitless"));
        assert!(!sources.contains(&"predictit"));
        assert!(!sources.contains(&"betdex"));
    }
}
