//! Read-only Combo/RFQ catalog support.
//!
//! The Combo/RFQ catalog is used only to annotate non-atomic opportunities with
//! a possible future atomic route. It does not enable live execution by itself.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};
use tracing::warn;

use crate::clob_client;
use crate::config::Config;
use crate::live_executor::configured_live_account_address;
use crate::models::{is_external_token_id, ArbType, ArbitrageOpportunity, OutcomeSide};
use crate::ws_client::PriceCache;
use polymarket_client_sdk_v2::types::Address;

const COMBO_RFQ_REQUESTER_SCOPE_NOTE: &str =
    "beta_bearer_token_with_write_orders_and_participant_id_required";
const COMBO_RFQ_MAKER_JOURNAL_FILE: &str = "combo_rfq_maker_journal.jsonl";
const COMBO_RFQ_EXECUTION_JOURNAL_FILE: &str = "combo_rfq_execution_journal.jsonl";
const COMBO_RFQ_ADVERSE_SELECTION_JOURNAL_FILE: &str = "combo_rfq_adverse_selection_journal.jsonl";
const COMBO_RFQ_MARKOUT_RACE_JOURNAL_FILE: &str = "combo_rfq_markout_race_journal.jsonl";
const COMBO_RFQ_ACTIVITY_PAGE_LIMIT: usize = 100;
const COMBO_RFQ_ACTIVITY_MAX_RECORDS: usize = 500;
const COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES: usize = 3;
const COMBO_RFQ_MAKER_MAX_REJECT_RATE: f64 = 0.25;
const COMBO_RFQ_MAKER_MAX_STALE_RATE: f64 = 0.25;
const COMBO_RFQ_MAKER_MIN_SUCCESS_RATE: f64 = 0.50;
const COMBO_RFQ_QUOTE_COLLECTION_WINDOW_MS: u64 = 400;
const COMBO_RFQ_LAST_LOOK_WINDOW_MS: u64 = 1_000;
const COMBO_RFQ_STREAM_QUOTE_MAX_AGE_MS: i64 =
    (COMBO_RFQ_QUOTE_COLLECTION_WINDOW_MS + COMBO_RFQ_LAST_LOOK_WINDOW_MS) as i64;
const COMBO_RFQ_DISPERSION_MIN_QUOTES: usize = 3;
const COMBO_RFQ_DISPERSION_SECOND_BEST_GAP_BPS: f64 = 100.0;
const COMBO_RFQ_DISPERSION_MEDIAN_GAP_BPS: f64 = 200.0;
const COMBO_RFQ_PRE_ACCEPT_MAX_ADVERSE_MARKOUT_BPS: f64 = 1.0;
const COMBO_RFQ_TOXICITY_WINDOW_MS: u64 = 1_000;
const COMBO_RFQ_TOXICITY_MAX_TRADE_PRINTS: usize = 8;
const COMBO_RFQ_MICROSTRUCTURE_DEPTH_LEVELS: usize = 3;
#[cfg(not(test))]
const COMBO_RFQ_MARKOUT_RACE_HORIZONS_MS: [u64; 4] = [100, 250, 500, 1_000];
const COMBO_RFQ_CAPITAL_LOCK_APR: f64 = 0.10;
const COMBO_RFQ_FINALITY_FAILURE_PROB_FLOOR: f64 = 0.001;
const COMBO_RFQ_PARTIAL_EXPOSURE_PROB_FLOOR: f64 = 0.005;
const COMBO_RFQ_ORPHAN_CLOSEOUT_LOSS_FLOOR: f64 = 0.02;
const COMBO_RFQ_RETRY_WAIT_MAX_MS: u64 = 30_000;
const COMBO_RFQ_CLIENT_REQUEST_PREFIX: &str = "scanner-";
const COMBO_RFQ_STREAM_PARSED_CACHE_MAX_RFQS: usize = 1_024;
const COMBO_RFQ_STREAM_PARSED_CACHE_MAX_PER_RFQ: usize = 64;
const COMBO_RFQ_STREAM_JOURNAL_UNCHANGED_SCAN_COOLDOWN_MS: u64 = 50;
const COMBO_RFQ_POST_ACCEPT_FINALITY_WAIT_MS: u64 = COMBO_RFQ_LAST_LOOK_WINDOW_MS;
static COMBO_RFQ_CLIENT_REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static COMBO_RFQ_READ_RATE_LIMITS: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
static COMBO_RFQ_STREAM_PARSED_QUOTE_CACHE: OnceLock<
    Mutex<HashMap<String, Vec<ComboRfqParsedStreamQuote>>>,
> = OnceLock::new();
static COMBO_RFQ_STREAM_JOURNAL_SCAN_CACHE: OnceLock<
    Mutex<HashMap<(PathBuf, String), ComboRfqStreamJournalScanState>>,
> = OnceLock::new();

#[derive(Debug, Clone)]
struct ComboRfqParsedStreamQuote {
    payload: Value,
    cached_at: Instant,
}

#[derive(Debug, Clone)]
struct ComboRfqStreamJournalScanState {
    len: u64,
    modified: Option<SystemTime>,
    scanned_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicRouteHint {
    None,
    ComboRfqCandidate,
}

impl AtomicRouteHint {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ComboRfqCandidate => "combo_rfq_candidate",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComboRouteReport {
    pub route: AtomicRouteHint,
    pub planned_legs: usize,
    pub unique_conditions: usize,
    pub combo_conditions: usize,
    pub token_position_matches: usize,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ComboRfqRequesterConfigReport {
    pub enabled: bool,
    pub api_url: String,
    pub bearer_token_present: bool,
    pub participant_id_present: bool,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqLegRequest {
    pub symbol: String,
    pub side: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqCreateRequest {
    #[serde(skip_serializing_if = "Option::is_none", rename = "qtyDecimal")]
    pub qty_decimal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "cashOrderQty")]
    pub cash_order_qty: Option<String>,
    pub legs: Vec<ComboRfqLegRequest>,
    pub side: String,
    #[serde(rename = "clientRequestId")]
    pub client_request_id: String,
    #[serde(rename = "expirationTime")]
    pub expiration_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqAcceptQuoteRequest {
    pub side: String,
    pub price: String,
    pub symbol: String,
    #[serde(rename = "qtyDecimal")]
    pub qty_decimal: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ComboRfqRequesterPlan {
    pub status: String,
    pub blockers: Vec<String>,
    pub request: Option<ComboRfqCreateRequest>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqQuoteCandidate {
    pub quote_id: String,
    pub rfq_id: Option<String>,
    pub maker_id: Option<String>,
    pub symbol: Option<String>,
    pub side: Option<String>,
    pub status: Option<String>,
    pub price: f64,
    pub qty_decimal: Option<f64>,
    pub created_at: Option<String>,
    pub expires_at: Option<String>,
    pub age_ms: Option<i64>,
    pub expected_edge_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ComboRfqBestExecutionReport {
    pub status: String,
    pub quotes_seen: usize,
    pub quotes_eligible: usize,
    pub selected_quote: Option<ComboRfqQuoteCandidate>,
    pub maker_scorecard: ComboRfqMakerScorecard,
    pub requester_ready: bool,
    pub accept_enabled: bool,
    pub edge_gate_pass: bool,
    pub last_look_gate_pass: bool,
    pub accept_gate_pass: bool,
    pub blockers: Vec<String>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqMakerJournalRecord {
    pub generated_at: String,
    pub maker_id: Option<String>,
    pub quote_id: String,
    pub rfq_id: Option<String>,
    pub event_id: String,
    pub quote_age_ms: Option<i64>,
    pub expected_edge_usd: Option<f64>,
    pub selected: bool,
    pub accepted: bool,
    pub terminal_status: Option<String>,
    pub realized_ev_usd: Option<f64>,
    pub blockers: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ComboRfqMakerScorecard {
    pub status: String,
    pub journal_path: String,
    pub records_seen: usize,
    pub maker_count: usize,
    pub min_terminal_samples: usize,
    pub makers: Vec<ComboRfqMakerScore>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ComboRfqMakerScore {
    pub maker_id: String,
    pub samples: usize,
    pub terminal_samples: usize,
    pub successes: usize,
    pub rejects: usize,
    pub failures: usize,
    pub stale_quotes: usize,
    pub pending: usize,
    pub reject_rate: f64,
    pub stale_rate: f64,
    pub success_rate: f64,
    pub avg_realized_ev_usd: Option<f64>,
    pub markout_samples: usize,
    pub adverse_markout_samples: usize,
    pub avg_markout_bps: Option<f64>,
    pub max_markout_bps: Option<f64>,
    pub adverse_markout_rate: Option<f64>,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ComboRfqExecutionStep {
    pub stage: String,
    pub status: String,
    pub detail: String,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ComboRfqExecutionReport {
    pub status: String,
    pub accept_outcome: Option<ComboRfqAcceptOutcome>,
    pub request: Option<ComboRfqCreateRequest>,
    pub rfq_id: Option<String>,
    pub quote_response: Option<Value>,
    pub best_execution: ComboRfqBestExecutionReport,
    pub pre_accept_markout: Option<ComboRfqPreAcceptMarkoutReport>,
    pub accept_request: Option<ComboRfqAcceptQuoteRequest>,
    pub accept_response: Option<Value>,
    pub blockers: Vec<String>,
    pub steps: Vec<ComboRfqExecutionStep>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ComboRfqPreAcceptMarkoutReport {
    pub status: String,
    pub blockers: Vec<String>,
    pub quote_to_accept_ms: Option<i64>,
    pub maker_id: Option<String>,
    pub quote_price: f64,
    pub quote_qty_decimal: f64,
    pub quote_cost_usd: f64,
    pub live_cost_buffer_usd: f64,
    pub synthetic_price: f64,
    pub synthetic_cost_usd: f64,
    pub quote_edge_usd: f64,
    pub public_edge_usd: f64,
    pub markout_bps: f64,
    pub toxicity_haircut_bps: f64,
    pub toxicity_haircut_usd: f64,
    pub toxicity_trade_prints: usize,
    pub toxicity_recent_book_updates: usize,
    pub ws_microprice_mean: Option<f64>,
    pub ws_queue_imbalance_mean: Option<f64>,
    pub ws_microstructure_tokens: usize,
    pub quote_edge_after_toxicity_usd: f64,
    pub public_edge_after_toxicity_usd: f64,
    pub token_ids: Vec<String>,
    pub book_hashes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqAdverseSelectionJournalRecord {
    pub generated_at: String,
    pub event_id: String,
    pub rfq_id: String,
    pub quote_id: String,
    pub maker_id: Option<String>,
    pub quote_age_ms: Option<i64>,
    pub quote_to_accept_ms: Option<i64>,
    pub quote_price: f64,
    pub quote_qty_decimal: f64,
    pub quote_cost_usd: f64,
    pub synthetic_price: f64,
    pub synthetic_cost_usd: f64,
    pub quote_edge_usd: f64,
    pub public_edge_usd: f64,
    pub markout_bps: f64,
    pub toxicity_haircut_bps: f64,
    pub toxicity_haircut_usd: f64,
    pub toxicity_trade_prints: usize,
    pub toxicity_recent_book_updates: usize,
    pub ws_microprice_mean: Option<f64>,
    pub ws_queue_imbalance_mean: Option<f64>,
    pub ws_microstructure_tokens: usize,
    pub token_ids: Vec<String>,
    pub book_hashes: Vec<String>,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqMarkoutRaceJournalRecord {
    pub generated_at: String,
    pub race_id: String,
    pub event_id: String,
    pub rfq_id: String,
    pub quote_id: String,
    pub maker_id: Option<String>,
    pub horizon_ms: u64,
    pub status: String,
    pub quote_price: f64,
    pub quote_qty_decimal: f64,
    pub pre_accept_synthetic_price: f64,
    pub sampled_synthetic_price: Option<f64>,
    pub sampled_public_edge_usd: Option<f64>,
    pub sampled_markout_bps: Option<f64>,
    pub token_ids: Vec<String>,
    pub pre_accept_book_hashes: Vec<String>,
    pub sampled_book_hashes: Vec<String>,
    pub blockers: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqExecutionJournalRecord {
    pub generated_at: String,
    pub event_id: String,
    pub stage: String,
    pub status: String,
    pub client_request_id: String,
    pub rfq_id: Option<String>,
    pub quote_id: Option<String>,
    pub maker_id: Option<String>,
    pub request: Option<ComboRfqCreateRequest>,
    pub selected_quote: Option<ComboRfqQuoteCandidate>,
    pub accept_request: Option<ComboRfqAcceptQuoteRequest>,
    pub response: Option<Value>,
    pub error: Option<String>,
    pub blockers: Vec<String>,
    pub note: String,
}

impl ComboRouteReport {
    pub fn requester_execution_status(&self) -> &'static str {
        if matches!(self.route, AtomicRouteHint::ComboRfqCandidate) {
            "beta_accept_endpoint_documented"
        } else {
            "not_applicable"
        }
    }

    pub fn requester_api_public(&self) -> bool {
        false
    }

    pub fn advisory_quote_window_ms(&self) -> Option<u64> {
        matches!(self.route, AtomicRouteHint::ComboRfqCandidate).then_some(400)
    }

    pub fn advisory_accept_window_ms(&self) -> Option<u64> {
        matches!(self.route, AtomicRouteHint::ComboRfqCandidate).then_some(5_000)
    }

    pub fn advisory_last_look_ms(&self) -> Option<u64> {
        matches!(self.route, AtomicRouteHint::ComboRfqCandidate).then_some(1_000)
    }

    pub fn note(&self) -> String {
        let quote_window = self
            .advisory_quote_window_ms()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".into());
        let accept_window = self
            .advisory_accept_window_ms()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".into());
        let last_look = self
            .advisory_last_look_ms()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".into());
        format!(
            "atomic_route={} combo_rfq_requester_execution={} combo_rfq_requester_api_public={} rfq_quote_window_ms={} rfq_accept_window_ms={} rfq_last_look_ms={} rfq_combo_conditions={}/{} rfq_position_matches={}/{} reason={}",
            self.route.as_str(),
            self.requester_execution_status(),
            self.requester_api_public(),
            quote_window,
            accept_window,
            last_look,
            self.combo_conditions,
            self.unique_conditions,
            self.token_position_matches,
            self.planned_legs,
            self.reason
        )
    }
}

pub fn combo_rfq_requester_config_report(config: &Config) -> ComboRfqRequesterConfigReport {
    let mut blockers = Vec::new();
    if !config.combo_rfq_requester_enabled {
        blockers.push("COMBO_RFQ_REQUESTER_ENABLED=false".to_string());
    }
    if config.combo_rfq_requester_api_url.trim().is_empty() {
        blockers.push("COMBO_RFQ_REQUESTER_API_URL_empty".to_string());
    }
    if config.combo_rfq_bearer_token.trim().is_empty() {
        blockers.push("COMBO_RFQ_BEARER_TOKEN_empty".to_string());
    }
    if config.combo_rfq_participant_id.trim().is_empty() {
        blockers.push("COMBO_RFQ_PARTICIPANT_ID_empty".to_string());
    }
    ComboRfqRequesterConfigReport {
        enabled: config.combo_rfq_requester_enabled,
        api_url: config.combo_rfq_requester_api_url.clone(),
        bearer_token_present: !config.combo_rfq_bearer_token.trim().is_empty(),
        participant_id_present: !config.combo_rfq_participant_id.trim().is_empty(),
        status: if blockers.is_empty() {
            "ready".into()
        } else {
            "blocked".into()
        },
        blockers,
    }
}

pub fn build_combo_rfq_requester_plan(
    config: &Config,
    catalog: &ComboMarketCatalog,
    opp: &ArbitrageOpportunity,
) -> ComboRfqRequesterPlan {
    let route = catalog.route_report(opp);
    let mut blockers = combo_rfq_requester_config_report(config).blockers;
    if !matches!(route.route, AtomicRouteHint::ComboRfqCandidate) {
        blockers.push(format!("not_combo_rfq_candidate:{}", route.reason));
    }
    if opp.execution_plan.is_empty() {
        blockers.push("empty_execution_plan".to_string());
    }
    if opp.execution_plan.len() > 15 {
        blockers.push(format!("too_many_combo_legs:{}", opp.execution_plan.len()));
    }

    let mut legs = Vec::new();
    for leg in &opp.execution_plan {
        match catalog.combo_symbol_for_condition(leg.condition_id.trim()) {
            Some(symbol) => legs.push(ComboRfqLegRequest {
                symbol: symbol.to_string(),
                side: rfq_side_for_outcome(leg.outcome).to_string(),
            }),
            None => blockers.push(format!("missing_combo_symbol:{}", leg.condition_id.trim())),
        }
    }

    let request = if blockers.is_empty() {
        Some(ComboRfqCreateRequest {
            qty_decimal: None,
            cash_order_qty: Some(format_decimal(config.live_trade_position_size_usd)),
            legs,
            side: "SIDE_BUY".into(),
            client_request_id: combo_rfq_client_request_id(opp),
            expiration_time: (Utc::now() + ChronoDuration::seconds(10)).to_rfc3339(),
        })
    } else {
        None
    };
    let status = if request.is_some() {
        "ready_no_submit".to_string()
    } else {
        "blocked".to_string()
    };
    ComboRfqRequesterPlan {
        status: status.clone(),
        note: if blockers.is_empty() {
            format!(
                "combo_rfq_requester={status} legs={} auth=present scope_note={}",
                opp.execution_plan.len(),
                COMBO_RFQ_REQUESTER_SCOPE_NOTE
            )
        } else {
            format!(
                "combo_rfq_requester={status} blockers={}",
                blockers.join("|")
            )
        },
        blockers,
        request,
    }
}

pub fn append_combo_rfq_maker_journal_record(
    config: &Config,
    record: &ComboRfqMakerJournalRecord,
) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(COMBO_RFQ_MAKER_JOURNAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening Combo/RFQ maker journal {}", path.display()))?;
    let line = serde_json::to_string(record)?;
    writeln!(file, "{line}")
        .with_context(|| format!("writing Combo/RFQ maker journal {}", path.display()))?;
    Ok(path)
}

pub fn append_combo_rfq_execution_journal_record(
    config: &Config,
    record: &ComboRfqExecutionJournalRecord,
) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config
        .diagnostics_dir
        .join(COMBO_RFQ_EXECUTION_JOURNAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening Combo/RFQ execution journal {}", path.display()))?;
    let line = serde_json::to_string(record)?;
    writeln!(file, "{line}")
        .with_context(|| format!("writing Combo/RFQ execution journal {}", path.display()))?;
    Ok(path)
}

pub fn append_combo_rfq_adverse_selection_journal_record(
    config: &Config,
    record: &ComboRfqAdverseSelectionJournalRecord,
) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config
        .diagnostics_dir
        .join(COMBO_RFQ_ADVERSE_SELECTION_JOURNAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| {
            format!(
                "opening Combo/RFQ adverse selection journal {}",
                path.display()
            )
        })?;
    let line = serde_json::to_string(record)?;
    writeln!(file, "{line}").with_context(|| {
        format!(
            "writing Combo/RFQ adverse selection journal {}",
            path.display()
        )
    })?;
    Ok(path)
}

pub fn append_combo_rfq_markout_race_journal_record(
    config: &Config,
    record: &ComboRfqMarkoutRaceJournalRecord,
) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config
        .diagnostics_dir
        .join(COMBO_RFQ_MARKOUT_RACE_JOURNAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening Combo/RFQ markout race journal {}", path.display()))?;
    let line = serde_json::to_string(record)?;
    writeln!(file, "{line}")
        .with_context(|| format!("writing Combo/RFQ markout race journal {}", path.display()))?;
    Ok(path)
}

fn read_combo_rfq_markout_race_journal_records(
    path: &Path,
) -> Result<Vec<ComboRfqMarkoutRaceJournalRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = fs::read_to_string(path)
        .with_context(|| format!("reading Combo/RFQ markout race journal {}", path.display()))?;
    body.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(idx, line)| {
            serde_json::from_str::<ComboRfqMarkoutRaceJournalRecord>(line).with_context(|| {
                format!(
                    "parsing Combo/RFQ markout race journal {} line {}",
                    path.display(),
                    idx + 1
                )
            })
        })
        .collect()
}

fn read_combo_rfq_execution_journal_records(
    path: &Path,
) -> Result<Vec<ComboRfqExecutionJournalRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = fs::read_to_string(path)
        .with_context(|| format!("reading Combo/RFQ execution journal {}", path.display()))?;
    body.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(idx, line)| {
            serde_json::from_str::<ComboRfqExecutionJournalRecord>(line).with_context(|| {
                format!(
                    "parsing Combo/RFQ execution journal {} line {}",
                    path.display(),
                    idx + 1
                )
            })
        })
        .collect()
}

pub fn pending_combo_rfq_execution_records(
    config: &Config,
    client_request_id: &str,
) -> Result<Vec<ComboRfqExecutionJournalRecord>> {
    let path = config
        .diagnostics_dir
        .join(COMBO_RFQ_EXECUTION_JOURNAL_FILE);
    let records = read_combo_rfq_execution_journal_records(&path)?;
    Ok(records
        .iter()
        .enumerate()
        .filter(|(_, record)| {
            combo_rfq_client_request_recovery_scopes_match(
                &record.client_request_id,
                client_request_id,
            )
        })
        .filter(|(_, record)| combo_rfq_execution_status_requires_recovery(&record.status))
        .filter(|(idx, _)| !combo_rfq_execution_record_is_cleared_later(&records, *idx))
        .map(|(_, record)| record.clone())
        .collect())
}

pub fn unresolved_combo_rfq_execution_records(
    config: &Config,
) -> Result<Vec<ComboRfqExecutionJournalRecord>> {
    let path = config
        .diagnostics_dir
        .join(COMBO_RFQ_EXECUTION_JOURNAL_FILE);
    let records = read_combo_rfq_execution_journal_records(&path)?;
    Ok(records
        .iter()
        .enumerate()
        .filter(|(_, record)| combo_rfq_execution_status_requires_recovery(&record.status))
        .filter(|(idx, _)| !combo_rfq_execution_record_is_cleared_later(&records, *idx))
        .map(|(_, record)| record.clone())
        .collect())
}

pub fn resolve_combo_rfq_execution_event_id(
    config: &Config,
    client_request_id: Option<&str>,
    rfq_id: Option<&str>,
    quote_id: Option<&str>,
) -> Result<Option<String>> {
    let path = config
        .diagnostics_dir
        .join(COMBO_RFQ_EXECUTION_JOURNAL_FILE);
    let records = read_combo_rfq_execution_journal_records(&path)?;
    Ok(records
        .iter()
        .rev()
        .find(|record| {
            combo_rfq_execution_record_matches_keys(record, client_request_id, rfq_id, quote_id)
        })
        .and_then(|record| nonempty_string(&record.event_id)))
}

pub fn resolve_combo_rfq_execution_reserve_amount_usd(
    config: &Config,
    client_request_id: Option<&str>,
    rfq_id: Option<&str>,
    quote_id: Option<&str>,
) -> Result<Option<f64>> {
    let path = config
        .diagnostics_dir
        .join(COMBO_RFQ_EXECUTION_JOURNAL_FILE);
    let records = read_combo_rfq_execution_journal_records(&path)?;
    Ok(records
        .iter()
        .rev()
        .filter(|record| {
            combo_rfq_execution_record_matches_keys(record, client_request_id, rfq_id, quote_id)
        })
        .find_map(combo_rfq_execution_record_reserve_amount_usd))
}

#[allow(clippy::too_many_arguments)]
pub fn append_combo_rfq_finality_execution_record(
    config: &Config,
    event_id: String,
    client_request_id: Option<String>,
    rfq_id: Option<String>,
    quote_id: Option<String>,
    maker_id: Option<String>,
    status: String,
    response: Value,
    blockers: Vec<String>,
) -> Result<PathBuf> {
    let record = ComboRfqExecutionJournalRecord {
        generated_at: Utc::now().to_rfc3339(),
        event_id,
        stage: "rfq_finality".into(),
        status,
        client_request_id: client_request_id.unwrap_or_default(),
        rfq_id,
        quote_id,
        maker_id,
        request: None,
        selected_quote: None,
        accept_request: None,
        response: Some(response),
        error: None,
        note: if blockers.is_empty() {
            "combo_rfq_journal stage=rfq_finality".into()
        } else {
            format!(
                "combo_rfq_journal stage=rfq_finality blockers={}",
                blockers.join("|")
            )
        },
        blockers,
    };
    append_combo_rfq_execution_journal_record(config, &record)
}

fn combo_rfq_execution_status_requires_recovery(status: &str) -> bool {
    matches!(
        status,
        "create_intent"
            | "request_created"
            | "create_state_unknown"
            | "quote_query_state_unknown"
            | "accept_intent"
            | "accept_request_unknown"
            | "accepted_pending_finality"
            | "accept_response_not_accepted"
            | "accept_state_unknown"
    )
}

fn combo_rfq_execution_record_is_cleared_later(
    records: &[ComboRfqExecutionJournalRecord],
    pending_idx: usize,
) -> bool {
    let Some(pending) = records.get(pending_idx) else {
        return false;
    };
    records.iter().skip(pending_idx + 1).any(|terminal| {
        combo_rfq_execution_status_clears_recovery(&terminal.status)
            && combo_rfq_execution_records_share_recovery_key(pending, terminal)
    })
}

fn combo_rfq_execution_status_clears_recovery(status: &str) -> bool {
    matches!(
        status,
        "blocked_best_execution"
            | "accept_rejected_proven"
            | "finality_confirmed_exposure_retained"
            | "finality_rejected_released"
            | "manual_reconciled"
    )
}

fn shared_nonempty_key(left: Option<&str>, right: Option<&str>) -> bool {
    left.zip(right)
        .map(|(left, right)| !left.trim().is_empty() && left == right)
        .unwrap_or(false)
}

fn nonempty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn parse_positive_f64(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn combo_rfq_execution_record_reserve_amount_usd(
    record: &ComboRfqExecutionJournalRecord,
) -> Option<f64> {
    record
        .request
        .as_ref()
        .and_then(|request| request.cash_order_qty.as_deref())
        .and_then(parse_positive_f64)
        .or_else(|| {
            record.selected_quote.as_ref().and_then(|quote| {
                let qty = quote
                    .qty_decimal
                    .filter(|qty| qty.is_finite() && *qty > 0.0)?;
                let amount = quote.price * qty;
                (amount.is_finite() && amount > 0.0).then_some(amount)
            })
        })
        .or_else(|| {
            record.accept_request.as_ref().and_then(|request| {
                let price = parse_positive_f64(&request.price)?;
                let qty = parse_positive_f64(&request.qty_decimal)?;
                let amount = price * qty;
                (amount.is_finite() && amount > 0.0).then_some(amount)
            })
        })
}

fn combo_rfq_execution_record_matches_keys(
    record: &ComboRfqExecutionJournalRecord,
    client_request_id: Option<&str>,
    rfq_id: Option<&str>,
    quote_id: Option<&str>,
) -> bool {
    shared_nonempty_key(Some(record.client_request_id.as_str()), client_request_id)
        || (shared_nonempty_key(record.rfq_id.as_deref(), rfq_id)
            && shared_nonempty_key(record.quote_id.as_deref(), quote_id))
}

fn combo_rfq_execution_records_share_recovery_key(
    left: &ComboRfqExecutionJournalRecord,
    right: &ComboRfqExecutionJournalRecord,
) -> bool {
    shared_nonempty_key(
        Some(left.client_request_id.as_str()),
        Some(right.client_request_id.as_str()),
    ) || (shared_nonempty_key(left.rfq_id.as_deref(), right.rfq_id.as_deref())
        && shared_nonempty_key(left.quote_id.as_deref(), right.quote_id.as_deref()))
}

pub fn build_combo_rfq_maker_scorecard(config: &Config) -> Result<ComboRfqMakerScorecard> {
    let path = config.diagnostics_dir.join(COMBO_RFQ_MAKER_JOURNAL_FILE);
    let records = read_combo_rfq_maker_journal_records(&path)?;
    let markout_race_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_MARKOUT_RACE_JOURNAL_FILE);
    let markout_race_records = read_combo_rfq_markout_race_journal_records(&markout_race_path)?;
    Ok(build_combo_rfq_maker_scorecard_from_records(
        config,
        &path,
        &records,
        &markout_race_records,
    ))
}

fn read_combo_rfq_maker_journal_records(path: &Path) -> Result<Vec<ComboRfqMakerJournalRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = fs::read_to_string(path)
        .with_context(|| format!("reading Combo/RFQ maker journal {}", path.display()))?;
    let mut records = Vec::new();
    for (idx, line) in body.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let record: ComboRfqMakerJournalRecord = serde_json::from_str(line).with_context(|| {
            format!(
                "parsing Combo/RFQ maker journal {} line {}",
                path.display(),
                idx + 1
            )
        })?;
        records.push(record);
    }
    Ok(records)
}

fn build_combo_rfq_maker_scorecard_from_records(
    config: &Config,
    path: &Path,
    records: &[ComboRfqMakerJournalRecord],
    markout_race_records: &[ComboRfqMarkoutRaceJournalRecord],
) -> ComboRfqMakerScorecard {
    #[derive(Default)]
    struct Accumulator {
        samples: usize,
        terminal_samples: usize,
        successes: usize,
        rejects: usize,
        failures: usize,
        stale_quotes: usize,
        pending: usize,
        realized_ev_sum: f64,
        realized_ev_count: usize,
        markout_samples: usize,
        adverse_markout_samples: usize,
        markout_bps_sum: f64,
        max_markout_bps: Option<f64>,
    }

    let mut by_maker: HashMap<String, Accumulator> = HashMap::new();
    for record in records {
        let Some(maker_id) = record
            .maker_id
            .as_deref()
            .map(str::trim)
            .filter(|maker_id| !maker_id.is_empty())
        else {
            continue;
        };
        let entry = by_maker.entry(maker_id.to_string()).or_default();
        entry.samples += 1;
        if record
            .quote_age_ms
            .map(|age_ms| age_ms >= 0 && age_ms as u64 > config.combo_rfq_quote_max_age_ms)
            .unwrap_or(false)
            || record
                .blockers
                .iter()
                .any(|blocker| blocker.contains("stale_quote"))
        {
            entry.stale_quotes += 1;
        }
        match record
            .terminal_status
            .as_deref()
            .map(normalize_rfq_status)
            .as_deref()
        {
            Some(status) if combo_rfq_maker_status_is_success(status) => {
                entry.terminal_samples += 1;
                entry.successes += 1;
            }
            Some(status) if combo_rfq_maker_status_is_reject(status) => {
                entry.terminal_samples += 1;
                entry.rejects += 1;
                entry.failures += 1;
            }
            Some(status) if combo_rfq_maker_status_is_failure(status) => {
                entry.terminal_samples += 1;
                entry.failures += 1;
            }
            Some(status) if combo_rfq_maker_status_is_pending(status) => {
                entry.pending += 1;
            }
            Some(_) => {
                entry.pending += 1;
            }
            None => {
                entry.pending += 1;
            }
        }
        if let Some(realized_ev) = record
            .realized_ev_usd
            .filter(|realized_ev| realized_ev.is_finite())
        {
            entry.realized_ev_sum += realized_ev;
            entry.realized_ev_count += 1;
        }
    }
    for record in markout_race_records {
        if !combo_rfq_markout_race_record_counts_for_score(config, record) {
            continue;
        }
        let Some(maker_id) = record
            .maker_id
            .as_deref()
            .map(str::trim)
            .filter(|maker_id| !maker_id.is_empty())
        else {
            continue;
        };
        let Some(markout_bps) = record.sampled_markout_bps.filter(|value| value.is_finite()) else {
            continue;
        };
        let entry = by_maker.entry(maker_id.to_string()).or_default();
        entry.markout_samples += 1;
        entry.markout_bps_sum += markout_bps;
        if markout_bps > config.combo_rfq_markout_race_max_adverse_bps {
            entry.adverse_markout_samples += 1;
        }
        entry.max_markout_bps = Some(
            entry
                .max_markout_bps
                .map(|current| current.max(markout_bps))
                .unwrap_or(markout_bps),
        );
    }

    let mut makers = by_maker
        .into_iter()
        .map(|(maker_id, acc)| {
            let reject_rate = if acc.terminal_samples > 0 {
                acc.rejects as f64 / acc.terminal_samples as f64
            } else {
                0.0
            };
            let stale_rate = if acc.samples > 0 {
                acc.stale_quotes as f64 / acc.samples as f64
            } else {
                0.0
            };
            let success_rate = if acc.terminal_samples > 0 {
                acc.successes as f64 / acc.terminal_samples as f64
            } else {
                0.0
            };
            let avg_realized_ev_usd = (acc.realized_ev_count > 0)
                .then_some(acc.realized_ev_sum / acc.realized_ev_count as f64);
            let avg_markout_bps = (acc.markout_samples > 0)
                .then_some(acc.markout_bps_sum / acc.markout_samples as f64);
            let adverse_markout_rate = (acc.markout_samples > 0)
                .then_some(acc.adverse_markout_samples as f64 / acc.markout_samples as f64);
            let mut blockers = Vec::new();
            if acc.terminal_samples >= COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES {
                if reject_rate > COMBO_RFQ_MAKER_MAX_REJECT_RATE {
                    blockers.push(format!(
                        "reject_rate:{reject_rate:.3}>{COMBO_RFQ_MAKER_MAX_REJECT_RATE:.3}"
                    ));
                }
                if success_rate < COMBO_RFQ_MAKER_MIN_SUCCESS_RATE {
                    blockers.push(format!(
                        "success_rate:{success_rate:.3}<{COMBO_RFQ_MAKER_MIN_SUCCESS_RATE:.3}"
                    ));
                }
                if avg_realized_ev_usd.map(|avg| avg < 0.0).unwrap_or(false) {
                    blockers.push(format!(
                        "avg_realized_ev_negative:{:.4}",
                        avg_realized_ev_usd.unwrap_or_default()
                    ));
                }
            }
            if acc.samples >= COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES
                && stale_rate > COMBO_RFQ_MAKER_MAX_STALE_RATE
            {
                blockers.push(format!(
                    "stale_rate:{stale_rate:.3}>{COMBO_RFQ_MAKER_MAX_STALE_RATE:.3}"
                ));
            }
            if acc.markout_samples >= config.combo_rfq_markout_race_min_samples
                && avg_markout_bps
                    .map(|avg| avg > config.combo_rfq_markout_race_max_adverse_bps)
                    .unwrap_or(false)
            {
                blockers.push(format!(
                    "avg_markout_bps:{:.2}>{:.2}:samples={}",
                    avg_markout_bps.unwrap_or_default(),
                    config.combo_rfq_markout_race_max_adverse_bps,
                    acc.markout_samples
                ));
            }
            let status = if acc.terminal_samples < COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES {
                "insufficient_terminal_samples"
            } else if blockers.is_empty() {
                "pass"
            } else {
                "blocked"
            };
            ComboRfqMakerScore {
                maker_id,
                samples: acc.samples,
                terminal_samples: acc.terminal_samples,
                successes: acc.successes,
                rejects: acc.rejects,
                failures: acc.failures,
                stale_quotes: acc.stale_quotes,
                pending: acc.pending,
                reject_rate,
                stale_rate,
                success_rate,
                avg_realized_ev_usd,
                markout_samples: acc.markout_samples,
                adverse_markout_samples: acc.adverse_markout_samples,
                avg_markout_bps,
                max_markout_bps: acc.max_markout_bps,
                adverse_markout_rate,
                status: status.to_string(),
                blockers,
            }
        })
        .collect::<Vec<_>>();
    makers.sort_by(|left, right| left.maker_id.cmp(&right.maker_id));
    let blocked_count = makers
        .iter()
        .filter(|maker| maker.status == "blocked")
        .count();
    ComboRfqMakerScorecard {
        status: if blocked_count == 0 {
            "ready".into()
        } else {
            "ready_with_blocked_makers".into()
        },
        journal_path: path.display().to_string(),
        records_seen: records.len(),
        maker_count: makers.len(),
        min_terminal_samples: COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES,
        makers,
        error: None,
    }
}

fn combo_rfq_markout_race_record_counts_for_score(
    config: &Config,
    record: &ComboRfqMarkoutRaceJournalRecord,
) -> bool {
    if record.status != "sampled" {
        return false;
    }
    if record.horizon_ms != config.combo_rfq_markout_race_score_horizon_ms {
        return false;
    }
    if record.sampled_markout_bps.is_none() {
        return false;
    }
    let Some(generated_at) = parse_rfc3339_timestamp(&record.generated_at) else {
        return false;
    };
    let now = Utc::now();
    if generated_at > now + ChronoDuration::seconds(5) {
        return false;
    }
    now.signed_duration_since(generated_at).num_seconds().max(0) as u64
        <= config.combo_rfq_markout_race_max_age_secs.max(1)
}

fn error_combo_rfq_maker_scorecard(config: &Config, err: &anyhow::Error) -> ComboRfqMakerScorecard {
    ComboRfqMakerScorecard {
        status: "error".into(),
        journal_path: config
            .diagnostics_dir
            .join(COMBO_RFQ_MAKER_JOURNAL_FILE)
            .display()
            .to_string(),
        records_seen: 0,
        maker_count: 0,
        min_terminal_samples: COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES,
        makers: Vec::new(),
        error: Some(err.to_string()),
    }
}

pub fn build_combo_rfq_best_execution_report(
    config: &Config,
    opp: &ArbitrageOpportunity,
    quote_response: Option<&Value>,
) -> ComboRfqBestExecutionReport {
    let requester = combo_rfq_requester_config_report(config);
    let requester_ready = requester.blockers.is_empty();
    let mut blockers = requester
        .blockers
        .iter()
        .map(|blocker| format!("requester:{blocker}"))
        .collect::<Vec<_>>();
    let maker_scorecard = match build_combo_rfq_maker_scorecard(config) {
        Ok(scorecard) => scorecard,
        Err(err) => {
            blockers.push(format!("maker_scorecard_unavailable:{err}"));
            error_combo_rfq_maker_scorecard(config, &err)
        }
    };
    if !config.combo_rfq_accept_enabled {
        blockers.push("COMBO_RFQ_ACCEPT_ENABLED=false".to_string());
    }

    let Some(quote_response) = quote_response else {
        blockers.push("missing_quote_response".to_string());
        return ComboRfqBestExecutionReport {
            status: "blocked_no_quote".into(),
            quotes_seen: 0,
            quotes_eligible: 0,
            selected_quote: None,
            maker_scorecard,
            requester_ready,
            accept_enabled: config.combo_rfq_accept_enabled,
            edge_gate_pass: false,
            last_look_gate_pass: false,
            accept_gate_pass: false,
            note: format!(
                "combo_rfq_best_execution=blocked_no_quote blockers={}",
                blockers.join("|")
            ),
            blockers,
        };
    };

    let mut quotes = parse_combo_rfq_quote_candidates(quote_response);
    let quotes_seen = quotes.len();
    let mut eligible = Vec::new();
    for mut quote in quotes.drain(..) {
        let mut quote_blockers = combo_rfq_quote_blockers(config, &quote);
        quote_blockers.extend(combo_rfq_maker_score_blockers(
            config,
            &maker_scorecard,
            &quote,
        ));
        quote.expected_edge_usd = combo_rfq_quote_expected_edge_usd(config, opp, &quote);
        if quote
            .expected_edge_usd
            .map(|edge| edge <= config.min_net_profit_usd)
            .unwrap_or(true)
        {
            quote_blockers.push(format!(
                "quote_edge_below_min:{}",
                quote
                    .expected_edge_usd
                    .map(|edge| format!("{edge:.4}"))
                    .unwrap_or_else(|| "unknown".into())
            ));
        }
        quote_blockers.extend(combo_rfq_last_look_blockers(config, &quote));
        if quote_blockers.is_empty() {
            eligible.push(quote);
        } else {
            blockers.push(format!(
                "quote_{}:{}",
                quote.quote_id,
                quote_blockers.join(",")
            ));
        }
    }

    eligible.sort_by(|left, right| {
        right
            .expected_edge_usd
            .unwrap_or(f64::NEG_INFINITY)
            .partial_cmp(&left.expected_edge_usd.unwrap_or(f64::NEG_INFINITY))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let selected_quote = loop {
        let Some(candidate) = eligible.first().cloned() else {
            break None;
        };
        let dispersion_blockers =
            combo_rfq_quote_dispersion_blockers(&maker_scorecard, &eligible, &candidate);
        if dispersion_blockers.is_empty() {
            break Some(candidate);
        }
        blockers.push(format!(
            "quote_{}:{}",
            candidate.quote_id,
            dispersion_blockers.join(",")
        ));
        eligible.remove(0);
    };
    if selected_quote.is_none() {
        blockers.push("no_eligible_quotes".to_string());
    }
    let edge_gate_pass = selected_quote
        .as_ref()
        .and_then(|quote| quote.expected_edge_usd)
        .map(|edge| edge > config.min_net_profit_usd)
        .unwrap_or(false);
    let last_look_gate_pass = selected_quote
        .as_ref()
        .map(|quote| combo_rfq_last_look_blockers(config, quote).is_empty())
        .unwrap_or(false);
    let accept_gate_pass = requester_ready
        && config.combo_rfq_accept_enabled
        && edge_gate_pass
        && last_look_gate_pass
        && selected_quote.is_some();
    let status = if accept_gate_pass {
        "ready_to_accept"
    } else if selected_quote.is_some() {
        "quote_selected_no_accept"
    } else {
        "blocked_no_eligible_quote"
    };

    ComboRfqBestExecutionReport {
        status: status.to_string(),
        quotes_seen,
        quotes_eligible: eligible.len(),
        selected_quote,
        maker_scorecard,
        requester_ready,
        accept_enabled: config.combo_rfq_accept_enabled,
        edge_gate_pass,
        last_look_gate_pass,
        accept_gate_pass,
        note: if blockers.is_empty() {
            format!(
                "combo_rfq_best_execution={status} quotes_seen={quotes_seen} quotes_eligible={}",
                eligible.len()
            )
        } else {
            format!(
                "combo_rfq_best_execution={status} quotes_seen={quotes_seen} quotes_eligible={} blockers={}",
                eligible.len(),
                blockers.join("|")
            )
        },
        blockers,
    }
}

#[cfg(test)]
pub async fn run_combo_rfq_execution_state_machine(
    client: &Client,
    config: &Config,
    catalog: &ComboMarketCatalog,
    opp: &ArbitrageOpportunity,
) -> Result<ComboRfqExecutionReport> {
    run_combo_rfq_execution_state_machine_inner(client, config, catalog, opp, None, false).await
}

pub async fn run_combo_rfq_execution_state_machine_with_price_cache(
    client: &Client,
    config: &Config,
    catalog: &ComboMarketCatalog,
    opp: &ArbitrageOpportunity,
    price_cache: Option<&PriceCache>,
) -> Result<ComboRfqExecutionReport> {
    run_combo_rfq_execution_state_machine_inner(client, config, catalog, opp, price_cache, true)
        .await
}

async fn run_combo_rfq_execution_state_machine_inner(
    client: &Client,
    config: &Config,
    catalog: &ComboMarketCatalog,
    opp: &ArbitrageOpportunity,
    price_cache: Option<&PriceCache>,
    require_price_cache: bool,
) -> Result<ComboRfqExecutionReport> {
    let started = Instant::now();
    let mut steps = Vec::new();
    let requester_plan = build_combo_rfq_requester_plan(config, catalog, opp);
    push_rfq_step(
        &mut steps,
        &started,
        "preflight",
        if requester_plan.blockers.is_empty() {
            "ready"
        } else {
            "blocked"
        },
        requester_plan.note.clone(),
    );
    let request = requester_plan.request.clone();
    if !requester_plan.blockers.is_empty() {
        return Ok(combo_rfq_execution_report(
            "blocked_preflight",
            request,
            None,
            None,
            build_combo_rfq_best_execution_report(config, opp, None),
            None,
            None,
            requester_plan.blockers,
            steps,
        ));
    }
    if !config.combo_rfq_accept_enabled {
        let blockers = vec!["COMBO_RFQ_ACCEPT_ENABLED=false".to_string()];
        return Ok(combo_rfq_execution_report(
            "blocked_accept_disabled",
            request,
            None,
            None,
            build_combo_rfq_best_execution_report(config, opp, None),
            None,
            None,
            blockers,
            steps,
        ));
    }

    let request = request.expect("requester plan ready implies request exists");
    let pending = pending_combo_rfq_execution_records(config, &request.client_request_id)?;
    if !pending.is_empty() {
        let blockers = vec![format!(
            "pending_combo_rfq_execution_recovery_required:{} records for client_request_id={}",
            pending.len(),
            request.client_request_id
        )];
        return Ok(combo_rfq_execution_report(
            "blocked_pending_recovery",
            Some(request),
            None,
            None,
            build_combo_rfq_best_execution_report(config, opp, None),
            None,
            None,
            blockers,
            steps,
        ));
    }
    let planned_condition_ids = combo_rfq_planned_condition_ids(opp);
    if let Err(err) =
        clob_client::verify_live_combo_rfq_markets(client, config, &planned_condition_ids).await
    {
        let (status, stage, blocker) = combo_rfq_market_readiness_failure(err, false);
        let blockers = vec![blocker];
        push_rfq_step(&mut steps, &started, stage, "blocked", blockers.join("|"));
        return Ok(combo_rfq_execution_report(
            status,
            Some(request),
            None,
            None,
            build_combo_rfq_best_execution_report(config, opp, None),
            None,
            None,
            blockers,
            steps,
        ));
    }
    push_rfq_step(
        &mut steps,
        &started,
        "delay_window_firewall",
        "ok",
        "planned_conditions_live_orderable",
    );
    record_combo_rfq_execution_journal(
        config,
        combo_rfq_execution_journal_record(
            opp,
            "create_rfq",
            "create_intent",
            Some(&request),
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
        ),
    )?;
    push_rfq_step(
        &mut steps,
        &started,
        "journal_execution",
        "ok",
        "stage=create_rfq status=create_intent",
    );
    let create_response = match create_combo_rfq(client, config, &request).await {
        Ok(response) => response,
        Err(err) => {
            record_combo_rfq_execution_journal(
                config,
                combo_rfq_execution_journal_record(
                    opp,
                    "create_rfq",
                    "create_state_unknown",
                    Some(&request),
                    None,
                    None,
                    None,
                    None,
                    Some(err.to_string()),
                    vec![
                        "rfq_create_state_unknown".to_string(),
                        "manual_recovery_required_before_recreate".to_string(),
                    ],
                ),
            )?;
            return Err(err).context("Combo/RFQ create request state unknown");
        }
    };
    let rfq_id = match combo_rfq_id_from_response(&create_response) {
        Some(rfq_id) => rfq_id,
        None => {
            let err = anyhow::anyhow!("Combo/RFQ create response missing rfq id");
            record_combo_rfq_execution_journal(
                config,
                combo_rfq_execution_journal_record(
                    opp,
                    "create_rfq",
                    "create_state_unknown",
                    Some(&request),
                    None,
                    None,
                    None,
                    Some(&create_response),
                    Some(err.to_string()),
                    vec![
                        "rfq_create_response_missing_id".to_string(),
                        "manual_recovery_required_before_recreate".to_string(),
                    ],
                ),
            )?;
            return Err(err);
        }
    };
    record_combo_rfq_execution_journal(
        config,
        combo_rfq_execution_journal_record(
            opp,
            "create_rfq",
            "request_created",
            Some(&request),
            Some(&rfq_id),
            None,
            None,
            Some(&create_response),
            None,
            Vec::new(),
        ),
    )?;
    push_rfq_step(
        &mut steps,
        &started,
        "create_rfq",
        "ok",
        format!("rfq_id={rfq_id}"),
    );

    let pre_accept_market_readiness =
        spawn_combo_rfq_market_readiness_check(client, config, planned_condition_ids.clone());
    push_rfq_step(
        &mut steps,
        &started,
        "pre_accept_market_readiness_prefetch",
        "started",
        "overlap_with_quote_collection",
    );

    let quote_collection_status = wait_for_combo_rfq_stream_quote_or_timeout(
        config,
        &rfq_id,
        Duration::from_millis(COMBO_RFQ_QUOTE_COLLECTION_WINDOW_MS),
    )
    .await?;
    push_rfq_step(
        &mut steps,
        &started,
        "collect_quotes",
        "ok",
        format!(
            "window_ms={COMBO_RFQ_QUOTE_COLLECTION_WINDOW_MS} status={quote_collection_status}"
        ),
    );

    let (quote_response, quote_source) =
        match query_combo_rfq_quotes_stream_first(client, config, &rfq_id, Some("ACTIVE")).await {
            Ok(response) => response,
            Err(err) => {
                record_combo_rfq_execution_journal(
                    config,
                    combo_rfq_execution_journal_record(
                        opp,
                        "query_quotes",
                        "quote_query_state_unknown",
                        Some(&request),
                        Some(&rfq_id),
                        None,
                        None,
                        None,
                        Some(err.to_string()),
                        vec![
                            "rfq_quote_query_state_unknown".to_string(),
                            "manual_recovery_required_before_requery_or_recreate".to_string(),
                        ],
                    ),
                )?;
                return Err(err).context("Combo/RFQ quote query state unknown");
            }
        };
    let quote_response_received_at = Instant::now();
    let best_execution = build_combo_rfq_best_execution_report(config, opp, Some(&quote_response));
    push_rfq_step(
        &mut steps,
        &started,
        "query_quotes",
        best_execution.status.as_str(),
        format!("source={quote_source} {}", best_execution.note),
    );
    if !best_execution.accept_gate_pass {
        record_combo_rfq_execution_journal(
            config,
            combo_rfq_execution_journal_record(
                opp,
                "query_quotes",
                "blocked_best_execution",
                Some(&request),
                Some(&rfq_id),
                best_execution.selected_quote.as_ref(),
                None,
                Some(&quote_response),
                None,
                best_execution.blockers.clone(),
            ),
        )?;
        return Ok(combo_rfq_execution_report(
            "blocked_best_execution",
            Some(request),
            Some(rfq_id),
            Some(quote_response),
            best_execution.clone(),
            None,
            None,
            best_execution.blockers.clone(),
            steps,
        ));
    }

    let quote = best_execution
        .selected_quote
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("RFQ best execution gate passed without selected quote"))?;
    let quote_contract_blockers =
        combo_rfq_quote_request_contract_blockers(config, &request, &rfq_id, quote);
    if !quote_contract_blockers.is_empty() {
        record_combo_rfq_execution_journal(
            config,
            combo_rfq_execution_journal_record(
                opp,
                "query_quotes",
                "blocked_best_execution",
                Some(&request),
                Some(&rfq_id),
                Some(quote),
                None,
                Some(&quote_response),
                None,
                quote_contract_blockers.clone(),
            ),
        )?;
        push_rfq_step(
            &mut steps,
            &started,
            "quote_contract",
            "blocked",
            quote_contract_blockers.join("|"),
        );
        return Ok(combo_rfq_execution_report(
            "blocked_quote_contract",
            Some(request),
            Some(rfq_id),
            Some(quote_response),
            best_execution,
            None,
            None,
            quote_contract_blockers,
            steps,
        ));
    }
    let accept_request = combo_rfq_accept_request_from_quote(quote)?;
    let pre_accept_markout = combo_rfq_pre_accept_markout_report(
        client,
        config,
        opp,
        quote,
        &accept_request,
        price_cache,
        require_price_cache,
    )
    .await;
    push_rfq_step(
        &mut steps,
        &started,
        "pre_accept_markout",
        pre_accept_markout.status.as_str(),
        if pre_accept_markout.blockers.is_empty() {
            format!(
                "quote_edge_usd={:.4} public_edge_usd={:.4} markout_bps={:.2} toxicity_haircut_usd={:.4}",
                pre_accept_markout.quote_edge_usd,
                pre_accept_markout.public_edge_usd,
                pre_accept_markout.markout_bps,
                pre_accept_markout.toxicity_haircut_usd
            )
        } else {
            pre_accept_markout.blockers.join("|")
        },
    );
    match append_combo_rfq_adverse_selection_journal_record(
        config,
        &combo_rfq_adverse_selection_journal_record(opp, &rfq_id, quote, &pre_accept_markout),
    ) {
        Ok(path) => push_rfq_step(
            &mut steps,
            &started,
            "journal_adverse_selection",
            "ok",
            format!("path={}", path.display()),
        ),
        Err(err) => {
            warn!("Failed to write Combo/RFQ adverse selection journal: {err:#}");
            push_rfq_step(
                &mut steps,
                &started,
                "journal_adverse_selection",
                "warning",
                err.to_string(),
            );
        }
    }
    spawn_combo_rfq_markout_race_sampler(client, config, opp, &rfq_id, quote, &pre_accept_markout);
    if !pre_accept_markout.blockers.is_empty() {
        record_combo_rfq_execution_journal(
            config,
            combo_rfq_execution_journal_record(
                opp,
                "pre_accept_markout",
                "blocked_best_execution",
                Some(&request),
                Some(&rfq_id),
                Some(quote),
                Some(&accept_request),
                Some(&serde_json::to_value(&pre_accept_markout)?),
                None,
                pre_accept_markout.blockers.clone(),
            ),
        )?;
        let mut report = combo_rfq_execution_report(
            "blocked_pre_accept_markout",
            Some(request),
            Some(rfq_id),
            Some(quote_response),
            best_execution,
            Some(accept_request),
            None,
            pre_accept_markout.blockers.clone(),
            steps,
        );
        report.pre_accept_markout = Some(pre_accept_markout);
        return Ok(report);
    }
    let pre_accept_freshness_blockers = combo_rfq_pre_accept_freshness_blockers(
        config,
        quote,
        quote_response_received_at.elapsed(),
    );
    if !pre_accept_freshness_blockers.is_empty() {
        record_combo_rfq_execution_journal(
            config,
            combo_rfq_execution_journal_record(
                opp,
                "pre_accept_freshness",
                "blocked_best_execution",
                Some(&request),
                Some(&rfq_id),
                Some(quote),
                Some(&accept_request),
                Some(&serde_json::json!({
                    "blockers": pre_accept_freshness_blockers.clone(),
                    "quote_response_to_accept_elapsed_ms": quote_response_received_at.elapsed().as_millis(),
                })),
                None,
                pre_accept_freshness_blockers.clone(),
            ),
        )?;
        push_rfq_step(
            &mut steps,
            &started,
            "pre_accept_freshness",
            "blocked",
            pre_accept_freshness_blockers.join("|"),
        );
        let mut report = combo_rfq_execution_report(
            "blocked_pre_accept_freshness",
            Some(request),
            Some(rfq_id),
            Some(quote_response),
            best_execution,
            Some(accept_request),
            None,
            pre_accept_freshness_blockers,
            steps,
        );
        report.pre_accept_markout = Some(pre_accept_markout);
        return Ok(report);
    }
    if let Err(err) = combo_rfq_market_readiness_result(pre_accept_market_readiness).await {
        let (status, stage, blocker) = combo_rfq_market_readiness_failure(err, true);
        let blockers = vec![blocker];
        record_combo_rfq_execution_journal(
            config,
            combo_rfq_execution_journal_record(
                opp,
                stage,
                "blocked_best_execution",
                Some(&request),
                Some(&rfq_id),
                Some(quote),
                Some(&accept_request),
                Some(&serde_json::json!({
                    "blockers": blockers.clone(),
                })),
                None,
                blockers.clone(),
            ),
        )?;
        push_rfq_step(&mut steps, &started, stage, "blocked", blockers.join("|"));
        let mut report = combo_rfq_execution_report(
            status,
            Some(request),
            Some(rfq_id),
            Some(quote_response),
            best_execution,
            Some(accept_request),
            None,
            blockers,
            steps,
        );
        report.pre_accept_markout = Some(pre_accept_markout);
        return Ok(report);
    }
    push_rfq_step(
        &mut steps,
        &started,
        "pre_accept_delay_window_firewall",
        "ok",
        "planned_conditions_live_orderable prefetch=ready",
    );
    record_combo_rfq_execution_journal(
        config,
        combo_rfq_execution_journal_record(
            opp,
            "accept_quote",
            "accept_intent",
            Some(&request),
            Some(&rfq_id),
            Some(quote),
            Some(&accept_request),
            None,
            None,
            Vec::new(),
        ),
    )?;
    push_rfq_step(
        &mut steps,
        &started,
        "journal_execution",
        "ok",
        format!(
            "stage=accept_quote status=accept_intent quote_id={}",
            quote.quote_id
        ),
    );
    let accept_response =
        match accept_combo_rfq_quote(client, config, &rfq_id, &quote.quote_id, &accept_request)
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let mut blockers = vec![
                    "rfq_accept_state_unknown".to_string(),
                    "exposure_must_remain_reserved_until_finality_or_manual_review".to_string(),
                    format!("accept_error:{err:#}"),
                ];
                match record_combo_rfq_execution_journal(
                    config,
                    combo_rfq_execution_journal_record(
                        opp,
                        "accept_quote",
                        "accept_state_unknown",
                        Some(&request),
                        Some(&rfq_id),
                        Some(quote),
                        Some(&accept_request),
                        None,
                        Some(err.to_string()),
                        blockers.clone(),
                    ),
                ) {
                    Ok(path) => push_rfq_step(
                        &mut steps,
                        &started,
                        "journal_execution",
                        "ok",
                        format!(
                            "stage=accept_quote status=accept_state_unknown path={}",
                            path.display()
                        ),
                    ),
                    Err(journal_err) => {
                        blockers.push(format!("execution_journal_write_failed:{journal_err}"));
                        push_rfq_step(
                            &mut steps,
                            &started,
                            "journal_execution",
                            "error",
                            journal_err.to_string(),
                        );
                    }
                }
                let maker_record = ComboRfqMakerJournalRecord {
                    generated_at: Utc::now().to_rfc3339(),
                    maker_id: quote.maker_id.clone(),
                    quote_id: quote.quote_id.clone(),
                    rfq_id: Some(rfq_id.clone()),
                    event_id: opp.event_id.clone(),
                    quote_age_ms: quote.age_ms,
                    expected_edge_usd: quote.expected_edge_usd,
                    selected: true,
                    accepted: false,
                    terminal_status: Some("accept_state_unknown".into()),
                    realized_ev_usd: None,
                    blockers: blockers.clone(),
                    notes: vec!["accept_request_error_after_best_execution_gate".into()],
                };
                match append_combo_rfq_maker_journal_record(config, &maker_record) {
                    Ok(path) => push_rfq_step(
                        &mut steps,
                        &started,
                        "journal_maker",
                        "ok",
                        format!("path={}", path.display()),
                    ),
                    Err(journal_err) => {
                        blockers.push(format!("maker_journal_write_failed:{journal_err}"));
                        push_rfq_step(
                            &mut steps,
                            &started,
                            "journal_maker",
                            "error",
                            journal_err.to_string(),
                        );
                    }
                }
                push_rfq_step(
                    &mut steps,
                    &started,
                    "accept_quote",
                    "state_unknown",
                    format!("quote_id={} error={err:#}", quote.quote_id),
                );
                let mut report = combo_rfq_execution_report(
                    "accept_state_unknown",
                    Some(request),
                    Some(rfq_id),
                    Some(quote_response),
                    best_execution,
                    Some(accept_request),
                    None,
                    blockers,
                    steps,
                );
                report.pre_accept_markout = Some(pre_accept_markout);
                return Ok(report);
            }
        };
    let (accept_outcome, accept_response_blockers) =
        combo_rfq_accept_response_outcome(&rfq_id, quote, &accept_request, &accept_response);
    push_rfq_step(
        &mut steps,
        &started,
        "accept_quote",
        match accept_outcome {
            ComboRfqAcceptOutcome::Accepted => "accepted",
            ComboRfqAcceptOutcome::RejectedProven => "rejected_proven",
            ComboRfqAcceptOutcome::Unknown => "not_accepted",
        },
        match accept_outcome {
            ComboRfqAcceptOutcome::Accepted => format!("quote_id={}", quote.quote_id),
            ComboRfqAcceptOutcome::RejectedProven | ComboRfqAcceptOutcome::Unknown => {
                accept_response_blockers.join("|")
            }
        },
    );
    if accept_outcome != ComboRfqAcceptOutcome::Accepted {
        let (status, mut blockers, maker_note) = match accept_outcome {
            ComboRfqAcceptOutcome::RejectedProven => (
                "accept_rejected_proven",
                accept_response_blockers,
                "accept_response_rejected_same_rfq_quote",
            ),
            ComboRfqAcceptOutcome::Unknown => {
                let mut blockers = vec![
                    "rfq_accept_response_not_proven_accepted".to_string(),
                    "exposure_must_remain_reserved_until_finality_or_manual_review".to_string(),
                ];
                blockers.extend(accept_response_blockers);
                (
                    "accept_response_not_accepted",
                    blockers,
                    "accept_response_failed_schema_or_status_validation",
                )
            }
            ComboRfqAcceptOutcome::Accepted => unreachable!(),
        };
        match record_combo_rfq_execution_journal(
            config,
            combo_rfq_execution_journal_record(
                opp,
                "accept_quote",
                status,
                Some(&request),
                Some(&rfq_id),
                Some(quote),
                Some(&accept_request),
                Some(&accept_response),
                None,
                blockers.clone(),
            ),
        ) {
            Ok(path) => push_rfq_step(
                &mut steps,
                &started,
                "journal_execution",
                "ok",
                format!(
                    "stage=accept_quote status={} path={}",
                    status,
                    path.display()
                ),
            ),
            Err(err) => {
                blockers.push(format!("execution_journal_write_failed:{err}"));
                push_rfq_step(
                    &mut steps,
                    &started,
                    "journal_execution",
                    "error",
                    err.to_string(),
                );
            }
        }
        let maker_record = ComboRfqMakerJournalRecord {
            generated_at: Utc::now().to_rfc3339(),
            maker_id: quote.maker_id.clone(),
            quote_id: quote.quote_id.clone(),
            rfq_id: Some(rfq_id.clone()),
            event_id: opp.event_id.clone(),
            quote_age_ms: quote.age_ms,
            expected_edge_usd: quote.expected_edge_usd,
            selected: true,
            accepted: false,
            terminal_status: Some(status.into()),
            realized_ev_usd: None,
            blockers: blockers.clone(),
            notes: vec![maker_note.into()],
        };
        match append_combo_rfq_maker_journal_record(config, &maker_record) {
            Ok(path) => push_rfq_step(
                &mut steps,
                &started,
                "journal_maker",
                "ok",
                format!("path={}", path.display()),
            ),
            Err(err) => {
                blockers.push(format!("maker_journal_write_failed:{err}"));
                push_rfq_step(
                    &mut steps,
                    &started,
                    "journal_maker",
                    "error",
                    err.to_string(),
                );
            }
        }
        let mut report = combo_rfq_execution_report(
            status,
            Some(request),
            Some(rfq_id),
            Some(quote_response),
            best_execution,
            Some(accept_request),
            Some(accept_response),
            blockers,
            steps,
        );
        report.pre_accept_markout = Some(pre_accept_markout);
        return Ok(report);
    }
    let mut blockers = vec![
        "rfq_finality_stream_not_verified".to_string(),
        "realized_pnl_label_not_written".to_string(),
    ];
    match record_combo_rfq_execution_journal(
        config,
        combo_rfq_execution_journal_record(
            opp,
            "accept_quote",
            "accepted_pending_finality",
            Some(&request),
            Some(&rfq_id),
            Some(quote),
            Some(&accept_request),
            Some(&accept_response),
            None,
            blockers.clone(),
        ),
    ) {
        Ok(path) => push_rfq_step(
            &mut steps,
            &started,
            "journal_execution",
            "ok",
            format!(
                "stage=accept_quote status=accepted_pending_finality path={}",
                path.display()
            ),
        ),
        Err(err) => {
            blockers.push(format!("execution_journal_write_failed:{err}"));
            push_rfq_step(
                &mut steps,
                &started,
                "journal_execution",
                "error",
                err.to_string(),
            );
        }
    }
    let maker_record = ComboRfqMakerJournalRecord {
        generated_at: Utc::now().to_rfc3339(),
        maker_id: quote.maker_id.clone(),
        quote_id: quote.quote_id.clone(),
        rfq_id: Some(rfq_id.clone()),
        event_id: opp.event_id.clone(),
        quote_age_ms: quote.age_ms,
        expected_edge_usd: quote.expected_edge_usd,
        selected: true,
        accepted: true,
        terminal_status: Some("accepted_pending_finality".into()),
        realized_ev_usd: None,
        blockers: blockers.clone(),
        notes: vec!["accept_response_recorded_before_finality".into()],
    };
    match append_combo_rfq_maker_journal_record(config, &maker_record) {
        Ok(path) => push_rfq_step(
            &mut steps,
            &started,
            "journal_maker",
            "ok",
            format!("path={}", path.display()),
        ),
        Err(err) => {
            blockers.push(format!("maker_journal_write_failed:{err}"));
            push_rfq_step(
                &mut steps,
                &started,
                "journal_maker",
                "error",
                err.to_string(),
            );
        }
    }
    maybe_ingest_combo_rfq_finality_after_accept(
        config,
        &rfq_id,
        &started,
        &mut blockers,
        &mut steps,
    )
    .await;
    let mut report = combo_rfq_execution_report(
        "accepted_pending_finality",
        Some(request),
        Some(rfq_id),
        Some(quote_response),
        best_execution,
        Some(accept_request),
        Some(accept_response),
        blockers,
        steps,
    );
    report.pre_accept_markout = Some(pre_accept_markout);
    Ok(report)
}

async fn maybe_ingest_combo_rfq_finality_after_accept(
    config: &Config,
    rfq_id: &str,
    started: &Instant,
    blockers: &mut Vec<String>,
    steps: &mut Vec<ComboRfqExecutionStep>,
) {
    if !config.combo_rfq_stream_enabled {
        return;
    }
    let wait = Duration::from_millis(COMBO_RFQ_POST_ACCEPT_FINALITY_WAIT_MS);
    if !crate::rfq_finality::wait_for_cached_combo_rfq_stream_event(rfq_id, wait).await {
        push_rfq_step(
            steps,
            started,
            "post_accept_finality",
            "wait_elapsed",
            format!("rfq_id={rfq_id} wait_ms={COMBO_RFQ_POST_ACCEPT_FINALITY_WAIT_MS}"),
        );
        return;
    }
    match crate::rfq_finality::write_combo_rfq_finality_report(config) {
        Ok(path) => push_rfq_step(
            steps,
            started,
            "post_accept_finality",
            "ingested",
            format!("rfq_id={rfq_id} report_path={}", path.display()),
        ),
        Err(err) => {
            blockers.push(format!("rfq_finality_ingest_failed:{err}"));
            push_rfq_step(
                steps,
                started,
                "post_accept_finality",
                "error",
                err.to_string(),
            );
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ComboMarketCatalog {
    markets_by_condition: HashMap<String, ComboMarketEntry>,
    conditions_by_position_id: HashMap<String, String>,
    duplicate_position_ids: HashSet<String>,
}

impl ComboMarketCatalog {
    pub fn from_markets(markets: Vec<ComboMarketEntry>) -> Self {
        let mut markets_by_condition = HashMap::new();
        let mut conditions_by_position_id = HashMap::new();
        let mut duplicate_position_ids = HashSet::new();

        for market in markets {
            if market.condition_id.trim().is_empty() {
                continue;
            }
            let condition_id = market.condition_id.trim().to_string();
            for position_id in &market.position_ids {
                if !position_id.trim().is_empty() {
                    let position_id = position_id.trim().to_string();
                    if conditions_by_position_id
                        .insert(position_id.clone(), condition_id.clone())
                        .is_some()
                    {
                        duplicate_position_ids.insert(position_id);
                    }
                }
            }
            markets_by_condition.insert(condition_id, market);
        }

        Self {
            markets_by_condition,
            conditions_by_position_id,
            duplicate_position_ids,
        }
    }

    pub fn len(&self) -> usize {
        self.markets_by_condition.len()
    }

    pub fn is_empty(&self) -> bool {
        self.markets_by_condition.is_empty()
    }

    pub fn combo_symbol_for_condition(&self, condition_id: &str) -> Option<&str> {
        self.markets_by_condition
            .get(condition_id)
            .map(|market| market.slug.trim())
            .filter(|symbol| !symbol.is_empty())
    }

    pub fn outcome_index_for_position_id(
        &self,
        condition_id: &str,
        position_id: &str,
    ) -> Option<u8> {
        let position_id = position_id.trim();
        if position_id.is_empty() || self.duplicate_position_ids.contains(position_id) {
            return None;
        }
        self.markets_by_condition
            .get(condition_id.trim())
            .and_then(|market| market.outcome_index_for_position_id(position_id))
    }

    pub fn route_report(&self, opp: &ArbitrageOpportunity) -> ComboRouteReport {
        let planned_legs = opp.execution_plan.len();
        if planned_legs <= 1 {
            return ComboRouteReport {
                route: AtomicRouteHint::None,
                planned_legs,
                unique_conditions: planned_legs,
                combo_conditions: 0,
                token_position_matches: 0,
                reason: "single_leg_or_empty_plan".into(),
            };
        }
        if matches!(opp.arb_type, ArbType::Bundle | ArbType::MintSell) {
            return ComboRouteReport {
                route: AtomicRouteHint::None,
                planned_legs,
                unique_conditions: unique_condition_count(opp),
                combo_conditions: 0,
                token_position_matches: 0,
                reason: "full_set_bundle_route_needs_ctf_adapter_not_combo_rfq".into(),
            };
        }
        if opp
            .execution_plan
            .iter()
            .any(|leg| leg.token_id.trim().is_empty() || is_external_token_id(&leg.token_id))
        {
            return ComboRouteReport {
                route: AtomicRouteHint::None,
                planned_legs,
                unique_conditions: unique_condition_count(opp),
                combo_conditions: 0,
                token_position_matches: 0,
                reason: "missing_or_external_token".into(),
            };
        }

        let unique_conditions: HashSet<String> = opp
            .execution_plan
            .iter()
            .map(|leg| leg.condition_id.trim().to_string())
            .filter(|condition_id| !condition_id.is_empty())
            .collect();
        if unique_conditions.len() < 2 || unique_conditions.len() != planned_legs {
            return ComboRouteReport {
                route: AtomicRouteHint::None,
                planned_legs,
                unique_conditions: unique_conditions.len(),
                combo_conditions: 0,
                token_position_matches: 0,
                reason: "combo_rfq_requires_multiple_distinct_underlying_conditions".into(),
            };
        }
        if let Some(condition_id) = opp
            .execution_plan
            .iter()
            .filter_map(|leg| {
                opp.markets
                    .get(leg.market_index)
                    .filter(|market| market.clob_rfq_enabled == Some(false))
                    .map(|market| market.condition_id.trim().to_string())
            })
            .find(|condition_id| !condition_id.is_empty())
        {
            return ComboRouteReport {
                route: AtomicRouteHint::None,
                planned_legs,
                unique_conditions: unique_conditions.len(),
                combo_conditions: 0,
                token_position_matches: 0,
                reason: format!("clob_market_rfq_disabled:{condition_id}"),
            };
        }

        let combo_conditions = unique_conditions
            .iter()
            .filter(|condition_id| self.markets_by_condition.contains_key(*condition_id))
            .count();
        let catalog_schema_blocker = unique_conditions
            .iter()
            .filter_map(|condition_id| self.condition_schema_blocker(condition_id))
            .next();
        let token_position_matches = opp
            .execution_plan
            .iter()
            .filter(|leg| {
                self.leg_position_matches(leg.condition_id.trim(), &leg.token_id, leg.outcome)
            })
            .count();
        let route = if combo_conditions == unique_conditions.len()
            && catalog_schema_blocker.is_none()
            && token_position_matches == planned_legs
        {
            AtomicRouteHint::ComboRfqCandidate
        } else {
            AtomicRouteHint::None
        };
        let reason = if combo_conditions != unique_conditions.len() {
            "one_or_more_conditions_missing_from_public_combo_catalog"
        } else if let Some(blocker) = catalog_schema_blocker {
            blocker
        } else if token_position_matches != planned_legs {
            "one_or_more_planned_tokens_do_not_match_combo_catalog_outcome_positions"
        } else if matches!(route, AtomicRouteHint::ComboRfqCandidate) {
            "all_planned_conditions_in_public_combo_catalog"
        } else {
            "not_combo_rfq_candidate"
        };

        ComboRouteReport {
            route,
            planned_legs,
            unique_conditions: unique_conditions.len(),
            combo_conditions,
            token_position_matches,
            reason: reason.into(),
        }
    }

    fn condition_schema_blocker(&self, condition_id: &str) -> Option<&'static str> {
        let market = self.markets_by_condition.get(condition_id)?;
        if market.slug.trim().is_empty() {
            return Some("combo_catalog_symbol_empty");
        }
        let yes_position = market
            .position_id_for_outcome(OutcomeSide::Yes)
            .map(str::trim)
            .filter(|position_id| !position_id.is_empty());
        let no_position = market
            .position_id_for_outcome(OutcomeSide::No)
            .map(str::trim)
            .filter(|position_id| !position_id.is_empty());
        let (Some(yes_position), Some(no_position)) = (yes_position, no_position) else {
            return Some("combo_catalog_missing_yes_no_position_ids");
        };
        if yes_position == no_position {
            return Some("combo_catalog_duplicate_yes_no_position_id");
        }
        if self.duplicate_position_ids.contains(yes_position)
            || self.duplicate_position_ids.contains(no_position)
        {
            return Some("combo_catalog_position_id_not_unique");
        }
        let yes_maps_to_condition = self
            .conditions_by_position_id
            .get(yes_position)
            .map(|known_condition| known_condition == condition_id)
            .unwrap_or(false);
        let no_maps_to_condition = self
            .conditions_by_position_id
            .get(no_position)
            .map(|known_condition| known_condition == condition_id)
            .unwrap_or(false);
        if !yes_maps_to_condition || !no_maps_to_condition {
            return Some("combo_catalog_position_index_unstable");
        }
        None
    }

    fn leg_position_matches(
        &self,
        condition_id: &str,
        token_id: &str,
        outcome: OutcomeSide,
    ) -> bool {
        if condition_id.is_empty() || token_id.trim().is_empty() {
            return false;
        }
        self.markets_by_condition
            .get(condition_id)
            .and_then(|market| market.position_id_for_outcome(outcome))
            .map(|position_id| position_id.trim() == token_id.trim())
            .unwrap_or(false)
    }
}

pub async fn create_combo_rfq(
    client: &Client,
    config: &Config,
    request: &ComboRfqCreateRequest,
) -> Result<Value> {
    ensure_combo_rfq_requester_ready(config)?;
    let url = format!(
        "{}/v1/combos/rfqs",
        config.combo_rfq_requester_api_url.trim_end_matches('/')
    );
    send_combo_rfq_request_with_retries(
        config,
        "Combo/RFQ create request",
        ComboRfqRetryPolicy::WriteRateLimitOnly,
        Some(combo_rfq_live_request_deadline(config)),
        || authed_combo_rfq_request(client, config, reqwest::Method::POST, &url).json(request),
    )
    .await
}

pub async fn query_combo_rfq_quotes(
    client: &Client,
    config: &Config,
    rfq_id: &str,
    status: Option<&str>,
) -> Result<Value> {
    ensure_combo_rfq_requester_ready(config)?;
    let rfq_id = combo_rfq_path_segment(rfq_id, "rfq_id")?;
    let url = format!(
        "{}/v1/combos/quotes",
        config.combo_rfq_requester_api_url.trim_end_matches('/')
    );
    let mut params = vec![("rfqId", rfq_id)];
    if let Some(status) = status.map(str::trim).filter(|status| !status.is_empty()) {
        params.push(("status", status.to_string()));
    }
    send_combo_rfq_request_with_retries(
        config,
        "Combo/RFQ quote query",
        ComboRfqRetryPolicy::ReadOnlyPreserveWriteCapacity,
        Some(combo_rfq_live_request_deadline(config)),
        || authed_combo_rfq_request(client, config, reqwest::Method::GET, &url).query(&params),
    )
    .await
}

async fn query_combo_rfq_quotes_stream_first(
    client: &Client,
    config: &Config,
    rfq_id: &str,
    status: Option<&str>,
) -> Result<(Value, &'static str)> {
    if let Some(response) = combo_rfq_quote_response_from_stream_journal(config, rfq_id)? {
        return Ok((response, "stream_journal"));
    }
    query_combo_rfq_quotes(client, config, rfq_id, status)
        .await
        .map(|response| (response, "polling_rest"))
}

async fn wait_for_combo_rfq_stream_quote_or_timeout(
    config: &Config,
    rfq_id: &str,
    timeout: Duration,
) -> Result<&'static str> {
    let started = Instant::now();
    loop {
        if combo_rfq_quote_response_from_stream_journal(config, rfq_id)?.is_some() {
            return Ok("stream_quote_ready");
        }
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            return Ok("collection_window_elapsed");
        }
        let wait = (timeout - elapsed).min(Duration::from_millis(25));
        let _ = crate::rfq_finality::wait_for_cached_combo_rfq_stream_event(rfq_id, wait).await;
    }
}

fn combo_rfq_quote_response_from_stream_journal(
    config: &Config,
    rfq_id: &str,
) -> Result<Option<Value>> {
    if let Some(response) =
        combo_rfq_parsed_stream_quote_response(rfq_id, "combo_rfq_stream_parsed_cache")
    {
        return Ok(Some(response));
    }
    if let Some(response) = combo_rfq_quote_response_from_stream_cache(rfq_id) {
        return Ok(Some(response));
    }

    let path = config
        .diagnostics_dir
        .join(crate::rfq_finality::COMBO_RFQ_FINALITY_EVENTS_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let metadata = fs::metadata(&path)
        .ok()
        .map(|metadata| (metadata.len(), metadata.modified().ok()));
    if let Some((len, modified)) = metadata {
        if combo_rfq_stream_journal_scan_recent(&path, rfq_id, len, modified) {
            return Ok(None);
        }
    }
    let file = File::open(&path)
        .with_context(|| format!("opening Combo/RFQ stream quote journal {}", path.display()))?;
    let mut quotes = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| {
            format!("reading Combo/RFQ stream quote journal {}", path.display())
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if combo_rfq_stream_quote_is_fresh(rfq_id, &value, None) {
            quotes.push(value);
        }
    }
    let quotes = combo_rfq_dedup_quote_values_by_quote_id(quotes);
    if quotes.is_empty() {
        if let Some((len, modified)) = metadata {
            record_combo_rfq_stream_journal_scan(&path, rfq_id, len, modified);
        }
        Ok(None)
    } else {
        Ok(Some(serde_json::json!({
            "quotes": quotes,
            "source": "combo_rfq_stream_journal",
        })))
    }
}

fn combo_rfq_quote_response_from_stream_cache(rfq_id: &str) -> Option<Value> {
    if let Some(response) =
        combo_rfq_parsed_stream_quote_response(rfq_id, "combo_rfq_stream_parsed_cache")
    {
        return Some(response);
    }
    let quotes: Vec<Value> = crate::rfq_finality::cached_combo_rfq_stream_events_for_rfq(rfq_id)
        .into_iter()
        .filter(|value| combo_rfq_stream_quote_is_fresh(rfq_id, value, None))
        .collect();
    let quotes = combo_rfq_dedup_quote_values_by_quote_id(quotes);
    if quotes.is_empty() {
        None
    } else {
        cache_combo_rfq_parsed_stream_quotes(rfq_id, quotes.clone());
        Some(serde_json::json!({
            "quotes": quotes,
            "source": "combo_rfq_stream_cache",
        }))
    }
}

fn combo_rfq_stream_quote_is_fresh(
    rfq_id: &str,
    payload: &Value,
    cached_at: Option<Instant>,
) -> bool {
    parse_combo_rfq_quote_candidate(payload)
        .filter(|quote| {
            quote.rfq_id.as_deref() == Some(rfq_id)
                && quote
                    .age_ms
                    .map(|age| age <= COMBO_RFQ_STREAM_QUOTE_MAX_AGE_MS)
                    .unwrap_or(false)
                && cached_at
                    .map(|cached_at| {
                        cached_at.elapsed().as_millis()
                            <= COMBO_RFQ_STREAM_QUOTE_MAX_AGE_MS.max(1) as u128
                    })
                    .unwrap_or(true)
        })
        .is_some()
}

fn combo_rfq_quote_value_id(payload: &Value) -> Option<String> {
    parse_combo_rfq_quote_candidate(payload)
        .map(|quote| quote.quote_id.trim().to_string())
        .filter(|quote_id| !quote_id.is_empty())
}

fn combo_rfq_dedup_quote_values_by_quote_id(quotes: Vec<Value>) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(quotes.len());
    for quote in quotes.into_iter().rev() {
        let Some(quote_id) = combo_rfq_quote_value_id(&quote) else {
            continue;
        };
        if seen.insert(quote_id) {
            deduped.push(quote);
        }
    }
    deduped.reverse();
    deduped
}

fn combo_rfq_parsed_stream_quote_cache(
) -> &'static Mutex<HashMap<String, Vec<ComboRfqParsedStreamQuote>>> {
    COMBO_RFQ_STREAM_PARSED_QUOTE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn combo_rfq_stream_journal_scan_cache(
) -> &'static Mutex<HashMap<(PathBuf, String), ComboRfqStreamJournalScanState>> {
    COMBO_RFQ_STREAM_JOURNAL_SCAN_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_combo_rfq_parsed_stream_quotes(rfq_id: &str, quotes: Vec<Value>) {
    let rfq_id = rfq_id.trim();
    let quotes = combo_rfq_dedup_quote_values_by_quote_id(quotes);
    if rfq_id.is_empty() || quotes.is_empty() {
        return;
    }
    let Ok(mut cache) = combo_rfq_parsed_stream_quote_cache().lock() else {
        return;
    };
    if !cache.contains_key(rfq_id) && cache.len() >= COMBO_RFQ_STREAM_PARSED_CACHE_MAX_RFQS {
        if let Some(oldest_key) = cache.keys().next().cloned() {
            cache.remove(&oldest_key);
        }
    }
    let now = Instant::now();
    let entries = cache.entry(rfq_id.to_string()).or_default();
    let incoming_quote_ids = quotes
        .iter()
        .filter_map(combo_rfq_quote_value_id)
        .collect::<HashSet<_>>();
    entries.retain(|entry| {
        combo_rfq_quote_value_id(&entry.payload)
            .map(|quote_id| !incoming_quote_ids.contains(&quote_id))
            .unwrap_or(false)
    });
    entries.extend(
        quotes
            .into_iter()
            .filter(|quote| combo_rfq_stream_quote_is_fresh(rfq_id, quote, None))
            .map(|payload| ComboRfqParsedStreamQuote {
                payload,
                cached_at: now,
            }),
    );
    if entries.len() > COMBO_RFQ_STREAM_PARSED_CACHE_MAX_PER_RFQ {
        let excess = entries.len() - COMBO_RFQ_STREAM_PARSED_CACHE_MAX_PER_RFQ;
        entries.drain(0..excess);
    }
}

fn combo_rfq_parsed_stream_quote_response(rfq_id: &str, source: &str) -> Option<Value> {
    let rfq_id = rfq_id.trim();
    if rfq_id.is_empty() {
        return None;
    }
    let Ok(mut cache) = combo_rfq_parsed_stream_quote_cache().lock() else {
        return None;
    };
    let entries = cache.get_mut(rfq_id)?;
    entries.retain(|entry| {
        combo_rfq_stream_quote_is_fresh(rfq_id, &entry.payload, Some(entry.cached_at))
    });
    let quotes = entries
        .iter()
        .map(|entry| entry.payload.clone())
        .collect::<Vec<_>>();
    if quotes.is_empty() {
        cache.remove(rfq_id);
        None
    } else {
        Some(serde_json::json!({
            "quotes": quotes,
            "source": source,
        }))
    }
}

fn combo_rfq_stream_journal_scan_recent(
    path: &Path,
    rfq_id: &str,
    len: u64,
    modified: Option<SystemTime>,
) -> bool {
    combo_rfq_stream_journal_scan_cache()
        .lock()
        .ok()
        .and_then(|cache| {
            cache
                .get(&(path.to_path_buf(), rfq_id.to_string()))
                .cloned()
        })
        .map(|state| {
            state.len == len
                && state.modified == modified
                && state.scanned_at.elapsed()
                    < Duration::from_millis(COMBO_RFQ_STREAM_JOURNAL_UNCHANGED_SCAN_COOLDOWN_MS)
        })
        .unwrap_or(false)
}

fn record_combo_rfq_stream_journal_scan(
    path: &Path,
    rfq_id: &str,
    len: u64,
    modified: Option<SystemTime>,
) {
    if let Ok(mut cache) = combo_rfq_stream_journal_scan_cache().lock() {
        cache.insert(
            (path.to_path_buf(), rfq_id.to_string()),
            ComboRfqStreamJournalScanState {
                len,
                modified,
                scanned_at: Instant::now(),
            },
        );
    }
}

pub async fn accept_combo_rfq_quote(
    client: &Client,
    config: &Config,
    rfq_id: &str,
    quote_id: &str,
    request: &ComboRfqAcceptQuoteRequest,
) -> Result<Value> {
    ensure_combo_rfq_requester_ready(config)?;
    let rfq_id = combo_rfq_path_segment(rfq_id, "rfq_id")?;
    let quote_id = combo_rfq_path_segment(quote_id, "quote_id")?;
    let url = format!(
        "{}/v1/combos/rfqs/{}/quotes/{}/accept",
        config.combo_rfq_requester_api_url.trim_end_matches('/'),
        rfq_id,
        quote_id
    );
    send_combo_rfq_request_with_retries(
        config,
        "Combo/RFQ accept quote",
        ComboRfqRetryPolicy::WriteRateLimitOnly,
        Some(combo_rfq_live_request_deadline(config)),
        || authed_combo_rfq_request(client, config, reqwest::Method::PUT, &url).json(request),
    )
    .await
}

#[derive(Debug, Clone)]
pub struct ComboMarketEntry {
    pub condition_id: String,
    pub position_ids: Vec<String>,
    pub outcomes: Vec<String>,
    pub slug: String,
}

impl ComboMarketEntry {
    fn position_id_for_outcome(&self, outcome: OutcomeSide) -> Option<&str> {
        let expected = match outcome {
            OutcomeSide::Yes => "yes",
            OutcomeSide::No => "no",
        };
        self.outcomes
            .iter()
            .position(|label| label.trim().eq_ignore_ascii_case(expected))
            .and_then(|idx| self.position_ids.get(idx))
            .map(String::as_str)
    }

    fn outcome_index_for_position_id(&self, position_id: &str) -> Option<u8> {
        self.position_ids
            .iter()
            .position(|known| known.trim() == position_id.trim())
            .and_then(|idx| u8::try_from(idx).ok())
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ComboExposureReport {
    pub user: Option<String>,
    pub open_combo_count: usize,
    pub redeemable_combo_count: usize,
    pub total_entry_cost_usdc: f64,
    pub total_cost_usdc: f64,
    pub activity: ComboActivityReport,
    pub combos: Vec<ComboPositionView>,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct ComboActivityReport {
    pub user: Option<String>,
    pub activity_count: usize,
    pub total_amount_usdc: f64,
    pub total_payout_usdc: f64,
    pub redeem_events: usize,
    pub latest_timestamp: Option<u64>,
    pub latest_tx_dttm: Option<String>,
    pub activities: Vec<ComboActivityView>,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ComboActivityView {
    pub id: Option<String>,
    pub event_kind: Option<String>,
    pub module_kind: Option<String>,
    pub user_address: Option<String>,
    pub combo_condition_id: Option<String>,
    pub combo_position_id: Option<String>,
    pub amount_usdc: Option<f64>,
    pub payout_usdc: Option<f64>,
    pub timestamp: Option<u64>,
    pub tx_dttm: Option<String>,
    pub transaction_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ComboPositionView {
    pub combo_condition_id: String,
    pub combo_position_id: Option<String>,
    pub combo_outcome_index: Option<u8>,
    pub status: Option<String>,
    pub shares_balance: Option<String>,
    pub entry_cost_usdc: Option<f64>,
    pub total_cost_usdc: Option<f64>,
    pub realized_payout_usdc: Option<f64>,
    pub legs_total: Option<u32>,
    pub legs_pending: Option<u32>,
    pub legs: Vec<ComboLegView>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ComboLegView {
    pub leg_condition_id: Option<String>,
    pub leg_position_id: Option<String>,
    pub leg_outcome_label: Option<String>,
    pub leg_status: Option<String>,
    pub market_title: Option<String>,
    pub market_slug: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ComboMarketsPage {
    #[serde(default)]
    markets: Vec<ComboMarketRaw>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ComboMarketRaw {
    condition_id: String,
    #[serde(default)]
    position_ids: Vec<String>,
    #[serde(default)]
    outcomes: Vec<String>,
    #[serde(default)]
    slug: String,
}

#[derive(Debug, Deserialize)]
struct ComboPositionsPage {
    #[serde(default)]
    combos: Vec<ComboPositionRaw>,
}

#[derive(Debug, Deserialize)]
struct ComboActivityPage {
    #[serde(default)]
    activity: Vec<ComboActivityRaw>,
    pagination: Option<ComboPaginationRaw>,
}

#[derive(Debug, Deserialize)]
struct ComboPaginationRaw {
    has_more: Option<bool>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ComboActivityRaw {
    id: Option<String>,
    event_kind: Option<String>,
    module_kind: Option<String>,
    user_address: Option<String>,
    combo_condition_id: Option<String>,
    combo_position_id: Option<String>,
    amount_usdc: Option<Value>,
    payout_usdc: Option<Value>,
    timestamp: Option<Value>,
    tx_dttm: Option<String>,
    transaction_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ComboPositionRaw {
    combo_condition_id: String,
    combo_position_id: Option<String>,
    status: Option<String>,
    shares_balance: Option<Value>,
    entry_cost_usdc: Option<Value>,
    total_cost_usdc: Option<Value>,
    realized_payout_usdc: Option<Value>,
    legs_total: Option<Value>,
    legs_pending: Option<Value>,
    #[serde(default)]
    legs: Vec<ComboLegRaw>,
}

#[derive(Debug, Deserialize)]
struct ComboLegRaw {
    leg_condition_id: Option<String>,
    leg_position_id: Option<String>,
    leg_outcome_label: Option<String>,
    leg_status: Option<String>,
    market: Option<ComboLegMarketRaw>,
}

#[derive(Debug, Deserialize)]
struct ComboLegMarketRaw {
    title: Option<String>,
    slug: Option<String>,
}

impl From<ComboMarketRaw> for ComboMarketEntry {
    fn from(raw: ComboMarketRaw) -> Self {
        Self {
            condition_id: raw.condition_id,
            position_ids: raw.position_ids,
            outcomes: raw.outcomes,
            slug: raw.slug,
        }
    }
}

impl From<ComboPositionRaw> for ComboPositionView {
    fn from(raw: ComboPositionRaw) -> Self {
        Self {
            combo_condition_id: raw.combo_condition_id,
            combo_position_id: raw.combo_position_id,
            combo_outcome_index: None,
            status: raw.status,
            shares_balance: raw.shares_balance.as_ref().map(value_as_string),
            entry_cost_usdc: raw.entry_cost_usdc.as_ref().and_then(value_as_f64),
            total_cost_usdc: raw.total_cost_usdc.as_ref().and_then(value_as_f64),
            realized_payout_usdc: raw.realized_payout_usdc.as_ref().and_then(value_as_f64),
            legs_total: raw.legs_total.as_ref().and_then(value_as_u32),
            legs_pending: raw.legs_pending.as_ref().and_then(value_as_u32),
            legs: raw.legs.into_iter().map(ComboLegView::from).collect(),
        }
    }
}

impl From<ComboActivityRaw> for ComboActivityView {
    fn from(raw: ComboActivityRaw) -> Self {
        Self {
            id: raw.id,
            event_kind: raw.event_kind,
            module_kind: raw.module_kind,
            user_address: raw.user_address,
            combo_condition_id: raw.combo_condition_id,
            combo_position_id: raw.combo_position_id,
            amount_usdc: raw.amount_usdc.as_ref().and_then(value_as_f64),
            payout_usdc: raw.payout_usdc.as_ref().and_then(value_as_f64),
            timestamp: raw.timestamp.as_ref().and_then(value_as_u64),
            tx_dttm: raw.tx_dttm,
            transaction_hash: raw.transaction_hash,
        }
    }
}

impl From<ComboLegRaw> for ComboLegView {
    fn from(raw: ComboLegRaw) -> Self {
        let (market_title, market_slug) = raw
            .market
            .map(|market| (market.title, market.slug))
            .unwrap_or((None, None));
        Self {
            leg_condition_id: raw.leg_condition_id,
            leg_position_id: raw.leg_position_id,
            leg_outcome_label: raw.leg_outcome_label,
            leg_status: raw.leg_status,
            market_title,
            market_slug,
        }
    }
}

pub async fn fetch_combo_market_catalog(
    client: &Client,
    config: &Config,
) -> Result<ComboMarketCatalog> {
    if !config.combo_rfq_discovery_enabled {
        return Ok(ComboMarketCatalog::default());
    }

    let mut markets = Vec::new();
    let mut cursor: Option<String> = None;
    let page_limit = config.combo_rfq_max_markets.clamp(1, 100);
    let max_markets = config.combo_rfq_max_markets.max(1);

    loop {
        let url = format!(
            "{}/v1/rfq/combo-markets",
            config.combo_rfq_api_url.trim_end_matches('/')
        );
        let mut params = vec![("limit", page_limit.to_string())];
        if let Some(cursor_value) = &cursor {
            params.push(("cursor", cursor_value.clone()));
        }
        let response = client
            .get(&url)
            .query(&params)
            .timeout(Duration::from_secs(config.api_timeout_secs.max(1)))
            .send()
            .await
            .context("Combo/RFQ market catalog request failed")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "Combo/RFQ market catalog failed with status {} body={}",
                status,
                body.chars().take(256).collect::<String>()
            );
        }

        let page = response
            .json::<ComboMarketsPage>()
            .await
            .context("Combo/RFQ market catalog response parse failed")?;
        for market in page.markets {
            markets.push(ComboMarketEntry::from(market));
            if markets.len() >= max_markets {
                return Ok(ComboMarketCatalog::from_markets(markets));
            }
        }
        match page.next_cursor {
            Some(next) if !next.trim().is_empty() => cursor = Some(next),
            _ => break,
        }
    }

    Ok(ComboMarketCatalog::from_markets(markets))
}

pub async fn fetch_live_combo_exposure_report(
    client: &Client,
    config: &Config,
) -> ComboExposureReport {
    let account = match configured_live_account_address(config) {
        Ok(address) => address,
        Err(err) => {
            return ComboExposureReport {
                status: "skipped_missing_live_account".into(),
                error: Some(err.to_string()),
                ..ComboExposureReport::default()
            }
        }
    };

    let activity = fetch_combo_activity_report(client, config, account).await;
    let open_combos = match fetch_open_combo_positions(client, config, account).await {
        Ok(combos) => combos,
        Err(err) => {
            return ComboExposureReport {
                user: Some(account.to_string()),
                activity,
                status: "error".into(),
                error: Some(err.to_string()),
                ..ComboExposureReport::default()
            }
        }
    };
    let redeemable_combos = match fetch_redeemable_combo_positions(client, config, account).await {
        Ok(combos) => combos,
        Err(err) => {
            return ComboExposureReport {
                user: Some(account.to_string()),
                activity,
                status: "error".into(),
                error: Some(err.to_string()),
                ..ComboExposureReport::default()
            }
        }
    };

    let mut combos =
        dedupe_combo_positions(open_combos.into_iter().chain(redeemable_combos).collect());
    let mut catalog_error = None;
    if combos.iter().any(combo_position_status_is_redeemable) {
        match fetch_combo_market_catalog(client, config).await {
            Ok(catalog) => enrich_combo_position_outcome_indexes(&mut combos, &catalog),
            Err(err) => {
                catalog_error = Some(format!("combo_catalog_outcome_index_unavailable:{err}"))
            }
        }
    }

    let mut report = combo_exposure_report(account, combos, activity);
    if catalog_error.is_some() {
        report.error = catalog_error;
    }
    report
}

pub async fn fetch_combo_activity_report(
    client: &Client,
    config: &Config,
    user: Address,
) -> ComboActivityReport {
    match fetch_combo_activity(client, config, user).await {
        Ok(activities) => combo_activity_report(user, activities),
        Err(err) => ComboActivityReport {
            user: Some(user.to_string()),
            status: "error".into(),
            error: Some(err.to_string()),
            ..ComboActivityReport::default()
        },
    }
}

pub async fn fetch_combo_activity(
    client: &Client,
    config: &Config,
    user: Address,
) -> Result<Vec<ComboActivityView>> {
    fetch_combo_activity_with_base_url(client, config, user, "https://data-api.polymarket.com")
        .await
}

async fn fetch_combo_activity_with_base_url(
    client: &Client,
    config: &Config,
    user: Address,
    base_url: &str,
) -> Result<Vec<ComboActivityView>> {
    let url = format!("{}/v1/activity/combos", base_url.trim_end_matches('/'));
    let mut cursor: Option<String> = None;
    let mut offset = 0usize;
    let mut activities = Vec::new();
    loop {
        let mut params = vec![
            ("user", user.to_string()),
            ("limit", COMBO_RFQ_ACTIVITY_PAGE_LIMIT.to_string()),
            ("offset", offset.to_string()),
        ];
        if let Some(cursor_value) = &cursor {
            params.push(("cursor", cursor_value.clone()));
        }
        let response = client
            .get(&url)
            .query(&params)
            .timeout(Duration::from_secs(config.api_timeout_secs.max(1)))
            .send()
            .await
            .context("combo activity request failed")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "combo activity failed with status {} body={}",
                status,
                body.chars().take(256).collect::<String>()
            );
        }
        let page = response
            .json::<ComboActivityPage>()
            .await
            .context("combo activity response parse failed")?;
        activities.extend(page.activity.into_iter().map(ComboActivityView::from));
        if activities.len() >= COMBO_RFQ_ACTIVITY_MAX_RECORDS {
            activities.truncate(COMBO_RFQ_ACTIVITY_MAX_RECORDS);
            break;
        }
        match page.pagination {
            Some(pagination) if pagination.has_more.unwrap_or(false) => {
                offset = offset.saturating_add(COMBO_RFQ_ACTIVITY_PAGE_LIMIT);
                match pagination.next_cursor {
                    Some(next) if !next.trim().is_empty() => cursor = Some(next),
                    _ => break,
                }
            }
            _ => break,
        }
    }
    Ok(activities)
}

pub async fn fetch_open_combo_positions(
    client: &Client,
    config: &Config,
    user: Address,
) -> Result<Vec<ComboPositionView>> {
    fetch_combo_positions(client, config, user, "OPEN").await
}

async fn fetch_redeemable_combo_positions(
    client: &Client,
    config: &Config,
    user: Address,
) -> Result<Vec<ComboPositionView>> {
    fetch_combo_positions(client, config, user, "RESOLVED_WIN").await
}

async fn fetch_combo_positions(
    client: &Client,
    config: &Config,
    user: Address,
    status_filter: &str,
) -> Result<Vec<ComboPositionView>> {
    fetch_combo_positions_with_base_url(
        client,
        config,
        user,
        status_filter,
        "https://data-api.polymarket.com",
    )
    .await
}

async fn fetch_combo_positions_with_base_url(
    client: &Client,
    config: &Config,
    user: Address,
    status_filter: &str,
    base_url: &str,
) -> Result<Vec<ComboPositionView>> {
    let url = format!("{}/v1/positions/combos", base_url.trim_end_matches('/'));
    let response = client
        .get(&url)
        .query(&[
            ("user", user.to_string()),
            ("status", status_filter.to_string()),
            ("limit", "100".to_string()),
        ])
        .timeout(Duration::from_secs(config.api_timeout_secs.max(1)))
        .send()
        .await
        .context("combo position request failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "combo positions failed with status {} body={}",
            status,
            body.chars().take(256).collect::<String>()
        );
    }
    let page = response
        .json::<ComboPositionsPage>()
        .await
        .context("combo position response parse failed")?;
    Ok(page
        .combos
        .into_iter()
        .map(|raw| {
            let mut view = ComboPositionView::from(raw);
            if view.status.as_deref().unwrap_or_default().trim().is_empty() {
                view.status = Some(status_filter.to_string());
            }
            view
        })
        .collect())
}

fn combo_exposure_report(
    user: Address,
    combos: Vec<ComboPositionView>,
    activity: ComboActivityReport,
) -> ComboExposureReport {
    let open_combo_count = combos
        .iter()
        .filter(|combo| combo_position_status_is_open(combo))
        .count();
    let redeemable_combo_count = combos
        .iter()
        .filter(|combo| combo_position_status_is_redeemable(combo))
        .count();
    let total_entry_cost_usdc = combos
        .iter()
        .filter_map(|combo| combo.entry_cost_usdc)
        .sum();
    let total_cost_usdc = combos
        .iter()
        .filter_map(|combo| combo.total_cost_usdc)
        .sum();
    ComboExposureReport {
        user: Some(user.to_string()),
        open_combo_count,
        redeemable_combo_count,
        total_entry_cost_usdc,
        total_cost_usdc,
        activity,
        status: if open_combo_count > 0 {
            "open_combo_exposure".into()
        } else if redeemable_combo_count > 0 {
            "redeemable_combo_exposure".into()
        } else {
            "clean".into()
        },
        error: None,
        combos,
    }
}

fn combo_position_status_is_open(combo: &ComboPositionView) -> bool {
    combo
        .status
        .as_deref()
        .map(normalize_combo_position_status)
        .as_deref()
        == Some("open")
}

fn combo_position_status_is_redeemable(combo: &ComboPositionView) -> bool {
    matches!(
        combo
            .status
            .as_deref()
            .map(normalize_combo_position_status)
            .as_deref(),
        Some("resolved_win") | Some("resolved_winning") | Some("redeemable")
    )
}

fn normalize_combo_position_status(status: &str) -> String {
    status.trim().to_ascii_lowercase().replace([' ', '-'], "_")
}

fn enrich_combo_position_outcome_indexes(
    combos: &mut [ComboPositionView],
    catalog: &ComboMarketCatalog,
) {
    for combo in combos {
        if combo.combo_outcome_index.is_some() {
            continue;
        }
        let Some(position_id) = combo.combo_position_id.as_deref() else {
            continue;
        };
        combo.combo_outcome_index =
            catalog.outcome_index_for_position_id(&combo.combo_condition_id, position_id);
    }
}

fn dedupe_combo_positions(combos: Vec<ComboPositionView>) -> Vec<ComboPositionView> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for combo in combos {
        let key = format!(
            "{}:{}",
            combo.combo_condition_id.trim(),
            combo
                .combo_position_id
                .as_deref()
                .unwrap_or_default()
                .trim()
        );
        if seen.insert(key) {
            unique.push(combo);
        }
    }
    unique
}

fn combo_activity_report(user: Address, activities: Vec<ComboActivityView>) -> ComboActivityReport {
    let total_amount_usdc = activities
        .iter()
        .filter_map(|activity| activity.amount_usdc)
        .sum();
    let total_payout_usdc = activities
        .iter()
        .filter_map(|activity| activity.payout_usdc)
        .sum();
    let redeem_events = activities
        .iter()
        .filter(|activity| {
            activity
                .event_kind
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("redeem")
                || activity
                    .module_kind
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains("redeem")
        })
        .count();
    let latest_timestamp = activities
        .iter()
        .filter_map(|activity| activity.timestamp)
        .max();
    let latest_tx_dttm = latest_timestamp.and_then(|timestamp| {
        activities
            .iter()
            .find(|activity| activity.timestamp == Some(timestamp))
            .and_then(|activity| activity.tx_dttm.clone())
    });
    ComboActivityReport {
        user: Some(user.to_string()),
        activity_count: activities.len(),
        total_amount_usdc,
        total_payout_usdc,
        redeem_events,
        latest_timestamp,
        latest_tx_dttm,
        status: if activities.is_empty() {
            "no_combo_activity".into()
        } else {
            "combo_activity_seen".into()
        },
        error: None,
        activities,
    }
}

fn ensure_combo_rfq_requester_ready(config: &Config) -> Result<()> {
    let report = combo_rfq_requester_config_report(config);
    if report.blockers.is_empty() {
        Ok(())
    } else {
        bail!(
            "Combo/RFQ requester is not ready: {}",
            report.blockers.join(",")
        )
    }
}

fn authed_combo_rfq_request(
    client: &Client,
    config: &Config,
    method: reqwest::Method,
    url: &str,
) -> reqwest::RequestBuilder {
    client
        .request(method, url)
        .timeout(Duration::from_secs(config.api_timeout_secs.max(1)))
        .bearer_auth(config.combo_rfq_bearer_token.trim())
        .header(
            "x-participant-id",
            config.combo_rfq_participant_id.trim().to_string(),
        )
}

async fn parse_combo_rfq_response(response: reqwest::Response, context: &str) -> Result<Value> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "{context} failed with status {} body={}",
            status,
            body.chars().take(256).collect::<String>()
        );
    }
    response
        .json::<Value>()
        .await
        .with_context(|| format!("{context} response parse failed"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComboRfqRetryPolicy {
    ReadOnlyPreserveWriteCapacity,
    WriteRateLimitOnly,
}

#[derive(Debug, Clone, Copy)]
struct ComboRfqLiveRequestDeadline {
    started_at: Instant,
    max_ms: u64,
}

fn combo_rfq_live_request_deadline(config: &Config) -> ComboRfqLiveRequestDeadline {
    ComboRfqLiveRequestDeadline {
        started_at: Instant::now(),
        max_ms: config.live_max_refresh_to_submit_ms.max(1),
    }
}

fn combo_rfq_live_deadline_remaining(
    deadline: ComboRfqLiveRequestDeadline,
    context: &str,
) -> Result<Duration> {
    let elapsed_ms = deadline.started_at.elapsed().as_millis();
    let max_ms = u128::from(deadline.max_ms);
    if elapsed_ms >= max_ms {
        bail!(
            "{context} live freshness deadline exhausted: elapsed={}ms >= LIVE_MAX_REFRESH_TO_SUBMIT_MS={}ms",
            elapsed_ms,
            deadline.max_ms
        );
    }
    Ok(Duration::from_millis((max_ms - elapsed_ms) as u64))
}

fn combo_rfq_ensure_success_within_deadline(
    deadline: Option<ComboRfqLiveRequestDeadline>,
    context: &str,
) -> Result<()> {
    if let Some(deadline) = deadline {
        let _ = combo_rfq_live_deadline_remaining(deadline, context)?;
    }
    Ok(())
}

fn combo_rfq_retry_wait_ms_with_deadline(
    config: &Config,
    headers: Option<&reqwest::header::HeaderMap>,
    attempt: u32,
    deadline: Option<ComboRfqLiveRequestDeadline>,
    context: &str,
) -> Result<u64> {
    let wait_ms = combo_rfq_retry_wait_ms(config, headers, attempt);
    let Some(deadline) = deadline else {
        return Ok(wait_ms);
    };
    let remaining_ms = combo_rfq_live_deadline_remaining(deadline, context)?.as_millis();
    if u128::from(wait_ms) >= remaining_ms {
        bail!(
            "{context} retry wait exceeds live freshness deadline: wait={}ms remaining={}ms LIVE_MAX_REFRESH_TO_SUBMIT_MS={}ms",
            wait_ms,
            remaining_ms,
            deadline.max_ms
        );
    }
    Ok(wait_ms)
}

fn combo_rfq_read_rate_limits() -> &'static Mutex<HashMap<String, Instant>> {
    COMBO_RFQ_READ_RATE_LIMITS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn combo_rfq_read_rate_limit_key(config: &Config) -> String {
    config
        .combo_rfq_requester_api_url
        .trim_end_matches('/')
        .to_string()
}

fn combo_rfq_read_rate_limit_remaining(config: &Config) -> Option<Duration> {
    let key = combo_rfq_read_rate_limit_key(config);
    let now = Instant::now();
    let mut limits = combo_rfq_read_rate_limits().lock().ok()?;
    let until = *limits.get(&key)?;
    if until <= now {
        limits.remove(&key);
        None
    } else {
        Some(until.saturating_duration_since(now))
    }
}

fn combo_rfq_record_read_rate_limit(config: &Config, wait_ms: u64) {
    let key = combo_rfq_read_rate_limit_key(config);
    let until = Instant::now() + Duration::from_millis(wait_ms.max(1));
    if let Ok(mut limits) = combo_rfq_read_rate_limits().lock() {
        limits.insert(key, until);
    }
}

async fn combo_rfq_preserve_write_capacity_rate_limit_error(
    config: &Config,
    context: &str,
    response: reqwest::Response,
    wait_ms: u64,
) -> Result<Value> {
    combo_rfq_record_read_rate_limit(config, wait_ms);
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    bail!(
        "{context} rate limited; preserving Combo/RFQ write capacity for accept/finality; retry_after_ms={} status={} body={}",
        wait_ms,
        status,
        body.chars().take(256).collect::<String>()
    )
}

async fn send_combo_rfq_request_with_retries<F>(
    config: &Config,
    context: &str,
    policy: ComboRfqRetryPolicy,
    deadline: Option<ComboRfqLiveRequestDeadline>,
    mut build_request: F,
) -> Result<Value>
where
    F: FnMut() -> reqwest::RequestBuilder,
{
    if matches!(policy, ComboRfqRetryPolicy::ReadOnlyPreserveWriteCapacity) {
        if let Some(remaining) = combo_rfq_read_rate_limit_remaining(config) {
            bail!(
                "{context} skipped while Combo/RFQ read endpoint is rate-limited; preserving write capacity for accept/finality; retry_after_ms={}",
                remaining.as_millis()
            );
        }
    }
    let max_attempts = config.max_retries.max(1);
    for attempt in 1..=max_attempts {
        let send = async {
            if let Some(deadline) = deadline {
                let remaining = combo_rfq_live_deadline_remaining(deadline, context)?;
                tokio::time::timeout(remaining, build_request().send())
                    .await
                    .with_context(|| {
                        format!(
                            "{context} timed out after {}ms live freshness budget",
                            remaining.as_millis()
                        )
                    })?
                    .with_context(|| format!("{context} request failed"))
            } else {
                build_request()
                    .send()
                    .await
                    .with_context(|| format!("{context} request failed"))
            }
        }
        .await;
        match send {
            Ok(response) if response.status().is_success() => {
                let parsed = response
                    .json::<Value>()
                    .await
                    .with_context(|| format!("{context} response parse failed"))?;
                combo_rfq_ensure_success_within_deadline(deadline, context)?;
                return Ok(parsed);
            }
            Ok(response) => {
                let status = response.status();
                if matches!(policy, ComboRfqRetryPolicy::ReadOnlyPreserveWriteCapacity)
                    && status.as_u16() == 429
                {
                    let wait_ms =
                        combo_rfq_retry_wait_ms(config, Some(response.headers()), attempt);
                    return combo_rfq_preserve_write_capacity_rate_limit_error(
                        config, context, response, wait_ms,
                    )
                    .await;
                }
                if combo_rfq_should_retry_status(policy, status) && attempt < max_attempts {
                    let wait_ms = combo_rfq_retry_wait_ms_with_deadline(
                        config,
                        Some(response.headers()),
                        attempt,
                        deadline,
                        context,
                    )?;
                    warn!(
                        "{context} retry after HTTP status {} attempt {attempt}/{max_attempts}; waiting {wait_ms}ms",
                        status
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                return parse_combo_rfq_response(response, context).await;
            }
            Err(err) => {
                let retryable_transport = err
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(combo_rfq_should_retry_transport_error);
                if policy == ComboRfqRetryPolicy::ReadOnlyPreserveWriteCapacity
                    && retryable_transport
                    && attempt < max_attempts
                {
                    let wait_ms = combo_rfq_retry_wait_ms_with_deadline(
                        config, None, attempt, deadline, context,
                    )?;
                    warn!(
                        "{context} transport retry attempt {attempt}/{max_attempts}; waiting {wait_ms}ms: {err}",
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
    anyhow::bail!("{context} retries exhausted")
}

fn combo_rfq_should_retry_status(policy: ComboRfqRetryPolicy, status: reqwest::StatusCode) -> bool {
    match policy {
        ComboRfqRetryPolicy::ReadOnlyPreserveWriteCapacity => {
            matches!(status.as_u16(), 408 | 425) || status.is_server_error()
        }
        ComboRfqRetryPolicy::WriteRateLimitOnly => status.as_u16() == 429,
    }
}

fn combo_rfq_should_retry_transport_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
}

fn combo_rfq_retry_wait_ms(
    config: &Config,
    headers: Option<&reqwest::header::HeaderMap>,
    attempt: u32,
) -> u64 {
    if let Some(seconds) = headers
        .and_then(|headers| headers.get(reqwest::header::RETRY_AFTER))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
    {
        return seconds
            .saturating_mul(1_000)
            .clamp(1, COMBO_RFQ_RETRY_WAIT_MAX_MS);
    }
    let exp = 2u64.saturating_pow(attempt.saturating_sub(1));
    config
        .retry_backoff_base_ms
        .max(1)
        .saturating_mul(exp)
        .min(COMBO_RFQ_RETRY_WAIT_MAX_MS)
}

fn combo_rfq_accept_response_blockers(
    rfq_id: &str,
    quote: &ComboRfqQuoteCandidate,
    accept_request: &ComboRfqAcceptQuoteRequest,
    response: &Value,
) -> Vec<String> {
    let mut blockers = Vec::new();
    match response_text_value(
        response,
        &["status", "state", "quoteStatus", "executionStatus"],
    ) {
        Some(status) => {
            let status = normalize_rfq_status(&status);
            if !combo_rfq_accept_response_status_is_accepted(&status) {
                blockers.push(format!("accept_response_status_not_accepted:{status}"));
            }
        }
        None => blockers.push("accept_response_missing_status".to_string()),
    }

    match response_text_value(response, &["rfqId", "rfq_id"]) {
        Some(response_rfq_id) if response_rfq_id == rfq_id => {}
        Some(response_rfq_id) => blockers.push(format!(
            "accept_response_rfq_id_mismatch:response={response_rfq_id}:expected={rfq_id}"
        )),
        None => blockers.push("accept_response_missing_rfq_id".to_string()),
    }
    match response_text_value(response, &["quoteId", "quote_id"]) {
        Some(response_quote_id) if response_quote_id == quote.quote_id => {}
        Some(response_quote_id) => blockers.push(format!(
            "accept_response_quote_id_mismatch:response={response_quote_id}:expected={}",
            quote.quote_id
        )),
        None => blockers.push("accept_response_missing_quote_id".to_string()),
    }

    match (
        response_number_value(
            response,
            &["price", "quotePrice", "limitPrice", "acceptedPrice"],
        ),
        parse_positive_f64(&accept_request.price),
    ) {
        (Some(response_price), Some(expected_price)) => {
            let tolerance = combo_rfq_price_tolerance(expected_price);
            if (response_price - expected_price).abs() > tolerance {
                blockers.push(format!(
                    "accept_response_price_mismatch:response={response_price:.6}:expected={expected_price:.6}:tol={tolerance:.6}"
                ));
            }
        }
        (None, _) => blockers.push("accept_response_missing_price".to_string()),
        _ => blockers.push("accept_request_invalid_price".to_string()),
    }
    match (
        response_number_value(
            response,
            &[
                "qtyDecimal",
                "quantity",
                "qty",
                "size",
                "filledQty",
                "acceptedQty",
            ],
        ),
        parse_positive_f64(&accept_request.qty_decimal),
    ) {
        (Some(response_qty), Some(expected_qty)) => {
            let tolerance = combo_rfq_qty_tolerance(expected_qty);
            if (response_qty - expected_qty).abs() > tolerance {
                blockers.push(format!(
                    "accept_response_qty_mismatch:response={response_qty:.6}:expected={expected_qty:.6}:tol={tolerance:.6}"
                ));
            }
        }
        (None, _) => blockers.push("accept_response_missing_qty".to_string()),
        _ => blockers.push("accept_request_invalid_qty".to_string()),
    }
    blockers
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComboRfqAcceptOutcome {
    Accepted,
    RejectedProven,
    Unknown,
}

fn combo_rfq_accept_response_outcome(
    rfq_id: &str,
    quote: &ComboRfqQuoteCandidate,
    accept_request: &ComboRfqAcceptQuoteRequest,
    response: &Value,
) -> (ComboRfqAcceptOutcome, Vec<String>) {
    let blockers = combo_rfq_accept_response_blockers(rfq_id, quote, accept_request, response);
    if blockers.is_empty() {
        return (ComboRfqAcceptOutcome::Accepted, blockers);
    }
    let status = response_text_value(
        response,
        &["status", "state", "quoteStatus", "executionStatus"],
    )
    .map(|status| normalize_rfq_status(&status));
    if status
        .as_deref()
        .is_some_and(combo_rfq_accept_response_status_is_proven_rejection)
        && combo_rfq_accept_response_matches_identity(rfq_id, quote, response)
    {
        return (
            ComboRfqAcceptOutcome::RejectedProven,
            vec![format!(
                "rfq_accept_rejected_proven:{}",
                status.unwrap_or_default()
            )],
        );
    }
    (ComboRfqAcceptOutcome::Unknown, blockers)
}

fn combo_rfq_accept_response_matches_identity(
    rfq_id: &str,
    quote: &ComboRfqQuoteCandidate,
    response: &Value,
) -> bool {
    response_text_value(response, &["rfqId", "rfq_id"]).as_deref() == Some(rfq_id)
        && response_text_value(response, &["quoteId", "quote_id"]).as_deref()
            == Some(quote.quote_id.as_str())
}

fn combo_rfq_accept_response_status_is_accepted(status: &str) -> bool {
    matches!(
        status,
        "ACCEPTED" | "QUOTE_ACCEPTED" | "ACCEPTED_PENDING_FINALITY" | "QUOTE_PENDING_END_TRADE"
    )
}

fn combo_rfq_accept_response_status_is_proven_rejection(status: &str) -> bool {
    matches!(
        status,
        "REJECTED"
            | "QUOTE_REJECTED"
            | "CANCELLED"
            | "CANCELED"
            | "EXPIRED"
            | "QUOTE_EXPIRED"
            | "DONE_AWAY"
            | "QUOTE_DONE_AWAY"
            | "PASSED"
    )
}

fn combo_rfq_accept_outcome_from_execution_status(
    status: &str,
    blockers: &[String],
) -> Option<ComboRfqAcceptOutcome> {
    match status {
        "accepted_pending_finality" => Some(ComboRfqAcceptOutcome::Accepted),
        "accept_rejected_proven" => Some(ComboRfqAcceptOutcome::RejectedProven),
        "accept_state_unknown" | "accept_response_not_accepted" => {
            Some(ComboRfqAcceptOutcome::Unknown)
        }
        _ if blockers.iter().any(|blocker| {
            blocker == "exposure_must_remain_reserved_until_finality_or_manual_review"
        }) =>
        {
            Some(ComboRfqAcceptOutcome::Unknown)
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn combo_rfq_execution_report(
    status: &str,
    request: Option<ComboRfqCreateRequest>,
    rfq_id: Option<String>,
    quote_response: Option<Value>,
    best_execution: ComboRfqBestExecutionReport,
    accept_request: Option<ComboRfqAcceptQuoteRequest>,
    accept_response: Option<Value>,
    blockers: Vec<String>,
    steps: Vec<ComboRfqExecutionStep>,
) -> ComboRfqExecutionReport {
    ComboRfqExecutionReport {
        status: status.to_string(),
        accept_outcome: combo_rfq_accept_outcome_from_execution_status(status, &blockers),
        request,
        rfq_id,
        quote_response,
        best_execution,
        pre_accept_markout: None,
        accept_request,
        accept_response,
        note: if blockers.is_empty() {
            format!("combo_rfq_execution={status}")
        } else {
            format!(
                "combo_rfq_execution={status} blockers={}",
                blockers.join("|")
            )
        },
        blockers,
        steps,
    }
}

#[allow(clippy::too_many_arguments)]
fn combo_rfq_execution_journal_record(
    opp: &ArbitrageOpportunity,
    stage: &str,
    status: &str,
    request: Option<&ComboRfqCreateRequest>,
    rfq_id: Option<&str>,
    selected_quote: Option<&ComboRfqQuoteCandidate>,
    accept_request: Option<&ComboRfqAcceptQuoteRequest>,
    response: Option<&Value>,
    error: Option<String>,
    blockers: Vec<String>,
) -> ComboRfqExecutionJournalRecord {
    ComboRfqExecutionJournalRecord {
        generated_at: Utc::now().to_rfc3339(),
        event_id: opp.event_id.clone(),
        stage: stage.to_string(),
        status: status.to_string(),
        client_request_id: request
            .map(|request| request.client_request_id.clone())
            .unwrap_or_default(),
        rfq_id: rfq_id.map(str::to_string),
        quote_id: selected_quote.map(|quote| quote.quote_id.clone()),
        maker_id: selected_quote.and_then(|quote| quote.maker_id.clone()),
        request: request.cloned(),
        selected_quote: selected_quote.cloned(),
        accept_request: accept_request.cloned(),
        response: response.cloned(),
        error,
        note: if blockers.is_empty() {
            format!("combo_rfq_journal stage={stage} status={status}")
        } else {
            format!(
                "combo_rfq_journal stage={stage} status={status} blockers={}",
                blockers.join("|")
            )
        },
        blockers,
    }
}

fn record_combo_rfq_execution_journal(
    config: &Config,
    record: ComboRfqExecutionJournalRecord,
) -> Result<PathBuf> {
    append_combo_rfq_execution_journal_record(config, &record)
}

fn combo_rfq_adverse_selection_journal_record(
    opp: &ArbitrageOpportunity,
    rfq_id: &str,
    quote: &ComboRfqQuoteCandidate,
    markout: &ComboRfqPreAcceptMarkoutReport,
) -> ComboRfqAdverseSelectionJournalRecord {
    ComboRfqAdverseSelectionJournalRecord {
        generated_at: Utc::now().to_rfc3339(),
        event_id: opp.event_id.clone(),
        rfq_id: rfq_id.to_string(),
        quote_id: quote.quote_id.clone(),
        maker_id: quote.maker_id.clone(),
        quote_age_ms: quote.age_ms,
        quote_to_accept_ms: markout.quote_to_accept_ms,
        quote_price: markout.quote_price,
        quote_qty_decimal: markout.quote_qty_decimal,
        quote_cost_usd: markout.quote_cost_usd,
        synthetic_price: markout.synthetic_price,
        synthetic_cost_usd: markout.synthetic_cost_usd,
        quote_edge_usd: markout.quote_edge_usd,
        public_edge_usd: markout.public_edge_usd,
        markout_bps: markout.markout_bps,
        toxicity_haircut_bps: markout.toxicity_haircut_bps,
        toxicity_haircut_usd: markout.toxicity_haircut_usd,
        toxicity_trade_prints: markout.toxicity_trade_prints,
        toxicity_recent_book_updates: markout.toxicity_recent_book_updates,
        ws_microprice_mean: markout.ws_microprice_mean,
        ws_queue_imbalance_mean: markout.ws_queue_imbalance_mean,
        ws_microstructure_tokens: markout.ws_microstructure_tokens,
        token_ids: markout.token_ids.clone(),
        book_hashes: markout.book_hashes.clone(),
        status: markout.status.clone(),
        blockers: markout.blockers.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
fn combo_rfq_markout_race_journal_record(
    config: &Config,
    opp: &ArbitrageOpportunity,
    rfq_id: &str,
    quote: &ComboRfqQuoteCandidate,
    markout: &ComboRfqPreAcceptMarkoutReport,
    horizon_ms: u64,
    sampled_snapshots: Option<&HashMap<String, crate::clob_client::DepthSnapshot>>,
    error: Option<String>,
) -> ComboRfqMarkoutRaceJournalRecord {
    let race_id = format!("{}:{}:{}", opp.event_id, rfq_id, quote.quote_id);
    let mut blockers = Vec::new();
    let mut sampled_synthetic_price = None;
    let mut sampled_public_edge_usd = None;
    let mut sampled_markout_bps = None;
    let mut sampled_book_hashes = Vec::new();

    if let Some(snapshots) = sampled_snapshots {
        let mut synthetic_price = 0.0;
        for (leg, token_id) in opp.execution_plan.iter().zip(markout.token_ids.iter()) {
            let target_shares = markout.quote_qty_decimal * leg.unit_shares;
            match snapshots
                .get(token_id)
                .and_then(|snapshot| snapshot.average_ask_for_shares(target_shares))
            {
                Some(avg_price) => synthetic_price += avg_price * leg.unit_shares,
                None => blockers.push(format!(
                    "race_markout_insufficient_depth:{token_id}:shares={target_shares:.6}"
                )),
            }
            if let Some(book_hash) = snapshots
                .get(token_id)
                .and_then(|snapshot| snapshot.book_hash.clone())
            {
                sampled_book_hashes.push(book_hash);
            }
        }
        if blockers.is_empty() && synthetic_price > f64::EPSILON {
            sampled_synthetic_price = Some(synthetic_price);
            sampled_public_edge_usd = combo_rfq_edge_usd_for_price(
                config,
                opp,
                synthetic_price,
                markout.quote_qty_decimal,
            );
            sampled_markout_bps =
                Some((quote.price - synthetic_price) / synthetic_price * 10_000.0);
        }
    }

    if let Some(err) = error.as_ref() {
        blockers.push(format!("race_markout_sample_unavailable:{err}"));
    }

    ComboRfqMarkoutRaceJournalRecord {
        generated_at: Utc::now().to_rfc3339(),
        race_id,
        event_id: opp.event_id.clone(),
        rfq_id: rfq_id.to_string(),
        quote_id: quote.quote_id.clone(),
        maker_id: quote.maker_id.clone(),
        horizon_ms,
        status: if blockers.is_empty() {
            "sampled".to_string()
        } else {
            "blocked".to_string()
        },
        quote_price: quote.price,
        quote_qty_decimal: markout.quote_qty_decimal,
        pre_accept_synthetic_price: markout.synthetic_price,
        sampled_synthetic_price,
        sampled_public_edge_usd,
        sampled_markout_bps,
        token_ids: markout.token_ids.clone(),
        pre_accept_book_hashes: markout.book_hashes.clone(),
        sampled_book_hashes,
        blockers,
        error,
    }
}

#[cfg(not(test))]
async fn sample_combo_rfq_markout_race(
    client: &Client,
    config: &Config,
    opp: &ArbitrageOpportunity,
    rfq_id: &str,
    quote: &ComboRfqQuoteCandidate,
    markout: &ComboRfqPreAcceptMarkoutReport,
    horizon_ms: u64,
) -> ComboRfqMarkoutRaceJournalRecord {
    match crate::clob_client::get_depth_snapshots(client, config, &markout.token_ids).await {
        Ok(snapshots) => combo_rfq_markout_race_journal_record(
            config,
            opp,
            rfq_id,
            quote,
            markout,
            horizon_ms,
            Some(&snapshots),
            None,
        ),
        Err(err) => combo_rfq_markout_race_journal_record(
            config,
            opp,
            rfq_id,
            quote,
            markout,
            horizon_ms,
            None,
            Some(err.to_string()),
        ),
    }
}

#[cfg(not(test))]
fn spawn_combo_rfq_markout_race_sampler(
    client: &Client,
    config: &Config,
    opp: &ArbitrageOpportunity,
    rfq_id: &str,
    quote: &ComboRfqQuoteCandidate,
    markout: &ComboRfqPreAcceptMarkoutReport,
) {
    if markout.token_ids.is_empty() || markout.quote_qty_decimal <= f64::EPSILON {
        return;
    }
    let client = client.clone();
    let config = config.clone();
    let opp = opp.clone();
    let rfq_id = rfq_id.to_string();
    let quote = quote.clone();
    let markout = markout.clone();
    tokio::spawn(async move {
        for horizon_ms in COMBO_RFQ_MARKOUT_RACE_HORIZONS_MS {
            tokio::time::sleep(Duration::from_millis(horizon_ms)).await;
            let record = sample_combo_rfq_markout_race(
                &client, &config, &opp, &rfq_id, &quote, &markout, horizon_ms,
            )
            .await;
            if let Err(err) = append_combo_rfq_markout_race_journal_record(&config, &record) {
                warn!("Failed to write Combo/RFQ markout race journal: {err:#}");
            }
        }
    });
}

#[cfg(test)]
fn spawn_combo_rfq_markout_race_sampler(
    _client: &Client,
    _config: &Config,
    _opp: &ArbitrageOpportunity,
    _rfq_id: &str,
    _quote: &ComboRfqQuoteCandidate,
    _markout: &ComboRfqPreAcceptMarkoutReport,
) {
}

fn push_rfq_step(
    steps: &mut Vec<ComboRfqExecutionStep>,
    started: &Instant,
    stage: &str,
    status: &str,
    detail: impl Into<String>,
) {
    steps.push(ComboRfqExecutionStep {
        stage: stage.to_string(),
        status: status.to_string(),
        detail: detail.into(),
        elapsed_ms: started.elapsed().as_millis(),
    });
}

fn combo_rfq_id_from_response(response: &Value) -> Option<String> {
    text_value(response, &["rfqId", "rfq_id", "id"]).or_else(|| {
        response
            .get("rfq")
            .and_then(|rfq| text_value(rfq, &["rfqId", "rfq_id", "id"]))
    })
}

fn combo_rfq_quote_request_contract_blockers(
    config: &Config,
    request: &ComboRfqCreateRequest,
    rfq_id: &str,
    quote: &ComboRfqQuoteCandidate,
) -> Vec<String> {
    let mut blockers = Vec::new();
    match quote.rfq_id.as_deref().map(str::trim) {
        Some(quote_rfq_id) if quote_rfq_id == rfq_id => {}
        Some(quote_rfq_id) if !quote_rfq_id.is_empty() => blockers.push(format!(
            "quote_rfq_id_mismatch:quote={quote_rfq_id}:expected={rfq_id}"
        )),
        _ => blockers.push("quote_missing_rfq_id".to_string()),
    }

    let expected_side = request.side.trim();
    match quote.side.as_deref().map(str::trim) {
        Some(side) if side == expected_side => {}
        Some(side) if !side.is_empty() => blockers.push(format!(
            "quote_side_mismatch:quote={side}:expected={expected_side}"
        )),
        _ => blockers.push("quote_missing_side_for_contract".to_string()),
    }

    let Some(qty_decimal) = quote
        .qty_decimal
        .filter(|qty| qty.is_finite() && *qty > 0.0)
    else {
        blockers.push("quote_missing_positive_qty_for_contract".to_string());
        return blockers;
    };
    if let Some(expected_qty) = request
        .qty_decimal
        .as_deref()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
    {
        let tolerance = combo_rfq_qty_tolerance(expected_qty);
        if (qty_decimal - expected_qty).abs() > tolerance {
            blockers.push(format!(
                "quote_qty_mismatch:quote={qty_decimal:.6}:expected={expected_qty:.6}:tol={tolerance:.6}"
            ));
        }
    }
    if let Some(expected_cash) = request
        .cash_order_qty
        .as_deref()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
    {
        let quote_cash = quote.price * qty_decimal;
        let tolerance = combo_rfq_cash_tolerance(config, expected_cash);
        if (quote_cash - expected_cash).abs() > tolerance {
            blockers.push(format!(
                "quote_notional_mismatch:quote={quote_cash:.6}:expected={expected_cash:.6}:tol={tolerance:.6}"
            ));
        }
    }
    blockers
}

fn combo_rfq_qty_tolerance(expected_qty: f64) -> f64 {
    (expected_qty.abs() * 0.0001).max(0.000001)
}

fn combo_rfq_price_tolerance(expected_price: f64) -> f64 {
    (expected_price.abs() * 0.0001).max(0.000001)
}

fn combo_rfq_cash_tolerance(config: &Config, expected_cash: f64) -> f64 {
    let half_cent = 0.005;
    let configured = (config.live_trade_position_size_usd.abs() * 0.0001).max(0.0);
    (expected_cash.abs() * 0.0001)
        .max(half_cent)
        .max(configured)
}

fn combo_rfq_accept_request_from_quote(
    quote: &ComboRfqQuoteCandidate,
) -> Result<ComboRfqAcceptQuoteRequest> {
    let symbol = quote
        .symbol
        .as_deref()
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .ok_or_else(|| anyhow::anyhow!("selected RFQ quote missing symbol"))?;
    let side = quote
        .side
        .as_deref()
        .map(str::trim)
        .filter(|side| !side.is_empty())
        .ok_or_else(|| anyhow::anyhow!("selected RFQ quote missing side"))?;
    let qty_decimal = quote
        .qty_decimal
        .filter(|qty| qty.is_finite() && *qty > 0.0)
        .ok_or_else(|| anyhow::anyhow!("selected RFQ quote missing positive qtyDecimal"))?;
    Ok(ComboRfqAcceptQuoteRequest {
        side: side.to_string(),
        price: format_decimal(quote.price),
        symbol: symbol.to_string(),
        qty_decimal: format_decimal(qty_decimal),
    })
}

fn parse_combo_rfq_quote_candidates(response: &Value) -> Vec<ComboRfqQuoteCandidate> {
    quote_array(response)
        .into_iter()
        .filter_map(parse_combo_rfq_quote_candidate)
        .collect()
}

fn quote_array(value: &Value) -> Vec<&Value> {
    if let Some(items) = value.as_array() {
        return items.iter().collect();
    }
    for key in ["quotes", "data", "items", "results"] {
        if let Some(items) = value.get(key).and_then(Value::as_array) {
            return items.iter().collect();
        }
    }
    Vec::new()
}

fn parse_combo_rfq_quote_candidate(value: &Value) -> Option<ComboRfqQuoteCandidate> {
    let quote_id = text_value(value, &["quoteId", "quote_id", "id"])?;
    let price = number_value(value, &["price", "quotePrice", "limitPrice"]).or_else(|| {
        number_value(value, &["priceE6", "price_e6"]).map(|price| price / 1_000_000.0)
    })?;
    let created_at = text_value(
        value,
        &[
            "createdAt",
            "created_at",
            "timestamp",
            "receivedAt",
            "generatedAt",
            "generated_at",
        ],
    );
    let age_ms = number_value(value, &["ageMs", "age_ms"])
        .map(|value| value as i64)
        .or_else(|| created_at.as_deref().and_then(parse_rfc3339_ms_age));
    Some(ComboRfqQuoteCandidate {
        quote_id,
        rfq_id: text_value(value, &["rfqId", "rfq_id"]),
        maker_id: text_value(value, &["makerId", "maker_id", "maker"]),
        symbol: text_value(value, &["symbol"]),
        side: text_value(value, &["side"]),
        status: text_value(value, &["status"]).map(|status| status.to_ascii_uppercase()),
        price,
        qty_decimal: number_value(value, &["qtyDecimal", "quantity", "qty", "size"])
            .or_else(|| number_value(value, &["sizeE6", "size_e6"]).map(|size| size / 1_000_000.0)),
        created_at,
        expires_at: text_value(value, &["expiresAt", "expires_at", "expirationTime"]),
        age_ms,
        expected_edge_usd: None,
    })
}

fn combo_rfq_quote_blockers(config: &Config, quote: &ComboRfqQuoteCandidate) -> Vec<String> {
    let mut blockers = Vec::new();
    if quote.price <= 0.0 || !quote.price.is_finite() {
        blockers.push("invalid_price".to_string());
    }
    if quote
        .symbol
        .as_deref()
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .is_none()
    {
        blockers.push("missing_symbol".to_string());
    }
    if quote
        .side
        .as_deref()
        .map(str::trim)
        .filter(|side| !side.is_empty())
        .is_none()
    {
        blockers.push("missing_side".to_string());
    }
    if quote
        .qty_decimal
        .filter(|qty| qty.is_finite() && *qty > 0.0)
        .is_none()
    {
        blockers.push("missing_positive_qty".to_string());
    }
    match quote.status.as_deref() {
        Some(status) if combo_rfq_quote_status_is_terminal(status) => {
            blockers.push(format!("terminal_status:{}", normalize_rfq_status(status)));
        }
        Some(status) if combo_rfq_quote_status_is_acceptable(status) => {}
        Some(status) => blockers.push(format!(
            "unsupported_quote_status:{}",
            normalize_rfq_status(status)
        )),
        None => blockers.push("missing_quote_status".to_string()),
    }
    if let Some(age_ms) = quote.age_ms {
        if age_ms < 0 {
            blockers.push(format!("future_quote_timestamp:{age_ms}ms"));
        } else if age_ms as u64 > config.combo_rfq_quote_max_age_ms {
            blockers.push(format!(
                "stale_quote:{}ms>{}ms",
                age_ms, config.combo_rfq_quote_max_age_ms
            ));
        }
    } else {
        blockers.push("missing_quote_age".to_string());
    }
    match quote
        .expires_at
        .as_deref()
        .and_then(parse_rfc3339_timestamp)
    {
        Some(expires_at) if expires_at <= Utc::now() => blockers.push("quote_expired".to_string()),
        Some(_) => {}
        None => blockers.push("missing_quote_expiration".to_string()),
    }
    blockers
}

fn combo_rfq_quote_status_is_terminal(status: &str) -> bool {
    matches!(
        combo_rfq_normalized_quote_status(status).as_str(),
        "EXPIRED"
            | "REJECTED"
            | "CANCELLED"
            | "CANCELED"
            | "FAILED"
            | "DONE_AWAY"
            | "PASSED"
            | "PARTIAL"
            | "ONE_LEG"
    )
}

fn combo_rfq_quote_status_is_acceptable(status: &str) -> bool {
    matches!(
        combo_rfq_normalized_quote_status(status).as_str(),
        "ACTIVE" | "PENDING" | "OPEN"
    )
}

fn combo_rfq_normalized_quote_status(status: &str) -> String {
    let normalized = normalize_rfq_status(status);
    normalized
        .strip_prefix("QUOTE_STATUS_")
        .unwrap_or(normalized.as_str())
        .to_string()
}

fn combo_rfq_maker_score_blockers(
    config: &Config,
    scorecard: &ComboRfqMakerScorecard,
    quote: &ComboRfqQuoteCandidate,
) -> Vec<String> {
    if !config.combo_rfq_accept_enabled {
        return Vec::new();
    }
    if scorecard.status == "error" {
        return vec!["maker_scorecard_unavailable".to_string()];
    }
    let Some(maker_id) = quote
        .maker_id
        .as_deref()
        .map(str::trim)
        .filter(|maker_id| !maker_id.is_empty())
    else {
        return vec!["missing_maker_id_for_accept".to_string()];
    };
    let Some(score) = scorecard
        .makers
        .iter()
        .find(|score| score.maker_id == maker_id)
    else {
        return vec![format!("maker_score_missing:{maker_id}")];
    };
    let counterparty_blockers =
        crate::settlement_monitor::settlement_counterparty_blockers(config, maker_id);
    if score.status == "insufficient_terminal_samples" {
        let mut blockers = vec![format!(
            "maker_score_insufficient_terminal_samples:{maker_id}:{}<{}",
            score.terminal_samples, scorecard.min_terminal_samples
        )];
        blockers.extend(counterparty_blockers);
        return blockers;
    }
    if score.status != "blocked" {
        return counterparty_blockers;
    }
    let mut blockers = score
        .blockers
        .iter()
        .map(|blocker| format!("maker_score_failed:{maker_id}:{blocker}"))
        .collect::<Vec<_>>();
    blockers.extend(counterparty_blockers);
    blockers
}

fn combo_rfq_quote_dispersion_blockers(
    _scorecard: &ComboRfqMakerScorecard,
    eligible: &[ComboRfqQuoteCandidate],
    selected: &ComboRfqQuoteCandidate,
) -> Vec<String> {
    if eligible.len() < COMBO_RFQ_DISPERSION_MIN_QUOTES {
        return Vec::new();
    }
    let Some(selected_edge) = selected.expected_edge_usd.filter(|edge| edge.is_finite()) else {
        return Vec::new();
    };
    let notional_usd = selected.price
        * selected
            .qty_decimal
            .filter(|qty| qty.is_finite() && *qty > 0.0)
            .unwrap_or(0.0);
    if notional_usd <= f64::EPSILON {
        return Vec::new();
    }

    let mut edges = eligible
        .iter()
        .filter_map(|quote| quote.expected_edge_usd.filter(|edge| edge.is_finite()))
        .collect::<Vec<_>>();
    if edges.len() < COMBO_RFQ_DISPERSION_MIN_QUOTES {
        return Vec::new();
    }
    edges.sort_by(|left, right| right.total_cmp(left));
    let second_best_edge = edges[1];
    let mut ascending = edges.clone();
    ascending.sort_by(|left, right| left.total_cmp(right));
    let median_edge = ascending[ascending.len() / 2];
    let second_best_gap_bps =
        ((selected_edge - second_best_edge).max(0.0) / notional_usd) * 10_000.0;
    let median_gap_bps = ((selected_edge - median_edge).max(0.0) / notional_usd) * 10_000.0;

    if second_best_gap_bps > COMBO_RFQ_DISPERSION_SECOND_BEST_GAP_BPS
        && median_gap_bps > COMBO_RFQ_DISPERSION_MEDIAN_GAP_BPS
    {
        vec![format!(
            "quote_dispersion_outlier:second_best_gap_bps={second_best_gap_bps:.2}>={:.2}:median_gap_bps={median_gap_bps:.2}>={:.2}",
            COMBO_RFQ_DISPERSION_SECOND_BEST_GAP_BPS,
            COMBO_RFQ_DISPERSION_MEDIAN_GAP_BPS,
        )]
    } else {
        Vec::new()
    }
}

fn combo_rfq_last_look_blockers(config: &Config, quote: &ComboRfqQuoteCandidate) -> Vec<String> {
    if !config.combo_rfq_accept_enabled {
        return Vec::new();
    }
    let mut blockers = Vec::new();
    if let Some(age_ms) = quote.age_ms.filter(|age_ms| *age_ms >= 0) {
        if age_ms as u64 >= COMBO_RFQ_LAST_LOOK_WINDOW_MS {
            blockers.push(format!(
                "last_look_quote_age:{}ms>={}ms",
                age_ms, COMBO_RFQ_LAST_LOOK_WINDOW_MS
            ));
        }
    }
    if let Some(expires_at) = quote
        .expires_at
        .as_deref()
        .and_then(parse_rfc3339_timestamp)
    {
        let millis_remaining = (expires_at - Utc::now()).num_milliseconds();
        if millis_remaining < COMBO_RFQ_LAST_LOOK_WINDOW_MS as i64 {
            blockers.push(format!(
                "last_look_expiration_too_close:{}ms<{}ms",
                millis_remaining, COMBO_RFQ_LAST_LOOK_WINDOW_MS
            ));
        }
    }
    if let Some(edge) = quote.expected_edge_usd.filter(|edge| edge.is_finite()) {
        let haircut = combo_rfq_last_look_edge_haircut_usd(config, quote.age_ms);
        let required_edge = config.min_net_profit_usd + haircut;
        if edge <= required_edge {
            blockers.push(format!(
                "last_look_edge_after_haircut_below_min:edge={edge:.4} required={required_edge:.4} haircut={haircut:.4}"
            ));
        }
    }
    blockers
}

fn combo_rfq_last_look_edge_haircut_usd(config: &Config, quote_age_ms: Option<i64>) -> f64 {
    let base = config.live_edge_haircut_usd.max(0.0)
        + config.live_trade_position_size_usd.max(0.0) * config.live_edge_haircut_bps as f64
            / 10_000.0;
    let age_fraction = quote_age_ms
        .filter(|age_ms| *age_ms > 0)
        .map(|age_ms| age_ms as f64 / COMBO_RFQ_LAST_LOOK_WINDOW_MS as f64)
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);
    let age_haircut =
        config.live_trade_position_size_usd.max(0.0) * config.live_slippage_bps as f64 / 10_000.0
            * age_fraction;
    base + age_haircut
}

fn combo_rfq_pre_accept_freshness_blockers(
    config: &Config,
    quote: &ComboRfqQuoteCandidate,
    quote_response_to_accept_elapsed: Duration,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let elapsed_ms = quote_response_to_accept_elapsed.as_millis();
    if elapsed_ms > u128::from(config.live_max_refresh_to_submit_ms.max(1)) {
        blockers.push(format!(
            "pre_accept_elapsed:{}ms>{}ms",
            elapsed_ms,
            config.live_max_refresh_to_submit_ms.max(1)
        ));
    }

    match combo_rfq_effective_quote_age_ms(quote, quote_response_to_accept_elapsed) {
        Some(age_ms) if age_ms < 0 => blockers.push(format!("pre_accept_future_quote:{age_ms}ms")),
        Some(age_ms) => {
            if age_ms as u64 > config.combo_rfq_quote_max_age_ms {
                blockers.push(format!(
                    "pre_accept_stale_quote:{}ms>{}ms",
                    age_ms, config.combo_rfq_quote_max_age_ms
                ));
            }
            if age_ms as u64 >= COMBO_RFQ_LAST_LOOK_WINDOW_MS {
                blockers.push(format!(
                    "pre_accept_last_look_quote_age:{}ms>={}ms",
                    age_ms, COMBO_RFQ_LAST_LOOK_WINDOW_MS
                ));
            }
        }
        None => blockers.push("pre_accept_missing_quote_age".to_string()),
    }

    match quote
        .expires_at
        .as_deref()
        .and_then(parse_rfc3339_timestamp)
    {
        Some(expires_at) => {
            let millis_remaining = (expires_at - Utc::now()).num_milliseconds();
            if millis_remaining <= 0 {
                blockers.push("pre_accept_quote_expired".to_string());
            } else if millis_remaining < COMBO_RFQ_LAST_LOOK_WINDOW_MS as i64 {
                blockers.push(format!(
                    "pre_accept_expiration_too_close:{}ms<{}ms",
                    millis_remaining, COMBO_RFQ_LAST_LOOK_WINDOW_MS
                ));
            }
        }
        None => blockers.push("pre_accept_missing_quote_expiration".to_string()),
    }
    blockers
}

fn combo_rfq_effective_quote_age_ms(
    quote: &ComboRfqQuoteCandidate,
    quote_response_to_accept_elapsed: Duration,
) -> Option<i64> {
    quote
        .age_ms
        .map(|age_ms| {
            age_ms.saturating_add(
                quote_response_to_accept_elapsed
                    .as_millis()
                    .min(i64::MAX as u128) as i64,
            )
        })
        .or_else(|| quote.created_at.as_deref().and_then(parse_rfc3339_ms_age))
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ComboRfqPreAcceptToxicity {
    haircut_bps: f64,
    haircut_usd: f64,
    trade_prints: usize,
    recent_book_updates: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ComboRfqPreAcceptMicrostructure {
    microprice_mean: Option<f64>,
    queue_imbalance_mean: Option<f64>,
    synthetic_price: Option<f64>,
    tokens: usize,
}

fn combo_rfq_depth_vamp(ask_depth: &[(f64, f64)], bid_depth: &[(f64, f64)]) -> Option<(f64, f64)> {
    let mut weighted_sum = 0.0;
    let mut size_sum = 0.0;
    let mut bid_size_sum = 0.0;
    let mut ask_size_sum = 0.0;

    for ((ask_price, ask_size), (bid_price, bid_size)) in ask_depth
        .iter()
        .take(COMBO_RFQ_MICROSTRUCTURE_DEPTH_LEVELS)
        .zip(bid_depth.iter().take(COMBO_RFQ_MICROSTRUCTURE_DEPTH_LEVELS))
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
        return None;
    }
    Some((
        weighted_sum / size_sum,
        (bid_size_sum - ask_size_sum) / size_sum,
    ))
}

async fn combo_rfq_pre_accept_microstructure(
    price_cache: Option<&PriceCache>,
    opp: &ArbitrageOpportunity,
    token_ids: &[String],
) -> ComboRfqPreAcceptMicrostructure {
    let Some(price_cache) = price_cache else {
        return ComboRfqPreAcceptMicrostructure {
            microprice_mean: None,
            queue_imbalance_mean: None,
            synthetic_price: None,
            tokens: 0,
        };
    };
    let cache = price_cache.read().await;
    let mut microprice_sum = 0.0;
    let mut queue_imbalance_sum = 0.0;
    let mut synthetic_price = 0.0;
    let mut tokens = 0usize;

    for (token_id, leg) in token_ids.iter().zip(opp.execution_plan.iter()) {
        let Some(snapshot) = cache.get(token_id.as_str()) else {
            continue;
        };
        let depth_vamp = combo_rfq_depth_vamp(&snapshot.ask_depth, &snapshot.bid_depth);
        let (microprice, queue_imbalance) = if let Some(depth_vamp) = depth_vamp {
            depth_vamp
        } else {
            let (Some(best_bid), Some(best_ask), Some(best_bid_size), Some(best_ask_size)) = (
                snapshot.best_bid,
                snapshot.best_ask,
                snapshot.best_bid_size,
                snapshot.best_ask_size,
            ) else {
                continue;
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
                continue;
            }
            let size_sum = best_bid_size + best_ask_size;
            if size_sum <= f64::EPSILON {
                continue;
            }
            (
                (best_ask * best_bid_size + best_bid * best_ask_size) / size_sum,
                (best_bid_size - best_ask_size) / size_sum,
            )
        };
        microprice_sum += microprice;
        synthetic_price += microprice * leg.unit_shares;
        queue_imbalance_sum += queue_imbalance;
        tokens += 1;
    }

    ComboRfqPreAcceptMicrostructure {
        microprice_mean: (tokens > 0).then_some(microprice_sum / tokens as f64),
        queue_imbalance_mean: (tokens > 0).then_some(queue_imbalance_sum / tokens as f64),
        synthetic_price: (tokens == token_ids.len()).then_some(synthetic_price),
        tokens,
    }
}

fn combo_rfq_pre_accept_microprice_blockers(
    config: &Config,
    quote_price: f64,
    microstructure: &ComboRfqPreAcceptMicrostructure,
) -> Vec<String> {
    let max_adverse_bps = config.combo_rfq_microprice_adverse_bps.max(0.0);
    if max_adverse_bps <= f64::EPSILON {
        return Vec::new();
    }
    let Some(ws_microprice) = microstructure.synthetic_price else {
        return Vec::new();
    };
    if quote_price <= f64::EPSILON
        || ws_microprice <= f64::EPSILON
        || !quote_price.is_finite()
        || !ws_microprice.is_finite()
    {
        return Vec::new();
    }
    let adverse_bps = (quote_price - ws_microprice) / ws_microprice * 10_000.0;
    if adverse_bps > max_adverse_bps {
        vec![format!(
            "pre_accept_microprice_adverse:quote={quote_price:.6}:ws_microprice={ws_microprice:.6}:adverse_bps={adverse_bps:.2}>{max_adverse_bps:.2}:tokens={}",
            microstructure.tokens
        )]
    } else {
        Vec::new()
    }
}

async fn combo_rfq_pre_accept_toxicity_haircut(
    config: &Config,
    price_cache: Option<&PriceCache>,
    token_ids: &[String],
    quote_to_accept_ms: Option<i64>,
    notional_usd: f64,
    markout_started_at: Instant,
) -> ComboRfqPreAcceptToxicity {
    let mut trade_prints = 0usize;
    let mut recent_book_updates = 0usize;
    let window_ms = COMBO_RFQ_TOXICITY_WINDOW_MS
        .max(config.live_max_refresh_to_submit_ms.max(1))
        .max(COMBO_RFQ_QUOTE_COLLECTION_WINDOW_MS);
    let window = Duration::from_millis(window_ms);

    if let Some(price_cache) = price_cache {
        let cache = price_cache.read().await;
        for token_id in token_ids {
            let Some(snapshot) = cache.get(token_id.as_str()) else {
                continue;
            };
            if markout_started_at
                .checked_duration_since(snapshot.last_updated)
                .map(|age| age <= window)
                .unwrap_or(false)
            {
                recent_book_updates += 1;
            }
            trade_prints += snapshot
                .recent_trades
                .iter()
                .filter(|trade| {
                    markout_started_at
                        .checked_duration_since(trade.observed_at)
                        .map(|age| age <= window)
                        .unwrap_or(false)
                })
                .count();
        }
    }

    let slippage_bps = config.live_slippage_bps.max(1) as f64;
    let quote_age_fraction = quote_to_accept_ms
        .filter(|age_ms| *age_ms > 0)
        .map(|age_ms| age_ms as f64 / COMBO_RFQ_LAST_LOOK_WINDOW_MS as f64)
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);
    let age_bps = slippage_bps * quote_age_fraction;
    let trade_bps = trade_prints.min(COMBO_RFQ_TOXICITY_MAX_TRADE_PRINTS) as f64 * slippage_bps;
    let book_update_bps = recent_book_updates as f64 * (slippage_bps / 2.0).max(0.5);
    let haircut_bps = age_bps + trade_bps + book_update_bps;
    let haircut_usd = notional_usd.max(0.0) * haircut_bps / 10_000.0;

    ComboRfqPreAcceptToxicity {
        haircut_bps,
        haircut_usd,
        trade_prints,
        recent_book_updates,
    }
}

async fn combo_rfq_pre_accept_markout_report(
    client: &Client,
    config: &Config,
    opp: &ArbitrageOpportunity,
    quote: &ComboRfqQuoteCandidate,
    accept_request: &ComboRfqAcceptQuoteRequest,
    price_cache: Option<&PriceCache>,
    require_price_cache: bool,
) -> ComboRfqPreAcceptMarkoutReport {
    let quote_qty_decimal = parse_positive_f64(&accept_request.qty_decimal).unwrap_or(0.0);
    let quote_price = quote.price;
    let quote_cost_usd = quote_price * quote_qty_decimal;
    let live_cost_buffer_usd = combo_rfq_live_cost_buffer_usd(config, opp, quote_cost_usd);
    let quote_edge_usd =
        combo_rfq_edge_usd_for_price(config, opp, quote_price, quote_qty_decimal).unwrap_or(0.0);
    let quote_to_accept_ms = combo_rfq_quote_to_accept_ms(quote);
    let token_ids = combo_rfq_markout_token_ids(opp).unwrap_or_default();
    let mut blockers = Vec::new();
    let mut synthetic_price = 0.0;
    let mut synthetic_cost_usd = 0.0;
    let mut public_edge_usd = 0.0;
    let mut markout_bps = 0.0;
    let mut toxicity_haircut_bps = 0.0;
    let mut toxicity_haircut_usd = 0.0;
    let mut toxicity_trade_prints = 0usize;
    let mut toxicity_recent_book_updates = 0usize;
    let mut ws_microprice_mean = None;
    let mut ws_queue_imbalance_mean = None;
    let mut ws_microstructure_tokens = 0usize;
    let mut quote_edge_after_toxicity_usd = quote_edge_usd;
    let mut public_edge_after_toxicity_usd = 0.0;
    let mut book_hashes = Vec::new();

    if quote_qty_decimal <= f64::EPSILON {
        blockers.push("pre_accept_markout_invalid_quote_qty".to_string());
    }
    if quote_price <= f64::EPSILON || !quote_price.is_finite() {
        blockers.push("pre_accept_markout_invalid_quote_price".to_string());
    }
    if token_ids.len() != opp.execution_plan.len() || token_ids.is_empty() {
        blockers.push("pre_accept_markout_missing_token_ids".to_string());
    }

    if blockers.is_empty() {
        let markout_started_at = Instant::now();
        match tokio::try_join!(
            crate::clob_client::get_live_depth_snapshots(client, config, &token_ids),
            crate::clob_client::get_live_sell_prices(client, config, &token_ids),
        ) {
            Ok((snapshots, sell_prices)) => {
                blockers.extend(combo_rfq_markout_depth_blockers(
                    config, &token_ids, &snapshots,
                ));
                blockers.extend(combo_rfq_markout_price_integrity_blockers(
                    &token_ids,
                    &snapshots,
                    &sell_prices,
                ));
                blockers.extend(
                    combo_rfq_pre_accept_causal_watermark_blockers(
                        config,
                        price_cache,
                        &token_ids,
                        &snapshots,
                        markout_started_at,
                        require_price_cache,
                    )
                    .await,
                );
                if blockers.is_empty() {
                    for (leg, token_id) in opp.execution_plan.iter().zip(token_ids.iter()) {
                        let target_shares = quote_qty_decimal * leg.unit_shares;
                        match snapshots
                            .get(token_id)
                            .and_then(|snapshot| snapshot.average_ask_for_shares(target_shares))
                        {
                            Some(avg_price) => synthetic_price += avg_price * leg.unit_shares,
                            None => blockers.push(format!(
                                "pre_accept_markout_insufficient_depth:{token_id}:shares={target_shares:.6}"
                            )),
                        }
                    }
                    if blockers.is_empty() {
                        synthetic_cost_usd = synthetic_price * quote_qty_decimal;
                        public_edge_usd = combo_rfq_edge_usd_for_price(
                            config,
                            opp,
                            synthetic_price,
                            quote_qty_decimal,
                        )
                        .unwrap_or(0.0);
                        markout_bps = if synthetic_price > f64::EPSILON {
                            (quote_price - synthetic_price) / synthetic_price * 10_000.0
                        } else {
                            0.0
                        };
                        book_hashes = token_ids
                            .iter()
                            .filter_map(|token_id| {
                                snapshots
                                    .get(token_id)
                                    .and_then(|snapshot| snapshot.book_hash.clone())
                            })
                            .collect();
                        let toxicity = combo_rfq_pre_accept_toxicity_haircut(
                            config,
                            price_cache,
                            &token_ids,
                            quote_to_accept_ms,
                            quote_cost_usd,
                            markout_started_at,
                        )
                        .await;
                        toxicity_haircut_bps = toxicity.haircut_bps;
                        toxicity_haircut_usd = toxicity.haircut_usd;
                        toxicity_trade_prints = toxicity.trade_prints;
                        toxicity_recent_book_updates = toxicity.recent_book_updates;
                        quote_edge_after_toxicity_usd = quote_edge_usd - toxicity_haircut_usd;
                        public_edge_after_toxicity_usd = public_edge_usd - toxicity_haircut_usd;
                        let microstructure =
                            combo_rfq_pre_accept_microstructure(price_cache, opp, &token_ids).await;
                        ws_microprice_mean = microstructure.microprice_mean;
                        ws_queue_imbalance_mean = microstructure.queue_imbalance_mean;
                        ws_microstructure_tokens = microstructure.tokens;
                        blockers.extend(combo_rfq_pre_accept_microprice_blockers(
                            config,
                            quote_price,
                            &microstructure,
                        ));
                    }
                }
            }
            Err(err) => blockers.push(format!(
                "pre_accept_markout_quote_integrity_unavailable:{err}"
            )),
        }
    }

    if blockers.is_empty() {
        if quote_edge_usd <= config.min_net_profit_usd {
            blockers.push(format!(
                "pre_accept_quote_edge_below_min:{quote_edge_usd:.4}<={:.4}",
                config.min_net_profit_usd
            ));
        }
        if public_edge_usd <= config.min_net_profit_usd {
            blockers.push(format!(
                "pre_accept_public_edge_below_min:{public_edge_usd:.4}<={:.4}",
                config.min_net_profit_usd
            ));
        }
        if quote_edge_after_toxicity_usd <= config.min_net_profit_usd {
            blockers.push(format!(
                "pre_accept_quote_edge_after_toxicity_below_min:edge_after={quote_edge_after_toxicity_usd:.4}<={:.4}:toxicity_haircut_usd={toxicity_haircut_usd:.4}:toxicity_bps={toxicity_haircut_bps:.2}:trades={toxicity_trade_prints}:book_updates={toxicity_recent_book_updates}",
                config.min_net_profit_usd
            ));
        }
        if public_edge_after_toxicity_usd <= config.min_net_profit_usd {
            blockers.push(format!(
                "pre_accept_public_edge_after_toxicity_below_min:edge_after={public_edge_after_toxicity_usd:.4}<={:.4}:toxicity_haircut_usd={toxicity_haircut_usd:.4}:toxicity_bps={toxicity_haircut_bps:.2}:trades={toxicity_trade_prints}:book_updates={toxicity_recent_book_updates}",
                config.min_net_profit_usd
            ));
        }
        if markout_bps > COMBO_RFQ_PRE_ACCEPT_MAX_ADVERSE_MARKOUT_BPS {
            blockers.push(format!(
                "pre_accept_adverse_markout:{markout_bps:.2}bps>{COMBO_RFQ_PRE_ACCEPT_MAX_ADVERSE_MARKOUT_BPS:.2}bps"
            ));
        }
    }

    ComboRfqPreAcceptMarkoutReport {
        status: if blockers.is_empty() {
            "ok".to_string()
        } else {
            "blocked".to_string()
        },
        blockers,
        quote_to_accept_ms,
        maker_id: quote.maker_id.clone(),
        quote_price,
        quote_qty_decimal,
        quote_cost_usd,
        live_cost_buffer_usd,
        synthetic_price,
        synthetic_cost_usd,
        quote_edge_usd,
        public_edge_usd,
        markout_bps,
        toxicity_haircut_bps,
        toxicity_haircut_usd,
        toxicity_trade_prints,
        toxicity_recent_book_updates,
        ws_microprice_mean,
        ws_queue_imbalance_mean,
        ws_microstructure_tokens,
        quote_edge_after_toxicity_usd,
        public_edge_after_toxicity_usd,
        token_ids,
        book_hashes,
    }
}

async fn combo_rfq_pre_accept_causal_watermark_blockers(
    config: &Config,
    price_cache: Option<&PriceCache>,
    token_ids: &[String],
    snapshots: &HashMap<String, crate::clob_client::DepthSnapshot>,
    markout_started_at: Instant,
    require_price_cache: bool,
) -> Vec<String> {
    let Some(price_cache) = price_cache else {
        return if require_price_cache {
            vec!["pre_accept_causal_watermark_missing_price_cache".to_string()]
        } else {
            Vec::new()
        };
    };
    let cache = price_cache.read().await;
    let mut blockers = Vec::new();
    for token_id in token_ids {
        let Some(ws_snapshot) = cache.get(token_id.as_str()) else {
            if require_price_cache {
                blockers.push(format!(
                    "pre_accept_causal_watermark_missing_ws_snapshot:{token_id}"
                ));
            }
            continue;
        };
        let Some(rest_snapshot) = snapshots.get(token_id) else {
            continue;
        };
        if require_price_cache {
            if ws_snapshot.venue_timestamp_ms.is_none() {
                blockers.push(format!(
                    "pre_accept_causal_watermark_missing_ws_timestamp:{token_id}"
                ));
            }
            if ws_snapshot
                .book_hash
                .as_deref()
                .is_none_or(|hash| hash.trim().is_empty())
            {
                blockers.push(format!(
                    "pre_accept_causal_watermark_missing_ws_book_hash:{token_id}"
                ));
            }
        }
        if let (Some(ws_ts), Some(rest_ts)) = (
            ws_snapshot.venue_timestamp_ms,
            rest_snapshot.venue_timestamp_ms,
        ) {
            if ws_ts > rest_ts {
                blockers.push(format!(
                    "pre_accept_causal_watermark_newer_ws_timestamp:{token_id}:ws={ws_ts}>rest={rest_ts}"
                ));
            } else {
                let lag_ms = rest_ts.saturating_sub(ws_ts);
                let max_lag_ms = config
                    .live_max_refresh_to_submit_ms
                    .max(config.ws_quote_max_age_ms)
                    .max(1);
                if lag_ms > max_lag_ms {
                    blockers.push(format!(
                        "pre_accept_causal_watermark_ws_lagging_rest:{token_id}:rest={rest_ts}>ws={ws_ts}:lag={lag_ms}ms>{max_lag_ms}ms"
                    ));
                }
            }
        }
        if ws_snapshot.book_hash.is_some()
            && rest_snapshot.book_hash.is_some()
            && ws_snapshot.book_hash != rest_snapshot.book_hash
        {
            let same_timestamp = matches!(
                (
                    ws_snapshot.venue_timestamp_ms,
                    rest_snapshot.venue_timestamp_ms
                ),
                (Some(ws_ts), Some(rest_ts)) if ws_ts == rest_ts
            );
            let changed_after_markout = ws_snapshot
                .last_updated
                .checked_duration_since(markout_started_at)
                .is_some();
            if same_timestamp {
                blockers.push(format!(
                    "pre_accept_causal_watermark_same_timestamp_book_hash_mismatch:{token_id}:ws={}:rest={}",
                    ws_snapshot.book_hash.as_deref().unwrap_or_default(),
                    rest_snapshot.book_hash.as_deref().unwrap_or_default()
                ));
            } else if matches!(
                (
                    ws_snapshot.venue_timestamp_ms,
                    rest_snapshot.venue_timestamp_ms
                ),
                (Some(ws_ts), Some(rest_ts)) if rest_ts > ws_ts
            ) {
                blockers.push(format!(
                    "pre_accept_causal_watermark_rest_newer_book_hash_mismatch:{token_id}:ws={}:rest={}",
                    ws_snapshot.book_hash.as_deref().unwrap_or_default(),
                    rest_snapshot.book_hash.as_deref().unwrap_or_default()
                ));
            } else if changed_after_markout {
                blockers.push(format!(
                    "pre_accept_causal_watermark_ws_book_changed:{token_id}:ws={}:rest={}",
                    ws_snapshot.book_hash.as_deref().unwrap_or_default(),
                    rest_snapshot.book_hash.as_deref().unwrap_or_default()
                ));
            }
        }
        for trade in &ws_snapshot.recent_trades {
            let trade_after_rest_timestamp =
                match (trade.venue_timestamp_ms, rest_snapshot.venue_timestamp_ms) {
                    (Some(trade_ts), Some(rest_ts)) => trade_ts > rest_ts,
                    _ => false,
                };
            let trade_after_markout_started = trade
                .observed_at
                .checked_duration_since(markout_started_at)
                .is_some();
            if trade_after_rest_timestamp || trade_after_markout_started {
                blockers.push(format!(
                    "pre_accept_causal_watermark_trade_print:{token_id}:side={}:observed_after_markout={}:venue_timestamp_ms={:?}:rest_timestamp_ms={:?}",
                    trade.side,
                    trade_after_markout_started,
                    trade.venue_timestamp_ms,
                    rest_snapshot.venue_timestamp_ms
                ));
            }
        }
    }
    blockers
}

fn combo_rfq_edge_usd_for_price(
    config: &Config,
    opp: &ArbitrageOpportunity,
    price: f64,
    qty_decimal: f64,
) -> Option<f64> {
    if price <= f64::EPSILON
        || qty_decimal <= f64::EPSILON
        || !price.is_finite()
        || !qty_decimal.is_finite()
        || opp.guaranteed_revenue <= f64::EPSILON
    {
        return None;
    }
    let edge_per_unit = opp.guaranteed_revenue - price - opp.total_fees.max(0.0);
    let gross_edge_usd = edge_per_unit * qty_decimal;
    let notional_usd = price * qty_decimal;
    Some(gross_edge_usd - combo_rfq_live_cost_buffer_usd(config, opp, notional_usd))
}

fn combo_rfq_quote_to_accept_ms(quote: &ComboRfqQuoteCandidate) -> Option<i64> {
    quote
        .created_at
        .as_deref()
        .and_then(parse_rfc3339_ms_age)
        .or(quote.age_ms)
}

fn combo_rfq_markout_token_ids(opp: &ArbitrageOpportunity) -> Option<Vec<String>> {
    opp.execution_plan
        .iter()
        .map(|leg| {
            let market = opp.markets.get(leg.market_index)?;
            let token_id = if !leg.token_id.trim().is_empty() {
                leg.token_id.clone()
            } else if matches!(leg.outcome, OutcomeSide::Yes) {
                market.clob_token_id_yes.clone()
            } else {
                market.clob_token_id_no.clone()
            };
            let token_id = token_id.trim().to_string();
            (!token_id.is_empty()).then_some(token_id)
        })
        .collect()
}

fn combo_rfq_markout_depth_blockers(
    config: &Config,
    token_ids: &[String],
    snapshots: &HashMap<String, crate::clob_client::DepthSnapshot>,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let mut timestamps = Vec::new();
    for token_id in token_ids {
        let Some(snapshot) = snapshots.get(token_id) else {
            blockers.push(format!("pre_accept_markout_missing_book:{token_id}"));
            continue;
        };
        if snapshot
            .book_hash
            .as_deref()
            .map(str::trim)
            .filter(|hash| !hash.is_empty())
            .is_none()
        {
            blockers.push(format!("pre_accept_markout_missing_book_hash:{token_id}"));
        }
        if snapshot.asks.is_empty() {
            blockers.push(format!("pre_accept_markout_empty_asks:{token_id}"));
        }
        match snapshot.venue_timestamp_ms {
            Some(timestamp) => timestamps.push((token_id, timestamp)),
            None => blockers.push(format!("pre_accept_markout_missing_timestamp:{token_id}")),
        }
    }
    if !blockers.is_empty() || timestamps.is_empty() {
        return blockers;
    }

    let min_timestamp = timestamps
        .iter()
        .map(|(_, timestamp)| *timestamp)
        .min()
        .unwrap_or_default();
    let max_timestamp = timestamps
        .iter()
        .map(|(_, timestamp)| *timestamp)
        .max()
        .unwrap_or_default();
    let max_skew_ms = config
        .live_max_refresh_to_submit_ms
        .max(config.ws_quote_max_age_ms)
        .max(250);
    if max_timestamp.saturating_sub(min_timestamp) > max_skew_ms {
        blockers.push(format!(
            "pre_accept_markout_book_skew:{}ms>{max_skew_ms}ms",
            max_timestamp.saturating_sub(min_timestamp)
        ));
    }
    let now_ms = Utc::now().timestamp_millis().max(0) as u64;
    let max_age_ms = config
        .max_signal_age_secs
        .max(1)
        .saturating_mul(1000)
        .max(config.live_max_refresh_to_submit_ms.max(1));
    for (token_id, timestamp) in timestamps {
        if timestamp.saturating_add(max_age_ms) < now_ms {
            blockers.push(format!(
                "pre_accept_markout_stale_book:{token_id}:{}ms>{max_age_ms}ms",
                now_ms.saturating_sub(timestamp)
            ));
        }
        if timestamp > now_ms.saturating_add(max_skew_ms) {
            blockers.push(format!(
                "pre_accept_markout_future_book:{token_id}:{}ms>{max_skew_ms}ms",
                timestamp.saturating_sub(now_ms)
            ));
        }
    }
    blockers
}

fn combo_rfq_markout_price_integrity_blockers(
    token_ids: &[String],
    snapshots: &HashMap<String, crate::clob_client::DepthSnapshot>,
    sell_prices: &HashMap<String, f64>,
) -> Vec<String> {
    let mut blockers = Vec::new();
    for token_id in token_ids {
        let Some(snapshot) = snapshots.get(token_id) else {
            continue;
        };
        let Some((book_best_ask, _)) = snapshot.asks.first() else {
            continue;
        };
        let book_best_ask = *book_best_ask;
        let Some(sell_price) = sell_prices.get(token_id) else {
            blockers.push(format!("pre_accept_markout_missing_sell_price:{token_id}"));
            continue;
        };
        let sell_price = *sell_price;
        if !sell_price.is_finite() || sell_price <= 0.0 || sell_price > 1.0 {
            blockers.push(format!(
                "pre_accept_markout_invalid_sell_price:{token_id}:{sell_price:.6}"
            ));
            continue;
        }
        let tolerance = snapshot
            .tick_size
            .filter(|tick| tick.is_finite() && *tick > 0.0)
            .unwrap_or(0.01)
            .max(0.0001);
        if (book_best_ask - sell_price).abs() > tolerance + 1e-12 {
            blockers.push(format!(
                "pre_accept_markout_price_endpoint_mismatch:{token_id}:books_ask={book_best_ask:.6}:prices_sell={sell_price:.6}:tol={tolerance:.6}"
            ));
        }
    }
    blockers
}

fn normalize_rfq_status(status: &str) -> String {
    status.trim().to_ascii_uppercase().replace('-', "_")
}

fn combo_rfq_maker_status_is_success(status: &str) -> bool {
    matches!(
        status,
        "FILLED"
            | "FILL"
            | "CONFIRMED"
            | "SETTLED"
            | "BOTH_CONFIRMED"
            | "QUOTE_FILLED"
            | "QUOTE_CONFIRMED"
            | "QUOTE_SETTLED"
            | "TRADE_CONFIRMED"
    )
}

fn combo_rfq_maker_status_is_reject(status: &str) -> bool {
    matches!(
        status,
        "REJECTED" | "DONE_AWAY" | "LAST_LOOK_REJECTED" | "MAKER_REJECTED"
    )
}

fn combo_rfq_maker_status_is_failure(status: &str) -> bool {
    matches!(
        status,
        "EXPIRED"
            | "FAILED"
            | "CANCELLED"
            | "CANCELED"
            | "PARTIAL"
            | "ONE_LEG"
            | "GHOST_REVERT"
            | "QUOTE_EXPIRED"
    ) || combo_rfq_maker_status_is_reject(status)
}

fn combo_rfq_maker_status_is_pending(status: &str) -> bool {
    matches!(
        status,
        "PENDING" | "ACCEPTED" | "ACCEPTED_PENDING_FINALITY" | "SUBMITTED" | "MATCHED"
    )
}

fn combo_rfq_live_cost_buffer_usd(
    config: &Config,
    opp: &ArbitrageOpportunity,
    notional_usd: f64,
) -> f64 {
    let position_usd = notional_usd.max(0.0);
    let inferred_entry_gas_usd = opp.estimated_total_gas_cost_usd.max(0.0);
    let closeout_gas_buffer_usd =
        config.effective_trade_gas_cost_usd(config.gas_fallback_usd.max(0.0));
    let lock_hours = opp
        .capital_lock_hours
        .filter(|hours| hours.is_finite() && *hours >= 0.0)
        .unwrap_or(config.capital_velocity_reference_hours)
        .max(1.0);
    let capital_lock_cost_usd =
        position_usd * (lock_hours / (24.0 * 365.0)) * COMBO_RFQ_CAPITAL_LOCK_APR;
    let finality_failure_ev_usd = position_usd * COMBO_RFQ_FINALITY_FAILURE_PROB_FLOOR;
    let orphan_loss_fraction =
        config.live_slippage_bps as f64 / 10_000.0 + COMBO_RFQ_ORPHAN_CLOSEOUT_LOSS_FLOOR;
    let partial_exposure_ev_usd =
        position_usd * orphan_loss_fraction * COMBO_RFQ_PARTIAL_EXPOSURE_PROB_FLOOR;
    inferred_entry_gas_usd
        + closeout_gas_buffer_usd
        + capital_lock_cost_usd
        + finality_failure_ev_usd
        + partial_exposure_ev_usd
}

fn combo_rfq_quote_expected_edge_usd(
    config: &Config,
    opp: &ArbitrageOpportunity,
    quote: &ComboRfqQuoteCandidate,
) -> Option<f64> {
    if quote.price <= f64::EPSILON || opp.guaranteed_revenue <= f64::EPSILON {
        return None;
    }
    let qty_decimal = quote
        .qty_decimal
        .filter(|qty| qty.is_finite() && *qty > 0.0)
        .unwrap_or_else(|| config.live_trade_position_size_usd / quote.price);
    combo_rfq_edge_usd_for_price(config, opp, quote.price, qty_decimal)
}

fn text_value(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        let Some(field) = value.get(*key) else {
            continue;
        };
        let text = match field {
            Value::String(text) => text.clone(),
            Value::Number(number) => number.to_string(),
            Value::Bool(boolean) => boolean.to_string(),
            _ => continue,
        };
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn response_text_value(value: &Value, keys: &[&str]) -> Option<String> {
    response_text_value_with_depth(value, keys, 2)
}

fn response_text_value_with_depth(value: &Value, keys: &[&str], depth: usize) -> Option<String> {
    if let Some(text) = text_value(value, keys) {
        return Some(text);
    }
    if depth == 0 {
        return None;
    }
    for key in [
        "data",
        "result",
        "payload",
        "quote",
        "rfq",
        "acceptedQuote",
        "execution",
        "trade",
        "order",
    ] {
        let Some(child) = value.get(key) else {
            continue;
        };
        if let Some(text) = response_text_value_with_depth(child, keys, depth - 1) {
            return Some(text);
        }
    }
    None
}

fn number_value(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let Some(field) = value.get(*key) else {
            continue;
        };
        let parsed = match field {
            Value::Number(number) => number.as_f64(),
            Value::String(text) => text.trim().parse::<f64>().ok(),
            _ => None,
        };
        if parsed.map(|value| value.is_finite()).unwrap_or(false) {
            return parsed;
        }
    }
    None
}

fn response_number_value(value: &Value, keys: &[&str]) -> Option<f64> {
    response_number_value_with_depth(value, keys, 2)
}

fn response_number_value_with_depth(value: &Value, keys: &[&str], depth: usize) -> Option<f64> {
    if let Some(number) = number_value(value, keys) {
        return Some(number);
    }
    if depth == 0 {
        return None;
    }
    for key in [
        "data",
        "result",
        "payload",
        "quote",
        "rfq",
        "acceptedQuote",
        "execution",
        "trade",
        "order",
    ] {
        let Some(child) = value.get(key) else {
            continue;
        };
        if let Some(number) = response_number_value_with_depth(child, keys, depth - 1) {
            return Some(number);
        }
    }
    None
}

fn parse_rfc3339_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn parse_rfc3339_ms_age(value: &str) -> Option<i64> {
    parse_rfc3339_timestamp(value).map(|timestamp| {
        Utc::now()
            .signed_duration_since(timestamp)
            .num_milliseconds()
    })
}

fn combo_rfq_path_segment(value: &str, name: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{name} is required for Combo/RFQ requester call");
    }
    if value.contains('/') || value.contains('?') || value.contains('#') {
        bail!("{name} contains invalid path characters");
    }
    Ok(value.to_string())
}

fn rfq_side_for_outcome(outcome: OutcomeSide) -> &'static str {
    match outcome {
        OutcomeSide::Yes => "SIDE_BUY",
        OutcomeSide::No => "SIDE_SELL",
    }
}

fn combo_rfq_client_request_id(opp: &ArbitrageOpportunity) -> String {
    let base = combo_rfq_client_request_base_id(opp);
    let now_ms = Utc::now().timestamp_millis().max(0) as u64;
    let seq = COMBO_RFQ_CLIENT_REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{base}-{now_ms:012x}-{seq:04x}")
}

fn combo_rfq_client_request_base_id(opp: &ArbitrageOpportunity) -> String {
    let mut key = format!("{}:{}", opp.event_id, opp.arb_type);
    for leg in &opp.execution_plan {
        key.push(':');
        key.push_str(&leg.token_id);
        key.push(':');
        key.push_str(match leg.outcome {
            OutcomeSide::Yes => "Y",
            OutcomeSide::No => "N",
        });
    }
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{COMBO_RFQ_CLIENT_REQUEST_PREFIX}{hash:016x}")
}

fn combo_rfq_client_request_recovery_scopes_match(left: &str, right: &str) -> bool {
    combo_rfq_client_request_recovery_scope(left) == combo_rfq_client_request_recovery_scope(right)
}

fn combo_rfq_client_request_recovery_scope(value: &str) -> String {
    let value = value.trim();
    let Some(rest) = value.strip_prefix(COMBO_RFQ_CLIENT_REQUEST_PREFIX) else {
        return value.to_string();
    };
    let hash = rest.split('-').next().unwrap_or_default();
    if hash.len() == 16 && hash.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        format!("{COMBO_RFQ_CLIENT_REQUEST_PREFIX}{hash}")
    } else {
        value.to_string()
    }
}

fn format_decimal(value: f64) -> String {
    let formatted = format!("{value:.6}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn unique_condition_count(opp: &ArbitrageOpportunity) -> usize {
    opp.execution_plan
        .iter()
        .map(|leg| leg.condition_id.trim())
        .filter(|condition_id| !condition_id.is_empty())
        .collect::<HashSet<_>>()
        .len()
}

fn combo_rfq_planned_condition_ids(opp: &ArbitrageOpportunity) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut condition_ids = Vec::new();
    for leg in &opp.execution_plan {
        let condition_id = leg.condition_id.trim();
        if !condition_id.is_empty() && seen.insert(condition_id.to_string()) {
            condition_ids.push(condition_id.to_string());
        }
    }
    condition_ids
}

fn combo_rfq_market_readiness_failure(
    err: anyhow::Error,
    pre_accept: bool,
) -> (&'static str, &'static str, String) {
    let err = err.to_string();
    let rfq_enabled_error = err.contains("RFQ-enabled") || err.contains("rfqe");
    match (pre_accept, rfq_enabled_error) {
        (false, true) => (
            "blocked_rfq_enabled_firewall",
            "rfq_enabled_firewall",
            format!("combo_rfq_rfq_enabled_firewall:{err}"),
        ),
        (false, false) => (
            "blocked_delay_window_firewall",
            "delay_window_firewall",
            format!("combo_rfq_delay_window_firewall:{err}"),
        ),
        (true, true) => (
            "blocked_pre_accept_rfq_enabled_firewall",
            "pre_accept_rfq_enabled_firewall",
            format!("combo_rfq_pre_accept_rfq_enabled_firewall:{err}"),
        ),
        (true, false) => (
            "blocked_pre_accept_delay_window_firewall",
            "pre_accept_delay_window_firewall",
            format!("combo_rfq_pre_accept_delay_window_firewall:{err}"),
        ),
    }
}

fn spawn_combo_rfq_market_readiness_check(
    client: &Client,
    config: &Config,
    condition_ids: Vec<String>,
) -> tokio::task::JoinHandle<Result<()>> {
    let client = client.clone();
    let config = config.clone();
    tokio::spawn(async move {
        clob_client::verify_live_combo_rfq_markets(&client, &config, &condition_ids).await
    })
}

async fn combo_rfq_market_readiness_result(
    handle: tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    handle
        .await
        .map_err(|err| anyhow::anyhow!("Combo/RFQ market readiness prefetch failed: {err}"))?
}

fn value_as_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(value) => value.to_string(),
        _ => value.to_string(),
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_u32(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
        Value::String(text) => text
            .trim()
            .parse::<u64>()
            .ok()
            .and_then(|value| u32::try_from(value).ok()),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ArbType, Market, OpportunityLeg};
    use httpmock::prelude::*;
    use httpmock::Mock;
    use std::str::FromStr;

    fn temp_rfq_dir(name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        std::env::temp_dir().join(format!("polymarket-rfq-{name}-{suffix}"))
    }

    fn test_opp(arb_type: ArbType) -> ArbitrageOpportunity {
        ArbitrageOpportunity {
            event_title: "Event".into(),
            event_id: "E".into(),
            category: "general".into(),
            arb_type,
            markets: vec![market("cond-a", "A"), market("cond-b", "B")],
            execution_plan: vec![
                leg(0, "cond-a", "111", OutcomeSide::Yes),
                leg(1, "cond-b", "222", OutcomeSide::Yes),
            ],
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
            detected_at: chrono::Utc::now(),
        }
    }

    fn market(condition_id: &str, question: &str) -> Market {
        Market {
            question: question.into(),
            condition_id: condition_id.into(),
            market_slug: question.to_lowercase(),
            clob_token_id_yes: format!("{condition_id}-yes"),
            clob_token_id_no: format!("{condition_id}-no"),
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
            fees_enabled: Some(false),
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
            liquidity: 1_000.0,
            closed: false,
        }
    }

    fn leg(
        market_index: usize,
        condition_id: &str,
        token_id: &str,
        outcome: OutcomeSide,
    ) -> OpportunityLeg {
        OpportunityLeg {
            market_index,
            question: condition_id.into(),
            market_slug: condition_id.into(),
            condition_id: condition_id.into(),
            token_id: token_id.into(),
            outcome,
            unit_shares: 1.0,
            reference_price: 0.4,
        }
    }

    fn catalog() -> ComboMarketCatalog {
        ComboMarketCatalog::from_markets(vec![
            ComboMarketEntry {
                condition_id: "cond-a".into(),
                position_ids: vec!["111".into(), "112".into()],
                outcomes: vec!["Yes".into(), "No".into()],
                slug: "a".into(),
            },
            ComboMarketEntry {
                condition_id: "cond-b".into(),
                position_ids: vec!["222".into(), "223".into()],
                outcomes: vec!["Yes".into(), "No".into()],
                slug: "b".into(),
            },
        ])
    }

    fn append_passing_maker_samples(cfg: &Config, maker_id: &str) {
        for idx in 0..COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES {
            append_combo_rfq_maker_journal_record(
                cfg,
                &ComboRfqMakerJournalRecord {
                    generated_at: format!("2026-01-01T00:00:0{idx}Z"),
                    maker_id: Some(maker_id.into()),
                    quote_id: format!("good-quote-{idx}"),
                    rfq_id: Some(format!("good-rfq-{idx}")),
                    event_id: "good-event".into(),
                    quote_age_ms: Some(10),
                    expected_edge_usd: Some(2.0),
                    selected: true,
                    accepted: true,
                    terminal_status: Some("filled".into()),
                    realized_ev_usd: Some(0.50),
                    blockers: Vec::new(),
                    notes: Vec::new(),
                },
            )
            .unwrap();
        }
    }

    fn enable_settlement_monitor_for_makers(cfg: &mut Config, maker_ids: &[&str]) {
        cfg.settlement_monitor_enabled = true;
        cfg.combo_rfq_counterparty_min_settlement_samples = 1;
        fs::create_dir_all(&cfg.diagnostics_dir).unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(
                cfg.diagnostics_dir
                    .join(crate::settlement_monitor::SETTLEMENT_RECEIPTS_FILE),
            )
            .unwrap();
        for maker_id in maker_ids {
            let now = Utc::now().to_rfc3339();
            writeln!(
                file,
                "{}",
                serde_json::json!({
                    "generatedAt": now,
                    "transactionHash": format!("0xsettlement{maker_id}"),
                    "makerId": maker_id,
                    "status": "success",
                })
            )
            .unwrap();
        }
    }

    fn append_accept_ready_maker_samples(cfg: &mut Config, maker_id: &str) {
        append_passing_maker_samples(cfg, maker_id);
        enable_settlement_monitor_for_makers(cfg, &[maker_id]);
    }

    fn append_markout_race_samples(cfg: &Config, maker_id: &str, markout_bps_values: &[f64]) {
        for (idx, markout_bps) in markout_bps_values.iter().enumerate() {
            append_combo_rfq_markout_race_journal_record(
                cfg,
                &ComboRfqMarkoutRaceJournalRecord {
                    generated_at: Utc::now().to_rfc3339(),
                    race_id: format!("event:rfq-{idx}:quote-{idx}"),
                    event_id: "event".into(),
                    rfq_id: format!("rfq-{idx}"),
                    quote_id: format!("quote-{idx}"),
                    maker_id: Some(maker_id.into()),
                    horizon_ms: cfg.combo_rfq_markout_race_score_horizon_ms,
                    status: "sampled".into(),
                    quote_price: 0.75,
                    quote_qty_decimal: 10.0,
                    pre_accept_synthetic_price: 0.74,
                    sampled_synthetic_price: Some(0.74),
                    sampled_public_edge_usd: Some(2.60),
                    sampled_markout_bps: Some(*markout_bps),
                    token_ids: vec!["111".into(), "222".into()],
                    pre_accept_book_hashes: vec!["pre-111".into(), "pre-222".into()],
                    sampled_book_hashes: vec!["sample-111".into(), "sample-222".into()],
                    blockers: Vec::new(),
                    error: None,
                },
            )
            .unwrap();
        }
    }

    #[test]
    fn rfq_accept_response_blockers_require_accepted_status_and_matching_fields() {
        let quote = ComboRfqQuoteCandidate {
            quote_id: "quote-1".into(),
            rfq_id: Some("rfq-1".into()),
            maker_id: Some("maker-good".into()),
            symbol: Some("combo-a".into()),
            side: Some("SIDE_BUY".into()),
            status: Some("ACTIVE".into()),
            price: 0.75,
            qty_decimal: Some(33.333333),
            created_at: None,
            expires_at: None,
            age_ms: Some(10),
            expected_edge_usd: Some(2.0),
        };
        let accept_request = ComboRfqAcceptQuoteRequest {
            side: "SIDE_BUY".into(),
            price: "0.75".into(),
            symbol: "combo-a".into(),
            qty_decimal: "33.333333".into(),
        };

        let accepted = serde_json::json!({
            "data": {
                "quote": {
                    "status": "accepted",
                    "rfqId": "rfq-1",
                    "quoteId": "quote-1",
                    "price": "0.75",
                    "qtyDecimal": "33.333333"
                }
            }
        });
        assert!(
            combo_rfq_accept_response_blockers("rfq-1", &quote, &accept_request, &accepted)
                .is_empty()
        );
        assert_eq!(
            combo_rfq_accept_response_outcome("rfq-1", &quote, &accept_request, &accepted).0,
            ComboRfqAcceptOutcome::Accepted
        );

        let rejected = serde_json::json!({
            "status": "rejected",
            "rfqId": "rfq-1",
            "quoteId": "quote-1"
        });
        let blockers =
            combo_rfq_accept_response_blockers("rfq-1", &quote, &accept_request, &rejected);
        assert!(blockers
            .iter()
            .any(|blocker| blocker == "accept_response_status_not_accepted:REJECTED"));
        let (outcome, blockers) =
            combo_rfq_accept_response_outcome("rfq-1", &quote, &accept_request, &rejected);
        assert_eq!(outcome, ComboRfqAcceptOutcome::RejectedProven);
        assert_eq!(blockers, vec!["rfq_accept_rejected_proven:REJECTED"]);

        let status_only_success = serde_json::json!({"status": "success"});
        let blockers = combo_rfq_accept_response_blockers(
            "rfq-1",
            &quote,
            &accept_request,
            &status_only_success,
        );
        assert!(blockers
            .iter()
            .any(|blocker| blocker == "accept_response_status_not_accepted:SUCCESS"));
        assert!(blockers
            .iter()
            .any(|blocker| blocker == "accept_response_missing_rfq_id"));
        assert!(blockers
            .iter()
            .any(|blocker| blocker == "accept_response_missing_price"));
        assert_eq!(
            combo_rfq_accept_response_outcome(
                "rfq-1",
                &quote,
                &accept_request,
                &status_only_success
            )
            .0,
            ComboRfqAcceptOutcome::Unknown
        );

        let mismatched = serde_json::json!({
            "status": "accepted",
            "rfqId": "rfq-2",
            "quoteId": "quote-2",
            "price": "0.80",
            "qtyDecimal": "10"
        });
        let blockers =
            combo_rfq_accept_response_blockers("rfq-1", &quote, &accept_request, &mismatched);
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("accept_response_rfq_id_mismatch:")));
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("accept_response_quote_id_mismatch:")));
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("accept_response_price_mismatch:")));
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("accept_response_qty_mismatch:")));
    }

    #[test]
    fn rfq_expected_edge_uses_quote_quantity_and_live_cost_buffer() {
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.gas_fallback_usd = 0.0;
        cfg.capital_velocity_reference_hours = 24.0;
        cfg.live_slippage_bps = 0;
        let mut opp = test_opp(ArbType::Yes);
        opp.estimated_total_gas_cost_usd = 0.50;
        opp.total_fees = 0.0;
        let quote = ComboRfqQuoteCandidate {
            quote_id: "quote-1".into(),
            rfq_id: Some("rfq-1".into()),
            maker_id: Some("maker-good".into()),
            symbol: Some("combo-a".into()),
            side: Some("SIDE_BUY".into()),
            status: Some("ACTIVE".into()),
            price: 0.75,
            qty_decimal: Some(10.0),
            created_at: None,
            expires_at: None,
            age_ms: Some(10),
            expected_edge_usd: None,
        };

        let edge = combo_rfq_quote_expected_edge_usd(&cfg, &opp, &quote).unwrap();
        let raw_quote_edge = (opp.guaranteed_revenue - quote.price) * 10.0;
        let raw_config_notional_edge = (opp.guaranteed_revenue - quote.price)
            * (cfg.live_trade_position_size_usd / quote.price);
        let expected_buffer = combo_rfq_live_cost_buffer_usd(&cfg, &opp, quote.price * 10.0);

        assert!((edge - (raw_quote_edge - expected_buffer)).abs() < 1e-9);
        assert!(edge < raw_quote_edge);
        assert!(edge < raw_config_notional_edge);
    }

    #[test]
    fn rfq_expected_edge_uses_larger_fresh_entry_gas_estimate() {
        let mut cfg = Config::from_env();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.gas_fallback_usd = 0.0;
        cfg.capital_velocity_reference_hours = 24.0;
        cfg.live_slippage_bps = 0;
        let mut stale_gas_opp = test_opp(ArbType::Yes);
        stale_gas_opp.estimated_total_gas_cost_usd = 0.05;
        stale_gas_opp.total_fees = 0.0;
        let mut fresh_gas_opp = stale_gas_opp.clone();
        fresh_gas_opp.estimated_total_gas_cost_usd =
            fresh_gas_opp.estimated_total_gas_cost_usd.max(1.25);
        let quote = ComboRfqQuoteCandidate {
            quote_id: "quote-1".into(),
            rfq_id: Some("rfq-1".into()),
            maker_id: Some("maker-good".into()),
            symbol: Some("combo-a".into()),
            side: Some("SIDE_BUY".into()),
            status: Some("ACTIVE".into()),
            price: 0.75,
            qty_decimal: Some(10.0),
            created_at: None,
            expires_at: None,
            age_ms: Some(10),
            expected_edge_usd: None,
        };

        let stale_edge = combo_rfq_quote_expected_edge_usd(&cfg, &stale_gas_opp, &quote).unwrap();
        let fresh_edge = combo_rfq_quote_expected_edge_usd(&cfg, &fresh_gas_opp, &quote).unwrap();

        assert!((stale_edge - fresh_edge - 1.20).abs() < 1e-9);
        assert!(fresh_edge < stale_edge);
    }

    #[test]
    fn rfq_live_cost_buffer_uses_opportunity_capital_lock_hours() {
        let mut cfg = Config::from_env();
        cfg.gas_fallback_usd = 0.0;
        cfg.capital_velocity_reference_hours = 240.0;
        cfg.live_slippage_bps = 0;
        let mut fallback_opp = test_opp(ArbType::Yes);
        fallback_opp.estimated_total_gas_cost_usd = 0.0;
        fallback_opp.total_fees = 0.0;
        let mut short_lock_opp = fallback_opp.clone();
        short_lock_opp.capital_lock_hours = Some(6.0);
        let notional_usd = 7.5;

        let fallback_buffer = combo_rfq_live_cost_buffer_usd(&cfg, &fallback_opp, notional_usd);
        let short_lock_buffer = combo_rfq_live_cost_buffer_usd(&cfg, &short_lock_opp, notional_usd);
        let expected_delta =
            notional_usd * ((240.0 - 6.0) / (24.0 * 365.0)) * COMBO_RFQ_CAPITAL_LOCK_APR;

        assert!((fallback_buffer - short_lock_buffer - expected_delta).abs() < 1e-9);
        assert!(short_lock_buffer < fallback_buffer);
    }

    #[test]
    fn rfq_pre_accept_freshness_rechecks_elapsed_age_and_expiration() {
        let mut cfg = Config::from_env();
        cfg.combo_rfq_quote_max_age_ms = 2_000;
        cfg.live_max_refresh_to_submit_ms = 250;
        let quote = ComboRfqQuoteCandidate {
            quote_id: "quote-1".into(),
            rfq_id: Some("rfq-1".into()),
            maker_id: Some("maker-good".into()),
            symbol: Some("combo-a".into()),
            side: Some("SIDE_BUY".into()),
            status: Some("ACTIVE".into()),
            price: 0.75,
            qty_decimal: Some(10.0),
            created_at: None,
            expires_at: Some((Utc::now() + chrono::Duration::milliseconds(500)).to_rfc3339()),
            age_ms: Some(900),
            expected_edge_usd: None,
        };

        let blockers =
            combo_rfq_pre_accept_freshness_blockers(&cfg, &quote, Duration::from_millis(300));

        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("pre_accept_elapsed:")));
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("pre_accept_last_look_quote_age:")));
        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("pre_accept_expiration_too_close:")));
    }

    fn combo_rfq_books_body_at(price_a: f64, price_b: f64, timestamp: i64) -> String {
        serde_json::json!([
            {
                "asset_id": "111",
                "asks": [{"price": format_decimal(price_a), "size": "100"}],
                "bids": [{"price": "0.30", "size": "100"}],
                "tick_size": "0.001",
                "min_order_size": "1",
                "neg_risk": true,
                "timestamp": timestamp,
                "hash": "book-111"
            },
            {
                "asset_id": "222",
                "asks": [{"price": format_decimal(price_b), "size": "100"}],
                "bids": [{"price": "0.30", "size": "100"}],
                "tick_size": "0.001",
                "min_order_size": "1",
                "neg_risk": true,
                "timestamp": timestamp,
                "hash": "book-222"
            }
        ])
        .to_string()
    }

    fn combo_rfq_books_body(price_a: f64, price_b: f64) -> String {
        combo_rfq_books_body_at(price_a, price_b, Utc::now().timestamp_millis())
    }

    fn watermark_depth_snapshot(
        token_id: &str,
        venue_timestamp_ms: u64,
        book_hash: &str,
    ) -> crate::clob_client::DepthSnapshot {
        crate::clob_client::DepthSnapshot {
            token_id: token_id.into(),
            asks: vec![(0.37, 100.0)],
            tick_size: Some(0.001),
            min_order_size: Some(1.0),
            neg_risk: Some(true),
            observed_at: Some(std::time::Instant::now()),
            venue_timestamp_ms: Some(venue_timestamp_ms),
            book_hash: Some(book_hash.into()),
        }
    }

    #[tokio::test]
    async fn rfq_pre_accept_causal_watermark_requires_live_ws_evidence() {
        let cfg = Config::from_env();
        let token_ids = vec!["111".to_string()];
        let snapshots = HashMap::from([(
            "111".to_string(),
            watermark_depth_snapshot("111", 1_700_000_002_000, "book-111"),
        )]);

        let live_missing_cache = combo_rfq_pre_accept_causal_watermark_blockers(
            &cfg,
            None,
            &token_ids,
            &snapshots,
            Instant::now(),
            true,
        )
        .await;
        assert_eq!(
            live_missing_cache,
            vec!["pre_accept_causal_watermark_missing_price_cache".to_string()]
        );

        let non_live_missing_cache = combo_rfq_pre_accept_causal_watermark_blockers(
            &cfg,
            None,
            &token_ids,
            &snapshots,
            Instant::now(),
            false,
        )
        .await;
        assert!(non_live_missing_cache.is_empty());

        let empty_cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let live_missing_snapshot = combo_rfq_pre_accept_causal_watermark_blockers(
            &cfg,
            Some(&empty_cache),
            &token_ids,
            &snapshots,
            Instant::now(),
            true,
        )
        .await;
        assert_eq!(
            live_missing_snapshot,
            vec!["pre_accept_causal_watermark_missing_ws_snapshot:111".to_string()]
        );
    }

    #[tokio::test]
    async fn rfq_pre_accept_microstructure_computes_microprice_and_queue_imbalance() {
        let token_ids = vec!["111".to_string(), "222".to_string()];
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::from([
            (
                "111".to_string(),
                crate::ws_client::Price {
                    best_bid: Some(0.39),
                    best_ask: Some(0.41),
                    best_bid_size: Some(40.0),
                    best_ask_size: Some(10.0),
                    ..Default::default()
                },
            ),
            (
                "222".to_string(),
                crate::ws_client::Price {
                    best_bid: Some(0.49),
                    best_ask: Some(0.51),
                    best_bid_size: Some(10.0),
                    best_ask_size: Some(30.0),
                    ..Default::default()
                },
            ),
        ])));

        let opp = test_opp(ArbType::Yes);
        let microstructure =
            combo_rfq_pre_accept_microstructure(Some(&cache), &opp, &token_ids).await;

        assert_eq!(microstructure.tokens, 2);
        assert!((microstructure.microprice_mean.unwrap() - 0.4505).abs() < 0.000001);
        assert!((microstructure.queue_imbalance_mean.unwrap() - 0.05).abs() < 0.000001);
        assert!((microstructure.synthetic_price.unwrap() - 0.901).abs() < 0.000001);
    }

    #[tokio::test]
    async fn rfq_pre_accept_microstructure_prefers_multilevel_depth_vamp() {
        let token_ids = vec!["111".to_string(), "222".to_string()];
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::from([
            (
                "111".to_string(),
                crate::ws_client::Price {
                    best_bid: Some(0.39),
                    best_ask: Some(0.41),
                    best_bid_size: Some(40.0),
                    best_ask_size: Some(10.0),
                    ask_depth: vec![(0.41, 10.0), (0.43, 90.0)],
                    bid_depth: vec![(0.39, 40.0), (0.38, 60.0)],
                    ..Default::default()
                },
            ),
            (
                "222".to_string(),
                crate::ws_client::Price {
                    best_bid: Some(0.49),
                    best_ask: Some(0.51),
                    best_bid_size: Some(10.0),
                    best_ask_size: Some(30.0),
                    ask_depth: vec![(0.51, 30.0), (0.52, 70.0)],
                    bid_depth: vec![(0.49, 10.0), (0.48, 90.0)],
                    ..Default::default()
                },
            ),
        ])));

        let opp = test_opp(ArbType::Yes);
        let microstructure =
            combo_rfq_pre_accept_microstructure(Some(&cache), &opp, &token_ids).await;

        assert_eq!(microstructure.tokens, 2);
        assert!((microstructure.microprice_mean.unwrap() - 0.45125).abs() < 0.000001);
        assert!((microstructure.queue_imbalance_mean.unwrap() - 0.0).abs() < 0.000001);
        assert!((microstructure.synthetic_price.unwrap() - 0.9025).abs() < 0.000001);
    }

    #[test]
    fn rfq_markout_race_record_computes_delayed_public_markout() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("markout-race-record");
        let opp = test_opp(ArbType::Yes);
        let quote = ComboRfqQuoteCandidate {
            quote_id: "quote-1".into(),
            rfq_id: Some("rfq-1".into()),
            maker_id: Some("maker-good".into()),
            symbol: Some("combo-a".into()),
            side: Some("SIDE_BUY".into()),
            status: Some("ACTIVE".into()),
            price: 0.75,
            qty_decimal: Some(10.0),
            created_at: None,
            expires_at: None,
            age_ms: Some(10),
            expected_edge_usd: None,
        };
        let markout = ComboRfqPreAcceptMarkoutReport {
            status: "ok".into(),
            blockers: Vec::new(),
            quote_to_accept_ms: Some(20),
            maker_id: Some("maker-good".into()),
            quote_price: 0.75,
            quote_qty_decimal: 10.0,
            quote_cost_usd: 7.5,
            live_cost_buffer_usd: 0.0,
            synthetic_price: 0.75,
            synthetic_cost_usd: 7.5,
            quote_edge_usd: 2.5,
            public_edge_usd: 2.5,
            markout_bps: 0.0,
            toxicity_haircut_bps: 0.0,
            toxicity_haircut_usd: 0.0,
            toxicity_trade_prints: 0,
            toxicity_recent_book_updates: 0,
            ws_microprice_mean: None,
            ws_queue_imbalance_mean: None,
            ws_microstructure_tokens: 0,
            quote_edge_after_toxicity_usd: 2.5,
            public_edge_after_toxicity_usd: 2.5,
            token_ids: vec!["111".into(), "222".into()],
            book_hashes: vec!["book-111".into(), "book-222".into()],
        };
        let snapshots = HashMap::from([
            (
                "111".to_string(),
                crate::clob_client::DepthSnapshot {
                    token_id: "111".into(),
                    asks: vec![(0.37, 100.0)],
                    tick_size: Some(0.001),
                    min_order_size: Some(1.0),
                    neg_risk: Some(true),
                    observed_at: Some(std::time::Instant::now()),
                    venue_timestamp_ms: Some(1_700_000_002_100),
                    book_hash: Some("sample-111".into()),
                },
            ),
            (
                "222".to_string(),
                crate::clob_client::DepthSnapshot {
                    token_id: "222".into(),
                    asks: vec![(0.37, 100.0)],
                    tick_size: Some(0.001),
                    min_order_size: Some(1.0),
                    neg_risk: Some(true),
                    observed_at: Some(std::time::Instant::now()),
                    venue_timestamp_ms: Some(1_700_000_002_100),
                    book_hash: Some("sample-222".into()),
                },
            ),
        ]);

        let record = combo_rfq_markout_race_journal_record(
            &cfg,
            &opp,
            "rfq-1",
            &quote,
            &markout,
            250,
            Some(&snapshots),
            None,
        );

        assert_eq!(record.race_id, "E:rfq-1:quote-1");
        assert_eq!(record.status, "sampled");
        assert_eq!(record.horizon_ms, 250);
        assert_eq!(record.sampled_synthetic_price, Some(0.74));
        assert!(record.sampled_markout_bps.unwrap() > 100.0);
        assert_eq!(record.sampled_book_hashes, vec!["sample-111", "sample-222"]);
        let path = append_combo_rfq_markout_race_journal_record(&cfg, &record).unwrap();
        let journal = std::fs::read_to_string(path).unwrap();
        let parsed: ComboRfqMarkoutRaceJournalRecord =
            serde_json::from_str(journal.lines().next().unwrap()).unwrap();
        assert_eq!(parsed, record);
    }

    #[tokio::test]
    async fn rfq_pre_accept_causal_watermark_blocks_same_timestamp_hash_mismatch() {
        let cfg = Config::from_env();
        let token_ids = vec!["111".to_string()];
        let snapshots = HashMap::from([(
            "111".to_string(),
            watermark_depth_snapshot("111", 1_700_000_002_000, "book-rest"),
        )]);
        let ws_price = crate::ws_client::Price {
            venue_timestamp_ms: Some(1_700_000_002_000),
            book_hash: Some("book-ws".into()),
            last_updated: Instant::now() - Duration::from_secs(1),
            ..Default::default()
        };
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::from([(
            "111".to_string(),
            ws_price,
        )])));

        let blockers = combo_rfq_pre_accept_causal_watermark_blockers(
            &cfg,
            Some(&cache),
            &token_ids,
            &snapshots,
            Instant::now(),
            true,
        )
        .await;

        assert!(blockers.iter().any(|blocker| blocker
            .starts_with("pre_accept_causal_watermark_same_timestamp_book_hash_mismatch:111")));
    }

    #[tokio::test]
    async fn rfq_pre_accept_causal_watermark_blocks_rest_newer_than_ws() {
        let mut cfg = Config::from_env();
        cfg.live_max_refresh_to_submit_ms = 100;
        cfg.ws_quote_max_age_ms = 10;
        let token_ids = vec!["111".to_string()];
        let snapshots = HashMap::from([(
            "111".to_string(),
            watermark_depth_snapshot("111", 1_700_000_002_000, "book-rest"),
        )]);
        let ws_price = crate::ws_client::Price {
            venue_timestamp_ms: Some(1_700_000_001_800),
            book_hash: Some("book-ws".into()),
            snapshot_ready: true,
            last_updated: Instant::now() - Duration::from_secs(1),
            ..Default::default()
        };
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::from([(
            "111".to_string(),
            ws_price,
        )])));

        let blockers = combo_rfq_pre_accept_causal_watermark_blockers(
            &cfg,
            Some(&cache),
            &token_ids,
            &snapshots,
            Instant::now(),
            true,
        )
        .await;

        assert!(blockers
            .iter()
            .any(|blocker| blocker.starts_with("pre_accept_causal_watermark_ws_lagging_rest:111")));
        assert!(blockers.iter().any(|blocker| {
            blocker.starts_with("pre_accept_causal_watermark_rest_newer_book_hash_mismatch:111")
        }));
    }

    async fn mock_combo_rfq_books<'a>(
        server: &'a MockServer,
        price_a: f64,
        price_b: f64,
    ) -> Mock<'a> {
        let body = combo_rfq_books_body(price_a, price_b);
        server
            .mock_async(move |when, then| {
                when.method(POST).path("/books");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(body.clone());
            })
            .await
    }

    async fn mock_combo_rfq_prices<'a>(
        server: &'a MockServer,
        price_a: f64,
        price_b: f64,
    ) -> Mock<'a> {
        let body = serde_json::json!({
            "111": {"SELL": format_decimal(price_a)},
            "222": {"SELL": format_decimal(price_b)}
        })
        .to_string();
        server
            .mock_async(move |when, then| {
                when.method(POST).path("/prices");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(body.clone());
            })
            .await
    }

    async fn mock_combo_rfq_orderable_markets<'a>(server: &'a MockServer) -> Vec<Mock<'a>> {
        let mut mocks = Vec::new();
        for (condition_id, yes_token_id, no_token_id) in
            [("cond-a", "111", "112"), ("cond-b", "222", "223")]
        {
            let body = serde_json::json!({
                "c": condition_id,
                "t": [
                    {"t": yes_token_id, "o": "Yes"},
                    {"t": no_token_id, "o": "No"}
                ],
                "mts": 0.01,
                "mos": 1,
                "fd": {"r": 0.0, "e": 1, "to": true},
                "nr": true,
                "rfqe": true,
                "ao": true,
                "active": true,
                "archived": false,
                "closed": false,
                "enable_order_book": true,
                "sd": 0,
                "oas": 0,
                "gst": null
            })
            .to_string();
            mocks.push(
                server
                    .mock_async(move |when, then| {
                        when.method(GET)
                            .path(format!("/clob-markets/{condition_id}"));
                        then.status(200)
                            .header("content-type", "application/json")
                            .body(body.clone());
                    })
                    .await,
            );
        }
        mocks
    }

    #[test]
    fn route_report_marks_multi_condition_combo_candidates() {
        let report = catalog().route_report(&test_opp(ArbType::Yes));

        assert_eq!(report.route, AtomicRouteHint::ComboRfqCandidate);
        assert_eq!(report.combo_conditions, 2);
        assert_eq!(report.token_position_matches, 2);
        assert!(report.note().contains("atomic_route=combo_rfq_candidate"));
        assert!(report
            .note()
            .contains("combo_rfq_requester_execution=beta_accept_endpoint_documented"));
        assert!(report
            .note()
            .contains("combo_rfq_requester_api_public=false"));
        assert!(report.note().contains("rfq_quote_window_ms=400"));
        assert!(report.note().contains("rfq_accept_window_ms=5000"));
        assert!(report.note().contains("rfq_last_look_ms=1000"));
    }

    #[test]
    fn route_report_blocks_combo_candidate_when_token_outcome_mapping_mismatches() {
        let mut opp = test_opp(ArbType::Yes);
        opp.execution_plan[0].token_id = "112".into();

        let report = catalog().route_report(&opp);

        assert_eq!(report.route, AtomicRouteHint::None);
        assert_eq!(report.combo_conditions, 2);
        assert_eq!(report.token_position_matches, 1);
        assert_eq!(
            report.reason,
            "one_or_more_planned_tokens_do_not_match_combo_catalog_outcome_positions"
        );
    }

    #[test]
    fn route_report_blocks_combo_candidate_when_clob_rfq_disabled() {
        let mut opp = test_opp(ArbType::Yes);
        opp.markets[0].clob_rfq_enabled = Some(false);

        let report = catalog().route_report(&opp);

        assert_eq!(report.route, AtomicRouteHint::None);
        assert_eq!(report.reason, "clob_market_rfq_disabled:cond-a");
    }

    #[test]
    fn requester_plan_blocks_combo_candidate_when_token_outcome_mapping_mismatches() {
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        let mut opp = test_opp(ArbType::Yes);
        opp.execution_plan[0].token_id = "112".into();

        let plan = build_combo_rfq_requester_plan(&cfg, &catalog(), &opp);

        assert_eq!(plan.status, "blocked");
        assert!(plan.request.is_none());
        assert!(plan.blockers.iter().any(|blocker| {
            blocker.contains(
                "not_combo_rfq_candidate:one_or_more_planned_tokens_do_not_match_combo_catalog_outcome_positions",
            )
        }));
    }

    #[test]
    fn route_report_blocks_ambiguous_combo_catalog_schema() {
        let ambiguous_catalog = ComboMarketCatalog::from_markets(vec![
            ComboMarketEntry {
                condition_id: "cond-a".into(),
                position_ids: vec!["111".into(), "111".into()],
                outcomes: vec!["Yes".into(), "No".into()],
                slug: "a".into(),
            },
            ComboMarketEntry {
                condition_id: "cond-b".into(),
                position_ids: vec!["222".into(), "223".into()],
                outcomes: vec!["Yes".into(), "No".into()],
                slug: "b".into(),
            },
        ]);

        let report = ambiguous_catalog.route_report(&test_opp(ArbType::Yes));

        assert_eq!(report.route, AtomicRouteHint::None);
        assert_eq!(report.reason, "combo_catalog_duplicate_yes_no_position_id");
    }

    #[test]
    fn rfq_requester_config_defaults_block_live_requests() {
        let cfg = Config::from_env();

        let report = combo_rfq_requester_config_report(&cfg);

        assert_eq!(report.status, "blocked");
        assert!(!report.enabled);
        assert!(!report.bearer_token_present);
        assert!(!report.participant_id_present);
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_REQUESTER_ENABLED=false".to_string()));
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_BEARER_TOKEN_empty".to_string()));
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_PARTICIPANT_ID_empty".to_string()));
    }

    #[test]
    fn rfq_requester_plan_builds_ready_no_submit_request_when_beta_configured() {
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 12.5;

        let plan = build_combo_rfq_requester_plan(&cfg, &catalog(), &test_opp(ArbType::Yes));

        assert_eq!(plan.status, "ready_no_submit");
        assert!(plan.blockers.is_empty());
        assert!(plan.note.contains("combo_rfq_requester=ready_no_submit"));
        let request = plan.request.expect("request preview");
        assert_eq!(request.cash_order_qty.as_deref(), Some("12.5"));
        assert_eq!(request.side, "SIDE_BUY");
        assert_eq!(request.legs.len(), 2);
        assert_eq!(request.legs[0].symbol, "a");
        assert_eq!(request.legs[0].side, "SIDE_BUY");
        assert_eq!(request.legs[1].symbol, "b");
        assert_eq!(request.legs[1].side, "SIDE_BUY");
        assert!(request.client_request_id.starts_with("scanner-"));
        assert!(
            request.client_request_id.len()
                > combo_rfq_client_request_base_id(&test_opp(ArbType::Yes)).len()
        );
    }

    #[test]
    fn rfq_client_request_ids_are_unique_with_stable_recovery_scope() {
        let opp = test_opp(ArbType::Yes);
        let first = combo_rfq_client_request_id(&opp);
        let second = combo_rfq_client_request_id(&opp);
        let base = combo_rfq_client_request_base_id(&opp);

        assert_ne!(first, second);
        assert!(first.starts_with(&base));
        assert!(second.starts_with(&base));
        assert_eq!(combo_rfq_client_request_recovery_scope(&first), base);
        assert_eq!(combo_rfq_client_request_recovery_scope(&second), base);
        assert!(combo_rfq_client_request_recovery_scopes_match(
            &first, &second
        ));
    }

    #[test]
    fn rfq_best_execution_blocks_without_quote_response() {
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();

        let report = build_combo_rfq_best_execution_report(&cfg, &test_opp(ArbType::Yes), None);

        assert_eq!(report.status, "blocked_no_quote");
        assert!(!report.accept_gate_pass);
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_ACCEPT_ENABLED=false".to_string()));
        assert!(report
            .blockers
            .contains(&"missing_quote_response".to_string()));
    }

    #[test]
    fn rfq_best_execution_selects_fresh_profitable_quote_but_blocks_accept_by_default() {
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        let quote_response = serde_json::json!({
            "quotes": [
                {
                    "quoteId": "stale",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.70",
                    "qtyDecimal": "10",
                    "status": "ACTIVE",
                    "ageMs": 5000,
                    "expiresAt": (chrono::Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                },
                {
                    "quoteId": "best",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.75",
                    "qtyDecimal": "10",
                    "status": "ACTIVE",
                    "ageMs": 10,
                    "expiresAt": (chrono::Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                }
            ]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "quote_selected_no_accept");
        assert_eq!(report.quotes_seen, 2);
        assert_eq!(report.quotes_eligible, 1);
        assert_eq!(
            report
                .selected_quote
                .as_ref()
                .map(|quote| quote.quote_id.as_str()),
            Some("best")
        );
        assert!(report.edge_gate_pass);
        assert!(!report.accept_gate_pass);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("quote_stale:stale_quote")));
    }

    #[test]
    fn rfq_best_execution_accept_gate_passes_only_when_explicitly_enabled() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("accept-good-maker");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.live_max_refresh_to_submit_ms = 2_500;
        cfg.min_net_profit_usd = 1.0;
        append_accept_ready_maker_samples(&mut cfg, "maker-good");
        let quote_response = serde_json::json!({
            "quotes": [{
                "quoteId": "best",
                "makerId": "maker-good",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": "0.75",
                "qtyDecimal": "10",
                "status": "ACTIVE",
                "ageMs": 10,
                "expiresAt": (chrono::Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
            }]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "ready_to_accept");
        assert!(report.edge_gate_pass);
        assert!(report.last_look_gate_pass);
        assert!(report.accept_gate_pass);
        assert!(report.blockers.is_empty());
    }

    #[test]
    fn rfq_best_execution_blocks_toxic_markout_race_maker() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("toxic-markout-race-maker");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.combo_rfq_markout_race_min_samples = 3;
        cfg.combo_rfq_markout_race_max_adverse_bps = 1.0;
        append_accept_ready_maker_samples(&mut cfg, "maker-toxic");
        append_markout_race_samples(&cfg, "maker-toxic", &[2.0, 2.5, 3.0]);
        let quote_response = serde_json::json!({
            "quotes": [{
                "quoteId": "toxic-now",
                "makerId": "maker-toxic",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": "0.75",
                "qtyDecimal": "10",
                "status": "ACTIVE",
                "ageMs": 10,
                "expiresAt": (chrono::Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
            }]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "blocked_no_eligible_quote");
        assert_eq!(report.quotes_eligible, 0);
        assert!(!report.accept_gate_pass);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| { blocker.contains("maker_score_failed:maker-toxic:avg_markout_bps") }));
        let maker = report
            .maker_scorecard
            .makers
            .iter()
            .find(|maker| maker.maker_id == "maker-toxic")
            .unwrap();
        assert_eq!(maker.status, "blocked");
        assert_eq!(maker.markout_samples, 3);
        assert_eq!(maker.adverse_markout_samples, 3);
        assert!(maker.avg_markout_bps.unwrap() > cfg.combo_rfq_markout_race_max_adverse_bps);
    }

    #[test]
    fn rfq_best_execution_blocks_good_maker_without_settlement_monitor() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("accept-good-maker-no-settlement-monitor");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        append_passing_maker_samples(&cfg, "maker-good");
        let quote_response = serde_json::json!({
            "quotes": [{
                "quoteId": "best",
                "makerId": "maker-good",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": "0.75",
                "qtyDecimal": "10",
                "status": "ACTIVE",
                "ageMs": 10,
                "expiresAt": (chrono::Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
            }]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "blocked_no_eligible_quote");
        assert!(!report.accept_gate_pass);
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker
                    .contains("settlement_counterparty_monitor_disabled:maker-good"))
        );
    }

    #[test]
    fn rfq_best_execution_skips_isolated_quote_dispersion_outlier() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("dispersion-outlier");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        append_accept_ready_maker_samples(&mut cfg, "maker-outlier");
        append_accept_ready_maker_samples(&mut cfg, "maker-second");
        append_accept_ready_maker_samples(&mut cfg, "maker-third");
        let expires_at = (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339();
        let quote_response = serde_json::json!({
            "quotes": [
                {
                    "quoteId": "too-good",
                    "makerId": "maker-outlier",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.65",
                    "qtyDecimal": "10",
                    "status": "ACTIVE",
                    "ageMs": 10,
                    "expiresAt": expires_at
                },
                {
                    "quoteId": "second",
                    "makerId": "maker-second",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.74",
                    "qtyDecimal": "10",
                    "status": "ACTIVE",
                    "ageMs": 10,
                    "expiresAt": expires_at
                },
                {
                    "quoteId": "third",
                    "makerId": "maker-third",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.75",
                    "qtyDecimal": "10",
                    "status": "ACTIVE",
                    "ageMs": 10,
                    "expiresAt": expires_at
                }
            ]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "ready_to_accept");
        assert_eq!(
            report
                .selected_quote
                .as_ref()
                .map(|quote| quote.quote_id.as_str()),
            Some("second")
        );
        assert_eq!(report.quotes_eligible, 2);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("quote_too-good:quote_dispersion_outlier")));
    }

    #[test]
    fn rfq_best_execution_dispersion_gate_does_not_exempt_strong_historical_maker() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("dispersion-no-maker-exemption");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        for _ in 0..5 {
            append_accept_ready_maker_samples(&mut cfg, "maker-outlier");
        }
        append_accept_ready_maker_samples(&mut cfg, "maker-second");
        append_accept_ready_maker_samples(&mut cfg, "maker-third");
        let expires_at = (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339();
        let quote_response = serde_json::json!({
            "quotes": [
                {
                    "quoteId": "too-good",
                    "makerId": "maker-outlier",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.65",
                    "qtyDecimal": "10",
                    "status": "ACTIVE",
                    "ageMs": 10,
                    "expiresAt": expires_at
                },
                {
                    "quoteId": "second",
                    "makerId": "maker-second",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.74",
                    "qtyDecimal": "10",
                    "status": "ACTIVE",
                    "ageMs": 10,
                    "expiresAt": expires_at
                },
                {
                    "quoteId": "third",
                    "makerId": "maker-third",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.75",
                    "qtyDecimal": "10",
                    "status": "ACTIVE",
                    "ageMs": 10,
                    "expiresAt": expires_at
                }
            ]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(
            report
                .selected_quote
                .as_ref()
                .map(|quote| quote.quote_id.as_str()),
            Some("second")
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("quote_too-good:quote_dispersion_outlier")));
    }

    #[test]
    fn rfq_best_execution_requires_last_look_edge_margin_before_accept() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("last-look-margin");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.live_edge_haircut_usd = 0.01;
        cfg.live_edge_haircut_bps = 5;
        cfg.live_slippage_bps = 10;
        cfg.min_net_profit_usd = 1.0;
        append_accept_ready_maker_samples(&mut cfg, "maker-good");
        let quote_response = serde_json::json!({
            "quotes": [{
                "quoteId": "marginal",
                "makerId": "maker-good",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": "0.961",
                "qtyDecimal": "10",
                "status": "ACTIVE",
                "ageMs": 10,
                "expiresAt": (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
            }]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "blocked_no_eligible_quote");
        assert!(!report.accept_gate_pass);
        assert_eq!(report.quotes_eligible, 0);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("last_look_edge_after_haircut_below_min")));
    }

    #[test]
    fn rfq_best_execution_rejects_prefixed_terminal_quote_status() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("prefixed-terminal-status");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        append_accept_ready_maker_samples(&mut cfg, "maker-good");
        let quote_response = serde_json::json!({
            "quotes": [{
                "quoteId": "done-away",
                "makerId": "maker-good",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": "0.75",
                "qtyDecimal": "10",
                "status": "QUOTE_STATUS_DONE_AWAY",
                "ageMs": 10,
                "expiresAt": (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
            }]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "blocked_no_eligible_quote");
        assert_eq!(report.quotes_eligible, 0);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("terminal_status:QUOTE_STATUS_DONE_AWAY")));
    }

    #[test]
    fn rfq_best_execution_blocks_accept_for_unknown_maker() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("unknown-maker");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        let quote_response = serde_json::json!({
            "quotes": [{
                "quoteId": "unknown-now",
                "makerId": "maker-new",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": "0.75",
                "qtyDecimal": "10",
                "status": "ACTIVE",
                "ageMs": 10,
                "expiresAt": (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
            }]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "blocked_no_eligible_quote");
        assert!(!report.accept_gate_pass);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("maker_score_missing:maker-new")));
    }

    #[test]
    fn rfq_best_execution_blocks_quote_from_bad_maker_scorecard() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("bad-maker");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        for idx in 0..COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES {
            append_combo_rfq_maker_journal_record(
                &cfg,
                &ComboRfqMakerJournalRecord {
                    generated_at: format!("2026-01-01T00:00:0{idx}Z"),
                    maker_id: Some("maker-bad".into()),
                    quote_id: format!("old-quote-{idx}"),
                    rfq_id: Some(format!("old-rfq-{idx}")),
                    event_id: "old-event".into(),
                    quote_age_ms: Some(10),
                    expected_edge_usd: Some(2.0),
                    selected: true,
                    accepted: false,
                    terminal_status: Some("last_look_rejected".into()),
                    realized_ev_usd: Some(-0.25),
                    blockers: Vec::new(),
                    notes: Vec::new(),
                },
            )
            .unwrap();
        }
        let quote_response = serde_json::json!({
            "quotes": [{
                "quoteId": "bad-now",
                "makerId": "maker-bad",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": "0.75",
                "qtyDecimal": "10",
                "status": "ACTIVE",
                "ageMs": 10,
                "expiresAt": (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
            }]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "blocked_no_eligible_quote");
        assert_eq!(report.quotes_eligible, 0);
        assert!(report.selected_quote.is_none());
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("maker_score_failed:maker-bad")));
        let maker = report
            .maker_scorecard
            .makers
            .iter()
            .find(|maker| maker.maker_id == "maker-bad")
            .unwrap();
        assert_eq!(maker.status, "blocked");
        assert_eq!(maker.rejects, COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES);
    }

    #[test]
    fn rfq_best_execution_blocks_quote_from_recent_failed_settlement_counterparty() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("counterparty-settlement-risk");
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.settlement_monitor_enabled = true;
        append_passing_maker_samples(&cfg, "maker-risky");
        fs::create_dir_all(&cfg.diagnostics_dir).unwrap();
        let now = Utc::now().to_rfc3339();
        fs::write(
            cfg.diagnostics_dir
                .join(crate::settlement_monitor::SETTLEMENT_RECEIPTS_FILE),
            format!(
                r#"{{"generatedAt":"{now}","transactionHash":"0xrisk","makerId":"maker-risky","status":"reverted","revertReason":"ghost fill reverted"}}"#
            ),
        )
        .unwrap();
        let quote_response = serde_json::json!({
            "quotes": [{
                "quoteId": "risky-now",
                "makerId": "maker-risky",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": "0.75",
                "qtyDecimal": "10",
                "status": "ACTIVE",
                "ageMs": 10,
                "expiresAt": (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
            }]
        });

        let report = build_combo_rfq_best_execution_report(
            &cfg,
            &test_opp(ArbType::Yes),
            Some(&quote_response),
        );

        assert_eq!(report.status, "blocked_no_eligible_quote");
        assert_eq!(report.quotes_eligible, 0);
        assert!(report.blockers.iter().any(|blocker| blocker.contains(
            "settlement_counterparty_failed_recent:maker-risky:1:generic_revert=1:ghost fill reverted"
        )));
    }

    #[test]
    fn route_report_does_not_label_standard_full_set_bundle_as_combo() {
        let mut opp = test_opp(ArbType::Bundle);
        opp.execution_plan = vec![
            leg(0, "cond-a", "111", OutcomeSide::Yes),
            leg(0, "cond-a", "112", OutcomeSide::No),
        ];

        let report = catalog().route_report(&opp);

        assert_eq!(report.route, AtomicRouteHint::None);
        assert!(report.reason.contains("ctf_adapter"));
    }

    #[tokio::test]
    async fn rfq_requester_rest_calls_send_auth_headers_and_body() {
        let server = MockServer::start_async().await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/v1/combos/rfqs")
                    .header("authorization", "Bearer token")
                    .header("x-participant-id", "participant")
                    .body_includes(r#""cashOrderQty":"12.5""#)
                    .body_includes(r#""clientRequestId":"client-1""#);
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quotes = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE")
                    .header("authorization", "Bearer token")
                    .header("x-participant-id", "participant");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"quotes":[{"quoteId":"quote-1"}]}"#);
            })
            .await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept")
                    .header("authorization", "Bearer token")
                    .header("x-participant-id", "participant")
                    .body_includes(r#""price":"0.99""#);
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-1","price":"0.99","qtyDecimal":"1"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.api_timeout_secs = 2;
        let client = Client::new();
        let request = ComboRfqCreateRequest {
            qty_decimal: None,
            cash_order_qty: Some("12.5".into()),
            legs: vec![ComboRfqLegRequest {
                symbol: "a".into(),
                side: "SIDE_BUY".into(),
            }],
            side: "SIDE_BUY".into(),
            client_request_id: "client-1".into(),
            expiration_time: "2026-01-01T00:00:00Z".into(),
        };
        let accept_request = ComboRfqAcceptQuoteRequest {
            side: "SIDE_BUY".into(),
            price: "0.99".into(),
            symbol: "combo-a".into(),
            qty_decimal: "1".into(),
        };

        let created = create_combo_rfq(&client, &cfg, &request).await.unwrap();
        let queried = query_combo_rfq_quotes(&client, &cfg, "rfq-1", Some("ACTIVE"))
            .await
            .unwrap();
        let accepted = accept_combo_rfq_quote(&client, &cfg, "rfq-1", "quote-1", &accept_request)
            .await
            .unwrap();

        assert_eq!(created["rfqId"], "rfq-1");
        assert_eq!(queried["quotes"][0]["quoteId"], "quote-1");
        assert_eq!(accepted["status"], "accepted");
        create.assert_calls_async(1).await;
        quotes.assert_calls_async(1).await;
        accept.assert_calls_async(1).await;
    }

    #[test]
    fn rfq_retry_policy_limits_write_retries_to_rate_limit_responses() {
        assert!(combo_rfq_should_retry_status(
            ComboRfqRetryPolicy::WriteRateLimitOnly,
            reqwest::StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(!combo_rfq_should_retry_status(
            ComboRfqRetryPolicy::WriteRateLimitOnly,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(combo_rfq_should_retry_status(
            ComboRfqRetryPolicy::ReadOnlyPreserveWriteCapacity,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(!combo_rfq_should_retry_status(
            ComboRfqRetryPolicy::ReadOnlyPreserveWriteCapacity,
            reqwest::StatusCode::TOO_MANY_REQUESTS
        ));
        let mut cfg = Config::from_env();
        cfg.retry_backoff_base_ms = 100;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "2".parse().unwrap());
        assert_eq!(combo_rfq_retry_wait_ms(&cfg, Some(&headers), 1), 2_000);
        assert_eq!(combo_rfq_retry_wait_ms(&cfg, None, 3), 400);
    }

    #[test]
    fn rfq_retry_wait_respects_live_deadline() {
        let mut cfg = Config::from_env();
        cfg.retry_backoff_base_ms = 200;
        cfg.live_max_refresh_to_submit_ms = 100;
        let deadline = ComboRfqLiveRequestDeadline {
            started_at: Instant::now() - Duration::from_millis(10),
            max_ms: cfg.live_max_refresh_to_submit_ms,
        };

        let err = combo_rfq_retry_wait_ms_with_deadline(
            &cfg,
            None,
            1,
            Some(deadline),
            "Combo/RFQ quote query",
        )
        .unwrap_err();

        assert!(err.to_string().contains("retry wait exceeds"));
        assert!(err.to_string().contains("Combo/RFQ quote query"));
    }

    #[tokio::test]
    async fn rfq_quote_query_rejects_slow_live_response() {
        let server = MockServer::start_async().await;
        let _quotes = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .delay(Duration::from_millis(25))
                    .header("content-type", "application/json")
                    .body(r#"{"quotes":[]}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.api_timeout_secs = 2;
        cfg.live_max_refresh_to_submit_ms = 1;
        cfg.max_retries = 1;

        let err = query_combo_rfq_quotes(&Client::new(), &cfg, "rfq-1", Some("ACTIVE"))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("timed out"));
        assert!(err.to_string().contains("live freshness budget"));
    }

    #[tokio::test]
    async fn rfq_quote_query_rate_limit_preserves_write_capacity() {
        let server = MockServer::start_async().await;
        let quotes = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/rate-limit-fixture/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(429)
                    .header("retry-after", "2")
                    .header("content-type", "application/json")
                    .body(r#"{"error":"rate limited"}"#);
            })
            .await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/rate-limit-fixture/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-1","price":"0.75","qtyDecimal":"10"}"#);
            })
            .await;
        let unaffected_quotes = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/unaffected-fixture/v1/combos/quotes")
                    .query_param("rfqId", "rfq-2")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"quotes":[]}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = format!("{}/rate-limit-fixture", server.base_url());
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.max_retries = 3;
        cfg.retry_backoff_base_ms = 1;

        let err = query_combo_rfq_quotes(&Client::new(), &cfg, "rfq-1", Some("ACTIVE"))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("preserving Combo/RFQ write capacity"));
        quotes.assert_calls_async(1).await;

        let err = query_combo_rfq_quotes(&Client::new(), &cfg, "rfq-1", Some("ACTIVE"))
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("skipped while Combo/RFQ read endpoint is rate-limited"));
        quotes.assert_calls_async(1).await;

        let accepted = accept_combo_rfq_quote(
            &Client::new(),
            &cfg,
            "rfq-1",
            "quote-1",
            &ComboRfqAcceptQuoteRequest {
                side: "SIDE_BUY".into(),
                price: "0.75".into(),
                symbol: "combo-a".into(),
                qty_decimal: "10".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(accepted["status"], "accepted");
        accept.assert_calls_async(1).await;

        let mut unaffected_cfg = cfg.clone();
        unaffected_cfg.combo_rfq_requester_api_url =
            format!("{}/unaffected-fixture", server.base_url());
        let unaffected =
            query_combo_rfq_quotes(&Client::new(), &unaffected_cfg, "rfq-2", Some("ACTIVE"))
                .await
                .expect("rate limit for one API endpoint must not poison another endpoint");
        assert_eq!(unaffected["quotes"], serde_json::json!([]));
        unaffected_quotes.assert_calls_async(1).await;

        let removed = combo_rfq_read_rate_limits()
            .lock()
            .expect("rate-limit cache lock")
            .remove(&combo_rfq_read_rate_limit_key(&cfg));
        assert!(removed.is_some());
    }

    #[tokio::test]
    async fn rfq_state_machine_stops_before_side_effects_when_accept_disabled() {
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_accept_disabled");
        assert!(report.request.is_some());
        assert!(report.rfq_id.is_none());
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_ACCEPT_ENABLED=false".to_string()));
        assert_eq!(report.steps.len(), 1);
        assert_eq!(report.steps[0].stage, "preflight");
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_delay_metadata_before_create() {
        let server = MockServer::start_async().await;
        let delayed = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-a");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"cond-a","t":[{"t":"111","o":"Yes"},{"t":"112","o":"No"}],"mts":0.01,"mos":1,"fd":{"r":0.0,"e":1,"to":true},"nr":true,"rfqe":true,"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0.5,"oas":0,"gst":null}"#);
            })
            .await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.max_retries = 1;
        cfg.diagnostics_dir = temp_rfq_dir("delay-window-firewall");

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_delay_window_firewall");
        assert!(report.rfq_id.is_none());
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_rfq_delay_window_firewall")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("seconds_delay=0.5")));
        assert!(report
            .steps
            .iter()
            .any(|step| step.stage == "delay_window_firewall" && step.status == "blocked"));
        delayed.assert_calls_async(1).await;
        create.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_rfq_disabled_markets_before_create() {
        let server = MockServer::start_async().await;
        let orderable = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-a");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"cond-a","t":[{"t":"111","o":"Yes"},{"t":"112","o":"No"}],"mts":0.01,"mos":1,"fd":{"r":0.0,"e":1,"to":true},"nr":true,"rfqe":false,"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0,"gst":null}"#);
            })
            .await;
        let other_orderable = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-b");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"cond-b","t":[{"t":"222","o":"Yes"},{"t":"223","o":"No"}],"mts":0.01,"mos":1,"fd":{"r":0.0,"e":1,"to":true},"nr":true,"rfqe":true,"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0,"gst":null}"#);
            })
            .await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.max_retries = 1;
        cfg.diagnostics_dir = temp_rfq_dir("rfq-enabled-firewall");

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_rfq_enabled_firewall");
        assert!(report.rfq_id.is_none());
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_rfq_rfq_enabled_firewall")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("rfqe=false")));
        assert!(report
            .steps
            .iter()
            .any(|step| step.stage == "rfq_enabled_firewall" && step.status == "blocked"));
        orderable.assert_calls_async(1).await;
        other_orderable.assert_calls_async(0).await;
        create.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_creates_selects_accepts_and_returns_pending_finality() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (chrono::Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.375, 0.375).await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept")
                    .body_includes(r#""symbol":"combo-a""#)
                    .body_includes(r#""qtyDecimal":"33.333333""#);
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-1","price":"0.75","qtyDecimal":"33.333333"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("state-machine-journal");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "accepted_pending_finality");
        assert_eq!(report.accept_outcome, Some(ComboRfqAcceptOutcome::Accepted));
        assert_eq!(report.rfq_id.as_deref(), Some("rfq-1"));
        assert_eq!(
            report
                .best_execution
                .selected_quote
                .as_ref()
                .map(|quote| quote.quote_id.as_str()),
            Some("quote-1")
        );
        assert!(report.accept_request.is_some());
        assert_eq!(
            report
                .pre_accept_markout
                .as_ref()
                .map(|report| report.status.as_str()),
            Some("ok")
        );
        assert_eq!(
            report.accept_response.as_ref().unwrap()["status"],
            "accepted"
        );
        assert!(report
            .blockers
            .contains(&"rfq_finality_stream_not_verified".to_string()));
        let stages = report
            .steps
            .iter()
            .map(|step| step.stage.as_str())
            .collect::<Vec<_>>();
        assert!(stages.contains(&"create_rfq"));
        assert!(stages.contains(&"query_quotes"));
        assert!(stages.contains(&"accept_quote"));
        assert!(stages.contains(&"journal_execution"));
        assert!(stages.contains(&"journal_adverse_selection"));
        assert!(stages.contains(&"journal_maker"));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("create_intent"));
        assert!(execution_journal.contains("request_created"));
        assert!(execution_journal.contains("accept_intent"));
        assert!(execution_journal.contains("accepted_pending_finality"));
        let adverse_journal = std::fs::read_to_string(
            cfg.diagnostics_dir
                .join(COMBO_RFQ_ADVERSE_SELECTION_JOURNAL_FILE),
        )
        .unwrap();
        let adverse_record: ComboRfqAdverseSelectionJournalRecord =
            serde_json::from_str(adverse_journal.lines().next().unwrap()).unwrap();
        assert_eq!(adverse_record.event_id, "E");
        assert_eq!(adverse_record.rfq_id, "rfq-1");
        assert_eq!(adverse_record.quote_id, "quote-1");
        assert_eq!(adverse_record.status, "ok");
        assert_eq!(adverse_record.markout_bps, 0.0);
        assert_eq!(adverse_record.toxicity_trade_prints, 0);
        let scorecard = build_combo_rfq_maker_scorecard(&cfg).unwrap();
        assert_eq!(
            scorecard.records_seen,
            COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES + 1
        );
        assert_eq!(scorecard.maker_count, 1);
        assert_eq!(scorecard.makers[0].maker_id, "maker-good");
        assert_eq!(
            scorecard.makers[0].successes,
            COMBO_RFQ_MAKER_MIN_TERMINAL_SAMPLES
        );
        assert_eq!(scorecard.makers[0].pending, 1);
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_uses_stream_journal_quote_before_polling_rest() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET).path("/v1/combos/quotes");
                then.status(500);
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.375, 0.375).await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-stream/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-stream","price":"0.75","qtyDecimal":"33.333333"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("stream-journal-quotes");
        fs::create_dir_all(&cfg.diagnostics_dir).unwrap();
        append_accept_ready_maker_samples(&mut cfg, "maker-good");
        fs::write(
            cfg.diagnostics_dir
                .join(crate::rfq_finality::COMBO_RFQ_FINALITY_EVENTS_FILE),
            format!(
                "{}\n",
                serde_json::json!({
                    "id": "stream-quote-1",
                    "rfqId": "rfq-1",
                    "quoteId": "quote-stream",
                    "makerId": "maker-good",
                    "symbol": "combo-a",
                    "side": "SIDE_BUY",
                    "price": "0.75",
                    "qtyDecimal": "33.333333",
                    "status": "ACTIVE",
                    "ageMs": 10,
                    "generatedAt": Utc::now().to_rfc3339(),
                    "expiresAt": (Utc::now() + ChronoDuration::seconds(5)).to_rfc3339(),
                    "source": "combo_rfq_stream_journal"
                })
            ),
        )
        .unwrap();

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "accepted_pending_finality");
        assert_eq!(
            report
                .best_execution
                .selected_quote
                .as_ref()
                .map(|quote| quote.quote_id.as_str()),
            Some("quote-stream")
        );
        assert!(report.steps.iter().any(|step| {
            step.stage == "query_quotes" && step.detail.contains("source=stream_journal")
        }));
        assert!(report.steps.iter().any(|step| {
            step.stage == "collect_quotes" && step.detail.contains("status=stream_quote_ready")
        }));
        create.assert_calls_async(1).await;
        quote.assert_calls_async(0).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_ingests_stream_finality_after_accept() {
        crate::rfq_finality::clear_combo_rfq_stream_event_cache_for_tests();
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (chrono::Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.375, 0.375).await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-1","price":"0.75","qtyDecimal":"33.333333"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_stream_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("state-machine-finality-ingest");
        fs::create_dir_all(&cfg.diagnostics_dir).unwrap();
        append_accept_ready_maker_samples(&mut cfg, "maker-good");
        let finality_event = serde_json::json!({
            "id": "evt-filled",
            "timestamp": Utc::now().to_rfc3339(),
            "source": "dropcopy",
            "rfqId": "rfq-1",
            "quoteId": "quote-1",
            "makerId": "maker-good",
            "marketEventId": "event-1",
            "status": "filled",
            "expectedEdgeUsd": 2.5,
            "realizedEvUsd": 2.1,
            "transactionHash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        });
        fs::write(
            cfg.diagnostics_dir
                .join(crate::rfq_finality::COMBO_RFQ_FINALITY_EVENTS_FILE),
            format!("{finality_event}\n"),
        )
        .unwrap();
        crate::rfq_finality::cache_combo_rfq_stream_event(&finality_event);

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "accepted_pending_finality");
        assert!(report
            .steps
            .iter()
            .any(|step| { step.stage == "post_accept_finality" && step.status == "ingested" }));
        let finality_journal = fs::read_to_string(
            cfg.diagnostics_dir
                .join(crate::rfq_finality::COMBO_RFQ_FINALITY_JOURNAL_FILE),
        )
        .unwrap();
        assert!(finality_journal.contains("evt-filled"));
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(1).await;
    }

    #[test]
    fn rfq_quote_lookup_uses_hot_stream_cache_before_journal_file() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("stream-cache-quotes");
        crate::rfq_finality::cache_combo_rfq_stream_event(&serde_json::json!({
            "id": "stream-cache-quote-1",
            "rfqId": "rfq-cache",
            "quoteId": "quote-cache",
            "makerId": "maker-cache",
            "symbol": "combo-a",
            "side": "SIDE_BUY",
            "price": "0.75",
            "qtyDecimal": "33.333333",
            "status": "ACTIVE",
            "ageMs": 10,
            "generatedAt": Utc::now().to_rfc3339(),
            "source": "combo_rfq_stream_cache"
        }));

        let response = combo_rfq_quote_response_from_stream_journal(&cfg, "rfq-cache")
            .unwrap()
            .unwrap();

        assert_eq!(response["source"], "combo_rfq_stream_cache");
        assert_eq!(response["quotes"][0]["quoteId"], "quote-cache");
        assert!(!cfg
            .diagnostics_dir
            .join(crate::rfq_finality::COMBO_RFQ_FINALITY_EVENTS_FILE)
            .exists());
    }

    #[test]
    fn rfq_quote_lookup_reuses_parsed_stream_cache_after_raw_cache_clear() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("parsed-stream-cache-quotes");
        crate::rfq_finality::cache_combo_rfq_stream_event(&serde_json::json!({
            "id": "stream-cache-quote-2",
            "rfqId": "rfq-parsed",
            "quoteId": "quote-parsed",
            "makerId": "maker-cache",
            "symbol": "combo-a",
            "side": "SIDE_BUY",
            "price": "0.75",
            "qtyDecimal": "33.333333",
            "status": "ACTIVE",
            "ageMs": 10,
            "generatedAt": Utc::now().to_rfc3339(),
            "source": "combo_rfq_stream_cache"
        }));

        let response = combo_rfq_quote_response_from_stream_journal(&cfg, "rfq-parsed")
            .unwrap()
            .unwrap();
        assert_eq!(response["source"], "combo_rfq_stream_cache");

        crate::rfq_finality::clear_combo_rfq_stream_events_for_rfq_for_tests("rfq-parsed");
        let response = combo_rfq_quote_response_from_stream_journal(&cfg, "rfq-parsed")
            .unwrap()
            .unwrap();

        assert_eq!(response["source"], "combo_rfq_stream_parsed_cache");
        assert_eq!(response["quotes"][0]["quoteId"], "quote-parsed");
        assert!(!cfg
            .diagnostics_dir
            .join(crate::rfq_finality::COMBO_RFQ_FINALITY_EVENTS_FILE)
            .exists());
    }

    #[test]
    fn rfq_quote_lookup_dedupes_repeated_stream_quote_ids() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("dedupe-stream-cache-quotes");
        let rfq_id = format!(
            "rfq-dedupe-{}",
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
        );
        for price in ["0.75", "0.74"] {
            crate::rfq_finality::cache_combo_rfq_stream_event(&serde_json::json!({
                "id": format!("stream-cache-quote-{price}"),
                "rfqId": rfq_id,
                "quoteId": "quote-repeated",
                "makerId": "maker-cache",
                "symbol": "combo-a",
                "side": "SIDE_BUY",
                "price": price,
                "qtyDecimal": "33.333333",
                "status": "ACTIVE",
                "ageMs": 10,
                "generatedAt": Utc::now().to_rfc3339(),
                "source": "combo_rfq_stream_cache"
            }));
        }

        let response = combo_rfq_quote_response_from_stream_journal(&cfg, &rfq_id)
            .unwrap()
            .unwrap();

        assert_eq!(response["source"], "combo_rfq_stream_cache");
        assert_eq!(response["quotes"].as_array().unwrap().len(), 1);
        assert_eq!(response["quotes"][0]["quoteId"], "quote-repeated");
        assert_eq!(response["quotes"][0]["price"], "0.74");
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_pre_accept_causal_watermark() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.375, 0.375).await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("pre-accept-causal-watermark");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let ws_price = crate::ws_client::Price {
            venue_timestamp_ms: Some(
                (Utc::now() + chrono::Duration::seconds(60)).timestamp_millis() as u64,
            ),
            book_hash: Some("book-111".into()),
            ..Default::default()
        };
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::from([(
            "111".to_string(),
            ws_price,
        )])));

        let report = run_combo_rfq_execution_state_machine_with_price_cache(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
            Some(&cache),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_pre_accept_markout");
        assert!(report.accept_response.is_none());
        let markout = report.pre_accept_markout.as_ref().unwrap();
        assert!(markout.blockers.iter().any(
            |blocker| blocker.starts_with("pre_accept_causal_watermark_newer_ws_timestamp:111")
        ));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("pre_accept_causal_watermark_newer_ws_timestamp"));
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_pre_accept_price_endpoint_mismatch() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.375, 0.375).await;
        let prices = mock_combo_rfq_prices(&server, 0.425, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("pre-accept-price-endpoint-mismatch");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_pre_accept_markout");
        assert!(report.accept_response.is_none());
        let markout = report.pre_accept_markout.as_ref().unwrap();
        assert!(markout.blockers.iter().any(|blocker| {
            blocker.starts_with("pre_accept_markout_price_endpoint_mismatch:111")
        }));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("pre_accept_markout_price_endpoint_mismatch"));
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_pre_accept_adverse_microprice() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let fixed_book_timestamp_ms = Utc::now().timestamp_millis().max(0);
        let books_body = combo_rfq_books_body_at(0.375, 0.375, fixed_book_timestamp_ms);
        let books = server
            .mock_async(move |when, then| {
                when.method(POST).path("/books");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(books_body.clone());
            })
            .await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.combo_rfq_microprice_adverse_bps = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("pre-accept-adverse-microprice");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::from([
            (
                "111".to_string(),
                crate::ws_client::Price {
                    best_bid: Some(0.35),
                    best_ask: Some(0.37),
                    best_bid_size: Some(10.0),
                    best_ask_size: Some(10.0),
                    venue_timestamp_ms: Some(fixed_book_timestamp_ms as u64),
                    book_hash: Some("book-111".into()),
                    snapshot_ready: true,
                    last_updated: Instant::now() - Duration::from_millis(100),
                    ..Default::default()
                },
            ),
            (
                "222".to_string(),
                crate::ws_client::Price {
                    best_bid: Some(0.35),
                    best_ask: Some(0.37),
                    best_bid_size: Some(10.0),
                    best_ask_size: Some(10.0),
                    venue_timestamp_ms: Some(fixed_book_timestamp_ms as u64),
                    book_hash: Some("book-222".into()),
                    snapshot_ready: true,
                    last_updated: Instant::now() - Duration::from_millis(100),
                    ..Default::default()
                },
            ),
        ])));

        let report = run_combo_rfq_execution_state_machine_with_price_cache(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
            Some(&cache),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_pre_accept_markout");
        assert!(report.accept_response.is_none());
        let markout = report.pre_accept_markout.as_ref().unwrap();
        assert_eq!(markout.ws_microstructure_tokens, 2);
        assert!(markout
            .blockers
            .iter()
            .any(|blocker| blocker.starts_with("pre_accept_microprice_adverse:")));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("pre_accept_microprice_adverse"));
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_releases_recovery_for_proven_rejected_accept_body() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.375, 0.375).await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"rejected","rfqId":"rfq-1","quoteId":"quote-1"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("accept-body-rejected");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "accept_rejected_proven");
        assert_eq!(
            report.accept_outcome,
            Some(ComboRfqAcceptOutcome::RejectedProven)
        );
        assert_eq!(report.rfq_id.as_deref(), Some("rfq-1"));
        assert!(report.accept_request.is_some());
        assert_eq!(
            report.accept_response.as_ref().unwrap()["status"],
            "rejected"
        );
        assert!(report
            .blockers
            .contains(&"rfq_accept_rejected_proven:REJECTED".to_string()));
        assert!(!report
            .blockers
            .contains(&"rfq_accept_response_not_proven_accepted".to_string()));
        assert!(!report.blockers.iter().any(|blocker| {
            blocker.contains("exposure_must_remain_reserved_until_finality_or_manual_review")
        }));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("accept_intent"));
        assert!(execution_journal.contains("accept_rejected_proven"));
        assert!(!execution_journal.contains("accepted_pending_finality"));
        assert!(unresolved_combo_rfq_execution_records(&cfg)
            .unwrap()
            .is_empty());
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_quote_notional_mismatch_before_accept() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "12.5",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-1","price":"0.75","qtyDecimal":"12.5"}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("quote-contract-notional");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_quote_contract");
        assert!(report.accept_request.is_none());
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("quote_notional_mismatch")));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("blocked_best_execution"));
        assert!(execution_journal.contains("quote_notional_mismatch"));
        assert!(unresolved_combo_rfq_execution_records(&cfg)
            .unwrap()
            .is_empty());
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        accept.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_adverse_pre_accept_markout() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.35, 0.35).await;
        let prices = mock_combo_rfq_prices(&server, 0.35, 0.35).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-1","price":"0.75","qtyDecimal":"33.333333"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("pre-accept-markout");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_pre_accept_markout");
        assert!(report.accept_request.is_some());
        assert!(report.accept_response.is_none());
        let markout = report.pre_accept_markout.as_ref().unwrap();
        assert_eq!(markout.status, "blocked");
        assert!(markout.markout_bps > COMBO_RFQ_PRE_ACCEPT_MAX_ADVERSE_MARKOUT_BPS);
        assert!(markout
            .blockers
            .iter()
            .any(|blocker| blocker.starts_with("pre_accept_adverse_markout:")));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("pre_accept_markout"));
        assert!(execution_journal.contains("pre_accept_adverse_markout"));
        assert!(unresolved_combo_rfq_execution_records(&cfg)
            .unwrap()
            .is_empty());
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_pre_accept_toxicity_haircut() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let fixed_book_timestamp_ms = Utc::now().timestamp_millis().max(0) as u64;
        let books_body = serde_json::json!([
            {
                "asset_id": "111",
                "asks": [{"price": "0.375", "size": "100"}],
                "bids": [{"price": "0.30", "size": "100"}],
                "tick_size": "0.001",
                "min_order_size": "1",
                "neg_risk": true,
                "timestamp": fixed_book_timestamp_ms,
                "hash": "book-111"
            },
            {
                "asset_id": "222",
                "asks": [{"price": "0.375", "size": "100"}],
                "bids": [{"price": "0.30", "size": "100"}],
                "tick_size": "0.001",
                "min_order_size": "1",
                "neg_risk": true,
                "timestamp": fixed_book_timestamp_ms,
                "hash": "book-222"
            }
        ])
        .to_string();
        let books = server
            .mock_async(move |when, then| {
                when.method(POST).path("/books");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(books_body.clone());
            })
            .await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-1","price":"0.75","qtyDecimal":"33.333333"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.live_slippage_bps = 100;
        cfg.gas_fallback_usd = 0.0;
        cfg.min_net_profit_usd = 8.25;
        cfg.diagnostics_dir = temp_rfq_dir("pre-accept-toxicity");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let mut toxic_price = crate::ws_client::Price {
            venue_timestamp_ms: Some(fixed_book_timestamp_ms),
            book_hash: Some("book-111".into()),
            snapshot_ready: true,
            last_updated: Instant::now() - Duration::from_millis(100),
            ..Default::default()
        };
        toxic_price
            .recent_trades
            .push_back(crate::ws_client::TradePrint {
                side: "BUY".into(),
                price: 0.375,
                size: 20.0,
                venue_timestamp_ms: Some(fixed_book_timestamp_ms),
                observed_at: Instant::now() - Duration::from_millis(100),
            });
        let quiet_price = crate::ws_client::Price {
            venue_timestamp_ms: Some(fixed_book_timestamp_ms),
            book_hash: Some("book-222".into()),
            snapshot_ready: true,
            last_updated: Instant::now() - Duration::from_millis(100),
            ..Default::default()
        };
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::from([
            ("111".to_string(), toxic_price),
            ("222".to_string(), quiet_price),
        ])));

        let report = run_combo_rfq_execution_state_machine_with_price_cache(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
            Some(&cache),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "blocked_pre_accept_markout");
        assert!(report.accept_response.is_none());
        let markout = report.pre_accept_markout.as_ref().unwrap();
        assert_eq!(markout.status, "blocked");
        assert!(markout.toxicity_haircut_usd > 0.0);
        assert_eq!(markout.toxicity_trade_prints, 1);
        assert_eq!(markout.toxicity_recent_book_updates, 2);
        assert!(markout.quote_edge_usd > cfg.min_net_profit_usd);
        assert!(markout.quote_edge_after_toxicity_usd <= cfg.min_net_profit_usd);
        assert!(markout.blockers.iter().any(|blocker| {
            blocker.starts_with("pre_accept_quote_edge_after_toxicity_below_min:")
        }));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("pre_accept_quote_edge_after_toxicity_below_min"));
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_journals_unknown_state_when_create_response_missing_id() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"created"}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.diagnostics_dir = temp_rfq_dir("create-missing-id");

        let err = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("Combo/RFQ create response missing rfq id"));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("create_intent"));
        assert!(execution_journal.contains("create_state_unknown"));
        assert!(execution_journal.contains("rfq_create_response_missing_id"));
        assert_eq!(
            unresolved_combo_rfq_execution_records(&cfg).unwrap().len(),
            2
        );
        create.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_journals_unknown_state_when_quote_query_fails() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(503)
                    .header("content-type", "application/json")
                    .body(r#"{"error":"quote gateway unavailable"}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.retry_backoff_base_ms = 1;
        cfg.diagnostics_dir = temp_rfq_dir("quote-query-unknown");

        let err = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("Combo/RFQ quote query state unknown"));
        let execution_journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))
                .unwrap();
        assert!(execution_journal.contains("request_created"));
        assert!(execution_journal.contains("quote_query_state_unknown"));
        assert!(execution_journal.contains("rfq_quote_query_state_unknown"));
        assert_eq!(
            unresolved_combo_rfq_execution_records(&cfg).unwrap().len(),
            3
        );
        create.assert_calls_async(1).await;
        quote
            .assert_calls_async(cfg.max_retries.max(1) as usize)
            .await;
    }

    #[tokio::test]
    async fn rfq_state_machine_records_unknown_state_when_accept_request_fails() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.375, 0.375).await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(503)
                    .header("content-type", "application/json")
                    .body(r#"{"error":"engine restarting"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("accept-state-unknown");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");

        let report = run_combo_rfq_execution_state_machine(
            &Client::new(),
            &cfg,
            &catalog(),
            &test_opp(ArbType::Yes),
        )
        .await
        .unwrap();

        assert_eq!(report.status, "accept_state_unknown");
        assert_eq!(report.accept_outcome, Some(ComboRfqAcceptOutcome::Unknown));
        assert_eq!(report.rfq_id.as_deref(), Some("rfq-1"));
        assert!(report.accept_request.is_some());
        assert!(report.accept_response.is_none());
        assert!(report
            .blockers
            .contains(&"rfq_accept_state_unknown".to_string()));
        assert!(report.blockers.iter().any(|blocker| {
            blocker.contains("exposure_must_remain_reserved_until_finality_or_manual_review")
        }));
        let journal =
            std::fs::read_to_string(cfg.diagnostics_dir.join(COMBO_RFQ_MAKER_JOURNAL_FILE))
                .unwrap();
        assert!(journal.contains("accept_state_unknown"));
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn rfq_state_machine_blocks_repeat_when_accept_recovery_is_pending() {
        let server = MockServer::start_async().await;
        let _market_info = mock_combo_rfq_orderable_markets(&server).await;
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path("/v1/combos/rfqs");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"rfqId":"rfq-1"}"#);
            })
            .await;
        let quote = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/combos/quotes")
                    .query_param("rfqId", "rfq-1")
                    .query_param("status", "ACTIVE");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(format!(
                        r#"{{
                            "quotes": [{{
                                "quoteId": "quote-1",
                                "rfqId": "rfq-1",
                                "makerId": "maker-good",
                                "symbol": "combo-a",
                                "side": "SIDE_BUY",
                                "price": "0.75",
                                "qtyDecimal": "33.333333",
                                "status": "ACTIVE",
                                "ageMs": 10,
                                "expiresAt": "{}"
                            }}]
                        }}"#,
                        (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339()
                    ));
            })
            .await;
        let books = mock_combo_rfq_books(&server, 0.375, 0.375).await;
        let prices = mock_combo_rfq_prices(&server, 0.375, 0.375).await;
        let accept = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/v1/combos/rfqs/rfq-1/quotes/quote-1/accept");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"status":"accepted","rfqId":"rfq-1","quoteId":"quote-1","price":"0.75","qtyDecimal":"33.333333"}"#);
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.combo_rfq_requester_enabled = true;
        cfg.combo_rfq_accept_enabled = true;
        cfg.combo_rfq_requester_api_url = server.base_url();
        cfg.clob_api_url = server.base_url();
        cfg.combo_rfq_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.diagnostics_dir = temp_rfq_dir("pending-recovery");
        append_accept_ready_maker_samples(&mut cfg, "maker-good");
        let opp = test_opp(ArbType::Yes);

        let first = run_combo_rfq_execution_state_machine(&Client::new(), &cfg, &catalog(), &opp)
            .await
            .unwrap();
        let second = run_combo_rfq_execution_state_machine(&Client::new(), &cfg, &catalog(), &opp)
            .await
            .unwrap();

        assert_eq!(first.status, "accepted_pending_finality");
        assert_eq!(second.status, "blocked_pending_recovery");
        assert!(second
            .blockers
            .iter()
            .any(|blocker| blocker.contains("pending_combo_rfq_execution_recovery_required")));
        create.assert_calls_async(1).await;
        quote.assert_calls_async(1).await;
        books.assert_calls_async(1).await;
        prices.assert_calls_async(1).await;
        accept.assert_calls_async(1).await;
    }

    #[test]
    fn pending_rfq_execution_is_cleared_by_terminal_rfq_quote_record() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("terminal-clear");
        let request = ComboRfqCreateRequest {
            qty_decimal: None,
            cash_order_qty: Some("25".into()),
            legs: vec![ComboRfqLegRequest {
                symbol: "a".into(),
                side: "SIDE_BUY".into(),
            }],
            side: "SIDE_BUY".into(),
            client_request_id: "client-1".into(),
            expiration_time: "2026-01-01T00:00:00Z".into(),
        };
        append_combo_rfq_execution_journal_record(
            &cfg,
            &combo_rfq_execution_journal_record(
                &test_opp(ArbType::Yes),
                "accept_quote",
                "accepted_pending_finality",
                Some(&request),
                Some("rfq-1"),
                Some(&ComboRfqQuoteCandidate {
                    quote_id: "quote-1".into(),
                    rfq_id: Some("rfq-1".into()),
                    maker_id: Some("maker-1".into()),
                    symbol: Some("a".into()),
                    side: Some("SIDE_BUY".into()),
                    status: Some("ACTIVE".into()),
                    price: 0.75,
                    qty_decimal: Some(12.5),
                    created_at: None,
                    expires_at: None,
                    age_ms: Some(10),
                    expected_edge_usd: Some(2.0),
                }),
                None,
                None,
                None,
                Vec::new(),
            ),
        )
        .unwrap();

        assert_eq!(
            pending_combo_rfq_execution_records(&cfg, "client-1")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            unresolved_combo_rfq_execution_records(&cfg).unwrap().len(),
            1
        );

        append_combo_rfq_finality_execution_record(
            &cfg,
            "event-1".into(),
            None,
            Some("rfq-1".into()),
            Some("quote-1".into()),
            Some("maker-1".into()),
            "finality_rejected_released".into(),
            serde_json::json!({"status":"QUOTE_DONE_AWAY"}),
            vec!["rfq_finality_terminal:QUOTE_DONE_AWAY".into()],
        )
        .unwrap();

        assert!(pending_combo_rfq_execution_records(&cfg, "client-1")
            .unwrap()
            .is_empty());
        assert!(unresolved_combo_rfq_execution_records(&cfg)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rfq_only_terminal_record_does_not_clear_pending_recovery() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("terminal-rfq-only");
        let request = ComboRfqCreateRequest {
            qty_decimal: None,
            cash_order_qty: Some("25".into()),
            legs: vec![ComboRfqLegRequest {
                symbol: "a".into(),
                side: "SIDE_BUY".into(),
            }],
            side: "SIDE_BUY".into(),
            client_request_id: "client-1".into(),
            expiration_time: "2026-01-01T00:00:00Z".into(),
        };
        append_combo_rfq_execution_journal_record(
            &cfg,
            &combo_rfq_execution_journal_record(
                &test_opp(ArbType::Yes),
                "accept_quote",
                "accepted_pending_finality",
                Some(&request),
                Some("rfq-1"),
                Some(&ComboRfqQuoteCandidate {
                    quote_id: "quote-1".into(),
                    rfq_id: Some("rfq-1".into()),
                    maker_id: Some("maker-1".into()),
                    symbol: Some("a".into()),
                    side: Some("SIDE_BUY".into()),
                    status: Some("ACTIVE".into()),
                    price: 0.75,
                    qty_decimal: Some(12.5),
                    created_at: None,
                    expires_at: None,
                    age_ms: Some(10),
                    expected_edge_usd: Some(2.0),
                }),
                None,
                None,
                None,
                Vec::new(),
            ),
        )
        .unwrap();
        append_combo_rfq_finality_execution_record(
            &cfg,
            "event-1".into(),
            None,
            Some("rfq-1".into()),
            None,
            Some("maker-1".into()),
            "finality_rejected_released".into(),
            serde_json::json!({"status":"QUOTE_DONE_AWAY"}),
            vec!["rfq_finality_terminal:QUOTE_DONE_AWAY".into()],
        )
        .unwrap();

        assert_eq!(
            pending_combo_rfq_execution_records(&cfg, "client-1")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            unresolved_combo_rfq_execution_records(&cfg).unwrap().len(),
            1
        );
    }

    #[test]
    fn stale_terminal_rfq_record_does_not_clear_later_pending_attempt() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_rfq_dir("terminal-before-pending");
        append_combo_rfq_finality_execution_record(
            &cfg,
            "event-1".into(),
            Some("client-1".into()),
            Some("rfq-1".into()),
            Some("quote-1".into()),
            Some("maker-1".into()),
            "finality_confirmed_exposure_retained".into(),
            serde_json::json!({"status":"FILLED"}),
            Vec::new(),
        )
        .unwrap();
        let request = ComboRfqCreateRequest {
            qty_decimal: None,
            cash_order_qty: Some("25".into()),
            legs: vec![ComboRfqLegRequest {
                symbol: "a".into(),
                side: "SIDE_BUY".into(),
            }],
            side: "SIDE_BUY".into(),
            client_request_id: "client-1".into(),
            expiration_time: "2026-01-01T00:00:00Z".into(),
        };
        append_combo_rfq_execution_journal_record(
            &cfg,
            &combo_rfq_execution_journal_record(
                &test_opp(ArbType::Yes),
                "accept_quote",
                "accept_intent",
                Some(&request),
                Some("rfq-1"),
                Some(&ComboRfqQuoteCandidate {
                    quote_id: "quote-1".into(),
                    rfq_id: Some("rfq-1".into()),
                    maker_id: Some("maker-1".into()),
                    symbol: Some("a".into()),
                    side: Some("SIDE_BUY".into()),
                    status: Some("ACTIVE".into()),
                    price: 0.75,
                    qty_decimal: Some(12.5),
                    created_at: None,
                    expires_at: None,
                    age_ms: Some(10),
                    expected_edge_usd: Some(2.0),
                }),
                None,
                None,
                None,
                Vec::new(),
            ),
        )
        .unwrap();

        assert_eq!(
            pending_combo_rfq_execution_records(&cfg, "client-1")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            unresolved_combo_rfq_execution_records(&cfg).unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn fetch_combo_market_catalog_paginates_public_endpoint() {
        let server = MockServer::start_async().await;
        let second = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/rfq/combo-markets")
                    .query_param("limit", "2")
                    .query_param("cursor", "next");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"markets":[{"condition_id":"cond-b","position_ids":["222","223"],"outcomes":["Yes","No"],"slug":"b","title":"B","volume":20}],"next_cursor":null}"#);
            })
            .await;
        let first = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/rfq/combo-markets")
                    .query_param("limit", "2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"markets":[{"condition_id":"cond-a","position_ids":["111","112"],"outcomes":["Yes","No"],"slug":"a","title":"A","volume":"10"}],"next_cursor":"next"}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.combo_rfq_api_url = server.base_url();
        cfg.combo_rfq_max_markets = 2;

        let catalog = fetch_combo_market_catalog(&Client::new(), &cfg)
            .await
            .unwrap();

        assert_eq!(catalog.len(), 2);
        first.assert_calls_async(1).await;
        second.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn fetch_open_combo_positions_parses_open_exposure() {
        let server = MockServer::start_async().await;
        let user = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let positions = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/positions/combos")
                    .query_param("user", user.to_string())
                    .query_param("status", "OPEN")
                    .query_param("limit", "100");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"combos":[{"combo_condition_id":"0x0391ab0ebea17b65ba87e071b0566e816b0000000000000000000000000000","combo_position_id":"777","status":"OPEN","shares_balance":"44.000000","entry_cost_usdc":"11.00","total_cost_usdc":12.5,"realized_payout_usdc":"0","legs_total":2,"legs_pending":"2","legs":[{"leg_condition_id":"cond-a","leg_position_id":"111","leg_outcome_label":"Yes","leg_status":"OPEN","market":{"title":"A","slug":"a"}}]}],"pagination":{"limit":100,"offset":0,"has_more":false}}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.api_timeout_secs = 2;

        let combos = fetch_combo_positions_with_base_url(
            &Client::new(),
            &cfg,
            user,
            "OPEN",
            &server.base_url(),
        )
        .await
        .unwrap();
        let report = combo_exposure_report(user, combos, ComboActivityReport::default());

        assert_eq!(report.status, "open_combo_exposure");
        assert_eq!(report.open_combo_count, 1);
        assert_eq!(report.redeemable_combo_count, 0);
        assert!((report.total_entry_cost_usdc - 11.0).abs() < f64::EPSILON);
        assert_eq!(report.combos[0].combo_outcome_index, None);
        assert_eq!(report.combos[0].legs[0].market_title.as_deref(), Some("A"));
        positions.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn fetch_combo_activity_paginates_and_summarizes_reconciliation_events() {
        let server = MockServer::start_async().await;
        let user = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        let first = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/activity/combos")
                    .query_param("user", user.to_string())
                    .query_param("limit", COMBO_RFQ_ACTIVITY_PAGE_LIMIT.to_string())
                    .query_param("offset", "0");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"activity":[{"id":"act-1","event_kind":"split","module_kind":"combo","user_address":"0x0000000000000000000000000000000000000001","combo_condition_id":"combo-1","combo_position_id":"pos-1","amount_usdc":"12.50","payout_usdc":0,"timestamp":"1700000000","tx_dttm":"2026-01-01T00:00:00Z","transaction_hash":"0xaaa"}],"pagination":{"has_more":true,"next_cursor":"cursor-2"}}"#);
            })
            .await;
        let second = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/v1/activity/combos")
                    .query_param("user", user.to_string())
                    .query_param("limit", COMBO_RFQ_ACTIVITY_PAGE_LIMIT.to_string())
                    .query_param("offset", COMBO_RFQ_ACTIVITY_PAGE_LIMIT.to_string())
                    .query_param("cursor", "cursor-2");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"activity":[{"id":"act-2","event_kind":"redeem","module_kind":"redeem","combo_condition_id":"combo-1","combo_position_id":"pos-1","amount_usdc":0,"payout_usdc":"14.00","timestamp":1700000100,"tx_dttm":"2026-01-01T00:01:40Z","transaction_hash":"0xbbb"}],"pagination":{"has_more":false,"next_cursor":null}}"#);
            })
            .await;
        let mut cfg = Config::from_env();
        cfg.api_timeout_secs = 2;

        let activities =
            fetch_combo_activity_with_base_url(&Client::new(), &cfg, user, &server.base_url())
                .await
                .unwrap();
        let report = combo_activity_report(user, activities);

        assert_eq!(report.status, "combo_activity_seen");
        assert_eq!(report.activity_count, 2);
        assert_eq!(report.redeem_events, 1);
        assert!((report.total_amount_usdc - 12.5).abs() < f64::EPSILON);
        assert!((report.total_payout_usdc - 14.0).abs() < f64::EPSILON);
        assert_eq!(report.latest_timestamp, Some(1_700_000_100));
        assert_eq!(
            report.latest_tx_dttm.as_deref(),
            Some("2026-01-01T00:01:40Z")
        );
        first.assert_calls_async(1).await;
        second.assert_calls_async(1).await;
    }
}
