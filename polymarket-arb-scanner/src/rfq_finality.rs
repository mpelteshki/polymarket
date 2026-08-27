//! Local RFQ finality ingestion.
//!
//! This module normalizes RFQ/dropcopy/order-stream shaped JSONL diagnostics into
//! a stable journal. It is not a network stream client; it provides the durable
//! event-sourced surface that a future stream client can write into.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{OnceLock, RwLock};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tracing::{debug, warn};

use crate::combo_rfq_client::{
    append_combo_rfq_finality_execution_record, append_combo_rfq_maker_journal_record,
    resolve_combo_rfq_execution_event_id, resolve_combo_rfq_execution_reserve_amount_usd,
    ComboRfqMakerJournalRecord,
};
use crate::config::Config;
use crate::exposure::append_exposure_ledger_delta;
use crate::live_executor::{
    append_combo_rfq_realized_pnl_record, append_live_route_replay_records_deduped,
    ComboRfqRealizedPnlRecord, LiveRouteReplayRecord, LIVE_REALIZED_PNL_FILE,
};
use crate::onchain_fills::{
    build_order_filled_reconciliation_report, onchain_log_summary_from_value, OnchainLogSummary,
    OrderFilledEvent, ORDER_FILLED_COLLECTOR_RUN_REPORT_FILE,
};
use crate::user_channel::LIVE_USER_EVENTS_FILE;
use polymarket_client_sdk_v2::types::{Address, U256};

pub const COMBO_RFQ_FINALITY_EVENTS_FILE: &str = "combo_rfq_finality_events.jsonl";
pub const COMBO_RFQ_FINALITY_JOURNAL_FILE: &str = "combo_rfq_finality_journal.jsonl";
pub const COMBO_RFQ_FINALITY_REPORT_FILE: &str = "combo_rfq_finality_report.json";
pub const COMBO_RFQ_STREAM_CHECKPOINT_FILE: &str = "combo_rfq_stream_checkpoint.json";
pub const COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE: &str =
    "combo_rfq_onchain_order_filled_logs.jsonl";
const COMBO_RFQ_ROUTE: &str = "combo_rfq_candidate";
const LIVE_COMBO_RFQ_FINALITY_INGEST_INTERVAL_SECS: u64 = 1;
const COMBO_RFQ_STREAM_EVENT_CACHE_MAX_RFQS: usize = 1_024;
const COMBO_RFQ_STREAM_EVENT_CACHE_MAX_PER_RFQ: usize = 64;
const COMBO_RFQ_STREAM_EVENT_BUS_CAPACITY: usize = 1_024;
static COMBO_RFQ_STREAM_EVENT_CACHE: OnceLock<RwLock<HashMap<String, Vec<Value>>>> =
    OnceLock::new();
static COMBO_RFQ_STREAM_EVENT_BUS: OnceLock<broadcast::Sender<Value>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqFinalityRecord {
    pub finality_id: String,
    pub generated_at: String,
    pub source: String,
    pub rfq_id: Option<String>,
    pub quote_id: Option<String>,
    pub client_request_id: Option<String>,
    pub maker_id: Option<String>,
    pub symbol: Option<String>,
    pub market_event_id: Option<String>,
    pub order_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub side: Option<String>,
    pub token_id: Option<String>,
    pub maker_amount_filled: Option<String>,
    pub taker_amount_filled: Option<String>,
    pub fee: Option<String>,
    pub status: String,
    pub status_class: String,
    pub quote_age_ms: Option<i64>,
    pub price: Option<f64>,
    pub qty_decimal: Option<f64>,
    pub expected_edge_usd: Option<f64>,
    pub realized_ev_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqFinalityReport {
    pub generated_at: String,
    pub events_path: String,
    pub journal_path: String,
    pub events_seen: usize,
    pub normalized_events: usize,
    pub newly_appended: usize,
    pub records_seen: usize,
    pub terminal_records: usize,
    pub confirmed_records: usize,
    pub rejected_records: usize,
    pub failed_records: usize,
    pub abnormal_records: usize,
    pub pending_records: usize,
    pub realized_terminal_records: usize,
    pub latest_terminal_at: Option<String>,
    pub latest_confirmed_at: Option<String>,
    pub max_finality_age_secs: u64,
    pub min_confirmed_samples: usize,
    pub stream_checkpoint: ComboRfqStreamCheckpoint,
    pub onchain_order_filled: ComboRfqOnchainOrderFilledQuorum,
    pub user_channel: ComboRfqUserChannelQuorum,
    pub realized_pnl_ledger: ComboRfqRealizedPnlLedgerQuorum,
    pub lifecycle: ComboRfqLifecycleSummary,
    pub maker_records_written: usize,
    pub replay_labels_written: usize,
    pub realized_pnl_records_written: usize,
    pub status: String,
    pub blockers: Vec<String>,
}

fn combo_rfq_stream_event_cache() -> &'static RwLock<HashMap<String, Vec<Value>>> {
    COMBO_RFQ_STREAM_EVENT_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn combo_rfq_stream_event_bus() -> &'static broadcast::Sender<Value> {
    COMBO_RFQ_STREAM_EVENT_BUS
        .get_or_init(|| broadcast::channel(COMBO_RFQ_STREAM_EVENT_BUS_CAPACITY).0)
}

pub fn cache_combo_rfq_stream_event(event: &Value) {
    let Some(rfq_id) = text_value(event, &["rfqId", "rfq_id"]) else {
        return;
    };
    let event = event.clone();
    {
        let Ok(mut cache) = combo_rfq_stream_event_cache().write() else {
            return;
        };
        if !cache.contains_key(&rfq_id) && cache.len() >= COMBO_RFQ_STREAM_EVENT_CACHE_MAX_RFQS {
            if let Some(oldest_key) = cache.keys().next().cloned() {
                cache.remove(&oldest_key);
            }
        }
        let events = cache.entry(rfq_id).or_default();
        events.push(event.clone());
        if events.len() > COMBO_RFQ_STREAM_EVENT_CACHE_MAX_PER_RFQ {
            let excess = events.len() - COMBO_RFQ_STREAM_EVENT_CACHE_MAX_PER_RFQ;
            events.drain(0..excess);
        }
    }
    let _ = combo_rfq_stream_event_bus().send(event);
}

pub fn cached_combo_rfq_stream_events_for_rfq(rfq_id: &str) -> Vec<Value> {
    let rfq_id = rfq_id.trim();
    if rfq_id.is_empty() {
        return Vec::new();
    }
    combo_rfq_stream_event_cache()
        .read()
        .ok()
        .and_then(|cache| cache.get(rfq_id).cloned())
        .unwrap_or_default()
}

pub async fn wait_for_cached_combo_rfq_stream_event(rfq_id: &str, timeout: Duration) -> bool {
    let rfq_id = rfq_id.trim().to_string();
    if rfq_id.is_empty() {
        return false;
    }
    let mut receiver = combo_rfq_stream_event_bus().subscribe();
    if !cached_combo_rfq_stream_events_for_rfq(&rfq_id).is_empty() {
        return true;
    }
    time::timeout(timeout, async {
        loop {
            match receiver.recv().await {
                Ok(event)
                    if text_value(&event, &["rfqId", "rfq_id"]).as_deref()
                        == Some(rfq_id.as_str()) =>
                {
                    return true
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if !cached_combo_rfq_stream_events_for_rfq(&rfq_id).is_empty() {
                        return true;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return false,
            }
        }
    })
    .await
    .unwrap_or(false)
}

#[cfg(test)]
pub fn clear_combo_rfq_stream_event_cache_for_tests() {
    if let Ok(mut cache) = combo_rfq_stream_event_cache().write() {
        cache.clear();
    }
}

#[cfg(test)]
pub fn clear_combo_rfq_stream_events_for_rfq_for_tests(rfq_id: &str) {
    if let Ok(mut cache) = combo_rfq_stream_event_cache().write() {
        cache.remove(rfq_id);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqOnchainOrderFilledQuorum {
    pub logs_path: String,
    pub collector_latest_block: Option<u64>,
    pub collector_finalized_block: Option<u64>,
    pub collector_finalized_lag_blocks: Option<u64>,
    pub raw_logs_seen: usize,
    pub parsed_logs: usize,
    pub order_filled_logs: usize,
    pub decoded_order_filled_logs: usize,
    pub account_order_filled_logs: usize,
    pub confirmed_records_with_chain_join_key: usize,
    pub matched_confirmed_records: usize,
    pub account_filter: Option<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqUserChannelQuorum {
    pub events_path: String,
    pub raw_events_seen: usize,
    pub parsed_trade_events: usize,
    pub confirmed_trade_events: usize,
    pub pending_trade_events: usize,
    pub failed_trade_events: usize,
    pub malformed_event_lines: usize,
    pub ignored_event_lines: usize,
    pub confirmed_records_with_user_join_key: usize,
    pub matched_confirmed_records: usize,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqRealizedPnlLedgerQuorum {
    pub ledger_path: String,
    pub ledger_records: usize,
    pub terminal_records_with_realized_ev: usize,
    pub matched_terminal_records: usize,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ComboRfqStreamCheckpoint {
    pub last_rfq_event_at: Option<String>,
    pub last_dropcopy_event_at: Option<String>,
    pub last_dropcopy_resume_token: Option<String>,
    pub last_heartbeat_at: Option<String>,
    #[serde(default)]
    pub reconnect_count: u64,
    #[serde(default)]
    pub gap_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqLifecycleSummary {
    pub sessions: usize,
    pub valid_sessions: usize,
    pub invalid_sessions: usize,
    pub confirmed_without_dropcopy: usize,
    pub blockers: Vec<String>,
}

pub fn write_combo_rfq_finality_report(config: &Config) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let events_path = config.diagnostics_dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE);
    let journal_path = config.diagnostics_dir.join(COMBO_RFQ_FINALITY_JOURNAL_FILE);
    let checkpoint_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_STREAM_CHECKPOINT_FILE);
    let onchain_logs_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE);
    let user_events_path = config.diagnostics_dir.join(LIVE_USER_EVENTS_FILE);
    let realized_pnl_path = config.diagnostics_dir.join(LIVE_REALIZED_PNL_FILE);
    let ingest = ingest_combo_rfq_finality_events(config, &events_path, &journal_path)?;
    ensure_combo_rfq_stream_checkpoint(&checkpoint_path, &ingest)?;
    let report_path = config.diagnostics_dir.join(COMBO_RFQ_FINALITY_REPORT_FILE);
    let report = build_combo_rfq_finality_report_from_paths(
        config,
        &events_path,
        &journal_path,
        &checkpoint_path,
        &onchain_logs_path,
        &user_events_path,
        &realized_pnl_path,
        ingest.events_seen,
        ingest.normalized_events,
        ingest.newly_appended,
        ingest.maker_records_written,
        ingest.replay_labels_written,
        ingest.realized_pnl_records_written,
    )?;
    let body = serde_json::to_string_pretty(&report)?;
    fs::write(&report_path, body).with_context(|| {
        format!(
            "writing Combo/RFQ finality report {}",
            report_path.display()
        )
    })?;
    Ok(report_path)
}

pub fn spawn_live_combo_rfq_finality_ingester(config: Config) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(
            LIVE_COMBO_RFQ_FINALITY_INGEST_INTERVAL_SECS,
        ));
        loop {
            interval.tick().await;
            match write_combo_rfq_finality_report(&config) {
                Ok(path) => debug!(
                    "Combo/RFQ live finality ingester updated {}",
                    path.display()
                ),
                Err(err) => warn!("Combo/RFQ live finality ingester failed: {err:#}"),
            }
        }
    })
}

pub fn build_combo_rfq_finality_report(config: &Config) -> Result<ComboRfqFinalityReport> {
    let events_path = config.diagnostics_dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE);
    let journal_path = config.diagnostics_dir.join(COMBO_RFQ_FINALITY_JOURNAL_FILE);
    let checkpoint_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_STREAM_CHECKPOINT_FILE);
    let onchain_logs_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE);
    let user_events_path = config.diagnostics_dir.join(LIVE_USER_EVENTS_FILE);
    let realized_pnl_path = config.diagnostics_dir.join(LIVE_REALIZED_PNL_FILE);
    build_combo_rfq_finality_report_from_paths(
        config,
        &events_path,
        &journal_path,
        &checkpoint_path,
        &onchain_logs_path,
        &user_events_path,
        &realized_pnl_path,
        0,
        0,
        0,
        0,
        0,
        0,
    )
}

fn build_combo_rfq_finality_report_from_paths(
    config: &Config,
    events_path: &Path,
    journal_path: &Path,
    checkpoint_path: &Path,
    onchain_logs_path: &Path,
    user_events_path: &Path,
    realized_pnl_path: &Path,
    events_seen: usize,
    normalized_events: usize,
    newly_appended: usize,
    maker_records_written: usize,
    replay_labels_written: usize,
    realized_pnl_records_written: usize,
) -> Result<ComboRfqFinalityReport> {
    let records = read_combo_rfq_finality_records(journal_path)?;
    let checkpoint = read_combo_rfq_stream_checkpoint(checkpoint_path)?;
    let onchain_logs = read_combo_rfq_onchain_order_filled_logs(onchain_logs_path)?;
    let user_events = read_combo_rfq_user_channel_events(user_events_path)?;
    let realized_pnl_ledger = read_raw_jsonl_values(realized_pnl_path)?;
    Ok(combo_rfq_finality_report(
        config,
        events_path,
        journal_path,
        onchain_logs_path,
        user_events_path,
        realized_pnl_path,
        events_seen,
        normalized_events,
        newly_appended,
        &records,
        checkpoint,
        onchain_logs,
        user_events,
        realized_pnl_ledger,
        maker_records_written,
        replay_labels_written,
        realized_pnl_records_written,
    ))
}

fn ingest_combo_rfq_finality_events(
    config: &Config,
    events_path: &Path,
    journal_path: &Path,
) -> Result<ComboRfqFinalityReport> {
    let raw_events = read_raw_jsonl_values(events_path)?;
    let mut normalized = raw_events
        .iter()
        .filter_map(normalize_combo_rfq_finality_event)
        .collect::<Vec<_>>();
    normalized.sort_by(|left, right| left.finality_id.cmp(&right.finality_id));

    let existing = read_combo_rfq_finality_records(journal_path)?;
    let mut seen = existing
        .iter()
        .map(|record| record.finality_id.clone())
        .collect::<HashSet<_>>();
    let new_records = normalized
        .iter()
        .filter(|record| seen.insert(record.finality_id.clone()))
        .cloned()
        .collect::<Vec<_>>();
    append_combo_rfq_finality_records(journal_path, &new_records)?;

    let mut all_records = existing;
    all_records.extend(new_records.clone());
    let maker_records_written = write_terminal_maker_records(config, &all_records)?;
    let replay_labels_written = write_terminal_replay_labels(config, &all_records)?;
    let realized_pnl_records_written = write_terminal_realized_pnl_records(config, &new_records)?;
    let _execution_records_written = write_terminal_execution_records(config, &new_records)?;

    Ok(combo_rfq_finality_report(
        config,
        events_path,
        journal_path,
        Path::new(""),
        Path::new(""),
        Path::new(""),
        raw_events.len(),
        normalized.len(),
        new_records.len(),
        &all_records,
        ComboRfqStreamCheckpoint::default(),
        ParsedOnchainOrderFilledLogs::default(),
        ParsedUserChannelTradeEvents::default(),
        Vec::new(),
        maker_records_written,
        replay_labels_written,
        realized_pnl_records_written,
    ))
}

fn combo_rfq_finality_report(
    config: &Config,
    events_path: &Path,
    journal_path: &Path,
    onchain_logs_path: &Path,
    user_events_path: &Path,
    realized_pnl_path: &Path,
    events_seen: usize,
    normalized_events: usize,
    newly_appended: usize,
    records: &[ComboRfqFinalityRecord],
    stream_checkpoint: ComboRfqStreamCheckpoint,
    onchain_logs: ParsedOnchainOrderFilledLogs,
    user_events: ParsedUserChannelTradeEvents,
    realized_pnl_ledger: Vec<Value>,
    maker_records_written: usize,
    replay_labels_written: usize,
    realized_pnl_records_written: usize,
) -> ComboRfqFinalityReport {
    let terminal_records = records
        .iter()
        .filter(|record| combo_rfq_status_class_is_terminal(&record.status_class))
        .count();
    let (confirmed_sessions, confirmed_records_without_session_key) =
        latest_unique_rfq_sessions(records, |record| record.status_class == "confirmed");
    let confirmed_records = confirmed_sessions.len();
    let rejected_records = records
        .iter()
        .filter(|record| record.status_class == "rejected")
        .count();
    let failed_records = records
        .iter()
        .filter(|record| record.status_class == "failed")
        .count();
    let abnormal_records = records
        .iter()
        .filter(|record| record.status_class == "abnormal")
        .count();
    let pending_records = records
        .iter()
        .filter(|record| record.status_class == "pending")
        .count();
    let realized_terminal_records = records
        .iter()
        .filter(|record| combo_rfq_status_class_is_terminal(&record.status_class))
        .filter(|record| record.realized_ev_usd.is_some())
        .count();
    let latest_terminal_at = latest_record_timestamp(
        records
            .iter()
            .filter(|record| combo_rfq_status_class_is_terminal(&record.status_class)),
    );
    let latest_confirmed_at =
        latest_record_timestamp(confirmed_sessions.iter().map(|(_, record)| *record));
    let now = Utc::now();
    let max_finality_age_secs = config.combo_rfq_finality_max_age_secs.max(1);
    let mut recent_confirmed_records = 0usize;
    let mut stale_confirmed_records = 0usize;
    let mut future_confirmed_records = 0usize;
    let mut missing_confirmed_timestamps = 0usize;
    let mut missing_confirmed_source_timestamps = 0usize;
    for (_, record) in &confirmed_sessions {
        if record.source.contains("missing_source_timestamp") {
            missing_confirmed_source_timestamps += 1;
        }
        match parse_rfc3339_timestamp(&record.generated_at) {
            Some(timestamp) if timestamp > now + chrono::Duration::seconds(5) => {
                future_confirmed_records += 1;
            }
            Some(timestamp) => {
                let age_secs = now.signed_duration_since(timestamp).num_seconds().max(0) as u64;
                if age_secs > max_finality_age_secs {
                    stale_confirmed_records += 1;
                } else {
                    recent_confirmed_records += 1;
                }
            }
            None => missing_confirmed_timestamps += 1,
        }
    }
    let lifecycle = build_combo_rfq_lifecycle_summary(records);
    let onchain_order_filled =
        combo_rfq_onchain_order_filled_quorum(config, onchain_logs_path, onchain_logs, records);
    let user_channel = combo_rfq_user_channel_quorum(user_events_path, user_events, records);
    let realized_pnl_ledger =
        combo_rfq_realized_pnl_ledger_quorum(realized_pnl_path, &realized_pnl_ledger, records);
    let mut blockers = Vec::new();
    if stream_checkpoint.gap_count > 0 {
        blockers.push(format!("rfq_stream_gap:{}", stream_checkpoint.gap_count));
    }
    if stream_checkpoint.last_dropcopy_resume_token.is_none() {
        blockers.push("dropcopy_resume_token_missing".to_string());
    }
    blockers.extend(lifecycle.blockers.iter().cloned());
    if records.is_empty() {
        blockers.push("missing_rfq_finality_records".to_string());
    }
    if terminal_records == 0 {
        blockers.push("missing_terminal_rfq_finality".to_string());
    }
    if confirmed_records == 0 {
        blockers.push("missing_confirmed_rfq_finality".to_string());
    }
    let session_resolver = RfqFinalitySessionResolver::from_records(records);
    for record in confirmed_records_without_session_key {
        let reason = if session_resolver.pair_is_ambiguous(record) {
            "ambiguous"
        } else {
            "missing"
        };
        blockers.push(format!(
            "confirmed_rfq_session_key_{reason}:{}",
            record.finality_id
        ));
    }
    if confirmed_records < config.combo_rfq_finality_min_confirmed_samples {
        blockers.push(format!(
            "insufficient_confirmed_rfq_finality:{}/{}",
            confirmed_records, config.combo_rfq_finality_min_confirmed_samples
        ));
    }
    if recent_confirmed_records < config.combo_rfq_finality_min_confirmed_samples {
        blockers.push(format!(
            "insufficient_recent_confirmed_rfq_finality:{}/{}",
            recent_confirmed_records, config.combo_rfq_finality_min_confirmed_samples
        ));
    }
    if stale_confirmed_records > 0 {
        blockers.push(format!(
            "stale_confirmed_rfq_finality:{stale_confirmed_records}>{max_finality_age_secs}s"
        ));
    }
    if future_confirmed_records > 0 {
        blockers.push(format!(
            "future_confirmed_rfq_finality:{future_confirmed_records}"
        ));
    }
    if missing_confirmed_timestamps > 0 {
        blockers.push(format!(
            "missing_confirmed_rfq_finality_timestamps:{missing_confirmed_timestamps}"
        ));
    }
    if missing_confirmed_source_timestamps > 0 {
        blockers.push(format!(
            "missing_confirmed_rfq_finality_source_timestamps:{missing_confirmed_source_timestamps}"
        ));
    }
    if realized_terminal_records == 0 {
        blockers.push("missing_realized_ev_rfq_finality".to_string());
    }
    blockers.extend(onchain_order_filled.blockers.iter().cloned());
    blockers.extend(user_channel.blockers.iter().cloned());
    blockers.extend(realized_pnl_ledger.blockers.iter().cloned());
    ComboRfqFinalityReport {
        generated_at: Utc::now().to_rfc3339(),
        events_path: events_path.display().to_string(),
        journal_path: journal_path.display().to_string(),
        events_seen,
        normalized_events,
        newly_appended,
        records_seen: records.len(),
        terminal_records,
        confirmed_records,
        rejected_records,
        failed_records,
        abnormal_records,
        pending_records,
        realized_terminal_records,
        latest_terminal_at,
        latest_confirmed_at,
        max_finality_age_secs: config.combo_rfq_finality_max_age_secs,
        min_confirmed_samples: config.combo_rfq_finality_min_confirmed_samples,
        stream_checkpoint,
        onchain_order_filled,
        user_channel,
        realized_pnl_ledger,
        lifecycle,
        maker_records_written,
        replay_labels_written,
        realized_pnl_records_written,
        status: if blockers.is_empty() {
            "ready".into()
        } else {
            "blocked".into()
        },
        blockers,
    }
}

fn combo_rfq_realized_pnl_ledger_quorum(
    ledger_path: &Path,
    ledger: &[Value],
    records: &[ComboRfqFinalityRecord],
) -> ComboRfqRealizedPnlLedgerQuorum {
    let terminal_with_realized = records
        .iter()
        .filter(|record| combo_rfq_status_class_is_terminal(&record.status_class))
        .filter(|record| record.realized_ev_usd.is_some())
        .collect::<Vec<_>>();
    let mut blockers = Vec::new();
    let mut matched_terminal_records = 0usize;
    if !terminal_with_realized.is_empty() && ledger.is_empty() {
        push_unique_finality_blocker(&mut blockers, "missing_combo_rfq_realized_pnl_ledger");
    }
    for record in &terminal_with_realized {
        if ledger
            .iter()
            .any(|entry| realized_pnl_ledger_matches_finality(record, entry))
        {
            matched_terminal_records += 1;
        } else {
            push_unique_finality_blocker(
                &mut blockers,
                format!(
                    "realized_pnl_ledger_missing_finality:{}",
                    record.finality_id
                ),
            );
        }
    }
    ComboRfqRealizedPnlLedgerQuorum {
        ledger_path: ledger_path.display().to_string(),
        ledger_records: ledger.len(),
        terminal_records_with_realized_ev: terminal_with_realized.len(),
        matched_terminal_records,
        blockers,
    }
}

fn realized_pnl_ledger_matches_finality(record: &ComboRfqFinalityRecord, entry: &Value) -> bool {
    let source = text_value(entry, &["source"]);
    let finality_id = text_value(entry, &["finality_id"]);
    let rfq_id = text_value(entry, &["rfq_id"]);
    let quote_id = text_value(entry, &["quote_id"]);
    let maker_id = text_value(entry, &["maker_id"]);
    let status = text_value(entry, &["status"]);
    let status_class = text_value(entry, &["status_class"]);
    let realized_ev_usd = number_value(entry, &["realized_ev_usd", "realizedEvUsd"]);

    source.as_deref() == Some("combo_rfq_finality")
        && finality_id.as_deref() == Some(record.finality_id.as_str())
        && realized_ev_usd
            .zip(record.realized_ev_usd)
            .map(|(left, right)| (left - right).abs() <= 0.000001)
            .unwrap_or(false)
        && optional_text_equal(record.rfq_id.as_deref(), rfq_id.as_deref())
        && optional_text_equal(record.quote_id.as_deref(), quote_id.as_deref())
        && optional_text_equal(record.maker_id.as_deref(), maker_id.as_deref())
        && optional_text_equal(Some(&record.status), status.as_deref())
        && optional_text_equal(Some(&record.status_class), status_class.as_deref())
}

fn latest_record_timestamp<'a>(
    records: impl IntoIterator<Item = &'a ComboRfqFinalityRecord>,
) -> Option<String> {
    records
        .into_iter()
        .filter_map(|record| parse_rfc3339_timestamp(&record.generated_at))
        .max()
        .map(|timestamp| timestamp.to_rfc3339())
}

fn latest_unique_rfq_sessions(
    records: &[ComboRfqFinalityRecord],
    mut include: impl FnMut(&ComboRfqFinalityRecord) -> bool,
) -> (
    Vec<(String, &ComboRfqFinalityRecord)>,
    Vec<&ComboRfqFinalityRecord>,
) {
    let session_resolver = RfqFinalitySessionResolver::from_records(records);
    let mut by_session: HashMap<String, (usize, &ComboRfqFinalityRecord)> = HashMap::new();
    let mut missing_session_key = Vec::new();
    for (index, record) in records.iter().enumerate() {
        let Some(session_key) = session_resolver.resolve(record) else {
            if include(record) {
                missing_session_key.push(record);
            }
            continue;
        };
        let replace = by_session
            .get(&session_key)
            .map(|(current_index, current)| {
                let candidate_timestamp = parse_rfc3339_timestamp(&record.generated_at);
                let current_timestamp = parse_rfc3339_timestamp(&current.generated_at);
                match (candidate_timestamp, current_timestamp) {
                    (Some(candidate), Some(current)) => {
                        candidate > current || (candidate == current && index > *current_index)
                    }
                    _ => index > *current_index,
                }
            })
            .unwrap_or(true);
        if replace {
            by_session.insert(session_key, (index, record));
        }
    }
    let mut sessions = by_session
        .into_iter()
        .filter_map(|(session_key, (_, record))| include(record).then_some((session_key, record)))
        .collect::<Vec<_>>();
    sessions.sort_by(|left, right| left.0.cmp(&right.0));
    (sessions, missing_session_key)
}

#[derive(Debug, Default)]
struct RfqFinalitySessionResolver {
    clients_by_rfq_quote: HashMap<String, HashSet<String>>,
}

impl RfqFinalitySessionResolver {
    fn from_records(records: &[ComboRfqFinalityRecord]) -> Self {
        let mut clients_by_rfq_quote: HashMap<String, HashSet<String>> = HashMap::new();
        for record in records {
            let pair_key =
                rfq_quote_session_key(record.rfq_id.as_deref(), record.quote_id.as_deref());
            let execution_key = client_execution_session_key(record.client_request_id.as_deref());
            if let (Some(pair_key), Some(execution_key)) = (pair_key, execution_key) {
                clients_by_rfq_quote
                    .entry(pair_key)
                    .or_default()
                    .insert(execution_key);
            }
        }
        Self {
            clients_by_rfq_quote,
        }
    }

    fn resolve(&self, record: &ComboRfqFinalityRecord) -> Option<String> {
        if let Some(execution_key) =
            client_execution_session_key(record.client_request_id.as_deref())
        {
            return Some(execution_key);
        }
        let pair_key = rfq_quote_session_key(record.rfq_id.as_deref(), record.quote_id.as_deref())?;
        match self.clients_by_rfq_quote.get(&pair_key) {
            Some(execution_keys) if execution_keys.len() == 1 => {
                execution_keys.iter().next().cloned()
            }
            Some(_) => None,
            None => Some(pair_key),
        }
    }

    fn pair_is_ambiguous(&self, record: &ComboRfqFinalityRecord) -> bool {
        if clean_text(record.client_request_id.as_deref()).is_some() {
            return false;
        }
        rfq_quote_session_key(record.rfq_id.as_deref(), record.quote_id.as_deref())
            .and_then(|pair_key| self.clients_by_rfq_quote.get(&pair_key))
            .map(|execution_keys| execution_keys.len() > 1)
            .unwrap_or(false)
    }
}

fn rfq_finality_session_key_from_parts(
    rfq_id: Option<&str>,
    quote_id: Option<&str>,
    client_request_id: Option<&str>,
) -> Option<String> {
    client_execution_session_key(client_request_id)
        .or_else(|| rfq_quote_session_key(rfq_id, quote_id))
}

fn rfq_quote_session_key(rfq_id: Option<&str>, quote_id: Option<&str>) -> Option<String> {
    let rfq_id = clean_text(rfq_id);
    let quote_id = clean_text(quote_id);
    if let (Some(rfq_id), Some(quote_id)) = (rfq_id, quote_id) {
        return Some(format!(
            "rfq_quote:{}:{rfq_id}:{}:{quote_id}",
            rfq_id.len(),
            quote_id.len()
        ));
    }
    None
}

fn client_execution_session_key(client_request_id: Option<&str>) -> Option<String> {
    clean_text(client_request_id).map(|client_request_id| {
        format!("execution:{}:{client_request_id}", client_request_id.len())
    })
}

fn parse_rfc3339_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

#[derive(Debug, Clone, Default)]
struct ParsedOnchainOrderFilledLogs {
    raw_logs_seen: usize,
    logs: Vec<OnchainLogSummary>,
    blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct OnchainCollectorFinalityWindow {
    latest_block: Option<u64>,
    finalized_block: Option<u64>,
    finalized_lag_blocks: Option<u64>,
    blockers: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct ParsedUserChannelTradeEvents {
    raw_events_seen: usize,
    trade_events: Vec<UserChannelTradeEvidence>,
    malformed_event_lines: usize,
    ignored_event_lines: usize,
    blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct UserChannelTradeEvidence {
    trade_id: Option<String>,
    order_id: Option<String>,
    taker_order_id: Option<String>,
    maker_order_id: Option<String>,
    transaction_hash: Option<String>,
    rfq_id: Option<String>,
    quote_id: Option<String>,
    client_request_id: Option<String>,
    condition_id: Option<String>,
    asset_id: Option<String>,
    side: Option<String>,
    size: Option<String>,
    price: Option<String>,
    status: Option<String>,
    status_class: Option<String>,
}

fn combo_rfq_onchain_order_filled_quorum(
    config: &Config,
    logs_path: &Path,
    onchain_logs: ParsedOnchainOrderFilledLogs,
    records: &[ComboRfqFinalityRecord],
) -> ComboRfqOnchainOrderFilledQuorum {
    let account_filter = configured_onchain_order_filled_account(config);
    let collector_finality = read_onchain_collector_finality_window(config);
    let reconciliation =
        build_order_filled_reconciliation_report(&onchain_logs.logs, account_filter);
    let (confirmed, _) =
        latest_unique_rfq_sessions(records, |record| record.status_class == "confirmed");
    let confirmed_records = confirmed.len();
    let mut blockers = onchain_logs.blockers;
    blockers.extend(collector_finality.blockers.iter().cloned());
    for blocker in &reconciliation.blockers {
        if blocker.starts_with("malformed_order_filled_log") {
            push_unique_finality_blocker(
                &mut blockers,
                format!("malformed_onchain_order_filled_log:{blocker}"),
            );
        }
    }

    if confirmed_records > 0 {
        if account_filter.is_none() {
            push_unique_finality_blocker(
                &mut blockers,
                "onchain_order_filled_account_filter_missing",
            );
        }
        if reconciliation.decoded_order_filled_logs == 0 {
            push_unique_finality_blocker(&mut blockers, "missing_onchain_order_filled_quorum");
        }
        if account_filter.is_some() && reconciliation.account_order_filled_logs == 0 {
            push_unique_finality_blocker(
                &mut blockers,
                "missing_account_onchain_order_filled_quorum",
            );
        }
    }
    let mut confirmed_records_with_chain_join_key = 0usize;
    let mut matched_confirmed_records = 0usize;
    let mut consumed_event_indices = HashSet::new();
    for (_, record) in confirmed {
        if !record_has_chain_join_key(record) {
            push_unique_finality_blocker(
                &mut blockers,
                format!(
                    "confirmed_rfq_chain_join_key_missing:{}",
                    record.finality_id
                ),
            );
            continue;
        }
        confirmed_records_with_chain_join_key += 1;
        let finalized_match = collector_finality
            .finalized_block
            .and_then(|finalized_block| {
                reconciliation
                    .events
                    .iter()
                    .enumerate()
                    .find(|(index, event)| {
                        !consumed_event_indices.contains(index)
                            && event
                                .block_number
                                .map(|block_number| block_number <= finalized_block)
                                .unwrap_or(false)
                            && record_matches_onchain_order_filled(record, event)
                    })
            });
        let matched_event = finalized_match.or_else(|| {
            reconciliation
                .events
                .iter()
                .enumerate()
                .find(|(index, event)| {
                    !consumed_event_indices.contains(index)
                        && record_matches_onchain_order_filled(record, event)
                })
        });
        let Some((event_index, event)) = matched_event else {
            push_unique_finality_blocker(
                &mut blockers,
                format!(
                    "confirmed_rfq_onchain_order_filled_mismatch:{}",
                    record.finality_id
                ),
            );
            continue;
        };
        consumed_event_indices.insert(event_index);
        if let Some(finalized_block) = collector_finality.finalized_block {
            match event.block_number {
                Some(block_number) if block_number <= finalized_block => {
                    matched_confirmed_records += 1;
                }
                Some(block_number) => push_unique_finality_blocker(
                    &mut blockers,
                    format!(
                        "confirmed_rfq_onchain_order_filled_not_finalized:{}:block={block_number}>finalized={finalized_block}",
                        record.finality_id
                    ),
                ),
                None => push_unique_finality_blocker(
                    &mut blockers,
                    format!(
                        "confirmed_rfq_onchain_order_filled_missing_block:{}",
                        record.finality_id
                    ),
                ),
            }
        } else {
            matched_confirmed_records += 1;
        }
    }

    ComboRfqOnchainOrderFilledQuorum {
        logs_path: logs_path.display().to_string(),
        collector_latest_block: collector_finality.latest_block,
        collector_finalized_block: collector_finality.finalized_block,
        collector_finalized_lag_blocks: collector_finality.finalized_lag_blocks,
        raw_logs_seen: onchain_logs.raw_logs_seen,
        parsed_logs: onchain_logs.logs.len(),
        order_filled_logs: reconciliation.order_filled_logs,
        decoded_order_filled_logs: reconciliation.decoded_order_filled_logs,
        account_order_filled_logs: reconciliation.account_order_filled_logs,
        confirmed_records_with_chain_join_key,
        matched_confirmed_records,
        account_filter: account_filter.map(|account| account.to_string()),
        blockers,
    }
}

fn combo_rfq_user_channel_quorum(
    events_path: &Path,
    user_events: ParsedUserChannelTradeEvents,
    records: &[ComboRfqFinalityRecord],
) -> ComboRfqUserChannelQuorum {
    let (confirmed, _) =
        latest_unique_rfq_sessions(records, |record| record.status_class == "confirmed");
    let confirmed_records = confirmed.len();
    let confirmed_trade_events = user_events
        .trade_events
        .iter()
        .filter(|event| event.status_class.as_deref() == Some("confirmed"))
        .count();
    let pending_trade_events = user_events
        .trade_events
        .iter()
        .filter(|event| event.status_class.as_deref() == Some("pending"))
        .count();
    let failed_trade_events = user_events
        .trade_events
        .iter()
        .filter(|event| event.status_class.as_deref() == Some("failed"))
        .count();

    let mut blockers = user_events.blockers;
    if confirmed_records > 0 && confirmed_trade_events == 0 {
        push_unique_finality_blocker(&mut blockers, "missing_user_channel_confirmed_trade_quorum");
    }

    let mut confirmed_records_with_user_join_key = 0usize;
    let mut matched_confirmed_records = 0usize;
    let mut consumed_event_indices = HashSet::new();
    for (_, record) in confirmed {
        if !record_has_user_channel_join_key(record) {
            push_unique_finality_blocker(
                &mut blockers,
                format!(
                    "confirmed_rfq_user_channel_join_key_missing:{}",
                    record.finality_id
                ),
            );
            continue;
        }
        confirmed_records_with_user_join_key += 1;
        let matching_confirmed =
            user_events
                .trade_events
                .iter()
                .enumerate()
                .find(|(index, event)| {
                    !consumed_event_indices.contains(index)
                        && event.status_class.as_deref() == Some("confirmed")
                        && record_matches_user_channel_trade(record, event)
                });
        let matching_event = matching_confirmed.or_else(|| {
            user_events
                .trade_events
                .iter()
                .enumerate()
                .find(|(index, event)| {
                    !consumed_event_indices.contains(index)
                        && record_matches_user_channel_trade(record, event)
                })
        });
        match matching_event {
            Some((event_index, event)) => {
                consumed_event_indices.insert(event_index);
                match event.status_class.as_deref() {
                    Some("confirmed") => matched_confirmed_records += 1,
                    Some("failed") => push_unique_finality_blocker(
                        &mut blockers,
                        format!(
                            "confirmed_rfq_user_channel_trade_failed:{}",
                            record.finality_id
                        ),
                    ),
                    Some("pending") => push_unique_finality_blocker(
                        &mut blockers,
                        format!(
                            "confirmed_rfq_user_channel_trade_pending:{}",
                            record.finality_id
                        ),
                    ),
                    _ => push_unique_finality_blocker(
                        &mut blockers,
                        format!(
                            "confirmed_rfq_user_channel_trade_mismatch:{}",
                            record.finality_id
                        ),
                    ),
                }
            }
            None => push_unique_finality_blocker(
                &mut blockers,
                format!(
                    "confirmed_rfq_user_channel_trade_mismatch:{}",
                    record.finality_id
                ),
            ),
        }
    }

    ComboRfqUserChannelQuorum {
        events_path: events_path.display().to_string(),
        raw_events_seen: user_events.raw_events_seen,
        parsed_trade_events: user_events.trade_events.len(),
        confirmed_trade_events,
        pending_trade_events,
        failed_trade_events,
        malformed_event_lines: user_events.malformed_event_lines,
        ignored_event_lines: user_events.ignored_event_lines,
        confirmed_records_with_user_join_key,
        matched_confirmed_records,
        blockers,
    }
}

fn record_has_user_channel_join_key(record: &ComboRfqFinalityRecord) -> bool {
    required_text_field_present(record.order_hash.as_deref())
        || required_text_field_present(record.transaction_hash.as_deref())
        || required_text_field_present(record.client_request_id.as_deref())
        || (required_text_field_present(record.rfq_id.as_deref())
            && required_text_field_present(record.quote_id.as_deref()))
}

fn record_matches_user_channel_trade(
    record: &ComboRfqFinalityRecord,
    event: &UserChannelTradeEvidence,
) -> bool {
    let hash_matches = any_equal_canonical_hex(
        record.transaction_hash.as_deref(),
        &[event.transaction_hash.as_deref()],
    ) || any_equal_canonical_hex(
        record.order_hash.as_deref(),
        &[
            event.order_id.as_deref(),
            event.taker_order_id.as_deref(),
            event.maker_order_id.as_deref(),
        ],
    );

    let client_request_matches = any_equal_trimmed(
        record.client_request_id.as_deref(),
        &[event.client_request_id.as_deref()],
    );
    let rfq_quote_matches = any_equal_trimmed(record.rfq_id.as_deref(), &[event.rfq_id.as_deref()])
        && any_equal_trimmed(record.quote_id.as_deref(), &[event.quote_id.as_deref()]);
    (hash_matches || client_request_matches || rfq_quote_matches)
        && user_trade_economics_match(record, event)
}

fn user_trade_economics_match(
    record: &ComboRfqFinalityRecord,
    event: &UserChannelTradeEvidence,
) -> bool {
    economics_fields_overlap(record, event)
        && optional_text_equal(
            record.market_event_id.as_deref(),
            event.condition_id.as_deref(),
        )
        && optional_text_equal(record.token_id.as_deref(), event.asset_id.as_deref())
        && optional_side_equal(record.side.as_deref(), event.side.as_deref())
        && optional_decimal_equal(record.qty_decimal, event.size.as_deref())
        && optional_decimal_equal(record.price, event.price.as_deref())
}

fn economics_fields_overlap(
    record: &ComboRfqFinalityRecord,
    event: &UserChannelTradeEvidence,
) -> bool {
    (clean_text(record.market_event_id.as_deref()).is_some()
        && clean_text(event.condition_id.as_deref()).is_some())
        || (clean_text(record.token_id.as_deref()).is_some()
            && clean_text(event.asset_id.as_deref()).is_some())
        || (clean_text(record.side.as_deref()).is_some()
            && clean_text(event.side.as_deref()).is_some())
        || (record.qty_decimal.is_some() && clean_text(event.size.as_deref()).is_some())
        || (record.price.is_some() && clean_text(event.price.as_deref()).is_some())
}

fn optional_text_equal(left: Option<&str>, right: Option<&str>) -> bool {
    match (clean_text(left), clean_text(right)) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(&right),
        _ => true,
    }
}

fn optional_side_equal(left: Option<&str>, right: Option<&str>) -> bool {
    match (clean_text(left), clean_text(right)) {
        (Some(left), Some(right)) => normalize_side_text(&left) == normalize_side_text(&right),
        _ => true,
    }
}

fn normalize_side_text(value: &str) -> String {
    match value.trim().to_ascii_uppercase().as_str() {
        "0" => "BUY".to_string(),
        "1" => "SELL".to_string(),
        other => other.to_string(),
    }
}

fn optional_decimal_equal(left: Option<f64>, right: Option<&str>) -> bool {
    match (
        left,
        right.and_then(|value| value.trim().parse::<f64>().ok()),
    ) {
        (Some(left), Some(right)) => (left - right).abs() <= 0.000001,
        _ => true,
    }
}

fn any_equal_canonical_hex(left: Option<&str>, rights: &[Option<&str>]) -> bool {
    let Some(left) = clean_text(left) else {
        return false;
    };
    let left = canonical_hex(&left);
    rights
        .iter()
        .filter_map(|right| clean_text(*right))
        .any(|right| canonical_hex(&right) == left)
}

fn any_equal_trimmed(left: Option<&str>, rights: &[Option<&str>]) -> bool {
    let Some(left) = clean_text(left) else {
        return false;
    };
    rights
        .iter()
        .filter_map(|right| clean_text(*right))
        .any(|right| right == left)
}

fn clean_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn record_has_chain_join_key(record: &ComboRfqFinalityRecord) -> bool {
    required_text_field_present(record.order_hash.as_deref())
        && required_text_field_present(record.transaction_hash.as_deref())
        && required_text_field_present(record.side.as_deref())
        && required_text_field_present(record.token_id.as_deref())
        && required_text_field_present(record.maker_amount_filled.as_deref())
        && required_text_field_present(record.taker_amount_filled.as_deref())
        && required_text_field_present(record.fee.as_deref())
}

fn record_matches_onchain_order_filled(
    record: &ComboRfqFinalityRecord,
    event: &OrderFilledEvent,
) -> bool {
    let Some(order_hash) = record.order_hash.as_deref() else {
        return false;
    };
    if canonical_hex(order_hash) != canonical_hex(&event.order_hash.to_string()) {
        return false;
    }

    let Some(transaction_hash) = record.transaction_hash.as_deref() else {
        return false;
    };
    let Some(event_transaction_hash) = event.transaction_hash.as_deref() else {
        return false;
    };
    if canonical_hex(transaction_hash) != canonical_hex(event_transaction_hash) {
        return false;
    }

    let Some(side) = record.side.as_deref() else {
        return false;
    };
    if parse_side(side) != event.side {
        return false;
    }

    let Some(token_id) = record.token_id.as_deref() else {
        return false;
    };
    if parse_u256_decimal(token_id) != event.token_id {
        return false;
    }

    let Some(maker_amount_filled) = record.maker_amount_filled.as_deref() else {
        return false;
    };
    if parse_u256_decimal(maker_amount_filled) != Some(event.maker_amount_filled) {
        return false;
    }

    let Some(taker_amount_filled) = record.taker_amount_filled.as_deref() else {
        return false;
    };
    if parse_u256_decimal(taker_amount_filled) != Some(event.taker_amount_filled) {
        return false;
    }

    let Some(fee) = record.fee.as_deref() else {
        return false;
    };
    if parse_u256_decimal(fee) != Some(event.fee) {
        return false;
    }

    true
}

fn required_text_field_present(value: Option<&str>) -> bool {
    value.map(|value| !value.trim().is_empty()).unwrap_or(false)
}

fn canonical_hex(value: &str) -> String {
    let value = value.trim().to_ascii_lowercase();
    if value.starts_with("0x") {
        value
    } else {
        format!("0x{value}")
    }
}

fn parse_side(value: &str) -> Option<u8> {
    match value.trim().to_ascii_uppercase().as_str() {
        "0" | "BUY" => Some(0),
        "1" | "SELL" => Some(1),
        _ => None,
    }
}

fn parse_u256_decimal(value: &str) -> Option<U256> {
    U256::from_str(value.trim()).ok()
}

fn configured_onchain_order_filled_account(config: &Config) -> Option<Address> {
    let funder = config.live_funder_address.trim();
    if !funder.is_empty() {
        return Address::from_str(funder).ok();
    }
    crate::live_executor::configured_live_account_address(config).ok()
}

fn push_unique_finality_blocker(blockers: &mut Vec<String>, blocker: impl Into<String>) {
    let blocker = blocker.into();
    if !blockers.contains(&blocker) {
        blockers.push(blocker);
    }
}

fn read_raw_jsonl_values(path: &Path) -> Result<Vec<Value>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut values = Vec::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("reading {} line {}", path.display(), idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str(&line)
            .with_context(|| format!("parsing {} line {}", path.display(), idx + 1))?;
        values.push(value);
    }
    Ok(values)
}

fn read_combo_rfq_onchain_order_filled_logs(path: &Path) -> Result<ParsedOnchainOrderFilledLogs> {
    let raw_logs = read_raw_jsonl_values(path)?;
    let mut parsed = ParsedOnchainOrderFilledLogs {
        raw_logs_seen: raw_logs.len(),
        logs: Vec::new(),
        blockers: Vec::new(),
    };
    for (idx, value) in raw_logs.iter().enumerate() {
        match onchain_log_summary_from_value(value) {
            Ok(log) => parsed.logs.push(log),
            Err(err) => parsed
                .blockers
                .push(format!("malformed_onchain_order_filled_log:{idx}:{err}")),
        }
    }
    Ok(parsed)
}

fn read_onchain_collector_finality_window(config: &Config) -> OnchainCollectorFinalityWindow {
    let path = config
        .diagnostics_dir
        .join(ORDER_FILLED_COLLECTOR_RUN_REPORT_FILE);
    if !path.exists() {
        let mut window = OnchainCollectorFinalityWindow::default();
        if config.onchain_order_filled_collector_enabled {
            window
                .blockers
                .push("onchain_order_filled_collector_run_report_missing".to_string());
        }
        return window;
    }
    let body = match fs::read_to_string(&path) {
        Ok(body) => body,
        Err(err) => {
            return OnchainCollectorFinalityWindow {
                blockers: vec![format!(
                    "onchain_order_filled_collector_run_report_unreadable:{err}"
                )],
                ..OnchainCollectorFinalityWindow::default()
            };
        }
    };
    let value = match serde_json::from_str::<Value>(&body) {
        Ok(value) => value,
        Err(err) => {
            return OnchainCollectorFinalityWindow {
                blockers: vec![format!(
                    "onchain_order_filled_collector_run_report_malformed:{err}"
                )],
                ..OnchainCollectorFinalityWindow::default()
            };
        }
    };
    let latest_block = json_value_u64(value.get("latest_block"));
    let finalized_block = json_value_u64(value.get("finalized_block"));
    let finalized_lag_blocks = json_value_u64(value.get("finalized_lag_blocks"));
    let mut blockers = Vec::new();
    if config.onchain_order_filled_collector_enabled && finalized_block.is_none() {
        blockers.push("onchain_order_filled_collector_finalized_block_missing".to_string());
    }
    OnchainCollectorFinalityWindow {
        latest_block,
        finalized_block,
        finalized_lag_blocks,
        blockers,
    }
}

fn json_value_u64(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            let hex = trimmed
                .strip_prefix("0x")
                .or_else(|| trimmed.strip_prefix("0X"));
            match hex {
                Some(hex) => u64::from_str_radix(hex, 16).ok(),
                None => trimmed.parse::<u64>().ok(),
            }
        }
        _ => None,
    }
}

fn read_combo_rfq_user_channel_events(path: &Path) -> Result<ParsedUserChannelTradeEvents> {
    if !path.exists() {
        return Ok(ParsedUserChannelTradeEvents::default());
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut parsed = ParsedUserChannelTradeEvents::default();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("reading {} line {}", path.display(), idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        parsed.raw_events_seen += 1;
        let value = match serde_json::from_str::<Value>(&line) {
            Ok(value) => value,
            Err(err) => {
                parsed.malformed_event_lines += 1;
                parsed
                    .blockers
                    .push(format!("malformed_user_channel_event:{idx}:{err}"));
                continue;
            }
        };
        let before = parsed.trade_events.len();
        append_user_channel_trade_evidence(&value, &mut parsed.trade_events);
        if parsed.trade_events.len() == before {
            parsed.ignored_event_lines += 1;
        }
    }
    Ok(parsed)
}

fn append_user_channel_trade_evidence(value: &Value, events: &mut Vec<UserChannelTradeEvidence>) {
    if let Value::Array(items) = value {
        for item in items {
            append_user_channel_trade_evidence(item, events);
        }
        return;
    }
    if let Some(event) = user_channel_trade_evidence_from_value(value) {
        events.push(event);
    }
}

fn user_channel_trade_evidence_from_value(value: &Value) -> Option<UserChannelTradeEvidence> {
    let event_type = text_value_or_raw(value, &["event_type"]).or_else(|| {
        let typ = text_value_or_raw(value, &["type"])?;
        typ.eq_ignore_ascii_case("TRADE")
            .then(|| "trade".to_string())
    })?;
    if !event_type.eq_ignore_ascii_case("trade") {
        return None;
    }
    let status =
        text_value_or_raw(value, &["status"]).map(|status| normalize_user_trade_status(&status));
    let status_class = text_value_or_raw(value, &["status_class"]).or_else(|| {
        status
            .as_deref()
            .map(user_trade_status_class)
            .map(str::to_string)
    });
    Some(UserChannelTradeEvidence {
        trade_id: text_value_or_raw(value, &["id", "trade_id"]),
        order_id: text_value_or_raw(value, &["order_id"]),
        taker_order_id: text_value_or_raw(value, &["taker_order_id"]),
        maker_order_id: text_value_or_raw(value, &["maker_order_id", "makerOrderId"]),
        transaction_hash: text_value(
            value,
            &["transactionHash", "transaction_hash", "txHash", "tx_hash"],
        )
        .or_else(|| {
            text_value_or_raw(
                value,
                &["transactionHash", "transaction_hash", "txHash", "tx_hash"],
            )
        }),
        rfq_id: text_value_or_raw(value, &["rfqId", "rfq_id"]),
        quote_id: text_value_or_raw(value, &["quoteId", "quote_id"]),
        client_request_id: text_value_or_raw(value, &["clientRequestId", "client_request_id"]),
        condition_id: text_value_or_raw(value, &["market", "condition_id"]),
        asset_id: text_value_or_raw(value, &["asset_id", "tokenId", "token_id"]),
        side: text_value_or_raw(value, &["side"]).map(|side| side.to_ascii_uppercase()),
        size: text_value_or_raw(value, &["size", "matched_amount"]),
        price: text_value_or_raw(value, &["price"]),
        status,
        status_class,
    })
}

fn text_value_or_raw(value: &Value, keys: &[&str]) -> Option<String> {
    text_value(value, keys).or_else(|| value.get("raw").and_then(|raw| text_value(raw, keys)))
}

fn normalize_user_trade_status(status: &str) -> String {
    let mut normalized = status.trim().to_ascii_uppercase();
    if let Some(stripped) = normalized.strip_prefix("TRADE_STATUS_") {
        normalized = stripped.to_string();
    }
    normalized
}

fn user_trade_status_class(status: &str) -> &'static str {
    match status {
        "CONFIRMED" => "confirmed",
        "FAILED" => "failed",
        "MATCHED" | "MATCHED_NOT_BROADCASTED" | "MINED" | "RETRYING" => "pending",
        _ => "unknown",
    }
}

fn read_combo_rfq_finality_records(path: &Path) -> Result<Vec<ComboRfqFinalityRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut records = Vec::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("reading {} line {}", path.display(), idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str(&line)
            .with_context(|| format!("parsing {} line {}", path.display(), idx + 1))?;
        records.push(record);
    }
    Ok(records)
}

fn append_combo_rfq_finality_records(
    path: &Path,
    records: &[ComboRfqFinalityRecord],
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating diagnostics directory {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening Combo/RFQ finality journal {}", path.display()))?;
    for record in records {
        let line = serde_json::to_string(record)?;
        writeln!(file, "{line}")
            .with_context(|| format!("writing Combo/RFQ finality journal {}", path.display()))?;
    }
    Ok(())
}

fn read_combo_rfq_stream_checkpoint(path: &Path) -> Result<ComboRfqStreamCheckpoint> {
    if !path.exists() {
        return Ok(ComboRfqStreamCheckpoint::default());
    }
    let body = fs::read_to_string(path)
        .with_context(|| format!("reading Combo/RFQ stream checkpoint {}", path.display()))?;
    serde_json::from_str(&body)
        .with_context(|| format!("parsing Combo/RFQ stream checkpoint {}", path.display()))
}

fn ensure_combo_rfq_stream_checkpoint(path: &Path, report: &ComboRfqFinalityReport) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating diagnostics directory {}", parent.display()))?;
    }
    let checkpoint = ComboRfqStreamCheckpoint {
        last_rfq_event_at: report
            .latest_terminal_at
            .clone()
            .or_else(|| Some(report.generated_at.clone())),
        last_dropcopy_event_at: None,
        last_dropcopy_resume_token: None,
        last_heartbeat_at: Some(report.generated_at.clone()),
        reconnect_count: 0,
        gap_count: 0,
    };
    let body = serde_json::to_string_pretty(&checkpoint)?;
    fs::write(path, body)
        .with_context(|| format!("writing Combo/RFQ stream checkpoint {}", path.display()))
}

fn build_combo_rfq_lifecycle_summary(
    records: &[ComboRfqFinalityRecord],
) -> ComboRfqLifecycleSummary {
    let session_resolver = RfqFinalitySessionResolver::from_records(records);
    let mut by_session: HashMap<String, Vec<&ComboRfqFinalityRecord>> = HashMap::new();
    let mut blockers = Vec::new();
    let mut missing_terminal_sessions = 0usize;
    for record in records {
        if let Some(session_key) = session_resolver.resolve(record) {
            by_session.entry(session_key).or_default().push(record);
        } else if combo_rfq_status_class_is_terminal(&record.status_class) {
            missing_terminal_sessions += 1;
            let reason = if session_resolver.pair_is_ambiguous(record) {
                "ambiguous"
            } else {
                "missing"
            };
            blockers.push(format!(
                "rfq_lifecycle_session_key_{reason}:{}",
                record.finality_id
            ));
        }
    }

    let mut valid_sessions = 0usize;
    let mut invalid_sessions = missing_terminal_sessions;
    let mut confirmed_without_dropcopy = 0usize;
    for (session, mut session_records) in by_session {
        let session_display = session_records
            .first()
            .map(|record| rfq_finality_session_display(record))
            .unwrap_or_else(|| session.clone());
        session_records.sort_by(|left, right| {
            parse_rfc3339_timestamp(&left.generated_at)
                .cmp(&parse_rfc3339_timestamp(&right.generated_at))
                .then_with(|| left.finality_id.cmp(&right.finality_id))
        });
        let has_terminal = session_records
            .iter()
            .any(|record| combo_rfq_status_class_is_terminal(&record.status_class));
        if !has_terminal {
            continue;
        }
        let accepted_idx = session_records
            .iter()
            .position(|record| combo_rfq_status_is_accept(&record.status));
        let pending_idx = session_records
            .iter()
            .position(|record| combo_rfq_status_is_pending_end_trade(&record.status));
        let terminal_idx = session_records
            .iter()
            .position(|record| combo_rfq_status_class_is_terminal(&record.status_class));
        let mut session_blockers = Vec::new();
        match (accepted_idx, pending_idx, terminal_idx) {
            (Some(accepted), Some(pending), Some(terminal))
                if accepted < pending && pending < terminal => {}
            (None, _, _) => session_blockers.push("missing_quote_accepted"),
            (_, None, _) => session_blockers.push("missing_quote_pending_end_trade"),
            (Some(accepted), Some(pending), Some(terminal)) => {
                if terminal <= accepted {
                    session_blockers.push("terminal_before_quote_accepted");
                }
                if terminal <= pending {
                    session_blockers.push("terminal_before_pending_end_trade");
                }
            }
            _ => session_blockers.push("invalid_lifecycle_order"),
        }
        let terminal_classes = session_records
            .iter()
            .filter(|record| combo_rfq_status_class_is_terminal(&record.status_class))
            .map(|record| record.status_class.as_str())
            .collect::<HashSet<_>>();
        if terminal_classes.len() > 1 {
            session_blockers.push("conflicting_terminal_statuses");
        }
        let confirmed = session_records
            .iter()
            .any(|record| record.status_class == "confirmed");
        if confirmed
            && !session_records
                .iter()
                .any(|record| record.status_class == "confirmed" && record_is_dropcopy(record))
        {
            confirmed_without_dropcopy += 1;
            session_blockers.push("dropcopy_missing_confirmed_trade");
        }

        if session_blockers.is_empty() {
            valid_sessions += 1;
        } else {
            invalid_sessions += 1;
            blockers.push(format!(
                "rfq_lifecycle_invalid:{session_display}:{}",
                session_blockers.join(",")
            ));
        }
    }

    ComboRfqLifecycleSummary {
        sessions: valid_sessions + invalid_sessions,
        valid_sessions,
        invalid_sessions,
        confirmed_without_dropcopy,
        blockers,
    }
}

fn rfq_finality_session_display(record: &ComboRfqFinalityRecord) -> String {
    match (
        clean_text(record.rfq_id.as_deref()),
        clean_text(record.quote_id.as_deref()),
    ) {
        (Some(rfq_id), Some(quote_id)) => format!("{rfq_id}/{quote_id}"),
        _ => clean_text(record.client_request_id.as_deref())
            .unwrap_or_else(|| record.finality_id.clone()),
    }
}

fn combo_rfq_status_is_accept(status: &str) -> bool {
    matches!(
        status,
        "ACCEPTED" | "QUOTE_ACCEPTED" | "SUBMITTED" | "MATCHED"
    )
}

fn combo_rfq_status_is_pending_end_trade(status: &str) -> bool {
    matches!(status, "QUOTE_PENDING_END_TRADE" | "PENDING_END_TRADE")
}

fn record_is_dropcopy(record: &ComboRfqFinalityRecord) -> bool {
    let source = record.source.to_ascii_lowercase();
    source.contains("dropcopy") || source.contains("trade") || source.contains("order")
}

fn normalize_combo_rfq_finality_event(value: &Value) -> Option<ComboRfqFinalityRecord> {
    let status = text_value(
        value,
        &[
            "status",
            "state",
            "eventType",
            "event_type",
            "type",
            "quoteStatus",
            "quote_status",
            "lifecycleStatus",
            "lifecycle_status",
        ],
    )?;
    let status = normalize_status(&status);
    let status_class = combo_rfq_status_class(&status).to_string();
    let rfq_id = text_value(value, &["rfqId", "rfq_id"]);
    let quote_id = text_value(value, &["quoteId", "quote_id"]);
    let client_request_id = text_value(value, &["clientRequestId", "client_request_id"]);
    let maker_id = text_value(value, &["makerId", "maker_id", "maker"]);
    let symbol = text_value(value, &["symbol", "market", "asset"]);
    let market_event_id = text_value(value, &["marketEventId", "market_event_id", "eventSlug"]);
    let source_timestamp = text_value(value, &["generatedAt", "generated_at", "timestamp", "time"]);
    let generated_at = source_timestamp
        .clone()
        .unwrap_or_else(|| Utc::now().to_rfc3339());
    let mut source = text_value(value, &["source", "stream", "channel"])
        .unwrap_or_else(|| "diagnostics_jsonl".to_string());
    if source_timestamp.is_none() {
        source.push_str(":missing_source_timestamp");
    }
    let finality_id =
        text_value(value, &["id", "eventId", "event_id", "sequence"]).unwrap_or_else(|| {
            stable_finality_id(
                rfq_id.as_deref(),
                quote_id.as_deref(),
                client_request_id.as_deref(),
                &status,
                &generated_at,
            )
        });

    Some(ComboRfqFinalityRecord {
        finality_id,
        generated_at,
        source,
        rfq_id,
        quote_id,
        client_request_id,
        maker_id,
        symbol,
        market_event_id,
        order_hash: text_value(value, &["orderHash", "order_hash"]),
        transaction_hash: text_value(
            value,
            &["transactionHash", "transaction_hash", "txHash", "tx_hash"],
        ),
        side: text_value(value, &["side", "orderSide", "order_side"]),
        token_id: text_value(value, &["tokenId", "token_id"]),
        maker_amount_filled: text_value(
            value,
            &["makerAmountFilled", "maker_amount_filled", "makerAmount"],
        ),
        taker_amount_filled: text_value(
            value,
            &["takerAmountFilled", "taker_amount_filled", "takerAmount"],
        ),
        fee: text_value(value, &["fee", "feeAmount", "fee_amount"]),
        status,
        status_class,
        quote_age_ms: number_value(value, &["quoteAgeMs", "quote_age_ms", "ageMs", "age_ms"])
            .map(|value| value as i64),
        price: number_value(value, &["price", "quotePrice", "quote_price", "limitPrice"]),
        qty_decimal: number_value(value, &["qtyDecimal", "qty_decimal", "quantity", "qty"]),
        expected_edge_usd: number_value(value, &["expectedEdgeUsd", "expected_edge_usd"]),
        realized_ev_usd: number_value(
            value,
            &["realizedEvUsd", "realized_ev_usd", "realizedPnlUsd"],
        ),
    })
}

fn write_terminal_maker_records(
    config: &Config,
    records: &[ComboRfqFinalityRecord],
) -> Result<usize> {
    let mut recorded_sessions = existing_combo_rfq_finality_maker_sessions(config)?;
    let (terminal_sessions, _) = latest_unique_rfq_sessions(records, |record| {
        combo_rfq_status_class_is_terminal(&record.status_class)
    });
    let mut written = 0usize;
    for (session_key, record) in terminal_sessions {
        if !recorded_sessions.insert(session_key.clone()) {
            continue;
        }
        let event_id = resolved_combo_rfq_finality_event_id(config, record)?;
        append_combo_rfq_maker_journal_record(
            config,
            &ComboRfqMakerJournalRecord {
                generated_at: Utc::now().to_rfc3339(),
                maker_id: record.maker_id.clone(),
                quote_id: record
                    .quote_id
                    .clone()
                    .unwrap_or_else(|| record.finality_id.clone()),
                rfq_id: record.rfq_id.clone(),
                event_id,
                quote_age_ms: record.quote_age_ms,
                expected_edge_usd: record.expected_edge_usd,
                selected: true,
                accepted: record.status_class == "confirmed",
                terminal_status: Some(record.status.clone()),
                realized_ev_usd: record.realized_ev_usd,
                blockers: if record.status_class == "confirmed" {
                    Vec::new()
                } else {
                    vec![format!("rfq_finality_terminal:{}", record.status)]
                },
                notes: vec![
                    "source=rfq_finality".into(),
                    format!("session_key={session_key}"),
                    format!("finality_id={}", record.finality_id),
                    format!("status_class={}", record.status_class),
                ],
            },
        )?;
        written += 1;
    }
    Ok(written)
}

fn existing_combo_rfq_finality_maker_sessions(config: &Config) -> Result<HashSet<String>> {
    let path = config.diagnostics_dir.join("combo_rfq_maker_journal.jsonl");
    let mut sessions = HashSet::new();
    for value in read_raw_jsonl_values(&path)? {
        let notes = value
            .get("notes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        if !notes.contains(&"source=rfq_finality") {
            continue;
        }
        if let Some(session_key) = notes
            .iter()
            .find_map(|note| note.strip_prefix("session_key="))
        {
            sessions.insert(session_key.to_string());
            continue;
        }
        if let Some(session_key) = rfq_finality_session_key_from_parts(
            text_value(&value, &["rfq_id", "rfqId"]).as_deref(),
            text_value(&value, &["quote_id", "quoteId"]).as_deref(),
            None,
        ) {
            sessions.insert(session_key);
        }
    }
    Ok(sessions)
}

fn write_terminal_replay_labels(
    config: &Config,
    records: &[ComboRfqFinalityRecord],
) -> Result<usize> {
    let mut replay_records = Vec::new();
    let (terminal_sessions, _) = latest_unique_rfq_sessions(records, |record| {
        combo_rfq_status_class_is_terminal(&record.status_class)
    });
    for (session_key, record) in terminal_sessions {
        let outcome_label = if record.status_class == "confirmed" {
            "both_confirmed"
        } else {
            "matched_then_failed"
        };
        let mut notes = vec![
            "source=rfq_finality".into(),
            format!("session_key={session_key}"),
            format!("finality_id={}", record.finality_id),
            format!("status={}", record.status),
            format!("status_class={}", record.status_class),
        ];
        if let Some(execution_id) = combo_rfq_finality_execution_id(record) {
            notes.push(format!("execution_id={execution_id}"));
        }
        replay_records.push(LiveRouteReplayRecord {
            label_id: Some(format!("combo_rfq_finality_session:{session_key}")),
            generated_at: Utc::now().to_rfc3339(),
            event_id: resolved_combo_rfq_finality_event_id(config, record)?,
            route: COMBO_RFQ_ROUTE.into(),
            outcome_label: outcome_label.into(),
            realized_ev_usd: record.realized_ev_usd,
            toxicity_score: None,
            notes,
        });
    }
    append_live_route_replay_records_deduped(config, &replay_records)
}

fn write_terminal_realized_pnl_records(
    config: &Config,
    records: &[ComboRfqFinalityRecord],
) -> Result<usize> {
    let mut written = 0usize;
    for record in records
        .iter()
        .filter(|record| combo_rfq_status_class_is_terminal(&record.status_class))
    {
        let Some(realized_ev_usd) = record.realized_ev_usd else {
            continue;
        };
        let condition_id = resolved_combo_rfq_finality_event_id(config, record)?;
        let transaction_hash = record
            .transaction_hash
            .clone()
            .filter(|hash| !hash.trim().is_empty())
            .unwrap_or_else(|| record.finality_id.clone());
        let pnl_record = ComboRfqRealizedPnlRecord {
            timestamp: Utc::now().to_rfc3339(),
            source: "combo_rfq_finality".into(),
            execution_id: combo_rfq_finality_execution_id(record),
            closeout_action_id: format!("combo_rfq_finality:{}", record.finality_id),
            condition_id,
            action: format!("combo_rfq_{}", record.status_class),
            transaction_hash,
            block_number: None,
            finality_id: record.finality_id.clone(),
            rfq_id: record.rfq_id.clone(),
            quote_id: record.quote_id.clone(),
            maker_id: record.maker_id.clone(),
            status: record.status.clone(),
            status_class: record.status_class.clone(),
            realized_ev_usd,
            expected_edge_usd: record.expected_edge_usd,
            price: record.price,
            qty_decimal: record.qty_decimal,
            order_hash: record.order_hash.clone(),
            token_id: record.token_id.clone(),
            fee: record.fee.clone(),
        };
        if append_combo_rfq_realized_pnl_record(config, &pnl_record)? {
            written += 1;
        }
    }
    Ok(written)
}

fn combo_rfq_finality_execution_id(record: &ComboRfqFinalityRecord) -> Option<String> {
    record
        .client_request_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| match (&record.rfq_id, &record.quote_id) {
            (Some(rfq_id), Some(quote_id))
                if !rfq_id.trim().is_empty() && !quote_id.trim().is_empty() =>
            {
                Some(format!("{rfq_id}:{quote_id}"))
            }
            _ => None,
        })
}

fn write_terminal_execution_records(
    config: &Config,
    records: &[ComboRfqFinalityRecord],
) -> Result<usize> {
    let mut written = 0usize;
    for record in records
        .iter()
        .filter(|record| combo_rfq_status_class_is_terminal(&record.status_class))
    {
        let mut status = match record.status_class.as_str() {
            "confirmed" => "finality_confirmed_exposure_retained",
            "rejected" => "finality_rejected_released",
            "failed" => "finality_failed_exposure_retained",
            "abnormal" => "finality_abnormal_exposure_retained",
            _ => continue,
        };
        let event_id = resolved_combo_rfq_finality_event_id(config, record)?;
        let (reserve_amount_usd, reserve_amount_source) =
            resolved_combo_rfq_finality_reserve_amount_usd(config, record)?;
        let mut blockers = if record.status_class == "confirmed" {
            Vec::new()
        } else {
            vec![format!("rfq_finality_terminal:{}", record.status)]
        };
        let release_exposure =
            combo_rfq_finality_status_class_releases_exposure(&record.status_class)
                && reserve_amount_usd > 0.0
                && reserve_amount_source == "execution_journal";
        if record.status_class == "rejected" && !release_exposure {
            status = "finality_rejected_exposure_retained";
            blockers.push("rfq_finality_execution_journal_match_missing".to_string());
            blockers
                .push("exposure_must_remain_reserved_until_finality_or_manual_review".to_string());
        }
        if record.status_class == "failed" {
            blockers.push("rfq_finality_failed_exposure_retained_until_manual_review".to_string());
            blockers
                .push("exposure_must_remain_reserved_until_finality_or_manual_review".to_string());
        }
        append_combo_rfq_finality_execution_record(
            config,
            event_id.clone(),
            record.client_request_id.clone(),
            record.rfq_id.clone(),
            record.quote_id.clone(),
            record.maker_id.clone(),
            status.into(),
            serde_json::json!({
                "finality_id": record.finality_id.clone(),
                "status": record.status.clone(),
                "status_class": record.status_class.clone(),
                "realized_ev_usd": record.realized_ev_usd,
                "order_hash": record.order_hash.clone(),
                "transaction_hash": record.transaction_hash.clone(),
                "reserve_amount_usd": reserve_amount_usd,
                "reserve_amount_source": reserve_amount_source,
            }),
            blockers,
        )?;
        if release_exposure {
            append_exposure_ledger_delta(
                &config.diagnostics_dir,
                &event_id,
                -reserve_amount_usd,
                "released",
                "rfq_finality",
            )?;
        }
        written += 1;
    }
    Ok(written)
}

fn combo_rfq_finality_status_class_releases_exposure(status_class: &str) -> bool {
    status_class == "rejected"
}

fn fallback_combo_rfq_finality_event_id(record: &ComboRfqFinalityRecord) -> String {
    record
        .market_event_id
        .clone()
        .or_else(|| record.rfq_id.clone())
        .unwrap_or_else(|| record.finality_id.clone())
}

fn resolved_combo_rfq_finality_event_id(
    config: &Config,
    record: &ComboRfqFinalityRecord,
) -> Result<String> {
    let event_id = resolve_combo_rfq_execution_event_id(
        config,
        record.client_request_id.as_deref(),
        record.rfq_id.as_deref(),
        record.quote_id.as_deref(),
    )
    .with_context(|| {
        format!(
            "resolving Combo/RFQ execution event id for finality_id={}",
            record.finality_id
        )
    })?
    .or_else(|| record.market_event_id.clone())
    .map(|event_id| event_id.trim().to_string())
    .filter(|event_id| !event_id.is_empty())
    .unwrap_or_else(|| fallback_combo_rfq_finality_event_id(record));
    Ok(event_id)
}

fn resolved_combo_rfq_finality_reserve_amount_usd(
    config: &Config,
    record: &ComboRfqFinalityRecord,
) -> Result<(f64, &'static str)> {
    let journaled = resolve_combo_rfq_execution_reserve_amount_usd(
        config,
        record.client_request_id.as_deref(),
        record.rfq_id.as_deref(),
        record.quote_id.as_deref(),
    )
    .with_context(|| {
        format!(
            "resolving Combo/RFQ reserve amount for finality_id={}",
            record.finality_id
        )
    })?;
    if let Some(amount) = journaled.filter(|amount| amount.is_finite() && *amount > 0.0) {
        return Ok((amount, "execution_journal"));
    }
    if config.live_trade_position_size_usd.is_finite() && config.live_trade_position_size_usd > 0.0
    {
        return Ok((config.live_trade_position_size_usd, "config_fallback"));
    }
    Ok((0.0, "unavailable"))
}

fn combo_rfq_status_class(status: &str) -> &'static str {
    match status {
        "FILLED" | "FILL" | "CONFIRMED" | "SETTLED" | "BOTH_CONFIRMED" | "QUOTE_FILLED"
        | "QUOTE_CONFIRMED" | "QUOTE_SETTLED" | "TRADE_CONFIRMED" => "confirmed",
        "REJECTED" | "DONE_AWAY" | "QUOTE_DONE_AWAY" | "LAST_LOOK_REJECTED" | "MAKER_REJECTED" => {
            "rejected"
        }
        "EXPIRED" | "FAILED" | "CANCELLED" | "CANCELED" | "QUOTE_EXPIRED" => "failed",
        "PARTIAL" | "PARTIALLY_FILLED" | "QUOTE_PARTIAL" | "ONE_LEG" | "GHOST_REVERT"
        | "REVERTED" | "REVERT" => "abnormal",
        "PENDING"
        | "ACCEPTED"
        | "QUOTE_ACCEPTED"
        | "SUBMITTED"
        | "MATCHED"
        | "QUOTE_PENDING_END_TRADE"
        | "PENDING_END_TRADE" => "pending",
        _ => "unknown",
    }
}

fn combo_rfq_status_class_is_terminal(status_class: &str) -> bool {
    matches!(
        status_class,
        "confirmed" | "rejected" | "failed" | "abnormal"
    )
}

fn normalize_status(status: &str) -> String {
    status.trim().to_ascii_uppercase().replace(['-', ' '], "_")
}

fn stable_finality_id(
    rfq_id: Option<&str>,
    quote_id: Option<&str>,
    client_request_id: Option<&str>,
    status: &str,
    generated_at: &str,
) -> String {
    let mut key = String::new();
    key.push_str(rfq_id.unwrap_or(""));
    key.push(':');
    key.push_str(quote_id.unwrap_or(""));
    key.push(':');
    key.push_str(client_request_id.unwrap_or(""));
    key.push(':');
    key.push_str(status);
    key.push(':');
    key.push_str(generated_at);
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("rfq-finality-{hash:016x}")
}

fn text_value(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(text) = text_value_at(value, key) {
            return Some(text);
        }
        for container in ["payload", "data", "event", "rfq", "quote", "order", "trade"] {
            if let Some(nested) = value
                .get(container)
                .and_then(|nested| text_value_at(nested, key))
            {
                return Some(nested);
            }
        }
    }
    None
}

fn text_value_at(value: &Value, key: &str) -> Option<String> {
    let field = value.get(key)?;
    let text = match field {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(value) => value.to_string(),
        _ => return None,
    };
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn number_value(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(number) = number_value_at(value, key) {
            return Some(number);
        }
        for container in ["payload", "data", "event", "rfq", "quote", "order", "trade"] {
            if let Some(nested) = value
                .get(container)
                .and_then(|nested| number_value_at(nested, key))
            {
                return Some(nested);
            }
        }
    }
    None
}

fn number_value_at(value: &Value, key: &str) -> Option<f64> {
    let field = value.get(key)?;
    let parsed = match field {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    };
    parsed.filter(|value| value.is_finite())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::combo_rfq_client::{
        append_combo_rfq_execution_journal_record, ComboRfqCreateRequest,
        ComboRfqExecutionJournalRecord, ComboRfqLegRequest,
    };
    use crate::onchain_fills::order_filled_v2_topic;
    use polymarket_client_sdk_v2::types::{B256, U256};
    use std::fs;

    const TEST_ORDER_HASH: &str =
        "0x0303030303030303030303030303030303030303030303030303030303030303";
    const TEST_TRANSACTION_HASH: &str = "0xabc";

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        std::env::temp_dir().join(format!("polymarket-rfq-finality-{name}-{suffix}"))
    }

    fn write_ready_checkpoint(dir: &Path) {
        let checkpoint = ComboRfqStreamCheckpoint {
            last_rfq_event_at: Some(Utc::now().to_rfc3339()),
            last_dropcopy_event_at: Some(Utc::now().to_rfc3339()),
            last_dropcopy_resume_token: Some("resume-1".into()),
            last_heartbeat_at: Some(Utc::now().to_rfc3339()),
            reconnect_count: 0,
            gap_count: 0,
        };
        fs::write(
            dir.join(COMBO_RFQ_STREAM_CHECKPOINT_FILE),
            serde_json::to_string_pretty(&checkpoint).unwrap(),
        )
        .unwrap();
    }

    fn address(raw: &str) -> Address {
        Address::from_str(raw).unwrap()
    }

    #[tokio::test]
    async fn cached_stream_event_wait_wakes_matching_rfq() {
        clear_combo_rfq_stream_event_cache_for_tests();
        let rfq_id = format!(
            "rfq-wake-{}",
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
        );
        let waiter_rfq_id = rfq_id.clone();
        let waiter = tokio::spawn(async move {
            wait_for_cached_combo_rfq_stream_event(&waiter_rfq_id, Duration::from_secs(1)).await
        });
        tokio::task::yield_now().await;
        cache_combo_rfq_stream_event(&serde_json::json!({
            "rfqId": rfq_id,
            "quoteId": "quote-1",
            "generatedAt": Utc::now().to_rfc3339()
        }));
        assert!(waiter.await.unwrap());
    }

    fn write_onchain_order_filled_log(dir: &Path, account: Address) {
        let taker = address("0x0000000000000000000000000000000000000002");
        let mut data = Vec::new();
        push_u256_word(&mut data, U256::ZERO);
        push_u256_word(&mut data, U256::from(202u64));
        push_u256_word(&mut data, U256::from(750_000u64));
        push_u256_word(&mut data, U256::from(1_000_000u64));
        push_u256_word(&mut data, U256::ZERO);
        push_b256_word(&mut data, B256::ZERO);
        push_b256_word(&mut data, B256::ZERO);
        let log = serde_json::json!({
            "address": "0x00000000000000000000000000000000000000ee",
            "topics": [
                order_filled_v2_topic().to_string(),
                TEST_ORDER_HASH,
                account.into_word().to_string(),
                taker.into_word().to_string()
            ],
            "data": format!("0x{}", hex_encode_lower(&data)),
            "transactionHash": TEST_TRANSACTION_HASH,
            "blockNumber": 123
        });
        fs::write(
            dir.join(COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE),
            format!("{log}\n"),
        )
        .unwrap();
    }

    fn write_collector_run_report(dir: &Path, latest_block: u64, finalized_block: u64) {
        fs::write(
            dir.join(ORDER_FILLED_COLLECTOR_RUN_REPORT_FILE),
            serde_json::json!({
                "generated_at": Utc::now().to_rfc3339(),
                "chain_id": 137,
                "latest_block": latest_block,
                "finalized_block": finalized_block,
                "finalized_lag_blocks": latest_block.saturating_sub(finalized_block),
                "from_block": finalized_block.saturating_sub(512),
                "to_block": finalized_block,
                "filters_sent": 2,
                "raw_logs_fetched": 1,
                "logs_appended": 1,
                "decoded_order_filled_logs": 1,
                "account_order_filled_logs": 1,
                "output_path": dir.join(COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE).display().to_string(),
                "report_path": dir.join(ORDER_FILLED_COLLECTOR_RUN_REPORT_FILE).display().to_string(),
                "status": "collected",
                "blockers": []
            })
            .to_string(),
        )
        .unwrap();
    }

    fn write_user_trade(dir: &Path, status: &str) {
        let event = serde_json::json!({
            "event_type": "trade",
            "id": "trade-rfq",
            "taker_order_id": TEST_ORDER_HASH,
            "transaction_hash": TEST_TRANSACTION_HASH,
            "rfq_id": "rfq-1",
            "quote_id": "quote-1",
            "market": "event-1",
            "asset_id": "202",
            "side": "BUY",
            "size": "10",
            "price": "0.75",
            "status": status
        });
        crate::user_channel::append_live_user_events_from_payload(dir, &event.to_string()).unwrap();
    }

    fn append_pending_execution_with_reserve(
        cfg: &Config,
        event_id: &str,
        client_request_id: &str,
        rfq_id: &str,
        quote_id: &str,
        maker_id: &str,
        reserve_amount_usd: f64,
    ) {
        append_combo_rfq_execution_journal_record(
            cfg,
            &ComboRfqExecutionJournalRecord {
                generated_at: Utc::now().to_rfc3339(),
                event_id: event_id.into(),
                stage: "accept_quote".into(),
                status: "accepted_pending_finality".into(),
                client_request_id: client_request_id.into(),
                rfq_id: Some(rfq_id.into()),
                quote_id: Some(quote_id.into()),
                maker_id: Some(maker_id.into()),
                request: Some(ComboRfqCreateRequest {
                    qty_decimal: None,
                    cash_order_qty: Some(format!("{reserve_amount_usd:.6}")),
                    legs: vec![ComboRfqLegRequest {
                        symbol: "a".into(),
                        side: "SIDE_BUY".into(),
                    }],
                    side: "SIDE_BUY".into(),
                    client_request_id: client_request_id.into(),
                    expiration_time: "2026-01-01T00:00:00Z".into(),
                }),
                selected_quote: None,
                accept_request: None,
                response: Some(serde_json::json!({
                    "rfqId": rfq_id,
                    "quoteId": quote_id
                })),
                error: None,
                blockers: Vec::new(),
                note: "test accepted_pending_finality".into(),
            },
        )
        .unwrap();
    }

    fn push_u256_word(bytes: &mut Vec<u8>, value: U256) {
        bytes.extend_from_slice(&value.to_be_bytes::<32>());
    }

    fn push_b256_word(bytes: &mut Vec<u8>, value: B256) {
        bytes.extend_from_slice(value.as_slice());
    }

    fn hex_encode_lower(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        out
    }

    #[test]
    fn rfq_finality_ingests_filled_event_and_writes_replay_and_maker_labels() {
        let dir = temp_dir("filled");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        write_user_trade(&dir, "CONFIRMED");
        let accepted_at = Utc::now();
        let pending_at = accepted_at + chrono::Duration::milliseconds(1);
        let filled_at = accepted_at + chrono::Duration::milliseconds(2);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            [
                serde_json::json!({
                    "id": "evt-accepted",
                    "timestamp": accepted_at.to_rfc3339(),
                    "rfqId": "rfq-1",
                    "quoteId": "quote-1",
                    "clientRequestId": "client-1",
                    "makerId": "maker-1",
                    "marketEventId": "event-1",
                    "status": "quote_accepted",
                    "expectedEdgeUsd": 2.5
                })
                .to_string(),
                serde_json::json!({
                    "id": "evt-pending",
                    "timestamp": pending_at.to_rfc3339(),
                    "rfqId": "rfq-1",
                    "quoteId": "quote-1",
                    "makerId": "maker-1",
                    "marketEventId": "event-1",
                    "status": "quote_pending_end_trade"
                })
                .to_string(),
                serde_json::json!({
                    "id": "evt-filled",
                    "timestamp": filled_at.to_rfc3339(),
                    "source": "dropcopy",
                    "rfqId": "rfq-1",
                    "quoteId": "quote-1",
                    "makerId": "maker-1",
                    "marketEventId": "event-1",
                    "status": "filled",
                    "expectedEdgeUsd": 2.5,
                    "realizedEvUsd": 2.1,
                    "price": "0.75",
                    "qtyDecimal": "10",
                    "orderHash": TEST_ORDER_HASH,
                    "transactionHash": TEST_TRANSACTION_HASH,
                    "side": "BUY",
                    "tokenId": "202",
                    "makerAmountFilled": "750000",
                    "takerAmountFilled": "1000000",
                    "fee": "0"
                })
                .to_string(),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        let path = write_combo_rfq_finality_report(&cfg).unwrap();
        let written_report: ComboRfqFinalityReport =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert!(path.exists());
        assert_eq!(written_report.realized_pnl_records_written, 1);
        assert_eq!(report.records_seen, 3);
        assert_eq!(report.terminal_records, 1);
        assert_eq!(report.confirmed_records, 1);
        assert_eq!(report.realized_terminal_records, 1);
        assert_eq!(report.onchain_order_filled.decoded_order_filled_logs, 1);
        assert_eq!(report.onchain_order_filled.account_order_filled_logs, 1);
        assert_eq!(report.onchain_order_filled.matched_confirmed_records, 1);
        assert_eq!(report.user_channel.confirmed_trade_events, 1);
        assert_eq!(report.user_channel.matched_confirmed_records, 1);
        assert_eq!(report.realized_pnl_ledger.ledger_records, 1);
        assert_eq!(
            report.realized_pnl_ledger.terminal_records_with_realized_ev,
            1
        );
        assert_eq!(report.realized_pnl_ledger.matched_terminal_records, 1);
        assert!(report.realized_pnl_ledger.blockers.is_empty());
        assert!(report.blockers.is_empty());
        let replay = fs::read_to_string(dir.join("live_route_replay_journal.jsonl")).unwrap();
        assert_eq!(replay.lines().count(), 1);
        let replay_record: Value = serde_json::from_str(replay.lines().next().unwrap()).unwrap();
        assert_eq!(
            replay_record["label_id"],
            "combo_rfq_finality_session:execution:8:client-1"
        );
        assert_eq!(replay_record["route"], COMBO_RFQ_ROUTE);
        assert_eq!(replay_record["outcome_label"], "both_confirmed");
        assert_eq!(replay_record["realized_ev_usd"], 2.1);
        let replay_execution_id = replay_record["notes"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .find_map(|note| note.strip_prefix("execution_id="))
            .expect("RFQ replay label execution ID");
        let maker = fs::read_to_string(dir.join("combo_rfq_maker_journal.jsonl")).unwrap();
        assert_eq!(maker.lines().count(), 1);
        let maker_record: Value = serde_json::from_str(maker.lines().next().unwrap()).unwrap();
        assert_eq!(maker_record["maker_id"], "maker-1");
        assert_eq!(maker_record["terminal_status"], "FILLED");
        let realized =
            fs::read_to_string(dir.join(crate::live_executor::LIVE_REALIZED_PNL_FILE)).unwrap();
        assert_eq!(realized.lines().count(), 1);
        let realized_record: Value =
            serde_json::from_str(realized.lines().next().unwrap()).unwrap();
        assert_eq!(realized_record["source"], "combo_rfq_finality");
        assert_eq!(realized_record["execution_id"], "rfq-1:quote-1");
        assert_eq!(
            realized_record["execution_id"].as_str(),
            Some(replay_execution_id)
        );
        assert_eq!(
            realized_record["closeout_action_id"],
            "combo_rfq_finality:evt-filled"
        );
        assert_eq!(realized_record["condition_id"], "event-1");
        assert_eq!(realized_record["action"], "combo_rfq_confirmed");
        assert_eq!(realized_record["realized_ev_usd"], 2.1);
        let execution = fs::read_to_string(dir.join("combo_rfq_execution_journal.jsonl")).unwrap();
        assert_eq!(execution.lines().count(), 1);
        let execution_record: Value =
            serde_json::from_str(execution.lines().next().unwrap()).unwrap();
        assert_eq!(
            execution_record["status"],
            "finality_confirmed_exposure_retained"
        );
        assert_eq!(execution_record["rfq_id"], "rfq-1");
        assert_eq!(execution_record["quote_id"], "quote-1");
    }

    #[test]
    fn rfq_finality_status_progression_counts_and_labels_one_session() {
        let dir = temp_dir("unique-status-progression");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let started_at = Utc::now();
        let statuses = [
            ("evt-accepted", "quote_accepted", 0),
            ("evt-pending", "quote_pending_end_trade", 1),
            ("evt-filled", "filled", 2),
            ("evt-confirmed", "confirmed", 3),
            ("evt-settled", "settled", 4),
        ];
        let events = statuses
            .into_iter()
            .map(|(id, status, offset_ms)| {
                serde_json::json!({
                    "id": id,
                    "timestamp": (started_at + chrono::Duration::milliseconds(offset_ms)).to_rfc3339(),
                    "source": "dropcopy",
                    "rfqId": "rfq-progression",
                    "quoteId": "quote-progression",
                    "clientRequestId": "client-progression",
                    "makerId": "maker-progression",
                    "marketEventId": "event-progression",
                    "status": status
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            format!("{events}\n"),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 2;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.terminal_records, 3);
        assert_eq!(report.confirmed_records, 1);
        assert!(report
            .blockers
            .contains(&"insufficient_confirmed_rfq_finality:1/2".to_string()));
        assert!(report
            .blockers
            .contains(&"insufficient_recent_confirmed_rfq_finality:1/2".to_string()));
        let replay = fs::read_to_string(dir.join("live_route_replay_journal.jsonl")).unwrap();
        assert_eq!(replay.lines().count(), 1);
        let replay_record: Value = serde_json::from_str(replay.lines().next().unwrap()).unwrap();
        assert_eq!(
            replay_record["label_id"],
            "combo_rfq_finality_session:execution:18:client-progression"
        );
        assert!(replay_record["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|note| note == "finality_id=evt-settled"));
        let maker = fs::read_to_string(dir.join("combo_rfq_maker_journal.jsonl")).unwrap();
        assert_eq!(maker.lines().count(), 1);
        let maker_record: Value = serde_json::from_str(maker.lines().next().unwrap()).unwrap();
        assert_eq!(maker_record["terminal_status"], "SETTLED");
    }

    #[test]
    fn rfq_finality_ambiguous_pair_to_execution_mapping_fails_closed() {
        let dir = temp_dir("ambiguous-session");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let started_at = Utc::now();
        let events = [
            serde_json::json!({
                "id": "evt-accepted-client-1",
                "timestamp": started_at.to_rfc3339(),
                "rfqId": "rfq-ambiguous",
                "quoteId": "quote-ambiguous",
                "clientRequestId": "client-1",
                "status": "quote_accepted"
            })
            .to_string(),
            serde_json::json!({
                "id": "evt-accepted-client-2",
                "timestamp": (started_at + chrono::Duration::milliseconds(1)).to_rfc3339(),
                "rfqId": "rfq-ambiguous",
                "quoteId": "quote-ambiguous",
                "clientRequestId": "client-2",
                "status": "quote_accepted"
            })
            .to_string(),
            serde_json::json!({
                "id": "evt-filled-ambiguous",
                "timestamp": (started_at + chrono::Duration::milliseconds(2)).to_rfc3339(),
                "source": "dropcopy",
                "rfqId": "rfq-ambiguous",
                "quoteId": "quote-ambiguous",
                "status": "filled"
            })
            .to_string(),
        ];
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            format!("{}\n", events.join("\n")),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        let report_path = write_combo_rfq_finality_report(&cfg).unwrap();
        let written_report: ComboRfqFinalityReport =
            serde_json::from_str(&fs::read_to_string(report_path).unwrap()).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(written_report.replay_labels_written, 0);
        assert_eq!(written_report.maker_records_written, 0);
        assert_eq!(report.confirmed_records, 0);
        assert!(report
            .blockers
            .contains(&"confirmed_rfq_session_key_ambiguous:evt-filled-ambiguous".to_string()));
        assert!(
            fs::read_to_string(dir.join("live_route_replay_journal.jsonl"))
                .unwrap()
                .is_empty()
        );
        assert!(!dir.join("combo_rfq_maker_journal.jsonl").exists());
    }

    #[test]
    fn rfq_finality_latest_failed_state_does_not_count_older_confirmation() {
        let dir = temp_dir("latest-failed");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let started_at = Utc::now();
        let statuses = [
            ("evt-accepted", "quote_accepted", 0),
            ("evt-pending", "quote_pending_end_trade", 1),
            ("evt-confirmed", "confirmed", 2),
            ("evt-failed", "failed", 3),
        ];
        let events = statuses
            .into_iter()
            .map(|(id, status, offset_ms)| {
                serde_json::json!({
                    "id": id,
                    "timestamp": (started_at + chrono::Duration::milliseconds(offset_ms)).to_rfc3339(),
                    "source": "dropcopy",
                    "rfqId": "rfq-latest-failed",
                    "quoteId": "quote-latest-failed",
                    "status": status
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            format!("{events}\n"),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.confirmed_records, 0);
        assert_eq!(report.latest_confirmed_at, None);
        assert!(report
            .blockers
            .contains(&"missing_confirmed_rfq_finality".to_string()));
        assert!(report
            .blockers
            .contains(&"insufficient_confirmed_rfq_finality:0/1".to_string()));
        let replay = fs::read_to_string(dir.join("live_route_replay_journal.jsonl")).unwrap();
        assert_eq!(replay.lines().count(), 1);
        let replay_record: Value = serde_json::from_str(replay.lines().next().unwrap()).unwrap();
        assert_eq!(replay_record["outcome_label"], "matched_then_failed");
        assert!(replay_record["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|note| note == "finality_id=evt-failed"));
    }

    #[test]
    fn rfq_finality_latest_pending_state_does_not_count_older_confirmation() {
        let dir = temp_dir("latest-pending");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let started_at = Utc::now();
        let statuses = [
            ("evt-accepted", "quote_accepted", 0),
            ("evt-pending-before-fill", "quote_pending_end_trade", 1),
            ("evt-confirmed", "confirmed", 2),
            ("evt-pending-latest", "quote_pending_end_trade", 3),
        ];
        let events = statuses
            .into_iter()
            .map(|(id, status, offset_ms)| {
                serde_json::json!({
                    "id": id,
                    "timestamp": (started_at + chrono::Duration::milliseconds(offset_ms)).to_rfc3339(),
                    "source": "dropcopy",
                    "rfqId": "rfq-latest-pending",
                    "quoteId": "quote-latest-pending",
                    "status": status
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            format!("{events}\n"),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.confirmed_records, 0);
        assert_eq!(report.latest_confirmed_at, None);
        assert!(report
            .blockers
            .contains(&"missing_confirmed_rfq_finality".to_string()));
        assert!(
            fs::read_to_string(dir.join("live_route_replay_journal.jsonl"))
                .unwrap()
                .is_empty()
        );
        assert!(!dir.join("combo_rfq_maker_journal.jsonl").exists());
    }

    #[test]
    fn rfq_finality_consumes_chain_and_user_evidence_once() {
        let dir = temp_dir("one-to-one-evidence");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        write_user_trade(&dir, "CONFIRMED");
        let started_at = Utc::now();
        let mut events = Vec::new();
        for execution in 1..=2 {
            let rfq_id = format!("rfq-{execution}");
            let quote_id = format!("quote-{execution}");
            events.push(
                serde_json::json!({
                    "id": format!("evt-accepted-{execution}"),
                    "timestamp": started_at.to_rfc3339(),
                    "rfqId": rfq_id,
                    "quoteId": quote_id,
                    "status": "quote_accepted"
                })
                .to_string(),
            );
            events.push(
                serde_json::json!({
                    "id": format!("evt-pending-{execution}"),
                    "timestamp": (started_at + chrono::Duration::milliseconds(1)).to_rfc3339(),
                    "rfqId": rfq_id,
                    "quoteId": quote_id,
                    "status": "quote_pending_end_trade"
                })
                .to_string(),
            );
            events.push(
                serde_json::json!({
                    "id": format!("evt-filled-{execution}"),
                    "timestamp": (started_at + chrono::Duration::milliseconds(2)).to_rfc3339(),
                    "source": "dropcopy",
                    "rfqId": rfq_id,
                    "quoteId": quote_id,
                    "marketEventId": "event-1",
                    "status": "filled",
                    "price": "0.75",
                    "qtyDecimal": "10",
                    "orderHash": TEST_ORDER_HASH,
                    "transactionHash": TEST_TRANSACTION_HASH,
                    "side": "BUY",
                    "tokenId": "202",
                    "makerAmountFilled": "750000",
                    "takerAmountFilled": "1000000",
                    "fee": "0"
                })
                .to_string(),
            );
        }
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            format!("{}\n", events.join("\n")),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 2;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.confirmed_records, 2);
        assert_eq!(
            report
                .onchain_order_filled
                .confirmed_records_with_chain_join_key,
            2
        );
        assert_eq!(report.onchain_order_filled.matched_confirmed_records, 1);
        assert!(report.onchain_order_filled.blockers.iter().any(|blocker| {
            blocker == "confirmed_rfq_onchain_order_filled_mismatch:evt-filled-2"
        }));
        assert_eq!(report.user_channel.confirmed_records_with_user_join_key, 2);
        assert_eq!(report.user_channel.matched_confirmed_records, 1);
        assert!(report
            .user_channel
            .blockers
            .iter()
            .any(|blocker| blocker == "confirmed_rfq_user_channel_trade_mismatch:evt-filled-2"));
    }

    #[test]
    fn rfq_finality_report_blocks_when_realized_pnl_ledger_missing() {
        let dir = temp_dir("missing-ledger");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        write_user_trade(&dir, "CONFIRMED");
        append_combo_rfq_finality_records(
            &dir.join(COMBO_RFQ_FINALITY_JOURNAL_FILE),
            &[ComboRfqFinalityRecord {
                finality_id: "evt-filled".into(),
                generated_at: Utc::now().to_rfc3339(),
                source: "dropcopy".into(),
                rfq_id: Some("rfq-1".into()),
                quote_id: Some("quote-1".into()),
                client_request_id: None,
                maker_id: Some("maker-1".into()),
                symbol: None,
                market_event_id: Some("event-1".into()),
                order_hash: Some(TEST_ORDER_HASH.into()),
                transaction_hash: Some(TEST_TRANSACTION_HASH.into()),
                side: Some("BUY".into()),
                token_id: Some("202".into()),
                maker_amount_filled: Some("750000".into()),
                taker_amount_filled: Some("1000000".into()),
                fee: Some("0".into()),
                status: "FILLED".into(),
                status_class: "confirmed".into(),
                quote_age_ms: None,
                price: Some(0.75),
                qty_decimal: Some(10.0),
                expected_edge_usd: Some(2.5),
                realized_ev_usd: Some(2.1),
            }],
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.realized_pnl_ledger.ledger_records, 0);
        assert_eq!(
            report.realized_pnl_ledger.terminal_records_with_realized_ev,
            1
        );
        assert_eq!(report.realized_pnl_ledger.matched_terminal_records, 0);
        assert!(report
            .blockers
            .contains(&"missing_combo_rfq_realized_pnl_ledger".to_string()));
        assert!(report
            .blockers
            .contains(&"realized_pnl_ledger_missing_finality:evt-filled".to_string()));
    }

    #[test]
    fn rfq_finality_counts_only_finalized_onchain_fills_when_collector_enabled() {
        let dir = temp_dir("finalized-onchain");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        write_collector_run_report(&dir, 130, 123);
        write_user_trade(&dir, "CONFIRMED");
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"filled","expectedEdgeUsd":2.5,"realizedEvUsd":2.1,"price":"0.75","qtyDecimal":"10","orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        cfg.onchain_order_filled_collector_enabled = true;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(
            report.onchain_order_filled.collector_latest_block,
            Some(130)
        );
        assert_eq!(
            report.onchain_order_filled.collector_finalized_block,
            Some(123)
        );
        assert_eq!(report.onchain_order_filled.matched_confirmed_records, 1);
        assert!(!report
            .onchain_order_filled
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not_finalized")));
    }

    #[test]
    fn rfq_finality_rejects_confirmed_onchain_fill_above_finalized_block() {
        let dir = temp_dir("provisional-onchain");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        write_collector_run_report(&dir, 130, 122);
        write_user_trade(&dir, "CONFIRMED");
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"filled","expectedEdgeUsd":2.5,"realizedEvUsd":2.1,"price":"0.75","qtyDecimal":"10","orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        cfg.onchain_order_filled_collector_enabled = true;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(
            report.onchain_order_filled.collector_finalized_block,
            Some(122)
        );
        assert_eq!(report.onchain_order_filled.matched_confirmed_records, 0);
        assert!(report.onchain_order_filled.blockers.iter().any(|blocker| {
            blocker
                == "confirmed_rfq_onchain_order_filled_not_finalized:evt-filled:block=123>finalized=122"
        }));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("confirmed_rfq_onchain_order_filled_not_finalized")));
    }

    #[test]
    fn rfq_finality_blocks_confirmed_event_without_source_timestamp() {
        let dir = temp_dir("missing-source-timestamp");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        write_user_trade(&dir, "CONFIRMED");
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"filled","expectedEdgeUsd":2.5,"realizedEvUsd":2.1,"price":"0.75","qtyDecimal":"10","orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.confirmed_records, 1);
        assert!(report
            .blockers
            .contains(&"missing_confirmed_rfq_finality_source_timestamps:1".to_string()));
        let record = read_combo_rfq_finality_records(
            &cfg.diagnostics_dir.join(COMBO_RFQ_FINALITY_JOURNAL_FILE),
        )
        .unwrap()
        .pop()
        .unwrap();
        assert!(record.source.contains("missing_source_timestamp"));
    }

    #[test]
    fn rfq_finality_report_blocks_confirmed_fill_without_user_channel_quorum() {
        let dir = temp_dir("missing-user-channel");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","status":"filled","realizedEvUsd":1.2,"orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.user_channel.confirmed_trade_events, 0);
        assert_eq!(report.user_channel.matched_confirmed_records, 0);
        assert!(report
            .blockers
            .contains(&"missing_user_channel_confirmed_trade_quorum".to_string()));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker
                .starts_with("confirmed_rfq_user_channel_trade_mismatch:evt-filled")));
    }

    #[test]
    fn rfq_finality_report_blocks_confirmed_fill_with_pending_user_channel_trade() {
        let dir = temp_dir("pending-user-channel");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        write_user_trade(&dir, "MATCHED");
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","status":"filled","realizedEvUsd":1.2,"orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.user_channel.pending_trade_events, 1);
        assert_eq!(report.user_channel.matched_confirmed_records, 0);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker
                .starts_with("confirmed_rfq_user_channel_trade_pending:evt-filled")));
    }

    #[test]
    fn rfq_finality_report_rejects_user_channel_rfq_id_match_with_token_mismatch() {
        let dir = temp_dir("user-channel-token-mismatch");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        let user_event = serde_json::json!({
            "event_type": "trade",
            "id": "trade-rfq",
            "rfq_id": "rfq-1",
            "quote_id": "quote-1",
            "market": "event-1",
            "asset_id": "999",
            "side": "BUY",
            "size": "10",
            "price": "0.75",
            "status": "CONFIRMED"
        });
        fs::write(dir.join(LIVE_USER_EVENTS_FILE), format!("{user_event}\n")).unwrap();
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"filled","realizedEvUsd":1.2,"price":"0.75","qtyDecimal":"10","orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.user_channel.confirmed_trade_events, 1);
        assert_eq!(report.user_channel.matched_confirmed_records, 0);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker
                .starts_with("confirmed_rfq_user_channel_trade_mismatch:evt-filled")));
    }

    #[test]
    fn rfq_finality_report_rejects_user_channel_hash_match_with_token_mismatch() {
        let dir = temp_dir("user-channel-hash-token-mismatch");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        let user_event = serde_json::json!({
            "event_type": "trade",
            "id": "trade-rfq",
            "taker_order_id": TEST_ORDER_HASH,
            "transaction_hash": TEST_TRANSACTION_HASH,
            "rfq_id": "rfq-1",
            "quote_id": "quote-1",
            "market": "event-1",
            "asset_id": "999",
            "side": "BUY",
            "size": "10",
            "price": "0.75",
            "status": "CONFIRMED"
        });
        fs::write(dir.join(LIVE_USER_EVENTS_FILE), format!("{user_event}\n")).unwrap();
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"filled","realizedEvUsd":1.2,"price":"0.75","qtyDecimal":"10","orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.user_channel.confirmed_trade_events, 1);
        assert_eq!(report.user_channel.matched_confirmed_records, 0);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker
                .starts_with("confirmed_rfq_user_channel_trade_mismatch:evt-filled")));
    }

    #[test]
    fn rfq_finality_report_rejects_single_rfq_id_user_channel_match() {
        let dir = temp_dir("user-channel-rfq-only");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        let user_event = serde_json::json!({
            "event_type": "trade",
            "id": "trade-rfq",
            "rfq_id": "rfq-1",
            "market": "event-1",
            "asset_id": "202",
            "side": "BUY",
            "size": "10",
            "price": "0.75",
            "status": "CONFIRMED"
        });
        fs::write(dir.join(LIVE_USER_EVENTS_FILE), format!("{user_event}\n")).unwrap();
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-1","quoteId":"quote-1","makerId":"maker-1","marketEventId":"event-1","status":"filled","realizedEvUsd":1.2,"price":"0.75","qtyDecimal":"10","orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.user_channel.confirmed_trade_events, 1);
        assert_eq!(report.user_channel.matched_confirmed_records, 0);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker
                .starts_with("confirmed_rfq_user_channel_trade_mismatch:evt-filled")));
    }

    #[test]
    fn rfq_finality_report_blocks_confirmed_fill_with_onchain_amount_mismatch() {
        let dir = temp_dir("amount-mismatch");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-mismatch","quoteId":"quote-mismatch","makerId":"maker-1","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-mismatch","quoteId":"quote-mismatch","makerId":"maker-1","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-mismatch","quoteId":"quote-mismatch","makerId":"maker-1","status":"filled","realizedEvUsd":1.2,"orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750001","takerAmountFilled":"1000000","fee":"0"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.onchain_order_filled.decoded_order_filled_logs, 1);
        assert_eq!(report.onchain_order_filled.matched_confirmed_records, 0);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker
                .starts_with("confirmed_rfq_onchain_order_filled_mismatch:evt-filled")));
    }

    #[test]
    fn rfq_finality_report_blocks_confirmed_fill_without_full_chain_join_fields() {
        let dir = temp_dir("partial-chain-join");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let account = address("0x0000000000000000000000000000000000000001");
        write_onchain_order_filled_log(&dir, account);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-partial","quoteId":"quote-partial","makerId":"maker-1","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-partial","quoteId":"quote-partial","makerId":"maker-1","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-partial","quoteId":"quote-partial","makerId":"maker-1","status":"filled","realizedEvUsd":1.2,"orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = account.to_string();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.onchain_order_filled.decoded_order_filled_logs, 1);
        assert_eq!(
            report
                .onchain_order_filled
                .confirmed_records_with_chain_join_key,
            0
        );
        assert_eq!(report.onchain_order_filled.matched_confirmed_records, 0);
        assert!(report
            .blockers
            .contains(&"confirmed_rfq_chain_join_key_missing:evt-filled".to_string()));
    }

    #[test]
    fn rfq_finality_report_blocks_confirmed_fill_without_onchain_order_filled_quorum() {
        let dir = temp_dir("missing-onchain");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-onchain","quoteId":"quote-onchain","makerId":"maker-1","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-onchain","quoteId":"quote-onchain","makerId":"maker-1","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-onchain","quoteId":"quote-onchain","makerId":"maker-1","status":"filled","realizedEvUsd":1.2,"orderHash":"0x0303030303030303030303030303030303030303030303030303030303030303","transactionHash":"0xabc","side":"BUY","tokenId":"202","makerAmountFilled":"750000","takerAmountFilled":"1000000","fee":"0"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = "0x0000000000000000000000000000000000000001".into();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.confirmed_records, 1);
        assert_eq!(report.onchain_order_filled.decoded_order_filled_logs, 0);
        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .contains(&"missing_onchain_order_filled_quorum".to_string()));
        assert!(report
            .blockers
            .contains(&"missing_account_onchain_order_filled_quorum".to_string()));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker
                .starts_with("confirmed_rfq_onchain_order_filled_mismatch:evt-filled")));
    }

    #[tokio::test]
    async fn rfq_finality_done_away_writes_reject_sample_and_failed_replay_label() {
        let dir = temp_dir("done-away");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-2","quoteId":"quote-2","makerId":"maker-2","marketEventId":"event-2","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-2","quoteId":"quote-2","makerId":"maker-2","marketEventId":"event-2","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-reject","rfqId":"rfq-2","quoteId":"quote-2","makerId":"maker-2","marketEventId":"event-2","status":"quote_done_away","realizedEvUsd":-0.4}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        append_pending_execution_with_reserve(
            &cfg,
            "event-2",
            "client-2",
            "rfq-2",
            "quote-2",
            "maker-2",
            cfg.live_trade_position_size_usd,
        );
        append_exposure_ledger_delta(
            &cfg.diagnostics_dir,
            "event-2",
            cfg.live_trade_position_size_usd,
            "reserved",
            "test",
        )
        .unwrap();

        let report_path = write_combo_rfq_finality_report(&cfg).unwrap();
        let report_body = fs::read_to_string(report_path).unwrap();
        let report: ComboRfqFinalityReport = serde_json::from_str(&report_body).unwrap();

        assert_eq!(report.terminal_records, 1);
        assert_eq!(report.rejected_records, 1);
        assert_eq!(report.confirmed_records, 0);
        assert!(report
            .blockers
            .contains(&"missing_confirmed_rfq_finality".to_string()));
        let replay = fs::read_to_string(dir.join("live_route_replay_journal.jsonl")).unwrap();
        let replay_record: Value = serde_json::from_str(replay.lines().next().unwrap()).unwrap();
        assert_eq!(replay_record["outcome_label"], "matched_then_failed");
        assert_eq!(replay_record["realized_ev_usd"], -0.4);
        let maker = fs::read_to_string(dir.join("combo_rfq_maker_journal.jsonl")).unwrap();
        let maker_record: Value = serde_json::from_str(maker.lines().next().unwrap()).unwrap();
        assert_eq!(maker_record["accepted"], false);
        assert!(maker_record["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| blocker == "rfq_finality_terminal:QUOTE_DONE_AWAY"));
        let exposure = crate::exposure::ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(exposure.current("event-2").await, 0.0);
    }

    #[tokio::test]
    async fn rfq_finality_partial_is_terminal_but_retains_exposure() {
        let dir = temp_dir("partial-retained");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-3","quoteId":"quote-3","makerId":"maker-3","marketEventId":"event-3","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-3","quoteId":"quote-3","makerId":"maker-3","marketEventId":"event-3","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-partial","rfqId":"rfq-3","quoteId":"quote-3","makerId":"maker-3","marketEventId":"event-3","status":"partial","realizedEvUsd":-0.2}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        append_exposure_ledger_delta(
            &cfg.diagnostics_dir,
            "event-3",
            cfg.live_trade_position_size_usd,
            "reserved",
            "test",
        )
        .unwrap();

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.terminal_records, 1);
        assert_eq!(report.abnormal_records, 1);
        assert_eq!(report.failed_records, 0);
        assert_eq!(report.rejected_records, 0);
        let execution = fs::read_to_string(dir.join("combo_rfq_execution_journal.jsonl")).unwrap();
        assert!(execution.contains("finality_abnormal_exposure_retained"));
        assert!(execution.contains("rfq_finality_terminal:PARTIAL"));
        let exposure = crate::exposure::ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(
            exposure.current("event-3").await,
            cfg.live_trade_position_size_usd
        );
    }

    #[tokio::test]
    async fn rfq_finality_release_resolves_original_event_id_from_execution_journal() {
        let dir = temp_dir("resolved-event-id");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        append_pending_execution_with_reserve(
            &cfg,
            "event-original",
            "client-original",
            "rfq-original",
            "quote-original",
            "maker-original",
            cfg.live_trade_position_size_usd,
        );
        append_exposure_ledger_delta(
            &cfg.diagnostics_dir,
            "event-original",
            cfg.live_trade_position_size_usd,
            "reserved",
            "test",
        )
        .unwrap();
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","clientRequestId":"client-original","rfqId":"rfq-original","quoteId":"quote-original","makerId":"maker-original","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-reject","clientRequestId":"client-original","rfqId":"rfq-original","quoteId":"quote-original","makerId":"maker-original","status":"quote_done_away","realizedEvUsd":-0.4}"#,
                "\n"
            ),
        )
        .unwrap();

        write_combo_rfq_finality_report(&cfg).unwrap();

        let exposure = crate::exposure::ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(exposure.current("event-original").await, 0.0);
        assert_eq!(exposure.current("rfq-original").await, 0.0);
        let execution_journal =
            fs::read_to_string(dir.join("combo_rfq_execution_journal.jsonl")).unwrap();
        let terminal_record: Value = serde_json::from_str(
            execution_journal
                .lines()
                .last()
                .expect("terminal execution journal record"),
        )
        .unwrap();
        assert_eq!(terminal_record["event_id"], "event-original");
        assert_eq!(terminal_record["status"], "finality_rejected_released");
        let replay = fs::read_to_string(dir.join("live_route_replay_journal.jsonl")).unwrap();
        let replay_record: Value = serde_json::from_str(replay.lines().next().unwrap()).unwrap();
        assert_eq!(replay_record["event_id"], "event-original");
    }

    #[tokio::test]
    async fn rfq_only_finality_does_not_release_journaled_exposure() {
        let dir = temp_dir("rfq-only-finality");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        append_pending_execution_with_reserve(
            &cfg,
            "event-original",
            "client-original",
            "rfq-original",
            "quote-original",
            "maker-original",
            25.0,
        );
        append_exposure_ledger_delta(
            &cfg.diagnostics_dir,
            "event-original",
            25.0,
            "reserved",
            "test",
        )
        .unwrap();
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","clientRequestId":"client-original","rfqId":"rfq-original","quoteId":"quote-original","makerId":"maker-original","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-reject","rfqId":"rfq-original","makerId":"maker-original","marketEventId":"event-original","status":"quote_done_away","realizedEvUsd":-0.4}"#,
                "\n"
            ),
        )
        .unwrap();

        write_combo_rfq_finality_report(&cfg).unwrap();

        let exposure = crate::exposure::ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(exposure.current("event-original").await, 25.0);
        let execution_journal =
            fs::read_to_string(dir.join("combo_rfq_execution_journal.jsonl")).unwrap();
        let terminal_record: Value = serde_json::from_str(
            execution_journal
                .lines()
                .last()
                .expect("terminal execution journal record"),
        )
        .unwrap();
        assert_eq!(
            terminal_record["status"],
            "finality_rejected_exposure_retained"
        );
        assert!(terminal_record["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| blocker == "rfq_finality_execution_journal_match_missing"));
    }

    #[tokio::test]
    async fn failed_finality_retains_journaled_exposure_for_manual_review() {
        let dir = temp_dir("failed-retains-exposure");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        append_pending_execution_with_reserve(
            &cfg,
            "event-failed",
            "client-failed",
            "rfq-failed",
            "quote-failed",
            "maker-failed",
            25.0,
        );
        append_exposure_ledger_delta(
            &cfg.diagnostics_dir,
            "event-failed",
            25.0,
            "reserved",
            "test",
        )
        .unwrap();
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","clientRequestId":"client-failed","rfqId":"rfq-failed","quoteId":"quote-failed","makerId":"maker-failed","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-failed","clientRequestId":"client-failed","rfqId":"rfq-failed","quoteId":"quote-failed","makerId":"maker-failed","status":"failed","realizedEvUsd":-0.4}"#,
                "\n"
            ),
        )
        .unwrap();

        write_combo_rfq_finality_report(&cfg).unwrap();

        let exposure = crate::exposure::ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(exposure.current("event-failed").await, 25.0);
        let execution_journal =
            fs::read_to_string(dir.join("combo_rfq_execution_journal.jsonl")).unwrap();
        let terminal_record: Value = serde_json::from_str(
            execution_journal
                .lines()
                .last()
                .expect("terminal execution journal record"),
        )
        .unwrap();
        assert_eq!(
            terminal_record["status"],
            "finality_failed_exposure_retained"
        );
        assert!(terminal_record["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| blocker == "rfq_finality_failed_exposure_retained_until_manual_review"));
        assert!(terminal_record["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| blocker
                == "exposure_must_remain_reserved_until_finality_or_manual_review"));
    }

    #[tokio::test]
    async fn rfq_finality_release_uses_journaled_reserve_amount() {
        let dir = temp_dir("journaled-reserve-amount");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.live_trade_position_size_usd = 25.0;
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        append_combo_rfq_execution_journal_record(
            &cfg,
            &ComboRfqExecutionJournalRecord {
                generated_at: Utc::now().to_rfc3339(),
                event_id: "event-reserve".into(),
                stage: "create_rfq".into(),
                status: "request_created".into(),
                client_request_id: "client-reserve".into(),
                rfq_id: Some("rfq-reserve".into()),
                quote_id: Some("quote-reserve".into()),
                maker_id: Some("maker-reserve".into()),
                request: Some(ComboRfqCreateRequest {
                    qty_decimal: None,
                    cash_order_qty: Some("17.5".into()),
                    legs: vec![ComboRfqLegRequest {
                        symbol: "a".into(),
                        side: "SIDE_BUY".into(),
                    }],
                    side: "SIDE_BUY".into(),
                    client_request_id: "client-reserve".into(),
                    expiration_time: "2026-01-01T00:00:00Z".into(),
                }),
                selected_quote: None,
                accept_request: None,
                response: Some(serde_json::json!({"rfqId":"rfq-reserve"})),
                error: None,
                blockers: Vec::new(),
                note: "test request_created".into(),
            },
        )
        .unwrap();
        append_exposure_ledger_delta(
            &cfg.diagnostics_dir,
            "event-reserve",
            17.5,
            "reserved",
            "test",
        )
        .unwrap();
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","clientRequestId":"client-reserve","rfqId":"rfq-reserve","quoteId":"quote-reserve","makerId":"maker-reserve","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-reject","clientRequestId":"client-reserve","rfqId":"rfq-reserve","quoteId":"quote-reserve","makerId":"maker-reserve","status":"quote_done_away","realizedEvUsd":-0.4}"#,
                "\n"
            ),
        )
        .unwrap();

        write_combo_rfq_finality_report(&cfg).unwrap();

        let exposure = crate::exposure::ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(exposure.current("event-reserve").await, 0.0);
        let execution_journal =
            fs::read_to_string(dir.join("combo_rfq_execution_journal.jsonl")).unwrap();
        let terminal_record: Value = serde_json::from_str(
            execution_journal
                .lines()
                .last()
                .expect("terminal execution journal record"),
        )
        .unwrap();
        assert_eq!(
            terminal_record["response"]["reserve_amount_source"],
            "execution_journal"
        );
        assert_eq!(terminal_record["response"]["reserve_amount_usd"], 17.5);
    }

    #[test]
    fn rfq_finality_ingestion_is_idempotent_for_duplicate_stream_replay() {
        let dir = temp_dir("dedupe");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-3","quoteId":"quote-3","makerId":"maker-3","marketEventId":"event-3","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-3","quoteId":"quote-3","makerId":"maker-3","marketEventId":"event-3","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-3","quoteId":"quote-3","makerId":"maker-3","marketEventId":"event-3","status":"filled","realizedEvUsd":1.2}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        write_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(
            fs::read_to_string(dir.join(COMBO_RFQ_FINALITY_JOURNAL_FILE))
                .unwrap()
                .lines()
                .count(),
            3
        );
        assert_eq!(
            fs::read_to_string(dir.join("live_route_replay_journal.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            fs::read_to_string(dir.join("combo_rfq_maker_journal.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            fs::read_to_string(dir.join(crate::live_executor::LIVE_REALIZED_PNL_FILE))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    #[test]
    fn rfq_finality_report_blocks_without_terminal_events() {
        let dir = temp_dir("pending");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-pending","rfqId":"rfq-4","quoteId":"quote-4","makerId":"maker-4","status":"quote_pending_end_trade"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.records_seen, 1);
        assert_eq!(report.pending_records, 1);
        assert!(report
            .blockers
            .contains(&"missing_terminal_rfq_finality".to_string()));
    }

    #[test]
    fn rfq_finality_success_status_is_not_confirmed_terminal() {
        let dir = temp_dir("generic-success");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-success","rfqId":"rfq-success","quoteId":"quote-success","makerId":"maker-success","status":"success","realizedEvUsd":1.0}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();
        let journal = fs::read_to_string(dir.join(COMBO_RFQ_FINALITY_JOURNAL_FILE)).unwrap();
        let record: Value = serde_json::from_str(journal.lines().next().unwrap()).unwrap();

        assert_eq!(record["status"], "SUCCESS");
        assert_eq!(record["status_class"], "unknown");
        assert_eq!(report.terminal_records, 0);
        assert_eq!(report.confirmed_records, 0);
        assert!(report
            .blockers
            .contains(&"missing_terminal_rfq_finality".to_string()));
        assert!(!dir.join("combo_rfq_execution_journal.jsonl").exists());
    }

    #[test]
    fn rfq_finality_report_blocks_stale_confirmed_samples() {
        let dir = temp_dir("stale");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","generatedAt":"2020-01-01T00:00:00Z","rfqId":"rfq-5","quoteId":"quote-5","makerId":"maker-5","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","generatedAt":"2020-01-01T00:00:01Z","rfqId":"rfq-5","quoteId":"quote-5","makerId":"maker-5","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-stale","source":"dropcopy","generatedAt":"2020-01-01T00:00:02Z","rfqId":"rfq-5","quoteId":"quote-5","makerId":"maker-5","status":"filled","realizedEvUsd":1.2}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        cfg.combo_rfq_finality_max_age_secs = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.confirmed_records, 1);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.starts_with("stale_confirmed_rfq_finality:")));
    }

    #[test]
    fn rfq_finality_report_blocks_invalid_lifecycle_order() {
        let dir = temp_dir("invalid-lifecycle");
        fs::create_dir_all(&dir).unwrap();
        write_ready_checkpoint(&dir);
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-filled","source":"dropcopy","generatedAt":"2026-01-01T00:00:00Z","rfqId":"rfq-6","quoteId":"quote-6","makerId":"maker-6","status":"filled","realizedEvUsd":1.2}"#,
                "\n",
                r#"{"id":"evt-accepted","generatedAt":"2026-01-01T00:00:01Z","rfqId":"rfq-6","quoteId":"quote-6","makerId":"maker-6","status":"quote_accepted"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        cfg.combo_rfq_finality_max_age_secs = u64::MAX;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.lifecycle.invalid_sessions, 1);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("rfq_lifecycle_invalid:rfq-6")));
    }

    #[test]
    fn rfq_finality_report_blocks_stream_gap_checkpoint() {
        let dir = temp_dir("stream-gap");
        fs::create_dir_all(&dir).unwrap();
        let checkpoint = ComboRfqStreamCheckpoint {
            last_rfq_event_at: Some(Utc::now().to_rfc3339()),
            last_dropcopy_event_at: Some(Utc::now().to_rfc3339()),
            last_dropcopy_resume_token: Some("resume-gap".into()),
            last_heartbeat_at: Some(Utc::now().to_rfc3339()),
            reconnect_count: 2,
            gap_count: 1,
        };
        fs::write(
            dir.join(COMBO_RFQ_STREAM_CHECKPOINT_FILE),
            serde_json::to_string_pretty(&checkpoint).unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE),
            concat!(
                r#"{"id":"evt-accepted","rfqId":"rfq-7","quoteId":"quote-7","makerId":"maker-7","status":"quote_accepted"}"#,
                "\n",
                r#"{"id":"evt-pending","rfqId":"rfq-7","quoteId":"quote-7","makerId":"maker-7","status":"quote_pending_end_trade"}"#,
                "\n",
                r#"{"id":"evt-filled","source":"dropcopy","rfqId":"rfq-7","quoteId":"quote-7","makerId":"maker-7","status":"filled","realizedEvUsd":1.2}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.combo_rfq_finality_min_confirmed_samples = 1;

        write_combo_rfq_finality_report(&cfg).unwrap();
        let report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(report.stream_checkpoint.gap_count, 1);
        assert!(report.blockers.contains(&"rfq_stream_gap:1".to_string()));
    }
}
