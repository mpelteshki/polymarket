//! RFQ gateway stream writer boundary.
//!
//! This module owns the durable RFQ WSS shadow boundary: authenticate to the
//! Quoter Gateway in no-submit mode, journal inbound frames, append
//! RFQ/DropCopy-shaped events to the finality inbox, and update the stream
//! checkpoint consumed by `rfq_finality`.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, info, warn};
use url::Url;

use crate::config::Config;
use crate::rfq_finality::{
    ComboRfqStreamCheckpoint, COMBO_RFQ_FINALITY_EVENTS_FILE, COMBO_RFQ_STREAM_CHECKPOINT_FILE,
};
use polymarket_client_sdk_v2::PRIVATE_KEY_VAR;

pub const COMBO_RFQ_STREAM_REPORT_FILE: &str = "combo_rfq_stream_report.json";
pub const COMBO_RFQ_STREAM_STATUS_FILE: &str = "combo_rfq_stream_status.json";
pub const COMBO_RFQ_SHADOW_SESSION_REPORT_FILE: &str = "combo_rfq_shadow_session_report.json";
pub const COMBO_RFQ_SHADOW_JOURNAL_FILE: &str = "combo_rfq_shadow_journal.jsonl";
const COMBO_RFQ_DOCUMENTED_GATEWAY_WSS_URLS: &[&str] = crate::config::COMBO_RFQ_GATEWAY_WSS_URLS;
const COMBO_RFQ_STREAM_STATUS_MAX_AGE_SECS: i64 = 30;
const COMBO_RFQ_DEADLINE_DRIFT_TOLERANCE_MS: i64 = 100;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqStreamConfigReport {
    pub enabled: bool,
    pub gateway_wss_url: String,
    pub transport: String,
    pub bearer_token_present: bool,
    pub participant_id_present: bool,
    pub reconnect_backoff_ms: u64,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqStreamEvent {
    pub payload: Value,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub generated_at: Option<String>,
    #[serde(default)]
    pub resume_token: Option<String>,
    #[serde(default)]
    pub gap_detected: bool,
    #[serde(default)]
    pub reconnect_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqStreamWriteReport {
    pub generated_at: String,
    pub config: ComboRfqStreamConfigReport,
    pub finality_events_path: String,
    pub checkpoint_path: String,
    pub events_written: usize,
    pub checkpoint: ComboRfqStreamCheckpoint,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqStreamStatus {
    pub timestamp: String,
    pub connected: bool,
    pub stage: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_inbound_at: Option<String>,
    pub process_id: u32,
    pub gateway_wss_url: String,
    pub participant_id_fingerprint: String,
    pub connection_nonce: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ComboRfqStreamIdentity {
    process_id: u32,
    gateway_wss_url: String,
    participant_id_fingerprint: String,
    connection_nonce: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqShadowSessionReport {
    pub generated_at: String,
    pub config: ComboRfqStreamConfigReport,
    pub gateway_wss_url: String,
    pub transport: String,
    pub mode: String,
    pub live_submissions_enabled: bool,
    pub auth_ready: bool,
    pub deadlines: ComboRfqShadowSessionDeadlines,
    pub expected_steps: Vec<String>,
    pub journal_path: String,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComboRfqShadowSessionDeadlines {
    pub quote_response_window_ms: u64,
    pub user_accept_window_ms: u64,
    pub last_look_window_ms: u64,
    pub reconnect_backoff_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqShadowRunReport {
    pub generated_at: String,
    pub session: ComboRfqShadowSessionReport,
    pub raw_messages_seen: usize,
    pub shadow_journal_path: String,
    pub shadow_records_written: usize,
    pub rfq_requests_seen: usize,
    pub confirmation_requests_seen: usize,
    pub expired_deadline_messages: usize,
    pub observed_deadlines: ComboRfqShadowObservedDeadlines,
    pub deadline_alerts: Vec<String>,
    pub normalized_events_written: usize,
    pub auth_sent: bool,
    pub closed_cleanly: bool,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ComboRfqShadowObservedDeadlines {
    pub quote_submission_samples: usize,
    pub quote_submission_min_ms: Option<i64>,
    pub quote_submission_max_ms: Option<i64>,
    pub last_look_samples: usize,
    pub last_look_min_ms: Option<i64>,
    pub last_look_max_ms: Option<i64>,
    pub missing_deadline_messages: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqShadowJournalRecord {
    pub generated_at: String,
    pub source: String,
    pub message_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rfq_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_id: Option<String>,
    pub raw: Value,
    pub normalized_event_written: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms_remaining: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_canary: Option<ComboRfqQuoteCanary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_look: Option<ComboRfqLastLookShadowDecision>,
    pub decision: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqQuoteCanary {
    pub outbound_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rfq_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_e6: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_e6: Option<String>,
    pub signed_order_required_fields: Vec<String>,
    pub signer_address_present: bool,
    pub maker_address_present: bool,
    pub private_key_present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature_type: Option<u8>,
    pub request_fields_ready: bool,
    pub signing_inputs_ready: bool,
    pub outbound_schema_ready: bool,
    pub live_submission_enabled: bool,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComboRfqLastLookShadowDecision {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rfq_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_e6: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill_size_e6: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_by: Option<String>,
    pub deadline_ms_remaining: Option<i64>,
    pub request_fields_ready: bool,
    pub identity_matches_config: bool,
    pub fresh_book_required: bool,
    pub inventory_check_required: bool,
    pub allowance_check_required: bool,
    pub user_channel_required: bool,
    pub live_confirmation_enabled: bool,
    pub decision: String,
    pub blockers: Vec<String>,
}

pub fn combo_rfq_stream_config_report(config: &Config) -> ComboRfqStreamConfigReport {
    let mut blockers = Vec::new();
    if !config.combo_rfq_stream_enabled {
        blockers.push("COMBO_RFQ_STREAM_ENABLED=false".to_string());
    }
    let gateway_wss_url = effective_gateway_wss_url(config);
    if gateway_wss_url.trim().is_empty() {
        blockers.push("COMBO_RFQ_GATEWAY_WSS_URL_empty".to_string());
    } else if !gateway_wss_url.starts_with("wss://") {
        blockers.push(format!(
            "COMBO_RFQ_GATEWAY_WSS_URL_not_wss:{}",
            gateway_wss_url
        ));
    } else if !combo_rfq_gateway_wss_url_is_documented(&gateway_wss_url) {
        blockers.push(format!(
            "COMBO_RFQ_GATEWAY_WSS_URL_not_documented_quoter_gateway:{}:accepted={}",
            gateway_wss_url,
            COMBO_RFQ_DOCUMENTED_GATEWAY_WSS_URLS.join(",")
        ));
    }
    if !config.combo_rfq_grpc_url.trim().is_empty() {
        blockers.push("COMBO_RFQ_GRPC_URL_legacy_use_COMBO_RFQ_GATEWAY_WSS_URL".to_string());
    }
    let token_present = !effective_stream_bearer_token(config).is_empty();
    if !token_present {
        blockers.push("COMBO_RFQ_STREAM_BEARER_TOKEN_empty".to_string());
    }
    let participant_id_present = !config.combo_rfq_participant_id.trim().is_empty();
    if !participant_id_present {
        blockers.push("COMBO_RFQ_PARTICIPANT_ID_empty".to_string());
    }
    ComboRfqStreamConfigReport {
        enabled: config.combo_rfq_stream_enabled,
        gateway_wss_url,
        transport: "wss_quoter_gateway".into(),
        bearer_token_present: token_present,
        participant_id_present,
        reconnect_backoff_ms: config.combo_rfq_stream_reconnect_backoff_ms,
        status: if blockers.is_empty() {
            "ready_for_transport".into()
        } else {
            "blocked".into()
        },
        blockers,
    }
}

fn combo_rfq_gateway_wss_url_is_documented(gateway_wss_url: &str) -> bool {
    let normalized = gateway_wss_url.trim().trim_end_matches('/');
    COMBO_RFQ_DOCUMENTED_GATEWAY_WSS_URLS
        .iter()
        .any(|documented| normalized == documented.trim_end_matches('/'))
}

pub fn ensure_live_combo_rfq_stream_ready(config: &Config) -> Result<()> {
    let config_report = combo_rfq_stream_config_report(config);
    if !config_report.blockers.is_empty() {
        anyhow::bail!(
            "Combo/RFQ stream config blocked: {}",
            config_report.blockers.join("|")
        );
    }
    let status = read_combo_rfq_stream_status(&config.diagnostics_dir)?;
    ensure_combo_rfq_stream_status_identity(config, &status)?;
    if !status.connected {
        anyhow::bail!("Combo/RFQ stream is not connected: stage={}", status.stage);
    }
    let timestamp = DateTime::parse_from_rfc3339(&status.timestamp)
        .with_context(|| {
            format!(
                "invalid Combo/RFQ stream status timestamp {}",
                status.timestamp
            )
        })?
        .with_timezone(&Utc);
    let age_secs = Utc::now().signed_duration_since(timestamp).num_seconds();
    if age_secs > COMBO_RFQ_STREAM_STATUS_MAX_AGE_SECS {
        anyhow::bail!(
            "Combo/RFQ stream status is stale: age={}s max={}s",
            age_secs,
            COMBO_RFQ_STREAM_STATUS_MAX_AGE_SECS
        );
    }
    let last_inbound_at = status
        .last_inbound_at
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Combo/RFQ stream has no inbound messages recorded"))?;
    let last_inbound_at = DateTime::parse_from_rfc3339(last_inbound_at)
        .with_context(|| format!("invalid Combo/RFQ stream last_inbound_at {last_inbound_at}"))?
        .with_timezone(&Utc);
    let inbound_age_secs = Utc::now()
        .signed_duration_since(last_inbound_at)
        .num_seconds();
    if inbound_age_secs > COMBO_RFQ_STREAM_STATUS_MAX_AGE_SECS {
        anyhow::bail!(
            "Combo/RFQ stream inbound status is stale: age={}s max={}s",
            inbound_age_secs,
            COMBO_RFQ_STREAM_STATUS_MAX_AGE_SECS
        );
    }
    Ok(())
}

pub async fn wait_for_live_combo_rfq_stream_ready(
    config: &Config,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Err(err) = ensure_live_combo_rfq_stream_ready(config) {
            let detail = err.to_string();
            if Instant::now() >= deadline {
                anyhow::bail!("Combo/RFQ stream did not become ready within {timeout:?}: {detail}");
            }
        } else {
            return Ok(());
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

fn read_combo_rfq_stream_status(root_dir: &Path) -> Result<ComboRfqStreamStatus> {
    let path = root_dir.join(COMBO_RFQ_STREAM_STATUS_FILE);
    let body = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("parsing {}", path.display()))
}

fn ensure_combo_rfq_stream_status_identity(
    config: &Config,
    status: &ComboRfqStreamStatus,
) -> Result<()> {
    let expected = expected_combo_rfq_stream_identity(config);
    if status.process_id != expected.process_id {
        anyhow::bail!(
            "Combo/RFQ stream status belongs to another process: status_pid={} current_pid={}",
            status.process_id,
            expected.process_id
        );
    }
    if status.gateway_wss_url != expected.gateway_wss_url {
        anyhow::bail!("Combo/RFQ stream status gateway_wss_url does not match config");
    }
    if status.participant_id_fingerprint != expected.participant_id_fingerprint {
        anyhow::bail!("Combo/RFQ stream status participant_id_fingerprint does not match config");
    }
    if status.connection_nonce.trim().is_empty() {
        anyhow::bail!("Combo/RFQ stream status missing connection_nonce");
    }
    Ok(())
}

pub fn write_combo_rfq_stream_report(config: &Config) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let events_path = config.diagnostics_dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE);
    let checkpoint_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_STREAM_CHECKPOINT_FILE);
    let checkpoint = read_checkpoint(&checkpoint_path)?;
    let config_report = combo_rfq_stream_config_report(config);
    let mut blockers = config_report.blockers.clone();
    blockers.push("wss_quoter_gateway_transport_not_started_in_diagnostics".to_string());
    let report = ComboRfqStreamWriteReport {
        generated_at: Utc::now().to_rfc3339(),
        config: config_report,
        finality_events_path: events_path.display().to_string(),
        checkpoint_path: checkpoint_path.display().to_string(),
        events_written: 0,
        checkpoint,
        status: "blocked_no_transport".into(),
        blockers,
    };
    let report_path = config.diagnostics_dir.join(COMBO_RFQ_STREAM_REPORT_FILE);
    fs::write(&report_path, serde_json::to_string_pretty(&report)?)
        .with_context(|| format!("writing Combo/RFQ stream report {}", report_path.display()))?;
    Ok(report_path)
}

pub fn build_combo_rfq_shadow_session_report(config: &Config) -> ComboRfqShadowSessionReport {
    let config_report = combo_rfq_stream_config_report(config);
    let mut blockers = config_report.blockers.clone();
    push_unique_blocker(
        &mut blockers,
        "rfq_wss_shadow_session_transport_not_started",
    );
    push_unique_blocker(
        &mut blockers,
        "rfq_wss_shadow_session_no_submit_until_metrics_promote",
    );
    ComboRfqShadowSessionReport {
        generated_at: Utc::now().to_rfc3339(),
        gateway_wss_url: config_report.gateway_wss_url.clone(),
        transport: config_report.transport.clone(),
        mode: "shadow_no_submit".to_string(),
        live_submissions_enabled: false,
        auth_ready: config_report.bearer_token_present && config_report.participant_id_present,
        deadlines: ComboRfqShadowSessionDeadlines {
            quote_response_window_ms: 400,
            user_accept_window_ms: 5_000,
            last_look_window_ms: 1_000,
            reconnect_backoff_ms: config.combo_rfq_stream_reconnect_backoff_ms,
        },
        expected_steps: vec![
            "connect_wss_quoter_gateway".to_string(),
            "authenticate_participant".to_string(),
            "maintain_ping_pong_heartbeat".to_string(),
            "ingest_rfq_request".to_string(),
            "compute_would_quote_before_400ms_deadline".to_string(),
            "record_would_cancel_or_reprice_on_book_inventory_edge_change".to_string(),
            "record_would_confirm_or_decline_last_look_within_1000ms".to_string(),
            "append_finality_event_for_replay_only".to_string(),
        ],
        journal_path: config
            .diagnostics_dir
            .join(COMBO_RFQ_SHADOW_JOURNAL_FILE)
            .display()
            .to_string(),
        status: "blocked_shadow_no_submit".to_string(),
        blockers,
        config: config_report,
    }
}

pub fn write_combo_rfq_shadow_session_report(config: &Config) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let report = build_combo_rfq_shadow_session_report(config);
    let report_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_SHADOW_SESSION_REPORT_FILE);
    fs::write(&report_path, serde_json::to_string_pretty(&report)?).with_context(|| {
        format!(
            "writing Combo/RFQ shadow session report {}",
            report_path.display()
        )
    })?;
    Ok(report_path)
}

pub fn spawn_live_combo_rfq_stream_ingester(config: Config) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_live_combo_rfq_stream_ingester(config).await;
    })
}

fn combo_rfq_stream_identity(config: &Config, gateway_wss_url: &str) -> ComboRfqStreamIdentity {
    ComboRfqStreamIdentity {
        process_id: std::process::id(),
        gateway_wss_url: gateway_wss_url.to_string(),
        participant_id_fingerprint: fingerprint(config.combo_rfq_participant_id.as_str()),
        connection_nonce: uuid::Uuid::new_v4().to_string(),
    }
}

fn expected_combo_rfq_stream_identity(config: &Config) -> ComboRfqStreamIdentity {
    ComboRfqStreamIdentity {
        process_id: std::process::id(),
        gateway_wss_url: effective_gateway_wss_url(config),
        participant_id_fingerprint: fingerprint(config.combo_rfq_participant_id.as_str()),
        connection_nonce: String::new(),
    }
}

fn record_combo_rfq_stream_status(
    config: &Config,
    identity: &ComboRfqStreamIdentity,
    connected: bool,
    stage: &str,
    detail: Option<&str>,
    inbound_received: bool,
) {
    if let Err(err) = write_combo_rfq_stream_status(
        &config.diagnostics_dir,
        identity,
        connected,
        stage,
        detail,
        inbound_received,
    ) {
        warn!("Combo/RFQ live stream failed to write status: {err:#}");
    }
}

fn write_combo_rfq_stream_status(
    root_dir: &Path,
    identity: &ComboRfqStreamIdentity,
    connected: bool,
    stage: &str,
    detail: Option<&str>,
    inbound_received: bool,
) -> Result<()> {
    fs::create_dir_all(root_dir)
        .with_context(|| format!("creating diagnostics directory {}", root_dir.display()))?;
    let now = Utc::now().to_rfc3339();
    let previous_status = if connected && !inbound_received {
        read_combo_rfq_stream_status(root_dir)
            .ok()
            .filter(|status| combo_rfq_stream_status_identity_matches(identity, status))
    } else {
        None
    };
    let status = ComboRfqStreamStatus {
        timestamp: now.clone(),
        connected,
        stage: stage.to_string(),
        detail: detail.map(str::to_string),
        last_inbound_at: if inbound_received {
            Some(now)
        } else if connected {
            previous_status.and_then(|status| status.last_inbound_at)
        } else {
            None
        },
        process_id: identity.process_id,
        gateway_wss_url: identity.gateway_wss_url.clone(),
        participant_id_fingerprint: identity.participant_id_fingerprint.clone(),
        connection_nonce: identity.connection_nonce.clone(),
    };
    let path = root_dir.join(COMBO_RFQ_STREAM_STATUS_FILE);
    fs::write(&path, serde_json::to_string_pretty(&status)?)
        .with_context(|| format!("writing {}", path.display()))
}

fn combo_rfq_stream_status_identity_matches(
    identity: &ComboRfqStreamIdentity,
    status: &ComboRfqStreamStatus,
) -> bool {
    status.process_id == identity.process_id
        && status.gateway_wss_url == identity.gateway_wss_url
        && status.participant_id_fingerprint == identity.participant_id_fingerprint
        && status.connection_nonce == identity.connection_nonce
}

async fn run_live_combo_rfq_stream_ingester(config: Config) {
    let mut reconnect_count = 0u64;
    loop {
        match run_live_combo_rfq_stream_once(&config, reconnect_count).await {
            Ok(()) => info!("Combo/RFQ live stream connection closed; reconnecting"),
            Err(err) => warn!("Combo/RFQ live stream ingester failed: {err:#}"),
        }
        reconnect_count = reconnect_count.saturating_add(1);
        time::sleep(Duration::from_millis(
            config.combo_rfq_stream_reconnect_backoff_ms.max(1),
        ))
        .await;
    }
}

async fn run_live_combo_rfq_stream_once(config: &Config, reconnect_count: u64) -> Result<()> {
    let config_report = combo_rfq_stream_config_report(config);
    if !config_report.blockers.is_empty() {
        anyhow::bail!(
            "Combo/RFQ stream config blocked: {}",
            config_report.blockers.join("|")
        );
    }
    let url = Url::parse(&config_report.gateway_wss_url).with_context(|| {
        format!(
            "invalid Combo/RFQ WSS URL {}",
            config_report.gateway_wss_url
        )
    })?;
    let auth_frame = combo_rfq_shadow_auth_frame(config)?;
    let identity = combo_rfq_stream_identity(config, &config_report.gateway_wss_url);
    record_combo_rfq_stream_status(
        config,
        &identity,
        false,
        "connecting",
        Some("opening WSS transport"),
        false,
    );
    let (mut ws_stream, _) = match connect_async(url).await {
        Ok(stream) => stream,
        Err(err) => {
            record_combo_rfq_stream_status(
                config,
                &identity,
                false,
                "connect_failed",
                Some(&err.to_string()),
                false,
            );
            return Err(err).context("Combo/RFQ WSS live stream connect failed");
        }
    };
    if let Err(err) = ws_stream.send(Message::Text(auth_frame.to_string())).await {
        record_combo_rfq_stream_status(
            config,
            &identity,
            false,
            "auth_send_failed",
            Some(&err.to_string()),
            false,
        );
        return Err(err).context("Combo/RFQ WSS live stream auth send failed");
    }
    record_combo_rfq_stream_status(
        config,
        &identity,
        true,
        "connected",
        Some("auth frame sent"),
        false,
    );
    info!(
        "Combo/RFQ live stream connected to {}",
        config_report.gateway_wss_url
    );

    while let Some(message) = ws_stream.next().await {
        match message {
            Ok(Message::Text(text)) => {
                record_combo_rfq_stream_status(
                    config,
                    &identity,
                    true,
                    "message_received",
                    Some("text"),
                    true,
                );
                handle_live_combo_rfq_stream_text(config, &text, reconnect_count)?;
            }
            Ok(Message::Ping(ping)) => {
                let _ = ws_stream.send(Message::Pong(ping)).await;
            }
            Ok(Message::Pong(_)) => {
                debug!("Combo/RFQ live stream pong received");
            }
            Ok(Message::Close(frame)) => {
                record_combo_rfq_stream_status(
                    config,
                    &identity,
                    false,
                    "closed",
                    frame.as_ref().map(|frame| frame.reason.as_ref()),
                    false,
                );
                break;
            }
            Ok(_) => {}
            Err(err) => {
                record_combo_rfq_stream_status(
                    config,
                    &identity,
                    false,
                    "read_failed",
                    Some(&err.to_string()),
                    false,
                );
                return Err(err).context("Combo/RFQ WSS live stream read failed");
            }
        }
    }
    record_combo_rfq_stream_status(
        config,
        &identity,
        false,
        "ended",
        Some("stream ended"),
        false,
    );
    Ok(())
}

fn handle_live_combo_rfq_stream_text(
    config: &Config,
    text: &str,
    reconnect_count: u64,
) -> Result<()> {
    let received_at = Utc::now();
    match serde_json::from_str::<Value>(text) {
        Ok(value) => {
            let mut normalized = combo_rfq_gateway_message_to_stream_event(&value);
            let record = combo_rfq_shadow_journal_record_from_message_with_config(
                &value,
                received_at,
                normalized.is_some(),
                Some(config),
            );
            append_combo_rfq_shadow_journal_records(config, &[record])?;
            if let Some(mut event) = normalized.take() {
                event.reconnect_count = reconnect_count;
                append_combo_rfq_stream_events(config, &[event])?;
            }
        }
        Err(_) => {
            let record = combo_rfq_shadow_journal_record_from_raw_text(text, received_at);
            append_combo_rfq_shadow_journal_records(config, &[record])?;
        }
    }
    Ok(())
}

pub async fn run_combo_rfq_wss_shadow_session(
    config: &Config,
    max_messages: usize,
    max_duration: Duration,
) -> Result<ComboRfqShadowRunReport> {
    let session = build_combo_rfq_shadow_session_report(config);
    let shadow_journal_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_SHADOW_JOURNAL_FILE)
        .display()
        .to_string();
    if !session.config.blockers.is_empty() {
        let blockers = session.config.blockers.clone();
        return Ok(ComboRfqShadowRunReport {
            generated_at: Utc::now().to_rfc3339(),
            session,
            raw_messages_seen: 0,
            shadow_journal_path,
            shadow_records_written: 0,
            rfq_requests_seen: 0,
            confirmation_requests_seen: 0,
            expired_deadline_messages: 0,
            observed_deadlines: ComboRfqShadowObservedDeadlines::default(),
            deadline_alerts: Vec::new(),
            normalized_events_written: 0,
            auth_sent: false,
            closed_cleanly: false,
            status: "blocked_config".to_string(),
            blockers,
        });
    }

    let url = Url::parse(&session.gateway_wss_url)
        .with_context(|| format!("invalid Combo/RFQ WSS URL {}", session.gateway_wss_url))?;
    let auth_frame = combo_rfq_shadow_auth_frame(config)?;
    let (mut ws_stream, _) = connect_async(url)
        .await
        .context("Combo/RFQ WSS shadow connect failed")?;
    ws_stream
        .send(Message::Text(auth_frame.to_string()))
        .await
        .context("Combo/RFQ WSS shadow auth send failed")?;

    let mut raw_messages_seen = 0usize;
    let mut normalized_events = Vec::new();
    let mut shadow_journal_records = Vec::new();
    let deadline = time::sleep(max_duration);
    tokio::pin!(deadline);
    let mut closed_cleanly = false;

    loop {
        if raw_messages_seen >= max_messages {
            break;
        }
        tokio::select! {
            _ = &mut deadline => {
                break;
            }
            message = ws_stream.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        raw_messages_seen += 1;
                        let received_at = Utc::now();
                        match serde_json::from_str::<Value>(&text) {
                            Ok(value) => {
                                let event = combo_rfq_gateway_message_to_stream_event(&value);
                                shadow_journal_records.push(
                                    combo_rfq_shadow_journal_record_from_message_with_config(
                                        &value,
                                        received_at,
                                        event.is_some(),
                                        Some(config),
                                    ),
                                );
                                if let Some(event) = event {
                                    normalized_events.push(event);
                                }
                            }
                            Err(_) => {
                                shadow_journal_records.push(
                                    combo_rfq_shadow_journal_record_from_raw_text(
                                        &text,
                                        received_at,
                                    ),
                                );
                            }
                        }
                    }
                    Some(Ok(Message::Ping(ping))) => {
                        let _ = ws_stream.send(Message::Pong(ping)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        closed_cleanly = true;
                        break;
                    }
                    Some(Err(err)) => {
                        return Err(err).context("Combo/RFQ WSS shadow stream error");
                    }
                    None => {
                        closed_cleanly = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    let shadow_journal_path =
        append_combo_rfq_shadow_journal_records(config, &shadow_journal_records)?;
    let write_report = append_combo_rfq_stream_events(config, &normalized_events)?;
    let (observed_deadlines, deadline_alerts) =
        combo_rfq_shadow_observed_deadlines(&shadow_journal_records, &session.deadlines);
    let mut blockers = session.blockers.clone();
    push_unique_blocker(
        &mut blockers,
        "rfq_wss_shadow_session_no_outbound_quote_cancel_confirm",
    );
    if raw_messages_seen == 0 {
        push_unique_blocker(&mut blockers, "rfq_wss_shadow_session_no_messages_seen");
    }

    Ok(ComboRfqShadowRunReport {
        generated_at: Utc::now().to_rfc3339(),
        session,
        raw_messages_seen,
        shadow_journal_path: shadow_journal_path.display().to_string(),
        shadow_records_written: shadow_journal_records.len(),
        rfq_requests_seen: shadow_journal_records
            .iter()
            .filter(|record| record.message_type == "RFQ_REQUEST")
            .count(),
        confirmation_requests_seen: shadow_journal_records
            .iter()
            .filter(|record| record.message_type == "RFQ_CONFIRMATION_REQUEST")
            .count(),
        expired_deadline_messages: shadow_journal_records
            .iter()
            .filter(|record| {
                record
                    .blockers
                    .iter()
                    .any(|blocker| blocker.starts_with("deadline_expired:"))
            })
            .count(),
        observed_deadlines,
        deadline_alerts,
        normalized_events_written: write_report.events_written,
        auth_sent: true,
        closed_cleanly,
        status: "blocked_shadow_no_submit".to_string(),
        blockers,
    })
}

fn combo_rfq_shadow_observed_deadlines(
    records: &[ComboRfqShadowJournalRecord],
    expected: &ComboRfqShadowSessionDeadlines,
) -> (ComboRfqShadowObservedDeadlines, Vec<String>) {
    let mut observed = ComboRfqShadowObservedDeadlines::default();
    for record in records {
        match (
            record.deadline_kind.as_deref(),
            record.deadline_ms_remaining,
        ) {
            (Some("quote_submission"), Some(ms)) => observe_deadline_ms(
                &mut observed.quote_submission_samples,
                &mut observed.quote_submission_min_ms,
                &mut observed.quote_submission_max_ms,
                ms,
            ),
            (Some("last_look"), Some(ms)) => observe_deadline_ms(
                &mut observed.last_look_samples,
                &mut observed.last_look_min_ms,
                &mut observed.last_look_max_ms,
                ms,
            ),
            (Some(_), None) => observed.missing_deadline_messages += 1,
            _ => {}
        }
    }

    let mut alerts = Vec::new();
    push_deadline_window_alert(
        &mut alerts,
        "quote_submission",
        observed.quote_submission_max_ms,
        expected.quote_response_window_ms,
    );
    push_deadline_window_alert(
        &mut alerts,
        "last_look",
        observed.last_look_max_ms,
        expected.last_look_window_ms,
    );
    if observed.missing_deadline_messages > 0 {
        alerts.push(format!(
            "deadline_fields_missing:{}",
            observed.missing_deadline_messages
        ));
    }
    (observed, alerts)
}

fn observe_deadline_ms(
    samples: &mut usize,
    min_ms: &mut Option<i64>,
    max_ms: &mut Option<i64>,
    ms: i64,
) {
    *samples += 1;
    *min_ms = Some(min_ms.map_or(ms, |current| current.min(ms)));
    *max_ms = Some(max_ms.map_or(ms, |current| current.max(ms)));
}

fn push_deadline_window_alert(
    alerts: &mut Vec<String>,
    kind: &str,
    observed_max_ms: Option<i64>,
    expected_ms: u64,
) {
    let Some(observed_max_ms) = observed_max_ms else {
        return;
    };
    let limit = expected_ms as i64 + COMBO_RFQ_DEADLINE_DRIFT_TOLERANCE_MS;
    if observed_max_ms > limit {
        alerts.push(format!(
            "{kind}_window_drift_observed_max:{observed_max_ms}ms>{limit}ms"
        ));
    }
}

pub fn append_combo_rfq_shadow_journal_records(
    config: &Config,
    records: &[ComboRfqShadowJournalRecord],
) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let journal_path = config.diagnostics_dir.join(COMBO_RFQ_SHADOW_JOURNAL_FILE);
    if !records.is_empty() {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .with_context(|| {
                format!(
                    "opening Combo/RFQ shadow journal {}",
                    journal_path.display()
                )
            })?;
        for record in records {
            writeln!(file, "{}", serde_json::to_string(record)?).with_context(|| {
                format!(
                    "writing Combo/RFQ shadow journal {}",
                    journal_path.display()
                )
            })?;
        }
    }
    Ok(journal_path)
}

#[cfg(test)]
pub fn combo_rfq_shadow_journal_record_from_message(
    value: &Value,
    received_at: DateTime<Utc>,
    normalized_event_written: bool,
) -> ComboRfqShadowJournalRecord {
    combo_rfq_shadow_journal_record_from_message_with_config(
        value,
        received_at,
        normalized_event_written,
        None,
    )
}

pub fn combo_rfq_shadow_journal_record_from_message_with_config(
    value: &Value,
    received_at: DateTime<Utc>,
    normalized_event_written: bool,
    config: Option<&Config>,
) -> ComboRfqShadowJournalRecord {
    let message_type = text_value(value, &["type"])
        .map(|value| value.trim().to_ascii_uppercase())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let mut blockers = vec!["shadow_no_submit".to_string()];
    let mut deadline_kind = None;
    let mut deadline_at = None;
    let mut deadline_ms_remaining = None;
    let decision = match message_type.as_str() {
        "RFQ_REQUEST" => {
            deadline_kind = Some("quote_submission".to_string());
            push_unique_blocker(&mut blockers, "signed_quote_engine_not_enabled");
            "would_not_quote_shadow_only".to_string()
        }
        "RFQ_CONFIRMATION_REQUEST" => {
            deadline_kind = Some("last_look".to_string());
            push_unique_blocker(&mut blockers, "last_look_confirmation_disabled_in_shadow");
            "would_not_confirm_shadow_only".to_string()
        }
        "RFQ_ERROR" => "record_error_only".to_string(),
        "ACK_RFQ_CONFIRMATION_RESPONSE" | "RFQ_TRADE" | "RFQ_EXECUTION_UPDATE" => {
            "observe_only".to_string()
        }
        "UNKNOWN" => {
            push_unique_blocker(&mut blockers, "rfq_wss_message_type_missing");
            "observe_unknown_only".to_string()
        }
        _ => {
            push_unique_blocker(
                &mut blockers,
                format!("rfq_wss_message_type_unhandled:{message_type}"),
            );
            "observe_unknown_only".to_string()
        }
    };

    if let Some(kind) = deadline_kind.as_deref() {
        let deadline_ms = match kind {
            "quote_submission" => deadline_millis(value, &["submission_deadline"]),
            "last_look" => deadline_millis(value, &["confirm_by"]),
            _ => None,
        };
        match deadline_ms {
            Some(deadline_ms) => {
                if let Some(timestamp) = DateTime::<Utc>::from_timestamp_millis(deadline_ms) {
                    deadline_at = Some(timestamp.to_rfc3339());
                }
                let remaining = deadline_ms.saturating_sub(received_at.timestamp_millis());
                deadline_ms_remaining = Some(remaining);
                if remaining < 0 {
                    push_unique_blocker(&mut blockers, format!("deadline_expired:{kind}"));
                }
            }
            None => {
                push_unique_blocker(&mut blockers, format!("deadline_missing:{kind}"));
            }
        }
    }

    let quote_canary =
        (message_type == "RFQ_REQUEST").then(|| combo_rfq_quote_canary_from_request(value, config));
    if let Some(canary) = &quote_canary {
        for blocker in &canary.blockers {
            push_unique_blocker(&mut blockers, format!("quote_canary:{blocker}"));
        }
    }
    let last_look = (message_type == "RFQ_CONFIRMATION_REQUEST")
        .then(|| combo_rfq_last_look_shadow_decision(value, config, deadline_ms_remaining));
    if let Some(decision) = &last_look {
        for blocker in &decision.blockers {
            push_unique_blocker(&mut blockers, format!("last_look:{blocker}"));
        }
    }

    ComboRfqShadowJournalRecord {
        generated_at: received_at.to_rfc3339(),
        source: "rfq_wss_shadow".to_string(),
        message_type,
        rfq_id: text_value(value, &["rfq_id", "rfqId"]),
        quote_id: text_value(value, &["quote_id", "quoteId"]),
        raw: value.clone(),
        normalized_event_written,
        deadline_kind,
        deadline_at,
        deadline_ms_remaining,
        quote_canary,
        last_look,
        decision,
        blockers,
    }
}

fn combo_rfq_quote_canary_from_request(
    value: &Value,
    config: Option<&Config>,
) -> ComboRfqQuoteCanary {
    let rfq_id = text_value(value, &["rfq_id", "rfqId"]);
    let token_id = quote_token_id_from_rfq_request(value);
    let size_e6 = requested_size_e6(value);
    let price_e6 = None;
    let private_key_present = std::env::var(PRIVATE_KEY_VAR)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let signer_address_present = std::env::var("LIVE_SIGNER_ADDRESS")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || private_key_present;
    let maker_address_present = config
        .map(|config| !config.live_funder_address.trim().is_empty())
        .unwrap_or(false);
    let signature_type = config.map(|config| config.live_signature_type);
    let mut blockers = vec!["shadow_no_submit".to_string()];

    if rfq_id.is_none() {
        blockers.push("rfq_quote_rfq_id_missing".to_string());
    }
    if token_id.is_none() {
        blockers.push("rfq_quote_token_id_missing".to_string());
    }
    if size_e6.is_none() {
        blockers.push("rfq_quote_size_e6_missing".to_string());
    }
    if deadline_millis(value, &["submission_deadline"]).is_none() {
        blockers.push("rfq_quote_submission_deadline_missing".to_string());
    }
    if text_value(value, &["side"]).is_none() {
        blockers.push("rfq_quote_side_missing".to_string());
    }
    if !signer_address_present {
        blockers.push("rfq_quote_signer_address_missing".to_string());
    }
    if !maker_address_present {
        blockers.push("rfq_quote_maker_address_missing".to_string());
    }
    if !private_key_present {
        blockers.push(format!("{PRIVATE_KEY_VAR}_missing_for_rfq_quote_canary"));
    }
    blockers.push("rfq_quote_price_e6_not_computed".to_string());
    blockers.push("rfq_quote_signed_order_not_built".to_string());

    let request_fields_ready = rfq_id.is_some()
        && token_id.is_some()
        && size_e6.is_some()
        && deadline_millis(value, &["submission_deadline"]).is_some()
        && text_value(value, &["side"]).is_some();
    let signing_inputs_ready =
        signer_address_present && maker_address_present && private_key_present;
    let outbound_schema_ready = false;

    ComboRfqQuoteCanary {
        outbound_type: "RFQ_QUOTE".to_string(),
        rfq_id,
        token_id,
        price_e6,
        size_e6,
        signed_order_required_fields: vec![
            "salt".to_string(),
            "maker".to_string(),
            "signer".to_string(),
            "tokenId".to_string(),
            "makerAmount".to_string(),
            "takerAmount".to_string(),
            "side".to_string(),
            "signatureType".to_string(),
            "timestamp".to_string(),
            "metadata".to_string(),
            "builder".to_string(),
            "signature".to_string(),
        ],
        signer_address_present,
        maker_address_present,
        private_key_present,
        signature_type,
        request_fields_ready,
        signing_inputs_ready,
        outbound_schema_ready,
        live_submission_enabled: false,
        status: "blocked_no_submit".to_string(),
        blockers,
    }
}

fn quote_token_id_from_rfq_request(value: &Value) -> Option<String> {
    let side = text_value(value, &["side"])
        .map(|side| side.trim().to_ascii_uppercase())
        .unwrap_or_default();
    match side.as_str() {
        "YES" => text_value(value, &["yes_position_id", "yesPositionId"]),
        "NO" => text_value(value, &["no_position_id", "noPositionId"]),
        _ => text_value(value, &["token_id", "tokenId"])
            .or_else(|| text_value(value, &["yes_position_id", "yesPositionId"]))
            .or_else(|| text_value(value, &["no_position_id", "noPositionId"])),
    }
}

fn requested_size_e6(value: &Value) -> Option<String> {
    text_value(value, &["size_e6", "fill_size_e6"]).or_else(|| {
        value
            .get("requested_size")
            .and_then(|requested| text_value(requested, &["value_e6", "valueE6"]))
    })
}

fn combo_rfq_last_look_shadow_decision(
    value: &Value,
    config: Option<&Config>,
    deadline_ms_remaining: Option<i64>,
) -> ComboRfqLastLookShadowDecision {
    let rfq_id = text_value(value, &["rfq_id", "rfqId"]);
    let quote_id = text_value(value, &["quote_id", "quoteId"]);
    let token_id = quote_token_id_from_rfq_request(value);
    let price_e6 = text_value(value, &["price_e6", "priceE6"]);
    let fill_size_e6 = text_value(value, &["fill_size_e6", "size_e6", "fillSizeE6", "sizeE6"]);
    let confirm_by = text_value(value, &["confirm_by", "confirmBy"]);
    let mut blockers = vec!["shadow_no_submit".to_string()];

    if rfq_id.is_none() {
        blockers.push("last_look_rfq_id_missing".to_string());
    }
    if quote_id.is_none() {
        blockers.push("last_look_quote_id_missing".to_string());
    }
    if token_id.is_none() {
        blockers.push("last_look_token_id_missing".to_string());
    }
    if price_e6.is_none() {
        blockers.push("last_look_price_e6_missing".to_string());
    }
    if fill_size_e6.is_none() {
        blockers.push("last_look_fill_size_e6_missing".to_string());
    }
    if confirm_by.is_none() {
        blockers.push("last_look_confirm_by_missing".to_string());
    }
    if matches!(deadline_ms_remaining, Some(value) if value < 0) {
        blockers.push("last_look_deadline_expired".to_string());
    }
    let identity_matches_config = last_look_identity_matches_config(value, config);
    if !identity_matches_config {
        blockers.push("last_look_identity_mismatch_or_unconfigured".to_string());
    }
    blockers.push("last_look_fresh_book_not_checked".to_string());
    blockers.push("last_look_inventory_not_checked".to_string());
    blockers.push("last_look_allowance_not_checked".to_string());
    blockers.push("last_look_user_channel_not_checked".to_string());
    blockers.push("last_look_ev_not_computed".to_string());

    let request_fields_ready = rfq_id.is_some()
        && quote_id.is_some()
        && token_id.is_some()
        && price_e6.is_some()
        && fill_size_e6.is_some()
        && confirm_by.is_some();

    ComboRfqLastLookShadowDecision {
        rfq_id,
        quote_id,
        token_id,
        price_e6,
        fill_size_e6,
        confirm_by,
        deadline_ms_remaining,
        request_fields_ready,
        identity_matches_config,
        fresh_book_required: true,
        inventory_check_required: true,
        allowance_check_required: true,
        user_channel_required: true,
        live_confirmation_enabled: false,
        decision: "would_decline_shadow_only".to_string(),
        blockers,
    }
}

fn last_look_identity_matches_config(value: &Value, config: Option<&Config>) -> bool {
    let Some(config) = config else {
        return false;
    };
    let maker_matches = optional_case_insensitive_match(
        text_value(value, &["maker_address", "makerAddress"]).as_deref(),
        Some(config.live_funder_address.as_str()),
    );
    let signature_matches = text_value(value, &["signature_type", "signatureType"])
        .and_then(|value| value.parse::<u8>().ok())
        .map(|signature_type| signature_type == config.live_signature_type)
        .unwrap_or(false);
    maker_matches && signature_matches
}

fn optional_case_insensitive_match(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            !left.trim().is_empty() && left.trim().eq_ignore_ascii_case(right.trim())
        }
        _ => false,
    }
}

fn combo_rfq_shadow_journal_record_from_raw_text(
    raw_text: &str,
    received_at: DateTime<Utc>,
) -> ComboRfqShadowJournalRecord {
    ComboRfqShadowJournalRecord {
        generated_at: received_at.to_rfc3339(),
        source: "rfq_wss_shadow".to_string(),
        message_type: "UNPARSEABLE_TEXT".to_string(),
        rfq_id: None,
        quote_id: None,
        raw: Value::String(raw_text.to_string()),
        normalized_event_written: false,
        deadline_kind: None,
        deadline_at: None,
        deadline_ms_remaining: None,
        quote_canary: None,
        last_look: None,
        decision: "observe_unknown_only".to_string(),
        blockers: vec![
            "shadow_no_submit".to_string(),
            "rfq_wss_message_json_parse_failed".to_string(),
        ],
    }
}

fn combo_rfq_shadow_auth_frame(config: &Config) -> Result<Value> {
    let api_key = std::env::var("CLOB_API_KEY").unwrap_or_default();
    let secret = std::env::var("CLOB_SECRET").unwrap_or_default();
    let passphrase = std::env::var("CLOB_PASS_PHRASE")
        .or_else(|_| std::env::var("CLOB_PASSPHRASE"))
        .unwrap_or_default();
    let maker_address = config.live_funder_address.trim();
    let signer_address = std::env::var("LIVE_SIGNER_ADDRESS")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| maker_address.to_string());

    if api_key.trim().is_empty() {
        anyhow::bail!("CLOB_API_KEY_empty_for_rfq_shadow_auth");
    }
    if secret.trim().is_empty() {
        anyhow::bail!("CLOB_SECRET_empty_for_rfq_shadow_auth");
    }
    if passphrase.trim().is_empty() {
        anyhow::bail!("CLOB_PASS_PHRASE_empty_for_rfq_shadow_auth");
    }
    if maker_address.is_empty() {
        anyhow::bail!("LIVE_FUNDER_ADDRESS_empty_for_rfq_shadow_identity");
    }
    Ok(json!({
        "type": "auth",
        "auth": {
            "apiKey": api_key,
            "secret": secret,
            "passphrase": passphrase
        },
        "identity": {
            "signer_address": signer_address,
            "maker_address": maker_address,
            "signature_type": config.live_signature_type
        }
    }))
}

pub fn append_combo_rfq_stream_events(
    config: &Config,
    events: &[ComboRfqStreamEvent],
) -> Result<ComboRfqStreamWriteReport> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let events_path = config.diagnostics_dir.join(COMBO_RFQ_FINALITY_EVENTS_FILE);
    let checkpoint_path = config
        .diagnostics_dir
        .join(COMBO_RFQ_STREAM_CHECKPOINT_FILE);
    let mut checkpoint = read_checkpoint(&checkpoint_path)?;
    let config_report = combo_rfq_stream_config_report(config);

    if !events.is_empty() {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&events_path)
            .with_context(|| format!("opening RFQ finality events {}", events_path.display()))?;
        for event in events {
            let mut payload = event.payload.clone();
            if let Some(source) = event.source.as_deref().filter(|source| !source.is_empty()) {
                set_missing_string(&mut payload, "source", source);
            }
            let generated_at = event
                .generated_at
                .clone()
                .or_else(|| text_value(&payload, &["generatedAt", "generated_at", "timestamp"]))
                .unwrap_or_else(|| Utc::now().to_rfc3339());
            set_missing_string(&mut payload, "generatedAt", &generated_at);
            let source = event
                .source
                .clone()
                .or_else(|| text_value(&payload, &["source", "stream", "channel"]))
                .unwrap_or_else(|| "rfq_stream".into());
            update_checkpoint_from_event(&mut checkpoint, event, &payload, &source, &generated_at);
            crate::rfq_finality::cache_combo_rfq_stream_event(&payload);
            writeln!(file, "{}", serde_json::to_string(&payload)?).with_context(|| {
                format!("writing RFQ finality events {}", events_path.display())
            })?;
        }
    }
    write_checkpoint(&checkpoint_path, &checkpoint)?;

    let mut blockers = config_report.blockers.clone();
    if checkpoint.gap_count > 0 {
        blockers.push(format!("rfq_stream_gap:{}", checkpoint.gap_count));
    }
    if checkpoint.last_dropcopy_resume_token.is_none() {
        blockers.push("dropcopy_resume_token_missing".to_string());
    }
    Ok(ComboRfqStreamWriteReport {
        generated_at: Utc::now().to_rfc3339(),
        config: config_report,
        finality_events_path: events_path.display().to_string(),
        checkpoint_path: checkpoint_path.display().to_string(),
        events_written: events.len(),
        checkpoint,
        status: if blockers.is_empty() {
            "ready".into()
        } else {
            "blocked".into()
        },
        blockers,
    })
}

pub fn combo_rfq_gateway_message_to_stream_event(value: &Value) -> Option<ComboRfqStreamEvent> {
    let message_type = text_value(value, &["type"])?;
    let message_type = message_type.trim().to_ascii_uppercase();
    let generated_at = gateway_event_time(value).unwrap_or_else(|| Utc::now().to_rfc3339());
    let mut payload = json!({
        "id": gateway_event_id(value, &message_type, &generated_at),
        "source": format!("rfq_wss_{}", message_type.to_ascii_lowercase()),
        "generatedAt": generated_at,
        "wssType": message_type,
    });
    copy_text_field(value, &mut payload, "rfq_id", "rfqId");
    copy_text_field(value, &mut payload, "quote_id", "quoteId");
    copy_text_field(value, &mut payload, "requester_id", "requesterId");
    copy_text_field(
        value,
        &mut payload,
        "requestor_public_id",
        "requestorPublicId",
    );
    copy_text_field(value, &mut payload, "condition_id", "marketEventId");
    copy_text_field(value, &mut payload, "maker_address", "makerId");
    copy_text_field(value, &mut payload, "tx_hash", "transactionHash");
    copy_text_field(value, &mut payload, "direction", "direction");
    copy_text_field(value, &mut payload, "side", "side");
    copy_e6_decimal_field(value, &mut payload, "price_e6", "price");
    copy_e6_decimal_field(value, &mut payload, "size_e6", "qtyDecimal");
    copy_e6_decimal_field(value, &mut payload, "fill_size_e6", "qtyDecimal");

    let status = match message_type.as_str() {
        "RFQ_CONFIRMATION_REQUEST" => "quote_accepted".to_string(),
        "ACK_RFQ_CONFIRMATION_RESPONSE" => {
            match text_value(value, &["decision"])
                .map(|decision| decision.trim().to_ascii_uppercase())
                .as_deref()
            {
                Some("CONFIRM") => "quote_pending_end_trade".to_string(),
                Some("DECLINE") => "last_look_rejected".to_string(),
                Some(other) => format!("last_look_{other}"),
                None => "quote_pending_end_trade".to_string(),
            }
        }
        "RFQ_TRADE" => "filled".to_string(),
        "RFQ_EXECUTION_UPDATE" => {
            text_value(value, &["status"]).unwrap_or("execution_update".into())
        }
        "RFQ_ERROR" => text_value(value, &["code"])
            .map(|code| format!("failed_{code}"))
            .unwrap_or_else(|| "failed_rfq_error".into()),
        _ => return None,
    };
    set_string(&mut payload, "status", &status);
    let generated_at = payload
        .get("generatedAt")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Some(ComboRfqStreamEvent {
        payload,
        source: Some(format!("rfq_wss_{}", message_type.to_ascii_lowercase())),
        generated_at: Some(generated_at),
        resume_token: text_value(value, &["resumeToken", "resume_token"]),
        gap_detected: false,
        reconnect_count: 0,
    })
}

#[cfg(test)]
pub fn combo_rfq_gateway_messages_to_stream_events(values: &[Value]) -> Vec<ComboRfqStreamEvent> {
    values
        .iter()
        .filter_map(combo_rfq_gateway_message_to_stream_event)
        .collect()
}

fn effective_stream_bearer_token(config: &Config) -> String {
    let stream_token = config.combo_rfq_stream_bearer_token.trim();
    if stream_token.is_empty() {
        config.combo_rfq_bearer_token.trim().to_string()
    } else {
        stream_token.to_string()
    }
}

fn fingerprint(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.trim().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn gateway_event_time(value: &Value) -> Option<String> {
    if let Some(text) = text_value(value, &["generatedAt", "generated_at", "timestamp", "time"]) {
        return Some(text);
    }
    for key in ["executed_at", "confirm_by", "submission_deadline"] {
        let Some(number) = text_value(value, &[key]).and_then(|text| text.parse::<i64>().ok())
        else {
            continue;
        };
        if let Some(timestamp) = DateTime::<Utc>::from_timestamp_millis(number) {
            return Some(timestamp.to_rfc3339());
        }
    }
    None
}

fn gateway_event_id(value: &Value, message_type: &str, generated_at: &str) -> String {
    let rfq_id = text_value(value, &["rfq_id"]).unwrap_or_default();
    let quote_id = text_value(value, &["quote_id"]).unwrap_or_default();
    let suffix = if quote_id.is_empty() {
        rfq_id
    } else {
        format!("{rfq_id}:{quote_id}")
    };
    format!("wss:{message_type}:{suffix}:{generated_at}")
}

fn copy_text_field(source: &Value, target: &mut Value, from: &str, to: &str) {
    if let Some(value) = text_value(source, &[from]) {
        set_string(target, to, &value);
    }
}

fn copy_e6_decimal_field(source: &Value, target: &mut Value, from: &str, to: &str) {
    if let Some(value) = text_value(source, &[from])
        .and_then(|text| text.parse::<f64>().ok())
        .filter(|value| value.is_finite())
    {
        set_string(target, to, &format!("{:.6}", value / 1_000_000.0));
    }
}

fn set_string(value: &mut Value, key: &str, new_value: &str) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object.insert(key.to_string(), Value::String(new_value.to_string()));
}

fn push_unique_blocker(blockers: &mut Vec<String>, blocker: impl Into<String>) {
    let blocker = blocker.into();
    if !blockers.contains(&blocker) {
        blockers.push(blocker);
    }
}

fn effective_gateway_wss_url(config: &Config) -> String {
    let gateway_url = config.combo_rfq_gateway_wss_url.trim();
    if gateway_url.is_empty() {
        config.combo_rfq_grpc_url.trim().to_string()
    } else {
        gateway_url.to_string()
    }
}

fn update_checkpoint_from_event(
    checkpoint: &mut ComboRfqStreamCheckpoint,
    event: &ComboRfqStreamEvent,
    payload: &Value,
    source: &str,
    generated_at: &str,
) {
    let source = source.to_ascii_lowercase();
    if source.contains("dropcopy") || source.contains("trade") || source.contains("order") {
        checkpoint.last_dropcopy_event_at = Some(generated_at.to_string());
        checkpoint.last_dropcopy_resume_token = event
            .resume_token
            .clone()
            .or_else(|| text_value(payload, &["resumeToken", "resume_token"]))
            .or_else(|| checkpoint.last_dropcopy_resume_token.clone());
    } else {
        checkpoint.last_rfq_event_at = Some(generated_at.to_string());
    }
    if source.contains("heartbeat") {
        checkpoint.last_heartbeat_at = Some(generated_at.to_string());
    }
    checkpoint.reconnect_count = checkpoint
        .reconnect_count
        .saturating_add(event.reconnect_count);
    if event.gap_detected {
        checkpoint.gap_count = checkpoint.gap_count.saturating_add(1);
    }
}

fn read_checkpoint(path: &Path) -> Result<ComboRfqStreamCheckpoint> {
    if !path.exists() {
        return Ok(ComboRfqStreamCheckpoint::default());
    }
    let body = fs::read_to_string(path)
        .with_context(|| format!("reading Combo/RFQ stream checkpoint {}", path.display()))?;
    serde_json::from_str(&body)
        .with_context(|| format!("parsing Combo/RFQ stream checkpoint {}", path.display()))
}

fn write_checkpoint(path: &Path, checkpoint: &ComboRfqStreamCheckpoint) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating diagnostics directory {}", parent.display()))?;
    }
    fs::write(path, serde_json::to_string_pretty(checkpoint)?)
        .with_context(|| format!("writing Combo/RFQ stream checkpoint {}", path.display()))
}

fn set_missing_string(value: &mut Value, key: &str, new_value: &str) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if !object.contains_key(key) {
        object.insert(key.to_string(), Value::String(new_value.to_string()));
    }
}

fn text_value(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        let Some(field) = value.get(*key) else {
            continue;
        };
        let text = match field {
            Value::String(text) => text.clone(),
            Value::Number(number) => number.to_string(),
            Value::Bool(value) => value.to_string(),
            _ => continue,
        };
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn deadline_millis(value: &Value, keys: &[&str]) -> Option<i64> {
    let text = text_value(value, keys)?;
    text.parse::<i64>().ok().or_else(|| {
        DateTime::parse_from_rfc3339(&text)
            .ok()
            .map(|timestamp| timestamp.timestamp_millis())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onchain_fills::order_filled_v2_topic;
    use crate::rfq_finality::{
        build_combo_rfq_finality_report, COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE,
    };
    use polymarket_client_sdk_v2::types::{Address, B256, U256};
    use std::str::FromStr;

    const TEST_ORDER_HASH: &str =
        "0x0404040404040404040404040404040404040404040404040404040404040404";
    const TEST_TRANSACTION_HASH: &str = "0xabc";

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        std::env::temp_dir().join(format!("polymarket-rfq-stream-{name}-{suffix}"))
    }

    fn ready_config(dir: PathBuf) -> Config {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.live_funder_address = test_account().to_string();
        cfg.combo_rfq_stream_enabled = true;
        cfg.combo_rfq_gateway_wss_url = crate::config::DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL.into();
        cfg.combo_rfq_grpc_url.clear();
        cfg.combo_rfq_stream_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.combo_rfq_finality_min_confirmed_samples = 1;
        cfg
    }

    fn test_account() -> Address {
        Address::from_str("0x0000000000000000000000000000000000000001").unwrap()
    }

    fn write_onchain_order_filled_log(dir: &Path) {
        fs::create_dir_all(dir).unwrap();
        let account = test_account();
        let taker = Address::from_str("0x0000000000000000000000000000000000000002").unwrap();
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

    fn write_user_confirmed_trade(dir: &Path) {
        fs::create_dir_all(dir).unwrap();
        let event = serde_json::json!({
            "event_type": "trade",
            "id": "trade-rfq",
            "taker_order_id": TEST_ORDER_HASH,
            "transaction_hash": TEST_TRANSACTION_HASH,
            "rfq_id": "rfq-1",
            "quote_id": "quote-1",
            "asset_id": "202",
            "side": "BUY",
            "size": "10",
            "price": "0.75",
            "status": "CONFIRMED"
        });
        fs::write(
            dir.join(crate::user_channel::LIVE_USER_EVENTS_FILE),
            format!("{event}\n"),
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
    fn stream_writer_appends_events_and_checkpoint_for_finality_report() {
        let dir = temp_dir("happy");
        let cfg = ready_config(dir.clone());
        write_onchain_order_filled_log(&dir);
        write_user_confirmed_trade(&dir);
        let events = vec![
            ComboRfqStreamEvent {
                source: Some("rfq".into()),
                payload: serde_json::json!({
                    "id": "evt-accepted",
                    "rfqId": "rfq-1",
                    "quoteId": "quote-1",
                    "makerId": "maker-1",
                    "status": "quote_accepted"
                }),
                generated_at: Some(Utc::now().to_rfc3339()),
                resume_token: None,
                gap_detected: false,
                reconnect_count: 0,
            },
            ComboRfqStreamEvent {
                source: Some("rfq".into()),
                payload: serde_json::json!({
                    "id": "evt-pending",
                    "rfqId": "rfq-1",
                    "quoteId": "quote-1",
                    "makerId": "maker-1",
                    "status": "quote_pending_end_trade"
                }),
                generated_at: Some(Utc::now().to_rfc3339()),
                resume_token: None,
                gap_detected: false,
                reconnect_count: 0,
            },
            ComboRfqStreamEvent {
                source: Some("dropcopy".into()),
                payload: serde_json::json!({
                    "id": "evt-filled",
                    "rfqId": "rfq-1",
                    "quoteId": "quote-1",
                    "makerId": "maker-1",
                    "status": "filled",
                    "realizedEvUsd": 1.5,
                    "orderHash": TEST_ORDER_HASH,
                    "transactionHash": TEST_TRANSACTION_HASH,
                    "side": "BUY",
                    "tokenId": "202",
                    "makerAmountFilled": "750000",
                    "takerAmountFilled": "1000000",
                    "fee": "0",
                    "resumeToken": "resume-1"
                }),
                generated_at: Some(Utc::now().to_rfc3339()),
                resume_token: Some("resume-1".into()),
                gap_detected: false,
                reconnect_count: 0,
            },
        ];

        let write_report = append_combo_rfq_stream_events(&cfg, &events).unwrap();
        let finality_path = crate::rfq_finality::write_combo_rfq_finality_report(&cfg).unwrap();
        let finality_report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(write_report.events_written, 3);
        assert_eq!(
            write_report
                .checkpoint
                .last_dropcopy_resume_token
                .as_deref(),
            Some("resume-1")
        );
        assert!(finality_path.exists());
        assert_eq!(
            finality_report.lifecycle.valid_sessions, 1,
            "lifecycle={:?} blockers={:?}",
            finality_report.lifecycle, finality_report.blockers
        );
        assert!(finality_report.blockers.is_empty());
    }

    #[test]
    fn stream_writer_populates_hot_event_cache() {
        crate::rfq_finality::clear_combo_rfq_stream_event_cache_for_tests();
        let cfg = ready_config(temp_dir("hot-cache"));
        let events = vec![ComboRfqStreamEvent {
            source: Some("rfq".into()),
            payload: serde_json::json!({
                "id": "evt-cache",
                "rfqId": "rfq-cache-writer",
                "quoteId": "quote-cache-writer",
                "makerId": "maker-cache",
                "status": "ACTIVE",
                "ageMs": 5
            }),
            generated_at: Some(Utc::now().to_rfc3339()),
            resume_token: None,
            gap_detected: false,
            reconnect_count: 0,
        }];

        append_combo_rfq_stream_events(&cfg, &events).unwrap();
        let cached =
            crate::rfq_finality::cached_combo_rfq_stream_events_for_rfq("rfq-cache-writer");

        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0]["quoteId"], "quote-cache-writer");
    }

    #[test]
    fn stream_writer_records_gap_for_finality_blocker() {
        let dir = temp_dir("gap");
        let cfg = ready_config(dir);
        let events = vec![ComboRfqStreamEvent {
            source: Some("dropcopy".into()),
            payload: serde_json::json!({
                "id": "evt-gap",
                "rfqId": "rfq-gap",
                "quoteId": "quote-gap",
                "status": "filled",
                "realizedEvUsd": 1.0,
                "resumeToken": "resume-gap"
            }),
            generated_at: Some(Utc::now().to_rfc3339()),
            resume_token: Some("resume-gap".into()),
            gap_detected: true,
            reconnect_count: 1,
        }];

        let report = append_combo_rfq_stream_events(&cfg, &events).unwrap();

        assert_eq!(report.checkpoint.gap_count, 1);
        assert!(report.blockers.contains(&"rfq_stream_gap:1".to_string()));
    }

    #[test]
    fn live_stream_status_ready_requires_fresh_same_process_connection() {
        let dir = temp_dir("status-ready");
        let cfg = ready_config(dir);
        let identity = combo_rfq_stream_identity(&cfg, &effective_gateway_wss_url(&cfg));
        write_combo_rfq_stream_status(
            &cfg.diagnostics_dir,
            &identity,
            true,
            "connected",
            Some("auth frame sent"),
            false,
        )
        .unwrap();

        let err = ensure_live_combo_rfq_stream_ready(&cfg).unwrap_err();
        assert!(err.to_string().contains("no inbound messages"));

        write_combo_rfq_stream_status(
            &cfg.diagnostics_dir,
            &identity,
            true,
            "message_received",
            Some("text"),
            true,
        )
        .unwrap();
        ensure_live_combo_rfq_stream_ready(&cfg).unwrap();

        write_combo_rfq_stream_status(
            &cfg.diagnostics_dir,
            &identity,
            false,
            "closed",
            Some("test close"),
            false,
        )
        .unwrap();
        let err = ensure_live_combo_rfq_stream_ready(&cfg).unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[test]
    fn live_stream_status_guard_rejects_wrong_process_and_stale_status() {
        let dir = temp_dir("status-identity");
        let cfg = ready_config(dir);
        let identity = combo_rfq_stream_identity(&cfg, &effective_gateway_wss_url(&cfg));
        let wrong_process = ComboRfqStreamStatus {
            timestamp: Utc::now().to_rfc3339(),
            connected: true,
            stage: "connected".into(),
            detail: None,
            last_inbound_at: None,
            process_id: identity.process_id.saturating_add(1),
            gateway_wss_url: identity.gateway_wss_url.clone(),
            participant_id_fingerprint: identity.participant_id_fingerprint.clone(),
            connection_nonce: identity.connection_nonce.clone(),
        };
        fs::create_dir_all(&cfg.diagnostics_dir).unwrap();
        fs::write(
            cfg.diagnostics_dir.join(COMBO_RFQ_STREAM_STATUS_FILE),
            serde_json::to_string_pretty(&wrong_process).unwrap(),
        )
        .unwrap();
        let err = ensure_live_combo_rfq_stream_ready(&cfg).unwrap_err();
        assert!(err.to_string().contains("another process"));

        let stale = ComboRfqStreamStatus {
            process_id: identity.process_id,
            timestamp: (Utc::now()
                - chrono::Duration::seconds(COMBO_RFQ_STREAM_STATUS_MAX_AGE_SECS + 1))
            .to_rfc3339(),
            connected: true,
            stage: "connected".into(),
            detail: None,
            last_inbound_at: Some(Utc::now().to_rfc3339()),
            gateway_wss_url: identity.gateway_wss_url,
            participant_id_fingerprint: identity.participant_id_fingerprint,
            connection_nonce: identity.connection_nonce,
        };
        fs::write(
            cfg.diagnostics_dir.join(COMBO_RFQ_STREAM_STATUS_FILE),
            serde_json::to_string_pretty(&stale).unwrap(),
        )
        .unwrap();
        let err = ensure_live_combo_rfq_stream_ready(&cfg).unwrap_err();
        assert!(err.to_string().contains("stale"));
    }

    #[test]
    fn shadow_session_report_is_ready_for_auth_but_no_submit() {
        let dir = temp_dir("shadow");
        let cfg = ready_config(dir.clone());

        let path = write_combo_rfq_shadow_session_report(&cfg).unwrap();
        let report = build_combo_rfq_shadow_session_report(&cfg);

        assert!(path.exists());
        assert_eq!(report.mode, "shadow_no_submit");
        assert!(!report.live_submissions_enabled);
        assert!(report.auth_ready);
        assert_eq!(report.deadlines.quote_response_window_ms, 400);
        assert_eq!(report.deadlines.user_accept_window_ms, 5_000);
        assert_eq!(report.deadlines.last_look_window_ms, 1_000);
        assert_eq!(report.status, "blocked_shadow_no_submit");
        assert!(report
            .blockers
            .contains(&"rfq_wss_shadow_session_transport_not_started".to_string()));
        assert!(report
            .blockers
            .contains(&"rfq_wss_shadow_session_no_submit_until_metrics_promote".to_string()));
        assert!(report
            .expected_steps
            .contains(&"ingest_rfq_request".to_string()));
    }

    #[test]
    fn shadow_journal_records_rfq_request_deadline_without_quote_submit() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-04T10:53:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let submission_deadline = received_at.timestamp_millis() + 250;
        let message = serde_json::json!({
            "type": "RFQ_REQUEST",
            "rfq_id": "rfq-shadow",
            "yes_position_id": "202",
            "no_position_id": "203",
            "side": "YES",
            "requested_size": {
                "unit": "notional",
                "value_e6": "1000000"
            },
            "submission_deadline": submission_deadline
        });

        let record = combo_rfq_shadow_journal_record_from_message(&message, received_at, false);

        assert_eq!(record.message_type, "RFQ_REQUEST");
        assert_eq!(record.rfq_id.as_deref(), Some("rfq-shadow"));
        assert_eq!(record.decision, "would_not_quote_shadow_only");
        assert_eq!(record.deadline_kind.as_deref(), Some("quote_submission"));
        assert_eq!(record.deadline_ms_remaining, Some(250));
        assert!(!record.normalized_event_written);
        assert!(record.blockers.contains(&"shadow_no_submit".to_string()));
        assert!(record
            .blockers
            .contains(&"signed_quote_engine_not_enabled".to_string()));
        let canary = record.quote_canary.as_ref().unwrap();
        assert_eq!(canary.outbound_type, "RFQ_QUOTE");
        assert_eq!(canary.rfq_id.as_deref(), Some("rfq-shadow"));
        assert_eq!(canary.token_id.as_deref(), Some("202"));
        assert_eq!(canary.size_e6.as_deref(), Some("1000000"));
        assert!(canary.request_fields_ready);
        assert!(!canary.signing_inputs_ready);
        assert!(!canary.outbound_schema_ready);
        assert!(!canary.live_submission_enabled);
        assert!(canary
            .signed_order_required_fields
            .contains(&"signature".to_string()));
        assert!(canary
            .blockers
            .contains(&"rfq_quote_price_e6_not_computed".to_string()));
        assert!(record
            .blockers
            .contains(&"quote_canary:rfq_quote_signed_order_not_built".to_string()));
        assert!(!record
            .blockers
            .iter()
            .any(|blocker| blocker.starts_with("deadline_expired:")));
    }

    #[test]
    fn shadow_journal_records_expired_last_look_without_confirm_submit() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-04T10:53:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let confirm_by = received_at.timestamp_millis() - 10;
        let message = serde_json::json!({
            "type": "RFQ_CONFIRMATION_REQUEST",
            "rfq_id": "rfq-shadow",
            "quote_id": "quote-shadow",
            "maker_address": "0x0000000000000000000000000000000000000001",
            "signature_type": 0,
            "yes_position_id": "202",
            "side": "YES",
            "fill_size_e6": "1000000",
            "price_e6": "450000",
            "confirm_by": confirm_by
        });

        let cfg = ready_config(temp_dir("last-look"));
        let record = combo_rfq_shadow_journal_record_from_message_with_config(
            &message,
            received_at,
            true,
            Some(&cfg),
        );

        assert_eq!(record.message_type, "RFQ_CONFIRMATION_REQUEST");
        assert_eq!(record.quote_id.as_deref(), Some("quote-shadow"));
        assert_eq!(record.decision, "would_not_confirm_shadow_only");
        assert_eq!(record.deadline_kind.as_deref(), Some("last_look"));
        assert_eq!(record.deadline_ms_remaining, Some(-10));
        let last_look = record.last_look.as_ref().unwrap();
        assert_eq!(last_look.decision, "would_decline_shadow_only");
        assert_eq!(last_look.token_id.as_deref(), Some("202"));
        assert_eq!(last_look.price_e6.as_deref(), Some("450000"));
        assert_eq!(last_look.fill_size_e6.as_deref(), Some("1000000"));
        assert!(last_look.request_fields_ready);
        assert!(last_look.identity_matches_config);
        assert!(!last_look.live_confirmation_enabled);
        assert!(last_look
            .blockers
            .contains(&"last_look_deadline_expired".to_string()));
        assert!(last_look
            .blockers
            .contains(&"last_look_ev_not_computed".to_string()));
        assert!(record.normalized_event_written);
        assert!(record
            .blockers
            .contains(&"last_look_confirmation_disabled_in_shadow".to_string()));
        assert!(record
            .blockers
            .contains(&"deadline_expired:last_look".to_string()));
        assert!(record
            .blockers
            .contains(&"last_look:last_look_fresh_book_not_checked".to_string()));
    }

    #[test]
    fn shadow_journal_appends_jsonl_records() {
        let dir = temp_dir("shadow-journal");
        let cfg = ready_config(dir.clone());
        let received_at = DateTime::parse_from_rfc3339("2026-06-04T10:53:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let records = vec![
            combo_rfq_shadow_journal_record_from_message(
                &serde_json::json!({
                    "type": "RFQ_REQUEST",
                    "rfq_id": "rfq-one",
                    "yes_position_id": "202",
                    "side": "YES",
                    "requested_size": {"value_e6": "1000000"},
                    "submission_deadline": received_at.timestamp_millis() + 400
                }),
                received_at,
                false,
            ),
            combo_rfq_shadow_journal_record_from_message(
                &serde_json::json!({
                    "type": "RFQ_ERROR",
                    "rfq_id": "rfq-one",
                    "code": "example"
                }),
                received_at,
                true,
            ),
        ];

        let path = append_combo_rfq_shadow_journal_records(&cfg, &records).unwrap();
        let body = fs::read_to_string(path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        let first: ComboRfqShadowJournalRecord = serde_json::from_str(lines[0]).unwrap();
        let second: ComboRfqShadowJournalRecord = serde_json::from_str(lines[1]).unwrap();

        assert_eq!(lines.len(), 2);
        assert_eq!(first.message_type, "RFQ_REQUEST");
        assert_eq!(second.message_type, "RFQ_ERROR");
        assert_eq!(
            dir.join(COMBO_RFQ_SHADOW_JOURNAL_FILE),
            cfg.diagnostics_dir.join(COMBO_RFQ_SHADOW_JOURNAL_FILE)
        );
    }

    #[test]
    fn shadow_deadline_canary_flags_runtime_window_drift() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-04T10:53:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let records = vec![
            combo_rfq_shadow_journal_record_from_message(
                &serde_json::json!({
                    "type": "RFQ_REQUEST",
                    "rfq_id": "rfq-wide",
                    "yes_position_id": "202",
                    "side": "YES",
                    "requested_size": {"value_e6": "1000000"},
                    "submission_deadline": received_at.timestamp_millis() + 900
                }),
                received_at,
                false,
            ),
            combo_rfq_shadow_journal_record_from_message(
                &serde_json::json!({
                    "type": "RFQ_CONFIRMATION_REQUEST",
                    "rfq_id": "rfq-wide",
                    "quote_id": "quote-wide",
                    "yes_position_id": "202",
                    "side": "YES",
                    "fill_size_e6": "1000000",
                    "price_e6": "450000",
                    "confirm_by": received_at.timestamp_millis() + 2_500
                }),
                received_at,
                true,
            ),
            combo_rfq_shadow_journal_record_from_message(
                &serde_json::json!({
                    "type": "RFQ_REQUEST",
                    "rfq_id": "rfq-missing",
                    "yes_position_id": "202",
                    "side": "YES",
                    "requested_size": {"value_e6": "1000000"}
                }),
                received_at,
                false,
            ),
        ];
        let expected = ComboRfqShadowSessionDeadlines {
            quote_response_window_ms: 400,
            user_accept_window_ms: 5_000,
            last_look_window_ms: 1_000,
            reconnect_backoff_ms: 1_000,
        };

        let (observed, alerts) = combo_rfq_shadow_observed_deadlines(&records, &expected);

        assert_eq!(observed.quote_submission_samples, 1);
        assert_eq!(observed.quote_submission_max_ms, Some(900));
        assert_eq!(observed.last_look_samples, 1);
        assert_eq!(observed.last_look_max_ms, Some(2_500));
        assert_eq!(observed.missing_deadline_messages, 1);
        assert!(
            alerts.contains(&"quote_submission_window_drift_observed_max:900ms>500ms".to_string())
        );
        assert!(alerts.contains(&"last_look_window_drift_observed_max:2500ms>1100ms".to_string()));
        assert!(alerts.contains(&"deadline_fields_missing:1".to_string()));
    }

    #[tokio::test]
    async fn shadow_runner_blocks_before_network_when_config_missing() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_dir("shadow-blocked");
        cfg.combo_rfq_stream_enabled = false;
        cfg.combo_rfq_stream_bearer_token.clear();
        cfg.combo_rfq_participant_id.clear();

        let report = run_combo_rfq_wss_shadow_session(&cfg, 1, Duration::from_millis(1))
            .await
            .unwrap();

        assert_eq!(report.status, "blocked_config");
        assert!(!report.auth_sent);
        assert_eq!(report.raw_messages_seen, 0);
        assert_eq!(report.shadow_records_written, 0);
        assert_eq!(report.rfq_requests_seen, 0);
        assert_eq!(report.confirmation_requests_seen, 0);
        assert_eq!(report.expired_deadline_messages, 0);
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_STREAM_ENABLED=false".to_string()));
    }

    #[test]
    fn shadow_auth_frame_matches_quoter_gateway_shape() {
        std::env::set_var("CLOB_API_KEY", "api-key");
        std::env::set_var("CLOB_SECRET", "secret");
        std::env::set_var("CLOB_PASS_PHRASE", "passphrase");
        std::env::set_var(
            "LIVE_SIGNER_ADDRESS",
            "0x0000000000000000000000000000000000000002",
        );
        let mut cfg = ready_config(temp_dir("shadow-auth"));
        cfg.live_funder_address = "0x0000000000000000000000000000000000000001".into();
        cfg.live_signature_type = 1;

        let frame = combo_rfq_shadow_auth_frame(&cfg).unwrap();

        assert_eq!(frame["type"], "auth");
        assert_eq!(frame["auth"]["apiKey"], "api-key");
        assert_eq!(frame["auth"]["secret"], "secret");
        assert_eq!(frame["auth"]["passphrase"], "passphrase");
        assert_eq!(
            frame["identity"]["signer_address"],
            "0x0000000000000000000000000000000000000002"
        );
        assert_eq!(
            frame["identity"]["maker_address"],
            "0x0000000000000000000000000000000000000001"
        );
        assert_eq!(frame["identity"]["signature_type"], 1);

        std::env::remove_var("CLOB_API_KEY");
        std::env::remove_var("CLOB_SECRET");
        std::env::remove_var("CLOB_PASS_PHRASE");
        std::env::remove_var("LIVE_SIGNER_ADDRESS");
    }

    #[test]
    fn wss_gateway_messages_normalize_into_finality_events_without_promoting_route() {
        let dir = temp_dir("wss-normalize");
        let cfg = ready_config(dir);
        let messages = vec![
            serde_json::json!({
                "type": "RFQ_CONFIRMATION_REQUEST",
                "rfq_id": "rfq-wss",
                "quote_id": "quote-wss",
                "maker_address": "maker-wss",
                "condition_id": "event-wss",
                "price_e6": "450000",
                "fill_size_e6": "1000000",
                "generatedAt": "2026-06-04T10:53:03Z",
                "confirm_by": 1780575184000_i64
            }),
            serde_json::json!({
                "type": "ACK_RFQ_CONFIRMATION_RESPONSE",
                "rfq_id": "rfq-wss",
                "quote_id": "quote-wss",
                "generatedAt": "2026-06-04T10:53:04Z",
                "decision": "CONFIRM"
            }),
            serde_json::json!({
                "type": "RFQ_TRADE",
                "rfq_id": "rfq-wss",
                "quote_id": "quote-wss",
                "requester_id": "req-public",
                "condition_id": "event-wss",
                "direction": "BUY",
                "side": "YES",
                "price_e6": "450000",
                "size_e6": "1000000",
                "executed_at": 1780575185000_i64
            }),
        ];

        let events = combo_rfq_gateway_messages_to_stream_events(&messages);
        let write_report = append_combo_rfq_stream_events(&cfg, &events).unwrap();
        crate::rfq_finality::write_combo_rfq_finality_report(&cfg).unwrap();
        let finality_report = build_combo_rfq_finality_report(&cfg).unwrap();

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].payload["status"], "quote_accepted");
        assert_eq!(events[1].payload["status"], "quote_pending_end_trade");
        assert_eq!(events[2].payload["status"], "filled");
        assert_eq!(events[2].payload["quoteId"], "quote-wss");
        assert_eq!(events[2].payload["price"], "0.450000");
        assert_eq!(write_report.events_written, 3);
        assert_eq!(finality_report.lifecycle.valid_sessions, 1);
        assert!(finality_report
            .blockers
            .contains(&"dropcopy_resume_token_missing".to_string()));
        assert!(finality_report
            .blockers
            .contains(&"missing_realized_ev_rfq_finality".to_string()));
    }

    #[test]
    fn stream_config_defaults_block_transport() {
        let cfg = Config::from_env();

        let report = combo_rfq_stream_config_report(&cfg);

        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_STREAM_ENABLED=false".to_string()));
        assert_eq!(
            report.gateway_wss_url,
            crate::config::DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL
        );
        assert_eq!(report.transport, "wss_quoter_gateway");
        assert!(!report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("GRPC_URL_empty")));
    }

    #[test]
    fn stream_config_blocks_undocumented_gateway_host_but_allows_docs_variants() {
        let mut cfg = Config::from_env();
        cfg.combo_rfq_stream_enabled = true;
        cfg.combo_rfq_stream_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();
        cfg.combo_rfq_grpc_url.clear();

        cfg.combo_rfq_gateway_wss_url = crate::config::DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL.into();
        let report = combo_rfq_stream_config_report(&cfg);
        assert!(!report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not_documented_quoter_gateway")));

        cfg.combo_rfq_gateway_wss_url =
            "wss://combos-rfq-gateway-quoter.polymarket.com/ws/rfq".into();
        let report = combo_rfq_stream_config_report(&cfg);
        assert!(!report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not_documented_quoter_gateway")));

        cfg.combo_rfq_gateway_wss_url = "wss://gateway.example.test/ws/rfq".into();
        let report = combo_rfq_stream_config_report(&cfg);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not_documented_quoter_gateway")));
    }
}
