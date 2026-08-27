//! Polymarket user-channel helpers.
//!
//! The live capturer is append-only: it records authenticated user-channel
//! order/trade events, then the report compares them with the live execution
//! journal so external account drift is visible before more live automation is
//! added. Live trading fails closed unless this channel is enabled.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, info, warn};
use url::Url;

use crate::config::Config;
use crate::live_executor::{
    append_live_route_replay_records_deduped, configured_live_account_address,
    LiveRouteReplayRecord,
};

const LIVE_EXECUTION_JOURNAL_FILE: &str = "live_execution_journal.jsonl";
const COMBO_RFQ_EXECUTION_JOURNAL_FILE: &str = "combo_rfq_execution_journal.jsonl";
pub const LIVE_USER_EVENTS_FILE: &str = "live_user_events.jsonl";
const LIVE_USER_RECONCILE_REPORT_FILE: &str = "live_user_reconcile_report.json";
const LIVE_USER_HALT_FILE: &str = "live_user_halt.flag";
const LIVE_USER_STATUS_FILE: &str = "live_user_channel_status.json";
const LIVE_USER_STATUS_MAX_AGE_SECS: i64 = 30;
const LIVE_USER_READY_WAIT_TIMEOUT_SECS: u64 = 30;
const LIVE_USER_READY_WAIT_POLL_MS: u64 = 250;
const LIVE_USER_EVENT_BUS_CAPACITY: usize = 1_024;
const CTF_MERGE_BUNDLE_SHADOW_ROUTE: &str = "ctf_merge_bundle_shadow";
static LIVE_USER_EVENT_BUS: std::sync::OnceLock<broadcast::Sender<NormalizedUserEvent>> =
    std::sync::OnceLock::new();

#[derive(Debug, Serialize)]
pub struct LiveUserReconcileReport {
    generated_at: String,
    ws_url: String,
    journal_path: String,
    user_events_path: String,
    subscription_mode: String,
    credential_env_present: bool,
    unresolved_journal_executions: usize,
    journal_order_ids: usize,
    journal_expected_order_hashes: usize,
    journal_trade_ids: usize,
    journal_condition_ids: usize,
    parsed_user_events: usize,
    malformed_event_lines: usize,
    ignored_messages: usize,
    heartbeat_messages: usize,
    alerts: Vec<UserReconcileAlert>,
    note: String,
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct UserReconcileAlert {
    severity: String,
    kind: String,
    order_id: Option<String>,
    trade_id: Option<String>,
    condition_id: Option<String>,
    status: Option<String>,
    reason: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct NormalizedUserEvent {
    event_type: String,
    stage: String,
    order_id: Option<String>,
    trade_id: Option<String>,
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
    timestamp: Option<String>,
    raw: Value,
}

#[derive(Debug, Default)]
pub struct UserChannelParseResult {
    events: Vec<NormalizedUserEvent>,
    malformed_messages: usize,
    ignored_messages: usize,
    heartbeat_messages: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveUserChannelFillStatus {
    pub confirmed_order_ids: Vec<String>,
    pub confirmed_trade_ids: Vec<String>,
    pub failed_order_ids: Vec<String>,
    pub failed_trade_ids: Vec<String>,
    pub pending_order_ids: Vec<String>,
    pub pending_trade_ids: Vec<String>,
    pub transaction_hashes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LiveUserHaltRecord {
    timestamp: String,
    reason: String,
    event: NormalizedUserEvent,
}

#[derive(Debug, Serialize, Deserialize)]
struct LiveUserChannelStatus {
    timestamp: String,
    connected: bool,
    stage: String,
    detail: Option<String>,
    last_inbound_at: Option<String>,
    account_address: Option<String>,
    api_key_fingerprint: Option<String>,
    process_id: Option<u32>,
    ws_url: Option<String>,
    subscription_mode: Option<String>,
    connection_nonce: Option<String>,
    last_inbound_type: Option<String>,
}

#[derive(Debug, Clone)]
struct LiveUserChannelIdentity {
    account_address: Option<String>,
    api_key_fingerprint: String,
    process_id: u32,
    ws_url: String,
    subscription_mode: String,
    connection_nonce: String,
}

#[derive(Debug, Default)]
struct JournalSnapshot {
    executions: HashMap<String, JournalExecution>,
}

#[derive(Debug, Default)]
struct JournalExecution {
    event_id: Option<String>,
    event_title: Option<String>,
    arb_type: Option<String>,
    position_usd: Option<f64>,
    projected_pnl_usd: Option<f64>,
    actual_entry_cost_usd: Option<f64>,
    stage: String,
    order_ids: HashSet<String>,
    expected_order_hashes: HashSet<String>,
    trade_ids: HashSet<String>,
    condition_ids: HashSet<String>,
    pending_intent_legs: Vec<JournalPendingIntentLeg>,
}

#[derive(Debug, Clone)]
struct JournalPendingIntentLeg {
    condition_id: String,
    token_id: String,
    side: String,
    size: f64,
    limit_price: f64,
}

#[derive(Debug, Default)]
struct ComboRfqUserJournalSnapshot {
    records: Vec<ComboRfqUserJournalRecord>,
}

#[derive(Debug, Deserialize)]
struct ComboRfqUserJournalRecord {
    status: Option<String>,
    client_request_id: Option<String>,
    rfq_id: Option<String>,
    quote_id: Option<String>,
    selected_quote: Option<Value>,
    accept_request: Option<Value>,
    response: Option<Value>,
}

#[derive(Debug, Clone)]
struct UserChannelCredentials {
    api_key: String,
    secret: String,
    passphrase: String,
}

#[derive(Debug, Deserialize)]
struct JournalLine {
    execution_id: Option<String>,
    stage: Option<String>,
    event_id: Option<String>,
    event_title: Option<String>,
    arb_type: Option<String>,
    position_usd: Option<f64>,
    projected_pnl_usd: Option<f64>,
    actual_entry_cost_usd: Option<f64>,
    order_ids: Option<Vec<String>>,
    expected_order_hashes: Option<Vec<String>>,
    trade_ids: Option<Vec<String>>,
    legs: Option<Vec<JournalLeg>>,
}

#[derive(Debug, Deserialize)]
struct JournalLeg {
    condition_id: Option<String>,
    token_id: Option<String>,
    side: Option<String>,
    size: Option<f64>,
    limit_price: Option<f64>,
}

impl UserChannelCredentials {
    fn from_env() -> Result<Self> {
        Ok(Self {
            api_key: required_env("POLYMARKET_API_KEY")?,
            secret: required_env("POLYMARKET_API_SECRET")?,
            passphrase: required_env("POLYMARKET_API_PASSPHRASE")?,
        })
    }
}

pub fn write_live_user_reconcile_report(config: &Config) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let journal_path = config.diagnostics_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
    let events_path = config.diagnostics_dir.join(LIVE_USER_EVENTS_FILE);
    let report = build_live_user_reconcile_report(config, &journal_path, &events_path)?;
    let report_path = config.diagnostics_dir.join(LIVE_USER_RECONCILE_REPORT_FILE);
    let file = File::create(&report_path)
        .with_context(|| format!("creating {}", report_path.display()))?;
    serde_json::to_writer_pretty(file, &report)?;
    Ok(report_path)
}

pub fn write_live_route_replay_labels_from_user_events(config: &Config) -> Result<usize> {
    let journal_path = config.diagnostics_dir.join(LIVE_EXECUTION_JOURNAL_FILE);
    let events_path = config.diagnostics_dir.join(LIVE_USER_EVENTS_FILE);
    let journal = read_journal_snapshot(&journal_path)?;
    let (events, _, _, _) = read_user_event_file(&events_path)?;
    let records = derive_live_route_replay_records(&journal, &events);
    append_live_route_replay_records_deduped(config, &records)
}

pub fn live_user_channel_fill_status(
    config: &Config,
    order_ids: &[String],
    trade_ids: &[String],
) -> Result<LiveUserChannelFillStatus> {
    let events_path = config.diagnostics_dir.join(LIVE_USER_EVENTS_FILE);
    let (events, _, _, _) = read_user_event_file(&events_path)?;
    Ok(live_user_channel_fill_status_from_events(
        &events, order_ids, trade_ids,
    ))
}

pub async fn wait_for_live_user_channel_fill_status(
    order_ids: &[String],
    trade_ids: &[String],
    timeout: Duration,
) -> LiveUserChannelFillStatus {
    let mut receiver = live_user_event_bus().subscribe();
    let deadline = time::Instant::now() + timeout;
    let mut events = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match time::timeout(remaining, receiver.recv()).await {
            Ok(Ok(event)) => {
                events.push(event);
                let status =
                    live_user_channel_fill_status_from_events(&events, order_ids, trade_ids);
                if !status.failed_order_ids.is_empty()
                    || !status.failed_trade_ids.is_empty()
                    || !status.confirmed_order_ids.is_empty()
                    || !status.confirmed_trade_ids.is_empty()
                {
                    return status;
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => break,
        }
    }
    live_user_channel_fill_status_from_events(&events, order_ids, trade_ids)
}

pub fn spawn_live_user_channel_capturer(config: Config) -> Result<JoinHandle<()>> {
    let credentials = UserChannelCredentials::from_env()?;
    let url = Url::parse(&config.clob_user_ws_url)
        .with_context(|| format!("invalid CLOB_USER_WS_URL={}", config.clob_user_ws_url))?;
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    Ok(tokio::spawn(async move {
        run_live_user_channel_capturer(config, url, credentials).await;
    }))
}

pub fn ensure_live_user_channel_ready(config: &Config) -> Result<()> {
    ensure_live_user_channel_configured(config)?;
    ensure_live_user_channel_status_fresh(config)?;
    ensure_no_live_user_channel_halt(config)
}

pub async fn wait_for_live_user_channel_ready(config: &Config) -> Result<()> {
    wait_for_live_user_channel_ready_with_timeout(
        config,
        Duration::from_secs(LIVE_USER_READY_WAIT_TIMEOUT_SECS),
    )
    .await
}

async fn wait_for_live_user_channel_ready_with_timeout(
    config: &Config,
    timeout: Duration,
) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let err = match ensure_live_user_channel_ready(config) {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };
        if std::time::Instant::now() >= deadline {
            bail!(
                "live user-channel did not become ready within {}ms: {}",
                timeout.as_millis(),
                err
            );
        }
        time::sleep(Duration::from_millis(LIVE_USER_READY_WAIT_POLL_MS)).await;
    }
}

pub fn ensure_live_user_channel_configured(config: &Config) -> Result<()> {
    if !config.live_user_ws_enabled {
        bail!(
            "LIVE_USER_WS_ENABLED=true is required for live execution; authenticated user-channel reconciliation must be active before live submit"
        );
    }
    UserChannelCredentials::from_env()
        .context("live execution requires user-channel API credentials")?;
    Url::parse(&config.clob_user_ws_url)
        .with_context(|| format!("invalid CLOB_USER_WS_URL={}", config.clob_user_ws_url))?;
    ensure_no_live_user_channel_halt(config)
}

pub fn mark_live_user_channel_starting(config: &Config) -> Result<()> {
    write_live_user_status(
        &config.diagnostics_dir,
        None,
        false,
        "starting",
        Some("live execution is waiting for authenticated user-channel connection"),
        false,
        None,
    )
}

pub fn ensure_no_live_user_channel_halt(config: &Config) -> Result<()> {
    let path = config.diagnostics_dir.join(LIVE_USER_HALT_FILE);
    if path.exists() {
        let reason = fs::read_to_string(&path).unwrap_or_else(|_| "unreadable halt file".into());
        bail!(
            "live user-channel halt is present at {}: {}; reconcile account state and remove the halt file before live submit",
            path.display(),
            reason.trim()
        );
    }
    Ok(())
}

fn ensure_live_user_channel_status_fresh(config: &Config) -> Result<()> {
    let path = config.diagnostics_dir.join(LIVE_USER_STATUS_FILE);
    let body = fs::read_to_string(&path).with_context(|| {
        format!(
            "live user-channel status file is missing at {}; capturer has not reported a live connection",
            path.display()
        )
    })?;
    let status: LiveUserChannelStatus = serde_json::from_str(&body)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    if !status.connected {
        bail!(
            "live user-channel is not connected: stage={} detail={}",
            status.stage,
            status.detail.as_deref().unwrap_or("none")
        );
    }
    let timestamp = DateTime::parse_from_rfc3339(&status.timestamp)
        .with_context(|| {
            format!(
                "invalid live user-channel status timestamp in {}",
                path.display()
            )
        })?
        .with_timezone(&Utc);
    let age = Utc::now().signed_duration_since(timestamp).num_seconds();
    if !(0..=LIVE_USER_STATUS_MAX_AGE_SECS).contains(&age) {
        bail!(
            "live user-channel status is stale: age={}s max={}s stage={}",
            age,
            LIVE_USER_STATUS_MAX_AGE_SECS,
            status.stage
        );
    }
    let last_inbound_at = status.last_inbound_at.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "live user-channel has no inbound activity recorded; outbound heartbeat alone is not sufficient for live submit"
        )
    })?;
    let last_inbound_at = DateTime::parse_from_rfc3339(last_inbound_at)
        .with_context(|| {
            format!(
                "invalid live user-channel last_inbound_at timestamp in {}",
                path.display()
            )
        })?
        .with_timezone(&Utc);
    let inbound_age = Utc::now()
        .signed_duration_since(last_inbound_at)
        .num_seconds();
    if !(0..=LIVE_USER_STATUS_MAX_AGE_SECS).contains(&inbound_age) {
        bail!(
            "live user-channel inbound stream is stale: age={}s max={}s stage={}",
            inbound_age,
            LIVE_USER_STATUS_MAX_AGE_SECS,
            status.stage
        );
    }
    ensure_live_user_channel_status_identity(config, &status)?;
    Ok(())
}

pub fn build_user_subscription_frame(
    api_key: &str,
    secret: &str,
    passphrase: &str,
    markets: &[String],
) -> Value {
    let mut frame = json!({
        "type": "user",
        "auth": {
            "apiKey": api_key,
            "secret": secret,
            "passphrase": passphrase
        }
    });
    if !markets.is_empty() {
        frame["markets"] = json!(markets);
    }
    frame
}

#[cfg(test)]
pub fn build_user_subscription_update(operation: &str, markets: &[String]) -> Value {
    json!({
        "operation": operation,
        "markets": markets
    })
}

pub fn heartbeat_payload() -> &'static str {
    "PING"
}

fn live_user_event_bus() -> &'static broadcast::Sender<NormalizedUserEvent> {
    LIVE_USER_EVENT_BUS.get_or_init(|| {
        let (sender, _) = broadcast::channel(LIVE_USER_EVENT_BUS_CAPACITY);
        sender
    })
}

fn emit_live_user_events(events: &[NormalizedUserEvent]) {
    let sender = live_user_event_bus();
    for event in events {
        let _ = sender.send(event.clone());
    }
}

#[cfg(test)]
pub fn append_live_user_events_from_payload(root_dir: &Path, payload: &str) -> Result<usize> {
    let parsed = parse_user_channel_payload(payload);
    if parsed.malformed_messages > 0 {
        bail!("malformed user-channel payload");
    }
    if parsed.events.is_empty() {
        return Ok(0);
    }

    fs::create_dir_all(root_dir)
        .with_context(|| format!("creating diagnostics directory {}", root_dir.display()))?;
    let path = root_dir.join(LIVE_USER_EVENTS_FILE);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    write_normalized_user_events(&mut writer, &parsed.events)?;
    writer.flush()?;
    emit_live_user_events(&parsed.events);
    Ok(parsed.events.len())
}

pub fn parse_user_channel_payload(payload: &str) -> UserChannelParseResult {
    let trimmed = payload.trim();
    if trimmed.is_empty() {
        return UserChannelParseResult::default();
    }
    if trimmed.eq_ignore_ascii_case("PING") || trimmed.eq_ignore_ascii_case("PONG") {
        return UserChannelParseResult {
            heartbeat_messages: 1,
            ..Default::default()
        };
    }

    let value: Value = match serde_json::from_str(trimmed) {
        Ok(value) => value,
        Err(_) => {
            return UserChannelParseResult {
                malformed_messages: 1,
                ..Default::default()
            };
        }
    };
    let values: Vec<&Value> = match &value {
        Value::Array(items) => items.iter().collect(),
        Value::Object(_) => vec![&value],
        _ => {
            return UserChannelParseResult {
                ignored_messages: 1,
                ..Default::default()
            };
        }
    };

    let mut result = UserChannelParseResult::default();
    for item in values {
        match normalize_user_event(item) {
            Some(event) => result.events.push(event),
            None => result.ignored_messages += 1,
        }
    }
    result
}

fn build_live_user_reconcile_report(
    config: &Config,
    journal_path: &Path,
    events_path: &Path,
) -> Result<LiveUserReconcileReport> {
    let journal = read_journal_snapshot(journal_path)?;
    let combo_rfq_journal = read_combo_rfq_user_journal_snapshot(
        &config
            .diagnostics_dir
            .join(COMBO_RFQ_EXECUTION_JOURNAL_FILE),
    )?;
    let (events, malformed_event_lines, ignored_messages, heartbeat_messages) =
        read_user_event_file(events_path)?;
    let unresolved = unresolved_executions(&journal);
    let order_ids = journal_order_ids(&unresolved);
    let expected_order_hashes = journal_expected_order_hashes(&unresolved);
    let trade_ids = journal_trade_ids(&unresolved);
    let condition_ids = journal_condition_ids(&unresolved);
    let alerts = reconcile_user_events(
        &events,
        &order_ids,
        &trade_ids,
        &condition_ids,
        &combo_rfq_journal,
    );
    Ok(LiveUserReconcileReport {
        generated_at: Utc::now().to_rfc3339(),
        ws_url: config.clob_user_ws_url.clone(),
        journal_path: journal_path.display().to_string(),
        user_events_path: events_path.display().to_string(),
        subscription_mode: "all_markets".to_string(),
        credential_env_present: clob_api_credentials_present(),
        unresolved_journal_executions: unresolved.len(),
        journal_order_ids: order_ids.len(),
        journal_expected_order_hashes: expected_order_hashes.len(),
        journal_trade_ids: trade_ids.len(),
        journal_condition_ids: condition_ids.len(),
        parsed_user_events: events.len(),
        malformed_event_lines,
        ignored_messages,
        heartbeat_messages,
        alerts,
        note: "read-only report; CONFIRMED user trades do not clear retained position exposure"
            .to_string(),
    })
}

async fn run_live_user_channel_capturer(
    config: Config,
    url: Url,
    credentials: UserChannelCredentials,
) {
    let identity = live_user_channel_identity(&config, &url, &credentials);
    let frame = build_user_subscription_frame(
        &credentials.api_key,
        &credentials.secret,
        &credentials.passphrase,
        &[],
    )
    .to_string();
    let events_path = config.diagnostics_dir.join(LIVE_USER_EVENTS_FILE);

    loop {
        info!("User WS: connecting to {}...", url);
        match connect_async(&url).await {
            Ok((mut ws_stream, _)) => {
                info!("User WS: connected; subscribing to all markets");
                if let Err(err) = ws_stream.send(Message::Text(frame.clone())).await {
                    warn!("User WS: failed to send auth subscription: {err}");
                    record_live_user_status(
                        &config.diagnostics_dir,
                        &identity,
                        false,
                        "subscription_failed",
                        Some(&err.to_string()),
                    );
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                let mut writer = match open_user_events_writer(&events_path) {
                    Ok(writer) => writer,
                    Err(err) => {
                        warn!("User WS: failed to open event journal: {err:#}");
                        record_live_user_status(
                            &config.diagnostics_dir,
                            &identity,
                            false,
                            "journal_open_failed",
                            Some(&err.to_string()),
                        );
                        time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };
                record_live_user_status(
                    &config.diagnostics_dir,
                    &identity,
                    true,
                    "subscribed",
                    Some("authenticated all-market user-channel subscription active"),
                );
                let mut heartbeat = time::interval(Duration::from_secs(10));

                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            if let Err(err) = ws_stream.send(Message::Text(heartbeat_payload().to_string())).await {
                                warn!("User WS: heartbeat send failed: {err}");
                                record_live_user_status(
                                    &config.diagnostics_dir,
                                    &identity,
                                    false,
                                    "heartbeat_failed",
                                    Some(&err.to_string()),
                                );
                                break;
                            }
                            record_live_user_status(
                                &config.diagnostics_dir,
                                &identity,
                                true,
                                "heartbeat_sent",
                                None,
                            );
                        }
                        msg = ws_stream.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(err) = handle_user_channel_text(&config.diagnostics_dir, &mut writer, &text) {
                                        warn!("User WS: failed to persist event(s): {err:#}");
                                    }
                                    record_live_user_inbound_status(
                                        &config.diagnostics_dir,
                                        &identity,
                                        true,
                                        "message_received",
                                        None,
                                    );
                                }
                                Some(Ok(Message::Ping(ping))) => {
                                    let _ = ws_stream.send(Message::Pong(ping)).await;
                                    record_live_user_inbound_status(
                                        &config.diagnostics_dir,
                                        &identity,
                                        true,
                                        "ping_received",
                                        None,
                                    );
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    record_live_user_inbound_status(
                                        &config.diagnostics_dir,
                                        &identity,
                                        true,
                                        "pong_received",
                                        None,
                                    );
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("User WS: socket closed by server");
                                    record_live_user_status(
                                        &config.diagnostics_dir,
                                        &identity,
                                        false,
                                        "socket_closed",
                                        Some("server closed websocket"),
                                    );
                                    break;
                                }
                                Some(Err(err)) => {
                                    warn!("User WS: stream error: {err}");
                                    record_live_user_status(
                                        &config.diagnostics_dir,
                                        &identity,
                                        false,
                                        "stream_error",
                                        Some(&err.to_string()),
                                    );
                                    break;
                                }
                                None => {
                                    warn!("User WS: socket ended");
                                    record_live_user_status(
                                        &config.diagnostics_dir,
                                        &identity,
                                        false,
                                        "socket_ended",
                                        None,
                                    );
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(err) => {
                warn!("User WS: failed to connect: {err}");
                record_live_user_status(
                    &config.diagnostics_dir,
                    &identity,
                    false,
                    "connect_failed",
                    Some(&err.to_string()),
                );
            }
        }

        time::sleep(Duration::from_secs(5)).await;
    }
}

fn record_live_user_status(
    root_dir: &Path,
    identity: &LiveUserChannelIdentity,
    connected: bool,
    stage: &str,
    detail: Option<&str>,
) {
    if let Err(err) = write_live_user_status(
        root_dir,
        Some(identity),
        connected,
        stage,
        detail,
        false,
        None,
    ) {
        warn!("User WS: failed to write status: {err:#}");
    }
}

fn record_live_user_inbound_status(
    root_dir: &Path,
    identity: &LiveUserChannelIdentity,
    connected: bool,
    stage: &str,
    detail: Option<&str>,
) {
    if let Err(err) = write_live_user_status(
        root_dir,
        Some(identity),
        connected,
        stage,
        detail,
        true,
        Some(stage),
    ) {
        warn!("User WS: failed to write status: {err:#}");
    }
}

fn write_live_user_status(
    root_dir: &Path,
    identity: Option<&LiveUserChannelIdentity>,
    connected: bool,
    stage: &str,
    detail: Option<&str>,
    inbound_received: bool,
    inbound_type: Option<&str>,
) -> Result<()> {
    fs::create_dir_all(root_dir)
        .with_context(|| format!("creating diagnostics directory {}", root_dir.display()))?;
    let now = Utc::now().to_rfc3339();
    let previous_status = if connected && !inbound_received {
        read_live_user_status(root_dir)
            .ok()
            .filter(|status| status_identity_matches(identity, status))
    } else {
        None
    };
    let status = LiveUserChannelStatus {
        timestamp: now.clone(),
        connected,
        stage: stage.to_string(),
        detail: detail.map(str::to_string),
        last_inbound_at: if inbound_received {
            Some(now)
        } else if connected {
            previous_status
                .as_ref()
                .and_then(|status| status.last_inbound_at.clone())
        } else {
            None
        },
        account_address: identity.and_then(|identity| identity.account_address.clone()),
        api_key_fingerprint: identity.map(|identity| identity.api_key_fingerprint.clone()),
        process_id: identity.map(|identity| identity.process_id),
        ws_url: identity.map(|identity| identity.ws_url.clone()),
        subscription_mode: identity.map(|identity| identity.subscription_mode.clone()),
        connection_nonce: identity.map(|identity| identity.connection_nonce.clone()),
        last_inbound_type: if inbound_received {
            inbound_type.map(str::to_string)
        } else if connected {
            previous_status
                .as_ref()
                .and_then(|status| status.last_inbound_type.clone())
        } else {
            None
        },
    };
    let path = root_dir.join(LIVE_USER_STATUS_FILE);
    let body = serde_json::to_string_pretty(&status)?;
    fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn live_user_channel_identity(
    config: &Config,
    url: &Url,
    credentials: &UserChannelCredentials,
) -> LiveUserChannelIdentity {
    LiveUserChannelIdentity {
        account_address: configured_live_account_address(config)
            .ok()
            .map(|address| normalize_account_address(&address.to_string())),
        api_key_fingerprint: api_key_fingerprint(&credentials.api_key),
        process_id: std::process::id(),
        ws_url: url.to_string(),
        subscription_mode: "all_markets".to_string(),
        connection_nonce: uuid::Uuid::new_v4().to_string(),
    }
}

fn expected_live_user_channel_identity(config: &Config) -> Result<LiveUserChannelIdentity> {
    let credentials = UserChannelCredentials::from_env()?;
    let url = Url::parse(&config.clob_user_ws_url)
        .with_context(|| format!("invalid CLOB_USER_WS_URL={}", config.clob_user_ws_url))?;
    let account_address = configured_live_account_address(config)
        .context("live user-channel status identity requires live account resolution")?;
    Ok(LiveUserChannelIdentity {
        account_address: Some(normalize_account_address(&account_address.to_string())),
        api_key_fingerprint: api_key_fingerprint(&credentials.api_key),
        process_id: std::process::id(),
        ws_url: url.to_string(),
        subscription_mode: "all_markets".to_string(),
        connection_nonce: String::new(),
    })
}

fn ensure_live_user_channel_status_identity(
    config: &Config,
    status: &LiveUserChannelStatus,
) -> Result<()> {
    let expected = expected_live_user_channel_identity(config)?;
    let account_address =
        required_status_field(status.account_address.as_deref(), "account_address")?;
    if normalize_account_address(account_address) != expected.account_address.unwrap_or_default() {
        bail!("live user-channel status account_address does not match current live account");
    }
    if required_status_field(status.api_key_fingerprint.as_deref(), "api_key_fingerprint")?
        != expected.api_key_fingerprint
    {
        bail!("live user-channel status api_key_fingerprint does not match current API key");
    }
    if required_status_field(status.ws_url.as_deref(), "ws_url")? != expected.ws_url {
        bail!("live user-channel status ws_url does not match current CLOB_USER_WS_URL");
    }
    if required_status_field(status.subscription_mode.as_deref(), "subscription_mode")?
        != expected.subscription_mode
    {
        bail!("live user-channel status subscription_mode is not the required all-market stream");
    }
    let process_id = status
        .process_id
        .ok_or_else(|| anyhow::anyhow!("live user-channel status missing process_id"))?;
    if process_id != expected.process_id {
        bail!(
            "live user-channel status belongs to another process: status_pid={} current_pid={}",
            process_id,
            expected.process_id
        );
    }
    required_status_field(status.connection_nonce.as_deref(), "connection_nonce")?;
    required_status_field(status.last_inbound_type.as_deref(), "last_inbound_type")?;
    Ok(())
}

fn status_identity_matches(
    identity: Option<&LiveUserChannelIdentity>,
    status: &LiveUserChannelStatus,
) -> bool {
    let Some(identity) = identity else {
        return false;
    };
    status
        .account_address
        .as_deref()
        .map(normalize_account_address)
        == identity.account_address.clone()
        && status.api_key_fingerprint.as_deref() == Some(identity.api_key_fingerprint.as_str())
        && status.process_id == Some(identity.process_id)
        && status.ws_url.as_deref() == Some(identity.ws_url.as_str())
        && status.subscription_mode.as_deref() == Some(identity.subscription_mode.as_str())
        && status.connection_nonce.as_deref() == Some(identity.connection_nonce.as_str())
}

fn required_status_field<'a>(value: Option<&'a str>, name: &str) -> Result<&'a str> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("live user-channel status missing {name}"))
}

fn normalize_account_address(address: &str) -> String {
    address.trim().to_ascii_lowercase()
}

fn api_key_fingerprint(api_key: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in api_key.trim().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn read_live_user_status(root_dir: &Path) -> Result<LiveUserChannelStatus> {
    let path = root_dir.join(LIVE_USER_STATUS_FILE);
    let body = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("failed to parse {}", path.display()))
}

fn handle_user_channel_text(
    root_dir: &Path,
    writer: &mut BufWriter<File>,
    text: &str,
) -> Result<()> {
    let parsed = parse_user_channel_payload(text);
    if parsed.heartbeat_messages > 0 {
        debug!("User WS: heartbeat response");
    }
    if parsed.malformed_messages > 0 {
        warn!("User WS: malformed message ignored");
    }
    if parsed.ignored_messages > 0 {
        debug!("User WS: ignored non-order/trade message");
    }
    for event in &parsed.events {
        match (event.event_type.as_str(), event.status_class.as_deref()) {
            ("trade", _) if !user_trade_is_journaled(root_dir, event)? => {
                warn!(
                    "User WS: unjournaled trade trade_id={:?} order_id={:?} condition={:?}",
                    event.trade_id, event.taker_order_id, event.condition_id
                );
                write_live_user_halt(root_dir, event, "unjournaled user trade")?;
            }
            ("trade", Some("failed")) => {
                warn!(
                    "User WS: trade failed trade_id={:?} order_id={:?} condition={:?}",
                    event.trade_id, event.taker_order_id, event.condition_id
                );
                write_live_user_halt(root_dir, event, "user trade failed")?;
            }
            ("trade", Some("pending")) if event.status.as_deref() == Some("RETRYING") => {
                warn!(
                    "User WS: trade retrying trade_id={:?} order_id={:?} condition={:?}",
                    event.trade_id, event.taker_order_id, event.condition_id
                );
                write_live_user_halt(root_dir, event, "user trade retrying")?;
            }
            _ => {}
        }
    }
    write_normalized_user_events(writer, &parsed.events)?;
    writer.flush()?;
    emit_live_user_events(&parsed.events);
    Ok(())
}

fn user_trade_is_journaled(root_dir: &Path, event: &NormalizedUserEvent) -> Result<bool> {
    let journal = read_journal_snapshot(&root_dir.join(LIVE_EXECUTION_JOURNAL_FILE))?;
    let unresolved = unresolved_executions(&journal);
    let order_ids = journal_order_ids(&unresolved);
    let trade_ids = journal_trade_ids(&unresolved);
    let known_order = event
        .order_id
        .as_ref()
        .or(event.taker_order_id.as_ref())
        .map(|order_id| order_ids.contains(order_id))
        .unwrap_or(false);
    let known_trade = event
        .trade_id
        .as_ref()
        .map(|trade_id| trade_ids.contains(trade_id))
        .unwrap_or(false);
    if known_order || known_trade || user_trade_matches_pending_intent(&unresolved, event) {
        return Ok(true);
    }

    let combo_rfq_journal =
        read_combo_rfq_user_journal_snapshot(&root_dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE))?;
    Ok(combo_rfq_user_trade_is_journaled(&combo_rfq_journal, event))
}

fn user_trade_matches_pending_intent(
    unresolved: &[&JournalExecution],
    event: &NormalizedUserEvent,
) -> bool {
    let Some(asset_id) = clean_optional_string(event.asset_id.as_deref()) else {
        return false;
    };
    let event_condition = clean_optional_string(event.condition_id.as_deref());
    let event_side = clean_optional_string(event.side.as_deref())
        .unwrap_or_else(|| "BUY".to_string())
        .to_ascii_uppercase();
    let Some(event_size) = parse_event_f64(event.size.as_deref()) else {
        return false;
    };
    let Some(event_price) = parse_event_f64(event.price.as_deref()) else {
        return false;
    };

    unresolved
        .iter()
        .filter(|execution| execution.stage == "submit_intent")
        .flat_map(|execution| execution.pending_intent_legs.iter())
        .any(|leg| {
            event_condition
                .as_deref()
                .map(|condition_id| condition_id == leg.condition_id)
                .unwrap_or(true)
                && asset_id == leg.token_id
                && event_side == leg.side
                && (event_size - leg.size).abs() <= 0.000001
                && event_price <= leg.limit_price + 0.000001
        })
}

fn parse_event_f64(value: Option<&str>) -> Option<f64> {
    value?.trim().parse::<f64>().ok()
}

fn open_user_events_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    Ok(BufWriter::new(file))
}

fn write_live_user_halt(root_dir: &Path, event: &NormalizedUserEvent, reason: &str) -> Result<()> {
    fs::create_dir_all(root_dir)
        .with_context(|| format!("creating diagnostics directory {}", root_dir.display()))?;
    let path = root_dir.join(LIVE_USER_HALT_FILE);
    let record = LiveUserHaltRecord {
        timestamp: Utc::now().to_rfc3339(),
        reason: reason.to_string(),
        event: event.clone(),
    };
    let body = serde_json::to_string_pretty(&record)?;
    fs::write(&path, body).with_context(|| format!("writing {}", path.display()))
}

fn write_normalized_user_events(
    writer: &mut BufWriter<File>,
    events: &[NormalizedUserEvent],
) -> Result<()> {
    for event in events {
        serde_json::to_writer(&mut *writer, event)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

fn normalize_user_event(raw: &Value) -> Option<NormalizedUserEvent> {
    let event_type = text_field(raw, &["event_type"]).or_else(|| {
        let typ = text_field(raw, &["type"])?;
        if typ.eq_ignore_ascii_case("TRADE") {
            Some("trade".to_string())
        } else {
            None
        }
    })?;

    match event_type.to_ascii_lowercase().as_str() {
        "order" => normalize_order_event(raw),
        "trade" => normalize_trade_event(raw),
        _ => None,
    }
}

fn normalize_order_event(raw: &Value) -> Option<NormalizedUserEvent> {
    let order_kind = text_field(raw, &["type"]).unwrap_or_else(|| "UPDATE".to_string());
    let stage = match order_kind.trim().to_ascii_uppercase().as_str() {
        "PLACEMENT" => "user_order_placement",
        "CANCELLATION" | "CANCELLED" | "CANCELED" => "user_order_cancellation",
        _ => "user_order_update",
    };
    Some(NormalizedUserEvent {
        event_type: "order".to_string(),
        stage: stage.to_string(),
        order_id: text_field(raw, &["id", "order_id"]),
        trade_id: None,
        taker_order_id: None,
        maker_order_id: text_field(raw, &["maker_order_id", "makerOrderId"]),
        transaction_hash: text_field(
            raw,
            &["transactionHash", "transaction_hash", "txHash", "tx_hash"],
        ),
        rfq_id: text_field(raw, &["rfqId", "rfq_id"]),
        quote_id: text_field(raw, &["quoteId", "quote_id"]),
        client_request_id: text_field(raw, &["clientRequestId", "client_request_id"]),
        condition_id: text_field(raw, &["market", "condition_id"]),
        asset_id: text_field(raw, &["asset_id"]),
        side: text_field(raw, &["side"]).map(|side| side.to_ascii_uppercase()),
        size: text_field(raw, &["size_matched", "original_size", "size"]),
        price: text_field(raw, &["price"]),
        status: text_field(raw, &["status"]),
        status_class: None,
        timestamp: text_field(raw, &["timestamp", "created_at", "last_update"]),
        raw: raw.clone(),
    })
}

fn normalize_trade_event(raw: &Value) -> Option<NormalizedUserEvent> {
    let status = text_field(raw, &["status"]).map(|status| normalize_trade_status(&status));
    let stage = status
        .as_deref()
        .map(trade_stage)
        .unwrap_or("user_trade_unknown");
    Some(NormalizedUserEvent {
        event_type: "trade".to_string(),
        stage: stage.to_string(),
        order_id: None,
        trade_id: text_field(raw, &["id", "trade_id"]),
        taker_order_id: text_field(raw, &["taker_order_id", "order_id"]),
        maker_order_id: text_field(raw, &["maker_order_id", "makerOrderId"]),
        transaction_hash: text_field(
            raw,
            &["transactionHash", "transaction_hash", "txHash", "tx_hash"],
        ),
        rfq_id: text_field(raw, &["rfqId", "rfq_id"]),
        quote_id: text_field(raw, &["quoteId", "quote_id"]),
        client_request_id: text_field(raw, &["clientRequestId", "client_request_id"]),
        condition_id: text_field(raw, &["market", "condition_id"]),
        asset_id: text_field(raw, &["asset_id"]),
        side: text_field(raw, &["side"]).map(|side| side.to_ascii_uppercase()),
        size: text_field(raw, &["size", "matched_amount"]),
        price: text_field(raw, &["price"]),
        status_class: status
            .as_deref()
            .map(trade_status_class)
            .map(str::to_string),
        status,
        timestamp: text_field(
            raw,
            &[
                "timestamp",
                "matchtime",
                "match_time",
                "last_update",
                "created_at",
            ],
        ),
        raw: raw.clone(),
    })
}

fn normalize_trade_status(status: &str) -> String {
    let mut normalized = status.trim().to_ascii_uppercase();
    if let Some(stripped) = normalized.strip_prefix("TRADE_STATUS_") {
        normalized = stripped.to_string();
    }
    normalized
}

fn trade_stage(status: &str) -> &'static str {
    match status {
        "CONFIRMED" => "user_trade_confirmed",
        "FAILED" => "user_trade_failed",
        "RETRYING" => "user_trade_retrying",
        "MINED" => "user_trade_mined",
        "MATCHED" | "MATCHED_NOT_BROADCASTED" => "user_trade_matched",
        _ => "user_trade_unknown",
    }
}

fn trade_status_class(status: &str) -> &'static str {
    match status {
        "CONFIRMED" => "confirmed",
        "FAILED" => "failed",
        "MATCHED" | "MATCHED_NOT_BROADCASTED" | "MINED" | "RETRYING" => "pending",
        _ => "unknown",
    }
}

fn text_field(value: &Value, keys: &[&str]) -> Option<String> {
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

fn read_journal_snapshot(path: &Path) -> Result<JournalSnapshot> {
    if !path.exists() {
        return Ok(JournalSnapshot::default());
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading live execution journal {}", path.display()))?;
    let mut snapshot = JournalSnapshot::default();
    for (idx, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: JournalLine = serde_json::from_str(line).with_context(|| {
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
        let entry = snapshot.executions.entry(execution_id).or_default();
        fill_optional_string(&mut entry.event_id, parsed.event_id);
        fill_optional_string(&mut entry.event_title, parsed.event_title);
        fill_optional_string(&mut entry.arb_type, parsed.arb_type);
        fill_optional_f64(&mut entry.position_usd, parsed.position_usd);
        fill_optional_f64(&mut entry.projected_pnl_usd, parsed.projected_pnl_usd);
        fill_optional_f64(
            &mut entry.actual_entry_cost_usd,
            parsed.actual_entry_cost_usd,
        );
        entry.stage = stage.clone();
        entry.pending_intent_legs.clear();
        entry.order_ids.extend(clean_set(parsed.order_ids));
        entry
            .expected_order_hashes
            .extend(clean_set(parsed.expected_order_hashes));
        entry.trade_ids.extend(clean_set(parsed.trade_ids));
        if let Some(legs) = parsed.legs {
            for leg in legs {
                if let Some(condition_id) = clean_optional_string(leg.condition_id.as_deref()) {
                    entry.condition_ids.insert(condition_id.clone());
                    if stage == "submit_intent" {
                        if let (Some(token_id), Some(size), Some(limit_price)) = (
                            clean_optional_string(leg.token_id.as_deref()),
                            leg.size,
                            leg.limit_price,
                        ) {
                            entry.pending_intent_legs.push(JournalPendingIntentLeg {
                                condition_id,
                                token_id,
                                side: clean_optional_string(leg.side.as_deref())
                                    .unwrap_or_else(|| "BUY".to_string())
                                    .to_ascii_uppercase(),
                                size,
                                limit_price,
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(snapshot)
}

fn read_combo_rfq_user_journal_snapshot(path: &Path) -> Result<ComboRfqUserJournalSnapshot> {
    if !path.exists() {
        return Ok(ComboRfqUserJournalSnapshot::default());
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading Combo/RFQ execution journal {}", path.display()))?;
    let mut snapshot = ComboRfqUserJournalSnapshot::default();
    for (idx, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: ComboRfqUserJournalRecord = serde_json::from_str(line).with_context(|| {
            format!(
                "Combo/RFQ execution journal {} has malformed JSON at line {}",
                path.display(),
                idx + 1
            )
        })?;
        snapshot.records.push(record);
    }
    Ok(snapshot)
}

fn combo_rfq_user_trade_is_journaled(
    journal: &ComboRfqUserJournalSnapshot,
    event: &NormalizedUserEvent,
) -> bool {
    journal
        .records
        .iter()
        .rev()
        .filter(|record| combo_rfq_journal_status_can_match_user_trade(record.status.as_deref()))
        .any(|record| combo_rfq_journal_record_matches_user_trade(record, event))
}

fn combo_rfq_journal_status_can_match_user_trade(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("accept_intent")
            | Some("accept_state_unknown")
            | Some("accepted_pending_finality")
            | Some("finality_confirmed_exposure_retained")
    )
}

fn combo_rfq_journal_record_matches_user_trade(
    record: &ComboRfqUserJournalRecord,
    event: &NormalizedUserEvent,
) -> bool {
    (combo_rfq_record_matches_hash(record, event)
        || combo_rfq_record_matches_metadata(record, event))
        && combo_rfq_record_economics_match(record, event)
}

fn combo_rfq_record_matches_hash(
    record: &ComboRfqUserJournalRecord,
    event: &NormalizedUserEvent,
) -> bool {
    let event_order_ids = [
        event.order_id.as_deref(),
        event.taker_order_id.as_deref(),
        event.maker_order_id.as_deref(),
    ];
    let event_transaction_hash = event.transaction_hash.as_deref();
    let response = record.response.as_ref();
    let response_transaction_hash = value_text(
        response,
        &["transactionHash", "transaction_hash", "txHash", "tx_hash"],
    );
    let response_order_hash = value_text(response, &["orderHash", "order_hash"]);

    any_equal_canonical_text(
        response_transaction_hash.as_deref(),
        &[event_transaction_hash],
    ) || any_equal_canonical_text(response_order_hash.as_deref(), &event_order_ids)
}

fn combo_rfq_record_matches_metadata(
    record: &ComboRfqUserJournalRecord,
    event: &NormalizedUserEvent,
) -> bool {
    let selected_quote_rfq_id = value_text(record.selected_quote.as_ref(), &["rfqId", "rfq_id"]);
    let selected_quote_quote_id =
        value_text(record.selected_quote.as_ref(), &["quoteId", "quote_id"]);
    let client_request_matches = any_equal_trimmed(
        record.client_request_id.as_deref(),
        &[event.client_request_id.as_deref()],
    );
    let rfq_matches = any_equal_trimmed(record.rfq_id.as_deref(), &[event.rfq_id.as_deref()])
        || any_equal_trimmed(selected_quote_rfq_id.as_deref(), &[event.rfq_id.as_deref()]);
    let quote_matches = any_equal_trimmed(record.quote_id.as_deref(), &[event.quote_id.as_deref()])
        || any_equal_trimmed(
            selected_quote_quote_id.as_deref(),
            &[event.quote_id.as_deref()],
        );
    client_request_matches || (rfq_matches && quote_matches)
}

fn combo_rfq_record_economics_match(
    record: &ComboRfqUserJournalRecord,
    event: &NormalizedUserEvent,
) -> bool {
    combo_rfq_record_economics_overlap(record, event)
        && optional_side_equal(
            combo_rfq_record_side(record).as_deref(),
            event.side.as_deref(),
        )
        && optional_decimal_equal(combo_rfq_record_qty(record), event.size.as_deref())
        && optional_decimal_equal(combo_rfq_record_price(record), event.price.as_deref())
}

fn combo_rfq_record_economics_overlap(
    record: &ComboRfqUserJournalRecord,
    event: &NormalizedUserEvent,
) -> bool {
    (combo_rfq_record_side(record).is_some()
        && clean_optional_string(event.side.as_deref()).is_some())
        || (combo_rfq_record_qty(record).is_some()
            && clean_optional_string(event.size.as_deref()).is_some())
        || (combo_rfq_record_price(record).is_some()
            && clean_optional_string(event.price.as_deref()).is_some())
}

fn combo_rfq_record_side(record: &ComboRfqUserJournalRecord) -> Option<String> {
    value_text(record.accept_request.as_ref(), &["side"])
        .or_else(|| value_text(record.selected_quote.as_ref(), &["side"]))
}

fn combo_rfq_record_qty(record: &ComboRfqUserJournalRecord) -> Option<f64> {
    value_text(
        record.accept_request.as_ref(),
        &["qtyDecimal", "qty_decimal"],
    )
    .and_then(|value| parse_event_f64(Some(&value)))
    .or_else(|| {
        value_text(
            record.selected_quote.as_ref(),
            &["qtyDecimal", "qty_decimal"],
        )
        .and_then(|value| parse_event_f64(Some(&value)))
    })
}

fn combo_rfq_record_price(record: &ComboRfqUserJournalRecord) -> Option<f64> {
    value_text(record.accept_request.as_ref(), &["price"])
        .and_then(|value| parse_event_f64(Some(&value)))
        .or_else(|| {
            value_text(record.selected_quote.as_ref(), &["price"])
                .and_then(|value| parse_event_f64(Some(&value)))
        })
}

fn value_text(value: Option<&Value>, keys: &[&str]) -> Option<String> {
    text_field(value?, keys)
}

fn optional_side_equal(left: Option<&str>, right: Option<&str>) -> bool {
    match (clean_optional_string(left), clean_optional_string(right)) {
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

fn any_equal_trimmed(left: Option<&str>, rights: &[Option<&str>]) -> bool {
    let Some(left) = clean_optional_string(left) else {
        return false;
    };
    rights
        .iter()
        .filter_map(|right| clean_optional_string(*right))
        .any(|right| right == left)
}

fn any_equal_canonical_text(left: Option<&str>, rights: &[Option<&str>]) -> bool {
    let Some(left) = clean_optional_string(left).map(|value| canonical_match_text(&value)) else {
        return false;
    };
    rights
        .iter()
        .filter_map(|right| clean_optional_string(*right))
        .map(|right| canonical_match_text(&right))
        .any(|right| right == left)
}

fn canonical_match_text(value: &str) -> String {
    value.trim().trim_start_matches("0x").to_ascii_lowercase()
}

fn derive_live_route_replay_records(
    journal: &JournalSnapshot,
    events: &[NormalizedUserEvent],
) -> Vec<LiveRouteReplayRecord> {
    let mut records = Vec::new();
    let generated_at = Utc::now().to_rfc3339();
    let mut execution_ids: Vec<&String> = journal.executions.keys().collect();
    execution_ids.sort();

    for execution_id in execution_ids {
        let Some(execution) = journal.executions.get(execution_id) else {
            continue;
        };
        let matching_events: Vec<&NormalizedUserEvent> = events
            .iter()
            .filter(|event| event.event_type == "trade")
            .filter(|event| trade_event_matches_execution(execution, event))
            .collect();
        if matching_events.is_empty() {
            continue;
        }
        let failed_count = matching_events
            .iter()
            .filter(|event| event.status_class.as_deref() == Some("failed"))
            .count();
        let confirmed_count = matching_events
            .iter()
            .filter(|event| event.status_class.as_deref() == Some("confirmed"))
            .map(|event| {
                event
                    .trade_id
                    .as_deref()
                    .or(event.asset_id.as_deref())
                    .or(event.taker_order_id.as_deref())
                    .unwrap_or("<unknown>")
                    .to_string()
            })
            .collect::<HashSet<_>>()
            .len();
        let planned_legs = execution_planned_leg_count(execution);
        let outcome_label = if failed_count > 0 {
            "matched_then_failed"
        } else if confirmed_count >= planned_legs {
            "both_confirmed"
        } else if confirmed_count > 0 {
            "one_leg_confirmed"
        } else {
            continue;
        };
        let route = route_for_replay_execution(execution.arb_type.as_deref()).to_string();
        records.push(LiveRouteReplayRecord {
            label_id: Some(format!(
                "user_channel:{execution_id}:{route}:{outcome_label}"
            )),
            generated_at: generated_at.clone(),
            event_id: execution
                .event_id
                .clone()
                .unwrap_or_else(|| execution_id.clone()),
            route,
            outcome_label: outcome_label.to_string(),
            realized_ev_usd: None,
            toxicity_score: None,
            notes: vec![
                format!("source=user_channel"),
                format!("execution_id={execution_id}"),
                format!("planned_legs={planned_legs}"),
                format!("confirmed_trades={confirmed_count}"),
                format!("failed_trades={failed_count}"),
            ],
        });
    }

    records
}

fn trade_event_matches_execution(
    execution: &JournalExecution,
    event: &NormalizedUserEvent,
) -> bool {
    event
        .trade_id
        .as_ref()
        .map(|trade_id| execution.trade_ids.contains(trade_id))
        .unwrap_or(false)
        || event
            .taker_order_id
            .as_ref()
            .or(event.order_id.as_ref())
            .map(|order_id| execution.order_ids.contains(order_id))
            .unwrap_or(false)
        || trade_event_matches_pending_intent(execution, event)
}

fn live_user_channel_fill_status_from_events(
    events: &[NormalizedUserEvent],
    order_ids: &[String],
    trade_ids: &[String],
) -> LiveUserChannelFillStatus {
    let order_ids = clean_id_set(order_ids);
    let trade_ids = clean_id_set(trade_ids);
    let mut status = LiveUserChannelFillStatus::default();
    for event in events.iter().filter(|event| event.event_type == "trade") {
        let matched_order_ids = matched_user_event_order_ids(event, &order_ids);
        let matched_trade_ids = matched_user_event_trade_ids(event, &trade_ids);
        if matched_order_ids.is_empty() && matched_trade_ids.is_empty() {
            continue;
        }
        match event.status_class.as_deref() {
            Some("confirmed") => {
                extend_unique(&mut status.confirmed_order_ids, matched_order_ids);
                extend_unique(&mut status.confirmed_trade_ids, matched_trade_ids);
                if let Some(hash) = clean_optional_string(event.transaction_hash.as_deref()) {
                    push_unique(&mut status.transaction_hashes, hash);
                }
            }
            Some("failed") => {
                extend_unique(&mut status.failed_order_ids, matched_order_ids);
                extend_unique(&mut status.failed_trade_ids, matched_trade_ids);
            }
            Some("pending") => {
                extend_unique(&mut status.pending_order_ids, matched_order_ids);
                extend_unique(&mut status.pending_trade_ids, matched_trade_ids);
            }
            _ => {}
        }
    }
    status
}

fn clean_id_set(values: &[String]) -> HashSet<String> {
    values
        .iter()
        .filter_map(|value| clean_optional_string(Some(value)))
        .collect()
}

fn matched_user_event_order_ids(
    event: &NormalizedUserEvent,
    order_ids: &HashSet<String>,
) -> Vec<String> {
    [
        event.order_id.as_ref(),
        event.taker_order_id.as_ref(),
        event.maker_order_id.as_ref(),
    ]
    .into_iter()
    .flatten()
    .filter_map(|value| clean_optional_string(Some(value)))
    .filter(|value| order_ids.contains(value))
    .collect()
}

fn matched_user_event_trade_ids(
    event: &NormalizedUserEvent,
    trade_ids: &HashSet<String>,
) -> Vec<String> {
    event
        .trade_id
        .as_ref()
        .and_then(|value| clean_optional_string(Some(value)))
        .filter(|value| trade_ids.contains(value))
        .into_iter()
        .collect()
}

fn extend_unique(target: &mut Vec<String>, values: Vec<String>) {
    for value in values {
        push_unique(target, value);
    }
}

fn push_unique(target: &mut Vec<String>, value: String) {
    if !target.contains(&value) {
        target.push(value);
    }
}

fn trade_event_matches_pending_intent(
    execution: &JournalExecution,
    event: &NormalizedUserEvent,
) -> bool {
    let Some(asset_id) = clean_optional_string(event.asset_id.as_deref()) else {
        return false;
    };
    let event_condition = clean_optional_string(event.condition_id.as_deref());
    let event_side = clean_optional_string(event.side.as_deref())
        .unwrap_or_else(|| "BUY".to_string())
        .to_ascii_uppercase();
    let Some(event_size) = parse_event_f64(event.size.as_deref()) else {
        return false;
    };
    let Some(event_price) = parse_event_f64(event.price.as_deref()) else {
        return false;
    };

    execution.pending_intent_legs.iter().any(|leg| {
        event_condition
            .as_deref()
            .map(|condition_id| condition_id == leg.condition_id)
            .unwrap_or(true)
            && asset_id == leg.token_id
            && event_side == leg.side
            && (event_size - leg.size).abs() <= 0.000001
            && event_price <= leg.limit_price + 0.000001
    })
}

fn execution_planned_leg_count(execution: &JournalExecution) -> usize {
    execution
        .condition_ids
        .len()
        .max(execution.pending_intent_legs.len())
        .max(execution.order_ids.len())
        .max(1)
}

fn route_for_replay_execution(arb_type: Option<&str>) -> &'static str {
    let normalized = arb_type.unwrap_or_default().to_ascii_lowercase();
    if normalized.contains("bundle") {
        CTF_MERGE_BUNDLE_SHADOW_ROUTE
    } else if normalized.contains("yes") || normalized.contains("no") {
        "yes_no_full_family_clob"
    } else {
        "clob_live_route"
    }
}

fn read_user_event_file(path: &Path) -> Result<(Vec<NormalizedUserEvent>, usize, usize, usize)> {
    if !path.exists() {
        return Ok((Vec::new(), 0, 0, 0));
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading live user events {}", path.display()))?;
    let mut events = Vec::new();
    let mut malformed = 0;
    let mut ignored = 0;
    let mut heartbeats = 0;
    for line in contents.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed = parse_user_channel_payload(line);
        malformed += parsed.malformed_messages;
        ignored += parsed.ignored_messages;
        heartbeats += parsed.heartbeat_messages;
        events.extend(parsed.events);
    }
    Ok((events, malformed, ignored, heartbeats))
}

fn unresolved_executions(snapshot: &JournalSnapshot) -> Vec<&JournalExecution> {
    snapshot
        .executions
        .values()
        .filter(|execution| !journal_stage_is_reconciled(&execution.stage))
        .collect()
}

fn journal_stage_is_reconciled(stage: &str) -> bool {
    matches!(
        stage,
        "pre_submit_released" | "submit_rejected_released" | "manual_reconciled"
    )
}

fn journal_order_ids(executions: &[&JournalExecution]) -> HashSet<String> {
    executions
        .iter()
        .flat_map(|execution| execution.order_ids.iter().cloned())
        .collect()
}

fn journal_expected_order_hashes(executions: &[&JournalExecution]) -> HashSet<String> {
    executions
        .iter()
        .flat_map(|execution| execution.expected_order_hashes.iter().cloned())
        .collect()
}

fn journal_trade_ids(executions: &[&JournalExecution]) -> HashSet<String> {
    executions
        .iter()
        .flat_map(|execution| execution.trade_ids.iter().cloned())
        .collect()
}

fn journal_condition_ids(executions: &[&JournalExecution]) -> HashSet<String> {
    executions
        .iter()
        .flat_map(|execution| execution.condition_ids.iter().cloned())
        .collect()
}

fn fill_optional_string(target: &mut Option<String>, value: Option<String>) {
    if let Some(value) = value.and_then(|value| clean_optional_string(Some(&value))) {
        *target = Some(value);
    }
}

fn fill_optional_f64(target: &mut Option<f64>, value: Option<f64>) {
    if let Some(value) = value.filter(|value| value.is_finite()) {
        *target = Some(value);
    }
}

fn clean_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn clean_set(values: Option<Vec<String>>) -> Vec<String> {
    values
        .unwrap_or_default()
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

fn reconcile_user_events(
    events: &[NormalizedUserEvent],
    journal_order_ids: &HashSet<String>,
    journal_trade_ids: &HashSet<String>,
    journal_condition_ids: &HashSet<String>,
    combo_rfq_journal: &ComboRfqUserJournalSnapshot,
) -> Vec<UserReconcileAlert> {
    let mut alerts = Vec::new();
    for event in events {
        let known_order = event
            .order_id
            .as_ref()
            .or(event.taker_order_id.as_ref())
            .map(|order_id| journal_order_ids.contains(order_id))
            .unwrap_or(false);
        let known_trade = event
            .trade_id
            .as_ref()
            .map(|trade_id| journal_trade_ids.contains(trade_id))
            .unwrap_or(false);
        let watched_condition = event
            .condition_id
            .as_ref()
            .map(|condition_id| journal_condition_ids.contains(condition_id))
            .unwrap_or(false);
        let known_combo_rfq_trade = event.event_type == "trade"
            && combo_rfq_user_trade_is_journaled(combo_rfq_journal, event);

        if !known_order && !known_trade && !watched_condition && !known_combo_rfq_trade {
            alerts.push(UserReconcileAlert {
                severity: if event.event_type == "trade" {
                    "critical".to_string()
                } else {
                    "warning".to_string()
                },
                kind: format!("unjournaled_user_{}", event.event_type),
                order_id: event.order_id.clone().or(event.taker_order_id.clone()),
                trade_id: event.trade_id.clone(),
                condition_id: event.condition_id.clone(),
                status: event.status.clone(),
                reason: "user-channel event is not tied to unresolved live journal state"
                    .to_string(),
            });
        }

        if event.event_type == "order" && event.stage == "user_order_cancellation" && known_order {
            alerts.push(UserReconcileAlert {
                severity: "warning".to_string(),
                kind: "journaled_order_cancelled".to_string(),
                order_id: event.order_id.clone(),
                trade_id: None,
                condition_id: event.condition_id.clone(),
                status: event.status.clone(),
                reason: "journaled live order was cancelled on user channel".to_string(),
            });
        }

        match event.status_class.as_deref() {
            Some("failed") => alerts.push(UserReconcileAlert {
                severity: "critical".to_string(),
                kind: "user_trade_failed".to_string(),
                order_id: event.taker_order_id.clone(),
                trade_id: event.trade_id.clone(),
                condition_id: event.condition_id.clone(),
                status: event.status.clone(),
                reason: "trade reached terminal failed state".to_string(),
            }),
            Some("pending") if event.status.as_deref() == Some("RETRYING") => {
                alerts.push(UserReconcileAlert {
                    severity: "warning".to_string(),
                    kind: "user_trade_retrying".to_string(),
                    order_id: event.taker_order_id.clone(),
                    trade_id: event.trade_id.clone(),
                    condition_id: event.condition_id.clone(),
                    status: event.status.clone(),
                    reason: "trade settlement is retrying and exposure remains uncertain"
                        .to_string(),
                });
            }
            Some("unknown") => alerts.push(UserReconcileAlert {
                severity: "warning".to_string(),
                kind: "user_trade_unknown_status".to_string(),
                order_id: event.taker_order_id.clone(),
                trade_id: event.trade_id.clone(),
                condition_id: event.condition_id.clone(),
                status: event.status.clone(),
                reason: "trade status is not in known terminal/pending set".to_string(),
            }),
            _ => {}
        }
    }
    alerts
}

fn clob_api_credentials_present() -> bool {
    [
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE",
    ]
    .iter()
    .all(|name| {
        std::env::var(name)
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    })
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("{name} is required for user WS"))?;
    let value = value.trim().to_string();
    if value.is_empty() {
        bail!("{name} is required for user WS");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    const TEST_API_KEY: &str = "unit-test-api-key";
    const TEST_FUNDER_ADDRESS: &str = "0x0000000000000000000000000000000000000001";
    const TEST_PRIVATE_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_USER_WS_URL: &str = "wss://example.test/ws/user";

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "polymarket-arb-scanner-user-channel-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn status_guard_config(dir: &Path) -> Config {
        std::env::set_var("POLYMARKET_API_KEY", TEST_API_KEY);
        std::env::set_var("POLYMARKET_API_SECRET", "unit-test-api-secret");
        std::env::set_var("POLYMARKET_API_PASSPHRASE", "unit-test-api-passphrase");
        std::env::set_var("POLYMARKET_PRIVATE_KEY", TEST_PRIVATE_KEY);

        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.to_path_buf();
        cfg.live_user_ws_enabled = true;
        cfg.clob_user_ws_url = TEST_USER_WS_URL.to_string();
        cfg.live_signature_type = 1;
        cfg.live_funder_address = TEST_FUNDER_ADDRESS.to_string();
        cfg
    }

    fn status_guard_identity(cfg: &Config) -> LiveUserChannelIdentity {
        let credentials = UserChannelCredentials::from_env().unwrap();
        let url = Url::parse(&cfg.clob_user_ws_url).unwrap();
        live_user_channel_identity(cfg, &url, &credentials)
    }

    #[test]
    fn user_subscription_frame_omits_markets_for_all_markets() {
        let frame = build_user_subscription_frame("key", "secret", "pass", &[]);
        assert_eq!(frame["type"], "user");
        assert_eq!(frame["auth"]["apiKey"], "key");
        assert!(frame.get("markets").is_none());
        assert_eq!(heartbeat_payload(), "PING");
    }

    #[test]
    fn user_subscription_frames_support_filtered_updates() {
        let markets = vec!["0xcondition".to_string()];
        let frame = build_user_subscription_frame("key", "secret", "pass", &markets);
        assert_eq!(frame["markets"], json!(["0xcondition"]));
        let update = build_user_subscription_update("subscribe", &markets);
        assert_eq!(
            update,
            json!({"operation": "subscribe", "markets": ["0xcondition"]})
        );
    }

    #[test]
    fn live_user_channel_config_guard_requires_enabled_flag() {
        let mut cfg = Config::from_env();
        cfg.live_user_ws_enabled = false;

        let err = ensure_live_user_channel_configured(&cfg).unwrap_err();

        assert!(err.to_string().contains("LIVE_USER_WS_ENABLED=true"));
    }

    #[test]
    fn live_user_channel_status_guard_requires_connected_status() {
        let dir = temp_dir("status-disconnected");
        let cfg = status_guard_config(&dir);
        write_live_user_status(
            &dir,
            None,
            false,
            "connect_failed",
            Some("dial failed"),
            false,
            None,
        )
        .unwrap();

        let err = ensure_live_user_channel_status_fresh(&cfg).unwrap_err();

        assert!(err.to_string().contains("not connected"));
        assert!(err.to_string().contains("connect_failed"));
    }

    #[test]
    fn live_user_channel_status_guard_accepts_fresh_connected_status() {
        let dir = temp_dir("status-connected");
        let cfg = status_guard_config(&dir);
        let identity = status_guard_identity(&cfg);
        write_live_user_status(
            &dir,
            Some(&identity),
            true,
            "message_received",
            None,
            true,
            Some("message_received"),
        )
        .unwrap();

        ensure_live_user_channel_status_fresh(&cfg).unwrap();
    }

    #[tokio::test]
    async fn live_user_channel_ready_wait_accepts_existing_ready_status() {
        let dir = temp_dir("ready-wait-ok");
        let cfg = status_guard_config(&dir);
        let identity = status_guard_identity(&cfg);
        write_live_user_status(
            &dir,
            Some(&identity),
            true,
            "message_received",
            None,
            true,
            Some("message_received"),
        )
        .unwrap();

        wait_for_live_user_channel_ready_with_timeout(&cfg, Duration::from_millis(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn live_user_channel_ready_wait_times_out_on_disconnected_status() {
        let dir = temp_dir("ready-wait-timeout");
        let cfg = status_guard_config(&dir);
        write_live_user_status(
            &dir,
            None,
            false,
            "connect_failed",
            Some("dial failed"),
            false,
            None,
        )
        .unwrap();

        let err = wait_for_live_user_channel_ready_with_timeout(&cfg, Duration::from_millis(1))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("did not become ready"));
        assert!(err.to_string().contains("connect_failed"));
    }

    #[test]
    fn live_user_channel_status_guard_rejects_heartbeat_only_status() {
        let dir = temp_dir("status-heartbeat-only");
        let cfg = status_guard_config(&dir);
        let identity = status_guard_identity(&cfg);
        write_live_user_status(
            &dir,
            Some(&identity),
            true,
            "heartbeat_sent",
            None,
            false,
            None,
        )
        .unwrap();

        let err = ensure_live_user_channel_status_fresh(&cfg).unwrap_err();

        assert!(err.to_string().contains("no inbound activity"));
    }

    #[test]
    fn live_user_channel_status_guard_rejects_identity_mismatch() {
        let dir = temp_dir("status-identity-mismatch");
        let cfg = status_guard_config(&dir);
        let mut identity = status_guard_identity(&cfg);
        identity.account_address = Some("0x0000000000000000000000000000000000000002".to_string());
        write_live_user_status(
            &dir,
            Some(&identity),
            true,
            "message_received",
            None,
            true,
            Some("message_received"),
        )
        .unwrap();

        let err = ensure_live_user_channel_status_fresh(&cfg).unwrap_err();

        assert!(err.to_string().contains("account_address"));
    }

    #[test]
    fn live_user_channel_status_guard_rejects_stale_connected_status() {
        let dir = temp_dir("status-stale");
        let cfg = status_guard_config(&dir);
        let identity = status_guard_identity(&cfg);
        let status = LiveUserChannelStatus {
            timestamp: (Utc::now() - chrono::Duration::seconds(LIVE_USER_STATUS_MAX_AGE_SECS + 1))
                .to_rfc3339(),
            connected: true,
            stage: "heartbeat_sent".into(),
            detail: None,
            last_inbound_at: Some(
                (Utc::now() - chrono::Duration::seconds(LIVE_USER_STATUS_MAX_AGE_SECS + 1))
                    .to_rfc3339(),
            ),
            account_address: identity.account_address.clone(),
            api_key_fingerprint: Some(identity.api_key_fingerprint.clone()),
            process_id: Some(identity.process_id),
            ws_url: Some(identity.ws_url.clone()),
            subscription_mode: Some(identity.subscription_mode.clone()),
            connection_nonce: Some(identity.connection_nonce.clone()),
            last_inbound_type: Some("message_received".to_string()),
        };
        fs::write(
            dir.join(LIVE_USER_STATUS_FILE),
            serde_json::to_string_pretty(&status).unwrap(),
        )
        .unwrap();

        let err = ensure_live_user_channel_status_fresh(&cfg).unwrap_err();

        assert!(err.to_string().contains("status is stale"));
    }

    #[test]
    fn parser_accepts_order_and_trade_samples() {
        let payload = r#"[
            {
                "event_type":"order",
                "id":"order-1",
                "market":"cond-1",
                "asset_id":"asset-1",
                "side":"BUY",
                "type":"PLACEMENT",
                "original_size":"10",
                "size_matched":"0",
                "price":"0.42",
                "timestamp":"1710000000"
            },
            {
                "event_type":"trade",
                "type":"TRADE",
                "id":"trade-1",
                "taker_order_id":"order-1",
                "market":"cond-1",
                "asset_id":"asset-1",
                "side":"BUY",
                "size":"10",
                "price":"0.42",
                "rfqId":"rfq-1",
                "quoteId":"quote-1",
                "clientRequestId":"client-1",
                "transactionHash":"0xabc",
                "status":"TRADE_STATUS_CONFIRMED",
                "match_time":"1710000001"
            }
        ]"#;
        let parsed = parse_user_channel_payload(payload);
        assert_eq!(parsed.events.len(), 2);
        assert_eq!(parsed.events[0].stage, "user_order_placement");
        assert_eq!(parsed.events[1].stage, "user_trade_confirmed");
        assert_eq!(parsed.events[1].status.as_deref(), Some("CONFIRMED"));
        assert_eq!(parsed.events[1].status_class.as_deref(), Some("confirmed"));
        assert_eq!(parsed.events[1].timestamp.as_deref(), Some("1710000001"));
        assert_eq!(parsed.events[1].rfq_id.as_deref(), Some("rfq-1"));
        assert_eq!(parsed.events[1].quote_id.as_deref(), Some("quote-1"));
        assert_eq!(
            parsed.events[1].client_request_id.as_deref(),
            Some("client-1")
        );
        assert_eq!(parsed.events[1].transaction_hash.as_deref(), Some("0xabc"));
    }

    #[test]
    fn parser_treats_retrying_as_pending_and_failed_as_terminal() {
        let retrying = parse_user_channel_payload(
            r#"{"event_type":"trade","id":"t1","taker_order_id":"o1","status":"RETRYING"}"#,
        );
        assert_eq!(retrying.events[0].stage, "user_trade_retrying");
        assert_eq!(retrying.events[0].status_class.as_deref(), Some("pending"));

        let failed = parse_user_channel_payload(
            r#"{"event_type":"trade","id":"t2","taker_order_id":"o2","status":"FAILED"}"#,
        );
        assert_eq!(failed.events[0].stage, "user_trade_failed");
        assert_eq!(failed.events[0].status_class.as_deref(), Some("failed"));
    }

    #[test]
    fn appends_normalized_user_events_jsonl() {
        let dir = temp_dir("append");
        let written = append_live_user_events_from_payload(
            &dir,
            r#"{"event_type":"trade","id":"trade-1","taker_order_id":"order-1","status":"MATCHED"}"#,
        )
        .unwrap();
        assert_eq!(written, 1);
        let body = fs::read_to_string(dir.join(LIVE_USER_EVENTS_FILE)).unwrap();
        assert!(body.contains(r#""stage":"user_trade_matched""#));
    }

    #[test]
    fn fill_status_matches_confirmed_and_failed_user_events() {
        let dir = temp_dir("fill-status");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(LIVE_USER_EVENTS_FILE),
            concat!(
                r#"{"event_type":"trade","id":"trade-ok","taker_order_id":"order-ok","transaction_hash":"0xabc","status":"CONFIRMED"}"#,
                "\n",
                r#"{"event_type":"trade","id":"trade-failed","taker_order_id":"order-failed","status":"FAILED"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;

        let status = live_user_channel_fill_status(
            &cfg,
            &["order-ok".into(), "order-failed".into()],
            &["trade-ok".into(), "trade-failed".into()],
        )
        .unwrap();

        assert_eq!(status.confirmed_order_ids, vec!["order-ok".to_string()]);
        assert_eq!(status.confirmed_trade_ids, vec!["trade-ok".to_string()]);
        assert_eq!(status.failed_order_ids, vec!["order-failed".to_string()]);
        assert_eq!(status.failed_trade_ids, vec!["trade-failed".to_string()]);
        assert_eq!(status.transaction_hashes, vec!["0xabc".to_string()]);
    }

    #[tokio::test]
    async fn fill_status_wait_receives_user_event_bus() {
        let dir = temp_dir("fill-status-bus");
        let order_ids = vec!["order-bus".to_string()];
        let trade_ids = vec!["trade-bus".to_string()];
        let waiter = tokio::spawn({
            let order_ids = order_ids.clone();
            let trade_ids = trade_ids.clone();
            async move {
                wait_for_live_user_channel_fill_status(
                    &order_ids,
                    &trade_ids,
                    Duration::from_secs(1),
                )
                .await
            }
        });
        tokio::task::yield_now().await;

        append_live_user_events_from_payload(
            &dir,
            r#"{"event_type":"trade","id":"trade-bus","taker_order_id":"order-bus","transaction_hash":"0xdef","status":"CONFIRMED"}"#,
        )
        .unwrap();

        let status = waiter.await.unwrap();
        assert_eq!(status.confirmed_order_ids, vec!["order-bus".to_string()]);
        assert_eq!(status.confirmed_trade_ids, vec!["trade-bus".to_string()]);
        assert_eq!(status.transaction_hashes, vec!["0xdef".to_string()]);
    }

    #[test]
    fn handle_user_channel_text_persists_normalized_events() {
        let dir = temp_dir("handle");
        fs::write(
            dir.join(LIVE_EXECUTION_JOURNAL_FILE),
            r#"{"execution_id":"exec-1","stage":"submitted","order_ids":["order-1"],"trade_ids":["trade-1"]}"#,
        )
        .unwrap();
        let path = dir.join(LIVE_USER_EVENTS_FILE);
        let mut writer = open_user_events_writer(&path).unwrap();
        handle_user_channel_text(
            &dir,
            &mut writer,
            r#"{"event_type":"trade","id":"trade-1","taker_order_id":"order-1","status":"RETRYING"}"#,
        )
        .unwrap();

        let body = fs::read_to_string(path).unwrap();
        assert!(body.contains(r#""stage":"user_trade_retrying""#));
        assert!(body.contains(r#""status_class":"pending""#));
        let halt = fs::read_to_string(dir.join(LIVE_USER_HALT_FILE)).unwrap();
        assert!(halt.contains("user trade retrying"));
    }

    #[test]
    fn handle_user_channel_text_halts_on_unjournaled_trade() {
        let dir = temp_dir("unjournaled");
        let path = dir.join(LIVE_USER_EVENTS_FILE);
        let mut writer = open_user_events_writer(&path).unwrap();
        handle_user_channel_text(
            &dir,
            &mut writer,
            r#"{"event_type":"trade","id":"trade-2","taker_order_id":"order-2","status":"CONFIRMED"}"#,
        )
        .unwrap();

        let halt = fs::read_to_string(dir.join(LIVE_USER_HALT_FILE)).unwrap();
        assert!(halt.contains("unjournaled user trade"));
    }

    #[test]
    fn handle_user_channel_text_accepts_combo_rfq_journaled_trade() {
        let dir = temp_dir("combo-rfq-journaled");
        fs::write(
            dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE),
            r#"{"generated_at":"2026-01-01T00:00:00Z","event_id":"event-1","stage":"accept_quote","status":"accepted_pending_finality","client_request_id":"client-1","rfq_id":"rfq-1","quote_id":"quote-1","selected_quote":{"quote_id":"quote-1","rfq_id":"rfq-1","side":"BUY","price":0.75,"qty_decimal":10.0},"accept_request":{"side":"BUY","price":"0.75","symbol":"event-1","qtyDecimal":"10"},"response":{"orderHash":"0xabc","transactionHash":"0xdef"}}"#,
        )
        .unwrap();
        let path = dir.join(LIVE_USER_EVENTS_FILE);
        let mut writer = open_user_events_writer(&path).unwrap();
        handle_user_channel_text(
            &dir,
            &mut writer,
            r#"{"event_type":"trade","id":"trade-rfq","rfqId":"rfq-1","quoteId":"quote-1","clientRequestId":"client-1","taker_order_id":"0xabc","transactionHash":"0xdef","side":"BUY","size":"10","price":"0.75","status":"CONFIRMED"}"#,
        )
        .unwrap();

        let body = fs::read_to_string(path).unwrap();
        assert!(body.contains(r#""rfq_id":"rfq-1""#));
        assert!(body.contains(r#""quote_id":"quote-1""#));
        assert!(body.contains(r#""client_request_id":"client-1""#));
        assert!(!dir.join(LIVE_USER_HALT_FILE).exists());

        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        let report_path = write_live_user_reconcile_report(&cfg).unwrap();
        let report: Value =
            serde_json::from_str(&fs::read_to_string(report_path).unwrap()).unwrap();
        assert_eq!(report["alerts"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn handle_user_channel_text_halts_on_combo_rfq_hash_match_with_economic_mismatch() {
        let dir = temp_dir("combo-rfq-hash-economic-mismatch");
        fs::write(
            dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE),
            r#"{"generated_at":"2026-01-01T00:00:00Z","event_id":"event-1","stage":"accept_quote","status":"accepted_pending_finality","client_request_id":"client-1","rfq_id":"rfq-1","quote_id":"quote-1","selected_quote":{"quote_id":"quote-1","rfq_id":"rfq-1","side":"BUY","price":0.75,"qty_decimal":10.0},"accept_request":{"side":"BUY","price":"0.75","symbol":"event-1","qtyDecimal":"10"},"response":{"orderHash":"0xabc","transactionHash":"0xdef"}}"#,
        )
        .unwrap();
        let path = dir.join(LIVE_USER_EVENTS_FILE);
        let mut writer = open_user_events_writer(&path).unwrap();
        handle_user_channel_text(
            &dir,
            &mut writer,
            r#"{"event_type":"trade","id":"trade-rfq","rfqId":"rfq-1","quoteId":"quote-1","clientRequestId":"client-1","taker_order_id":"0xabc","transactionHash":"0xdef","side":"BUY","size":"10","price":"0.76","status":"CONFIRMED"}"#,
        )
        .unwrap();

        let halt = fs::read_to_string(dir.join(LIVE_USER_HALT_FILE)).unwrap();
        assert!(halt.contains("unjournaled user trade"));
    }

    #[test]
    fn handle_user_channel_text_halts_on_sparse_combo_rfq_metadata_match() {
        let dir = temp_dir("combo-rfq-sparse-metadata");
        fs::write(
            dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE),
            r#"{"generated_at":"2026-01-01T00:00:00Z","event_id":"event-1","stage":"accept_quote","status":"accepted_pending_finality","client_request_id":"client-1","rfq_id":"rfq-1","quote_id":"quote-1","selected_quote":{"quote_id":"quote-1","rfq_id":"rfq-1","side":"BUY","price":0.75,"qty_decimal":10.0},"accept_request":{"side":"BUY","price":"0.75","symbol":"event-1","qtyDecimal":"10"}}"#,
        )
        .unwrap();
        let path = dir.join(LIVE_USER_EVENTS_FILE);
        let mut writer = open_user_events_writer(&path).unwrap();
        handle_user_channel_text(
            &dir,
            &mut writer,
            r#"{"event_type":"trade","id":"trade-rfq","rfqId":"rfq-1","status":"CONFIRMED"}"#,
        )
        .unwrap();

        let halt = fs::read_to_string(dir.join(LIVE_USER_HALT_FILE)).unwrap();
        assert!(halt.contains("unjournaled user trade"));

        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        let report_path = write_live_user_reconcile_report(&cfg).unwrap();
        let report: Value =
            serde_json::from_str(&fs::read_to_string(report_path).unwrap()).unwrap();
        assert!(report["alerts"].as_array().unwrap().iter().any(|alert| {
            alert["kind"] == "unjournaled_user_trade" && alert["trade_id"] == "trade-rfq"
        }));
    }

    #[test]
    fn handle_user_channel_text_halts_on_trade_after_combo_rfq_release() {
        let dir = temp_dir("combo-rfq-after-release");
        fs::write(
            dir.join(COMBO_RFQ_EXECUTION_JOURNAL_FILE),
            r#"{"generated_at":"2026-01-01T00:00:00Z","event_id":"event-1","stage":"rfq_finality","status":"finality_rejected_released","client_request_id":"client-1","rfq_id":"rfq-1","quote_id":"quote-1","selected_quote":{"quote_id":"quote-1","rfq_id":"rfq-1","side":"BUY","price":0.75,"qty_decimal":10.0},"accept_request":{"side":"BUY","price":"0.75","symbol":"event-1","qtyDecimal":"10"},"response":{"status":"QUOTE_DONE_AWAY"}}"#,
        )
        .unwrap();
        let path = dir.join(LIVE_USER_EVENTS_FILE);
        let mut writer = open_user_events_writer(&path).unwrap();
        handle_user_channel_text(
            &dir,
            &mut writer,
            r#"{"event_type":"trade","id":"trade-rfq","rfqId":"rfq-1","quoteId":"quote-1","clientRequestId":"client-1","side":"BUY","size":"10","price":"0.75","status":"CONFIRMED"}"#,
        )
        .unwrap();

        let halt = fs::read_to_string(dir.join(LIVE_USER_HALT_FILE)).unwrap();
        assert!(halt.contains("unjournaled user trade"));
    }

    #[test]
    fn handle_user_channel_text_matches_fast_trade_to_submit_intent_leg() {
        let dir = temp_dir("fast-submit-intent");
        fs::write(
            dir.join(LIVE_EXECUTION_JOURNAL_FILE),
            r#"{"execution_id":"exec-1","stage":"submit_intent","legs":[{"condition_id":"cond-1","token_id":"asset-1","side":"BUY","size":10.0,"limit_price":0.42}]}"#,
        )
        .unwrap();
        let path = dir.join(LIVE_USER_EVENTS_FILE);
        let mut writer = open_user_events_writer(&path).unwrap();
        handle_user_channel_text(
            &dir,
            &mut writer,
            r#"{"event_type":"trade","id":"trade-fast","taker_order_id":"order-fast","market":"cond-1","asset_id":"asset-1","side":"BUY","size":"10","price":"0.41","status":"CONFIRMED"}"#,
        )
        .unwrap();

        let body = fs::read_to_string(path).unwrap();
        assert!(body.contains(r#""trade_id":"trade-fast""#));
        assert!(!dir.join(LIVE_USER_HALT_FILE).exists());
    }

    #[test]
    fn live_user_halt_guard_blocks_when_flag_exists() {
        let dir = temp_dir("halt-guard");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        assert!(ensure_no_live_user_channel_halt(&cfg).is_ok());

        fs::write(dir.join(LIVE_USER_HALT_FILE), "halted").unwrap();

        let err = ensure_no_live_user_channel_halt(&cfg).unwrap_err();
        assert!(err.to_string().contains("live user-channel halt"));
    }

    #[test]
    fn reconcile_report_flags_failed_and_unjournaled_events() {
        let dir = temp_dir("report");
        fs::write(
            dir.join(LIVE_EXECUTION_JOURNAL_FILE),
            concat!(
                r#"{"execution_id":"exec-1","stage":"fill_confirmed_exposure_retained","order_ids":["order-1"],"expected_order_hashes":["0xexpected"],"trade_ids":["trade-1"],"legs":[{"condition_id":"cond-1"}]}"#,
                "\n"
            ),
        )
        .unwrap();
        fs::write(
            dir.join(LIVE_USER_EVENTS_FILE),
            concat!(
                r#"{"event_type":"trade","id":"trade-1","taker_order_id":"order-1","market":"cond-1","status":"FAILED"}"#,
                "\n",
                r#"{"event_type":"trade","id":"trade-2","taker_order_id":"order-2","market":"cond-2","status":"MATCHED_NOT_BROADCASTED"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.clob_user_ws_url = "wss://example/ws/user".to_string();

        let path = write_live_user_reconcile_report(&cfg).unwrap();
        let report: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(report["ws_url"], "wss://example/ws/user");
        assert_eq!(report["parsed_user_events"], 2);
        assert_eq!(report["journal_expected_order_hashes"], 1);
        let alerts = report["alerts"].as_array().unwrap();
        assert!(alerts
            .iter()
            .any(|alert| alert["kind"] == "user_trade_failed"));
        assert!(alerts
            .iter()
            .any(|alert| alert["kind"] == "unjournaled_user_trade"));
    }

    #[test]
    fn user_channel_finality_writes_deduped_route_replay_labels() {
        let dir = temp_dir("route-replay-labels");
        fs::write(
            dir.join(LIVE_EXECUTION_JOURNAL_FILE),
            concat!(
                r#"{"execution_id":"exec-1","stage":"submit_intent","event_id":"event-1","arb_type":"Bundle","order_ids":["order-y","order-n"],"legs":[{"condition_id":"cond-1","token_id":"asset-y","side":"BUY","size":10.0,"limit_price":0.49},{"condition_id":"cond-1","token_id":"asset-n","side":"BUY","size":10.0,"limit_price":0.49}]}"#,
                "\n"
            ),
        )
        .unwrap();
        fs::write(
            dir.join(LIVE_USER_EVENTS_FILE),
            concat!(
                r#"{"event_type":"trade","id":"trade-y","taker_order_id":"order-y","market":"cond-1","asset_id":"asset-y","side":"BUY","size":"10","price":"0.49","status":"CONFIRMED"}"#,
                "\n",
                r#"{"event_type":"trade","id":"trade-n","taker_order_id":"order-n","market":"cond-1","asset_id":"asset-n","side":"BUY","size":"10","price":"0.49","status":"CONFIRMED"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();

        assert_eq!(
            write_live_route_replay_labels_from_user_events(&cfg).unwrap(),
            1
        );
        assert_eq!(
            write_live_route_replay_labels_from_user_events(&cfg).unwrap(),
            0
        );

        let body = fs::read_to_string(dir.join("live_route_replay_journal.jsonl")).unwrap();
        assert_eq!(body.lines().count(), 1);
        let record: Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(
            record["label_id"],
            "user_channel:exec-1:ctf_merge_bundle_shadow:both_confirmed"
        );
        assert_eq!(record["event_id"], "event-1");
        assert_eq!(record["route"], "ctf_merge_bundle_shadow");
        assert_eq!(record["outcome_label"], "both_confirmed");
        assert_eq!(record["realized_ev_usd"], Value::Null);
        assert!(record["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|note| note == "planned_legs=2"));
    }

    #[test]
    fn user_channel_finality_labels_partial_and_failed_executions() {
        let dir = temp_dir("route-replay-partial-failed");
        fs::write(
            dir.join(LIVE_EXECUTION_JOURNAL_FILE),
            concat!(
                r#"{"execution_id":"exec-partial","stage":"submit_intent","event_id":"event-partial","arb_type":"Bundle","order_ids":["order-y","order-n"],"legs":[{"condition_id":"cond-1","token_id":"asset-y","side":"BUY","size":10.0,"limit_price":0.49},{"condition_id":"cond-1","token_id":"asset-n","side":"BUY","size":10.0,"limit_price":0.49}]}"#,
                "\n",
                r#"{"execution_id":"exec-failed","stage":"submitted","event_id":"event-failed","arb_type":"Bundle","order_ids":["order-failed"],"legs":[{"condition_id":"cond-2","token_id":"asset-f","side":"BUY","size":5.0,"limit_price":0.51}]}"#,
                "\n"
            ),
        )
        .unwrap();
        fs::write(
            dir.join(LIVE_USER_EVENTS_FILE),
            concat!(
                r#"{"event_type":"trade","id":"trade-y","taker_order_id":"order-y","market":"cond-1","asset_id":"asset-y","side":"BUY","size":"10","price":"0.49","status":"CONFIRMED"}"#,
                "\n",
                r#"{"event_type":"trade","id":"trade-failed","taker_order_id":"order-failed","market":"cond-2","asset_id":"asset-f","side":"BUY","size":"5","price":"0.51","status":"FAILED"}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();

        assert_eq!(
            write_live_route_replay_labels_from_user_events(&cfg).unwrap(),
            2
        );

        let body = fs::read_to_string(dir.join("live_route_replay_journal.jsonl")).unwrap();
        assert!(body.contains(r#""outcome_label":"one_leg_confirmed""#));
        assert!(body.contains(r#""outcome_label":"matched_then_failed""#));
    }

    #[test]
    fn reconciled_journal_execution_is_not_trusted_for_user_events() {
        let dir = temp_dir("manual-reconciled");
        fs::write(
            dir.join(LIVE_EXECUTION_JOURNAL_FILE),
            concat!(
                r#"{"execution_id":"exec-1","stage":"fill_confirmed_exposure_retained","order_ids":["order-1"],"legs":[{"condition_id":"cond-1"}]}"#,
                "\n",
                r#"{"execution_id":"exec-1","stage":"manual_reconciled"}"#,
                "\n"
            ),
        )
        .unwrap();
        fs::write(
            dir.join(LIVE_USER_EVENTS_FILE),
            r#"{"event_type":"order","id":"order-1","market":"cond-1","type":"UPDATE"}"#,
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;

        let path = write_live_user_reconcile_report(&cfg).unwrap();
        let report: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(report["unresolved_journal_executions"], 0);
        assert_eq!(report["alerts"][0]["kind"], "unjournaled_user_order");
    }
}
