//! Persistent Polymarket CLOB engine-mode oracle.
//!
//! The scanner treats matching-engine restart, post-only, cancel-only, disabled,
//! and throttling signals as venue state, not just request-local retry errors.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::config::Config;

pub const ENGINE_MODE_STATE_FILE: &str = "engine_mode_state.json";
pub const ENGINE_MODE_REPORT_FILE: &str = "engine_mode_report.json";
pub const ENGINE_MODE_JOURNAL_FILE: &str = "engine_mode_journal.jsonl";

const DEFAULT_RESTART_STICKY_SECONDS: u64 = 120;
const DEFAULT_POST_ONLY_SECONDS: u64 = 120;
const DEFAULT_RATE_LIMIT_SECONDS: u64 = 15;
const DEFAULT_TRANSIENT_SECONDS: u64 = 30;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EngineMode {
    Normal,
    Restarting,
    PostOnly,
    CancelOnly,
    Disabled,
    RateLimited,
    TransientError,
    Unknown,
}

impl EngineMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Restarting => "restarting",
            Self::PostOnly => "post_only",
            Self::CancelOnly => "cancel_only",
            Self::Disabled => "disabled",
            Self::RateLimited => "rate_limited",
            Self::TransientError => "transient_error",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineModeObservation {
    pub observed_at: String,
    pub source: String,
    pub endpoint: String,
    pub http_status: Option<u16>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub retry_after_seconds: Option<u64>,
    pub mode: EngineMode,
    pub route_blocker: Option<String>,
    pub block_new_orders: bool,
    pub cancels_allowed: Option<bool>,
    pub post_only_required: bool,
    pub sticky_until: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineModeState {
    pub updated_at: Option<String>,
    pub mode: EngineMode,
    pub active_until: Option<String>,
    pub retry_after_seconds: Option<u64>,
    pub route_blockers: Vec<String>,
    pub last_source: Option<String>,
    pub last_endpoint: Option<String>,
    pub last_http_status: Option<u16>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub observations: u64,
}

impl Default for EngineModeState {
    fn default() -> Self {
        Self {
            updated_at: None,
            mode: EngineMode::Unknown,
            active_until: None,
            retry_after_seconds: None,
            route_blockers: Vec::new(),
            last_source: None,
            last_endpoint: None,
            last_http_status: None,
            last_error_code: None,
            last_error_message: None,
            observations: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineModeReport {
    pub generated_at: String,
    pub state: EngineModeState,
    pub active: bool,
    pub status: String,
    pub blockers: Vec<String>,
}

pub fn classify_engine_mode_observation(
    observed_at: DateTime<Utc>,
    source: impl Into<String>,
    endpoint: impl Into<String>,
    http_status: Option<u16>,
    headers: Option<&HeaderMap>,
    error_body: Option<&str>,
) -> EngineModeObservation {
    let source = source.into();
    let endpoint = endpoint.into();
    let body = error_body.unwrap_or_default().trim();
    let retry_after_seconds = retry_after_seconds(headers, body);
    let (error_code, error_message) = parse_error_body(body);
    let text = format!(
        "{} {} {}",
        http_status
            .map(|status| status.to_string())
            .unwrap_or_default(),
        error_code.as_deref().unwrap_or_default(),
        error_message.as_deref().unwrap_or(body)
    )
    .to_ascii_lowercase();

    let mut mode = EngineMode::Unknown;
    let mut route_blocker = None;
    let mut block_new_orders = false;
    let mut cancels_allowed = None;
    let mut post_only_required = false;
    let mut sticky_seconds = None;

    if http_status == Some(425) || text.contains("matching engine") || text.contains("restarting") {
        mode = EngineMode::Restarting;
        route_blocker = Some("matching_engine_restarting".to_string());
        block_new_orders = true;
        cancels_allowed = Some(false);
        sticky_seconds = Some(retry_after_seconds.unwrap_or(DEFAULT_RESTART_STICKY_SECONDS));
    } else if text.contains("post_only") || text.contains("post-only") {
        mode = EngineMode::PostOnly;
        route_blocker = Some("matching_engine_post_only".to_string());
        block_new_orders = true;
        cancels_allowed = Some(true);
        post_only_required = true;
        sticky_seconds = Some(retry_after_seconds.unwrap_or(DEFAULT_POST_ONLY_SECONDS));
    } else if text.contains("cancel_only")
        || text.contains("cancel-only")
        || text.contains("cancel only")
    {
        mode = EngineMode::CancelOnly;
        route_blocker = Some("matching_engine_cancel_only".to_string());
        block_new_orders = true;
        cancels_allowed = Some(true);
        sticky_seconds = retry_after_seconds;
    } else if text.contains("trading is currently disabled") || text.contains("trading disabled") {
        mode = EngineMode::Disabled;
        route_blocker = Some("trading_disabled".to_string());
        block_new_orders = true;
        cancels_allowed = Some(false);
        sticky_seconds = retry_after_seconds;
    } else if http_status == Some(429)
        || text.contains("too many requests")
        || text.contains("rate limit")
    {
        mode = EngineMode::RateLimited;
        route_blocker = Some("clob_rate_limited".to_string());
        block_new_orders = true;
        cancels_allowed = None;
        sticky_seconds = Some(retry_after_seconds.unwrap_or(DEFAULT_RATE_LIMIT_SECONDS));
    } else if matches!(http_status, Some(502..=504))
        || text.contains("timeout")
        || text.contains("timed out")
    {
        mode = EngineMode::TransientError;
        route_blocker = Some("clob_transient_error".to_string());
        block_new_orders = true;
        cancels_allowed = None;
        sticky_seconds = Some(retry_after_seconds.unwrap_or(DEFAULT_TRANSIENT_SECONDS));
    } else if http_status.is_some_and(|status| (200..300).contains(&status)) {
        mode = EngineMode::Normal;
    }

    EngineModeObservation {
        observed_at: observed_at.to_rfc3339(),
        source,
        endpoint,
        http_status,
        error_code,
        error_message: error_message.or_else(|| (!body.is_empty()).then(|| body.to_string())),
        retry_after_seconds,
        mode,
        route_blocker,
        block_new_orders,
        cancels_allowed,
        post_only_required,
        sticky_until: sticky_seconds.and_then(|seconds| sticky_until(observed_at, seconds)),
    }
}

pub fn observe_http_response(
    config: &Config,
    source: impl Into<String>,
    endpoint: impl Into<String>,
    http_status: u16,
    headers: Option<&HeaderMap>,
    error_body: Option<&str>,
) -> Result<EngineModeReport> {
    let observation = classify_engine_mode_observation(
        Utc::now(),
        source,
        endpoint,
        Some(http_status),
        headers,
        error_body,
    );
    record_engine_mode_observation(config, &observation)
}

pub fn observe_error_text(
    config: &Config,
    source: impl Into<String>,
    endpoint: impl Into<String>,
    error: impl ToString,
) -> Result<EngineModeReport> {
    let error = error.to_string();
    let observation =
        classify_engine_mode_observation(Utc::now(), source, endpoint, None, None, Some(&error));
    record_engine_mode_observation(config, &observation)
}

pub async fn poll_status_page_summary(
    http: &Client,
    config: &Config,
) -> Result<Option<EngineModeReport>> {
    if !config.live_status_page_enabled {
        return Ok(None);
    }

    let url = config.polymarket_status_api_url.trim();
    if url.is_empty() {
        bail!("POLYMARKET_STATUS_API_URL_empty");
    }

    let response = http
        .get(url)
        .timeout(std::time::Duration::from_secs(
            config.api_timeout_secs.max(1),
        ))
        .send()
        .await
        .with_context(|| format!("fetching Polymarket status summary {url}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("reading Polymarket status summary response {url}"))?;
    if !status.is_success() {
        bail!(
            "Polymarket status summary failed status={} body={}",
            status,
            body.chars().take(256).collect::<String>()
        );
    }

    let summary_observation = classify_status_page_summary(
        Utc::now(),
        url,
        &body,
        config.live_status_page_maintenance_prehalt_secs,
    )?;
    if summary_observation.block_new_orders {
        return record_engine_mode_observation(config, &summary_observation).map(Some);
    }

    let components_url = config.polymarket_status_components_api_url.trim();
    if components_url.is_empty() {
        return record_engine_mode_observation(config, &summary_observation).map(Some);
    }
    let response = http
        .get(components_url)
        .timeout(std::time::Duration::from_secs(
            config.api_timeout_secs.max(1),
        ))
        .send()
        .await
        .with_context(|| format!("fetching Polymarket status components {components_url}"))?;
    let status = response.status();
    let body = response.text().await.with_context(|| {
        format!("reading Polymarket status components response {components_url}")
    })?;
    if !status.is_success() {
        bail!(
            "Polymarket status components failed status={} body={}",
            status,
            body.chars().take(256).collect::<String>()
        );
    }

    let observation = classify_status_page_components(
        Utc::now(),
        components_url,
        &body,
        config.live_status_page_maintenance_prehalt_secs,
    )?;
    record_engine_mode_observation(config, &observation).map(Some)
}

pub fn record_engine_mode_observation(
    config: &Config,
    observation: &EngineModeObservation,
) -> Result<EngineModeReport> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    append_engine_mode_journal(config, observation)?;

    let mut state = read_engine_mode_state(config)?;
    state.observations = state.observations.saturating_add(1);
    state.updated_at = Some(observation.observed_at.clone());
    state.last_source = Some(observation.source.clone());
    state.last_endpoint = Some(observation.endpoint.clone());
    state.last_http_status = observation.http_status;
    state.last_error_code = observation.error_code.clone();
    state.last_error_message = observation.error_message.clone();

    match observation.mode {
        EngineMode::Unknown => {}
        _ => {
            state.mode = observation.mode;
            state.active_until = observation.sticky_until.clone();
            state.retry_after_seconds = observation.retry_after_seconds;
            state.route_blockers = observation
                .route_blocker
                .iter()
                .cloned()
                .collect::<Vec<String>>();
        }
    }

    write_engine_mode_state(config, &state)?;
    let report = build_engine_mode_report_from_state(state, Utc::now());
    write_engine_mode_report_body(config, &report)?;
    Ok(report)
}

pub fn active_route_blockers(config: &Config) -> Result<Vec<String>> {
    let state = read_engine_mode_state(config)?;
    Ok(active_blockers_for_state(&state, Utc::now()))
}

pub fn ensure_no_active_new_order_blocker(config: &Config) -> Result<()> {
    let blockers = active_route_blockers(config)?;
    if blockers.is_empty() {
        return Ok(());
    }
    bail!(
        "active CLOB engine-mode blocker(s) prevent new live orders: {}",
        blockers.join("|")
    )
}

pub fn build_engine_mode_report(config: &Config) -> Result<EngineModeReport> {
    let state = read_engine_mode_state(config)?;
    Ok(build_engine_mode_report_from_state(state, Utc::now()))
}

pub fn write_engine_mode_report(config: &Config) -> Result<PathBuf> {
    let report = build_engine_mode_report(config)?;
    write_engine_mode_report_body(config, &report)
}

fn build_engine_mode_report_from_state(
    state: EngineModeState,
    now: DateTime<Utc>,
) -> EngineModeReport {
    let blockers = active_blockers_for_state(&state, now);
    let active = !blockers.is_empty();
    let status = if active {
        "blocked".to_string()
    } else if state.updated_at.is_some() {
        "clear".to_string()
    } else {
        "unknown_no_observations".to_string()
    };
    EngineModeReport {
        generated_at: now.to_rfc3339(),
        state,
        active,
        status,
        blockers,
    }
}

fn active_blockers_for_state(state: &EngineModeState, now: DateTime<Utc>) -> Vec<String> {
    if state.route_blockers.is_empty() {
        return Vec::new();
    }
    match state.active_until.as_deref() {
        Some(raw) => match DateTime::parse_from_rfc3339(raw) {
            Ok(until) if until.with_timezone(&Utc) <= now => Vec::new(),
            Ok(_) => state.route_blockers.clone(),
            Err(_) => state.route_blockers.clone(),
        },
        None => state.route_blockers.clone(),
    }
}

fn read_engine_mode_state(config: &Config) -> Result<EngineModeState> {
    let path = config.diagnostics_dir.join(ENGINE_MODE_STATE_FILE);
    if !path.exists() {
        return Ok(EngineModeState::default());
    }
    let body = fs::read_to_string(&path)
        .with_context(|| format!("reading engine-mode state {}", path.display()))?;
    serde_json::from_str(&body)
        .with_context(|| format!("parsing engine-mode state {}", path.display()))
}

fn write_engine_mode_state(config: &Config, state: &EngineModeState) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(ENGINE_MODE_STATE_FILE);
    fs::write(&path, serde_json::to_string_pretty(state)?)
        .with_context(|| format!("writing engine-mode state {}", path.display()))?;
    Ok(path)
}

fn write_engine_mode_report_body(config: &Config, report: &EngineModeReport) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(ENGINE_MODE_REPORT_FILE);
    fs::write(&path, serde_json::to_string_pretty(report)?)
        .with_context(|| format!("writing engine-mode report {}", path.display()))?;
    Ok(path)
}

fn append_engine_mode_journal(config: &Config, observation: &EngineModeObservation) -> Result<()> {
    let path = config.diagnostics_dir.join(ENGINE_MODE_JOURNAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening engine-mode journal {}", path.display()))?;
    serde_json::to_writer(&mut file, observation)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn retry_after_seconds(headers: Option<&HeaderMap>, body: &str) -> Option<u64> {
    retry_after_from_headers(headers).or_else(|| retry_after_from_body(body))
}

fn retry_after_from_headers(headers: Option<&HeaderMap>) -> Option<u64> {
    let header = headers?.get(RETRY_AFTER)?.to_str().ok()?.trim();
    header.parse::<u64>().ok()
}

fn retry_after_from_body(body: &str) -> Option<u64> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    value
        .get("retry_after_seconds")
        .or_else(|| value.get("retryAfterSeconds"))
        .and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_u64(),
            serde_json::Value::String(raw) => raw.trim().parse::<u64>().ok(),
            _ => None,
        })
}

fn parse_error_body(body: &str) -> (Option<String>, Option<String>) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return (None, None);
    };
    let code = value
        .get("code")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let message = value
        .get("error")
        .or_else(|| value.get("errorMsg"))
        .or_else(|| value.get("message"))
        .and_then(|value| value.as_str())
        .map(str::to_string);
    (code, message)
}

fn sticky_until(observed_at: DateTime<Utc>, seconds: u64) -> Option<String> {
    if seconds == 0 {
        return None;
    }
    let seconds = seconds.min(i64::MAX as u64) as i64;
    Some((observed_at + ChronoDuration::seconds(seconds)).to_rfc3339())
}

#[derive(Debug, Deserialize)]
struct StatusPageSummary {
    page: Option<StatusPagePage>,
    #[serde(default, rename = "activeIncidents")]
    active_incidents: Vec<StatusPageIncident>,
    #[serde(default, rename = "activeMaintenances")]
    active_maintenances: Vec<StatusPageMaintenance>,
}

#[derive(Debug, Deserialize)]
struct StatusPagePage {
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatusPageIncident {
    id: Option<String>,
    name: Option<String>,
    status: Option<String>,
    impact: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatusPageMaintenance {
    id: Option<String>,
    name: Option<String>,
    start: Option<String>,
    status: Option<String>,
    duration: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StatusPageComponentsEnvelope {
    Wrapped {
        components: Vec<StatusPageComponent>,
    },
    Direct(Vec<StatusPageComponent>),
}

#[derive(Debug, Deserialize)]
struct StatusPageComponent {
    id: Option<String>,
    name: Option<String>,
    status: Option<String>,
    #[serde(default, rename = "activeIncidents")]
    active_incidents: Vec<StatusPageIncident>,
    #[serde(default, rename = "activeMaintenances")]
    active_maintenances: Vec<StatusPageMaintenance>,
}

pub fn classify_status_page_summary(
    observed_at: DateTime<Utc>,
    endpoint: impl Into<String>,
    body: &str,
    maintenance_prehalt_secs: u64,
) -> Result<EngineModeObservation> {
    let endpoint = endpoint.into();
    let summary: StatusPageSummary =
        serde_json::from_str(body).context("parsing Polymarket status summary")?;

    if let Some(incident) = summary.active_incidents.first() {
        return Ok(status_page_observation(
            observed_at,
            endpoint,
            EngineMode::Disabled,
            Some("status_page_active_incident".to_string()),
            true,
            Some(false),
            false,
            Some(DEFAULT_TRANSIENT_SECONDS),
            Some(status_page_incident_detail(incident)),
        ));
    }

    if let Some(page_status) = summary
        .page
        .as_ref()
        .and_then(|page| page.status.as_deref())
        .filter(|status| !status.eq_ignore_ascii_case("UP"))
    {
        return Ok(status_page_observation(
            observed_at,
            endpoint,
            EngineMode::TransientError,
            Some("status_page_not_up".to_string()),
            true,
            None,
            false,
            Some(DEFAULT_TRANSIENT_SECONDS),
            Some(format!("page_status={page_status}")),
        ));
    }

    if let Some((maintenance, sticky_seconds)) = summary
        .active_maintenances
        .iter()
        .filter_map(|maintenance| {
            status_page_maintenance_sticky_seconds(
                observed_at,
                maintenance,
                maintenance_prehalt_secs,
            )
            .map(|seconds| (maintenance, seconds))
        })
        .next()
    {
        return Ok(status_page_observation(
            observed_at,
            endpoint,
            EngineMode::Restarting,
            Some("status_page_active_maintenance".to_string()),
            true,
            Some(false),
            false,
            Some(sticky_seconds),
            Some(status_page_maintenance_detail(maintenance)),
        ));
    }

    Ok(status_page_observation(
        observed_at,
        endpoint,
        EngineMode::Normal,
        None,
        false,
        None,
        false,
        None,
        Some("status_page_clear".to_string()),
    ))
}

pub fn classify_status_page_components(
    observed_at: DateTime<Utc>,
    endpoint: impl Into<String>,
    body: &str,
    maintenance_prehalt_secs: u64,
) -> Result<EngineModeObservation> {
    let endpoint = endpoint.into();
    let envelope: StatusPageComponentsEnvelope =
        serde_json::from_str(body).context("parsing Polymarket status components")?;
    let components = match envelope {
        StatusPageComponentsEnvelope::Wrapped { components } => components,
        StatusPageComponentsEnvelope::Direct(components) => components,
    };

    let Some(component) = components
        .iter()
        .find(|component| status_page_component_is_live_critical(component))
    else {
        return Ok(status_page_observation(
            observed_at,
            endpoint,
            EngineMode::Normal,
            None,
            false,
            None,
            false,
            None,
            Some("status_components_no_live_critical_component".to_string()),
        ));
    };

    if let Some(incident) = component.active_incidents.first() {
        return Ok(status_page_observation(
            observed_at,
            endpoint,
            EngineMode::Disabled,
            Some("status_component_active_incident".to_string()),
            true,
            Some(false),
            false,
            Some(DEFAULT_TRANSIENT_SECONDS),
            Some(format!(
                "component={} {}",
                component.name.as_deref().unwrap_or("unknown"),
                status_page_incident_detail(incident)
            )),
        ));
    }

    if let Some(status) = component
        .status
        .as_deref()
        .filter(|status| !status.eq_ignore_ascii_case("OPERATIONAL"))
    {
        return Ok(status_page_observation(
            observed_at,
            endpoint,
            EngineMode::TransientError,
            Some("status_component_not_operational".to_string()),
            true,
            None,
            false,
            Some(DEFAULT_TRANSIENT_SECONDS),
            Some(format!(
                "component={} status={status}",
                component.name.as_deref().unwrap_or("unknown")
            )),
        ));
    }

    if let Some((maintenance, sticky_seconds)) = component
        .active_maintenances
        .iter()
        .filter_map(|maintenance| {
            status_page_maintenance_sticky_seconds(
                observed_at,
                maintenance,
                maintenance_prehalt_secs,
            )
            .map(|seconds| (maintenance, seconds))
        })
        .next()
    {
        return Ok(status_page_observation(
            observed_at,
            endpoint,
            EngineMode::Restarting,
            Some("status_component_active_maintenance".to_string()),
            true,
            Some(false),
            false,
            Some(sticky_seconds),
            Some(format!(
                "component={} {}",
                component.name.as_deref().unwrap_or("unknown"),
                status_page_maintenance_detail(maintenance)
            )),
        ));
    }

    Ok(status_page_observation(
        observed_at,
        endpoint,
        EngineMode::Normal,
        None,
        false,
        None,
        false,
        None,
        Some(format!(
            "status_component_clear component={} id={}",
            component.name.as_deref().unwrap_or("unknown"),
            component.id.as_deref().unwrap_or("unknown")
        )),
    ))
}

fn status_page_component_is_live_critical(component: &StatusPageComponent) -> bool {
    component
        .name
        .as_deref()
        .map(|name| name.eq_ignore_ascii_case("CLOB API"))
        .unwrap_or(false)
}

fn status_page_observation(
    observed_at: DateTime<Utc>,
    endpoint: String,
    mode: EngineMode,
    route_blocker: Option<String>,
    block_new_orders: bool,
    cancels_allowed: Option<bool>,
    post_only_required: bool,
    sticky_seconds: Option<u64>,
    error_message: Option<String>,
) -> EngineModeObservation {
    EngineModeObservation {
        observed_at: observed_at.to_rfc3339(),
        source: "status_page".to_string(),
        endpoint,
        http_status: None,
        error_code: None,
        error_message,
        retry_after_seconds: sticky_seconds,
        mode,
        route_blocker,
        block_new_orders,
        cancels_allowed,
        post_only_required,
        sticky_until: sticky_seconds.and_then(|seconds| sticky_until(observed_at, seconds)),
    }
}

fn status_page_incident_detail(incident: &StatusPageIncident) -> String {
    format!(
        "incident id={} status={} impact={} name={}",
        incident.id.as_deref().unwrap_or("unknown"),
        incident.status.as_deref().unwrap_or("unknown"),
        incident.impact.as_deref().unwrap_or("unknown"),
        incident.name.as_deref().unwrap_or("unknown")
    )
}

fn status_page_maintenance_detail(maintenance: &StatusPageMaintenance) -> String {
    format!(
        "maintenance id={} status={} start={} duration_minutes={} name={}",
        maintenance.id.as_deref().unwrap_or("unknown"),
        maintenance.status.as_deref().unwrap_or("unknown"),
        maintenance.start.as_deref().unwrap_or("unknown"),
        maintenance_duration_minutes_text(maintenance),
        maintenance.name.as_deref().unwrap_or("unknown")
    )
}

fn status_page_maintenance_sticky_seconds(
    observed_at: DateTime<Utc>,
    maintenance: &StatusPageMaintenance,
    prehalt_secs: u64,
) -> Option<u64> {
    let status = maintenance
        .status
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if status.contains("complete") || status.contains("finished") || status.contains("resolved") {
        return None;
    }

    let start = maintenance
        .start
        .as_deref()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc));
    let is_in_progress = status.contains("inprogress")
        || status.contains("in_progress")
        || status.contains("ongoing")
        || (status.contains("started") && !status.contains("notstarted"));
    let should_block = match start {
        Some(start) if start > observed_at => {
            let seconds_until = (start - observed_at).num_seconds().max(0) as u64;
            seconds_until <= prehalt_secs
        }
        Some(_) => true,
        None => true,
    } || is_in_progress;

    if !should_block {
        return None;
    }

    let duration_secs = maintenance
        .duration
        .as_ref()
        .and_then(status_page_duration_minutes)
        .unwrap_or(0)
        .saturating_mul(60);
    let seconds_until_start = start
        .filter(|start| *start > observed_at)
        .map(|start| (start - observed_at).num_seconds().max(0) as u64)
        .unwrap_or(0);
    Some(
        seconds_until_start
            .saturating_add(duration_secs)
            .saturating_add(DEFAULT_POST_ONLY_SECONDS)
            .max(DEFAULT_TRANSIENT_SECONDS),
    )
}

fn status_page_duration_minutes(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(number) => number.as_u64(),
        serde_json::Value::String(raw) => raw.trim().parse::<u64>().ok(),
        _ => None,
    }
}

fn maintenance_duration_minutes_text(maintenance: &StatusPageMaintenance) -> String {
    maintenance
        .duration
        .as_ref()
        .and_then(status_page_duration_minutes)
        .map(|minutes| minutes.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;
    use httpmock::prelude::*;

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        std::env::temp_dir().join(format!("polymarket-engine-mode-{name}-{suffix}"))
    }

    #[test]
    fn classifies_restart_as_blocking_engine_mode() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let observation = classify_engine_mode_observation(
            observed_at,
            "test",
            "POST /orders",
            Some(425),
            None,
            Some(r#"{"error":"matching engine restarting"}"#),
        );

        assert_eq!(observation.mode, EngineMode::Restarting);
        assert_eq!(
            observation.route_blocker.as_deref(),
            Some("matching_engine_restarting")
        );
        assert!(observation.block_new_orders);
        assert!(observation.sticky_until.is_some());
    }

    #[test]
    fn post_only_uses_retry_after_seconds_from_body() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let observation = classify_engine_mode_observation(
            observed_at,
            "test",
            "POST /order",
            Some(503),
            None,
            Some(
                r#"{"error":"post-only mode: only post-only orders and cancels are allowed","code":"post_only_mode","retry_after_seconds":79}"#,
            ),
        );

        assert_eq!(observation.mode, EngineMode::PostOnly);
        assert_eq!(observation.retry_after_seconds, Some(79));
        assert!(observation.post_only_required);
        assert_eq!(observation.cancels_allowed, Some(true));
        assert_eq!(
            observation.sticky_until.as_deref(),
            Some("2026-01-01T00:01:19+00:00")
        );
    }

    #[test]
    fn state_blocks_until_normal_observation_clears() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_dir("clear");

        observe_http_response(
            &cfg,
            "test",
            "POST /orders",
            503,
            None,
            Some("Trading is currently cancel-only. New orders are not accepted, but cancels are allowed."),
        )
        .unwrap();
        assert_eq!(
            active_route_blockers(&cfg).unwrap(),
            vec!["matching_engine_cancel_only".to_string()]
        );

        observe_http_response(&cfg, "test", "GET /ok", 200, None, None).unwrap();
        assert!(active_route_blockers(&cfg).unwrap().is_empty());
        assert!(cfg.diagnostics_dir.join(ENGINE_MODE_STATE_FILE).exists());
        assert!(cfg.diagnostics_dir.join(ENGINE_MODE_REPORT_FILE).exists());
        assert!(cfg.diagnostics_dir.join(ENGINE_MODE_JOURNAL_FILE).exists());
    }

    #[test]
    fn status_page_active_incident_blocks_live_temporarily() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let observation = classify_status_page_summary(
            observed_at,
            "https://status.polymarket.com/v3/summary.json",
            r#"{
              "page":{"name":"Polymarket","status":"UP"},
              "activeIncidents":[{
                "id":"inc-1",
                "name":"CLOB degraded",
                "status":"INVESTIGATING",
                "impact":"MAJOROUTAGE"
              }],
              "activeMaintenances":[]
            }"#,
            1_800,
        )
        .unwrap();

        assert_eq!(observation.mode, EngineMode::Disabled);
        assert_eq!(
            observation.route_blocker.as_deref(),
            Some("status_page_active_incident")
        );
        assert!(observation.block_new_orders);
        assert_eq!(observation.cancels_allowed, Some(false));
        assert_eq!(
            observation.sticky_until.as_deref(),
            Some("2026-01-01T00:00:30+00:00")
        );
    }

    #[test]
    fn status_page_near_maintenance_blocks_through_post_only_window() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let observation = classify_status_page_summary(
            observed_at,
            "https://status.polymarket.com/v3/summary.json",
            r#"{
              "page":{"name":"Polymarket","status":"UP"},
              "activeIncidents":[],
              "activeMaintenances":[{
                "id":"maint-1",
                "name":"Matching engine restart",
                "start":"2026-01-01T00:10:00Z",
                "status":"NOTSTARTEDYET",
                "duration":"15"
              }]
            }"#,
            1_800,
        )
        .unwrap();

        assert_eq!(observation.mode, EngineMode::Restarting);
        assert_eq!(
            observation.route_blocker.as_deref(),
            Some("status_page_active_maintenance")
        );
        assert!(observation.block_new_orders);
        assert_eq!(
            observation.sticky_until.as_deref(),
            Some("2026-01-01T00:27:00+00:00")
        );
    }

    #[test]
    fn status_page_far_future_maintenance_does_not_block() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let observation = classify_status_page_summary(
            observed_at,
            "https://status.polymarket.com/v3/summary.json",
            r#"{
              "page":{"name":"Polymarket","status":"UP"},
              "activeIncidents":[],
              "activeMaintenances":[{
                "id":"maint-1",
                "name":"Matching engine restart",
                "start":"2026-01-01T02:00:00Z",
                "status":"NOTSTARTEDYET",
                "duration":"15"
              }]
            }"#,
            1_800,
        )
        .unwrap();

        assert_eq!(observation.mode, EngineMode::Normal);
        assert!(!observation.block_new_orders);
        assert!(observation.route_blocker.is_none());
    }

    #[test]
    fn status_components_degraded_clob_blocks_live() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let observation = classify_status_page_components(
            observed_at,
            "https://status.polymarket.com/v3/components.json",
            r#"{
              "components":[{
                "id":"clob-api",
                "name":"CLOB API",
                "status":"DEGRADED",
                "activeIncidents":[],
                "activeMaintenances":[]
              }]
            }"#,
            1_800,
        )
        .unwrap();

        assert_eq!(observation.mode, EngineMode::TransientError);
        assert_eq!(
            observation.route_blocker.as_deref(),
            Some("status_component_not_operational")
        );
        assert!(observation.block_new_orders);
        assert_eq!(
            observation.sticky_until.as_deref(),
            Some("2026-01-01T00:00:30+00:00")
        );
    }

    #[test]
    fn status_components_near_clob_maintenance_blocks_numeric_duration() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let observation = classify_status_page_components(
            observed_at,
            "https://status.polymarket.com/v3/components.json",
            r#"{
              "components":[{
                "id":"clob-api",
                "name":"CLOB API",
                "status":"OPERATIONAL",
                "activeIncidents":[],
                "activeMaintenances":[{
                  "id":"maint-1",
                  "name":"Scheduled CLOB maintenance",
                  "start":"2026-01-01T00:10:00Z",
                  "duration":60,
                  "status":"NOTSTARTEDYET"
                }]
              }]
            }"#,
            1_800,
        )
        .unwrap();

        assert_eq!(observation.mode, EngineMode::Restarting);
        assert_eq!(
            observation.route_blocker.as_deref(),
            Some("status_component_active_maintenance")
        );
        assert!(observation.block_new_orders);
        assert_eq!(
            observation.sticky_until.as_deref(),
            Some("2026-01-01T01:12:00+00:00")
        );
        assert!(observation
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("duration_minutes=60"));
    }

    #[test]
    fn status_components_ignore_noncritical_components() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let observation = classify_status_page_components(
            observed_at,
            "https://status.polymarket.com/v3/components.json",
            r#"{
              "components":[{
                "id":"website",
                "name":"Website",
                "status":"DEGRADED",
                "activeIncidents":[],
                "activeMaintenances":[]
              }]
            }"#,
            1_800,
        )
        .unwrap();

        assert_eq!(observation.mode, EngineMode::Normal);
        assert!(!observation.block_new_orders);
        assert!(observation.route_blocker.is_none());
    }

    #[test]
    fn status_page_blocker_records_engine_mode_state() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_dir("status-page");

        let observation = classify_status_page_summary(
            Utc::now(),
            "https://status.polymarket.com/v3/summary.json",
            r#"{
              "page":{"name":"Polymarket","status":"UP"},
              "activeIncidents":[{
                "id":"inc-1",
                "name":"CLOB degraded",
                "status":"INVESTIGATING",
                "impact":"MAJOROUTAGE"
              }],
              "activeMaintenances":[]
            }"#,
            cfg.live_status_page_maintenance_prehalt_secs,
        )
        .unwrap();
        let report = record_engine_mode_observation(&cfg, &observation).unwrap();

        assert_eq!(report.status, "blocked");
        assert_eq!(
            active_route_blockers(&cfg).unwrap(),
            vec!["status_page_active_incident".to_string()]
        );
    }

    #[tokio::test]
    async fn status_page_clear_records_normal_engine_mode_state() {
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
                        "status": "OPERATIONAL",
                        "activeIncidents": [],
                        "activeMaintenances": []
                    }]
                }));
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_dir("status-page-clear");
        cfg.live_status_page_enabled = true;
        cfg.polymarket_status_api_url = format!("{}/v3/summary.json", server.base_url());
        cfg.polymarket_status_components_api_url =
            format!("{}/v3/components.json", server.base_url());

        let report = poll_status_page_summary(&Client::new(), &cfg)
            .await
            .unwrap()
            .expect("clear status page report");

        assert_eq!(report.status, "clear");
        assert!(!report.active);
        assert!(report.blockers.is_empty());
        assert_eq!(report.state.mode, EngineMode::Normal);
        assert_eq!(report.state.observations, 1);
        assert!(report
            .state
            .last_error_message
            .as_deref()
            .unwrap_or_default()
            .contains("status_component_clear"));
        assert!(cfg.diagnostics_dir.join(ENGINE_MODE_STATE_FILE).exists());
        assert!(cfg.diagnostics_dir.join(ENGINE_MODE_REPORT_FILE).exists());
        assert!(cfg.diagnostics_dir.join(ENGINE_MODE_JOURNAL_FILE).exists());
        summary.assert_calls_async(1).await;
        components.assert_calls_async(1).await;
    }
}
