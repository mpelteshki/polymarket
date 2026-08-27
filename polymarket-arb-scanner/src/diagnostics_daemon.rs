use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tokio::time::Duration;

use crate::config::Config;
use crate::{
    accounting_snapshot, engine_mode, live_executor, onchain_fills, rfq_finality,
    rfq_stream_client, settlement_monitor, user_channel,
};

pub const DIAGNOSTICS_DAEMON_REPORT_FILE: &str = "diagnostics_daemon_report.json";
pub const DIAGNOSTICS_DAEMON_STATE_FILE: &str = "diagnostics_daemon_state.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiagnosticsDaemonReport {
    pub generated_at: String,
    pub mode: String,
    pub live_submissions_enabled: bool,
    pub rfq_shadow_run_attempted: bool,
    pub rfq_shadow_status: Option<String>,
    pub rfq_shadow_messages_seen: usize,
    pub order_filled_collector_attempted: bool,
    pub order_filled_collector_status: Option<String>,
    pub order_filled_logs_appended: usize,
    pub engine_mode_status: Option<String>,
    pub engine_mode_blockers: Vec<String>,
    pub status_page_attempted: bool,
    pub status_page_status: Option<String>,
    pub status_page_blockers: Vec<String>,
    pub accounting_snapshot_attempted: bool,
    pub accounting_snapshot_status: Option<String>,
    pub accounting_snapshot_blockers: Vec<String>,
    pub finality_status: Option<String>,
    pub finality_confirmed_records: usize,
    pub finality_realized_terminal_records: usize,
    pub finality_onchain_matches: usize,
    pub finality_user_channel_matches: usize,
    pub settlement_hazard_status: Option<String>,
    pub settlement_hazard_recent_records: usize,
    pub settlement_hazard_failed_receipts: usize,
    pub settlement_hazard_revert_rate: f64,
    pub state: DiagnosticsDaemonState,
    pub reports_written: Vec<String>,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DiagnosticsDaemonState {
    pub runs: u64,
    pub consecutive_blocked_runs: u64,
    pub last_run_at: Option<String>,
    pub last_status: Option<String>,
    pub joined_positive_sample_runs: u64,
    pub last_joined_positive_sample_at: Option<String>,
    pub last_confirmed_records: usize,
    pub last_onchain_matches: usize,
    pub last_user_channel_matches: usize,
}

pub async fn run_no_submit_diagnostics_daemon_once(
    http: &Client,
    config: &Config,
) -> Result<DiagnosticsDaemonReport> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;

    let mut blockers = Vec::new();
    let mut reports_written = Vec::new();

    match user_channel::write_live_route_replay_labels_from_user_events(config) {
        Ok(written) => {
            if written == 0 {
                push_unique(&mut blockers, "user_channel_replay_labels_missing");
            }
        }
        Err(err) => push_unique(
            &mut blockers,
            format!("user_channel_replay_labels_failed:{err:#}"),
        ),
    }

    match onchain_fills::write_order_filled_collector_report(config) {
        Ok(path) => reports_written.push(path.display().to_string()),
        Err(err) => push_unique(
            &mut blockers,
            format!("order_filled_collector_report_failed:{err:#}"),
        ),
    }

    let mut order_filled_collector_attempted = false;
    let mut order_filled_collector_status = None;
    let mut order_filled_logs_appended = 0usize;
    if config.onchain_order_filled_collector_enabled {
        order_filled_collector_attempted = true;
        match onchain_fills::collect_recent_order_filled_logs(http, config).await {
            Ok(report) => {
                order_filled_collector_status = Some(report.status.clone());
                order_filled_logs_appended = report.logs_appended;
                reports_written.push(report.report_path);
                for blocker in report.blockers {
                    push_unique(&mut blockers, format!("order_filled_collector:{blocker}"));
                }
            }
            Err(err) => push_unique(
                &mut blockers,
                format!("order_filled_collector_failed:{err:#}"),
            ),
        }
    } else {
        push_unique(
            &mut blockers,
            "ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED=false",
        );
    }

    let mut settlement_hazard_status = None;
    let mut settlement_hazard_recent_records = 0usize;
    let mut settlement_hazard_failed_receipts = 0usize;
    let mut settlement_hazard_revert_rate = 0.0;
    match settlement_monitor::write_settlement_hazard_report(config) {
        Ok(path) => {
            reports_written.push(path.display().to_string());
            match settlement_monitor::build_settlement_hazard_report(config) {
                Ok(report) => {
                    settlement_hazard_status = Some(report.status.clone());
                    settlement_hazard_recent_records = report.recent_records;
                    settlement_hazard_failed_receipts = report.failed_receipts;
                    settlement_hazard_revert_rate = report.revert_rate;
                    for blocker in report.blockers {
                        push_unique(&mut blockers, format!("settlement_hazard:{blocker}"));
                    }
                }
                Err(err) => push_unique(
                    &mut blockers,
                    format!("settlement_hazard_build_failed:{err:#}"),
                ),
            }
        }
        Err(err) => push_unique(
            &mut blockers,
            format!("settlement_hazard_report_failed:{err:#}"),
        ),
    }

    let mut engine_mode_status = None;
    let mut engine_mode_blockers = Vec::new();
    let mut status_page_attempted = false;
    let mut status_page_status = None;
    let mut status_page_blockers = Vec::new();
    if config.live_status_page_enabled {
        status_page_attempted = true;
        match engine_mode::poll_status_page_summary(http, config).await {
            Ok(Some(report)) => {
                status_page_status = Some(report.status.clone());
                status_page_blockers = report.blockers.clone();
                for blocker in &status_page_blockers {
                    push_unique(&mut blockers, format!("status_page:{blocker}"));
                }
            }
            Ok(None) => {
                status_page_status = Some("clear".to_string());
            }
            Err(err) => push_unique(&mut blockers, format!("status_page_failed:{err:#}")),
        }
    } else {
        push_unique(&mut blockers, "LIVE_STATUS_PAGE_ENABLED=false");
    }

    let mut accounting_snapshot_attempted = false;
    let mut accounting_snapshot_status = None;
    let mut accounting_snapshot_blockers = Vec::new();
    if config.live_accounting_snapshot_enabled {
        accounting_snapshot_attempted = true;
        match live_executor::configured_live_account_address(config) {
            Ok(account_address) => {
                match accounting_snapshot::fetch_and_write_accounting_snapshot_report(
                    http,
                    config,
                    account_address,
                )
                .await
                {
                    Ok(report) => {
                        accounting_snapshot_status = Some(report.status.clone());
                        accounting_snapshot_blockers = report.blockers.clone();
                        reports_written.push(
                            config
                                .diagnostics_dir
                                .join(accounting_snapshot::ACCOUNTING_SNAPSHOT_REPORT_FILE)
                                .display()
                                .to_string(),
                        );
                        for blocker in &accounting_snapshot_blockers {
                            push_unique(&mut blockers, format!("accounting_snapshot:{blocker}"));
                        }
                    }
                    Err(err) => {
                        push_unique(&mut blockers, format!("accounting_snapshot_failed:{err:#}"))
                    }
                }
            }
            Err(err) => {
                accounting_snapshot_status = Some("unavailable".to_string());
                push_unique(
                    &mut blockers,
                    format!("accounting_snapshot_account_unavailable:{err:#}"),
                );
            }
        }
    } else {
        accounting_snapshot_status = Some("disabled".to_string());
    }

    match engine_mode::write_engine_mode_report(config) {
        Ok(path) => {
            reports_written.push(path.display().to_string());
            match engine_mode::build_engine_mode_report(config) {
                Ok(report) => {
                    engine_mode_status = Some(report.status);
                    engine_mode_blockers = report.blockers;
                    for blocker in &engine_mode_blockers {
                        push_unique(&mut blockers, format!("engine_mode:{blocker}"));
                    }
                }
                Err(err) => push_unique(&mut blockers, format!("engine_mode_build_failed:{err:#}")),
            }
        }
        Err(err) => push_unique(&mut blockers, format!("engine_mode_report_failed:{err:#}")),
    }

    let mut rfq_shadow_run_attempted = false;
    let mut rfq_shadow_status = None;
    let mut rfq_shadow_messages_seen = 0usize;
    if config.combo_rfq_stream_enabled {
        rfq_shadow_run_attempted = true;
        match rfq_stream_client::run_combo_rfq_wss_shadow_session(
            config,
            128,
            Duration::from_millis(config.combo_rfq_quote_max_age_ms.saturating_mul(2).max(1)),
        )
        .await
        {
            Ok(report) => {
                rfq_shadow_status = Some(report.status.clone());
                rfq_shadow_messages_seen = report.raw_messages_seen;
                for blocker in report.blockers {
                    push_unique(&mut blockers, format!("rfq_shadow:{blocker}"));
                }
            }
            Err(err) => push_unique(&mut blockers, format!("rfq_shadow_failed:{err:#}")),
        }
    } else {
        match rfq_stream_client::write_combo_rfq_shadow_session_report(config) {
            Ok(path) => reports_written.push(path.display().to_string()),
            Err(err) => push_unique(&mut blockers, format!("rfq_shadow_report_failed:{err:#}")),
        }
        push_unique(&mut blockers, "COMBO_RFQ_STREAM_ENABLED=false");
    }

    match rfq_stream_client::write_combo_rfq_stream_report(config) {
        Ok(path) => reports_written.push(path.display().to_string()),
        Err(err) => push_unique(&mut blockers, format!("rfq_stream_report_failed:{err:#}")),
    }

    let mut finality_status = None;
    let mut finality_confirmed_records = 0usize;
    let mut finality_realized_terminal_records = 0usize;
    let mut finality_onchain_matches = 0usize;
    let mut finality_user_channel_matches = 0usize;
    match rfq_finality::write_combo_rfq_finality_report(config) {
        Ok(path) => {
            reports_written.push(path.display().to_string());
            match rfq_finality::build_combo_rfq_finality_report(config) {
                Ok(report) => {
                    finality_status = Some(report.status.clone());
                    finality_confirmed_records = report.confirmed_records;
                    finality_realized_terminal_records = report.realized_terminal_records;
                    finality_onchain_matches =
                        report.onchain_order_filled.matched_confirmed_records;
                    finality_user_channel_matches = report.user_channel.matched_confirmed_records;
                    for blocker in report.blockers {
                        push_unique(&mut blockers, format!("finality:{blocker}"));
                    }
                }
                Err(err) => push_unique(&mut blockers, format!("finality_build_failed:{err:#}")),
            }
        }
        Err(err) => push_unique(&mut blockers, format!("finality_report_failed:{err:#}")),
    }

    if finality_confirmed_records == 0 {
        push_unique(&mut blockers, "daemon_missing_confirmed_finality");
    }
    if finality_realized_terminal_records == 0 {
        push_unique(&mut blockers, "daemon_missing_realized_ev");
    }
    if finality_onchain_matches == 0 {
        push_unique(&mut blockers, "daemon_missing_onchain_match");
    }
    if finality_user_channel_matches == 0 {
        push_unique(&mut blockers, "daemon_missing_user_channel_match");
    }

    let generated_at = Utc::now().to_rfc3339();
    let status = if blockers.is_empty() {
        "ready_for_shadow_samples".to_string()
    } else {
        "blocked_no_submit".to_string()
    };
    let state = update_diagnostics_daemon_state(
        config,
        &generated_at,
        &status,
        finality_confirmed_records,
        finality_realized_terminal_records,
        finality_onchain_matches,
        finality_user_channel_matches,
    )?;

    let report = DiagnosticsDaemonReport {
        generated_at,
        mode: "no_submit_one_pass".to_string(),
        live_submissions_enabled: false,
        rfq_shadow_run_attempted,
        rfq_shadow_status,
        rfq_shadow_messages_seen,
        order_filled_collector_attempted,
        order_filled_collector_status,
        order_filled_logs_appended,
        engine_mode_status,
        engine_mode_blockers,
        status_page_attempted,
        status_page_status,
        status_page_blockers,
        accounting_snapshot_attempted,
        accounting_snapshot_status,
        accounting_snapshot_blockers,
        finality_status,
        finality_confirmed_records,
        finality_realized_terminal_records,
        finality_onchain_matches,
        finality_user_channel_matches,
        settlement_hazard_status,
        settlement_hazard_recent_records,
        settlement_hazard_failed_receipts,
        settlement_hazard_revert_rate,
        state,
        reports_written,
        status,
        blockers,
    };

    let path = write_diagnostics_daemon_report(config, &report)?;
    if !report.reports_written.contains(&path.display().to_string()) {
        let mut report = report;
        report.reports_written.push(path.display().to_string());
        write_diagnostics_daemon_report(config, &report)?;
        Ok(report)
    } else {
        Ok(report)
    }
}

fn update_diagnostics_daemon_state(
    config: &Config,
    generated_at: &str,
    status: &str,
    confirmed_records: usize,
    realized_terminal_records: usize,
    onchain_matches: usize,
    user_channel_matches: usize,
) -> Result<DiagnosticsDaemonState> {
    let mut state = read_diagnostics_daemon_state(config)?;
    state.runs = state.runs.saturating_add(1);
    state.last_run_at = Some(generated_at.to_string());
    state.last_status = Some(status.to_string());
    state.last_confirmed_records = confirmed_records;
    state.last_onchain_matches = onchain_matches;
    state.last_user_channel_matches = user_channel_matches;
    if status == "blocked_no_submit" {
        state.consecutive_blocked_runs = state.consecutive_blocked_runs.saturating_add(1);
    } else {
        state.consecutive_blocked_runs = 0;
    }
    if confirmed_records > 0
        && realized_terminal_records > 0
        && onchain_matches > 0
        && user_channel_matches > 0
    {
        state.joined_positive_sample_runs = state.joined_positive_sample_runs.saturating_add(1);
        state.last_joined_positive_sample_at = Some(generated_at.to_string());
    }
    write_diagnostics_daemon_state(config, &state)?;
    Ok(state)
}

fn read_diagnostics_daemon_state(config: &Config) -> Result<DiagnosticsDaemonState> {
    let path = config.diagnostics_dir.join(DIAGNOSTICS_DAEMON_STATE_FILE);
    if !path.exists() {
        return Ok(DiagnosticsDaemonState::default());
    }
    let body = fs::read_to_string(&path)
        .with_context(|| format!("reading diagnostics daemon state {}", path.display()))?;
    serde_json::from_str(&body)
        .with_context(|| format!("parsing diagnostics daemon state {}", path.display()))
}

fn write_diagnostics_daemon_state(
    config: &Config,
    state: &DiagnosticsDaemonState,
) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(DIAGNOSTICS_DAEMON_STATE_FILE);
    fs::write(&path, serde_json::to_string_pretty(state)?)
        .with_context(|| format!("writing diagnostics daemon state {}", path.display()))?;
    Ok(path)
}

pub fn write_diagnostics_daemon_report(
    config: &Config,
    report: &DiagnosticsDaemonReport,
) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(DIAGNOSTICS_DAEMON_REPORT_FILE);
    fs::write(&path, serde_json::to_string_pretty(report)?)
        .with_context(|| format!("writing diagnostics daemon report {}", path.display()))?;
    Ok(path)
}

fn push_unique(blockers: &mut Vec<String>, blocker: impl Into<String>) {
    let blocker = blocker.into();
    if !blockers.contains(&blocker) {
        blockers.push(blocker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use httpmock::prelude::*;

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        std::env::temp_dir().join(format!("polymarket-diagnostics-daemon-{name}-{suffix}"))
    }

    #[tokio::test]
    async fn daemon_one_pass_is_no_submit_and_writes_report_when_disabled() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_dir("disabled");
        cfg.combo_rfq_stream_enabled = false;
        cfg.onchain_order_filled_collector_enabled = false;
        cfg.live_status_page_enabled = false;
        cfg.live_accounting_snapshot_enabled = false;

        let report = run_no_submit_diagnostics_daemon_once(&Client::new(), &cfg)
            .await
            .unwrap();

        assert_eq!(report.mode, "no_submit_one_pass");
        assert!(!report.live_submissions_enabled);
        assert!(!report.rfq_shadow_run_attempted);
        assert!(!report.order_filled_collector_attempted);
        assert!(!report.status_page_attempted);
        assert_eq!(report.status, "blocked_no_submit");
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_STREAM_ENABLED=false".to_string()));
        assert!(report
            .blockers
            .contains(&"ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED=false".to_string()));
        assert!(!report
            .blockers
            .contains(&"live_submissions_disabled".to_string()));
        assert!(cfg
            .diagnostics_dir
            .join(DIAGNOSTICS_DAEMON_REPORT_FILE)
            .exists());
        assert!(cfg
            .diagnostics_dir
            .join(DIAGNOSTICS_DAEMON_STATE_FILE)
            .exists());
        assert_eq!(report.state.runs, 1);
        assert_eq!(report.state.consecutive_blocked_runs, 1);
        assert_eq!(
            report.state.last_status.as_deref(),
            Some("blocked_no_submit")
        );

        let second_report = run_no_submit_diagnostics_daemon_once(&Client::new(), &cfg)
            .await
            .unwrap();
        assert_eq!(second_report.state.runs, 2);
        assert_eq!(second_report.state.consecutive_blocked_runs, 2);
    }

    #[tokio::test]
    async fn daemon_records_status_page_engine_mode_blocker() {
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
        cfg.diagnostics_dir = temp_dir("status-page");
        cfg.polymarket_status_api_url = format!("{}/v3/summary.json", server.base_url());
        cfg.live_status_page_enabled = true;
        cfg.live_accounting_snapshot_enabled = false;
        cfg.combo_rfq_stream_enabled = false;
        cfg.onchain_order_filled_collector_enabled = false;

        let report = run_no_submit_diagnostics_daemon_once(&Client::new(), &cfg)
            .await
            .unwrap();

        assert!(report.status_page_attempted);
        assert_eq!(report.status_page_status.as_deref(), Some("blocked"));
        assert_eq!(
            report.status_page_blockers,
            vec!["status_page_active_incident".to_string()]
        );
        assert!(report
            .engine_mode_blockers
            .contains(&"status_page_active_incident".to_string()));
        status.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn daemon_records_status_component_engine_mode_blocker() {
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
        cfg.diagnostics_dir = temp_dir("status-components");
        cfg.polymarket_status_api_url = format!("{}/v3/summary.json", server.base_url());
        cfg.polymarket_status_components_api_url =
            format!("{}/v3/components.json", server.base_url());
        cfg.live_status_page_enabled = true;
        cfg.live_accounting_snapshot_enabled = false;
        cfg.combo_rfq_stream_enabled = false;
        cfg.onchain_order_filled_collector_enabled = false;

        let report = run_no_submit_diagnostics_daemon_once(&Client::new(), &cfg)
            .await
            .unwrap();

        assert!(report.status_page_attempted);
        assert_eq!(report.status_page_status.as_deref(), Some("blocked"));
        assert_eq!(
            report.status_page_blockers,
            vec!["status_component_not_operational".to_string()]
        );
        assert!(report
            .engine_mode_blockers
            .contains(&"status_component_not_operational".to_string()));
        summary.assert_calls_async(1).await;
        components.assert_calls_async(1).await;
    }
}
