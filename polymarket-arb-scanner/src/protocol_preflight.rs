//! Read-only live protocol capability diagnostics for blocked route candidates.

use crate::combo_rfq_client;
use crate::config::Config;
use crate::engine_mode;
use crate::execution_routes::{BlockedLiveRoutePlan, LiveRouteKind};
use crate::live_executor::{configured_live_account_address, live_arbitrage_routes_available};
use crate::protocol_drift;
use crate::rfq_stream_client;
use crate::user_channel;
use polymarket_client_sdk_v2::contract_config;
use polymarket_client_sdk_v2::types::Address;
use std::str::FromStr as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityState {
    Ready,
    Blocked,
    Unknown,
}

impl CapabilityState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Blocked => "blocked",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityStatus {
    pub key: &'static str,
    pub state: CapabilityState,
    pub detail: String,
}

impl CapabilityStatus {
    fn new(key: &'static str, state: CapabilityState, detail: impl Into<String>) -> Self {
        Self {
            key,
            state,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolPreflightReport {
    pub statuses: Vec<CapabilityStatus>,
}

impl ProtocolPreflightReport {
    #[cfg(test)]
    pub fn status(&self, key: &str) -> Option<&CapabilityStatus> {
        self.statuses.iter().find(|status| status.key == key)
    }

    pub fn note(&self) -> String {
        let statuses = self
            .statuses
            .iter()
            .map(|status| format!("{}:{}", status.key, status.state.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        let blockers = self
            .statuses
            .iter()
            .filter(|status| matches!(status.state, CapabilityState::Blocked))
            .map(|status| format!("{}({})", status.key, note_value(&status.detail)))
            .collect::<Vec<_>>();
        if blockers.is_empty() {
            format!("protocol_preflight={statuses}")
        } else {
            format!(
                "protocol_preflight={statuses} protocol_blockers={}",
                blockers.join(",")
            )
        }
    }
}

pub fn blocked_live_protocol_preflight(
    config: &Config,
    plan: &BlockedLiveRoutePlan,
) -> ProtocolPreflightReport {
    let mut statuses = vec![
        live_route_matrix_status(),
        account_identity_status(config),
        chain_status(config),
        contract_status(config, false, "standard_contract_config"),
        protocol_drift_status(config),
        user_channel_config_status(config),
        user_channel_ready_status(config),
        CapabilityStatus::new(
            "pusd_balance",
            CapabilityState::Unknown,
            "not_checked_in_blocked_live_diagnostics",
        ),
        CapabilityStatus::new(
            "pusd_allowance_exchange_v2",
            CapabilityState::Unknown,
            "not_checked_in_blocked_live_diagnostics",
        ),
        CapabilityStatus::new(
            "erc1155_operator_approval",
            CapabilityState::Unknown,
            "not_checked_in_blocked_live_diagnostics",
        ),
        CapabilityStatus::new(
            "native_pol_balance",
            CapabilityState::Unknown,
            "not_checked_in_blocked_live_diagnostics",
        ),
        CapabilityStatus::new(
            "route_snapshot_skew_limit",
            CapabilityState::Ready,
            format!("max_route_skew_ms={}", route_snapshot_skew_limit_ms(config)),
        ),
        engine_mode_status(config),
        execution_mode_circuit_breaker_status(plan),
    ];

    match plan.kind {
        LiveRouteKind::ComboRfqCandidate => {
            statuses.push(contract_status(config, true, "neg_risk_contract_config"));
            statuses.extend(combo_rfq_statuses(config));
        }
        LiveRouteKind::CtfMergeBundleCandidate => {
            statuses.push(ctf_primitive_status(
                config,
                "ctf_merge_primitive",
                "mergePositions",
            ));
            statuses.push(CapabilityStatus::new(
                "entry_atomicity",
                CapabilityState::Blocked,
                "no_atomic_two_leg_entry_fill_confirmation",
            ));
        }
        LiveRouteKind::CtfSplitSellCandidate => {
            statuses.push(ctf_primitive_status(
                config,
                "ctf_split_primitive",
                "splitPosition",
            ));
            statuses.push(CapabilityStatus::new(
                "sell_fill_atomicity",
                CapabilityState::Blocked,
                "no_atomic_dual_sell_fill_and_rollback_route",
            ));
        }
        LiveRouteKind::None => {
            statuses.push(CapabilityStatus::new(
                "route_supported",
                CapabilityState::Blocked,
                "no_live_route_candidate_for_opportunity_shape",
            ));
        }
    }

    ProtocolPreflightReport { statuses }
}

fn live_route_matrix_status() -> CapabilityStatus {
    if live_arbitrage_routes_available() {
        CapabilityStatus::new(
            "live_route_matrix",
            CapabilityState::Ready,
            "at_least_one_live_arbitrage_route_supported",
        )
    } else {
        CapabilityStatus::new(
            "live_route_matrix",
            CapabilityState::Blocked,
            "no_live_arbitrage_routes_supported",
        )
    }
}

fn account_identity_status(config: &Config) -> CapabilityStatus {
    match configured_live_account_address(config) {
        Ok(address) => CapabilityStatus::new(
            "account_identity",
            CapabilityState::Ready,
            format!("account={address}"),
        ),
        Err(err) => CapabilityStatus::new(
            "account_identity",
            CapabilityState::Blocked,
            format!("live_account_unavailable:{err}"),
        ),
    }
}

fn chain_status(config: &Config) -> CapabilityStatus {
    match config.live_chain_id {
        137 => CapabilityStatus::new("chain_id", CapabilityState::Ready, "polygon_mainnet"),
        80002 => CapabilityStatus::new(
            "chain_id",
            CapabilityState::Unknown,
            "amoy_testnet_not_live_profit_route",
        ),
        chain_id => CapabilityStatus::new(
            "chain_id",
            CapabilityState::Blocked,
            format!("unsupported_live_chain_id={chain_id}"),
        ),
    }
}

fn contract_status(config: &Config, neg_risk: bool, key: &'static str) -> CapabilityStatus {
    match contract_config(config.live_chain_id, neg_risk) {
        Some(contract) if contract.exchange_v2.is_some() => CapabilityStatus::new(
            key,
            CapabilityState::Ready,
            format!(
                "collateral={} conditional_tokens={} exchange_v2={}",
                contract.collateral,
                contract.conditional_tokens,
                contract.exchange_v2.expect("checked exchange_v2 presence")
            ),
        ),
        Some(_) => CapabilityStatus::new(
            key,
            CapabilityState::Blocked,
            format!(
                "sdk_contract_config_missing_exchange_v2 chain_id={} neg_risk={neg_risk}",
                config.live_chain_id
            ),
        ),
        None => CapabilityStatus::new(
            key,
            CapabilityState::Blocked,
            format!(
                "missing_sdk_contract_config chain_id={} neg_risk={neg_risk}",
                config.live_chain_id
            ),
        ),
    }
}

fn protocol_drift_status(config: &Config) -> CapabilityStatus {
    let report = protocol_drift::build_protocol_drift_report(config);
    match report.status.as_str() {
        "ready" => CapabilityStatus::new(
            "protocol_drift",
            CapabilityState::Ready,
            format!(
                "no_protocol_drift_detected source_count={}",
                report.source_urls.len()
            ),
        ),
        "blocked" => CapabilityStatus::new(
            "protocol_drift",
            CapabilityState::Blocked,
            format!("protocol_drift_blockers={}", report.blockers.join("|")),
        ),
        _ => CapabilityStatus::new(
            "protocol_drift",
            CapabilityState::Unknown,
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

fn user_channel_config_status(config: &Config) -> CapabilityStatus {
    match user_channel::ensure_live_user_channel_configured(config) {
        Ok(()) => CapabilityStatus::new(
            "user_channel_config",
            CapabilityState::Ready,
            "authenticated_user_channel_configured",
        ),
        Err(err) => CapabilityStatus::new(
            "user_channel_config",
            CapabilityState::Blocked,
            format!("user_channel_not_configured:{err}"),
        ),
    }
}

fn user_channel_ready_status(config: &Config) -> CapabilityStatus {
    match user_channel::ensure_live_user_channel_ready(config) {
        Ok(()) => CapabilityStatus::new(
            "user_channel_ready",
            CapabilityState::Ready,
            "fresh_authenticated_user_channel_status",
        ),
        Err(err) => CapabilityStatus::new(
            "user_channel_ready",
            CapabilityState::Blocked,
            format!("user_channel_not_ready:{err}"),
        ),
    }
}

fn ctf_primitive_status(
    config: &Config,
    key: &'static str,
    primitive: &'static str,
) -> CapabilityStatus {
    match contract_config(config.live_chain_id, false) {
        Some(contract) => CapabilityStatus::new(
            key,
            CapabilityState::Ready,
            format!(
                "{primitive}_available conditional_tokens={} collateral={}",
                contract.conditional_tokens, contract.collateral
            ),
        ),
        None => CapabilityStatus::new(
            key,
            CapabilityState::Blocked,
            format!(
                "{primitive}_unavailable_missing_contract_config chain_id={}",
                config.live_chain_id
            ),
        ),
    }
}

fn combo_rfq_statuses(config: &Config) -> Vec<CapabilityStatus> {
    let requester = combo_rfq_client::combo_rfq_requester_config_report(config);
    vec![
        CapabilityStatus::new(
            "combo_rfq_catalog",
            if config.combo_rfq_discovery_enabled {
                CapabilityState::Ready
            } else {
                CapabilityState::Unknown
            },
            if config.combo_rfq_discovery_enabled {
                "read_only_catalog_discovery_enabled"
            } else {
                "read_only_catalog_discovery_disabled"
            },
        ),
        CapabilityStatus::new(
            "rfq_requester_api",
            if requester.blockers.is_empty() {
                CapabilityState::Ready
            } else {
                CapabilityState::Blocked
            },
            if requester.blockers.is_empty() {
                format!("requester_api_configured api_url={}", requester.api_url)
            } else {
                format!("requester_api_blocked:{}", requester.blockers.join("|"))
            },
        ),
        CapabilityStatus::new(
            "rfq_quote_window",
            CapabilityState::Ready,
            format!(
                "quote_window_ms={} accept_window_ms=5000 last_look_ms=1000",
                config.combo_rfq_quote_max_age_ms
            ),
        ),
        rfq_stream_status(config),
        exchange_v3_approval_status(config),
    ]
}

fn rfq_stream_status(config: &Config) -> CapabilityStatus {
    let report = rfq_stream_client::combo_rfq_stream_config_report(config);
    if report.blockers.is_empty() {
        CapabilityStatus::new(
            "rfq_stream_client",
            CapabilityState::Unknown,
            format!(
                "stream_configured gateway_wss_url={} transport={} transport_start_required",
                report.gateway_wss_url, report.transport
            ),
        )
    } else {
        CapabilityStatus::new(
            "rfq_stream_client",
            CapabilityState::Blocked,
            format!("stream_blocked:{}", report.blockers.join("|")),
        )
    }
}

fn exchange_v3_approval_status(config: &Config) -> CapabilityStatus {
    let raw = config.combo_rfq_exchange_v3_address.trim();
    if raw.is_empty() {
        return CapabilityStatus::new(
            "exchange_v3_approval",
            CapabilityState::Blocked,
            "COMBO_RFQ_EXCHANGE_V3_ADDRESS_empty",
        );
    }
    match Address::from_str(raw) {
        Ok(address) => CapabilityStatus::new(
            "exchange_v3_approval",
            CapabilityState::Unknown,
            format!("exchange_v3_address={address} allowance_probe_not_run"),
        ),
        Err(err) => CapabilityStatus::new(
            "exchange_v3_approval",
            CapabilityState::Blocked,
            format!("COMBO_RFQ_EXCHANGE_V3_ADDRESS_invalid:{err}"),
        ),
    }
}

fn execution_mode_circuit_breaker_status(plan: &BlockedLiveRoutePlan) -> CapabilityStatus {
    CapabilityStatus::new(
        "execution_mode_circuit_breaker",
        CapabilityState::Blocked,
        format!(
            "not_proven_clear route={} requires_clob_market_info_live_orderable=true no_itode_delay no_matching_engine_restart no_post_only no_cancel_only no_rate_limit_pause closed_only=false",
            plan.kind.as_str()
        ),
    )
}

fn engine_mode_status(config: &Config) -> CapabilityStatus {
    match engine_mode::build_engine_mode_report(config) {
        Ok(report) if report.active => CapabilityStatus::new(
            "clob_engine_mode",
            CapabilityState::Blocked,
            format!(
                "mode={} blockers={}",
                report.state.mode.as_str(),
                report.blockers.join("|")
            ),
        ),
        Ok(report) if report.state.observations == 0 => CapabilityStatus::new(
            "clob_engine_mode",
            CapabilityState::Unknown,
            "no_engine_mode_observations",
        ),
        Ok(report) => CapabilityStatus::new(
            "clob_engine_mode",
            CapabilityState::Ready,
            format!(
                "mode={} status={}",
                report.state.mode.as_str(),
                report.status
            ),
        ),
        Err(err) => CapabilityStatus::new(
            "clob_engine_mode",
            CapabilityState::Blocked,
            format!("engine_mode_report_unavailable:{err}"),
        ),
    }
}

fn route_snapshot_skew_limit_ms(config: &Config) -> u64 {
    config
        .live_max_refresh_to_submit_ms
        .max(config.ws_quote_max_age_ms)
        .max(250)
}

fn note_value(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '=' | '@') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_routes::LiveRouteStatus;

    fn plan(kind: LiveRouteKind) -> BlockedLiveRoutePlan {
        BlockedLiveRoutePlan {
            kind,
            status: LiveRouteStatus::DryRunOnly,
            planned_legs: 2,
            unique_conditions: 2,
            reason: "test".into(),
            steps: Vec::new(),
            combo_report: None,
        }
    }

    #[test]
    fn preflight_reports_rfq_blockers() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 137;
        cfg.live_max_refresh_to_submit_ms = 1_000;
        cfg.ws_quote_max_age_ms = 10;
        cfg.combo_rfq_discovery_enabled = true;

        let report = blocked_live_protocol_preflight(&cfg, &plan(LiveRouteKind::ComboRfqCandidate));

        assert_eq!(
            report
                .status("rfq_requester_api")
                .map(|status| status.state),
            Some(CapabilityState::Blocked)
        );
        assert_eq!(
            report
                .status("exchange_v3_approval")
                .map(|status| status.state),
            Some(CapabilityState::Unknown)
        );
        assert_eq!(
            report
                .status("route_snapshot_skew_limit")
                .map(|status| status.state),
            Some(CapabilityState::Ready)
        );
        assert_eq!(
            report.status("protocol_drift").map(|status| status.state),
            Some(CapabilityState::Ready)
        );
        assert_eq!(
            report
                .status("rfq_stream_client")
                .map(|status| status.state),
            Some(CapabilityState::Blocked)
        );
        let execution_mode = report.status("execution_mode_circuit_breaker").unwrap();
        assert_eq!(execution_mode.state, CapabilityState::Blocked);
        assert!(execution_mode.detail.contains("no_post_only"));
        assert!(execution_mode.detail.contains("no_itode_delay"));
        let note = report.note();
        assert!(note.contains("protocol_preflight="));
        assert!(note.contains("rfq_requester_api:blocked"));
        assert!(note.contains("rfq_stream_client:blocked"));
        assert!(note.contains("execution_mode_circuit_breaker:blocked"));
        assert!(note.contains("exchange_v3_approval:unknown"));
        assert!(note.contains("user_channel_config:blocked"));
        assert!(note.contains("user_channel_ready:blocked"));
    }

    #[test]
    fn preflight_marks_configured_exchange_v3_allowance_probe_unknown() {
        let mut cfg = Config::from_env();
        cfg.combo_rfq_exchange_v3_address = "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa".into();
        cfg.combo_rfq_stream_enabled = true;
        cfg.combo_rfq_gateway_wss_url = crate::config::DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL.into();
        cfg.combo_rfq_grpc_url.clear();
        cfg.combo_rfq_stream_bearer_token = "token".into();
        cfg.combo_rfq_participant_id = "participant".into();

        let report = blocked_live_protocol_preflight(&cfg, &plan(LiveRouteKind::ComboRfqCandidate));

        let status = report.status("exchange_v3_approval").unwrap();
        assert_eq!(status.state, CapabilityState::Unknown);
        assert!(status.detail.contains("exchange_v3_address="));
        assert!(status.detail.contains("allowance_probe_not_run"));
        assert_eq!(
            report
                .status("rfq_stream_client")
                .map(|status| status.state),
            Some(CapabilityState::Unknown)
        );
        assert_eq!(
            report.status("protocol_drift").map(|status| status.state),
            Some(CapabilityState::Ready)
        );
    }

    #[test]
    fn preflight_blocks_unsupported_chain_contracts() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = 1;

        let report =
            blocked_live_protocol_preflight(&cfg, &plan(LiveRouteKind::CtfMergeBundleCandidate));

        assert_eq!(
            report.status("chain_id").map(|status| status.state),
            Some(CapabilityState::Blocked)
        );
        assert_eq!(
            report
                .status("standard_contract_config")
                .map(|status| status.state),
            Some(CapabilityState::Blocked)
        );
        assert_eq!(
            report
                .status("ctf_merge_primitive")
                .map(|status| status.state),
            Some(CapabilityState::Blocked)
        );
        assert!(report.note().contains("standard_contract_config:blocked"));
    }
}
