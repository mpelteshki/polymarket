use crate::config::Config;
use crate::engine_mode;
use chrono::Utc;
use polymarket_client_sdk_v2::contract_config;
use serde::Serialize;
use url::Url;

const POLYGON_CHAIN_ID: u64 = 137;
const EXPECTED_CLOB_EIP712_NAME: &str = "Polymarket CTF Exchange";
const EXPECTED_CLOB_EIP712_VERSION: &str = "2";
const EXPECTED_CLOB_API_URL: &str = "https://clob.polymarket.com";
const EXPECTED_CLOB_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const EXPECTED_CLOB_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const EXPECTED_COMBO_RFQ_API_URL: &str = "https://combos-rfq-api.polymarket.sh";
const EXPECTED_COMBO_RFQ_GATEWAY_WSS_URLS: &[&str] = crate::config::COMBO_RFQ_GATEWAY_WSS_URLS;
const EXPECTED_STANDARD_EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
const EXPECTED_NEG_RISK_EXCHANGE_V2: &str = "0xe2222d279d744050d28e00520010520000310F59";
const EXPECTED_CONDITIONAL_TOKENS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const EXPECTED_PUSD_COLLATERAL: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
const EXPECTED_CTF_COLLATERAL_ADAPTER: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";
const EXPECTED_NEG_RISK_CTF_COLLATERAL_ADAPTER: &str = "0xadA2005600Dec949baf300f4C6120000bDB6eAab";
const EXPECTED_COMBO_EXCHANGE_V3: &str = "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa";
const CONTRACTS_DOC_URL: &str = "https://docs.polymarket.com/resources/contracts";
const CLOB_DOC_URL: &str = "https://docs.polymarket.com/developers/CLOB/introduction";
const COMBOS_DOC_URL: &str = "https://docs.polymarket.com/market-makers/combos";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProtocolDriftReport {
    pub generated_at: String,
    pub status: String,
    pub source_urls: Vec<String>,
    pub checks: Vec<ProtocolDriftCheck>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProtocolDriftCheck {
    pub key: String,
    pub state: String,
    pub expected: Option<String>,
    pub observed: Option<String>,
    pub source_url: Option<String>,
    pub detail: String,
}

impl ProtocolDriftCheck {
    fn ready(
        key: impl Into<String>,
        expected: impl Into<String>,
        observed: impl Into<String>,
    ) -> Self {
        let expected = expected.into();
        let observed = observed.into();
        Self {
            key: key.into(),
            state: "ready".into(),
            expected: Some(expected.clone()),
            observed: Some(observed),
            source_url: None,
            detail: format!("matches_expected:{expected}"),
        }
    }

    fn blocked(
        key: impl Into<String>,
        expected: impl Into<String>,
        observed: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            state: "blocked".into(),
            expected: Some(expected.into()),
            observed: Some(observed.into()),
            source_url: None,
            detail: detail.into(),
        }
    }

    fn unknown(key: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            state: "unknown".into(),
            expected: None,
            observed: None,
            source_url: None,
            detail: detail.into(),
        }
    }

    fn with_source(mut self, source_url: &'static str) -> Self {
        self.source_url = Some(source_url.into());
        self
    }
}

pub fn build_protocol_drift_report(config: &Config) -> ProtocolDriftReport {
    let mut checks = Vec::new();
    checks.push(
        ProtocolDriftCheck::ready(
            "clob_eip712_name",
            EXPECTED_CLOB_EIP712_NAME,
            EXPECTED_CLOB_EIP712_NAME,
        )
        .with_source(CLOB_DOC_URL),
    );
    checks.push(
        ProtocolDriftCheck::ready(
            "clob_eip712_version",
            EXPECTED_CLOB_EIP712_VERSION,
            EXPECTED_CLOB_EIP712_VERSION,
        )
        .with_source(CLOB_DOC_URL),
    );
    checks.extend(clob_endpoint_drift_checks(config));
    checks.extend(combo_endpoint_drift_checks(config));
    checks.extend(sdk_contract_drift_checks(config));
    checks.push(combo_exchange_v3_config_check(config));
    checks.push(signature_type_check(config));
    checks.push(order_version_mismatch_check(config));

    let blockers = checks
        .iter()
        .filter(|check| check.state == "blocked")
        .map(|check| format!("{}:{}", check.key, check.detail))
        .collect::<Vec<_>>();
    ProtocolDriftReport {
        generated_at: Utc::now().to_rfc3339(),
        status: if !blockers.is_empty() {
            "blocked".into()
        } else if checks.iter().any(|check| check.state == "unknown") {
            "unknown".into()
        } else {
            "ready".into()
        },
        source_urls: vec![
            CONTRACTS_DOC_URL.into(),
            CLOB_DOC_URL.into(),
            COMBOS_DOC_URL.into(),
        ],
        checks,
        blockers,
    }
}

fn clob_endpoint_drift_checks(config: &Config) -> Vec<ProtocolDriftCheck> {
    vec![
        endpoint_check(
            "clob_api_url",
            EXPECTED_CLOB_API_URL,
            &config.clob_api_url,
            CLOB_DOC_URL,
        ),
        endpoint_check(
            "clob_ws_url",
            EXPECTED_CLOB_WS_URL,
            &config.clob_ws_url,
            CLOB_DOC_URL,
        ),
        endpoint_check(
            "clob_user_ws_url",
            EXPECTED_CLOB_USER_WS_URL,
            &config.clob_user_ws_url,
            CLOB_DOC_URL,
        ),
    ]
}

fn combo_endpoint_drift_checks(config: &Config) -> Vec<ProtocolDriftCheck> {
    vec![
        endpoint_check(
            "combo_rfq_api_url",
            EXPECTED_COMBO_RFQ_API_URL,
            &config.combo_rfq_api_url,
            COMBOS_DOC_URL,
        ),
        endpoint_check_one_of(
            "combo_rfq_gateway_wss_url",
            EXPECTED_COMBO_RFQ_GATEWAY_WSS_URLS,
            &config.combo_rfq_gateway_wss_url,
            COMBOS_DOC_URL,
        ),
    ]
}

fn endpoint_check_one_of(
    key: impl Into<String>,
    expected: &'static [&'static str],
    observed: &str,
    source_url: &'static str,
) -> ProtocolDriftCheck {
    let key = key.into();
    let mut expected_endpoints = Vec::new();
    for endpoint in expected {
        match normalize_endpoint(endpoint) {
            Ok(normalized) => expected_endpoints.push(normalized),
            Err(err) => {
                return ProtocolDriftCheck::unknown(
                    key,
                    format!("expected_endpoint_invalid:{err} expected={endpoint}"),
                )
                .with_source(source_url);
            }
        }
    }
    let observed_endpoint = match normalize_endpoint(observed) {
        Ok(endpoint) => endpoint,
        Err(err) => {
            return ProtocolDriftCheck::blocked(
                key,
                expected_endpoints.join(","),
                observed.trim().to_string(),
                format!("clob_endpoint_invalid:{err} observed={}", observed.trim()),
            )
            .with_source(source_url);
        }
    };
    let expected_display = expected_endpoints.join(",");
    if expected_endpoints
        .iter()
        .any(|expected| expected == &observed_endpoint)
    {
        ProtocolDriftCheck {
            key,
            state: "ready".into(),
            expected: Some(expected_display.clone()),
            observed: Some(observed_endpoint),
            source_url: None,
            detail: format!("matches_expected_one_of:{expected_display}"),
        }
        .with_source(source_url)
    } else {
        ProtocolDriftCheck::blocked(
            key,
            expected_display.clone(),
            observed_endpoint.clone(),
            format!("clob_endpoint_drift expected_one_of={expected_display} observed={observed_endpoint}"),
        )
        .with_source(source_url)
    }
}

fn endpoint_check(
    key: impl Into<String>,
    expected: &'static str,
    observed: &str,
    source_url: &'static str,
) -> ProtocolDriftCheck {
    let key = key.into();
    let expected_endpoint = match normalize_endpoint(expected) {
        Ok(endpoint) => endpoint,
        Err(err) => {
            return ProtocolDriftCheck::unknown(
                key,
                format!("expected_endpoint_invalid:{err} expected={expected}"),
            )
            .with_source(source_url);
        }
    };
    let observed_endpoint = match normalize_endpoint(observed) {
        Ok(endpoint) => endpoint,
        Err(err) => {
            return ProtocolDriftCheck::blocked(
                key,
                expected_endpoint,
                observed.trim().to_string(),
                format!("clob_endpoint_invalid:{err} observed={}", observed.trim()),
            )
            .with_source(source_url);
        }
    };
    if observed_endpoint == expected_endpoint {
        ProtocolDriftCheck::ready(key, expected_endpoint, observed_endpoint).with_source(source_url)
    } else {
        ProtocolDriftCheck::blocked(
            key,
            expected_endpoint.clone(),
            observed_endpoint.clone(),
            format!(
                "clob_endpoint_drift expected={expected_endpoint} observed={observed_endpoint}"
            ),
        )
        .with_source(source_url)
    }
}

fn normalize_endpoint(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("empty_endpoint".into());
    }
    let url = Url::parse(trimmed).map_err(|err| err.to_string())?;
    let host = url
        .host_str()
        .ok_or_else(|| "missing_host".to_string())?
        .to_ascii_lowercase();
    if url.query().is_some() || url.fragment().is_some() {
        return Err("query_or_fragment_not_allowed".into());
    }
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    let path = url.path().trim_end_matches('/');
    let path = if path.is_empty() { "" } else { path };
    Ok(format!("{}://{}{}{}", url.scheme(), host, port, path))
}

fn sdk_contract_drift_checks(config: &Config) -> Vec<ProtocolDriftCheck> {
    if config.live_chain_id != POLYGON_CHAIN_ID {
        return vec![ProtocolDriftCheck::blocked(
            "contract_chain_id",
            POLYGON_CHAIN_ID.to_string(),
            config.live_chain_id.to_string(),
            format!("unsupported_live_chain_id={}", config.live_chain_id),
        )
        .with_source(CONTRACTS_DOC_URL)];
    }

    let mut checks = Vec::new();
    checks.extend(contract_config_checks(
        false,
        EXPECTED_STANDARD_EXCHANGE_V2,
        "standard",
    ));
    checks.extend(contract_config_checks(
        true,
        EXPECTED_NEG_RISK_EXCHANGE_V2,
        "neg_risk",
    ));
    checks.push(
        ProtocolDriftCheck::ready(
            "ctf_collateral_adapter",
            EXPECTED_CTF_COLLATERAL_ADAPTER,
            EXPECTED_CTF_COLLATERAL_ADAPTER,
        )
        .with_source(CONTRACTS_DOC_URL),
    );
    checks.push(
        ProtocolDriftCheck::ready(
            "neg_risk_ctf_collateral_adapter",
            EXPECTED_NEG_RISK_CTF_COLLATERAL_ADAPTER,
            EXPECTED_NEG_RISK_CTF_COLLATERAL_ADAPTER,
        )
        .with_source(CONTRACTS_DOC_URL),
    );
    checks
}

fn contract_config_checks(
    neg_risk: bool,
    expected_exchange_v2: &'static str,
    label: &'static str,
) -> Vec<ProtocolDriftCheck> {
    let Some(contract) = contract_config(POLYGON_CHAIN_ID, neg_risk) else {
        return vec![ProtocolDriftCheck::blocked(
            format!("{label}_sdk_contract_config"),
            "present",
            "missing",
            "sdk_contract_config_missing",
        )
        .with_source(CONTRACTS_DOC_URL)];
    };
    let mut checks = Vec::new();
    checks.push(address_check(
        format!("{label}_exchange_v2"),
        expected_exchange_v2,
        contract
            .exchange_v2
            .map(|address| address.to_string())
            .unwrap_or_else(|| "<missing>".into()),
        CONTRACTS_DOC_URL,
    ));
    checks.push(address_check(
        format!("{label}_conditional_tokens"),
        EXPECTED_CONDITIONAL_TOKENS,
        contract.conditional_tokens.to_string(),
        CONTRACTS_DOC_URL,
    ));
    checks.push(address_check(
        format!("{label}_pusd_collateral"),
        EXPECTED_PUSD_COLLATERAL,
        contract.collateral.to_string(),
        CONTRACTS_DOC_URL,
    ));
    checks
}

fn combo_exchange_v3_config_check(config: &Config) -> ProtocolDriftCheck {
    let observed = config.combo_rfq_exchange_v3_address.trim();
    if observed.is_empty() {
        return ProtocolDriftCheck::unknown(
            "combo_exchange_v3",
            "COMBO_RFQ_EXCHANGE_V3_ADDRESS_empty; promotion gate must still block",
        )
        .with_source(CONTRACTS_DOC_URL);
    }
    address_check(
        "combo_exchange_v3",
        EXPECTED_COMBO_EXCHANGE_V3,
        observed.to_string(),
        CONTRACTS_DOC_URL,
    )
}

fn signature_type_check(config: &Config) -> ProtocolDriftCheck {
    match config.live_signature_type {
        0..=3 => ProtocolDriftCheck::ready(
            "live_signature_type",
            "0|1|2|3",
            config.live_signature_type.to_string(),
        )
        .with_source(CLOB_DOC_URL),
        other => ProtocolDriftCheck::blocked(
            "live_signature_type",
            "0|1|2|3",
            other.to_string(),
            "unsupported_signature_type; expected EOA, Proxy, Safe, or Poly1271 deposit-wallet signature type",
        )
        .with_source(CLOB_DOC_URL),
    }
}

fn order_version_mismatch_check(config: &Config) -> ProtocolDriftCheck {
    match engine_mode::build_engine_mode_report(config) {
        Ok(report)
            if report
                .state
                .last_error_code
                .as_deref()
                .is_some_and(contains_order_version_mismatch)
                || report
                    .state
                    .last_error_message
                    .as_deref()
                    .is_some_and(contains_order_version_mismatch) =>
        {
            ProtocolDriftCheck::blocked(
                "clob_order_version",
                EXPECTED_CLOB_EIP712_VERSION,
                "order_version_mismatch",
                "recent_clob_order_version_mismatch_observed",
            )
            .with_source(CLOB_DOC_URL)
        }
        Ok(_) => ProtocolDriftCheck::ready(
            "clob_order_version",
            EXPECTED_CLOB_EIP712_VERSION,
            EXPECTED_CLOB_EIP712_VERSION,
        )
        .with_source(CLOB_DOC_URL),
        Err(err) => ProtocolDriftCheck::unknown(
            "clob_order_version",
            format!("engine_mode_report_unavailable:{err}"),
        )
        .with_source(CLOB_DOC_URL),
    }
}

fn contains_order_version_mismatch(value: &str) -> bool {
    value
        .to_ascii_lowercase()
        .contains("order_version_mismatch")
        || value
            .to_ascii_lowercase()
            .contains("order version mismatch")
}

fn address_check(
    key: impl Into<String>,
    expected: &'static str,
    observed: impl Into<String>,
    source_url: &'static str,
) -> ProtocolDriftCheck {
    let observed = observed.into();
    if addresses_equal(expected, &observed) {
        ProtocolDriftCheck::ready(key, expected, observed).with_source(source_url)
    } else {
        ProtocolDriftCheck::blocked(
            key,
            expected,
            observed.clone(),
            format!("protocol_address_drift expected={expected} observed={observed}"),
        )
        .with_source(source_url)
    }
}

fn addresses_equal(expected: &str, observed: &str) -> bool {
    expected.trim().eq_ignore_ascii_case(observed.trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_mode;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "polymarket-protocol-drift-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn protocol_drift_report_matches_expected_polygon_contracts() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = POLYGON_CHAIN_ID;
        cfg.clob_api_url = EXPECTED_CLOB_API_URL.into();
        cfg.clob_ws_url = EXPECTED_CLOB_WS_URL.into();
        cfg.clob_user_ws_url = EXPECTED_CLOB_USER_WS_URL.into();
        cfg.combo_rfq_api_url = EXPECTED_COMBO_RFQ_API_URL.into();
        cfg.combo_rfq_gateway_wss_url = crate::config::DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL.into();
        cfg.combo_rfq_exchange_v3_address = EXPECTED_COMBO_EXCHANGE_V3.into();
        cfg.diagnostics_dir = temp_dir("ready");

        let report = build_protocol_drift_report(&cfg);

        assert_eq!(report.status, "ready");
        assert!(report.blockers.is_empty());
        assert!(report.source_urls.contains(&CONTRACTS_DOC_URL.to_string()));
        assert!(report.source_urls.contains(&CLOB_DOC_URL.to_string()));
        assert!(report.source_urls.contains(&COMBOS_DOC_URL.to_string()));
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.key == "standard_exchange_v2")
                .map(|check| check.state.as_str()),
            Some("ready")
        );
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.key == "clob_eip712_version")
                .and_then(|check| check.expected.as_deref()),
            Some(EXPECTED_CLOB_EIP712_VERSION)
        );
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.key == "combo_rfq_gateway_wss_url")
                .and_then(|check| check.source_url.as_deref()),
            Some(COMBOS_DOC_URL)
        );
    }

    #[test]
    fn protocol_drift_blocks_noncanonical_clob_endpoints() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = POLYGON_CHAIN_ID;
        cfg.clob_api_url = "http://clob.polymarket.com".into();
        cfg.clob_ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/old".into();
        cfg.clob_user_ws_url = "wss://example.com/ws/user".into();
        cfg.combo_rfq_api_url = EXPECTED_COMBO_RFQ_API_URL.into();
        cfg.combo_rfq_gateway_wss_url = crate::config::DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL.into();
        cfg.combo_rfq_exchange_v3_address = EXPECTED_COMBO_EXCHANGE_V3.into();
        cfg.diagnostics_dir = temp_dir("endpoint-drift");

        let report = build_protocol_drift_report(&cfg);

        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("clob_api_url:clob_endpoint_drift")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("clob_ws_url:clob_endpoint_drift")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("clob_user_ws_url:clob_endpoint_drift")));
    }

    #[test]
    fn protocol_drift_blocks_stale_combo_endpoints() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = POLYGON_CHAIN_ID;
        cfg.combo_rfq_api_url = "https://combos-rfq-api.polymarket.com".into();
        cfg.combo_rfq_gateway_wss_url =
            "wss://combos-rfq-gateway-quoter.polymarket.com/ws/rfq".into();
        cfg.combo_rfq_exchange_v3_address = EXPECTED_COMBO_EXCHANGE_V3.into();
        cfg.diagnostics_dir = temp_dir("combo-endpoint-drift");

        let report = build_protocol_drift_report(&cfg);

        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_rfq_api_url:clob_endpoint_drift")));
        assert!(!report
            .blockers
            .iter()
            .any(|blocker| { blocker.contains("combo_rfq_gateway_wss_url:clob_endpoint_drift") }));
    }

    #[test]
    fn protocol_drift_allows_signature_type_3_but_blocks_combo_exchange_mismatch() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = POLYGON_CHAIN_ID;
        cfg.combo_rfq_api_url = EXPECTED_COMBO_RFQ_API_URL.into();
        cfg.combo_rfq_gateway_wss_url = crate::config::DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL.into();
        cfg.combo_rfq_exchange_v3_address = "0x0000000000000000000000000000000000000003".into();
        cfg.live_signature_type = 3;
        cfg.diagnostics_dir = temp_dir("blocked");

        let report = build_protocol_drift_report(&cfg);

        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("combo_exchange_v3:protocol_address_drift")));
        assert!(!report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("live_signature_type:unsupported_signature_type")));
    }

    #[test]
    fn protocol_drift_blocks_recent_order_version_mismatch_observation() {
        let mut cfg = Config::from_env();
        cfg.live_chain_id = POLYGON_CHAIN_ID;
        cfg.combo_rfq_api_url = EXPECTED_COMBO_RFQ_API_URL.into();
        cfg.combo_rfq_gateway_wss_url = crate::config::DEFAULT_COMBO_RFQ_GATEWAY_WSS_URL.into();
        cfg.combo_rfq_exchange_v3_address = EXPECTED_COMBO_EXCHANGE_V3.into();
        cfg.diagnostics_dir = temp_dir("order-version");
        engine_mode::observe_http_response(
            &cfg,
            "test",
            "POST /order",
            400,
            None,
            Some(r#"{"code":"order_version_mismatch"}"#),
        )
        .unwrap();

        let report = build_protocol_drift_report(&cfg);

        assert_eq!(report.status, "blocked");
        assert!(report.blockers.iter().any(|blocker| blocker
            .contains("clob_order_version:recent_clob_order_version_mismatch_observed")));
    }
}
