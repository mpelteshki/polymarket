use anyhow::{bail, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::info;

use crate::config::Config;

#[derive(Debug, Deserialize)]
struct GeoblockResponse {
    blocked: bool,
    ip: Option<String>,
    country: Option<String>,
    region: Option<String>,
}

pub async fn ensure_live_geoblock_allows_trading(
    client: &Client,
    config: &Config,
    phase: &str,
) -> Result<()> {
    let response = client
        .get("https://polymarket.com/api/geoblock")
        .timeout(std::time::Duration::from_secs(
            config.api_timeout_secs.max(1),
        ))
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("Live geoblock {phase} failed: {err}"))?
        .error_for_status()
        .map_err(|err| anyhow::anyhow!("Live geoblock {phase} returned error status: {err}"))?
        .json::<GeoblockResponse>()
        .await
        .map_err(|err| anyhow::anyhow!("Live geoblock {phase} parse failed: {err}"))?;

    ensure_response_allows_trading(response, phase)
}

fn ensure_response_allows_trading(geo: GeoblockResponse, phase: &str) -> Result<()> {
    if geo.blocked {
        bail!(
            "Live geoblock {phase} blocked trading for ip={} country={} region={}",
            geo.ip.as_deref().unwrap_or("unknown"),
            geo.country.as_deref().unwrap_or("unknown"),
            geo.region.as_deref().unwrap_or("unknown"),
        );
    }

    info!(
        "Live geoblock {phase} passed: country={} region={}",
        geo.country.as_deref().unwrap_or("unknown"),
        geo.region.as_deref().unwrap_or("unknown"),
    );
    Ok(())
}

#[cfg(test)]
pub(crate) fn payload_is_blocked(payload: &str) -> bool {
    serde_json::from_str::<GeoblockResponse>(payload)
        .map(|geo| geo.blocked)
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geoblock_payload_blocks_live_when_blocked_or_invalid() {
        assert!(payload_is_blocked(
            r#"{"blocked":true,"country":"US","region":"NY"}"#
        ));
        assert!(!payload_is_blocked(
            r#"{"blocked":false,"country":"CA","region":"ON"}"#
        ));
        assert!(payload_is_blocked("not json"));
    }

    #[test]
    fn blocked_response_fails_closed_with_location() {
        let err = ensure_response_allows_trading(
            GeoblockResponse {
                blocked: true,
                ip: Some("203.0.113.1".into()),
                country: Some("US".into()),
                region: Some("NY".into()),
            },
            "pre-submit",
        )
        .unwrap_err();

        assert!(err.to_string().contains("pre-submit blocked trading"));
        assert!(err.to_string().contains("country=US"));
    }
}
