//! Real-time gas cost estimator for Polygon PoS.
//!
//! Fetches the current gas price from the official Polygon Gas Station v2 API
//! and the live POL/USD price from Binance, then converts them into a USD
//! cost estimate for executing a Polymarket arbitrage trade.
//!
//! # Architecture
//!
//! Trades on Polymarket's CLOB are settled on-chain via `CTFExchange.fillOrders`.
//! EOA users still need to account for gas, while proxy/safe flows may be
//! effectively gasless when routed through Polymarket's relayer.  This module
//! therefore computes an EOA-style upper bound and lets higher layers zero it
//! out when a gasless signature flow is configured:
//!
//!   gas_cost_usd = (gas_limit × max_fee_per_gas_gwei × 1e-9) × pol_price_usd
//!
//! ## Gas limit per leg
//!
//! This module uses **175 000 gas** as a conservative per-leg heuristic for an
//! on-chain taker fill path. Real gas can be lower or higher depending on the
//! account model, relayer path, contract state, and batch composition, so higher
//! layers should treat the result as a risk-control estimate rather than a
//! guaranteed realized cost.
//!
//! ## Gas price
//!
//! We use the `fast` tier from Polygon Gas Station v2 (`maxFee`) which is the
//! `baseFee + maxPriorityFee`.  This guarantees timely inclusion even during
//! moderate network spikes.  All values are in Gwei (1e-9 POL).
//!
//! ## POL price
//!
//! Fetch from Binance's `/api/v3/ticker/price` endpoint.
//! No API key is required for this public tier and it has much higher rate limits.
//!
//! ## Caching
//!
//! Both values are cached for `CACHE_TTL_SECS` to avoid hammering external APIs
//! on every scan cycle.

use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, warn};

// ───────────────────────────── Constants ──────────────────────────────────────

/// Gas limit per trade leg used as a conservative heuristic for ROI gating.
pub const GAS_LIMIT_PER_LEG: u64 = 175_000;

/// How long to keep a fetched gas/price estimate before refreshing (seconds).
const CACHE_TTL_SECS: u64 = 60;

/// Polygon Gas Station v2 — returns `fast.maxFee` in Gwei (EIP-1559).
const GAS_STATION_URL: &str = "https://gasstation.polygon.technology/v2";

/// Binance public ticker API (no API key required).
const BINANCE_URL: &str = "https://api.binance.com/api/v3/ticker/price?symbol=POLUSDT";

// ───────────────────────── API response shapes ────────────────────────────────

#[derive(Debug, Deserialize)]
struct GasStationResponse {
    fast: GasTier,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GasTier {
    /// EIP-1559 maxFeePerGas in Gwei (baseFee + maxPriorityFee).
    max_fee: f64,
}

#[derive(Debug, Deserialize)]
struct BinanceTickerResponse {
    price: String,
}

// ───────────────────────────── Cached state ───────────────────────────────────

#[derive(Debug, Clone)]
pub struct GasSnapshot {
    /// EIP-1559 maxFeePerGas in Gwei for the `fast` tier.
    pub max_fee_gwei: f64,
    /// POL (MATIC) price in USD.
    pub pol_usd: f64,
    pub fetched_at: Instant,
}

impl GasSnapshot {
    fn is_fresh(&self) -> bool {
        self.fetched_at.elapsed() < Duration::from_secs(CACHE_TTL_SECS)
    }

    pub fn cost_usd(&self, gas_units: u64) -> f64 {
        let gwei = self.max_fee_gwei * gas_units as f64;
        let pol = gwei * 1e-9; // 1 Gwei = 1e-9 POL
        pol * self.pol_usd
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GasEstimateSource {
    Refreshed,
    FreshCache,
    StaleCache,
    Fallback,
}

impl GasEstimateSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Refreshed => "refreshed",
            Self::FreshCache => "fresh_cache",
            Self::StaleCache => "stale_cache",
            Self::Fallback => "fallback",
        }
    }

    pub fn is_fresh_oracle_backed(self) -> bool {
        matches!(self, Self::Refreshed | Self::FreshCache)
    }
}

#[derive(Debug, Clone)]
pub struct GasEstimate {
    pub cost_usd: f64,
    pub source: GasEstimateSource,
    pub legs: usize,
}

// ─────────────────────────── Public interface ─────────────────────────────────

/// Shared gas oracle; clone the `Arc` to share across tasks.
#[derive(Clone, Debug)]
pub struct GasOracle {
    inner: Arc<Mutex<Option<GasSnapshot>>>,
}

impl GasOracle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn trade_cost_usd(&self, client: &Client, num_legs: usize, fallback_usd: f64) -> f64 {
        self.trade_cost_estimate_usd(client, num_legs, fallback_usd)
            .await
            .cost_usd
    }

    pub async fn trade_cost_estimate_usd(
        &self,
        client: &Client,
        num_legs: usize,
        fallback_usd: f64,
    ) -> GasEstimate {
        let leg_count = num_legs.max(1);
        let mut guard = self.inner.lock().await;
        let mut source = match guard.as_ref() {
            Some(snapshot) if snapshot.is_fresh() => GasEstimateSource::FreshCache,
            Some(_) => GasEstimateSource::StaleCache,
            None => GasEstimateSource::Fallback,
        };

        // Refresh if stale or absent
        if guard.as_ref().is_none_or(|s| !s.is_fresh()) {
            match fetch_snapshot(client).await {
                Ok(snapshot) => {
                    debug!(
                        max_fee_gwei = snapshot.max_fee_gwei,
                        pol_usd = snapshot.pol_usd,
                        "Gas snapshot refreshed"
                    );
                    *guard = Some(snapshot);
                    source = GasEstimateSource::Refreshed;
                }
                Err(err) => {
                    warn!("Gas oracle fetch failed: {err}. Using fallback ${fallback_usd:.4}.");
                }
            }
        }

        let cost_usd = match guard.as_ref() {
            Some(snapshot) => {
                let gas_units = GAS_LIMIT_PER_LEG * leg_count as u64;
                snapshot.cost_usd(gas_units)
            }
            None => fallback_usd.max(0.0) * leg_count as f64,
        };

        GasEstimate {
            cost_usd,
            source,
            legs: leg_count,
        }
    }

    /// Convert an actual native POL gas spend into USD using the cached/fresh
    /// POL/USD snapshot. Falls back to the supplied USD value if price refresh
    /// is unavailable.
    pub async fn native_pol_cost_usd(
        &self,
        client: &Client,
        pol_amount: f64,
        fallback_usd: f64,
    ) -> f64 {
        if !pol_amount.is_finite() || pol_amount <= 0.0 {
            return 0.0;
        }

        let mut guard = self.inner.lock().await;
        if guard.as_ref().is_none_or(|s| !s.is_fresh()) {
            match fetch_snapshot(client).await {
                Ok(snapshot) => {
                    debug!(
                        max_fee_gwei = snapshot.max_fee_gwei,
                        pol_usd = snapshot.pol_usd,
                        "Gas snapshot refreshed"
                    );
                    *guard = Some(snapshot);
                }
                Err(err) => {
                    warn!("Gas oracle fetch failed: {err}. Using fallback ${fallback_usd:.4}.");
                }
            }
        }

        guard
            .as_ref()
            .map(|snapshot| pol_amount * snapshot.pol_usd)
            .unwrap_or(fallback_usd)
    }

    pub async fn snapshot_struct(&self) -> Option<GasSnapshot> {
        self.inner.lock().await.clone()
    }
}

// ─────────────────────────── Internal fetchers ───────────────────────────────

async fn fetch_snapshot(client: &Client) -> anyhow::Result<GasSnapshot> {
    let (gas_result, price_result) = tokio::join!(fetch_gas_price(client), fetch_pol_price(client));

    Ok(GasSnapshot {
        max_fee_gwei: gas_result?,
        pol_usd: price_result?,
        fetched_at: Instant::now(),
    })
}

async fn fetch_gas_price(client: &Client) -> anyhow::Result<f64> {
    let resp: GasStationResponse = client
        .get(GAS_STATION_URL)
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(resp.fast.max_fee)
}

async fn fetch_pol_price(client: &Client) -> anyhow::Result<f64> {
    let resp: BinanceTickerResponse = client
        .get(BINANCE_URL)
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let price = resp.price.parse::<f64>()?;
    Ok(price)
}

// ───────────────────────────────── Tests ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_calculation_is_correct() {
        let snapshot = GasSnapshot {
            max_fee_gwei: 100.0, // 100 Gwei maxFee
            pol_usd: 0.50,       // POL at $0.50
            fetched_at: Instant::now(),
        };

        // 1 leg: 175_000 gas × 100 Gwei × 1e-9 × $0.50
        //   = 0.0175 POL × $0.50 = $0.00875
        let cost = snapshot.cost_usd(GAS_LIMIT_PER_LEG);
        assert!((cost - 0.00875).abs() < 1e-8, "unexpected cost: {cost}");
    }

    #[test]
    fn cost_scales_linearly_with_legs() {
        let snapshot = GasSnapshot {
            max_fee_gwei: 100.0,
            pol_usd: 1.0,
            fetched_at: Instant::now(),
        };

        let one_leg = snapshot.cost_usd(GAS_LIMIT_PER_LEG);
        let three_legs = snapshot.cost_usd(GAS_LIMIT_PER_LEG * 3);
        assert!((three_legs - one_leg * 3.0).abs() < 1e-10);
    }

    #[test]
    fn snapshot_freshness() {
        let fresh = GasSnapshot {
            max_fee_gwei: 50.0,
            pol_usd: 0.30,
            fetched_at: Instant::now(),
        };
        assert!(fresh.is_fresh());
    }

    #[tokio::test]
    async fn native_pol_cost_usd_uses_cached_price() {
        let client = Client::new();
        let oracle = GasOracle::new();
        {
            let mut guard = oracle.inner.lock().await;
            *guard = Some(GasSnapshot {
                max_fee_gwei: 100.0,
                pol_usd: 0.50,
                fetched_at: Instant::now(),
            });
        }

        let gas = oracle.native_pol_cost_usd(&client, 0.02, 99.0).await;

        assert!((gas - 0.01).abs() < 1e-10);
    }

    #[tokio::test]
    async fn trade_cost_fallback_scales_with_leg_count() {
        let client = Client::builder()
            .no_proxy()
            .resolve(
                "gasstation.polygon.technology",
                "127.0.0.1:9".parse().unwrap(),
            )
            .build()
            .unwrap();
        let oracle = GasOracle::new();

        let gas = oracle.trade_cost_usd(&client, 3, 0.05).await;

        assert!((gas - 0.15).abs() < 1e-10);
    }

    #[tokio::test]
    async fn trade_cost_estimate_reports_fallback_source() {
        let client = Client::builder()
            .no_proxy()
            .resolve(
                "gasstation.polygon.technology",
                "127.0.0.1:9".parse().unwrap(),
            )
            .build()
            .unwrap();
        let oracle = GasOracle::new();

        let estimate = oracle.trade_cost_estimate_usd(&client, 2, 0.05).await;

        assert_eq!(estimate.source, GasEstimateSource::Fallback);
        assert!(!estimate.source.is_fresh_oracle_backed());
        assert_eq!(estimate.legs, 2);
        assert!((estimate.cost_usd - 0.10).abs() < 1e-10);
    }

    #[tokio::test]
    async fn trade_cost_estimate_reports_fresh_cache_source() {
        let client = Client::new();
        let oracle = GasOracle::new();
        {
            let mut guard = oracle.inner.lock().await;
            *guard = Some(GasSnapshot {
                max_fee_gwei: 100.0,
                pol_usd: 1.0,
                fetched_at: Instant::now(),
            });
        }

        let estimate = oracle.trade_cost_estimate_usd(&client, 1, 0.05).await;

        assert_eq!(estimate.source, GasEstimateSource::FreshCache);
        assert!(estimate.source.is_fresh_oracle_backed());
    }
}
