//! Polymarket fee calculator.
//!
//! Implements the Polymarket taker fee model:
//!     fee = C x feeRate x (p x (1 - p))^feeExponent
//!
//! Where:
//!     feeRate = effective taker fee coefficient for the market
//!     feeExponent = market-specific exponent from CLOB `fd.e`
//!     C       = number of shares traded
//!     p       = share price (0 to 1)
//!
//! Makers pay zero. Arbitrage execution is modeled as taker flow.

use crate::config::Config;
use crate::models::{Market, MAX_SUPPORTED_CLOB_FEE_EXPONENT};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClobFeeSchedule {
    pub rate: f64,
    pub exponent: u32,
}

/// Calculate the fee per share for the documented CLOB fee curve.
pub fn fee_per_share_with_curve(price: f64, fee_rate: f64, exponent: u32) -> f64 {
    if price <= 0.0
        || price >= 1.0
        || fee_rate <= 0.0
        || exponent == 0
        || exponent > MAX_SUPPORTED_CLOB_FEE_EXPONENT
    {
        return 0.0;
    }
    fee_rate * (price * (1.0 - price)).powi(exponent as i32)
}

/// Calculate the fee per share at a given price.
pub fn fee_per_share(price: f64, fee_rate: f64) -> f64 {
    fee_per_share_with_curve(price, fee_rate, 1)
}

/// Calculate and protocol-round a fee for the documented CLOB fee curve.
pub fn total_fee_with_curve(price: f64, num_shares: f64, fee_rate: f64, exponent: u32) -> f64 {
    round_fee_usd(fee_per_share_with_curve(price, fee_rate, exponent) * num_shares)
}

/// Calculate total fee for a trade.
pub fn total_fee(price: f64, num_shares: f64, fee_rate: f64) -> f64 {
    total_fee_with_curve(price, num_shares, fee_rate, 1)
}

fn round_fee_usd(fee: f64) -> f64 {
    if !fee.is_finite() || fee <= 0.0 {
        return 0.0;
    }
    let rounded = (fee * 100_000.0).round() / 100_000.0;
    if rounded < 0.00001 {
        0.0
    } else {
        rounded
    }
}

fn normalized_market_fee_rate(raw: Option<f64>) -> Option<f64> {
    let raw = raw?;
    if !raw.is_finite() || raw < 0.0 {
        return None;
    }
    if raw <= 1.0 {
        Some(raw)
    } else {
        Some(raw / 10_000.0)
    }
}

/// Return the authoritative CLOB taker fee schedule from `fd` metadata.
pub fn verified_clob_fee_schedule(market: &Market) -> Option<ClobFeeSchedule> {
    let rate = market.clob_fee_rate?;
    let exponent = market.clob_fee_exponent?;
    if !rate.is_finite()
        || !(0.0..=1.0).contains(&rate)
        || exponent == 0
        || exponent > MAX_SUPPORTED_CLOB_FEE_EXPONENT
    {
        return None;
    }
    Some(ClobFeeSchedule { rate, exponent })
}

/// Calculate a fill fee only when authoritative CLOB fee metadata is present.
pub fn total_fee_from_clob_metadata(price: f64, num_shares: f64, market: &Market) -> Option<f64> {
    let schedule = verified_clob_fee_schedule(market)?;
    Some(total_fee_with_curve(
        price,
        num_shares,
        schedule.rate,
        schedule.exponent,
    ))
}

/// Effective taker fee rate for a market.
///
/// Preference order:
/// 1. CLOB fee details (`fd.r`) when present.
/// 2. Explicit fees-disabled flag from Gamma.
/// 3. Normalized Gamma-side taker fee.
/// 4. Legacy CLOB bps metadata fallback for scan-only estimates.
/// 5. Category-level fallback from config.
pub fn effective_fee_rate(market: &Market, category: &str, config: &Config) -> f64 {
    if let Some(rate) = normalized_market_fee_rate(market.clob_fee_rate) {
        return rate;
    }

    if matches!(market.fees_enabled, Some(false)) {
        return 0.0;
    }

    if let Some(rate) = normalized_market_fee_rate(market.taker_fee_rate) {
        return rate;
    }

    if let Some(bps) = market.clob_taker_fee_bps {
        let rate = bps as f64 / 10_000.0;
        if rate.is_finite() && rate >= 0.0 {
            return rate;
        }
    }

    config.fee_theta(category)
}

pub fn market_fee_curve_supported(market: &Market) -> bool {
    market.supports_standard_fee_curve()
}

pub fn total_fee_for_market(
    price: f64,
    num_shares: f64,
    market: &Market,
    category: &str,
    config: &Config,
) -> f64 {
    if let Some(schedule) = verified_clob_fee_schedule(market) {
        return total_fee_with_curve(price, num_shares, schedule.rate, schedule.exponent);
    }
    total_fee(
        price,
        num_shares,
        effective_fee_rate(market, category, config),
    )
}

/// Calculate total fees for an arbitrage trade across specific markets.
pub fn arbitrage_fees_for_markets(
    markets: &[Market],
    market_prices: &[f64],
    num_shares: f64,
    category: &str,
    config: &Config,
) -> f64 {
    markets
        .iter()
        .zip(market_prices.iter().copied())
        .map(|(market, price)| total_fee_for_market(price, num_shares, market, category, config))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market() -> Market {
        Market {
            question: "Q".into(),
            condition_id: "C".into(),
            market_slug: "q".into(),
            clob_token_id_yes: "Y".into(),
            clob_token_id_no: "N".into(),
            gamma_yes_price: 0.5,
            gamma_no_price: 0.5,
            clob_yes_ask: None,
            clob_yes_bid: None,
            clob_no_ask: None,
            clob_no_bid: None,
            clob_yes_ask_size: None,
            clob_yes_bid_size: None,
            clob_no_ask_size: None,
            clob_no_bid_size: None,
            fees_enabled: Some(true),
            taker_fee_rate: None,
            maker_fee_rate: None,
            order_price_min_tick_size: None,
            order_min_size: None,
            clob_tick_size: None,
            clob_min_order_size: None,
            clob_taker_fee_bps: None,
            clob_fee_rate: None,
            clob_fee_exponent: None,
            clob_neg_risk: None,
            clob_rfq_enabled: None,
            liquidity: 1000.0,
            closed: false,
        }
    }

    #[test]
    fn test_fee_per_share_at_midpoint() {
        let fee = fee_per_share(0.50, 0.05);
        assert!((fee - 0.0125).abs() < 1e-10);
    }

    #[test]
    fn test_fee_per_share_at_extremes() {
        assert_eq!(fee_per_share(0.0, 0.05), 0.0);
        assert_eq!(fee_per_share(1.0, 0.05), 0.0);
    }

    #[test]
    fn test_fee_per_share_asymmetric() {
        let fee = fee_per_share(0.20, 0.05);
        assert!((fee - 0.008).abs() < 1e-10);
    }

    #[test]
    fn test_total_fee() {
        let fee = total_fee(0.50, 100.0, 0.05);
        assert!((fee - 1.25).abs() < 1e-10);
    }

    #[test]
    fn total_fee_with_curve_honors_clob_exponent() {
        let fee = total_fee_with_curve(0.50, 100.0, 0.05, 2);
        assert!((fee - 0.3125).abs() < 1e-10);
    }

    #[test]
    fn test_total_fee_rounds_to_documented_precision() {
        assert_eq!(round_fee_usd(0.000004), 0.0);
        assert_eq!(round_fee_usd(0.000009), 0.00001);
        assert_eq!(total_fee(0.01, 0.0101, 0.05), 0.0);
        assert_eq!(total_fee(0.01, 0.03031, 0.05), 0.00002);
    }

    #[test]
    fn test_effective_market_fee_rate_overrides_category() {
        let mut m = market();
        m.taker_fee_rate = Some(0.01);
        let cfg = Config::from_env();
        assert!((effective_fee_rate(&m, "crypto", &cfg) - 0.01).abs() < 1e-10);
    }

    #[test]
    fn test_effective_market_fee_prefers_clob_fee_details() {
        let mut m = market();
        m.fees_enabled = Some(false);
        m.taker_fee_rate = Some(0.0);
        m.clob_taker_fee_bps = Some(1_000);
        m.clob_fee_rate = Some(0.05);
        m.clob_fee_exponent = Some(1);
        let cfg = Config::from_env();
        assert!((effective_fee_rate(&m, "crypto", &cfg) - 0.05).abs() < 1e-10);
        assert_eq!(
            verified_clob_fee_schedule(&m),
            Some(ClobFeeSchedule {
                rate: 0.05,
                exponent: 1,
            })
        );
        assert_eq!(total_fee_from_clob_metadata(0.4, 20.0, &m), Some(0.24));
    }

    #[test]
    fn verified_clob_fee_does_not_use_legacy_or_gamma_fallbacks() {
        let mut m = market();
        m.taker_fee_rate = Some(0.0);
        m.clob_taker_fee_bps = Some(1_000);

        assert_eq!(total_fee_from_clob_metadata(0.5, 100.0, &m), None);

        m.clob_fee_rate = Some(0.05);
        m.clob_fee_exponent = Some(2);
        assert_eq!(
            verified_clob_fee_schedule(&m),
            Some(ClobFeeSchedule {
                rate: 0.05,
                exponent: 2,
            })
        );
    }

    #[test]
    fn test_effective_market_fee_respects_fees_disabled() {
        let mut m = market();
        m.fees_enabled = Some(false);
        m.taker_fee_rate = Some(0.04);
        let cfg = Config::from_env();
        assert_eq!(effective_fee_rate(&m, "politics", &cfg), 0.0);
    }
}
