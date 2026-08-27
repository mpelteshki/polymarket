//! Data models for the Polymarket arbitrage scanner.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

pub const EXTERNAL_TOKEN_PREFIX: &str = "external:";
pub const MAX_SUPPORTED_CLOB_FEE_EXPONENT: u32 = 16;

pub fn is_external_token_id(token_id: &str) -> bool {
    token_id.starts_with(EXTERNAL_TOKEN_PREFIX)
}

/// Type of arbitrage opportunity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArbType {
    Yes,
    No,
    Bundle,
    MintSell,
    Ranked,
}

impl fmt::Display for ArbType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArbType::Yes => write!(f, "YES"),
            ArbType::No => write!(f, "NO"),
            ArbType::Bundle => write!(f, "BUNDLE"),
            ArbType::MintSell => write!(f, "MINT_SELL"),
            ArbType::Ranked => write!(f, "RANKED"),
        }
    }
}

/// Side purchased on a given market leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeSide {
    Yes,
    No,
}

impl OutcomeSide {
    pub fn as_str(self) -> &'static str {
        match self {
            OutcomeSide::Yes => "yes",
            OutcomeSide::No => "no",
        }
    }
}

impl fmt::Display for OutcomeSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A single market (outcome) within a Polymarket event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub question: String,
    pub condition_id: String,
    pub market_slug: String,
    pub clob_token_id_yes: String,
    pub clob_token_id_no: String,

    /// Gamma API mid prices (indicative only)
    pub gamma_yes_price: f64,
    pub gamma_no_price: f64,

    /// CLOB API real prices (what you'd actually pay)
    pub clob_yes_ask: Option<f64>,
    pub clob_yes_bid: Option<f64>,
    pub clob_no_ask: Option<f64>,
    pub clob_no_bid: Option<f64>,
    pub clob_yes_ask_size: Option<f64>,
    pub clob_yes_bid_size: Option<f64>,
    pub clob_no_ask_size: Option<f64>,
    pub clob_no_bid_size: Option<f64>,

    /// Fee metadata from Gamma / CLOB.
    ///
    /// `taker_fee_rate` and `maker_fee_rate` are normalized rates (0.04 = 4%).
    /// `clob_taker_fee_bps` is the raw platform-fee basis-point representation from
    /// `/fee-rate`. Do not populate it from market-info `tbf`/`mbf`; CLOB V2 treats
    /// those fields as builder/legacy fee metadata, not the platform fee curve.
    pub fees_enabled: Option<bool>,
    pub taker_fee_rate: Option<f64>,
    pub maker_fee_rate: Option<f64>,
    pub clob_taker_fee_bps: Option<u32>,
    pub clob_fee_rate: Option<f64>,
    pub clob_fee_exponent: Option<u32>,

    /// Execution constraints surfaced by Gamma / CLOB market metadata.
    pub order_price_min_tick_size: Option<f64>,
    pub order_min_size: Option<f64>,
    pub clob_tick_size: Option<f64>,
    pub clob_min_order_size: Option<f64>,
    pub clob_neg_risk: Option<bool>,
    pub clob_rfq_enabled: Option<bool>,

    pub liquidity: f64,
    pub closed: bool,
}

impl Market {
    /// Get the best YES ask price, preferring CLOB when available.
    pub fn yes_ask(&self, use_clob: bool, spread_estimate: f64) -> f64 {
        if use_clob {
            if let Some(ask) = self.clob_yes_ask {
                return ask;
            }
        }
        (self.gamma_yes_price + spread_estimate).min(0.99)
    }

    /// Get the best NO ask price, preferring CLOB when available.
    pub fn no_ask(&self, use_clob: bool, spread_estimate: f64) -> f64 {
        if use_clob {
            if let Some(ask) = self.clob_no_ask {
                return ask;
            }
        }
        (self.gamma_no_price + spread_estimate).min(0.99)
    }

    /// Effective minimum tick size for executable prices.
    pub fn tick_size(&self) -> f64 {
        self.clob_tick_size
            .or(self.order_price_min_tick_size)
            .filter(|v| v.is_finite() && *v > 0.0 && *v <= 1.0)
            .unwrap_or(0.0001)
    }

    /// Effective minimum order size in shares, if known.
    pub fn min_order_size(&self) -> Option<f64> {
        self.clob_min_order_size
            .or(self.order_min_size)
            .filter(|v| v.is_finite() && *v > 0.0)
    }

    /// Convenience helper returning a numeric minimum order size in shares, defaulting to zero.
    pub fn effective_min_order_size(&self) -> f64 {
        self.min_order_size().unwrap_or(0.0)
    }

    /// Alias kept for clarity at call sites that compare share quantities.
    pub fn min_order_size_shares(&self) -> f64 {
        self.effective_min_order_size()
    }

    /// Returns true when the market has an executable YES quote and visible size.
    pub fn has_full_yes_quote(&self) -> bool {
        matches!((self.clob_yes_ask, self.clob_yes_ask_size), (Some(price), Some(size)) if price > 0.0 && size > 0.0)
    }

    /// Returns true when the market has an executable NO quote and visible size.
    pub fn has_full_no_quote(&self) -> bool {
        matches!((self.clob_no_ask, self.clob_no_ask_size), (Some(price), Some(size)) if price > 0.0 && size > 0.0)
    }

    /// Returns true when the market has a scan-time YES ask price.
    ///
    /// Scan-time discovery is allowed to treat a visible best ask price as actionable
    /// even when the latest quote feed did not include the displayed size. Paper/live
    /// execution still revalidates full depth immediately before trading.
    pub fn has_yes_price_quote(&self) -> bool {
        matches!(self.clob_yes_ask, Some(price) if price > 0.0)
    }

    /// Returns true when the market has a scan-time NO ask price.
    pub fn has_no_price_quote(&self) -> bool {
        matches!(self.clob_no_ask, Some(price) if price > 0.0)
    }

    /// Returns true when the market has executable YES and NO bid quotes with visible size.
    pub fn has_full_yes_no_bid_quotes(&self) -> bool {
        matches!(
            (
                self.clob_yes_bid,
                self.clob_yes_bid_size,
                self.clob_no_bid,
                self.clob_no_bid_size,
            ),
            (Some(yes_price), Some(yes_size), Some(no_price), Some(no_size))
                if yes_price > 0.0 && no_price > 0.0 && yes_size > 0.0 && no_size > 0.0
        )
    }

    /// Return the token id corresponding to an outcome side.
    pub fn token_id_for_outcome(&self, outcome: OutcomeSide) -> &str {
        match outcome {
            OutcomeSide::Yes => self.clob_token_id_yes.as_str(),
            OutcomeSide::No => self.clob_token_id_no.as_str(),
        }
    }

    /// Return true when the market has a scan-time price quote for the requested outcome side.
    pub fn has_price_quote_for_outcome(&self, outcome: OutcomeSide) -> bool {
        match outcome {
            OutcomeSide::Yes => self.has_yes_price_quote(),
            OutcomeSide::No => self.has_no_price_quote(),
        }
    }

    /// Return true when the market has an executable quote + visible size for the requested outcome side.
    pub fn has_full_quote_for_outcome(&self, outcome: OutcomeSide) -> bool {
        match outcome {
            OutcomeSide::Yes => self.has_full_yes_quote(),
            OutcomeSide::No => self.has_full_no_quote(),
        }
    }

    /// Return whether the advertised CLOB fee curve can be evaluated safely.
    pub fn supports_standard_fee_curve(&self) -> bool {
        match (self.clob_fee_rate, self.clob_fee_exponent) {
            (None, None) => true,
            (Some(rate), Some(exponent)) => {
                (1..=MAX_SUPPORTED_CLOB_FEE_EXPONENT).contains(&exponent)
                    && rate.is_finite()
                    && (0.0..=1.0).contains(&rate)
            }
            _ => false,
        }
    }
}

/// A Polymarket event containing multiple mutually-exclusive markets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_id: String,
    pub title: String,
    pub slug: String,
    pub category: String,
    pub enable_neg_risk: bool,
    pub neg_risk: bool,
    pub neg_risk_augmented: bool,
    pub lifecycle: EventLifecycle,
    pub markets: Vec<Market>,
}

/// Lifecycle and rule metadata used to avoid scanning stale or post-event markets.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventLifecycle {
    pub end_date: Option<DateTime<Utc>>,
    pub game_start_time: Option<DateTime<Utc>>,
    pub resolution_source: Option<String>,
    pub description: Option<String>,
    pub rules: Option<String>,
    pub uma_resolution_status: Option<String>,
}

impl EventLifecycle {
    pub fn capital_lock_hours_from(&self, now: DateTime<Utc>) -> Option<f64> {
        self.end_date.map(|end_date| {
            let millis = (end_date - now).num_milliseconds().max(0);
            millis as f64 / 3_600_000.0
        })
    }

    pub fn scan_block_reason(
        &self,
        now: DateTime<Utc>,
        pre_cutoff_buffer_secs: u64,
        game_start_quarantine_secs: u64,
    ) -> Option<String> {
        let mut candidates = Vec::new();
        if let Some(end_date) = self.end_date {
            candidates.push(("event end", end_date, pre_cutoff_buffer_secs));
        }
        if let Some(game_start_time) = self.game_start_time {
            candidates.push(("game start", game_start_time, game_start_quarantine_secs));
        }
        candidates.sort_by_key(|(_, cutoff, _)| *cutoff);

        for (label, cutoff, buffer) in candidates {
            let buffer_secs = buffer.min(i64::MAX as u64) as i64;
            let gate_at = cutoff - chrono::Duration::seconds(buffer_secs);
            if now < gate_at {
                continue;
            }

            if now >= cutoff {
                return Some(format!(
                    "event lifecycle {label} cutoff already passed at {}",
                    cutoff.to_rfc3339()
                ));
            }
            return Some(format!(
                "event lifecycle {label} cutoff {} is within configured buffer of {}s",
                cutoff.to_rfc3339(),
                buffer
            ));
        }

        None
    }

    pub fn merge_missing_or_earlier(&mut self, other: &EventLifecycle) {
        self.end_date = earlier_datetime(self.end_date, other.end_date);
        self.game_start_time = earlier_datetime(self.game_start_time, other.game_start_time);
        fill_missing_string(&mut self.resolution_source, &other.resolution_source);
        fill_missing_string(&mut self.description, &other.description);
        fill_missing_string(&mut self.rules, &other.rules);
        fill_missing_string(
            &mut self.uma_resolution_status,
            &other.uma_resolution_status,
        );
    }
}

fn earlier_datetime(
    current: Option<DateTime<Utc>>,
    incoming: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match (current, incoming) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn fill_missing_string(current: &mut Option<String>, incoming: &Option<String>) {
    if current
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        *current = incoming
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
    }
}

/// A family of ranked events grouping together contestants across mutually-exclusive rank positions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedFamily {
    pub id: String,
    pub title: String,
    pub category: String,
    pub markets: Vec<RankedMarketInstance>,
    pub contestants: Vec<String>,
    pub ranks: Vec<u32>,
}

/// A specific market acting as a `(contestant, rank)` leg in a ranked family.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedMarketInstance {
    pub contestant_id: usize,
    pub rank_idx: usize,
    pub market: Market,
}

/// One executable basket leg.
///
/// `unit_shares` expresses how many shares of this leg are bought for one basket unit.
/// YES/NO/bundle opportunities normally use `1.0`; ranked opportunities may use
/// optimizer-derived non-unity share counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityLeg {
    pub market_index: usize,
    pub question: String,
    pub market_slug: String,
    pub condition_id: String,
    pub token_id: String,
    pub outcome: OutcomeSide,
    pub unit_shares: f64,
    pub reference_price: f64,
}

/// A detected arbitrage opportunity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageOpportunity {
    pub event_title: String,
    pub event_id: String,
    pub category: String,
    pub arb_type: ArbType,
    pub markets: Vec<Market>,
    pub execution_plan: Vec<OpportunityLeg>,

    pub total_cost: f64,
    pub guaranteed_revenue: f64,
    pub gross_profit: f64,
    pub total_fees: f64,
    pub net_profit: f64,
    #[serde(default)]
    pub estimated_total_gas_cost_usd: f64,
    pub roi_pct: f64,

    pub prices_from_clob: bool,
    pub max_executable_size_usd: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capital_lock_hours: Option<f64>,
    pub expected_slippage_pct: f64,
    pub detected_at: DateTime<Utc>,
}

/// Validate the payoff shape used by plain YES/NO neg-risk arbitrage.
///
/// A supported YES/NO family buys exactly one matching side token for every
/// market in the event family. Single-leg or partially covered YES/NO plans are
/// not guaranteed arbitrage routes even if their projected accounting says so.
pub fn is_supported_yes_no_full_family_plan(opp: &ArbitrageOpportunity) -> bool {
    let expected_outcome = match opp.arb_type {
        ArbType::Yes => OutcomeSide::Yes,
        ArbType::No => OutcomeSide::No,
        _ => return false,
    };
    if opp.markets.len() < 2 || opp.execution_plan.len() != opp.markets.len() {
        return false;
    }

    let mut seen_indices = vec![false; opp.markets.len()];
    let mut seen_conditions = HashSet::new();
    for leg in &opp.execution_plan {
        if leg.market_index >= opp.markets.len()
            || seen_indices[leg.market_index]
            || leg.outcome != expected_outcome
            || (leg.unit_shares - 1.0).abs() > f64::EPSILON
        {
            return false;
        }

        let market = &opp.markets[leg.market_index];
        let condition_id = market.condition_id.trim();
        if condition_id.is_empty()
            || leg.condition_id.trim() != condition_id
            || !seen_conditions.insert(condition_id)
        {
            return false;
        }

        let expected_token = match expected_outcome {
            OutcomeSide::Yes => market.clob_token_id_yes.trim(),
            OutcomeSide::No => market.clob_token_id_no.trim(),
        };
        if expected_token.is_empty() || leg.token_id.trim() != expected_token {
            return false;
        }

        seen_indices[leg.market_index] = true;
    }

    seen_indices.into_iter().all(|seen| seen)
}

/// Raw event data from the Gamma API (for deserialization).
#[derive(Debug, Deserialize)]
pub struct GammaEvent {
    pub id: Option<serde_json::Value>,
    pub title: Option<String>,
    pub slug: Option<String>,
    pub category: Option<String>,
    #[serde(rename = "endDate", alias = "end_date")]
    pub end_date: Option<serde_json::Value>,
    #[serde(rename = "gameStartTime", alias = "game_start_time")]
    pub game_start_time: Option<serde_json::Value>,
    #[serde(rename = "resolutionSource", alias = "resolution_source")]
    pub resolution_source: Option<String>,
    pub description: Option<String>,
    pub rules: Option<String>,
    #[serde(rename = "umaResolutionStatus", alias = "uma_resolution_status")]
    pub uma_resolution_status: Option<String>,
    #[serde(rename = "enableNegRisk")]
    pub enable_neg_risk: Option<bool>,
    #[serde(rename = "negRisk")]
    pub neg_risk: Option<bool>,
    #[serde(rename = "negRiskAugmented")]
    pub neg_risk_augmented: Option<bool>,
    pub markets: Option<Vec<GammaMarket>>,
}

/// Raw market data from the Gamma API (for deserialization).
#[derive(Debug, Deserialize)]
pub struct GammaMarket {
    pub question: Option<String>,
    pub slug: Option<String>,
    #[serde(rename = "endDate", alias = "end_date")]
    pub end_date: Option<serde_json::Value>,
    #[serde(rename = "gameStartTime", alias = "game_start_time")]
    pub game_start_time: Option<serde_json::Value>,
    #[serde(rename = "resolutionSource", alias = "resolution_source")]
    pub resolution_source: Option<String>,
    pub description: Option<String>,
    pub rules: Option<String>,
    #[serde(rename = "umaResolutionStatus", alias = "uma_resolution_status")]
    pub uma_resolution_status: Option<String>,
    #[serde(rename = "conditionId")]
    pub condition_id: Option<String>,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: Option<serde_json::Value>,
    pub outcomes: Option<serde_json::Value>,
    #[serde(rename = "outcomePrices")]
    pub outcome_prices: Option<serde_json::Value>,
    pub liquidity: Option<serde_json::Value>,
    pub closed: Option<bool>,
    pub active: Option<bool>,
    pub archived: Option<bool>,
    #[serde(rename = "acceptingOrders")]
    pub accepting_orders: Option<bool>,
    #[serde(rename = "enableOrderBook")]
    pub enable_order_book: Option<bool>,
    #[serde(rename = "feesEnabled")]
    pub fees_enabled: Option<bool>,
    #[serde(rename = "takerBaseFee")]
    pub taker_base_fee: Option<serde_json::Value>,
    #[serde(rename = "makerBaseFee")]
    pub maker_base_fee: Option<serde_json::Value>,
    #[serde(rename = "orderPriceMinTickSize")]
    pub order_price_min_tick_size: Option<serde_json::Value>,
    #[serde(rename = "orderMinSize")]
    pub order_min_size: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market() -> Market {
        Market {
            question: "Q".into(),
            condition_id: "cond".into(),
            market_slug: "q".into(),
            clob_token_id_yes: "yes".into(),
            clob_token_id_no: "no".into(),
            gamma_yes_price: 0.4,
            gamma_no_price: 0.6,
            clob_yes_ask: Some(0.41),
            clob_yes_bid: Some(0.39),
            clob_no_ask: Some(0.61),
            clob_no_bid: Some(0.59),
            clob_yes_ask_size: Some(100.0),
            clob_yes_bid_size: None,
            clob_no_ask_size: Some(100.0),
            clob_no_bid_size: None,
            fees_enabled: Some(true),
            taker_fee_rate: None,
            maker_fee_rate: None,
            clob_taker_fee_bps: None,
            clob_fee_rate: None,
            clob_fee_exponent: None,
            order_price_min_tick_size: Some(0.01),
            order_min_size: Some(1.0),
            clob_tick_size: None,
            clob_min_order_size: None,
            clob_neg_risk: None,
            clob_rfq_enabled: None,
            liquidity: 1000.0,
            closed: false,
        }
    }

    #[test]
    fn tick_and_min_order_helpers_prioritize_clob_metadata() {
        let mut m = market();
        m.clob_tick_size = Some(0.001);
        m.clob_min_order_size = Some(5.0);
        assert!((m.tick_size() - 0.001).abs() < f64::EPSILON);
        assert_eq!(m.min_order_size_shares(), 5.0);
    }

    #[test]
    fn fee_curve_support_accepts_bounded_documented_exponents() {
        let mut m = market();
        m.clob_fee_exponent = Some(1);
        m.clob_fee_rate = Some(0.03);
        assert!(m.supports_standard_fee_curve());

        m.clob_fee_exponent = Some(2);
        assert!(m.supports_standard_fee_curve());

        m.clob_fee_exponent = Some(MAX_SUPPORTED_CLOB_FEE_EXPONENT + 1);
        assert!(!m.supports_standard_fee_curve());

        m.clob_fee_exponent = None;
        assert!(!m.supports_standard_fee_curve());
    }

    fn yes_no_family_opp(arb_type: ArbType) -> ArbitrageOpportunity {
        let mut first = market();
        first.condition_id = "cond-1".into();
        first.clob_token_id_yes = "yes-1".into();
        first.clob_token_id_no = "no-1".into();
        let mut second = market();
        second.condition_id = "cond-2".into();
        second.clob_token_id_yes = "yes-2".into();
        second.clob_token_id_no = "no-2".into();
        let expected_outcome = match arb_type {
            ArbType::No => OutcomeSide::No,
            _ => OutcomeSide::Yes,
        };
        let token_id = |market: &Market| match expected_outcome {
            OutcomeSide::Yes => market.clob_token_id_yes.clone(),
            OutcomeSide::No => market.clob_token_id_no.clone(),
        };
        let markets = vec![first, second];
        let execution_plan = markets
            .iter()
            .enumerate()
            .map(|(market_index, market)| OpportunityLeg {
                market_index,
                question: market.question.clone(),
                market_slug: market.market_slug.clone(),
                condition_id: market.condition_id.clone(),
                token_id: token_id(market),
                outcome: expected_outcome,
                unit_shares: 1.0,
                reference_price: 0.4,
            })
            .collect();

        ArbitrageOpportunity {
            event_title: "E".into(),
            event_id: "1".into(),
            category: "sports".into(),
            arb_type,
            markets,
            execution_plan,
            total_cost: 0.8,
            guaranteed_revenue: 1.0,
            gross_profit: 0.2,
            total_fees: 0.0,
            net_profit: 0.2,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 20.0,
            prices_from_clob: true,
            max_executable_size_usd: 10.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        }
    }

    #[test]
    fn supported_yes_no_full_family_plan_requires_complete_matching_family() {
        let yes_opp = yes_no_family_opp(ArbType::Yes);
        assert!(is_supported_yes_no_full_family_plan(&yes_opp));
        let no_opp = yes_no_family_opp(ArbType::No);
        assert!(is_supported_yes_no_full_family_plan(&no_opp));

        let mut partial = yes_opp.clone();
        partial.execution_plan.pop();
        assert!(!is_supported_yes_no_full_family_plan(&partial));

        let mut wrong_token = yes_opp.clone();
        wrong_token.execution_plan[0].token_id = "no-1".into();
        assert!(!is_supported_yes_no_full_family_plan(&wrong_token));

        let mut bundle = yes_opp;
        bundle.arb_type = ArbType::Bundle;
        assert!(!is_supported_yes_no_full_family_plan(&bundle));
    }
}
