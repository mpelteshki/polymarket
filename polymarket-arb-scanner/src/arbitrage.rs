//! Pure arbitrage detection logic.
//!
//! No I/O, no side effects - takes Market objects with prices and returns
//! ArbitrageOpportunity objects if arbitrage exists.

use chrono::Utc;

use crate::config::Config;
use crate::fees;
use crate::models::{ArbType, ArbitrageOpportunity, Event, Market, OpportunityLeg, OutcomeSide};

/// Conservative spread estimate when CLOB prices are not available.
/// The Gamma API returns midpoint / last-trade-like prices; the actual ask is
/// typically higher. We keep a modest uplift for non-executable idea scans.
const GAMMA_SPREAD_ESTIMATE: f64 = 0.02;
const MIRRORED_YES_NO_LIQUIDITY_COLLISION_FACTOR: f64 = 0.5;

#[derive(Debug, Clone)]
pub struct RawEdgeProbe {
    pub event_title: String,
    pub event_id: String,
    pub arb_type: ArbType,
    pub total_cost: f64,
    pub guaranteed_revenue: f64,
    pub gross_profit: f64,
    pub total_fees: f64,
    pub net_profit: f64,
    pub roi_pct: f64,
    pub prices_from_clob: bool,
}

fn is_tradable_market(market: &Market, config: &Config) -> bool {
    !market.closed
        && market.liquidity >= config.min_liquidity_usd
        && fees::market_fee_curve_supported(market)
}

fn compute_roi_pct(net_profit: f64, total_cost: f64) -> f64 {
    if total_cost > f64::EPSILON {
        net_profit / total_cost * 100.0
    } else {
        0.0
    }
}

fn edge_probe(
    event: &Event,
    arb_type: ArbType,
    total_cost: f64,
    guaranteed_revenue: f64,
    total_fees: f64,
    prices_from_clob: bool,
) -> RawEdgeProbe {
    let gross_profit = guaranteed_revenue - total_cost;
    let net_profit = gross_profit - total_fees;
    RawEdgeProbe {
        event_title: event.title.clone(),
        event_id: event.event_id.clone(),
        arb_type,
        total_cost,
        guaranteed_revenue,
        gross_profit,
        total_fees,
        net_profit,
        roi_pct: compute_roi_pct(net_profit, total_cost),
        prices_from_clob,
    }
}

pub fn probe_yes_raw_edge(event: &Event, use_clob: bool, config: &Config) -> Option<RawEdgeProbe> {
    let markets = &event.markets;
    if markets.len() < 2
        || markets.len() > config.max_batchable_legs()
        || !markets.iter().all(|m| is_tradable_market(m, config))
        || !yes_arb_is_theoretically_safe(event, config)
    {
        return None;
    }
    if use_clob {
        let coverage = quote_coverage(markets, true, config.execute_only_full_clob_prices);
        if coverage < config.min_clob_quote_coverage_pct
            || (config.execute_only_full_clob_prices && coverage < 1.0)
        {
            return None;
        }
    }
    let (yes_asks, prices_from_clob) = side_prices(markets, use_clob, true)?;
    let total_cost: f64 = yes_asks.iter().sum();
    let total_fees =
        fees::arbitrage_fees_for_markets(markets, &yes_asks, 1.0, &event.category, config);
    Some(edge_probe(
        event,
        ArbType::Yes,
        total_cost,
        1.0,
        total_fees,
        prices_from_clob,
    ))
}

pub fn probe_no_raw_edge(event: &Event, use_clob: bool, config: &Config) -> Option<RawEdgeProbe> {
    let markets = &event.markets;
    if markets.len() < 2
        || markets.len() > config.max_batchable_legs()
        || !markets.iter().all(|m| is_tradable_market(m, config))
        || !no_arb_is_theoretically_safe(event, config)
    {
        return None;
    }
    if use_clob {
        let coverage = quote_coverage(markets, false, config.execute_only_full_clob_prices);
        if coverage < config.min_clob_quote_coverage_pct
            || (config.execute_only_full_clob_prices && coverage < 1.0)
        {
            return None;
        }
    }
    let (no_asks, prices_from_clob) = side_prices(markets, use_clob, false)?;
    let total_cost: f64 = no_asks.iter().sum();
    let guaranteed_revenue = (markets.len() - 1) as f64;
    let total_fees =
        fees::arbitrage_fees_for_markets(markets, &no_asks, 1.0, &event.category, config);
    Some(edge_probe(
        event,
        ArbType::No,
        total_cost,
        guaranteed_revenue,
        total_fees,
        prices_from_clob,
    ))
}

pub fn probe_bundle_raw_edges(event: &Event, use_clob: bool, config: &Config) -> Vec<RawEdgeProbe> {
    let mut probes = Vec::new();
    for market in &event.markets {
        if !is_tradable_market(market, config) {
            continue;
        }
        let (yes_ask, no_ask, prices_from_clob) = if use_clob {
            if config.execute_only_full_clob_prices
                && (!market.has_full_yes_quote() || !market.has_full_no_quote())
            {
                continue;
            }
            if !market.has_yes_price_quote() || !market.has_no_price_quote() {
                continue;
            }
            match (market.clob_yes_ask, market.clob_no_ask) {
                (Some(yes), Some(no)) => (yes, no, true),
                _ => continue,
            }
        } else {
            (
                market.yes_ask(false, GAMMA_SPREAD_ESTIMATE),
                market.no_ask(false, GAMMA_SPREAD_ESTIMATE),
                false,
            )
        };
        let total_cost = yes_ask + no_ask;
        let total_fees = fees::total_fee_for_market(yes_ask, 1.0, market, &event.category, config)
            + fees::total_fee_for_market(no_ask, 1.0, market, &event.category, config);
        probes.push(RawEdgeProbe {
            event_title: format!("[BUNDLE] {} - {}", event.title, market.question),
            event_id: event.event_id.clone(),
            arb_type: ArbType::Bundle,
            total_cost,
            guaranteed_revenue: 1.0,
            gross_profit: 1.0 - total_cost,
            total_fees,
            net_profit: 1.0 - total_cost - total_fees,
            roi_pct: compute_roi_pct(1.0 - total_cost - total_fees, total_cost),
            prices_from_clob,
        });
    }
    probes
}

fn quote_coverage(markets: &[Market], yes_side: bool, require_visible_size: bool) -> f64 {
    if markets.is_empty() {
        return 0.0;
    }
    let quoted = markets
        .iter()
        .filter(|m| {
            if yes_side {
                if require_visible_size {
                    m.has_full_yes_quote()
                } else {
                    m.has_yes_price_quote()
                }
            } else if require_visible_size {
                m.has_full_no_quote()
            } else {
                m.has_no_price_quote()
            }
        })
        .count();
    quoted as f64 / markets.len() as f64
}

fn side_prices(markets: &[Market], use_clob: bool, yes_side: bool) -> Option<(Vec<f64>, bool)> {
    if use_clob {
        let prices: Option<Vec<f64>> = markets
            .iter()
            .map(|m| {
                if yes_side {
                    m.clob_yes_ask
                } else {
                    m.clob_no_ask
                }
            })
            .collect();
        prices.map(|p| (p, true))
    } else {
        Some((
            markets
                .iter()
                .map(|m| {
                    if yes_side {
                        m.yes_ask(false, GAMMA_SPREAD_ESTIMATE)
                    } else {
                        m.no_ask(false, GAMMA_SPREAD_ESTIMATE)
                    }
                })
                .collect(),
            false,
        ))
    }
}

fn max_executable_size_usd(markets: &[Market], prices: &[f64], yes_side: bool) -> f64 {
    if markets.is_empty() || prices.is_empty() || markets.len() != prices.len() {
        return 0.0;
    }

    let total_cost_per_unit: f64 = prices.iter().sum();
    if total_cost_per_unit <= f64::EPSILON {
        return 0.0;
    }

    let bottleneck_shares = markets
        .iter()
        .filter_map(|m| {
            if yes_side {
                m.clob_yes_ask_size
            } else {
                m.clob_no_ask_size
            }
        })
        .fold(f64::INFINITY, f64::min);

    if bottleneck_shares.is_finite() {
        return bottleneck_shares * total_cost_per_unit;
    }

    markets
        .iter()
        .zip(prices)
        .map(|(m, &p)| m.liquidity / p.max(0.01))
        .fold(f64::INFINITY, f64::min)
        * total_cost_per_unit
}

fn mirrored_yes_no_collision_cap_usd(shares: f64, unit_cost_or_collateral: f64) -> f64 {
    if shares <= f64::EPSILON || unit_cost_or_collateral <= f64::EPSILON {
        0.0
    } else {
        shares * unit_cost_or_collateral * MIRRORED_YES_NO_LIQUIDITY_COLLISION_FACTOR
    }
}

fn is_plain_neg_risk_family(event: &Event) -> bool {
    let clob_confirmed_neg_risk = !event.markets.is_empty()
        && event
            .markets
            .iter()
            .all(|market| market.clob_neg_risk == Some(true));
    let neg_risk = event.enable_neg_risk || event.neg_risk || clob_confirmed_neg_risk;
    if !neg_risk {
        return false;
    }
    // Augmented neg-risk families can introduce placeholder / "other" logic, so
    // a YES basket is not treated as guaranteed by this scanner even if the user
    // elects to include augmented events for other strategy types.
    if event.neg_risk_augmented {
        return false;
    }
    true
}

fn yes_arb_is_theoretically_safe(event: &Event, _config: &Config) -> bool {
    is_plain_neg_risk_family(event)
}

fn no_arb_is_theoretically_safe(event: &Event, _config: &Config) -> bool {
    is_plain_neg_risk_family(event)
}

fn build_execution_plan(
    markets: &[Market],
    prices: &[f64],
    outcome: OutcomeSide,
) -> Vec<OpportunityLeg> {
    markets
        .iter()
        .enumerate()
        .zip(prices.iter().copied())
        .map(|((market_index, market), reference_price)| OpportunityLeg {
            market_index,
            question: market.question.clone(),
            market_slug: market.market_slug.clone(),
            condition_id: market.condition_id.clone(),
            token_id: match outcome {
                OutcomeSide::Yes => market.clob_token_id_yes.clone(),
                OutcomeSide::No => market.clob_token_id_no.clone(),
            },
            outcome,
            unit_shares: 1.0,
            reference_price,
        })
        .collect()
}

/// Detect YES arbitrage: buy YES on every outcome.
///
/// In a complete, mutually exclusive event, exactly one outcome resolves to YES
/// and pays $1.00. If the sum of all YES ask prices < $1.00 (after fees), you
/// have guaranteed profit. This scanner only considers YES arbitrage safe on
/// neg-risk families and blocks augmented neg-risk by default.
pub fn detect_yes_arbitrage(
    event: &Event,
    use_clob: bool,
    config: &Config,
    gas_cost_usd: f64,
) -> Option<ArbitrageOpportunity> {
    let markets = &event.markets;
    if markets.len() < 2
        || markets.len() > config.max_batchable_legs()
        || !markets.iter().all(|m| is_tradable_market(m, config))
        || !yes_arb_is_theoretically_safe(event, config)
    {
        return None;
    }

    if use_clob {
        let coverage = quote_coverage(markets, true, config.execute_only_full_clob_prices);
        if coverage < config.min_clob_quote_coverage_pct {
            return None;
        }
        if config.execute_only_full_clob_prices && coverage < 1.0 {
            return None;
        }
    }

    let (yes_asks, prices_from_clob) = side_prices(markets, use_clob, true)?;

    let total_cost: f64 = yes_asks.iter().sum();
    let guaranteed_revenue = 1.0;
    let gross_profit = guaranteed_revenue - total_cost;
    if gross_profit <= 0.0 {
        return None;
    }

    let total_fees =
        fees::arbitrage_fees_for_markets(markets, &yes_asks, 1.0, &event.category, config);
    let net_profit = gross_profit - total_fees;
    if net_profit <= 0.0 {
        return None;
    }

    let roi_pct = compute_roi_pct(net_profit, total_cost);
    if roi_pct < config.min_roi_pct {
        return None;
    }

    let max_executable_size_usd = max_executable_size_usd(markets, &yes_asks, true);

    Some(ArbitrageOpportunity {
        event_title: event.title.clone(),
        event_id: event.event_id.clone(),
        category: event.category.clone(),
        arb_type: ArbType::Yes,
        markets: markets.clone(),
        execution_plan: build_execution_plan(markets, &yes_asks, OutcomeSide::Yes),
        total_cost,
        guaranteed_revenue,
        gross_profit,
        total_fees,
        net_profit,
        estimated_total_gas_cost_usd: gas_cost_usd * markets.len() as f64,
        roi_pct,
        prices_from_clob,
        max_executable_size_usd,
        capital_lock_hours: event.lifecycle.capital_lock_hours_from(Utc::now()),
        expected_slippage_pct: 0.0,
        detected_at: Utc::now(),
    })
}

/// Detect NO arbitrage: buy NO on every outcome.
///
/// In a mutually exclusive event with N outcomes, exactly N-1 resolve to NO,
/// each paying $1.00. Total guaranteed revenue = $(N-1).
pub fn detect_no_arbitrage(
    event: &Event,
    use_clob: bool,
    config: &Config,
    gas_cost_usd: f64,
) -> Option<ArbitrageOpportunity> {
    let markets = &event.markets;
    if markets.len() < 2
        || markets.len() > config.max_batchable_legs()
        || !markets.iter().all(|m| is_tradable_market(m, config))
        || !no_arb_is_theoretically_safe(event, config)
    {
        return None;
    }

    if use_clob {
        let coverage = quote_coverage(markets, false, config.execute_only_full_clob_prices);
        if coverage < config.min_clob_quote_coverage_pct {
            return None;
        }
        if config.execute_only_full_clob_prices && coverage < 1.0 {
            return None;
        }
    }

    let (no_asks, prices_from_clob) = side_prices(markets, use_clob, false)?;

    let total_cost: f64 = no_asks.iter().sum();
    let guaranteed_revenue = (markets.len() - 1) as f64;
    let gross_profit = guaranteed_revenue - total_cost;
    if gross_profit <= 0.0 {
        return None;
    }

    let total_fees =
        fees::arbitrage_fees_for_markets(markets, &no_asks, 1.0, &event.category, config);
    let net_profit = gross_profit - total_fees;
    if net_profit <= 0.0 {
        return None;
    }

    let roi_pct = compute_roi_pct(net_profit, total_cost);
    if roi_pct < config.min_roi_pct {
        return None;
    }

    let max_executable_size_usd = max_executable_size_usd(markets, &no_asks, false);

    Some(ArbitrageOpportunity {
        event_title: event.title.clone(),
        event_id: event.event_id.clone(),
        category: event.category.clone(),
        arb_type: ArbType::No,
        markets: markets.clone(),
        execution_plan: build_execution_plan(markets, &no_asks, OutcomeSide::No),
        total_cost,
        guaranteed_revenue,
        gross_profit,
        total_fees,
        net_profit,
        estimated_total_gas_cost_usd: gas_cost_usd * markets.len() as f64,
        roi_pct,
        prices_from_clob,
        max_executable_size_usd,
        capital_lock_hours: event.lifecycle.capital_lock_hours_from(Utc::now()),
        expected_slippage_pct: 0.0,
        detected_at: Utc::now(),
    })
}

/// Detect bundle arbitrage: buy YES + NO on a single market.
///
/// Also known as "Gabagool" strategy. If ask_yes + ask_no < $1.00
/// for any individual market, you can buy both sides and guarantee
/// a $1.00 payout regardless of outcome.
pub fn detect_bundle_arbitrage(
    event: &Event,
    use_clob: bool,
    config: &Config,
    gas_cost_usd: f64,
) -> Vec<ArbitrageOpportunity> {
    let mut opportunities = Vec::new();

    for market in &event.markets {
        if !is_tradable_market(market, config) {
            continue;
        }

        let (yes_ask, no_ask, prices_from_clob) = if use_clob {
            if config.execute_only_full_clob_prices
                && (!market.has_full_yes_quote() || !market.has_full_no_quote())
            {
                continue;
            }
            if !market.has_yes_price_quote() || !market.has_no_price_quote() {
                continue;
            }
            match (market.clob_yes_ask, market.clob_no_ask) {
                (Some(yes), Some(no)) => (yes, no, true),
                _ => continue,
            }
        } else {
            (
                market.yes_ask(false, GAMMA_SPREAD_ESTIMATE),
                market.no_ask(false, GAMMA_SPREAD_ESTIMATE),
                false,
            )
        };

        let total_cost = yes_ask + no_ask;
        let guaranteed_revenue = 1.0;
        let gross_profit = guaranteed_revenue - total_cost;

        if gross_profit <= 0.0 {
            continue;
        }

        let total_fees = fees::total_fee_for_market(yes_ask, 1.0, market, &event.category, config)
            + fees::total_fee_for_market(no_ask, 1.0, market, &event.category, config);
        let net_profit = gross_profit - total_fees;
        if net_profit <= 0.0 {
            continue;
        }

        let roi_pct = compute_roi_pct(net_profit, total_cost);
        if roi_pct < config.min_roi_pct {
            continue;
        }

        let max_executable_size_usd = match (market.clob_yes_ask_size, market.clob_no_ask_size) {
            (Some(yes_sz), Some(no_sz)) => {
                mirrored_yes_no_collision_cap_usd(yes_sz.min(no_sz), total_cost)
            }
            _ => {
                (market.liquidity / yes_ask.max(0.01)).min(market.liquidity / no_ask.max(0.01))
                    * total_cost
            }
        };

        opportunities.push(ArbitrageOpportunity {
            event_title: format!("[BUNDLE] {} - {}", event.title, market.question),
            event_id: event.event_id.clone(),
            category: event.category.clone(),
            arb_type: ArbType::Bundle,
            markets: vec![market.clone()],
            execution_plan: vec![
                OpportunityLeg {
                    market_index: 0,
                    question: market.question.clone(),
                    market_slug: market.market_slug.clone(),
                    condition_id: market.condition_id.clone(),
                    token_id: market.clob_token_id_yes.clone(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 1.0,
                    reference_price: yes_ask,
                },
                OpportunityLeg {
                    market_index: 0,
                    question: market.question.clone(),
                    market_slug: market.market_slug.clone(),
                    condition_id: market.condition_id.clone(),
                    token_id: market.clob_token_id_no.clone(),
                    outcome: OutcomeSide::No,
                    unit_shares: 1.0,
                    reference_price: no_ask,
                },
            ],
            total_cost,
            guaranteed_revenue,
            gross_profit,
            total_fees,
            net_profit,
            estimated_total_gas_cost_usd: gas_cost_usd * 2.0,
            roi_pct,
            prices_from_clob,
            max_executable_size_usd,
            capital_lock_hours: event.lifecycle.capital_lock_hours_from(Utc::now()),
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        });
    }

    opportunities
}

/// Detect mint/sell arbitrage: split $1 collateral into YES+NO and sell both.
///
/// This is the inverse of a buy-side full-set bundle. It is reported as a
/// scan/paper candidate only; live execution needs an atomic split-and-sell
/// route before this can be traded safely.
pub fn detect_mint_sell_arbitrage(
    event: &Event,
    use_clob: bool,
    config: &Config,
    gas_cost_usd: f64,
) -> Vec<ArbitrageOpportunity> {
    if !use_clob {
        return Vec::new();
    }

    let mut opportunities = Vec::new();

    for market in &event.markets {
        if !is_tradable_market(market, config) || !market.has_full_yes_no_bid_quotes() {
            continue;
        }

        let (Some(yes_bid), Some(no_bid), Some(yes_bid_size), Some(no_bid_size)) = (
            market.clob_yes_bid,
            market.clob_no_bid,
            market.clob_yes_bid_size,
            market.clob_no_bid_size,
        ) else {
            continue;
        };
        let gross_revenue = yes_bid + no_bid;
        let collateral_cost = 1.0;
        let gross_profit = gross_revenue - collateral_cost;
        if gross_profit <= 0.0 {
            continue;
        }

        let total_fees = fees::total_fee_for_market(yes_bid, 1.0, market, &event.category, config)
            + fees::total_fee_for_market(no_bid, 1.0, market, &event.category, config);
        let net_profit = gross_profit - total_fees;
        if net_profit <= 0.0 {
            continue;
        }

        let roi_pct = compute_roi_pct(net_profit, collateral_cost);
        if roi_pct < config.min_roi_pct {
            continue;
        }

        let executable_shares = yes_bid_size.min(no_bid_size);
        if executable_shares <= f64::EPSILON {
            continue;
        }
        let max_executable_size_usd =
            mirrored_yes_no_collision_cap_usd(executable_shares, collateral_cost);

        opportunities.push(ArbitrageOpportunity {
            event_title: format!("[MINT_SELL] {} - {}", event.title, market.question),
            event_id: event.event_id.clone(),
            category: event.category.clone(),
            arb_type: ArbType::MintSell,
            markets: vec![market.clone()],
            execution_plan: vec![
                OpportunityLeg {
                    market_index: 0,
                    question: market.question.clone(),
                    market_slug: market.market_slug.clone(),
                    condition_id: market.condition_id.clone(),
                    token_id: market.clob_token_id_yes.clone(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 1.0,
                    reference_price: yes_bid,
                },
                OpportunityLeg {
                    market_index: 0,
                    question: market.question.clone(),
                    market_slug: market.market_slug.clone(),
                    condition_id: market.condition_id.clone(),
                    token_id: market.clob_token_id_no.clone(),
                    outcome: OutcomeSide::No,
                    unit_shares: 1.0,
                    reference_price: no_bid,
                },
            ],
            total_cost: collateral_cost,
            guaranteed_revenue: gross_revenue,
            gross_profit,
            total_fees,
            net_profit,
            estimated_total_gas_cost_usd: gas_cost_usd * 3.0,
            roi_pct,
            prices_from_clob: true,
            max_executable_size_usd,
            capital_lock_hours: event.lifecycle.capital_lock_hours_from(Utc::now()),
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        });
    }

    opportunities
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_market(yes: f64, no: f64) -> Market {
        Market {
            question: "Test?".into(),
            condition_id: "cond".into(),
            market_slug: "test-market".into(),
            clob_token_id_yes: "yes".into(),
            clob_token_id_no: "no".into(),
            gamma_yes_price: yes,
            gamma_no_price: no,
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
            clob_taker_fee_bps: None,
            clob_fee_rate: None,
            clob_fee_exponent: None,
            order_price_min_tick_size: None,
            order_min_size: None,
            clob_tick_size: None,
            clob_min_order_size: None,
            clob_neg_risk: None,
            clob_rfq_enabled: None,
            liquidity: 5000.0,
            closed: false,
        }
    }

    fn make_event(markets: Vec<Market>) -> Event {
        Event {
            event_id: "test-1".into(),
            title: "Test Event".into(),
            slug: "test".into(),
            category: "geopolitics".into(),
            enable_neg_risk: true,
            neg_risk: true,
            neg_risk_augmented: false,
            lifecycle: Default::default(),
            markets,
        }
    }

    fn test_config() -> Config {
        let mut cfg = Config::from_env();
        cfg.min_net_profit_usd = 0.001;
        cfg.min_roi_pct = 0.0;
        cfg.max_opportunity_legs = 16;
        cfg
    }

    #[test]
    fn test_yes_arbitrage_found() {
        let event = make_event(vec![
            make_market(0.20, 0.80),
            make_market(0.20, 0.80),
            make_market(0.20, 0.80),
        ]);
        let cfg = test_config();
        let opp = detect_yes_arbitrage(&event, false, &cfg, 0.0);
        assert!(opp.is_some());
        let opp = opp.unwrap();
        assert!(opp.net_profit > 0.0);
        assert_eq!(opp.arb_type, ArbType::Yes);
        assert_eq!(opp.execution_plan.len(), 3);
    }

    #[test]
    fn test_yes_arbitrage_not_found_when_expensive() {
        let event = make_event(vec![make_market(0.50, 0.50), make_market(0.50, 0.50)]);
        let cfg = test_config();
        let opp = detect_yes_arbitrage(&event, false, &cfg, 0.0);
        assert!(opp.is_none());
    }

    #[test]
    fn test_yes_arbitrage_rejects_augmented_neg_risk_even_if_config_allows_it() {
        let mut event = make_event(vec![make_market(0.20, 0.80), make_market(0.20, 0.80)]);
        event.neg_risk_augmented = true;
        let mut cfg = test_config();
        cfg.allow_augmented_neg_risk = true;
        assert!(detect_yes_arbitrage(&event, false, &cfg, 0.0).is_none());
    }

    #[test]
    fn test_yes_arbitrage_requires_neg_risk() {
        let mut event = make_event(vec![make_market(0.20, 0.80), make_market(0.20, 0.80)]);
        event.enable_neg_risk = false;
        event.neg_risk = false;
        let cfg = test_config();
        assert!(detect_yes_arbitrage(&event, false, &cfg, 0.0).is_none());
    }

    #[test]
    fn test_yes_arbitrage_accepts_clob_confirmed_neg_risk_when_event_flag_missing() {
        let mut market_a = make_market(0.20, 0.80);
        market_a.clob_neg_risk = Some(true);
        let mut market_b = make_market(0.20, 0.80);
        market_b.clob_neg_risk = Some(true);
        let mut event = make_event(vec![market_a, market_b]);
        event.enable_neg_risk = false;
        event.neg_risk = false;
        let cfg = test_config();
        assert!(detect_yes_arbitrage(&event, false, &cfg, 0.0).is_some());
    }

    #[test]
    fn test_yes_arbitrage_requires_visible_size_in_strict_clob_mode() {
        let mut market_a = make_market(0.20, 0.80);
        market_a.clob_yes_ask = Some(0.20);
        market_a.clob_yes_ask_size = Some(100.0);
        let mut market_b = make_market(0.20, 0.80);
        market_b.clob_yes_ask = Some(0.20);
        market_b.clob_yes_ask_size = None;
        let event = make_event(vec![market_a, market_b]);
        let mut cfg = test_config();
        cfg.execute_only_full_clob_prices = true;

        assert!(detect_yes_arbitrage(&event, true, &cfg, 0.0).is_none());
    }

    #[test]
    fn test_no_arbitrage_rejects_augmented_neg_risk_even_if_config_allows_it() {
        let mut event = make_event(vec![
            make_market(0.80, 0.20),
            make_market(0.80, 0.20),
            make_market(0.80, 0.20),
        ]);
        event.neg_risk_augmented = true;
        let mut cfg = test_config();
        cfg.allow_augmented_neg_risk = true;
        assert!(detect_no_arbitrage(&event, false, &cfg, 0.0).is_none());
    }

    #[test]
    fn test_no_arbitrage_requires_neg_risk() {
        let mut event = make_event(vec![
            make_market(0.80, 0.20),
            make_market(0.80, 0.20),
            make_market(0.80, 0.20),
        ]);
        event.enable_neg_risk = false;
        event.neg_risk = false;
        let cfg = test_config();
        assert!(detect_no_arbitrage(&event, false, &cfg, 0.0).is_none());
    }

    #[test]
    fn test_yes_arbitrage_stores_trade_gas_separately_from_per_basket_edge() {
        let event = make_event(vec![
            make_market(0.31, 0.69),
            make_market(0.31, 0.69),
            make_market(0.31, 0.69),
        ]);
        let cfg = test_config();
        let opp = detect_yes_arbitrage(&event, false, &cfg, 0.01).expect("arb survives gas");
        assert!((opp.estimated_total_gas_cost_usd - 0.03).abs() < 1e-10);
        assert!(opp.net_profit > 0.0);
    }

    #[test]
    fn test_no_arbitrage_found() {
        let event = make_event(vec![
            make_market(0.80, 0.20),
            make_market(0.80, 0.20),
            make_market(0.80, 0.20),
        ]);
        let cfg = test_config();
        let opp = detect_no_arbitrage(&event, false, &cfg, 0.0);
        assert!(opp.is_some());
        let opp = opp.unwrap();
        assert!(opp.net_profit > 0.0);
        assert_eq!(opp.arb_type, ArbType::No);
        assert_eq!(opp.execution_plan.len(), 3);
    }

    #[test]
    fn test_no_arbitrage_requires_visible_size_in_strict_clob_mode() {
        let mut market_a = make_market(0.80, 0.20);
        market_a.clob_no_ask = Some(0.20);
        market_a.clob_no_ask_size = Some(100.0);
        let mut market_b = make_market(0.80, 0.20);
        market_b.clob_no_ask = Some(0.20);
        market_b.clob_no_ask_size = None;
        let event = make_event(vec![market_a, market_b]);
        let mut cfg = test_config();
        cfg.execute_only_full_clob_prices = true;

        assert!(detect_no_arbitrage(&event, true, &cfg, 0.0).is_none());
    }

    #[test]
    fn test_single_market_rejected() {
        let event = make_event(vec![make_market(0.50, 0.50)]);
        let cfg = test_config();
        assert!(detect_yes_arbitrage(&event, false, &cfg, 0.0).is_none());
        assert!(detect_no_arbitrage(&event, false, &cfg, 0.0).is_none());
    }

    #[test]
    fn test_bundle_arbitrage() {
        let event = make_event(vec![make_market(0.40, 0.40)]);
        let cfg = test_config();
        let opps = detect_bundle_arbitrage(&event, false, &cfg, 0.0);
        assert_eq!(opps.len(), 1);
        assert!(opps[0].net_profit > 0.0);
        assert_eq!(opps[0].arb_type, ArbType::Bundle);
        assert_eq!(opps[0].execution_plan.len(), 2);
    }

    #[test]
    fn test_bundle_clob_requires_visible_sizes() {
        let mut market = make_market(0.49, 0.49);
        market.clob_yes_ask = Some(0.49);
        market.clob_no_ask = Some(0.49);
        market.clob_yes_ask_size = Some(100.0);
        market.clob_no_ask_size = None;
        let event = make_event(vec![market]);
        let cfg = test_config();
        let opps = detect_bundle_arbitrage(&event, true, &cfg, 0.0);
        assert!(opps.is_empty());
    }

    #[test]
    fn test_bundle_clob_caps_same_condition_mirrored_liquidity() {
        let mut market = make_market(0.49, 0.49);
        market.clob_yes_ask = Some(0.49);
        market.clob_no_ask = Some(0.49);
        market.clob_yes_ask_size = Some(100.0);
        market.clob_no_ask_size = Some(80.0);
        let event = make_event(vec![market]);
        let cfg = test_config();

        let opps = detect_bundle_arbitrage(&event, true, &cfg, 0.0);

        assert_eq!(opps.len(), 1);
        assert!((opps[0].max_executable_size_usd - 39.2).abs() < 1e-9);
    }

    #[test]
    fn test_mint_sell_arbitrage_found_from_bid_depth() {
        let mut market = make_market(0.49, 0.49);
        market.clob_yes_bid = Some(0.52);
        market.clob_no_bid = Some(0.53);
        market.clob_yes_bid_size = Some(40.0);
        market.clob_no_bid_size = Some(25.0);
        let event = make_event(vec![market]);
        let cfg = test_config();

        let opps = detect_mint_sell_arbitrage(&event, true, &cfg, 0.01);

        assert_eq!(opps.len(), 1);
        assert_eq!(opps[0].arb_type, ArbType::MintSell);
        assert_eq!(opps[0].execution_plan.len(), 2);
        assert!((opps[0].gross_profit - 0.05).abs() < 1e-9);
        assert!((opps[0].max_executable_size_usd - 12.5).abs() < 1e-9);
        assert!((opps[0].estimated_total_gas_cost_usd - 0.03).abs() < 1e-9);
    }

    #[test]
    fn test_mint_sell_requires_visible_bid_sizes() {
        let mut market = make_market(0.49, 0.49);
        market.clob_yes_bid = Some(0.52);
        market.clob_no_bid = Some(0.53);
        market.clob_yes_bid_size = Some(40.0);
        market.clob_no_bid_size = None;
        let event = make_event(vec![market]);
        let cfg = test_config();

        let opps = detect_mint_sell_arbitrage(&event, true, &cfg, 0.0);

        assert!(opps.is_empty());
    }
}
