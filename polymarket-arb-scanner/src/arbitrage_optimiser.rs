use chrono::Utc;
use good_lp::{default_solver, variable, variables, Expression, Solution, SolverModel};
use std::collections::HashSet;
use tracing::debug;

use crate::config::Config;
use crate::convex_inference;
use crate::fees;
use crate::models::{ArbType, ArbitrageOpportunity, OpportunityLeg, OutcomeSide, RankedFamily};

#[derive(Debug, Clone)]
struct TradeLeg {
    contestant_idx: usize,
    rank_idx: usize,
    base_ask_price: f64,
    fee_per_share: f64,
    all_in_ask_price: f64,
    depth_shares: f64,
}

const MAX_EXACT_ASSIGNMENTS: usize = 50_000;
const GAMMA_SPREAD_ESTIMATE: f64 = 0.02;

fn yes_quote_coverage(family: &RankedFamily, require_size: bool) -> f64 {
    if family.markets.is_empty() {
        return 0.0;
    }
    let quoted = family
        .markets
        .iter()
        .filter(|inst| {
            if require_size {
                inst.market.has_full_yes_quote()
            } else {
                inst.market.clob_yes_ask.is_some_and(|price| price > 0.0)
            }
        })
        .count();
    quoted as f64 / family.markets.len() as f64
}

fn compute_roi_pct(net_profit: f64, total_cost: f64) -> f64 {
    if total_cost > f64::EPSILON {
        net_profit / total_cost * 100.0
    } else {
        0.0
    }
}

fn falling_permutations_count(n: usize, k: usize) -> Option<usize> {
    if k > n {
        return Some(0);
    }
    let mut total = 1usize;
    for v in (n - k + 1)..=n {
        total = total.checked_mul(v)?;
    }
    Some(total)
}

fn enumerate_assignments(
    num_contestants: usize,
    num_ranks: usize,
    max_assignments: usize,
) -> Option<Vec<Vec<usize>>> {
    let count = falling_permutations_count(num_contestants, num_ranks)?;
    if count == 0 || count > max_assignments {
        return None;
    }

    let mut used = vec![false; num_contestants];
    let mut current = Vec::with_capacity(num_ranks);
    let mut all = Vec::with_capacity(count);

    fn dfs(
        num_contestants: usize,
        num_ranks: usize,
        rank_idx: usize,
        used: &mut [bool],
        current: &mut Vec<usize>,
        all: &mut Vec<Vec<usize>>,
    ) {
        if rank_idx == num_ranks {
            all.push(current.clone());
            return;
        }

        for contestant_idx in 0..num_contestants {
            if used[contestant_idx] {
                continue;
            }
            used[contestant_idx] = true;
            current.push(contestant_idx);
            dfs(num_contestants, num_ranks, rank_idx + 1, used, current, all);
            current.pop();
            used[contestant_idx] = false;
        }
    }

    dfs(
        num_contestants,
        num_ranks,
        0,
        &mut used,
        &mut current,
        &mut all,
    );

    Some(all)
}

fn build_trade_legs(family: &RankedFamily, use_clob: bool, config: &Config) -> Vec<TradeLeg> {
    family
        .markets
        .iter()
        .filter_map(|inst| {
            if !fees::market_fee_curve_supported(&inst.market) {
                return None;
            }
            let ask = if use_clob {
                inst.market
                    .clob_yes_ask
                    .filter(|ask| ask.is_finite() && *ask > 0.0 && *ask < 1.0)?
            } else {
                inst.market.yes_ask(false, GAMMA_SPREAD_ESTIMATE)
            };
            if ask <= f64::EPSILON || ask >= 1.0 {
                return None;
            }
            let fee_per_share =
                if let Some(schedule) = fees::verified_clob_fee_schedule(&inst.market) {
                    fees::fee_per_share_with_curve(ask, schedule.rate, schedule.exponent)
                } else {
                    let fee_rate = fees::effective_fee_rate(&inst.market, &family.category, config);
                    fees::fee_per_share(ask, fee_rate)
                };
            let depth_shares = if use_clob {
                inst.market
                    .clob_yes_ask_size
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .unwrap_or_else(|| inst.market.liquidity / ask.max(0.01))
            } else {
                inst.market.liquidity / ask.max(0.01)
            };
            if depth_shares <= f64::EPSILON {
                return None;
            }

            Some(TradeLeg {
                contestant_idx: inst.contestant_id,
                rank_idx: inst.rank_idx,
                base_ask_price: ask,
                fee_per_share,
                all_in_ask_price: ask + fee_per_share,
                depth_shares,
            })
        })
        .collect()
}

fn worst_case_payout(legs: &[TradeLeg], x_vals: &[f64], assignment: &[usize]) -> f64 {
    legs.iter()
        .zip(x_vals)
        .filter(|(leg, _)| assignment[leg.rank_idx] == leg.contestant_idx)
        .map(|(_, x)| *x)
        .sum()
}

fn family_has_complete_grid(family: &RankedFamily) -> bool {
    let expected = family.contestants.len().saturating_mul(family.ranks.len());
    if expected == 0 || family.markets.len() != expected {
        return false;
    }
    let mut seen = HashSet::with_capacity(expected);
    for inst in &family.markets {
        if inst.contestant_id >= family.contestants.len() || inst.rank_idx >= family.ranks.len() {
            return false;
        }
        if !seen.insert((inst.contestant_id, inst.rank_idx)) {
            return false;
        }
    }
    true
}

pub fn optimize_ranked_bundle(
    family: &RankedFamily,
    use_clob: bool,
    gas_cost_usd: f64,
    config: &Config,
) -> Vec<ArbitrageOpportunity> {
    let _min_profit_usd = config.min_net_profit_usd;
    let min_roi_pct = config.min_roi_pct;
    let num_contestants = family.contestants.len();
    let num_ranks = family.ranks.len();

    if num_contestants == 0 || num_ranks == 0 || num_contestants < num_ranks {
        return vec![];
    }
    if !family_has_complete_grid(family) {
        debug!(
            "Skipping ranked family '{}' because the contestant/rank grid is incomplete",
            family.title
        );
        return vec![];
    }

    if use_clob {
        let coverage = yes_quote_coverage(family, config.execute_only_full_clob_prices);
        if coverage < config.min_clob_quote_coverage_pct {
            return vec![];
        }
    }

    if family
        .markets
        .iter()
        .any(|inst| !fees::market_fee_curve_supported(&inst.market))
    {
        return vec![];
    }

    if let Some(surface) = convex_inference::project_to_coherent_surface(family, use_clob) {
        let mut total_deviation = 0.0;
        for inst in &family.markets {
            let p = inst.market.yes_ask(use_clob, GAMMA_SPREAD_ESTIMATE);
            let coherent_p = surface.matrix[inst.contestant_id][inst.rank_idx];
            total_deviation += (p - coherent_p).abs();
        }

        if total_deviation < 0.001 {
            debug!(
                "Family {} is coherent (deviation {:.4}), skipping ranked optimization",
                family.title, total_deviation
            );
            return vec![];
        }
    }

    let legs = build_trade_legs(family, use_clob, config);
    if legs.is_empty() {
        return vec![];
    }

    let assignments = match enumerate_assignments(num_contestants, num_ranks, MAX_EXACT_ASSIGNMENTS)
    {
        Some(assignments) => assignments,
        None => {
            debug!(
                "Skipping ranked family '{}' because exact enumeration would exceed {} assignments",
                family.title, MAX_EXACT_ASSIGNMENTS
            );
            return vec![];
        }
    };

    let mut vars = variables!();
    let t = vars.add(variable().min(-1_000_000.0));
    let x_vars: Vec<_> = legs
        .iter()
        .map(|leg| vars.add(variable().min(0.0).max(leg.depth_shares)))
        .collect();

    let mut model = vars.maximise(t).using(default_solver);
    for assignment in &assignments {
        let cost_expr = legs
            .iter()
            .zip(&x_vars)
            .fold(Expression::from(0.0), |acc, (leg, &x)| {
                acc + x * leg.all_in_ask_price
            });
        let payout_expr = legs
            .iter()
            .zip(&x_vars)
            .filter(|(leg, _)| assignment[leg.rank_idx] == leg.contestant_idx)
            .fold(Expression::from(0.0), |acc, (_, &x)| acc + x);
        model = model.with((t - payout_expr + cost_expr).leq(0.0));
    }

    let solution = match model.solve() {
        Ok(solution) => solution,
        Err(err) => {
            debug!(
                "Ranked optimizer failed to solve family '{}': {}",
                family.title, err
            );
            return vec![];
        }
    };

    let x_vals: Vec<f64> = x_vars.iter().map(|&v| solution.value(v)).collect();
    let base_cost: f64 = legs
        .iter()
        .zip(&x_vals)
        .map(|(leg, x)| leg.base_ask_price * x)
        .sum();
    if base_cost <= f64::EPSILON {
        return vec![];
    }

    let total_fees: f64 = legs
        .iter()
        .zip(&x_vals)
        .map(|(leg, x)| x * leg.fee_per_share)
        .sum();
    let total_cost = base_cost + total_fees;
    let worst_payout = assignments
        .iter()
        .map(|assignment| worst_case_payout(&legs, &x_vals, assignment))
        .fold(f64::INFINITY, f64::min);
    let gross_profit = worst_payout - base_cost;
    let net_profit = gross_profit - total_fees;
    if net_profit <= 0.0 {
        return vec![];
    }
    let roi_pct = compute_roi_pct(net_profit, total_cost);

    if roi_pct < min_roi_pct {
        return vec![];
    }

    let selected_plan: Vec<OpportunityLeg> = legs
        .iter()
        .zip(x_vals.iter().copied())
        .filter(|(_, shares)| *shares > 1e-8)
        .map(|(leg, shares)| {
            let inst = family
                .markets
                .iter()
                .find(|inst| {
                    inst.contestant_id == leg.contestant_idx && inst.rank_idx == leg.rank_idx
                })
                .expect("ranked trade leg must map back to family market");
            OpportunityLeg {
                market_index: family
                    .markets
                    .iter()
                    .position(|candidate| {
                        candidate.contestant_id == inst.contestant_id
                            && candidate.rank_idx == inst.rank_idx
                    })
                    .expect("ranked family index must exist"),
                question: inst.market.question.clone(),
                market_slug: inst.market.market_slug.clone(),
                condition_id: inst.market.condition_id.clone(),
                token_id: inst.market.clob_token_id_yes.clone(),
                outcome: OutcomeSide::Yes,
                unit_shares: shares,
                reference_price: leg.base_ask_price,
            }
        })
        .collect();

    if selected_plan.is_empty() {
        return vec![];
    }
    let selected_prices_from_clob = use_clob
        && selected_plan.iter().all(|leg| {
            family
                .markets
                .get(leg.market_index)
                .and_then(|inst| inst.market.clob_yes_ask)
                .is_some_and(|ask| ask.is_finite() && ask > 0.0)
        });

    vec![ArbitrageOpportunity {
        event_title: family.title.clone(),
        event_id: family.id.clone(),
        category: family.category.clone(),
        arb_type: ArbType::Ranked,
        markets: family.markets.iter().map(|m| m.market.clone()).collect(),
        execution_plan: selected_plan,
        total_cost,
        guaranteed_revenue: worst_payout,
        gross_profit,
        total_fees,
        net_profit,
        estimated_total_gas_cost_usd: gas_cost_usd,
        roi_pct,
        prices_from_clob: selected_prices_from_clob,
        max_executable_size_usd: total_cost,
        capital_lock_hours: None,
        expected_slippage_pct: 0.0,
        detected_at: Utc::now(),
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Market, RankedMarketInstance};

    fn market() -> Market {
        Market {
            question: "Q".into(),
            condition_id: "cond".into(),
            market_slug: "q".into(),
            clob_token_id_yes: "y".into(),
            clob_token_id_no: "n".into(),
            gamma_yes_price: 0.4,
            gamma_no_price: 0.6,
            clob_yes_ask: Some(0.4),
            clob_yes_bid: Some(0.39),
            clob_no_ask: Some(0.6),
            clob_no_bid: Some(0.59),
            clob_yes_ask_size: Some(100.0),
            clob_yes_bid_size: None,
            clob_no_ask_size: Some(100.0),
            clob_no_bid_size: None,
            fees_enabled: Some(true),
            taker_fee_rate: Some(0.0),
            maker_fee_rate: Some(0.0),
            clob_taker_fee_bps: Some(0),
            clob_fee_rate: Some(0.0),
            clob_fee_exponent: Some(1),
            order_price_min_tick_size: Some(0.01),
            order_min_size: Some(1.0),
            clob_tick_size: Some(0.01),
            clob_min_order_size: Some(1.0),
            clob_neg_risk: Some(true),
            clob_rfq_enabled: None,
            liquidity: 1000.0,
            closed: false,
        }
    }

    #[test]
    fn complete_grid_check_rejects_missing_cell() {
        let family = RankedFamily {
            id: "f".into(),
            title: "F".into(),
            category: "sports".into(),
            contestants: vec!["A".into(), "B".into()],
            ranks: vec![1, 2],
            markets: vec![
                RankedMarketInstance {
                    contestant_id: 0,
                    rank_idx: 0,
                    market: market(),
                },
                RankedMarketInstance {
                    contestant_id: 0,
                    rank_idx: 1,
                    market: market(),
                },
                RankedMarketInstance {
                    contestant_id: 1,
                    rank_idx: 0,
                    market: market(),
                },
            ],
        };
        assert!(!family_has_complete_grid(&family));
    }

    #[test]
    fn complete_grid_check_accepts_full_rectangle() {
        let family = RankedFamily {
            id: "f".into(),
            title: "F".into(),
            category: "sports".into(),
            contestants: vec!["A".into(), "B".into()],
            ranks: vec![1, 2],
            markets: vec![
                RankedMarketInstance {
                    contestant_id: 0,
                    rank_idx: 0,
                    market: market(),
                },
                RankedMarketInstance {
                    contestant_id: 0,
                    rank_idx: 1,
                    market: market(),
                },
                RankedMarketInstance {
                    contestant_id: 1,
                    rank_idx: 0,
                    market: market(),
                },
                RankedMarketInstance {
                    contestant_id: 1,
                    rank_idx: 1,
                    market: market(),
                },
            ],
        };
        assert!(family_has_complete_grid(&family));
    }

    #[test]
    fn ranked_optimizer_profit_accounting_is_consistent_for_simple_family() {
        let mut cfg = Config::from_env();
        cfg.min_net_profit_usd = 0.01;
        cfg.min_roi_pct = 0.0;

        let mut single = market();
        single.gamma_yes_price = 0.4;
        single.gamma_no_price = 0.6;
        single.clob_yes_ask = Some(0.4);
        single.clob_yes_ask_size = Some(10.0);
        single.liquidity = 100.0;

        let family = RankedFamily {
            id: "simple".into(),
            title: "Simple".into(),
            category: "sports".into(),
            contestants: vec!["A".into()],
            ranks: vec![1],
            markets: vec![RankedMarketInstance {
                contestant_id: 0,
                rank_idx: 0,
                market: single,
            }],
        };

        let opps = optimize_ranked_bundle(&family, true, 0.0, &cfg);
        assert_eq!(opps.len(), 1);
        let opp = &opps[0];
        assert!((opp.total_cost - 4.0).abs() < 1e-9);
        assert!((opp.guaranteed_revenue - 10.0).abs() < 1e-9);
        assert!((opp.gross_profit - 6.0).abs() < 1e-9);
        assert!((opp.total_fees - 0.0).abs() < 1e-9);
        assert!((opp.net_profit - 6.0).abs() < 1e-9);
        assert!((opp.roi_pct - 150.0).abs() < 1e-9);
    }

    #[test]
    fn ranked_optimizer_rejects_selected_leg_without_clob_ask_in_clob_mode() {
        let mut cfg = Config::from_env();
        cfg.min_net_profit_usd = 0.01;
        cfg.min_roi_pct = 0.0;
        cfg.min_clob_quote_coverage_pct = 0.0;
        cfg.execute_only_full_clob_prices = false;

        let mut single = market();
        single.gamma_yes_price = 0.2;
        single.clob_yes_ask = None;
        single.clob_yes_ask_size = None;
        single.liquidity = 100.0;

        let family = RankedFamily {
            id: "relaxed".into(),
            title: "Relaxed".into(),
            category: "sports".into(),
            contestants: vec!["A".into()],
            ranks: vec![1],
            markets: vec![RankedMarketInstance {
                contestant_id: 0,
                rank_idx: 0,
                market: single,
            }],
        };

        let opps = optimize_ranked_bundle(&family, true, 0.0, &cfg);

        assert!(opps.is_empty());
    }

    #[test]
    fn ranked_optimizer_prices_documented_fee_exponents() {
        let mut cfg = Config::from_env();
        cfg.min_roi_pct = 0.0;

        let mut single = market();
        single.clob_fee_rate = Some(0.02);
        single.clob_fee_exponent = Some(3);

        let family = RankedFamily {
            id: "bad-fee".into(),
            title: "Bad Fee".into(),
            category: "sports".into(),
            contestants: vec!["A".into()],
            ranks: vec![1],
            markets: vec![RankedMarketInstance {
                contestant_id: 0,
                rank_idx: 0,
                market: single,
            }],
        };

        let legs = build_trade_legs(&family, true, &cfg);

        assert_eq!(legs.len(), 1);
        assert!(
            (legs[0].fee_per_share - fees::fee_per_share_with_curve(0.4, 0.02, 3)).abs() < 1e-12
        );
    }
}
