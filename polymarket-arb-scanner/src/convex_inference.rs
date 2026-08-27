use crate::models::RankedFamily;
use good_lp::{default_solver, variable, variables, Expression, Solution, SolverModel};
use tracing::warn;

fn observed_yes_price(market: &crate::models::Market, use_clob: bool) -> Option<f64> {
    if use_clob {
        market.clob_yes_ask
    } else {
        Some(market.yes_ask(false, 0.02))
    }
}

/// A coherent probability surface for a ranked family.
#[derive(Debug, Clone)]
pub struct CoherentSurface {
    pub matrix: Vec<Vec<f64>>, // [contestant_idx][rank_idx]
}

/// Projects raw (possibly noisy/incoherent) prices onto a coherent probability surface.
/// This uses L1-minimization (absolute distance) to find the closest valid marginal matrix.
pub fn project_to_coherent_surface(
    family: &RankedFamily,
    use_clob: bool,
) -> Option<CoherentSurface> {
    let num_contestants = family.contestants.len();
    let num_ranks = family.ranks.len();

    if num_contestants == 0 || num_ranks == 0 {
        return None;
    }

    let expected_cells = num_contestants.saturating_mul(num_ranks);
    if family.markets.len() != expected_cells {
        return None;
    }
    let mut seen_cells = std::collections::HashSet::with_capacity(expected_cells);
    for inst in &family.markets {
        if inst.contestant_id >= num_contestants || inst.rank_idx >= num_ranks {
            return None;
        }
        if !seen_cells.insert((inst.contestant_id, inst.rank_idx)) {
            return None;
        }
    }

    let mut lp_vars = variables!();

    // X[i][r] is the probability that contestant i finishes in rank r
    let x_vars: Vec<Vec<_>> = (0..num_contestants)
        .map(|_| {
            (0..num_ranks)
                .map(|_| lp_vars.add(variable().min(0.0).max(1.0)))
                .collect()
        })
        .collect();

    // d[i][r] is the absolute difference |X[i][r] - p[i][r]|
    let d_vars: Vec<Vec<_>> = (0..num_contestants)
        .map(|_| {
            (0..num_ranks)
                .map(|_| lp_vars.add(variable().min(0.0)))
                .collect()
        })
        .collect();

    let mut model = lp_vars
        .minimise(
            d_vars
                .iter()
                .flatten()
                .fold(Expression::from(0.0), |acc, &v| acc + v),
        )
        .using(default_solver);

    // 1. Column sums: Each rank must have exactly one winner (sum(i) X[i][r] = 1)
    for r in 0..num_ranks {
        let col_sum = x_vars
            .iter()
            .take(num_contestants)
            .fold(Expression::from(0.0), |acc, row| acc + row[r]);
        model = model.with(col_sum.eq(1.0));
    }

    // 2. Row sums: Each contestant must span at most 1 in total probability (sum(r) X[i][r] <= 1)
    for row in x_vars.iter().take(num_contestants) {
        let row_sum = row
            .iter()
            .take(num_ranks)
            .fold(Expression::from(0.0), |acc, &value| acc + value);
        model = model.with(row_sum.leq(1.0));
    }

    // 3. Absolute difference constraints: -d <= X - p <= d
    for inst in &family.markets {
        let p = observed_yes_price(&inst.market, use_clob)?;
        let i = inst.contestant_id;
        let r = inst.rank_idx;

        // Defensive check: Skip if indices are out of bounds (prevents panic)
        if i >= num_contestants || r >= num_ranks {
            warn!("Skipping market with out-of-bounds indices: contestant {i}/{num_contestants}, rank {r}/{num_ranks}");
            continue;
        }

        // X - p <= d  => X - d <= p
        model = model.with((x_vars[i][r] - d_vars[i][r]).leq(p));
        // p <= X + d  => X + d >= p
        model = model.with((x_vars[i][r] + d_vars[i][r]).geq(p));
    }

    let solution = model.solve().ok()?;

    let mut matrix = vec![vec![0.0; num_ranks]; num_contestants];
    for i in 0..num_contestants {
        for r in 0..num_ranks {
            matrix[i][r] = solution.value(x_vars[i][r]);
        }
    }

    Some(CoherentSurface { matrix })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Market, RankedMarketInstance};

    fn make_market(p: f64) -> Market {
        Market {
            question: "Q".into(),
            condition_id: "C".into(),
            market_slug: "q".into(),
            clob_token_id_yes: "T".into(),
            clob_token_id_no: "N".into(),
            gamma_yes_price: p,
            gamma_no_price: 1.0 - p,
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
            liquidity: 1000.0,
            closed: false,
        }
    }

    #[test]
    fn test_projection_requires_live_quotes_in_clob_mode() {
        let mut m = make_market(0.6);
        m.clob_yes_ask = None;
        let family = RankedFamily {
            id: "test".into(),
            title: "Test Family".into(),
            category: "geopolitics".into(),
            contestants: vec!["A".into()],
            ranks: vec![1],
            markets: vec![RankedMarketInstance {
                contestant_id: 0,
                rank_idx: 0,
                market: m,
            }],
        };
        assert!(project_to_coherent_surface(&family, true).is_none());
    }

    #[test]
    fn test_projection_rejects_incomplete_grid() {
        let family = RankedFamily {
            id: "test".into(),
            title: "Test Family".into(),
            category: "geopolitics".into(),
            contestants: vec!["A".into(), "B".into()],
            ranks: vec![1, 2],
            markets: vec![
                RankedMarketInstance {
                    contestant_id: 0,
                    rank_idx: 0,
                    market: make_market(0.6),
                },
                RankedMarketInstance {
                    contestant_id: 1,
                    rank_idx: 0,
                    market: make_market(0.4),
                },
            ],
        };
        assert!(project_to_coherent_surface(&family, false).is_none());
    }

    #[test]
    fn test_projection_simple_incoherent() {
        // 2 contestants, 2 ranks.
        // Contestant A: Rank 1 (0.6), Rank 2 (0.6) -> Sum 1.2 (Invalid)
        // Contestant B: Rank 1 (0.6), Rank 2 (0.6) -> Sum 1.2 (Invalid)
        // Correct should probably be 0.5 across all to minimize distance.
        let family = RankedFamily {
            id: "test".into(),
            title: "Test Family".into(),
            category: "geopolitics".into(),
            contestants: vec!["A".into(), "B".into()],
            ranks: vec![1, 2],
            markets: vec![
                RankedMarketInstance {
                    contestant_id: 0,
                    rank_idx: 0,
                    market: make_market(0.6),
                },
                RankedMarketInstance {
                    contestant_id: 0,
                    rank_idx: 1,
                    market: make_market(0.6),
                },
                RankedMarketInstance {
                    contestant_id: 1,
                    rank_idx: 0,
                    market: make_market(0.6),
                },
                RankedMarketInstance {
                    contestant_id: 1,
                    rank_idx: 1,
                    market: make_market(0.6),
                },
            ],
        };

        let surf = project_to_coherent_surface(&family, false).unwrap();
        // Check row sums and column sums
        for i in 0..2 {
            let row_sum: f64 = surf.matrix[i].iter().sum();
            assert!((row_sum - 1.0).abs() < 1e-6);
        }
        for r in 0..2 {
            let mut col_sum = 0.0;
            for i in 0..2 {
                col_sum += surf.matrix[i][r];
            }
            assert!((col_sum - 1.0).abs() < 1e-6);
        }
    }
}
