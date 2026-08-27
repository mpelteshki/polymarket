//! Notification system for arbitrage alerts.
//!
//! Supports console logging (default) and optional webhook notifications
//! for Discord or Slack.

use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::models::ArbitrageOpportunity;

const GAMMA_SPREAD_ESTIMATE: f64 = 0.02;
const MAX_WEBHOOK_TIMEOUT_SECS: u64 = 2;

fn webhook_timeout(config: &Config) -> Duration {
    Duration::from_secs(config.api_timeout_secs.clamp(1, MAX_WEBHOOK_TIMEOUT_SECS))
}

fn inferred_gas_cost(opp: &ArbitrageOpportunity) -> f64 {
    opp.estimated_total_gas_cost_usd.max(0.0)
}

fn projected_pnl_for_position(
    opp: &ArbitrageOpportunity,
    requested_position_usd: f64,
) -> Option<(f64, f64, f64, f64)> {
    if requested_position_usd <= f64::EPSILON || opp.total_cost <= f64::EPSILON {
        return None;
    }
    let effective_position_usd =
        if opp.max_executable_size_usd.is_finite() && opp.max_executable_size_usd > 0.0 {
            requested_position_usd.min(opp.max_executable_size_usd)
        } else {
            requested_position_usd
        };
    if effective_position_usd <= f64::EPSILON {
        return None;
    }
    let basket_units = effective_position_usd / opp.total_cost;
    let pnl = basket_units * (opp.gross_profit - opp.total_fees) - inferred_gas_cost(opp);
    let roi_pct = if effective_position_usd > f64::EPSILON {
        pnl / effective_position_usd * 100.0
    } else {
        0.0
    };
    Some((effective_position_usd, basket_units, pnl, roi_pct))
}

/// Format an opportunity into a human-readable string.
pub fn format_opportunity(config: &Config, opp: &ArbitrageOpportunity) -> String {
    let price_source = if opp.prices_from_clob {
        "CLOB"
    } else {
        "Gamma (est.)"
    };
    let inferred_gas = inferred_gas_cost(opp);

    let projected_pnl_per_100 = projected_pnl_for_position(opp, 100.0)
        .map(|(_, _, pnl, _)| pnl)
        .unwrap_or(0.0);
    let basket_edge_ex_gas = opp.gross_profit - opp.total_fees;
    let basket_roi_ex_gas = if opp.total_cost > f64::EPSILON {
        basket_edge_ex_gas / opp.total_cost * 100.0
    } else {
        0.0
    };

    let mut lines = vec![
        format!("🚨 {} ARBITRAGE: {}", opp.arb_type, opp.event_title),
        format!("  Source:       {price_source}"),
        format!(
            "  Basket Edge:  cost=${:.4} revenue=${:.4} gross=${:.4} fees=${:.4} edge_ex_gas=${:.4} edge_roi_ex_gas={:.2}%",
            opp.total_cost,
            opp.guaranteed_revenue,
            opp.gross_profit,
            opp.total_fees,
            basket_edge_ex_gas,
            basket_roi_ex_gas,
        ),
        format!(
            "  Trade PnL:    trade_gas=${:.4} projected_trade_pnl=${:.4} projected_trade_roi={:.2}% projected_pnl_per_$100=${:.4}",
            inferred_gas,
            opp.net_profit,
            opp.roi_pct,
            projected_pnl_per_100,
        ),
        format!(
            "  Capacity:     max_notional=${:.2} est_slippage={:.2}% legs={}",
            opp.max_executable_size_usd,
            opp.expected_slippage_pct,
            if !opp.execution_plan.is_empty() {
                opp.execution_plan.len()
            } else {
                match opp.arb_type {
                    crate::models::ArbType::Bundle | crate::models::ArbType::MintSell => {
                        opp.markets.len() * 2
                    }
                    _ => opp.markets.len(),
                }
            }
        ),
    ];

    if let Some((paper_position_usd, paper_units, paper_pnl, paper_roi)) =
        projected_pnl_for_position(opp, config.effective_paper_position_size_usd())
    {
        let requested = config.effective_paper_position_size_usd();
        lines.push(format!(
            "  Paper target: requested=${:.2} effective=${:.2} basket_units={:.4} projected_pnl=${:.4} projected_roi={:.2}%",
            requested,
            paper_position_usd,
            paper_units,
            paper_pnl,
            paper_roi,
        ));
    }
    if (config.live_trade_position_size_usd - config.effective_paper_position_size_usd()).abs()
        > f64::EPSILON
    {
        if let Some((live_position_usd, live_units, live_pnl, live_roi)) =
            projected_pnl_for_position(opp, config.live_trade_position_size_usd)
        {
            lines.push(format!(
                "  Live target:  requested=${:.2} effective=${:.2} basket_units={:.4} projected_pnl=${:.4} projected_roi={:.2}%",
                config.live_trade_position_size_usd,
                live_position_usd,
                live_units,
                live_pnl,
                live_roi,
            ));
        }
    }

    if !opp.execution_plan.is_empty() {
        lines.push("  Execution plan:".to_string());
        for leg in &opp.execution_plan {
            let Some(market) = opp.markets.get(leg.market_index) else {
                continue;
            };
            let (quote_label, quote, quote_size) = match (opp.arb_type, leg.outcome) {
                (crate::models::ArbType::MintSell, crate::models::OutcomeSide::Yes) => (
                    "bid",
                    market.clob_yes_bid.unwrap_or(leg.reference_price),
                    market.clob_yes_bid_size,
                ),
                (crate::models::ArbType::MintSell, crate::models::OutcomeSide::No) => (
                    "bid",
                    market.clob_no_bid.unwrap_or(leg.reference_price),
                    market.clob_no_bid_size,
                ),
                (_, crate::models::OutcomeSide::Yes) => (
                    "ask",
                    market.yes_ask(opp.prices_from_clob, GAMMA_SPREAD_ESTIMATE),
                    market.clob_yes_ask_size,
                ),
                (_, crate::models::OutcomeSide::No) => (
                    "ask",
                    market.no_ask(opp.prices_from_clob, GAMMA_SPREAD_ESTIMATE),
                    market.clob_no_ask_size,
                ),
            };
            let quote_size_text = quote_size
                .map(|size| format!("{size:.4}"))
                .unwrap_or_else(|| "n/a".to_string());
            lines.push(format!(
                "    - {} | {} | unit_shares={:.4} {}=${:.4} {}_size={} ref=${:.4}",
                leg.question,
                leg.outcome,
                leg.unit_shares,
                quote_label,
                quote,
                quote_label,
                quote_size_text,
                leg.reference_price,
            ));
        }
    } else {
        for market in &opp.markets {
            match opp.arb_type {
                crate::models::ArbType::Bundle => {
                    let yes_ask = market.yes_ask(opp.prices_from_clob, GAMMA_SPREAD_ESTIMATE);
                    let no_ask = market.no_ask(opp.prices_from_clob, GAMMA_SPREAD_ESTIMATE);
                    lines.push(format!("  → {} | YES ask: ${yes_ask:.4}", market.question));
                    lines.push(format!("  → {} | NO  ask: ${no_ask:.4}", market.question));
                }
                crate::models::ArbType::No => {
                    let ask = market.no_ask(opp.prices_from_clob, GAMMA_SPREAD_ESTIMATE);
                    lines.push(format!("  → {} | NO ask: ${ask:.4}", market.question));
                }
                _ => {
                    let ask = market.yes_ask(opp.prices_from_clob, GAMMA_SPREAD_ESTIMATE);
                    lines.push(format!("  → {} | YES ask: ${ask:.4}", market.question));
                }
            }
        }
    }

    lines.push("─".repeat(50));
    lines.join("\n")
}

/// Log the opportunity to console/file.
pub fn notify_console(config: &Config, opp: &ArbitrageOpportunity) {
    info!("\n{}", format_opportunity(config, opp));
}

/// Send the opportunity to a Discord or Slack webhook.
pub async fn notify_webhook(client: &Client, config: &Config, opp: &ArbitrageOpportunity) {
    if config.webhook_url.is_empty() {
        return;
    }

    let text = format_opportunity(config, opp);

    let payload = if config.webhook_url.contains("discord") {
        json!({ "content": format!("```\n{text}\n```") })
    } else {
        json!({ "text": format!("```\n{text}\n```") })
    };

    match client
        .post(&config.webhook_url)
        .timeout(webhook_timeout(config))
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            debug!("Webhook notification sent successfully");
        }
        Ok(resp) => {
            warn!("Webhook returned status {}", resp.status());
        }
        Err(err) => {
            warn!("Webhook notification failed: {err}");
        }
    }
}

/// Send notifications through all configured channels.
pub async fn notify(client: &Client, config: &Config, opp: &ArbitrageOpportunity) {
    notify_console(config, opp);

    if !config.webhook_url.is_empty() {
        notify_webhook(client, config, opp).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ArbType, Market, OpportunityLeg, OutcomeSide};
    use chrono::Utc;
    use httpmock::prelude::*;
    use std::time::Instant;

    fn market(question: &str) -> Market {
        Market {
            question: question.into(),
            condition_id: "cond".into(),
            market_slug: "mkt".into(),
            clob_token_id_yes: "yes".into(),
            clob_token_id_no: "no".into(),
            gamma_yes_price: 0.4,
            gamma_no_price: 0.6,
            clob_yes_ask: Some(0.41),
            clob_yes_bid: Some(0.39),
            clob_no_ask: Some(0.61),
            clob_no_bid: Some(0.59),
            clob_yes_ask_size: Some(50.0),
            clob_yes_bid_size: None,
            clob_no_ask_size: Some(50.0),
            clob_no_bid_size: None,
            fees_enabled: Some(true),
            taker_fee_rate: None,
            maker_fee_rate: None,
            clob_taker_fee_bps: None,
            clob_fee_rate: Some(0.0),
            clob_fee_exponent: None,
            order_price_min_tick_size: Some(0.01),
            order_min_size: Some(1.0),
            clob_tick_size: None,
            clob_min_order_size: None,
            clob_neg_risk: Some(true),
            clob_rfq_enabled: None,
            liquidity: 1000.0,
            closed: false,
        }
    }

    fn config() -> Config {
        let mut cfg = Config::from_env();
        cfg.paper_match_live_position_size = true;
        cfg.live_trade_position_size_usd = 25.0;
        cfg.paper_trade_position_size_usd = 100.0;
        cfg
    }

    fn webhook_opportunity() -> ArbitrageOpportunity {
        ArbitrageOpportunity {
            event_title: "Webhook timeout".into(),
            event_id: "webhook-timeout".into(),
            category: "test".into(),
            arb_type: ArbType::Yes,
            markets: vec![market("Will timeout stay bounded?")],
            execution_plan: vec![],
            total_cost: 0.8,
            guaranteed_revenue: 1.0,
            gross_profit: 0.2,
            total_fees: 0.0,
            net_profit: 0.2,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 25.0,
            prices_from_clob: true,
            max_executable_size_usd: 10.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        }
    }

    #[test]
    fn webhook_timeout_uses_api_timeout_with_low_latency_cap() {
        let mut cfg = config();
        cfg.api_timeout_secs = 15;
        assert_eq!(webhook_timeout(&cfg), Duration::from_secs(2));

        cfg.api_timeout_secs = 1;
        assert_eq!(webhook_timeout(&cfg), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn webhook_request_returns_before_slow_response() {
        let server = MockServer::start_async().await;
        let webhook = server
            .mock_async(|when, then| {
                when.method(POST).path("/webhook");
                then.status(204).delay(Duration::from_secs(3));
            })
            .await;
        let mut cfg = config();
        cfg.webhook_url = server.url("/webhook");
        cfg.api_timeout_secs = 1;

        let started = Instant::now();
        notify_webhook(&Client::new(), &cfg, &webhook_opportunity()).await;

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "webhook request exceeded configured timeout: {:?}",
            started.elapsed()
        );
        webhook.assert_calls_async(1).await;
    }

    #[test]
    fn bundle_format_shows_both_yes_and_no_legs() {
        let cfg = config();
        let opp = ArbitrageOpportunity {
            event_title: "Bundle event".into(),
            event_id: "e1".into(),
            category: "geopolitics".into(),
            arb_type: ArbType::Bundle,
            markets: vec![market("Will X happen?")],
            execution_plan: vec![],
            total_cost: 0.90,
            guaranteed_revenue: 1.0,
            gross_profit: 0.10,
            total_fees: 0.0,
            net_profit: 0.10,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 11.11,
            prices_from_clob: true,
            max_executable_size_usd: 100.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        };

        let text = format_opportunity(&cfg, &opp);
        assert!(text.contains("YES ask"));
        assert!(text.contains("NO  ask"));
    }

    #[test]
    fn projected_pnl_uses_effective_paper_position_size() {
        let cfg = config();
        let opp = ArbitrageOpportunity {
            event_title: "Yes basket".into(),
            event_id: "e2".into(),
            category: "geopolitics".into(),
            arb_type: ArbType::Yes,
            markets: vec![market("A"), market("B")],
            execution_plan: vec![],
            total_cost: 0.80,
            guaranteed_revenue: 1.0,
            gross_profit: 0.20,
            total_fees: 0.0,
            net_profit: 0.18,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 22.5,
            prices_from_clob: true,
            max_executable_size_usd: 100.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        };

        let text = format_opportunity(&cfg, &opp);
        assert!(text.contains("Paper target: requested=$25.00 effective=$25.00"));
        assert!(!text.contains("requested=$100.00 effective=$100.00"));
    }

    #[test]
    fn projected_pnl_caps_display_size_at_max_executable_notional() {
        let cfg = config();
        let opp = ArbitrageOpportunity {
            event_title: "Capped".into(),
            event_id: "e3".into(),
            category: "geopolitics".into(),
            arb_type: ArbType::Yes,
            markets: vec![market("A"), market("B")],
            execution_plan: vec![],
            total_cost: 0.80,
            guaranteed_revenue: 1.0,
            gross_profit: 0.20,
            total_fees: 0.0,
            net_profit: 0.18,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 22.5,
            prices_from_clob: true,
            max_executable_size_usd: 10.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        };

        let text = format_opportunity(&cfg, &opp);
        assert!(text.contains("Paper target: requested=$25.00 effective=$10.00"));
        assert!(!text.contains("effective=$25.00 basket_units"));
    }

    #[test]
    fn execution_plan_format_shows_unit_shares() {
        let cfg = config();
        let opp = ArbitrageOpportunity {
            event_title: "Ranked".into(),
            event_id: "r1".into(),
            category: "sports".into(),
            arb_type: ArbType::Ranked,
            markets: vec![market("A"), market("B")],
            execution_plan: vec![
                OpportunityLeg {
                    market_index: 0,
                    question: "Alice to finish 1st".into(),
                    market_slug: "alice-1st".into(),
                    condition_id: "c1".into(),
                    token_id: "t1".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 0.75,
                    reference_price: 0.42,
                },
                OpportunityLeg {
                    market_index: 1,
                    question: "Bob to finish 2nd".into(),
                    market_slug: "bob-2nd".into(),
                    condition_id: "c2".into(),
                    token_id: "t2".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 0.25,
                    reference_price: 0.31,
                },
            ],
            total_cost: 0.73,
            guaranteed_revenue: 1.0,
            gross_profit: 0.27,
            total_fees: 0.01,
            net_profit: 0.24,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 32.8,
            prices_from_clob: true,
            max_executable_size_usd: 20.0,
            capital_lock_hours: None,
            expected_slippage_pct: 1.2,
            detected_at: Utc::now(),
        };

        let text = format_opportunity(&cfg, &opp);
        assert!(text.contains("Execution plan:"));
        assert!(text.contains("unit_shares=0.7500"));
        assert!(text.contains("projected_pnl"));
        assert!(text.contains("projected_roi"));
    }

    #[test]
    fn mint_sell_execution_plan_formats_bid_quotes() {
        let cfg = config();
        let mut mint_market = market("Will X happen?");
        mint_market.clob_yes_bid = Some(0.52);
        mint_market.clob_yes_bid_size = Some(100.0);
        mint_market.clob_no_bid = Some(0.53);
        mint_market.clob_no_bid_size = Some(50.0);
        let opp = ArbitrageOpportunity {
            event_title: "Mint sell".into(),
            event_id: "ms1".into(),
            category: "crypto".into(),
            arb_type: ArbType::MintSell,
            markets: vec![mint_market],
            execution_plan: vec![
                OpportunityLeg {
                    market_index: 0,
                    question: "Will X happen?".into(),
                    market_slug: "x".into(),
                    condition_id: "c1".into(),
                    token_id: "yes-token".into(),
                    outcome: OutcomeSide::Yes,
                    unit_shares: 1.0,
                    reference_price: 0.52,
                },
                OpportunityLeg {
                    market_index: 0,
                    question: "Will X happen?".into(),
                    market_slug: "x".into(),
                    condition_id: "c1".into(),
                    token_id: "no-token".into(),
                    outcome: OutcomeSide::No,
                    unit_shares: 1.0,
                    reference_price: 0.53,
                },
            ],
            total_cost: 1.0,
            guaranteed_revenue: 1.05,
            gross_profit: 0.05,
            total_fees: 0.0,
            net_profit: 0.04,
            estimated_total_gas_cost_usd: 0.01,
            roi_pct: 4.0,
            prices_from_clob: true,
            max_executable_size_usd: 25.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        };

        let text = format_opportunity(&cfg, &opp);
        assert!(text.contains("MINT_SELL ARBITRAGE"));
        assert!(text.contains("bid=$0.5200 bid_size=100.0000 ref=$0.5200"));
        assert!(text.contains("bid=$0.5300 bid_size=50.0000 ref=$0.5300"));
        assert!(!text.contains("ask=$"));
    }
}
