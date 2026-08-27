//! Advisory live-route planning for blocked arbitrage shapes.
//!
//! These plans are diagnostics only. They describe the missing execution
//! primitive needed before a blocked opportunity can be promoted to live
//! trading.

use std::collections::HashSet;

use crate::combo_rfq_client::{AtomicRouteHint, ComboMarketCatalog, ComboRouteReport};
use crate::models::{ArbType, ArbitrageOpportunity, OutcomeSide};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRouteKind {
    None,
    ComboRfqCandidate,
    CtfMergeBundleCandidate,
    CtfSplitSellCandidate,
}

impl LiveRouteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ComboRfqCandidate => "combo_rfq_candidate",
            Self::CtfMergeBundleCandidate => "ctf_merge_bundle_candidate",
            Self::CtfSplitSellCandidate => "ctf_split_sell_candidate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRouteStatus {
    Unsupported,
    DryRunOnly,
}

impl LiveRouteStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::DryRunOnly => "dry_run_only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedLiveRoutePlan {
    pub kind: LiveRouteKind,
    pub status: LiveRouteStatus,
    pub planned_legs: usize,
    pub unique_conditions: usize,
    pub reason: String,
    pub steps: Vec<&'static str>,
    pub combo_report: Option<ComboRouteReport>,
}

impl BlockedLiveRoutePlan {
    pub fn note(&self) -> String {
        let steps = if self.steps.is_empty() {
            "n/a".to_string()
        } else {
            self.steps.join(">")
        };
        let mut note = format!(
            "live_route={} live_route_status={} planned_legs={} unique_conditions={} route_steps={} route_reason={}",
            self.kind.as_str(),
            self.status.as_str(),
            self.planned_legs,
            self.unique_conditions,
            steps,
            self.reason
        );
        if let Some(combo_report) = &self.combo_report {
            note = format!("{note}; {}", combo_report.note());
        }
        note
    }
}

pub fn plan_blocked_live_route(
    opp: &ArbitrageOpportunity,
    combo_catalog: Option<&ComboMarketCatalog>,
) -> BlockedLiveRoutePlan {
    let planned_legs = opp.execution_plan.len();
    let unique_conditions = unique_condition_count(opp);
    if planned_legs <= 1 {
        return unsupported(
            planned_legs,
            unique_conditions,
            "single_leg_or_empty_plan",
            None,
        );
    }

    if matches!(opp.arb_type, ArbType::MintSell) && is_single_condition_yes_no_pair(opp) {
        return BlockedLiveRoutePlan {
            kind: LiveRouteKind::CtfSplitSellCandidate,
            status: LiveRouteStatus::DryRunOnly,
            planned_legs,
            unique_conditions,
            reason:
                "split_sell_requires_split_receipt_dual_sell_fill_confirmation_and_merge_rollback"
                    .into(),
            steps: vec![
                "splitPosition",
                "verify_split_receipt",
                "sell_yes_fok",
                "sell_no_fok",
                "merge_rollback_on_partial",
            ],
            combo_report: None,
        };
    }

    if matches!(opp.arb_type, ArbType::Bundle) && is_single_condition_yes_no_pair(opp) {
        return BlockedLiveRoutePlan {
            kind: LiveRouteKind::CtfMergeBundleCandidate,
            status: LiveRouteStatus::DryRunOnly,
            planned_legs,
            unique_conditions,
            reason: "bundle_requires_atomic_two_leg_entry_before_ctf_merge".into(),
            steps: vec![
                "buy_yes_fok",
                "buy_no_fok",
                "verify_both_entry_fills",
                "mergePositions",
                "verify_merge_receipt",
            ],
            combo_report: None,
        };
    }

    match combo_catalog {
        Some(catalog) if !catalog.is_empty() => {
            let report = catalog.route_report(opp);
            let kind = if matches!(report.route, AtomicRouteHint::ComboRfqCandidate) {
                LiveRouteKind::ComboRfqCandidate
            } else {
                LiveRouteKind::None
            };
            let status = if matches!(kind, LiveRouteKind::ComboRfqCandidate) {
                LiveRouteStatus::DryRunOnly
            } else {
                LiveRouteStatus::Unsupported
            };
            let reason = if matches!(kind, LiveRouteKind::ComboRfqCandidate) {
                "combo_rfq_requires_authenticated_requester_accept_flow".into()
            } else {
                report.reason.clone()
            };
            BlockedLiveRoutePlan {
                kind,
                status,
                planned_legs,
                unique_conditions,
                reason,
                steps: if matches!(kind, LiveRouteKind::ComboRfqCandidate) {
                    vec![
                        "request_combo_quote",
                        "validate_quote_expiry",
                        "accept_signed_rfq",
                        "verify_combo_fill",
                    ]
                } else {
                    Vec::new()
                },
                combo_report: Some(report),
            }
        }
        Some(_) => unsupported(
            planned_legs,
            unique_conditions,
            "empty_combo_rfq_catalog",
            None,
        ),
        None => unsupported(
            planned_legs,
            unique_conditions,
            "combo_rfq_catalog_unavailable",
            None,
        ),
    }
}

fn unsupported(
    planned_legs: usize,
    unique_conditions: usize,
    reason: &str,
    combo_report: Option<ComboRouteReport>,
) -> BlockedLiveRoutePlan {
    BlockedLiveRoutePlan {
        kind: LiveRouteKind::None,
        status: LiveRouteStatus::Unsupported,
        planned_legs,
        unique_conditions,
        reason: reason.into(),
        steps: Vec::new(),
        combo_report,
    }
}

fn unique_condition_count(opp: &ArbitrageOpportunity) -> usize {
    opp.execution_plan
        .iter()
        .map(|leg| leg.condition_id.trim())
        .filter(|condition_id| !condition_id.is_empty())
        .collect::<HashSet<_>>()
        .len()
}

fn is_single_condition_yes_no_pair(opp: &ArbitrageOpportunity) -> bool {
    if opp.execution_plan.len() != 2 || unique_condition_count(opp) != 1 {
        return false;
    }
    let has_yes = opp
        .execution_plan
        .iter()
        .any(|leg| matches!(leg.outcome, OutcomeSide::Yes));
    let has_no = opp
        .execution_plan
        .iter()
        .any(|leg| matches!(leg.outcome, OutcomeSide::No));
    has_yes && has_no
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::combo_rfq_client::ComboMarketEntry;
    use crate::models::{Market, OpportunityLeg};
    use chrono::Utc;

    fn market(condition_id: &str, question: &str) -> Market {
        Market {
            question: question.into(),
            condition_id: condition_id.into(),
            market_slug: question.to_lowercase().replace(' ', "-"),
            clob_token_id_yes: format!("{condition_id}-yes"),
            clob_token_id_no: format!("{condition_id}-no"),
            gamma_yes_price: 0.5,
            gamma_no_price: 0.5,
            clob_yes_ask: Some(0.49),
            clob_yes_bid: Some(0.48),
            clob_no_ask: Some(0.49),
            clob_no_bid: Some(0.48),
            clob_yes_ask_size: Some(100.0),
            clob_yes_bid_size: Some(100.0),
            clob_no_ask_size: Some(100.0),
            clob_no_bid_size: Some(100.0),
            fees_enabled: Some(false),
            taker_fee_rate: None,
            maker_fee_rate: None,
            clob_taker_fee_bps: None,
            clob_fee_rate: Some(0.0),
            clob_fee_exponent: None,
            order_price_min_tick_size: Some(0.01),
            order_min_size: Some(1.0),
            clob_tick_size: Some(0.01),
            clob_min_order_size: Some(1.0),
            clob_neg_risk: Some(false),
            clob_rfq_enabled: None,
            liquidity: 1_000.0,
            closed: false,
        }
    }

    fn leg(condition_id: &str, token_id: &str, outcome: OutcomeSide) -> OpportunityLeg {
        OpportunityLeg {
            market_index: 0,
            question: condition_id.into(),
            market_slug: condition_id.into(),
            condition_id: condition_id.into(),
            token_id: token_id.into(),
            outcome,
            unit_shares: 1.0,
            reference_price: 0.5,
        }
    }

    fn opportunity(arb_type: ArbType, execution_plan: Vec<OpportunityLeg>) -> ArbitrageOpportunity {
        ArbitrageOpportunity {
            event_title: "Route event".into(),
            event_id: "route-event".into(),
            category: "test".into(),
            arb_type,
            markets: vec![market("cond-a", "A"), market("cond-b", "B")],
            execution_plan,
            total_cost: 0.98,
            guaranteed_revenue: 1.0,
            gross_profit: 0.02,
            total_fees: 0.0,
            net_profit: 0.02,
            estimated_total_gas_cost_usd: 0.0,
            roi_pct: 2.0,
            prices_from_clob: true,
            max_executable_size_usd: 100.0,
            capital_lock_hours: None,
            expected_slippage_pct: 0.0,
            detected_at: Utc::now(),
        }
    }

    #[test]
    fn standard_bundle_route_is_dry_run_ctf_merge_candidate() {
        let opp = opportunity(
            ArbType::Bundle,
            vec![
                leg("cond-a", "cond-a-yes", OutcomeSide::Yes),
                leg("cond-a", "cond-a-no", OutcomeSide::No),
            ],
        );

        let plan = plan_blocked_live_route(&opp, None);

        assert_eq!(plan.kind, LiveRouteKind::CtfMergeBundleCandidate);
        assert_eq!(plan.status, LiveRouteStatus::DryRunOnly);
        assert!(plan.note().contains("mergePositions"));
        assert!(plan
            .note()
            .contains("bundle_requires_atomic_two_leg_entry_before_ctf_merge"));
    }

    #[test]
    fn mint_sell_route_is_dry_run_split_sell_candidate() {
        let opp = opportunity(
            ArbType::MintSell,
            vec![
                leg("cond-a", "cond-a-yes", OutcomeSide::Yes),
                leg("cond-a", "cond-a-no", OutcomeSide::No),
            ],
        );

        let plan = plan_blocked_live_route(&opp, None);

        assert_eq!(plan.kind, LiveRouteKind::CtfSplitSellCandidate);
        assert_eq!(plan.status, LiveRouteStatus::DryRunOnly);
        assert!(plan.note().contains("splitPosition"));
        assert!(plan.note().contains("merge_rollback_on_partial"));
    }

    #[test]
    fn combo_catalog_route_is_dry_run_rfq_candidate() {
        let opp = opportunity(
            ArbType::Yes,
            vec![
                leg("cond-a", "yes-a", OutcomeSide::Yes),
                leg("cond-b", "yes-b", OutcomeSide::Yes),
            ],
        );
        let catalog = ComboMarketCatalog::from_markets(vec![
            ComboMarketEntry {
                condition_id: "cond-a".into(),
                position_ids: vec!["yes-a".into(), "no-a".into()],
                outcomes: vec!["Yes".into(), "No".into()],
                slug: "a".into(),
            },
            ComboMarketEntry {
                condition_id: "cond-b".into(),
                position_ids: vec!["yes-b".into(), "no-b".into()],
                outcomes: vec!["Yes".into(), "No".into()],
                slug: "b".into(),
            },
        ]);

        let plan = plan_blocked_live_route(&opp, Some(&catalog));

        assert_eq!(plan.kind, LiveRouteKind::ComboRfqCandidate);
        assert_eq!(plan.status, LiveRouteStatus::DryRunOnly);
        let note = plan.note();
        assert!(note.contains("request_combo_quote"));
        assert!(note.contains("atomic_route=combo_rfq_candidate"));
        assert!(note.contains("combo_rfq_requester_execution=beta_accept_endpoint_documented"));
        assert!(note.contains("combo_rfq_requester_api_public=false"));
        assert!(note.contains("rfq_quote_window_ms=400"));
        assert!(note.contains("rfq_accept_window_ms=5000"));
        assert!(note.contains("rfq_last_look_ms=1000"));
    }
}
