import csv
import hashlib
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "paper_profitability_gate.py"
SPEC = importlib.util.spec_from_file_location("paper_profitability_gate", MODULE_PATH)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


FIELDS = [
    "timestamp",
    "scan_id",
    "mode",
    "status",
    "pnl_scale",
    "event_id",
    "event_title",
    "arb_type",
    "legs_summary",
    "target_position_usd",
    "projected_net_profit",
    "projected_roi_pct",
    "filled_cost_usd",
    "conservative_pnl_usd",
    "conservative_roi_pct",
    "planned_basket_units",
    "hedged_basket_units",
    "fill_count",
    "partial_fill",
    "parity_ok",
    "unhedged_notional_usd",
    "prices_from_clob",
    "note",
]


def thresholds():
    return {
        "min_trades": 3,
        "min_unique_events": 2,
        "min_observation_hours": 2,
        "max_evidence_age_hours": 1,
        "min_total_pnl_usd": 1,
        "min_weighted_roi_pct": 0.25,
        "min_lower_mean_pnl_usd": 0,
        "min_event_lower_mean_pnl_usd": 0,
        "min_fill_success_rate": 0.8,
        "min_positive_trade_rate": 0.8,
        "max_drawdown_usd": 1,
        "max_unhedged_notional_usd": 0,
    }


def paper_row(
    timestamp,
    scan_id,
    event_id,
    pnl="0.50",
    *,
    pre_submit_code="final_profit",
    **overrides,
):
    default_unhedged = float(overrides.get("unhedged_notional_usd", "0") or 0)
    default_gas = 0.10
    default_cost = 10.0 - float(pnl) - default_unhedged - default_gas
    row = {field: "" for field in FIELDS}
    row.update(
        {
            "timestamp": timestamp.isoformat(),
            "scan_id": str(scan_id),
            "mode": "paper",
            "status": "ok",
            "pnl_scale": "filled_hedged",
            "event_id": event_id,
            "arb_type": "YES",
            "target_position_usd": "10.00",
            "filled_cost_usd": f"{default_cost:.12f}",
            "conservative_pnl_usd": pnl,
            "conservative_roi_pct": "",
            "planned_basket_units": "10.00",
            "hedged_basket_units": "10.00",
            "fill_count": "2",
            "partial_fill": "false",
            "parity_ok": "true",
            "unhedged_notional_usd": "0.00",
            "prices_from_clob": "true",
            "note": "real scanner paper execution",
        }
    )
    row.update(overrides)
    if not row["conservative_roi_pct"] and row["conservative_pnl_usd"]:
        outflow = float(row["filled_cost_usd"]) + float(
            row["unhedged_notional_usd"] or 0
        )
        row["conservative_roi_pct"] = f"{float(row['conservative_pnl_usd']) / outflow * 100:.12f}"
    if not row["event_id"].startswith("synthetic-"):
        if row["status"] == "pre_submit_rejected":
            row["note"] = (
                f"{GATE.PAPER_PRE_SUBMIT_REJECTION_PREFIX}={pre_submit_code}; "
                "fresh market validation removed the edge"
            )
            return row
        attempt_status = {
            "ok": "accepted",
            "parity_rejected": "rejected",
        }.get(row["status"], "error")
        row["note"] = (
            f"{row['note']}; paper_attempt_id=attempt-{scan_id}; "
            f"paper_attempt_status={attempt_status}"
        )
    return row


class ProfitabilityGateTests(unittest.TestCase):
    def evaluate(self, rows, now):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trades.csv"
            attempts_path = Path(directory) / "paper_execution_attempts.jsonl"
            with path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            self.write_attempts(attempts_path, rows)
            return GATE.evaluate(
                path,
                thresholds(),
                now=now,
                attempts_jsonl=attempts_path,
            )

    @staticmethod
    def write_attempts(path, rows):
        previous_latest_trade_id = 0
        with path.open("w") as handle:
            for row in rows:
                if (
                    row["mode"] != "paper"
                    or row["event_id"].startswith("synthetic-")
                    or row["status"] == "pre_submit_rejected"
                ):
                    continue
                attempt_id = f"attempt-{row['scan_id']}"
                producer_sha = "1" * 64
                adapter_sha = "2" * 64
                profile = {
                    "schema_version": 1,
                    "execution_route": "legged_clob_paper",
                    "live_route_compatible": False,
                    "order_mode": "market_style",
                    "effective_order_type": "fok",
                    "live_order_type": "fok",
                    "paper_use_limit_orders_requested": True,
                    "effective_paper_use_limit_orders": False,
                    "full_clob_required": True,
                    "match_live_position_size": True,
                    "effective_position_size_usd": 10.0,
                    "live_position_size_usd": 10.0,
                    "paper_max_share_mismatch_pct": 0.5,
                    "min_net_profit_usd": 0.0,
                    "min_roi_pct": 0.0,
                    "max_signal_age_secs": 5,
                    "gas_fallback_usd": 0.05,
                    "assume_gasless_for_proxy_signature_types": False,
                    "live_signature_type": 0,
                    "exclusive_paper_account_lock": True,
                    "order_size_step_shares": 0.01,
                    "validate_opportunities_at_target_size": True,
                    "execute_only_full_clob_prices": True,
                    "live_slippage_bps": 10,
                    "live_edge_haircut_usd": 0.0,
                    "live_edge_haircut_bps": 0,
                    "live_min_leg_size_usd": 1.0,
                    "live_max_refresh_to_submit_ms": 1000,
                    "fresh_clob_enrichment_complete": True,
                    "fresh_depth_complete": True,
                    "fresh_fee_schedules_complete": True,
                    "pre_submit_orderability_complete": True,
                    "clob_api_url": "https://clob.polymarket.com",
                    "gamma_api_url": "https://gamma-api.polymarket.com",
                    "external_paper_command": "pm-trader",
                    "external_paper_executable_path": "/test/pm-trader",
                    "external_paper_executable_sha256": adapter_sha,
                    "producer_version": "test",
                    "producer_executable_sha256": producer_sha,
                }
                baseline_trade_id = previous_latest_trade_id
                common = {
                    "schema_version": GATE.PAPER_EXECUTION_ATTEMPT_SCHEMA_VERSION,
                    "attempt_id": attempt_id,
                    "recorded_at": row["timestamp"],
                    "event_id": row["event_id"],
                    "arb_type": row["arb_type"],
                    "account": "test",
                    "data_dir": "/tmp/test",
                    "account_lock_key": "6" * 64,
                    "baseline_trade_id": baseline_trade_id,
                    "execution_route": "legged_clob_paper",
                    "live_route_compatible": False,
                    "order_mode": "market_style",
                    "effective_order_type": "fok",
                    "live_order_type": "fok",
                    "full_clob_required": True,
                    "match_live_position_size": True,
                    "effective_position_size_usd": 10.0,
                    "config_fingerprint": "0x" + "3" * 64,
                    "launch_config_fingerprint": "0x" + "4" * 64,
                    "profit_compatibility_fingerprint": "0x" + "5" * 64,
                    "config_field_count": 100,
                    "producer_version": "test",
                    "producer_executable_sha256": producer_sha,
                    "external_paper_executable_sha256": adapter_sha,
                    "execution_profile_sha256": GATE.canonical_json_sha256(profile),
                }
                planned_basket_units = float(row["planned_basket_units"] or 10)
                terminal_status = {
                    "ok": "accepted",
                    "parity_rejected": "rejected",
                }.get(row["status"], "error")
                if terminal_status in {"accepted", "rejected"}:
                    hedged_cost = float(row["filled_cost_usd"])
                    unhedged = float(row["unhedged_notional_usd"])
                    hedged_units = float(row["hedged_basket_units"])
                    pnl = float(row["conservative_pnl_usd"])
                    gas_policy_floor_usd = 0.10
                    gas_cost_usd = 0.10
                    guaranteed_revenue = 1.0
                else:
                    hedged_cost = 9.4
                    unhedged = 0.0
                    hedged_units = planned_basket_units
                    pnl = 0.5
                    gas_policy_floor_usd = 0.10
                    gas_cost_usd = 0.10
                    guaranteed_revenue = 1.0
                planned_limit_price = hedged_cost / 2.0 / planned_basket_units
                planned_legs = [
                    {
                        "condition_id": "condition-a",
                        "token_id": "token-a",
                        "market_slug": "slug-a",
                        "outcome": "yes",
                        "unit_shares": 1.0,
                        "shares": planned_basket_units,
                        "amount_usd": hedged_cost / 2.0,
                        "limit_price": planned_limit_price,
                        "fee_rate": 0.0,
                        "fee_exponent": 1,
                    },
                    {
                        "condition_id": "condition-b",
                        "token_id": "token-b",
                        "market_slug": "slug-b",
                        "outcome": "yes",
                        "unit_shares": 1.0,
                        "shares": planned_basket_units,
                        "amount_usd": hedged_cost / 2.0,
                        "limit_price": planned_limit_price,
                        "fee_rate": 0.0,
                        "fee_exponent": 1,
                    },
                ]
                payoff_certificate = {
                    "schema_version": 1,
                    "arb_type": row["arb_type"],
                    "supported_for_profit_evidence": row["arb_type"] == "YES",
                    "topology": (
                        "yes_full_family"
                        if row["arb_type"] == "YES"
                        else "unsupported_arb_type"
                    ),
                    "raw_market_count": 2,
                    "raw_condition_ids": ["condition-a", "condition-b"],
                    "derived_guaranteed_revenue_per_basket_unit": (
                        1.0 if row["arb_type"] == "YES" else None
                    ),
                }
                common["payoff_certificate_sha256"] = GATE.canonical_json_sha256(
                    payoff_certificate
                )

                handle.write(
                    json.dumps(
                        {
                            **common,
                            "stage": "started",
                            "status": "started",
                            "execution_profile": profile,
                            "planned_basket_units": planned_basket_units,
                            "payoff_certificate": payoff_certificate,
                            "guaranteed_revenue_per_basket_unit": guaranteed_revenue,
                            "gas_policy_floor_usd": gas_policy_floor_usd,
                            "gas_cost_usd": gas_cost_usd,
                            "projected_cost_usd": 10.0,
                            "projected_pnl_usd": 0.5,
                            "projected_roi_pct": 5.0,
                            "leg_count": 2,
                            "planned_legs": planned_legs,
                        }
                    )
                    + "\n"
                )
                raw_trade_ids = [baseline_trade_id + 1, baseline_trade_id + 2]
                partial = row["partial_fill"].lower() == "true"
                first_amount = hedged_cost / 2.0
                second_amount = hedged_cost / 2.0 + unhedged
                first_shares = hedged_units
                fill_price = first_amount / first_shares
                second_shares = hedged_units + unhedged / fill_price
                filled_legs = [
                    {
                        "market_slug": "slug-a",
                        "outcome": "yes",
                        "label": "A",
                        "unit_shares": 1.0,
                        "shares": first_shares,
                        "notional_usd": first_amount,
                        "avg_price": first_amount / first_shares,
                        "is_partial": False,
                        "fee_rate": 0.0,
                        "fee_exponent": 1,
                        "recomputed_fee_usd": 0.0,
                        "submission_kind": "market_trade",
                        "submission_id": raw_trade_ids[0],
                        "attribution_mode": "direct_trade_id",
                        "trade_ids": [raw_trade_ids[0]],
                        "raw_trades": [
                            {
                                "trade_id": raw_trade_ids[0],
                                "shares": first_shares,
                                "amount_usd": first_amount,
                                "avg_price": first_amount / first_shares,
                                "is_partial": False,
                                "fee_usd": 0.0,
                            }
                        ],
                    },
                    {
                        "market_slug": "slug-b",
                        "outcome": "yes",
                        "label": "B",
                        "unit_shares": 1.0,
                        "shares": second_shares,
                        "notional_usd": second_amount,
                        "avg_price": second_amount / second_shares,
                        "is_partial": partial,
                        "fee_rate": 0.0,
                        "fee_exponent": 1,
                        "recomputed_fee_usd": 0.0,
                        "submission_kind": "market_trade",
                        "submission_id": raw_trade_ids[1],
                        "attribution_mode": "direct_trade_id",
                        "trade_ids": [raw_trade_ids[1]],
                        "raw_trades": [
                            {
                                "trade_id": raw_trade_ids[1],
                                "shares": second_shares,
                                "amount_usd": second_amount,
                                "avg_price": second_amount / second_shares,
                                "is_partial": partial,
                                "fee_usd": 0.0,
                            }
                        ],
                    },
                ]
                handle.write(
                    json.dumps(
                        {
                            **common,
                            "stage": "terminal",
                            "status": terminal_status,
                            **(
                                {
                                    "parity_ok": row["parity_ok"].lower() == "true",
                                    "any_partial": row["partial_fill"].lower()
                                    == "true",
                                    "fill_count": int(row["fill_count"] or 1),
                                    "planned_basket_units": float(
                                        row["planned_basket_units"]
                                    ),
                                    "hedged_basket_units": float(
                                        row["hedged_basket_units"]
                                    ),
                                    "hedged_cost_usd": float(
                                        row["filled_cost_usd"]
                                    ),
                                    "conservative_pnl_usd": float(
                                        row["conservative_pnl_usd"]
                                    ),
                                    "conservative_roi_pct": float(
                                        row["conservative_roi_pct"]
                                    ),
                                    "unhedged_notional_usd": float(
                                        row["unhedged_notional_usd"]
                                    ),
                                    "raw_trade_count": len(raw_trade_ids),
                                    "raw_trade_ids": raw_trade_ids,
                                    "filled_legs": filled_legs,
                                    "total_fill_notional_usd": first_amount
                                    + second_amount,
                                    "total_recomputed_fees_usd": 0.0,
                                    "guaranteed_revenue_per_basket_unit": guaranteed_revenue,
                                    "gas_policy_floor_usd": gas_policy_floor_usd,
                                    "gas_cost_usd": gas_cost_usd,
                                }
                                if terminal_status in {"accepted", "rejected"}
                                else {"error": "test error"}
                            ),
                        }
                    )
                    + "\n"
                )
                if terminal_status in {"accepted", "rejected"}:
                    previous_latest_trade_id = raw_trade_ids[-1]

    def test_passes_diverse_fresh_complete_sample(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b", pnl="0.40"),
            paper_row(start + timedelta(hours=2), 3, "event-a", pnl="0.60"),
        ]
        report = self.evaluate(rows, start + timedelta(hours=2, minutes=30))
        self.assertTrue(report["verified_profitable"])
        self.assertTrue(report["paper_evidence_eligible"])
        self.assertFalse(report["live_route_compatible"])
        self.assertFalse(report["activation_eligible"])
        self.assertEqual(report["sample"]["accepted_trades"], 3)
        self.assertGreater(
            report["metrics"]["one_sided_95_conservative_lower_mean_pnl_usd"], 0
        )
        self.assertGreater(
            report["metrics"][
                "one_sided_95_conservative_event_clustered_lower_mean_pnl_usd"
            ],
            0,
        )

    def test_economic_tamper_breaks_journal_trade_binding(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            self.write_attempts(attempts_path, rows)
            rows[0]["conservative_pnl_usd"] = "99.00"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertIn(
            "paper_attempt_journal_economics_match_trades",
            "\n".join(report["blockers"]),
        )
        self.assertGreater(report["execution_binding"]["binding_error_count"], 0)

    def test_arb_type_tamper_breaks_journal_trade_binding(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            self.write_attempts(attempts_path, rows)
            rows[0]["arb_type"] = "tampered"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertIn(
            "paper_attempt_journal_economics_match_trades",
            "\n".join(report["blockers"]),
        )
        self.assertIn(
            "attempt-1:arb_type",
            report["execution_binding"]["binding_errors"],
        )

    def test_expected_binary_hash_mismatches_are_activation_blockers(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            self.write_attempts(attempts_path, rows)
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
                expected_producer_binary_sha256="9" * 64,
                expected_adapter_executable_sha256="8" * 64,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertFalse(report["paper_evidence_eligible"])
        self.assertFalse(report["activation_eligible"])
        self.assertFalse(report["execution_binding"]["expected_producer_matches"])
        self.assertFalse(report["execution_binding"]["expected_adapter_matches"])
        self.assertIn(
            "expected_producer_binary_matches",
            "\n".join(report["activation_blockers"]),
        )
        self.assertIn(
            "expected_adapter_executable_matches",
            "\n".join(report["activation_blockers"]),
        )

    def test_execution_profile_tamper_invalidates_evidence(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            self.write_attempts(attempts_path, rows)
            records = [json.loads(line) for line in attempts_path.read_text().splitlines()]
            records[0]["execution_profile"]["effective_order_type"] = "fak"
            attempts_path.write_text(
                "".join(json.dumps(record) + "\n" for record in records)
            )
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertGreater(report["execution_attempts"]["malformed_records"], 0)
        self.assertIn("paper_attempt_journal_valid", "\n".join(report["blockers"]))

    def test_excludes_synthetic_rows_from_profit_sample(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "synthetic-proof", note="synthetic proof"),
            paper_row(start + timedelta(hours=2), 2, "event-a"),
        ]
        report = self.evaluate(rows, start + timedelta(hours=2, minutes=10))
        self.assertFalse(report["verified_profitable"])
        self.assertEqual(report["sample"]["accepted_trades"], 1)
        self.assertEqual(report["sample"]["excluded_synthetic_or_canary_rows"], 1)

    def test_duplicate_and_unhedged_rows_block(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        first = paper_row(start, 1, "event-a")
        rows = [
            first,
            dict(first),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(
                start + timedelta(hours=2),
                3,
                "event-c",
                unhedged_notional_usd="0.01",
            ),
        ]
        report = self.evaluate(rows, start + timedelta(hours=2, minutes=10))
        self.assertFalse(report["verified_profitable"])
        self.assertEqual(report["sample"]["duplicate_rows"], 1)
        blocker_text = "\n".join(report["blockers"])
        self.assertIn("no_duplicate_rows", blocker_text)
        self.assertIn("max_unhedged_notional_usd", blocker_text)

    def test_rejected_losses_count_and_unknown_errors_block(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b", pnl="0.40"),
            paper_row(
                start + timedelta(hours=1, minutes=30),
                3,
                "event-c",
                pnl="-0.80",
                status="parity_rejected",
                parity_ok="false",
                partial_fill="true",
                unhedged_notional_usd="0.80",
            ),
            paper_row(start + timedelta(hours=2), 4, "event-a", pnl="0.60"),
            paper_row(
                start + timedelta(hours=2, minutes=5),
                5,
                "event-d",
                status="error",
                filled_cost_usd="",
                conservative_pnl_usd="",
            ),
        ]
        report = self.evaluate(rows, start + timedelta(hours=2, minutes=30))
        self.assertFalse(report["verified_profitable"])
        self.assertEqual(report["sample"]["accepted_trades"], 3)
        self.assertEqual(report["sample"]["completed_trade_rows"], 4)
        self.assertEqual(report["sample"]["parity_rejected_attempts"], 1)
        self.assertEqual(report["sample"]["unaccounted_error_attempts"], 1)
        self.assertEqual(report["sample"]["completed_event_clusters"], 3)
        self.assertAlmostEqual(report["metrics"]["total_conservative_pnl_usd"], 0.70)
        blocker_text = "\n".join(report["blockers"])
        self.assertIn("complete_hedges", blocker_text)
        self.assertIn("no_unaccounted_execution_errors", blocker_text)
        self.assertIn("no_terminal_paper_attempt_errors", blocker_text)

    def test_event_clustered_bound_blocks_one_event_dominance(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(
                start + timedelta(minutes=index),
                index + 1,
                "event-dominant",
                pnl="0.50",
            )
            for index in range(100)
        ]
        rows.append(
            paper_row(
                start + timedelta(hours=2),
                101,
                "event-loss",
                pnl="-0.01",
            )
        )
        report = self.evaluate(rows, start + timedelta(hours=2, minutes=10))
        self.assertFalse(report["verified_profitable"])
        self.assertGreater(
            report["metrics"]["one_sided_95_conservative_lower_mean_pnl_usd"],
            0,
        )
        self.assertLess(
            report["metrics"][
                "one_sided_95_conservative_event_clustered_lower_mean_pnl_usd"
            ],
            0,
        )
        self.assertIn(
            "positive_conservative_event_clustered_lower_mean_pnl",
            "\n".join(report["blockers"]),
        )

    def test_started_attempt_without_terminal_reconciliation_blocks(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        row = paper_row(start, 1, "event-a")
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerow(row)
            self.write_attempts(attempts_path, [row])
            attempts_path.write_text(attempts_path.read_text().splitlines()[0] + "\n")
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(minutes=10),
                attempts_jsonl=attempts_path,
            )
        self.assertFalse(report["verified_profitable"])
        self.assertEqual(
            report["execution_attempts"]["unresolved_started_attempts"], 1
        )
        blocker_text = "\n".join(report["blockers"])
        self.assertIn("all_started_paper_attempts_reconciled", blocker_text)
        self.assertIn("paper_attempt_journal_matches_trades", blocker_text)

    def test_known_pre_submit_rejection_is_accounted_without_journal_terminal(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
            paper_row(start + timedelta(hours=3), 4, "event-b"),
            paper_row(
                start + timedelta(hours=3, minutes=5),
                5,
                "event-c",
                status="pre_submit_rejected",
                pre_submit_code="fresh_refresh",
            ),
        ]

        report = self.evaluate(rows, start + timedelta(hours=3, minutes=30))

        self.assertTrue(report["verified_profitable"])
        self.assertEqual(report["sample"]["paper_attempts"], 5)
        self.assertEqual(report["sample"]["pre_submit_rejected_attempts"], 1)
        self.assertEqual(
            report["sample"]["pre_submit_rejections_by_code"],
            {"fresh_refresh": 1},
        )
        self.assertEqual(report["sample"]["invalid_pre_submit_rejections"], 0)
        self.assertEqual(report["execution_attempts"]["started_attempts"], 4)
        self.assertAlmostEqual(report["metrics"]["fill_success_rate"], 0.8)
        self.assertAlmostEqual(report["metrics"]["submit_conversion_rate"], 0.8)
        self.assertAlmostEqual(report["metrics"]["pre_submit_rejection_rate"], 0.2)

    def test_pre_submit_rejection_with_attempt_marker_is_invalid(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
            paper_row(start + timedelta(hours=3), 4, "event-b"),
            paper_row(
                start + timedelta(hours=3, minutes=5),
                5,
                "event-c",
                status="pre_submit_rejected",
            ),
        ]
        rows[-1]["note"] += (
            "; paper_attempt_id=attempt-5; paper_attempt_status=error"
        )

        report = self.evaluate(rows, start + timedelta(hours=3, minutes=30))

        self.assertFalse(report["verified_profitable"])
        self.assertEqual(report["sample"]["invalid_pre_submit_rejections"], 1)
        self.assertIn("valid_pre_submit_rejections", "\n".join(report["blockers"]))

    def test_equal_campaign_baseline_is_a_hard_blocker(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            self.write_attempts(attempts_path, rows)
            records = [json.loads(line) for line in attempts_path.read_text().splitlines()]
            records[2]["baseline_trade_id"] = records[0]["baseline_trade_id"]
            records[3]["baseline_trade_id"] = records[0]["baseline_trade_id"]
            attempts_path.write_text(
                "".join(json.dumps(record) + "\n" for record in records)
            )
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertEqual(
            report["execution_attempts"]["non_increasing_baseline_trade_ids"], 1
        )
        self.assertIn(
            "strict_campaign_baseline_continuity", "\n".join(report["blockers"])
        )

    def test_raw_fee_tamper_breaks_independent_fill_recomputation(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            self.write_attempts(attempts_path, rows)
            records = [json.loads(line) for line in attempts_path.read_text().splitlines()]
            records[1]["filled_legs"][0]["raw_trades"][0]["fee_usd"] = 0.01
            attempts_path.write_text(
                "".join(json.dumps(record) + "\n" for record in records)
            )
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertIn(
            "attempt-1:fill_evidence:('slug-a', 'yes'):trade_1_fee",
            report["execution_binding"]["binding_errors"],
        )

    def test_coherent_guaranteed_revenue_inflation_cannot_spoof_payoff(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            self.write_attempts(attempts_path, rows)
            records = [json.loads(line) for line in attempts_path.read_text().splitlines()]
            records[0]["payoff_certificate"][
                "derived_guaranteed_revenue_per_basket_unit"
            ] = 2.0
            certificate_hash = GATE.canonical_json_sha256(
                records[0]["payoff_certificate"]
            )
            records[0]["payoff_certificate_sha256"] = certificate_hash
            records[1]["payoff_certificate_sha256"] = certificate_hash
            records[0]["guaranteed_revenue_per_basket_unit"] = 2.0
            records[1]["guaranteed_revenue_per_basket_unit"] = 2.0
            records[1]["conservative_pnl_usd"] += 10.0
            records[1]["conservative_roi_pct"] = (
                records[1]["conservative_pnl_usd"]
                / (
                    records[1]["hedged_cost_usd"]
                    + records[1]["unhedged_notional_usd"]
                )
                * 100.0
            )
            rows[0]["conservative_pnl_usd"] = str(
                records[1]["conservative_pnl_usd"]
            )
            rows[0]["conservative_roi_pct"] = str(
                records[1]["conservative_roi_pct"]
            )
            attempts_path.write_text(
                "".join(json.dumps(record) + "\n" for record in records)
            )
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertGreater(report["execution_attempts"]["malformed_records"], 0)
        self.assertIn("paper_attempt_journal_valid", "\n".join(report["blockers"]))

    def test_gas_below_bound_policy_floor_cannot_spoof_profit(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            self.write_attempts(attempts_path, rows)
            records = [json.loads(line) for line in attempts_path.read_text().splitlines()]
            records[0]["gas_policy_floor_usd"] = 0.0
            records[0]["gas_cost_usd"] = 0.0
            records[1]["gas_policy_floor_usd"] = 0.0
            records[1]["gas_cost_usd"] = 0.0
            records[1]["conservative_pnl_usd"] += 0.1
            records[1]["conservative_roi_pct"] = (
                records[1]["conservative_pnl_usd"]
                / records[1]["hedged_cost_usd"]
                * 100.0
            )
            rows[0]["conservative_pnl_usd"] = str(
                records[1]["conservative_pnl_usd"]
            )
            rows[0]["conservative_roi_pct"] = str(
                records[1]["conservative_roi_pct"]
            )
            attempts_path.write_text(
                "".join(json.dumps(record) + "\n" for record in records)
            )
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertGreater(report["execution_attempts"]["malformed_records"], 0)

    def test_unsupported_ranked_payoff_is_not_profitability_evidence(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a", arb_type="RANKED"),
            paper_row(start + timedelta(hours=1), 2, "event-b", arb_type="RANKED"),
            paper_row(start + timedelta(hours=2), 3, "event-a", arb_type="RANKED"),
        ]

        report = self.evaluate(rows, start + timedelta(hours=2, minutes=30))

        self.assertFalse(report["verified_profitable"])
        self.assertGreater(report["execution_attempts"]["malformed_records"], 0)
        self.assertIn("paper_attempt_journal_valid", "\n".join(report["blockers"]))

    def test_reused_raw_trade_id_across_attempts_is_a_hard_blocker(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            paper_row(start, 1, "event-a"),
            paper_row(start + timedelta(hours=1), 2, "event-b"),
            paper_row(start + timedelta(hours=2), 3, "event-a"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            self.write_attempts(attempts_path, rows)
            records = [json.loads(line) for line in attempts_path.read_text().splitlines()]
            records[2]["baseline_trade_id"] = 1
            records[3]["baseline_trade_id"] = 1
            records[3]["filled_legs"][0]["trade_ids"] = [2]
            records[3]["filled_legs"][0]["raw_trades"][0]["trade_id"] = 2
            records[3]["filled_legs"][0]["submission_id"] = 2
            records[3]["raw_trade_ids"] = [2, 4]
            attempts_path.write_text(
                "".join(json.dumps(record) + "\n" for record in records)
            )
            report = GATE.evaluate(
                trades_path,
                thresholds(),
                now=start + timedelta(hours=2, minutes=30),
                attempts_jsonl=attempts_path,
            )

        self.assertFalse(report["verified_profitable"])
        self.assertEqual(report["execution_attempts"]["duplicate_raw_trade_ids"], 1)
        self.assertIn("globally_unique_raw_trade_ids", "\n".join(report["blockers"]))

    def test_python_fee_rounding_matches_protocol_examples(self):
        self.assertEqual(GATE.protocol_fee_usd(0.4, 20.0, 0.02, 2), 0.02304)
        self.assertEqual(GATE.protocol_fee_usd(0.5, 0.0008, 0.02, 1), 0.0)
        self.assertEqual(GATE.protocol_fee_usd(0.5, 0.0018, 0.02, 1), 0.00001)

    def test_activation_flag_ignores_weakened_environment_thresholds(self):
        start = datetime.now(timezone.utc) - timedelta(minutes=5)
        rows = [paper_row(start, 1, "event-a")]
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            trades_path = directory_path / "trades.csv"
            attempts_path = directory_path / "paper_execution_attempts.jsonl"
            report_path = directory_path / "report.json"
            with trades_path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            self.write_attempts(attempts_path, rows)
            weakened = {
                "PAPER_PROFIT_MIN_TRADES": "0",
                "PAPER_PROFIT_MIN_UNIQUE_EVENTS": "0",
                "PAPER_PROFIT_MIN_OBSERVATION_HOURS": "0",
                "PAPER_PROFIT_MAX_EVIDENCE_AGE_HOURS": "999999",
                "PAPER_PROFIT_MIN_TOTAL_PNL_USD": "0",
                "PAPER_PROFIT_MIN_WEIGHTED_ROI_PCT": "0",
                "PAPER_PROFIT_MIN_LOWER_MEAN_PNL_USD": "0",
                "PAPER_PROFIT_MIN_EVENT_LOWER_MEAN_PNL_USD": "0",
                "PAPER_PROFIT_MIN_FILL_SUCCESS_RATE": "0",
                "PAPER_PROFIT_MIN_POSITIVE_TRADE_RATE": "0",
                "PAPER_PROFIT_MAX_DRAWDOWN_USD": "999999",
                "PAPER_PROFIT_MAX_UNHEDGED_NOTIONAL_USD": "999999",
            }
            completed = subprocess.run(
                [
                    "python3",
                    str(MODULE_PATH),
                    "--trades-csv",
                    str(trades_path),
                    "--attempts-jsonl",
                    str(attempts_path),
                    "--output",
                    str(report_path),
                    "--activation-thresholds",
                ],
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, **weakened},
            )
            self.assertEqual(completed.returncode, 1, completed.stderr)
            report = json.loads(report_path.read_text())
            self.assertEqual(report["threshold_profile"], "activation")
            self.assertEqual(report["thresholds"], GATE.ACTIVATION_THRESHOLDS)
            self.assertEqual(report["thresholds"]["min_trades"], 100)
            trades_snapshot = directory_path / "report-trades-source.csv"
            attempts_snapshot = directory_path / "report-attempts-source.jsonl"
            self.assertEqual(report["source"], str(trades_snapshot.resolve()))
            self.assertEqual(
                report["source_sha256"],
                hashlib.sha256(trades_snapshot.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                report["attempts_source_sha256"],
                hashlib.sha256(attempts_snapshot.read_bytes()).hexdigest(),
            )
            self.assertEqual(trades_snapshot.stat().st_mode & 0o222, 0)
            self.assertEqual(attempts_snapshot.stat().st_mode & 0o222, 0)


if __name__ == "__main__":
    unittest.main()
