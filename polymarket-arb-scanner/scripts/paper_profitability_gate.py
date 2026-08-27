#!/usr/bin/env python3
"""Evaluate real scanner paper fills against conservative profitability gates."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
import statistics
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_COLUMNS = {
    "timestamp",
    "scan_id",
    "mode",
    "status",
    "pnl_scale",
    "event_id",
    "arb_type",
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
}

MAX_TERMINAL_TRADE_LAG_SECONDS = 30.0
FLOAT_BIND_ABS_TOL = 1e-8
ONE_SIDED_95_CONSERVATIVE_CRITICAL = 1.70
MAX_SUPPORTED_CLOB_FEE_EXPONENT = 16
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
CONFIG_FINGERPRINT_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
OFFICIAL_CLOB_API_URL = "https://clob.polymarket.com"
OFFICIAL_GAMMA_API_URL = "https://gamma-api.polymarket.com"
PAPER_PRE_SUBMIT_REJECTION_PREFIX = "paper_pre_submit_rejection_v1"
PAPER_EXECUTION_ATTEMPT_SCHEMA_VERSION = 2
PAPER_PRE_SUBMIT_REJECTION_CODES = {
    "signal_freshness",
    "fresh_refresh",
    "depth_validation",
    "fee_metadata",
    "orderability",
    "fee_projection",
    "payoff_certificate",
    "final_profit",
    "submit_freshness",
}

EXECUTION_PROFILE_KEYS = {
    "schema_version",
    "execution_route",
    "live_route_compatible",
    "order_mode",
    "effective_order_type",
    "live_order_type",
    "paper_use_limit_orders_requested",
    "effective_paper_use_limit_orders",
    "full_clob_required",
    "match_live_position_size",
    "effective_position_size_usd",
    "live_position_size_usd",
    "paper_max_share_mismatch_pct",
    "min_net_profit_usd",
    "min_roi_pct",
    "max_signal_age_secs",
    "gas_fallback_usd",
    "assume_gasless_for_proxy_signature_types",
    "live_signature_type",
    "exclusive_paper_account_lock",
    "order_size_step_shares",
    "validate_opportunities_at_target_size",
    "execute_only_full_clob_prices",
    "live_slippage_bps",
    "live_edge_haircut_usd",
    "live_edge_haircut_bps",
    "live_min_leg_size_usd",
    "live_max_refresh_to_submit_ms",
    "fresh_clob_enrichment_complete",
    "fresh_depth_complete",
    "fresh_fee_schedules_complete",
    "pre_submit_orderability_complete",
    "clob_api_url",
    "gamma_api_url",
    "external_paper_command",
    "external_paper_executable_path",
    "external_paper_executable_sha256",
    "producer_version",
    "producer_executable_sha256",
}

PAPER_LIVE_PROFILE_CONFIG_KEYS = {
    "schema_version",
    "execution_route",
    "order_mode",
    "effective_order_type",
    "live_order_type",
    "paper_use_limit_orders_requested",
    "effective_paper_use_limit_orders",
    "full_clob_required",
    "match_live_position_size",
    "effective_position_size_usd",
    "live_position_size_usd",
    "paper_max_share_mismatch_pct",
    "min_net_profit_usd",
    "min_roi_pct",
    "max_signal_age_secs",
    "gas_fallback_usd",
    "assume_gasless_for_proxy_signature_types",
    "live_signature_type",
    "order_size_step_shares",
    "validate_opportunities_at_target_size",
    "execute_only_full_clob_prices",
    "live_slippage_bps",
    "live_edge_haircut_usd",
    "live_edge_haircut_bps",
    "live_min_leg_size_usd",
    "live_max_refresh_to_submit_ms",
    "clob_api_url",
    "gamma_api_url",
    "external_paper_command",
}

ATTEMPT_COMMON_FIELDS = (
    "event_id",
    "arb_type",
    "account",
    "data_dir",
    "account_lock_key",
    "baseline_trade_id",
    "execution_route",
    "live_route_compatible",
    "order_mode",
    "effective_order_type",
    "live_order_type",
    "full_clob_required",
    "match_live_position_size",
    "effective_position_size_usd",
    "config_fingerprint",
    "launch_config_fingerprint",
    "profit_compatibility_fingerprint",
    "config_field_count",
    "producer_version",
    "producer_executable_sha256",
    "external_paper_executable_sha256",
    "execution_profile_sha256",
    "payoff_certificate_sha256",
)

ACTIVATION_THRESHOLDS: dict[str, float] = {
    "min_trades": 100,
    "min_unique_events": 30,
    "min_observation_hours": 168,
    "max_evidence_age_hours": 24,
    "min_total_pnl_usd": 25,
    "min_weighted_roi_pct": 0.25,
    "min_lower_mean_pnl_usd": 0,
    "min_event_lower_mean_pnl_usd": 0,
    "min_fill_success_rate": 0.80,
    "min_positive_trade_rate": 0.80,
    "max_drawdown_usd": 25,
    "max_unhedged_notional_usd": 0,
}


def env_number(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = float(raw)
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return value


def thresholds_from_env() -> dict[str, float]:
    thresholds = {
        "min_trades": env_number(
            "PAPER_PROFIT_MIN_TRADES", ACTIVATION_THRESHOLDS["min_trades"]
        ),
        "min_unique_events": env_number(
            "PAPER_PROFIT_MIN_UNIQUE_EVENTS",
            ACTIVATION_THRESHOLDS["min_unique_events"],
        ),
        "min_observation_hours": env_number(
            "PAPER_PROFIT_MIN_OBSERVATION_HOURS",
            ACTIVATION_THRESHOLDS["min_observation_hours"],
        ),
        "max_evidence_age_hours": env_number(
            "PAPER_PROFIT_MAX_EVIDENCE_AGE_HOURS",
            ACTIVATION_THRESHOLDS["max_evidence_age_hours"],
        ),
        "min_total_pnl_usd": env_number(
            "PAPER_PROFIT_MIN_TOTAL_PNL_USD",
            ACTIVATION_THRESHOLDS["min_total_pnl_usd"],
        ),
        "min_weighted_roi_pct": env_number(
            "PAPER_PROFIT_MIN_WEIGHTED_ROI_PCT",
            ACTIVATION_THRESHOLDS["min_weighted_roi_pct"],
        ),
        "min_lower_mean_pnl_usd": env_number(
            "PAPER_PROFIT_MIN_LOWER_MEAN_PNL_USD",
            ACTIVATION_THRESHOLDS["min_lower_mean_pnl_usd"],
        ),
        "min_event_lower_mean_pnl_usd": env_number(
            "PAPER_PROFIT_MIN_EVENT_LOWER_MEAN_PNL_USD",
            ACTIVATION_THRESHOLDS["min_event_lower_mean_pnl_usd"],
        ),
        "min_fill_success_rate": env_number(
            "PAPER_PROFIT_MIN_FILL_SUCCESS_RATE",
            ACTIVATION_THRESHOLDS["min_fill_success_rate"],
        ),
        "min_positive_trade_rate": env_number(
            "PAPER_PROFIT_MIN_POSITIVE_TRADE_RATE",
            ACTIVATION_THRESHOLDS["min_positive_trade_rate"],
        ),
        "max_drawdown_usd": env_number(
            "PAPER_PROFIT_MAX_DRAWDOWN_USD",
            ACTIVATION_THRESHOLDS["max_drawdown_usd"],
        ),
        "max_unhedged_notional_usd": env_number(
            "PAPER_PROFIT_MAX_UNHEDGED_NOTIONAL_USD",
            ACTIVATION_THRESHOLDS["max_unhedged_notional_usd"],
        ),
    }
    for key in ("min_fill_success_rate", "min_positive_trade_rate"):
        if thresholds[key] > 1:
            raise ValueError(f"{key} must be between 0 and 1")
    return thresholds


def activation_thresholds() -> dict[str, float]:
    """Return the non-configurable minimums required for live activation."""
    return dict(ACTIVATION_THRESHOLDS)


def sha256_open_file(handle: Any) -> str:
    handle.seek(0)
    digest = hashlib.sha256()
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def canonical_json_sha256(value: Any) -> str:
    body = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def required_string(record: dict[str, Any], key: str) -> str:
    value = record[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value.strip()


def required_bool(record: dict[str, Any], key: str) -> bool:
    value = record[key]
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be boolean")
    return value


def required_number(
    record: dict[str, Any], key: str, *, minimum: float | None = None
) -> float:
    value = record[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or (minimum is not None and parsed < minimum):
        raise ValueError(f"{key} is invalid")
    return parsed


def required_int(
    record: dict[str, Any], key: str, *, minimum: int | None = None
) -> int:
    value = record[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{key} is invalid")
    return value


def normalize_planned_legs(
    record: dict[str, Any], leg_count: int
) -> list[dict[str, Any]]:
    raw_legs = record.get("planned_legs")
    if not isinstance(raw_legs, list) or len(raw_legs) != leg_count:
        raise ValueError("planned_legs does not match leg_count")
    legs: list[dict[str, Any]] = []
    keys: set[tuple[str, str]] = set()
    for raw in raw_legs:
        if not isinstance(raw, dict):
            raise ValueError("planned leg must be an object")
        leg = {
            "condition_id": required_string(raw, "condition_id"),
            "token_id": required_string(raw, "token_id"),
            "market_slug": required_string(raw, "market_slug"),
            "outcome": required_string(raw, "outcome").lower(),
            "unit_shares": required_number(raw, "unit_shares", minimum=1e-12),
            "shares": required_number(raw, "shares", minimum=1e-12),
            "amount_usd": required_number(raw, "amount_usd", minimum=1e-12),
            "limit_price": required_number(raw, "limit_price", minimum=1e-12),
            "fee_rate": required_number(raw, "fee_rate", minimum=0.0),
            "fee_exponent": required_int(raw, "fee_exponent", minimum=1),
        }
        if leg["limit_price"] > 1.0 or leg["fee_rate"] > 1.0:
            raise ValueError("planned leg price/fee rate is invalid")
        if leg["fee_exponent"] > MAX_SUPPORTED_CLOB_FEE_EXPONENT:
            raise ValueError("planned leg fee exponent is unsupported")
        key = (leg["market_slug"], leg["outcome"])
        if key in keys:
            raise ValueError("duplicate planned market_slug/outcome")
        keys.add(key)
        legs.append(leg)
    return legs


def normalize_filled_legs(
    record: dict[str, Any], baseline_trade_id: int
) -> tuple[list[dict[str, Any]], list[int]]:
    raw_legs = record.get("filled_legs")
    if not isinstance(raw_legs, list) or not raw_legs:
        raise ValueError("filled_legs must be a non-empty array")
    legs: list[dict[str, Any]] = []
    all_trade_ids: list[int] = []
    leg_keys: set[tuple[str, str]] = set()
    submission_keys: set[tuple[str, int]] = set()
    for raw_leg in raw_legs:
        if not isinstance(raw_leg, dict):
            raise ValueError("filled leg must be an object")
        leg = {
            "market_slug": required_string(raw_leg, "market_slug"),
            "outcome": required_string(raw_leg, "outcome").lower(),
            "label": required_string(raw_leg, "label"),
            "unit_shares": required_number(raw_leg, "unit_shares", minimum=1e-12),
            "shares": required_number(raw_leg, "shares", minimum=1e-12),
            "notional_usd": required_number(raw_leg, "notional_usd", minimum=1e-12),
            "avg_price": required_number(raw_leg, "avg_price", minimum=1e-12),
            "is_partial": required_bool(raw_leg, "is_partial"),
            "fee_rate": required_number(raw_leg, "fee_rate", minimum=0.0),
            "fee_exponent": required_int(raw_leg, "fee_exponent", minimum=1),
            "recomputed_fee_usd": required_number(
                raw_leg, "recomputed_fee_usd", minimum=0.0
            ),
            "submission_kind": required_string(raw_leg, "submission_kind").lower(),
            "submission_id": required_int(raw_leg, "submission_id", minimum=1),
            "attribution_mode": required_string(
                raw_leg, "attribution_mode"
            ).lower(),
        }
        if leg["avg_price"] > 1.0 or leg["fee_rate"] > 1.0:
            raise ValueError("filled leg price/fee rate is invalid")
        if leg["fee_exponent"] > MAX_SUPPORTED_CLOB_FEE_EXPONENT:
            raise ValueError("filled leg fee exponent is unsupported")
        expected_attribution = {
            "market_trade": "direct_trade_id",
            "limit_order": "exclusive_account_window",
        }.get(leg["submission_kind"])
        if (
            expected_attribution is None
            or leg["attribution_mode"] != expected_attribution
            or (leg["submission_kind"], leg["submission_id"]) in submission_keys
        ):
            raise ValueError("filled leg has invalid/duplicate submission attribution")
        submission_keys.add((leg["submission_kind"], leg["submission_id"]))
        key = (leg["market_slug"], leg["outcome"])
        if key in leg_keys:
            raise ValueError("duplicate filled market_slug/outcome")
        leg_keys.add(key)

        raw_trade_ids = raw_leg.get("trade_ids")
        raw_trades = raw_leg.get("raw_trades")
        if not isinstance(raw_trade_ids, list) or not isinstance(raw_trades, list):
            raise ValueError("filled leg trade_ids/raw_trades must be arrays")
        if not raw_trade_ids or len(raw_trade_ids) != len(raw_trades):
            raise ValueError("filled leg trade IDs do not match raw trades")
        trade_ids = [
            value
            for value in raw_trade_ids
            if isinstance(value, int) and not isinstance(value, bool)
        ]
        if (
            len(trade_ids) != len(raw_trade_ids)
            or trade_ids != sorted(trade_ids)
            or len(set(trade_ids)) != len(trade_ids)
            or any(trade_id <= baseline_trade_id for trade_id in trade_ids)
        ):
            raise ValueError("filled leg has invalid/replayed trade IDs")
        trades: list[dict[str, Any]] = []
        for raw_trade in raw_trades:
            if not isinstance(raw_trade, dict):
                raise ValueError("raw trade must be an object")
            trade = {
                "trade_id": required_int(raw_trade, "trade_id", minimum=1),
                "shares": required_number(raw_trade, "shares", minimum=1e-12),
                "amount_usd": required_number(
                    raw_trade, "amount_usd", minimum=1e-12
                ),
                "avg_price": required_number(
                    raw_trade, "avg_price", minimum=1e-12
                ),
                "is_partial": required_bool(raw_trade, "is_partial"),
                "fee_usd": required_number(raw_trade, "fee_usd", minimum=0.0),
            }
            if trade["avg_price"] > 1.0:
                raise ValueError("raw trade avg price is invalid")
            trades.append(trade)
        if [trade["trade_id"] for trade in trades] != trade_ids:
            raise ValueError("raw trade order/IDs do not match trade_ids")
        if leg["submission_kind"] == "market_trade" and trade_ids != [
            leg["submission_id"]
        ]:
            raise ValueError("market submission id does not match raw trade id")
        leg["trade_ids"] = trade_ids
        leg["raw_trades"] = trades
        all_trade_ids.extend(trade_ids)
        legs.append(leg)

    top_trade_ids = record.get("raw_trade_ids")
    if not isinstance(top_trade_ids, list) or top_trade_ids != sorted(all_trade_ids):
        raise ValueError("terminal raw_trade_ids do not match filled legs")
    if required_int(record, "raw_trade_count", minimum=1) != len(all_trade_ids):
        raise ValueError("terminal raw_trade_count does not match filled legs")
    return legs, all_trade_ids


def protocol_fee_usd(price: float, shares: float, rate: float, exponent: int) -> float:
    if (
        price <= 0.0
        or price >= 1.0
        or rate <= 0.0
        or exponent <= 0
        or exponent > MAX_SUPPORTED_CLOB_FEE_EXPONENT
    ):
        return 0.0
    fee = rate * (price * (1.0 - price)) ** exponent * shares
    rounded = math.floor(fee * 100_000.0 + 0.5) / 100_000.0
    return 0.0 if rounded < 0.00001 else rounded


def numbers_match(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-10, abs_tol=FLOAT_BIND_ABS_TOL)


def normalize_payoff_certificate(
    record: dict[str, Any],
    common: dict[str, Any],
    planned_legs: list[dict[str, Any]],
) -> tuple[dict[str, Any], float]:
    certificate = record.get("payoff_certificate")
    if not isinstance(certificate, dict) or certificate.get("schema_version") != 1:
        raise ValueError("invalid payoff certificate")
    if canonical_json_sha256(certificate) != common["payoff_certificate_sha256"]:
        raise ValueError("payoff certificate hash mismatch")
    arb_type = common["arb_type"].upper()
    if required_string(certificate, "arb_type").upper() != arb_type:
        raise ValueError("payoff certificate arb type mismatch")
    if not required_bool(certificate, "supported_for_profit_evidence"):
        raise ValueError("unsupported payoff certificate")
    topology = required_string(certificate, "topology")
    raw_market_count = required_int(certificate, "raw_market_count", minimum=1)
    raw_ids = certificate.get("raw_condition_ids")
    if (
        not isinstance(raw_ids, list)
        or len(raw_ids) != raw_market_count
        or any(not isinstance(value, str) or not value.strip() for value in raw_ids)
    ):
        raise ValueError("invalid payoff certificate raw condition ids")
    raw_condition_ids = [value.strip() for value in raw_ids]
    if raw_condition_ids != sorted(raw_condition_ids) or len(set(raw_condition_ids)) != len(
        raw_condition_ids
    ):
        raise ValueError("payoff certificate condition ids must be sorted and unique")

    plan_conditions = [leg["condition_id"] for leg in planned_legs]
    plan_tokens = [leg["token_id"] for leg in planned_legs]
    if arb_type in {"YES", "NO"}:
        expected_topology = "yes_full_family" if arb_type == "YES" else "no_full_family"
        expected_outcome = arb_type.lower()
        if (
            topology != expected_topology
            or raw_market_count < 2
            or len(planned_legs) != raw_market_count
            or sorted(plan_conditions) != raw_condition_ids
            or len(set(plan_conditions)) != raw_market_count
            or len(set(plan_tokens)) != raw_market_count
            or len({leg["market_slug"] for leg in planned_legs}) != raw_market_count
            or any(leg["outcome"] != expected_outcome for leg in planned_legs)
            or any(not numbers_match(leg["unit_shares"], 1.0) for leg in planned_legs)
        ):
            raise ValueError("payoff certificate does not prove a full YES/NO family")
        derived_revenue = 1.0 if arb_type == "YES" else float(raw_market_count - 1)
    elif arb_type == "BUNDLE":
        if (
            topology != "binary_yes_no_bundle"
            or raw_market_count != 1
            or len(planned_legs) != 2
            or len(set(plan_conditions)) != 1
            or plan_conditions[0] != raw_condition_ids[0]
            or len(set(plan_tokens)) != 2
            or len({leg["market_slug"] for leg in planned_legs}) != 1
            or {leg["outcome"] for leg in planned_legs} != {"yes", "no"}
            or any(not numbers_match(leg["unit_shares"], 1.0) for leg in planned_legs)
        ):
            raise ValueError("payoff certificate does not prove a binary bundle")
        derived_revenue = 1.0
    else:
        raise ValueError(f"unsupported profitability-evidence arb type {arb_type}")

    recorded_revenue = required_number(
        certificate, "derived_guaranteed_revenue_per_basket_unit", minimum=1e-12
    )
    if not numbers_match(recorded_revenue, derived_revenue):
        raise ValueError("payoff certificate derived revenue mismatch")
    return certificate, derived_revenue


def derived_gas_policy_floor(profile: dict[str, Any], leg_count: int) -> float:
    if (
        profile["assume_gasless_for_proxy_signature_types"]
        and profile["live_signature_type"] != 0
    ):
        return 0.0
    return profile["gas_fallback_usd"] * leg_count


def recompute_fill_evidence(
    start: dict[str, Any], terminal: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    profile = start["execution_profile"]
    planned = {
        (leg["market_slug"], leg["outcome"]): leg
        for leg in start["planned_legs"]
    }
    filled = {
        (leg["market_slug"], leg["outcome"]): leg
        for leg in terminal["filled_legs"]
    }
    if planned.keys() != filled.keys():
        return ["planned_filled_leg_mapping"]
    expected_submission_kind = (
        "market_trade" if profile["order_mode"] == "market_style" else "limit_order"
    )
    if not profile["exclusive_paper_account_lock"]:
        errors.append("exclusive_paper_account_lock")
    if any(
        fill["submission_kind"] != expected_submission_kind for fill in filled.values()
    ):
        errors.append("submission_kind_for_order_mode")

    for key, plan in planned.items():
        expected_shares = start["planned_basket_units"] * plan["unit_shares"]
        if not numbers_match(plan["shares"], expected_shares):
            errors.append(f"{key}:planned_shares")
        # Market-style paper orders target the depth-weighted expected price,
        # while `limit_price` is the more conservative cutoff/slippage cap.
        # Both routes quantize the submitted USD amount to cents.
        if plan["amount_usd"] > plan["shares"] * plan["limit_price"] + 0.00501:
            errors.append(f"{key}:planned_amount_limit_bound")

    realized_units: list[float] = []
    recomputed_outflows: list[float] = []
    total_fill_notional = 0.0
    total_fees = 0.0
    any_partial = False
    for key, fill in filled.items():
        plan = planned[key]
        for field in ("unit_shares", "fee_rate"):
            if not numbers_match(fill[field], plan[field]):
                errors.append(f"{key}:{field}")
        if fill["fee_exponent"] != plan["fee_exponent"]:
            errors.append(f"{key}:fee_exponent")

        shares = sum(trade["shares"] for trade in fill["raw_trades"])
        notional = sum(trade["amount_usd"] for trade in fill["raw_trades"])
        fee = 0.0
        partial = False
        for trade in fill["raw_trades"]:
            derived_price = trade["amount_usd"] / trade["shares"]
            if not numbers_match(trade["avg_price"], derived_price):
                errors.append(f"{key}:trade_{trade['trade_id']}_avg_price")
            expected_fee = protocol_fee_usd(
                derived_price,
                trade["shares"],
                plan["fee_rate"],
                plan["fee_exponent"],
            )
            if not numbers_match(trade["fee_usd"], expected_fee):
                errors.append(f"{key}:trade_{trade['trade_id']}_fee")
            fee += expected_fee
            partial = partial or trade["is_partial"]

        aggregate_price = notional / shares
        for field, actual, expected in (
            ("shares", fill["shares"], shares),
            ("notional_usd", fill["notional_usd"], notional),
            ("avg_price", fill["avg_price"], aggregate_price),
            ("recomputed_fee_usd", fill["recomputed_fee_usd"], fee),
        ):
            if not numbers_match(actual, expected):
                errors.append(f"{key}:{field}")
        if fill["is_partial"] != partial:
            errors.append(f"{key}:is_partial")
        realized_units.append(shares / plan["unit_shares"])
        recomputed_outflows.append(notional + fee)
        total_fill_notional += notional
        total_fees += fee
        any_partial = any_partial or partial

    hedged_units = min(realized_units)
    max_units = max(realized_units)
    hedged_cost = 0.0
    unhedged = 0.0
    for outflow, units in zip(recomputed_outflows, realized_units):
        hedged_fraction = min(max(hedged_units / max(units, 0.0001), 0.0), 1.0)
        hedged_cost += outflow * hedged_fraction
        unhedged += outflow * (1.0 - hedged_fraction)

    guaranteed = (
        hedged_units * terminal["guaranteed_revenue_per_basket_unit"]
    )
    conservative_pnl = (
        guaranteed - hedged_cost - terminal["gas_cost_usd"] - unhedged
    )
    total_outflow = hedged_cost + unhedged
    conservative_roi = (
        conservative_pnl / total_outflow * 100.0 if total_outflow > 0 else 0.0
    )
    drift_pct = (max_units - hedged_units) / max(hedged_units, 0.0001) * 100.0
    shortfall_pct = max(start["planned_basket_units"] - hedged_units, 0.0) / max(
        start["planned_basket_units"], 1e-12
    ) * 100.0
    parity_ok = (
        not any_partial
        and drift_pct <= profile["paper_max_share_mismatch_pct"]
        and shortfall_pct <= profile["paper_max_share_mismatch_pct"]
        and conservative_pnl >= profile["min_net_profit_usd"]
        and conservative_roi >= profile["min_roi_pct"]
    )

    for field, actual, expected in (
        ("total_fill_notional_usd", terminal["total_fill_notional_usd"], total_fill_notional),
        ("total_recomputed_fees_usd", terminal["total_recomputed_fees_usd"], total_fees),
        ("hedged_basket_units", terminal["hedged_basket_units"], hedged_units),
        ("hedged_cost_usd", terminal["hedged_cost_usd"], hedged_cost),
        ("unhedged_notional_usd", terminal["unhedged_notional_usd"], unhedged),
        ("conservative_pnl_usd", terminal["conservative_pnl_usd"], conservative_pnl),
        ("conservative_roi_pct", terminal["conservative_roi_pct"], conservative_roi),
    ):
        if not numbers_match(actual, expected):
            errors.append(field)
    if terminal["fill_count"] != len(filled):
        errors.append("fill_count")
    if terminal["any_partial"] != any_partial:
        errors.append("any_partial")
    if terminal["parity_ok"] != parity_ok:
        errors.append("parity_ok")
    if not numbers_match(
        terminal["planned_basket_units"], start["planned_basket_units"]
    ):
        errors.append("planned_basket_units")
    if not numbers_match(
        terminal["guaranteed_revenue_per_basket_unit"],
        start["guaranteed_revenue_per_basket_unit"],
    ):
        errors.append("guaranteed_revenue_per_basket_unit")
    if not numbers_match(terminal["gas_cost_usd"], start["gas_cost_usd"]):
        errors.append("gas_cost_usd")
    if not numbers_match(
        terminal["gas_policy_floor_usd"], start["gas_policy_floor_usd"]
    ):
        errors.append("gas_policy_floor_usd")
    derived_gas_floor = derived_gas_policy_floor(profile, len(planned))
    if not numbers_match(start["gas_policy_floor_usd"], derived_gas_floor):
        errors.append("derived_gas_policy_floor_usd")
    if start["gas_cost_usd"] + FLOAT_BIND_ABS_TOL < derived_gas_floor:
        errors.append("gas_cost_below_policy_floor")
    return errors


def validate_attempt_common(record: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "event_id": required_string(record, "event_id"),
        "arb_type": required_string(record, "arb_type"),
        "account": required_string(record, "account"),
        "data_dir": required_string(record, "data_dir"),
        "account_lock_key": required_string(record, "account_lock_key").lower(),
        "baseline_trade_id": required_int(record, "baseline_trade_id", minimum=0),
        "execution_route": required_string(record, "execution_route"),
        "live_route_compatible": required_bool(record, "live_route_compatible"),
        "order_mode": required_string(record, "order_mode").lower(),
        "effective_order_type": required_string(
            record, "effective_order_type"
        ).lower(),
        "live_order_type": required_string(record, "live_order_type").lower(),
        "full_clob_required": required_bool(record, "full_clob_required"),
        "match_live_position_size": required_bool(
            record, "match_live_position_size"
        ),
        "effective_position_size_usd": required_number(
            record, "effective_position_size_usd", minimum=0.000000001
        ),
        "config_fingerprint": required_string(record, "config_fingerprint"),
        "launch_config_fingerprint": required_string(
            record, "launch_config_fingerprint"
        ),
        "profit_compatibility_fingerprint": required_string(
            record, "profit_compatibility_fingerprint"
        ),
        "config_field_count": required_int(record, "config_field_count", minimum=1),
        "producer_version": required_string(record, "producer_version"),
        "producer_executable_sha256": required_string(
            record, "producer_executable_sha256"
        ).lower(),
        "external_paper_executable_sha256": required_string(
            record, "external_paper_executable_sha256"
        ).lower(),
        "execution_profile_sha256": required_string(
            record, "execution_profile_sha256"
        ).lower(),
        "payoff_certificate_sha256": required_string(
            record, "payoff_certificate_sha256"
        ).lower(),
    }
    if not CONFIG_FINGERPRINT_RE.fullmatch(normalized["config_fingerprint"]):
        raise ValueError("invalid config_fingerprint")
    if not CONFIG_FINGERPRINT_RE.fullmatch(
        normalized["launch_config_fingerprint"]
    ):
        raise ValueError("invalid launch_config_fingerprint")
    if not CONFIG_FINGERPRINT_RE.fullmatch(
        normalized["profit_compatibility_fingerprint"]
    ):
        raise ValueError("invalid profit_compatibility_fingerprint")
    for key in (
        "producer_executable_sha256",
        "external_paper_executable_sha256",
        "execution_profile_sha256",
        "account_lock_key",
        "payoff_certificate_sha256",
    ):
        if not SHA256_RE.fullmatch(normalized[key]):
            raise ValueError(f"invalid {key}")
    return normalized


def validate_execution_profile(
    record: dict[str, Any], common: dict[str, Any]
) -> dict[str, Any]:
    profile = record.get("execution_profile")
    if not isinstance(profile, dict):
        raise ValueError("execution_profile must be an object")
    missing = EXECUTION_PROFILE_KEYS - profile.keys()
    if missing:
        raise ValueError(f"execution_profile missing {sorted(missing)}")
    if profile.get("schema_version") != 1:
        raise ValueError("unsupported execution_profile schema")
    if canonical_json_sha256(profile) != common["execution_profile_sha256"]:
        raise ValueError("execution_profile_sha256 mismatch")

    for key in (
        "execution_route",
        "order_mode",
        "effective_order_type",
        "live_order_type",
        "clob_api_url",
        "gamma_api_url",
        "external_paper_command",
        "external_paper_executable_path",
        "producer_version",
    ):
        required_string(profile, key)
    for key in (
        "live_route_compatible",
        "paper_use_limit_orders_requested",
        "effective_paper_use_limit_orders",
        "full_clob_required",
        "match_live_position_size",
        "assume_gasless_for_proxy_signature_types",
        "validate_opportunities_at_target_size",
        "execute_only_full_clob_prices",
        "fresh_clob_enrichment_complete",
        "fresh_depth_complete",
        "fresh_fee_schedules_complete",
        "pre_submit_orderability_complete",
        "exclusive_paper_account_lock",
    ):
        required_bool(profile, key)
    for key in (
        "effective_position_size_usd",
        "live_position_size_usd",
        "order_size_step_shares",
    ):
        required_number(profile, key, minimum=0.000000001)
    for key in (
        "paper_max_share_mismatch_pct",
        "min_net_profit_usd",
        "min_roi_pct",
        "gas_fallback_usd",
        "live_edge_haircut_usd",
        "live_min_leg_size_usd",
    ):
        required_number(profile, key, minimum=0.0)
    for key in (
        "max_signal_age_secs",
        "live_slippage_bps",
        "live_edge_haircut_bps",
        "live_max_refresh_to_submit_ms",
        "live_signature_type",
    ):
        required_int(profile, key, minimum=0)
    if profile["live_signature_type"] > 3:
        raise ValueError("execution_profile live_signature_type is invalid")
    for key in (
        "external_paper_executable_sha256",
        "producer_executable_sha256",
    ):
        if not SHA256_RE.fullmatch(required_string(profile, key)):
            raise ValueError(f"invalid execution_profile {key}")

    for key in (
        "execution_route",
        "live_route_compatible",
        "order_mode",
        "effective_order_type",
        "live_order_type",
        "full_clob_required",
        "match_live_position_size",
        "effective_position_size_usd",
        "producer_version",
        "producer_executable_sha256",
        "external_paper_executable_sha256",
    ):
        profile_value = profile[key]
        common_value = common[key]
        if isinstance(profile_value, str) and key in {
            "order_mode",
            "effective_order_type",
            "live_order_type",
            "producer_executable_sha256",
            "external_paper_executable_sha256",
        }:
            profile_value = profile_value.lower()
        if profile_value != common_value:
            raise ValueError(f"execution_profile/top-level mismatch for {key}")
    return profile


def read_attempt_journal(path: Path) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "source_sha256": None,
        "record_count": 0,
        "excluded_synthetic_records": 0,
        "started_attempts": 0,
        "terminal_attempts": 0,
        "terminal_accepted": 0,
        "terminal_rejected": 0,
        "terminal_errors": 0,
        "unresolved_started_attempts": 0,
        "terminal_without_start": 0,
        "terminal_before_start": 0,
        "duplicate_start_records": 0,
        "duplicate_terminal_records": 0,
        "malformed_records": 0,
        "common_field_mismatches": 0,
        "non_monotonic_recorded_timestamps": 0,
        "non_monotonic_baseline_trade_ids": 0,
        "non_increasing_baseline_trade_ids": 0,
        "baseline_below_prior_raw_trade_id": 0,
        "duplicate_raw_trade_ids": 0,
        "source_changed_during_evaluation": False,
        "terminal_status_by_attempt_id": {},
        "distinct_accounts": [],
        "distinct_data_dirs": [],
        "distinct_account_lock_keys": [],
        "distinct_config_fingerprints": [],
        "distinct_profit_compatibility_fingerprints": [],
        "distinct_producer_executable_sha256": [],
        "distinct_external_paper_executable_sha256": [],
        "distinct_execution_profile_sha256": [],
        "_start_records": {},
        "_terminal_records": {},
    }
    if not path.is_file():
        return metrics

    starts: dict[str, dict[str, Any]] = {}
    terminals: dict[str, dict[str, Any]] = {}
    terminal_statuses: dict[str, str] = {}
    previous_recorded_at: datetime | None = None
    previous_baseline_trade_id: int | None = None
    highest_observed_trade_id = 0
    campaign_trade_ids: set[int] = set()
    with path.open("rb") as raw_handle:
        source_sha256 = sha256_open_file(raw_handle)
        metrics["source_sha256"] = source_sha256
        raw_handle.seek(0)
        text_handle = io.TextIOWrapper(raw_handle, encoding="utf-8", newline="")
        try:
            for line_number, raw_line in enumerate(text_handle, start=1):
                if not raw_line.strip():
                    continue
                metrics["record_count"] += 1
                try:
                    record = json.loads(raw_line)
                    if (
                        not isinstance(record, dict)
                        or record.get("schema_version")
                        != PAPER_EXECUTION_ATTEMPT_SCHEMA_VERSION
                    ):
                        raise ValueError("invalid required attempt fields")
                    attempt_id = required_string(record, "attempt_id")
                    event_id = required_string(record, "event_id")
                    stage = required_string(record, "stage").lower()
                    status = required_string(record, "status").lower()
                    recorded_at = parse_timestamp(required_string(record, "recorded_at"))
                except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                    metrics["malformed_records"] += 1
                    continue
                if event_id.lower().startswith("synthetic-"):
                    metrics["excluded_synthetic_records"] += 1
                    continue

                try:
                    common = validate_attempt_common(record)
                    normalized = {
                        **common,
                        "attempt_id": attempt_id,
                        "recorded_at": recorded_at.isoformat(),
                        "line_number": line_number,
                        "status": status,
                    }
                    if previous_recorded_at is not None and recorded_at < previous_recorded_at:
                        metrics["non_monotonic_recorded_timestamps"] += 1
                    previous_recorded_at = recorded_at

                    if stage == "started" and status == "started":
                        profile = validate_execution_profile(record, common)
                        normalized["execution_profile"] = profile
                        normalized["planned_basket_units"] = required_number(
                            record, "planned_basket_units", minimum=0.000000001
                        )
                        normalized["guaranteed_revenue_per_basket_unit"] = required_number(
                            record,
                            "guaranteed_revenue_per_basket_unit",
                            minimum=0.000000001,
                        )
                        normalized["gas_cost_usd"] = required_number(
                            record, "gas_cost_usd", minimum=0.0
                        )
                        normalized["gas_policy_floor_usd"] = required_number(
                            record, "gas_policy_floor_usd", minimum=0.0
                        )
                        normalized["projected_cost_usd"] = required_number(
                            record, "projected_cost_usd", minimum=0.000000001
                        )
                        normalized["projected_pnl_usd"] = required_number(
                            record, "projected_pnl_usd"
                        )
                        normalized["projected_roi_pct"] = required_number(
                            record, "projected_roi_pct"
                        )
                        leg_count = required_int(record, "leg_count", minimum=1)
                        normalized["planned_legs"] = normalize_planned_legs(
                            record, leg_count
                        )
                        certificate, derived_revenue = normalize_payoff_certificate(
                            record, common, normalized["planned_legs"]
                        )
                        normalized["payoff_certificate"] = certificate
                        normalized[
                            "derived_guaranteed_revenue_per_basket_unit"
                        ] = derived_revenue
                        if not numbers_match(
                            normalized["guaranteed_revenue_per_basket_unit"],
                            derived_revenue,
                        ):
                            raise ValueError(
                                "journal guaranteed revenue does not match payoff certificate"
                            )
                        gas_floor = derived_gas_policy_floor(profile, leg_count)
                        if not numbers_match(
                            normalized["gas_policy_floor_usd"], gas_floor
                        ) or normalized["gas_cost_usd"] + FLOAT_BIND_ABS_TOL < gas_floor:
                            raise ValueError("journal gas cost is below bound policy floor")
                        if attempt_id in starts:
                            metrics["duplicate_start_records"] += 1
                        else:
                            starts[attempt_id] = normalized
                            baseline = common["baseline_trade_id"]
                            if (
                                previous_baseline_trade_id is not None
                                and baseline < previous_baseline_trade_id
                            ):
                                metrics["non_monotonic_baseline_trade_ids"] += 1
                            if (
                                previous_baseline_trade_id is not None
                                and baseline <= previous_baseline_trade_id
                            ):
                                metrics["non_increasing_baseline_trade_ids"] += 1
                            if baseline < highest_observed_trade_id:
                                metrics["baseline_below_prior_raw_trade_id"] += 1
                            previous_baseline_trade_id = baseline
                        continue

                    if stage == "terminal" and status in {
                        "accepted",
                        "rejected",
                        "error",
                    }:
                        if status in {"accepted", "rejected"}:
                            normalized.update(
                                {
                                    "parity_ok": required_bool(record, "parity_ok"),
                                    "any_partial": required_bool(record, "any_partial"),
                                    "fill_count": required_int(
                                        record, "fill_count", minimum=1
                                    ),
                                    "planned_basket_units": required_number(
                                        record,
                                        "planned_basket_units",
                                        minimum=0.000000001,
                                    ),
                                    "hedged_basket_units": required_number(
                                        record, "hedged_basket_units", minimum=0.0
                                    ),
                                    "hedged_cost_usd": required_number(
                                        record, "hedged_cost_usd", minimum=0.000000001
                                    ),
                                    "conservative_pnl_usd": required_number(
                                        record, "conservative_pnl_usd"
                                    ),
                                    "conservative_roi_pct": required_number(
                                        record, "conservative_roi_pct"
                                    ),
                                    "unhedged_notional_usd": required_number(
                                        record,
                                        "unhedged_notional_usd",
                                        minimum=0.0,
                                    ),
                                    "total_fill_notional_usd": required_number(
                                        record,
                                        "total_fill_notional_usd",
                                        minimum=0.000000001,
                                    ),
                                    "total_recomputed_fees_usd": required_number(
                                        record,
                                        "total_recomputed_fees_usd",
                                        minimum=0.0,
                                    ),
                                    "guaranteed_revenue_per_basket_unit": required_number(
                                        record,
                                        "guaranteed_revenue_per_basket_unit",
                                        minimum=0.000000001,
                                    ),
                                    "gas_cost_usd": required_number(
                                        record, "gas_cost_usd", minimum=0.0
                                    ),
                                    "gas_policy_floor_usd": required_number(
                                        record, "gas_policy_floor_usd", minimum=0.0
                                    ),
                                }
                            )
                            filled_legs, raw_trade_ids = normalize_filled_legs(
                                record, common["baseline_trade_id"]
                            )
                            normalized["filled_legs"] = filled_legs
                            normalized["raw_trade_ids"] = raw_trade_ids
                            for trade_id in raw_trade_ids:
                                if trade_id in campaign_trade_ids:
                                    metrics["duplicate_raw_trade_ids"] += 1
                                campaign_trade_ids.add(trade_id)
                                highest_observed_trade_id = max(
                                    highest_observed_trade_id, trade_id
                                )
                        else:
                            normalized["error"] = required_string(record, "error")
                        if attempt_id in terminals:
                            metrics["duplicate_terminal_records"] += 1
                        else:
                            terminals[attempt_id] = normalized
                            terminal_statuses[attempt_id] = status
                        continue
                    raise ValueError("invalid stage/status combination")
                except (KeyError, TypeError, ValueError):
                    metrics["malformed_records"] += 1
        finally:
            text_handle.detach()
        metrics["source_changed_during_evaluation"] = (
            sha256_open_file(raw_handle) != source_sha256
        )

    metrics["started_attempts"] = len(starts)
    metrics["terminal_attempts"] = len(terminals)
    metrics["terminal_accepted"] = sum(
        status == "accepted" for status in terminal_statuses.values()
    )
    metrics["terminal_rejected"] = sum(
        status == "rejected" for status in terminal_statuses.values()
    )
    metrics["terminal_errors"] = sum(
        status == "error" for status in terminal_statuses.values()
    )
    metrics["terminal_status_by_attempt_id"] = terminal_statuses
    metrics["_start_records"] = starts
    metrics["_terminal_records"] = terminals
    metrics["unresolved_started_attempts"] = len(starts.keys() - terminals.keys())
    metrics["terminal_without_start"] = len(terminals.keys() - starts.keys())
    metrics["terminal_before_start"] = sum(
        attempt_id in starts
        and terminal["line_number"] < starts[attempt_id]["line_number"]
        for attempt_id, terminal in terminals.items()
    )
    for attempt_id in starts.keys() & terminals.keys():
        start = starts[attempt_id]
        terminal = terminals[attempt_id]
        if any(start[key] != terminal[key] for key in ATTEMPT_COMMON_FIELDS):
            metrics["common_field_mismatches"] += 1
        if parse_timestamp(terminal["recorded_at"]) < parse_timestamp(
            start["recorded_at"]
        ):
            metrics["terminal_before_start"] += 1

    metrics["distinct_accounts"] = sorted(
        {record["account"] for record in starts.values()}
    )
    metrics["distinct_data_dirs"] = sorted(
        {record["data_dir"] for record in starts.values()}
    )
    metrics["distinct_account_lock_keys"] = sorted(
        {record["account_lock_key"] for record in starts.values()}
    )
    metrics["distinct_config_fingerprints"] = sorted(
        {record["config_fingerprint"] for record in starts.values()}
    )
    metrics["distinct_profit_compatibility_fingerprints"] = sorted(
        {record["profit_compatibility_fingerprint"] for record in starts.values()}
    )
    metrics["distinct_producer_executable_sha256"] = sorted(
        {record["producer_executable_sha256"] for record in starts.values()}
    )
    metrics["distinct_external_paper_executable_sha256"] = sorted(
        {record["external_paper_executable_sha256"] for record in starts.values()}
    )
    metrics["distinct_execution_profile_sha256"] = sorted(
        {record["execution_profile_sha256"] for record in starts.values()}
    )
    return metrics


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"invalid boolean {value!r}")


def parse_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite number {value!r}")
    return parsed


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_synthetic(row: dict[str, str]) -> bool:
    event_id = row.get("event_id", "").strip().lower()
    note = row.get("note", "").strip().lower()
    return event_id.startswith("synthetic-") or "synthetic" in note or "canary" in note


def check(key: str, passed: bool, actual: Any, required: str) -> dict[str, Any]:
    return {"key": key, "passed": passed, "actual": actual, "required": required}


def evaluate(
    trades_csv: Path,
    thresholds: dict[str, float],
    now: datetime | None = None,
    *,
    attempts_jsonl: Path | None = None,
    source_path: Path | None = None,
    source_input: Path | None = None,
    source_input_exists: bool | None = None,
    source_snapshot: bool = False,
    attempts_source_path: Path | None = None,
    attempts_source_input: Path | None = None,
    attempts_source_input_exists: bool | None = None,
    attempts_source_snapshot: bool = False,
    threshold_profile: str = "custom",
    expected_producer_binary_sha256: str | None = None,
    expected_adapter_executable_sha256: str | None = None,
) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    attempts_jsonl = attempts_jsonl or trades_csv.with_name(
        "paper_execution_attempts.jsonl"
    )
    reported_source = (source_path or trades_csv).resolve()
    reported_attempts_source = (attempts_source_path or attempts_jsonl).resolve()
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": now.isoformat(),
        "source": str(reported_source),
        "source_input": str((source_input or trades_csv).resolve()),
        "source_input_exists": (
            trades_csv.is_file()
            if source_input_exists is None
            else source_input_exists
        ),
        "source_snapshot": source_snapshot,
        "source_sha256": None,
        "attempts_source": str(reported_attempts_source),
        "attempts_source_input": str(
            (attempts_source_input or attempts_jsonl).resolve()
        ),
        "attempts_source_input_exists": (
            attempts_jsonl.is_file()
            if attempts_source_input_exists is None
            else attempts_source_input_exists
        ),
        "attempts_source_snapshot": attempts_source_snapshot,
        "attempts_source_sha256": None,
        "verified_profitable": False,
        "paper_evidence_eligible": False,
        "activation_eligible": False,
        "live_route_compatible": False,
        "future_profit_guaranteed": False,
        "live_profitability_proven": False,
        "threshold_profile": threshold_profile,
        "thresholds": dict(thresholds),
        "sample": {},
        "metrics": {},
        "execution_attempts": {},
        "checks": [],
        "blockers": [],
        "activation_checks": [],
        "activation_blockers": [],
        "execution_binding": {},
        "limitations": [
            "Paper fills cannot prove future or live profitability.",
            "Trade-level confidence assumes samples are representative.",
            "Event-clustered confidence reduces within-event correlation but still assumes events are representative.",
            "Fee, gas, settlement, and fill realism remain limited by scanner and paper-engine models.",
        ],
    }
    if not trades_csv.is_file():
        report["blockers"] = [f"trades_csv_missing:{trades_csv}"]
        return report

    attempt_metrics = read_attempt_journal(attempts_jsonl)
    report["attempts_source_sha256"] = attempt_metrics["source_sha256"]
    report["execution_attempts"] = {
        key: value
        for key, value in attempt_metrics.items()
        if key != "source_sha256" and not key.startswith("_")
    }

    detected_rows = 0
    paper_attempts = 0
    parity_rejected_attempts = 0
    pre_submit_rejected_attempts = 0
    invalid_pre_submit_rejections = 0
    pre_submit_rejections_by_code: dict[str, int] = {}
    unaccounted_error_attempts = 0
    excluded_synthetic = 0
    malformed_rows = 0
    duplicate_rows = 0
    missing_terminal_attempt_markers = 0
    invalid_terminal_attempt_markers = 0
    duplicate_terminal_attempt_markers = 0
    trade_terminal_statuses: dict[str, str] = {}
    trade_terminal_records: dict[str, dict[str, Any]] = {}
    samples: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    source_rows = 0

    missing_columns: list[str] = []
    with trades_csv.open("rb") as raw_handle:
        source_sha256 = sha256_open_file(raw_handle)
        report["source_sha256"] = source_sha256
        raw_handle.seek(0)
        text_handle = io.TextIOWrapper(raw_handle, encoding="utf-8-sig", newline="")
        try:
            reader = csv.DictReader(text_handle)
            missing_columns = sorted(REQUIRED_COLUMNS - set(reader.fieldnames or []))
            if not missing_columns:
                for row in reader:
                    source_rows += 1
                    if is_synthetic(row):
                        excluded_synthetic += 1
                        continue
                    mode = row["mode"].strip().lower()
                    if mode == "detected":
                        detected_rows += 1
                        continue
                    if mode != "paper":
                        continue
                    paper_attempts += 1
                    status = row["status"].strip().lower()
                    note = row["note"].strip()
                    if status == "pre_submit_rejected":
                        pre_submit_rejected_attempts += 1
                        attempt_ids = re.findall(
                            r"(?:^|;\s*)paper_attempt_id=([0-9A-Za-z-]+)(?=;|$)",
                            note,
                        )
                        attempt_statuses = re.findall(
                            r"(?:^|;\s*)paper_attempt_status=(accepted|rejected|error)(?=;|$)",
                            note,
                        )
                        rejection_match = re.fullmatch(
                            rf"{re.escape(PAPER_PRE_SUBMIT_REJECTION_PREFIX)}=([a-z0-9_]+); [^\r\n]+",
                            note,
                        )
                        valid_rejection = (
                            rejection_match is not None
                            and rejection_match.group(1)
                            in PAPER_PRE_SUBMIT_REJECTION_CODES
                            and not attempt_ids
                            and not attempt_statuses
                        )
                        try:
                            if not row["event_id"].strip() or not row["arb_type"].strip():
                                raise ValueError("missing pre-submit event id or arb type")
                            parse_timestamp(row["timestamp"])
                        except (TypeError, ValueError):
                            valid_rejection = False
                            malformed_rows += 1
                        if valid_rejection:
                            code = rejection_match.group(1)
                            pre_submit_rejections_by_code[code] = (
                                pre_submit_rejections_by_code.get(code, 0) + 1
                            )
                        else:
                            invalid_pre_submit_rejections += 1
                        continue
                    expected_terminal_status = {
                        "ok": "accepted",
                        "parity_rejected": "rejected",
                    }.get(status, "error")
                    attempt_ids = re.findall(
                        r"(?:^|;\s*)paper_attempt_id=([0-9A-Za-z-]+)(?=;|$)",
                        note,
                    )
                    attempt_statuses = re.findall(
                        r"(?:^|;\s*)paper_attempt_status=(accepted|rejected|error)(?=;|$)",
                        note,
                    )
                    marker_required = status in {"ok", "parity_rejected"}
                    marker_present = bool(attempt_ids or attempt_statuses)
                    valid_attempt_id: str | None = None
                    if marker_required and not marker_present:
                        missing_terminal_attempt_markers += 1
                    elif marker_present:
                        if (
                            len(attempt_ids) != 1
                            or len(attempt_statuses) != 1
                            or attempt_statuses[0] != expected_terminal_status
                        ):
                            invalid_terminal_attempt_markers += 1
                        elif attempt_ids[0] in trade_terminal_statuses:
                            duplicate_terminal_attempt_markers += 1
                        else:
                            valid_attempt_id = attempt_ids[0]
                            trade_terminal_statuses[valid_attempt_id] = attempt_statuses[0]
                    if status not in {"ok", "parity_rejected"}:
                        unaccounted_error_attempts += 1
                        try:
                            event_id = row["event_id"].strip()
                            arb_type = row["arb_type"].strip()
                            if not event_id or not arb_type:
                                raise ValueError("missing error event id or arb type")
                            if valid_attempt_id is not None:
                                trade_terminal_records[valid_attempt_id] = {
                                    "timestamp": parse_timestamp(row["timestamp"]),
                                    "status": "error",
                                    "event_id": event_id,
                                    "arb_type": arb_type,
                                }
                        except (TypeError, ValueError):
                            malformed_rows += 1
                        continue
                    if status == "parity_rejected":
                        parity_rejected_attempts += 1
                    fingerprint = tuple(
                        row[key]
                        for key in (
                            "timestamp",
                            "scan_id",
                            "event_id",
                            "filled_cost_usd",
                            "conservative_pnl_usd",
                        )
                    )
                    if fingerprint in seen:
                        duplicate_rows += 1
                        continue
                    seen.add(fingerprint)
                    try:
                        cost = parse_float(row["filled_cost_usd"])
                        unhedged = parse_float(row["unhedged_notional_usd"])
                        if cost <= 0 or unhedged < 0 or not row["event_id"].strip():
                            raise ValueError("invalid accepted-trade economics")
                        sample = {
                            "timestamp": parse_timestamp(row["timestamp"]),
                            "accepted": status == "ok",
                            "event_id": row["event_id"].strip(),
                            "arb_type": row["arb_type"].strip(),
                            "cost": cost,
                            "pnl": parse_float(row["conservative_pnl_usd"]),
                            "roi": parse_float(row["conservative_roi_pct"]),
                            "partial": parse_bool(row["partial_fill"]),
                            "parity": parse_bool(row["parity_ok"]),
                            "unhedged": unhedged,
                            "clob": parse_bool(row["prices_from_clob"]),
                            "filled_hedged": row["pnl_scale"].strip().lower()
                            == "filled_hedged",
                            "planned_basket_units": parse_float(
                                row["planned_basket_units"]
                            ),
                            "hedged_basket_units": parse_float(
                                row["hedged_basket_units"]
                            ),
                            "fill_count": int(row["fill_count"]),
                        }
                        if (
                            sample["planned_basket_units"] <= 0
                            or sample["hedged_basket_units"] < 0
                            or sample["fill_count"] <= 0
                            or not sample["arb_type"]
                        ):
                            raise ValueError("invalid basket/fill metrics")
                        samples.append(sample)
                        if valid_attempt_id is not None:
                            trade_terminal_records[valid_attempt_id] = {
                                "timestamp": sample["timestamp"],
                                "status": expected_terminal_status,
                                "event_id": sample["event_id"],
                                "arb_type": sample["arb_type"],
                                "parity_ok": sample["parity"],
                                "any_partial": sample["partial"],
                                "hedged_cost_usd": sample["cost"],
                                "conservative_pnl_usd": sample["pnl"],
                                "conservative_roi_pct": sample["roi"],
                                "planned_basket_units": sample[
                                    "planned_basket_units"
                                ],
                                "hedged_basket_units": sample[
                                    "hedged_basket_units"
                                ],
                                "fill_count": sample["fill_count"],
                                "unhedged_notional_usd": sample["unhedged"],
                            }
                    except (TypeError, ValueError):
                        malformed_rows += 1
        finally:
            text_handle.detach()
        if sha256_open_file(raw_handle) != source_sha256:
            report["blockers"] = ["source_changed_during_evaluation"]
            return report

    if missing_columns:
        report["blockers"] = [f"missing_columns:{'|'.join(missing_columns)}"]
        return report

    start_records: dict[str, dict[str, Any]] = attempt_metrics["_start_records"]
    terminal_records: dict[str, dict[str, Any]] = attempt_metrics[
        "_terminal_records"
    ]
    binding_errors: list[str] = []
    for attempt_id, terminal in sorted(terminal_records.items()):
        if terminal["status"] not in {"accepted", "rejected"}:
            continue
        start = start_records.get(attempt_id)
        if start is None:
            continue
        for error in recompute_fill_evidence(start, terminal):
            binding_errors.append(f"{attempt_id}:fill_evidence:{error}")
    for attempt_id in sorted(
        terminal_records.keys() & trade_terminal_records.keys()
    ):
        terminal = terminal_records[attempt_id]
        trade = trade_terminal_records[attempt_id]
        start = start_records.get(attempt_id)
        if start is None:
            binding_errors.append(f"{attempt_id}:missing_start")
            continue
        if not (
            start["event_id"] == terminal["event_id"] == trade["event_id"]
        ):
            binding_errors.append(f"{attempt_id}:event_id")
        if not (
            start["arb_type"] == terminal["arb_type"] == trade["arb_type"]
        ):
            binding_errors.append(f"{attempt_id}:arb_type")
        if terminal["status"] != trade["status"]:
            binding_errors.append(f"{attempt_id}:status")
        start_at = parse_timestamp(start["recorded_at"])
        terminal_at = parse_timestamp(terminal["recorded_at"])
        trade_at = trade["timestamp"]
        if terminal_at < start_at:
            binding_errors.append(f"{attempt_id}:terminal_before_start")
        lag_seconds = (trade_at - terminal_at).total_seconds()
        if not 0 <= lag_seconds <= MAX_TERMINAL_TRADE_LAG_SECONDS:
            binding_errors.append(
                f"{attempt_id}:terminal_trade_lag={lag_seconds:.6f}"
            )

        if terminal["status"] in {"accepted", "rejected"}:
            expected_parity = terminal["status"] == "accepted"
            if terminal["parity_ok"] != expected_parity:
                binding_errors.append(f"{attempt_id}:terminal_parity_status")
            for key in ("parity_ok", "any_partial", "fill_count"):
                if terminal[key] != trade[key]:
                    binding_errors.append(f"{attempt_id}:{key}")
            for key in (
                "hedged_cost_usd",
                "conservative_pnl_usd",
                "conservative_roi_pct",
                "planned_basket_units",
                "hedged_basket_units",
                "unhedged_notional_usd",
            ):
                if not math.isclose(
                    terminal[key],
                    trade[key],
                    rel_tol=1e-10,
                    abs_tol=FLOAT_BIND_ABS_TOL,
                ):
                    binding_errors.append(f"{attempt_id}:{key}")

    report["execution_binding"] = {
        "bound_attempts": len(terminal_records.keys() & trade_terminal_records.keys()),
        "binding_error_count": len(binding_errors),
        "binding_errors": binding_errors[:50],
        "max_terminal_trade_lag_seconds": MAX_TERMINAL_TRADE_LAG_SECONDS,
        "expected_producer_binary_sha256": expected_producer_binary_sha256,
        "expected_adapter_executable_sha256": expected_adapter_executable_sha256,
    }

    samples.sort(key=lambda item: item["timestamp"])
    accepted_samples = [item for item in samples if item["accepted"]]
    pnls = [item["pnl"] for item in samples]
    costs = [item["cost"] for item in samples]
    timestamps = [item["timestamp"] for item in accepted_samples]
    sample_count = len(accepted_samples)
    completed_count = len(samples)
    unique_events = len(
        {item["event_id"] for item in accepted_samples if item["event_id"]}
    )
    total_pnl = sum(pnls)
    total_cost = sum(costs)
    weighted_roi = total_pnl / total_cost * 100 if total_cost > 0 else 0.0
    mean_pnl = statistics.fmean(pnls) if pnls else 0.0
    stddev_pnl = statistics.stdev(pnls) if len(pnls) > 1 else 0.0
    lower_mean = (
        mean_pnl
        - ONE_SIDED_95_CONSERVATIVE_CRITICAL
        * stddev_pnl
        / math.sqrt(completed_count)
        if completed_count
        else 0.0
    )
    pnls_by_event: dict[str, list[float]] = {}
    for item in samples:
        pnls_by_event.setdefault(item["event_id"], []).append(item["pnl"])
    event_mean_pnls = [statistics.fmean(values) for values in pnls_by_event.values()]
    event_mean_pnl = statistics.fmean(event_mean_pnls) if event_mean_pnls else 0.0
    event_mean_stddev = (
        statistics.stdev(event_mean_pnls) if len(event_mean_pnls) > 1 else 0.0
    )
    event_lower_mean = (
        event_mean_pnl
        - ONE_SIDED_95_CONSERVATIVE_CRITICAL
        * event_mean_stddev
        / math.sqrt(len(event_mean_pnls))
        if event_mean_pnls
        else 0.0
    )
    duration_hours = (
        (timestamps[-1] - timestamps[0]).total_seconds() / 3600 if len(timestamps) > 1 else 0.0
    )
    evidence_age_hours = (
        (now - timestamps[-1]).total_seconds() / 3600 if timestamps else math.inf
    )
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    gains = sum(value for value in pnls if value > 0)
    losses = -sum(value for value in pnls if value < 0)
    fill_success_rate = sample_count / paper_attempts if paper_attempts else 0.0
    submit_conversion_rate = (
        len(trade_terminal_records) / paper_attempts if paper_attempts else 0.0
    )
    pre_submit_rejection_rate = (
        pre_submit_rejected_attempts / paper_attempts if paper_attempts else 0.0
    )
    positive_trade_rate = sum(value > 0 for value in pnls) / completed_count if completed_count else 0.0
    unsafe_partial = sum(item["partial"] for item in samples)
    unsafe_parity = sum(not item["parity"] for item in samples)
    unsafe_clob = sum(not item["clob"] for item in samples)
    unsafe_scale = sum(not item["filled_hedged"] for item in samples)
    max_unhedged = max((item["unhedged"] for item in samples), default=0.0)

    report["sample"] = {
        "source_rows": source_rows,
        "detected_rows": detected_rows,
        "paper_attempts": paper_attempts,
        "accepted_trades": sample_count,
        "completed_trade_rows": completed_count,
        "parity_rejected_attempts": parity_rejected_attempts,
        "pre_submit_rejected_attempts": pre_submit_rejected_attempts,
        "pre_submit_rejections_by_code": dict(
            sorted(pre_submit_rejections_by_code.items())
        ),
        "invalid_pre_submit_rejections": invalid_pre_submit_rejections,
        "unaccounted_error_attempts": unaccounted_error_attempts,
        "unique_events": unique_events,
        "completed_event_clusters": len(event_mean_pnls),
        "excluded_synthetic_or_canary_rows": excluded_synthetic,
        "duplicate_rows": duplicate_rows,
        "malformed_rows": malformed_rows,
        "missing_terminal_attempt_markers": missing_terminal_attempt_markers,
        "invalid_terminal_attempt_markers": invalid_terminal_attempt_markers,
        "duplicate_terminal_attempt_markers": duplicate_terminal_attempt_markers,
        "first_trade_at": timestamps[0].isoformat() if timestamps else None,
        "last_trade_at": timestamps[-1].isoformat() if timestamps else None,
        "observation_hours": duration_hours,
        "evidence_age_hours": evidence_age_hours if math.isfinite(evidence_age_hours) else None,
    }
    report["metrics"] = {
        "total_filled_cost_usd": total_cost,
        "total_conservative_pnl_usd": total_pnl,
        "weighted_conservative_roi_pct": weighted_roi,
        "mean_conservative_pnl_usd": mean_pnl,
        "pnl_stddev_usd": stddev_pnl,
        "one_sided_95_conservative_critical": ONE_SIDED_95_CONSERVATIVE_CRITICAL,
        "one_sided_95_conservative_lower_mean_pnl_usd": lower_mean,
        "mean_event_mean_conservative_pnl_usd": event_mean_pnl,
        "event_mean_pnl_stddev_usd": event_mean_stddev,
        "one_sided_95_conservative_event_clustered_lower_mean_pnl_usd": event_lower_mean,
        "fill_success_rate": fill_success_rate,
        "submit_conversion_rate": submit_conversion_rate,
        "pre_submit_rejection_rate": pre_submit_rejection_rate,
        "positive_trade_rate": positive_trade_rate,
        "max_drawdown_usd": max_drawdown,
        "profit_factor": gains / losses if losses > 0 else None,
        "profit_factor_infinite": losses == 0 and gains > 0,
        "partial_fill_rows": unsafe_partial,
        "parity_failure_rows": unsafe_parity,
        "non_clob_price_rows": unsafe_clob,
        "non_filled_hedged_rows": unsafe_scale,
        "max_unhedged_notional_usd": max_unhedged,
    }

    starts = list(start_records.values())
    parity_safe_profiles = bool(starts) and all(
        start["execution_route"] == "legged_clob_paper"
        and start["order_mode"] == "market_style"
        and start["effective_order_type"] == "fok"
        and start["live_order_type"] == "fok"
        and start["full_clob_required"]
        and start["match_live_position_size"]
        and start["execution_profile"]["exclusive_paper_account_lock"]
        and math.isclose(
            start["effective_position_size_usd"],
            start["execution_profile"]["live_position_size_usd"],
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
        and not start["execution_profile"]["effective_paper_use_limit_orders"]
        and all(
            start["execution_profile"][key]
            for key in (
                "fresh_clob_enrichment_complete",
                "fresh_depth_complete",
                "fresh_fee_schedules_complete",
                "pre_submit_orderability_complete",
            )
        )
        for start in starts
    )
    official_endpoints = bool(starts) and all(
        start["execution_profile"]["clob_api_url"].rstrip("/")
        == OFFICIAL_CLOB_API_URL
        and start["execution_profile"]["gamma_api_url"].rstrip("/")
        == OFFICIAL_GAMMA_API_URL
        for start in starts
    )
    uniform_campaign_binding = bool(starts) and all(
        len(attempt_metrics[key]) == 1
        for key in (
            "distinct_accounts",
            "distinct_data_dirs",
            "distinct_account_lock_keys",
            "distinct_config_fingerprints",
            "distinct_profit_compatibility_fingerprints",
            "distinct_producer_executable_sha256",
            "distinct_external_paper_executable_sha256",
            "distinct_execution_profile_sha256",
        )
    )
    expected_producer = (
        expected_producer_binary_sha256.lower()
        if isinstance(expected_producer_binary_sha256, str)
        else None
    )
    expected_producer_matches = expected_producer is None or (
        SHA256_RE.fullmatch(expected_producer) is not None
        and bool(starts)
        and all(
            start["producer_executable_sha256"] == expected_producer
            for start in starts
        )
    )
    expected_adapter = (
        expected_adapter_executable_sha256.lower()
        if isinstance(expected_adapter_executable_sha256, str)
        else None
    )
    expected_adapter_matches = expected_adapter is None or (
        SHA256_RE.fullmatch(expected_adapter) is not None
        and bool(starts)
        and all(
            start["external_paper_executable_sha256"] == expected_adapter
            for start in starts
        )
    )
    live_route_compatible = bool(starts) and all(
        start["live_route_compatible"]
        and start["execution_route"] != "legged_clob_paper"
        for start in starts
    )
    uniform_execution_profile = (
        starts[0]["execution_profile"]
        if starts
        and len(attempt_metrics["distinct_execution_profile_sha256"]) == 1
        else None
    )
    paper_live_profile_config = (
        {
            key: uniform_execution_profile[key]
            for key in sorted(PAPER_LIVE_PROFILE_CONFIG_KEYS)
        }
        if uniform_execution_profile is not None
        else None
    )
    report["live_route_compatible"] = live_route_compatible
    report["execution_binding"].update(
        {
            "accounts": attempt_metrics["distinct_accounts"],
            "data_dirs": attempt_metrics["distinct_data_dirs"],
            "account_lock_keys": attempt_metrics["distinct_account_lock_keys"],
            "config_fingerprints": attempt_metrics[
                "distinct_config_fingerprints"
            ],
            "profit_compatibility_fingerprints": attempt_metrics[
                "distinct_profit_compatibility_fingerprints"
            ],
            "producer_executable_sha256": attempt_metrics[
                "distinct_producer_executable_sha256"
            ],
            "external_paper_executable_sha256": attempt_metrics[
                "distinct_external_paper_executable_sha256"
            ],
            "execution_profile_sha256": attempt_metrics[
                "distinct_execution_profile_sha256"
            ],
            "execution_profile": uniform_execution_profile,
            "paper_live_profile_config": paper_live_profile_config,
            "paper_live_profile_config_sha256": (
                canonical_json_sha256(paper_live_profile_config)
                if paper_live_profile_config is not None
                else None
            ),
            "parity_safe_profiles": parity_safe_profiles,
            "official_endpoints": official_endpoints,
            "uniform_campaign_binding": uniform_campaign_binding,
            "expected_producer_matches": expected_producer_matches,
            "expected_adapter_matches": expected_adapter_matches,
        }
    )

    checks = [
        check(
            "paper_attempt_journal_present",
            bool(report["attempts_source_input_exists"]),
            report["attempts_source_input_exists"],
            "true",
        ),
        check(
            "paper_attempt_journal_stable",
            not attempt_metrics["source_changed_during_evaluation"],
            attempt_metrics["source_changed_during_evaluation"],
            "false",
        ),
        check(
            "paper_attempt_journal_valid",
            all(
                attempt_metrics[key] == 0
                for key in (
                    "malformed_records",
                    "duplicate_start_records",
                    "duplicate_terminal_records",
                    "terminal_without_start",
                    "terminal_before_start",
                    "common_field_mismatches",
                )
            ),
            {
                key: attempt_metrics[key]
                for key in (
                    "malformed_records",
                    "duplicate_start_records",
                    "duplicate_terminal_records",
                    "terminal_without_start",
                    "terminal_before_start",
                    "common_field_mismatches",
                )
            },
            "all zero",
        ),
        check(
            "strict_campaign_baseline_continuity",
            attempt_metrics["non_increasing_baseline_trade_ids"] == 0
            and attempt_metrics["baseline_below_prior_raw_trade_id"] == 0,
            {
                "non_increasing_baseline_trade_ids": attempt_metrics[
                    "non_increasing_baseline_trade_ids"
                ],
                "baseline_below_prior_raw_trade_id": attempt_metrics[
                    "baseline_below_prior_raw_trade_id"
                ],
            },
            "all zero; baselines strictly increase and never reset below prior raw fills",
        ),
        check(
            "globally_unique_raw_trade_ids",
            attempt_metrics["duplicate_raw_trade_ids"] == 0,
            attempt_metrics["duplicate_raw_trade_ids"],
            "0",
        ),
        check(
            "all_started_paper_attempts_reconciled",
            attempt_metrics["unresolved_started_attempts"] == 0,
            attempt_metrics["unresolved_started_attempts"],
            "0",
        ),
        check(
            "no_terminal_paper_attempt_errors",
            attempt_metrics["terminal_errors"] == 0,
            attempt_metrics["terminal_errors"],
            "0",
        ),
        check(
            "paper_attempt_journal_matches_trades",
            attempt_metrics["terminal_status_by_attempt_id"]
            == trade_terminal_statuses
            and missing_terminal_attempt_markers == 0
            and invalid_terminal_attempt_markers == 0
            and duplicate_terminal_attempt_markers == 0,
            {
                "journal_terminals": attempt_metrics[
                    "terminal_status_by_attempt_id"
                ],
                "trade_terminals": trade_terminal_statuses,
                "missing_markers": missing_terminal_attempt_markers,
                "invalid_markers": invalid_terminal_attempt_markers,
                "duplicate_markers": duplicate_terminal_attempt_markers,
            },
            "exact attempt-id/status bijection",
        ),
        check(
            "paper_attempt_journal_economics_match_trades",
            terminal_records.keys() == trade_terminal_records.keys()
            and not binding_errors,
            {
                "journal_attempt_ids": sorted(terminal_records),
                "trade_attempt_ids": sorted(trade_terminal_records),
                "binding_errors": binding_errors[:50],
            },
            "exact planned/raw-fill mapping, independently recomputed fees/economics, CSV fields, and start<=terminal<=trade within 30s",
        ),
        check("min_trades", sample_count >= thresholds["min_trades"], sample_count, f">={thresholds['min_trades']:g}"),
        check("min_unique_events", unique_events >= thresholds["min_unique_events"], unique_events, f">={thresholds['min_unique_events']:g}"),
        check("min_observation_hours", duration_hours >= thresholds["min_observation_hours"], duration_hours, f">={thresholds['min_observation_hours']:g}"),
        check("fresh_evidence", 0 <= evidence_age_hours <= thresholds["max_evidence_age_hours"], evidence_age_hours if math.isfinite(evidence_age_hours) else None, f"0..{thresholds['max_evidence_age_hours']:g}"),
        check("min_total_pnl_usd", total_pnl >= thresholds["min_total_pnl_usd"], total_pnl, f">={thresholds['min_total_pnl_usd']:g}"),
        check("min_weighted_roi_pct", weighted_roi >= thresholds["min_weighted_roi_pct"], weighted_roi, f">={thresholds['min_weighted_roi_pct']:g}"),
        check("positive_conservative_lower_mean_pnl", lower_mean > thresholds["min_lower_mean_pnl_usd"], lower_mean, f">{thresholds['min_lower_mean_pnl_usd']:g}"),
        check("positive_conservative_event_clustered_lower_mean_pnl", event_lower_mean > thresholds["min_event_lower_mean_pnl_usd"], event_lower_mean, f">{thresholds['min_event_lower_mean_pnl_usd']:g}"),
        check("min_fill_success_rate", fill_success_rate >= thresholds["min_fill_success_rate"], fill_success_rate, f">={thresholds['min_fill_success_rate']:g}"),
        check("min_positive_trade_rate", positive_trade_rate >= thresholds["min_positive_trade_rate"], positive_trade_rate, f">={thresholds['min_positive_trade_rate']:g}"),
        check("max_drawdown_usd", max_drawdown <= thresholds["max_drawdown_usd"], max_drawdown, f"<={thresholds['max_drawdown_usd']:g}"),
        check("complete_hedges", unsafe_partial == 0 and unsafe_parity == 0 and unsafe_scale == 0, {"partial": unsafe_partial, "parity": unsafe_parity, "scale": unsafe_scale}, "all zero"),
        check("clob_prices_only", unsafe_clob == 0, unsafe_clob, "0"),
        check("max_unhedged_notional_usd", max_unhedged <= thresholds["max_unhedged_notional_usd"], max_unhedged, f"<={thresholds['max_unhedged_notional_usd']:g}"),
        check("no_unaccounted_execution_errors", unaccounted_error_attempts == 0, unaccounted_error_attempts, "0"),
        check(
            "valid_pre_submit_rejections",
            invalid_pre_submit_rejections == 0,
            invalid_pre_submit_rejections,
            f"0; only typed {PAPER_PRE_SUBMIT_REJECTION_PREFIX}=<known-code> rows without attempt markers",
        ),
        check("no_duplicate_rows", duplicate_rows == 0, duplicate_rows, "0"),
        check("no_malformed_rows", malformed_rows == 0, malformed_rows, "0"),
    ]
    report["checks"] = checks
    report["blockers"] = [
        f"{item['key']}:actual={item['actual']}:required={item['required']}"
        for item in checks
        if not item["passed"]
    ]
    report["verified_profitable"] = not report["blockers"]
    activation_checks = [
        check(
            "exploratory_profitability_verified",
            report["verified_profitable"],
            report["verified_profitable"],
            "true",
        ),
        check(
            "parity_safe_fok_execution_profile",
            parity_safe_profiles,
            parity_safe_profiles,
            "market_style FOK matching live FOK, full CLOB, live sizing, strict freshness",
        ),
        check(
            "official_polymarket_endpoints",
            official_endpoints,
            official_endpoints,
            f"{OFFICIAL_CLOB_API_URL} and {OFFICIAL_GAMMA_API_URL}",
        ),
        check(
            "uniform_campaign_execution_binding",
            uniform_campaign_binding,
            {
                key: attempt_metrics[key]
                for key in (
                    "distinct_accounts",
                    "distinct_data_dirs",
                    "distinct_account_lock_keys",
                    "distinct_config_fingerprints",
                    "distinct_profit_compatibility_fingerprints",
                    "distinct_producer_executable_sha256",
                    "distinct_external_paper_executable_sha256",
                    "distinct_execution_profile_sha256",
                )
            },
            "exactly one value for every binding",
        ),
        check(
            "campaign_chronology_monotonic",
            attempt_metrics["non_monotonic_recorded_timestamps"] == 0
            and attempt_metrics["non_increasing_baseline_trade_ids"] == 0
            and attempt_metrics["baseline_below_prior_raw_trade_id"] == 0,
            {
                "timestamp_regressions": attempt_metrics[
                    "non_monotonic_recorded_timestamps"
                ],
                "non_increasing_baseline_trade_ids": attempt_metrics[
                    "non_increasing_baseline_trade_ids"
                ],
                "baseline_below_prior_raw_trade_id": attempt_metrics[
                    "baseline_below_prior_raw_trade_id"
                ],
            },
            "all zero",
        ),
        check(
            "expected_producer_binary_matches",
            expected_producer_matches,
            {
                "expected": expected_producer,
                "observed": attempt_metrics[
                    "distinct_producer_executable_sha256"
                ],
            },
            "all attempts match when an expected SHA-256 is supplied",
        ),
        check(
            "expected_adapter_executable_matches",
            expected_adapter_matches,
            {
                "expected": expected_adapter,
                "observed": attempt_metrics[
                    "distinct_external_paper_executable_sha256"
                ],
            },
            "all attempts match when an expected adapter SHA-256 is supplied",
        ),
        check(
            "paper_evidence_live_route_compatible",
            live_route_compatible,
            {
                "live_route_compatible": live_route_compatible,
                "routes": sorted(
                    {start["execution_route"] for start in starts}
                ),
            },
            "true; legged_clob_paper cannot establish Combo/RFQ route profitability",
        ),
    ]
    report["activation_checks"] = activation_checks
    report["activation_blockers"] = [
        f"{item['key']}:actual={item['actual']}:required={item['required']}"
        for item in activation_checks
        if not item["passed"]
    ]
    report["paper_evidence_eligible"] = all(
        item["passed"]
        for item in activation_checks
        if item["key"] != "paper_evidence_live_route_compatible"
    )
    non_route_binding_blockers = [
        f"{item['key']}:actual={item['actual']}:required={item['required']}"
        for item in activation_checks
        if item["key"] != "paper_evidence_live_route_compatible"
        and not item["passed"]
    ]
    for blocker in non_route_binding_blockers:
        if blocker not in report["blockers"]:
            report["blockers"].append(blocker)
    # "Verified profitable" means one deployable, uniformly bound paper
    # campaign. Combo/RFQ route compatibility remains a separate live proof.
    report["verified_profitable"] = report["paper_evidence_eligible"]
    report["activation_eligible"] = not report["activation_blockers"]
    return report


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def stage_source_snapshot(source: Path, destination: Path) -> tuple[Path, bool]:
    if source.resolve() == destination.resolve():
        raise ValueError("source snapshot path must differ from its input path")
    destination.parent.mkdir(parents=True, exist_ok=True)
    input_exists = source.is_file()
    with tempfile.NamedTemporaryFile(
        mode="wb", prefix=f".{destination.name}.", dir=destination.parent, delete=False
    ) as temporary:
        temporary_path = Path(temporary.name)
        if input_exists:
            with source.open("rb") as source_handle:
                shutil.copyfileobj(source_handle, temporary, length=1024 * 1024)
        temporary.flush()
        os.fsync(temporary.fileno())
    return temporary_path, input_exists


def publish_source_snapshot(staged: Path, destination: Path) -> None:
    staged.chmod(0o444)
    os.replace(staged, destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trades-csv", type=Path, required=True)
    parser.add_argument(
        "--attempts-jsonl",
        type=Path,
        help="paper execution attempt journal (defaults beside trades.csv)",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--source-snapshot",
        type=Path,
        help="immutable trades snapshot path (defaults beside output)",
    )
    parser.add_argument(
        "--attempts-snapshot",
        type=Path,
        help="immutable attempt-journal snapshot path (defaults beside output)",
    )
    parser.add_argument(
        "--activation-thresholds",
        action="store_true",
        help="ignore PAPER_PROFIT_* overrides and use fixed live-activation minimums",
    )
    parser.add_argument(
        "--expected-producer-binary-sha256",
        help="require every attempt to come from this independently hashed scanner executable",
    )
    parser.add_argument(
        "--expected-adapter-executable-sha256",
        help="require every attempt to use this independently hashed paper-adapter executable",
    )
    args = parser.parse_args()
    if args.expected_producer_binary_sha256 and not SHA256_RE.fullmatch(
        args.expected_producer_binary_sha256
    ):
        parser.error("--expected-producer-binary-sha256 must be 64 hexadecimal characters")
    if args.expected_adapter_executable_sha256 and not SHA256_RE.fullmatch(
        args.expected_adapter_executable_sha256
    ):
        parser.error("--expected-adapter-executable-sha256 must be 64 hexadecimal characters")
    attempts_jsonl = args.attempts_jsonl or args.trades_csv.with_name(
        "paper_execution_attempts.jsonl"
    )
    source_snapshot = args.source_snapshot or args.output.with_name(
        f"{args.output.stem}-trades-source.csv"
    )
    attempts_snapshot = args.attempts_snapshot or args.output.with_name(
        f"{args.output.stem}-attempts-source.jsonl"
    )
    staged_trades: Path | None = None
    staged_attempts: Path | None = None
    try:
        if source_snapshot.resolve() == attempts_snapshot.resolve():
            raise ValueError("trades and attempt snapshots must use different paths")
        if args.output.resolve() in {
            source_snapshot.resolve(),
            attempts_snapshot.resolve(),
        }:
            raise ValueError("report output must differ from source snapshot paths")
        staged_trades, trades_input_exists = stage_source_snapshot(
            args.trades_csv, source_snapshot
        )
        staged_attempts, attempts_input_exists = stage_source_snapshot(
            attempts_jsonl, attempts_snapshot
        )
        thresholds = (
            activation_thresholds()
            if args.activation_thresholds
            else thresholds_from_env()
        )
        report = evaluate(
            staged_trades,
            thresholds,
            attempts_jsonl=staged_attempts,
            source_path=source_snapshot,
            source_input=args.trades_csv,
            source_input_exists=trades_input_exists,
            source_snapshot=True,
            attempts_source_path=attempts_snapshot,
            attempts_source_input=attempts_jsonl,
            attempts_source_input_exists=attempts_input_exists,
            attempts_source_snapshot=True,
            threshold_profile="activation" if args.activation_thresholds else "environment",
            expected_producer_binary_sha256=args.expected_producer_binary_sha256,
            expected_adapter_executable_sha256=args.expected_adapter_executable_sha256,
        )
        publish_source_snapshot(staged_trades, source_snapshot)
        staged_trades = None
        publish_source_snapshot(staged_attempts, attempts_snapshot)
        staged_attempts = None
    except (OSError, UnicodeError, ValueError) as error:
        report = {
            "schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": str(source_snapshot.resolve()),
            "source_input": str(args.trades_csv.resolve()),
            "source_snapshot": True,
            "attempts_source": str(attempts_snapshot.resolve()),
            "attempts_source_input": str(attempts_jsonl.resolve()),
            "attempts_source_snapshot": True,
            "verified_profitable": False,
            "paper_evidence_eligible": False,
            "activation_eligible": False,
            "live_route_compatible": False,
            "future_profit_guaranteed": False,
            "live_profitability_proven": False,
            "threshold_profile": (
                "activation" if args.activation_thresholds else "environment"
            ),
            "checks": [],
            "blockers": [f"evaluation_error:{error}"],
            "activation_checks": [],
            "activation_blockers": [f"evaluation_error:{error}"],
        }
    finally:
        for staged in (staged_trades, staged_attempts):
            if staged is not None:
                staged.unlink(missing_ok=True)
    write_report(args.output, report)
    print(
        f"paper_profitability_verified={str(report['verified_profitable']).lower()} "
        f"paper_evidence_eligible={str(report['paper_evidence_eligible']).lower()} "
        f"activation_eligible={str(report['activation_eligible']).lower()} "
        f"blockers={len(report['blockers'])} "
        f"activation_blockers={len(report['activation_blockers'])} output={args.output}"
    )
    success = (
        report["paper_evidence_eligible"]
        if args.activation_thresholds
        else report["verified_profitable"]
    )
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
