#!/usr/bin/env python3
"""Read-only Limitless/Polymarket mirror spread evaluator.

This tool never authenticates and never submits. Exact normalized titles are
discovery hints only. A reviewed, hash-bound pair certificate is required to
mark resolution rules as certified, and even certified pairs remain shadow-only
until a separate cross-chain execution route is implemented and verified.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import html
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
LIMITLESS_API = "https://api.limitless.exchange"
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"
PAGE_SIZE = 25
MAX_LIMITLESS_PAGES = 40
POLYMARKET_SEARCH_PAGE_SIZE = 25
MAX_POLYMARKET_SEARCH_PAGES = 20
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_LIMITLESS_BUY_FEE_RATE = 0.03
USER_AGENT = "polymarket-arb-scanner-cross-venue-shadow/1"


class ShadowError(RuntimeError):
    """Fail-closed input, schema, or network error."""


def finite_number(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ShadowError(f"{label} is not numeric") from error
    if not math.isfinite(number):
        raise ShadowError(f"{label} is not finite")
    return number


def normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def strip_html(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def rules_fingerprint(
    title: str, description: str, expiry: Any, identity: Any = None
) -> str:
    body = json.dumps(
        {
            "title": title.strip(),
            "description": description.strip(),
            "expiry": expiry,
            "identity": identity,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(body).hexdigest()


def parse_json_array(value: Any, label: str) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ShadowError(f"{label} is not valid JSON") from error
    if not isinstance(value, list):
        raise ShadowError(f"{label} is not an array")
    return value


def get_json(
    base_url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 15.0,
    retries: int = 3,
) -> Any:
    query = urllib.parse.urlencode(params or {})
    url = f"{base_url}?{query}" if query else base_url
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise ShadowError(f"response too large: {url}")
                return json.loads(body)
        except urllib.error.HTTPError as error:
            last_error = error
            if error.code != 429 and not 500 <= error.code < 600:
                raise ShadowError(f"HTTP {error.code}: {url}") from error
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
        if attempt + 1 < max(1, retries):
            time.sleep(min(0.25 * (2**attempt), 2.0))
    raise ShadowError(f"request failed after {retries} attempts: {url}: {last_error}")


def is_limitless_mirror(market: dict[str, Any]) -> bool:
    tokens = market.get("tokens")
    metadata = market.get("metadata")
    return (
        market.get("status") == "FUNDED"
        and market.get("expired") is False
        and market.get("marketType") == "single"
        and market.get("tradeType") == "clob"
        and isinstance(tokens, dict)
        and bool(str(tokens.get("yes", "")).strip())
        and bool(str(tokens.get("no", "")).strip())
        and isinstance(metadata, dict)
        and metadata.get("isPolyArbitrage") is True
        and bool(str(market.get("slug", "")).strip())
        and bool(str(market.get("title", "")).strip())
    )


def discover_limitless_markets(timeout: float) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in range(1, MAX_LIMITLESS_PAGES + 1):
        payload = get_json(
            f"{LIMITLESS_API}/markets/active",
            params={
                "tradeType": "clob",
                "sortBy": "high_value",
                "limit": PAGE_SIZE,
                "page": page,
            },
            timeout=timeout,
        )
        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise ShadowError("Limitless active-market response missing data array")
        for row in rows:
            if not isinstance(row, dict) or not is_limitless_mirror(row):
                continue
            slug = str(row["slug"])
            if slug not in seen:
                seen.add(slug)
                markets.append(row)
        total = payload.get("totalMarketsCount")
        if len(rows) < PAGE_SIZE or (
            isinstance(total, int) and page * PAGE_SIZE >= total
        ):
            break
        if page == MAX_LIMITLESS_PAGES:
            raise ShadowError("Limitless discovery hit page safety cap before completion")
    return markets


def search_polymarket_title(title: str, timeout: float) -> list[dict[str, Any]]:
    """Return every active Gamma market with this exact normalized title.

    Gamma currently exposes tens of thousands of open markets, so walking the
    complete keyset for a few dozen Limitless mirrors is needlessly slow. The
    public search endpoint is used only to produce candidates; exact title,
    orderability, token, condition, and orderbook identity checks still happen
    before any route is evaluated.
    """

    normalized_title = normalize_title(title)
    if not normalized_title:
        raise ShadowError("Polymarket search title is empty")
    markets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in range(1, MAX_POLYMARKET_SEARCH_PAGES + 1):
        payload = get_json(
            f"{POLYMARKET_GAMMA_API}/public-search",
            params={
                "q": title,
                "events_status": "active",
                "limit_per_type": POLYMARKET_SEARCH_PAGE_SIZE,
                "page": page,
                "keep_closed_markets": 0,
                "cache": "false",
                "search_tags": "false",
                "search_profiles": "false",
            },
            timeout=timeout,
        )
        events = payload.get("events") if isinstance(payload, dict) else None
        pagination = payload.get("pagination") if isinstance(payload, dict) else None
        if not isinstance(pagination, dict):
            raise ShadowError("Polymarket search response schema invalid")
        has_more = pagination.get("hasMore")
        if not isinstance(has_more, bool):
            raise ShadowError("Polymarket search pagination missing hasMore")
        if events is None and pagination.get("totalResults") == 0 and not has_more:
            events = []
        if not isinstance(events, list):
            raise ShadowError("Polymarket search response schema invalid")
        for event in events:
            if not isinstance(event, dict):
                raise ShadowError("Polymarket search event is not an object")
            rows = event.get("markets")
            if not isinstance(rows, list):
                raise ShadowError("Polymarket search event missing markets array")
            for row in rows:
                if not isinstance(row, dict):
                    raise ShadowError("Polymarket search market is not an object")
                slug = str(row.get("slug", "")).strip()
                question = str(row.get("question", "")).strip()
                if (
                    slug
                    and normalize_title(question) == normalized_title
                    and slug not in seen
                ):
                    seen.add(slug)
                    markets.append(row)
        if not has_more:
            return markets
        if not events:
            raise ShadowError("Polymarket search hasMore page contained no events")
        if page == MAX_POLYMARKET_SEARCH_PAGES:
            raise ShadowError("Polymarket search hit page safety cap before completion")
    return markets


def discover_polymarket_markets(
    limitless: Iterable[dict[str, Any]], timeout: float, concurrency: int
) -> list[dict[str, Any]]:
    titles = sorted(
        {
            str(market.get("title", "")).strip()
            for market in limitless
            if str(market.get("title", "")).strip()
        }
    )
    markets: list[dict[str, Any]] = []
    seen: set[str] = set()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(concurrency, 8))
    ) as executor:
        for rows in executor.map(
            lambda title: search_polymarket_title(title, timeout), titles
        ):
            for row in rows:
                slug = str(row.get("slug", "")).strip()
                if slug and slug not in seen:
                    seen.add(slug)
                    markets.append(row)
    return markets


def discover_pairs(
    limitless: Iterable[dict[str, Any]],
    polymarket: Iterable[dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    by_title: dict[str, list[dict[str, Any]]] = {}
    for market in polymarket:
        key = normalize_title(str(market.get("question", "")))
        if key:
            by_title.setdefault(key, []).append(market)
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for market in limitless:
        matches = by_title.get(normalize_title(str(market.get("title", ""))), [])
        # Ambiguous titles fail closed rather than guessing a contract.
        if len(matches) == 1:
            pairs.append((market, matches[0]))
    return pairs


def parse_limitless_levels(
    orderbook: dict[str, Any], side: str
) -> list[tuple[float, float]]:
    key = "asks" if side == "yes" else "bids"
    rows = orderbook.get(key)
    if not isinstance(rows, list) or not rows:
        raise ShadowError(f"Limitless {key} missing")
    levels: list[tuple[float, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ShadowError(f"Limitless {key} level is not an object")
        raw_price = finite_number(row.get("price"), f"Limitless {key} price")
        raw_size = finite_number(row.get("size"), f"Limitless {key} size")
        price = raw_price if side == "yes" else 1.0 - raw_price
        shares = raw_size / 1_000_000.0
        if not 0.0 < price < 1.0 or shares <= 0.0:
            raise ShadowError(f"invalid Limitless {side} level")
        levels.append((price, shares))
    return aggregate_levels(levels)


def parse_polymarket_asks(orderbook: dict[str, Any]) -> list[tuple[float, float]]:
    rows = orderbook.get("asks")
    if not isinstance(rows, list) or not rows:
        raise ShadowError("Polymarket asks missing")
    levels: list[tuple[float, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ShadowError("Polymarket ask level is not an object")
        price = finite_number(row.get("price"), "Polymarket ask price")
        shares = finite_number(row.get("size"), "Polymarket ask size")
        if not 0.0 < price < 1.0 or shares <= 0.0:
            raise ShadowError("invalid Polymarket ask level")
        levels.append((price, shares))
    return aggregate_levels(levels)


def aggregate_levels(levels: Iterable[tuple[float, float]]) -> list[tuple[float, float]]:
    totals: dict[float, float] = {}
    for price, shares in levels:
        totals[price] = totals.get(price, 0.0) + shares
    return sorted(totals.items())


def walk_asks(levels: list[tuple[float, float]], shares: float) -> dict[str, Any]:
    if not math.isfinite(shares) or shares <= 0.0:
        raise ShadowError("requested shares must be positive and finite")
    remaining = shares
    cost = 0.0
    worst = 0.0
    fills: list[dict[str, float]] = []
    for price, available in levels:
        take = min(remaining, available)
        cost += take * price
        fills.append({"price": price, "shares": take, "cost_usd": take * price})
        remaining -= take
        worst = price
        if remaining <= 1e-9:
            return {
                "shares": shares,
                "cost_usd": cost,
                "vwap": cost / shares,
                "worst_price": worst,
                "fills": fills,
            }
    raise ShadowError(f"insufficient ask depth for {shares:g} shares")


def polymarket_fee(
    fills: list[dict[str, float]],
    market_info: dict[str, Any],
    fees_enabled: bool | None,
) -> tuple[float, dict[str, Any]]:
    fee = market_info.get("fd")
    if not isinstance(fee, dict):
        if fees_enabled is False:
            return 0.0, {"enabled": False}
        raise ShadowError("Polymarket compact market metadata missing fd")
    rate = finite_number(fee.get("r"), "Polymarket fd.r")
    exponent_number = finite_number(fee.get("e"), "Polymarket fd.e")
    if not exponent_number.is_integer():
        raise ShadowError("Polymarket fee exponent is not an integer")
    exponent = int(exponent_number)
    if not 0.0 <= rate <= 1.0 or not 1 <= exponent <= 4:
        raise ShadowError("unsupported Polymarket fee schedule")
    amount = 0.0
    for fill in fills:
        price = finite_number(fill.get("price"), "Polymarket fill price")
        shares = finite_number(fill.get("shares"), "Polymarket fill shares")
        decimal_price = Decimal(str(price))
        raw_fee = (
            Decimal(str(shares))
            * Decimal(str(rate))
            * (decimal_price * (Decimal(1) - decimal_price)) ** exponent
        )
        rounded = float(
            raw_fee.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
        )
        amount += rounded if rounded >= 0.00001 else 0.0
    return amount, {"rate": rate, "exponent": exponent, "rounding_decimals": 5}


def pair_certificate_status(
    certificate: dict[str, Any] | None,
    limitless_slug: str,
    polymarket_slug: str,
    limitless_hash: str,
    polymarket_hash: str,
) -> str:
    if not certificate:
        return "missing"
    pairs = certificate.get("pairs")
    if certificate.get("schema_version") != SCHEMA_VERSION or not isinstance(pairs, list):
        return "invalid_certificate"
    matches = [
        pair
        for pair in pairs
        if isinstance(pair, dict)
        and pair.get("limitless_slug") == limitless_slug
        and pair.get("polymarket_slug") == polymarket_slug
    ]
    if len(matches) != 1:
        return "missing" if not matches else "ambiguous"
    pair = matches[0]
    if pair.get("reviewed") is not True:
        return "unreviewed"
    if (
        pair.get("limitless_rules_sha256") != limitless_hash
        or pair.get("polymarket_rules_sha256") != polymarket_hash
    ):
        return "rules_drift"
    return "certified"


def compact_market_info(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ShadowError("Polymarket compact market response is not an object")
    return payload


def validate_pair_identity(
    *,
    limitless_discovery: dict[str, Any],
    limitless_detail: dict[str, Any],
    limitless_book: dict[str, Any],
    polymarket_market: dict[str, Any],
    poly_yes_book: dict[str, Any],
    poly_no_book: dict[str, Any],
    market_info: dict[str, Any],
    tokens: list[Any],
    condition_id: str,
) -> None:
    if not is_limitless_mirror(limitless_detail):
        raise ShadowError("Limitless detail is no longer an active explicit mirror")
    for field in ("id", "slug", "title", "conditionId"):
        if limitless_detail.get(field) != limitless_discovery.get(field):
            raise ShadowError(f"Limitless discovery/detail {field} mismatch")
    discovery_tokens = limitless_discovery.get("tokens")
    detail_tokens = limitless_detail.get("tokens")
    if discovery_tokens != detail_tokens or not isinstance(detail_tokens, dict):
        raise ShadowError("Limitless discovery/detail token mapping mismatch")
    if str(limitless_book.get("tokenId", "")) != str(detail_tokens.get("yes", "")):
        raise ShadowError("Limitless orderbook tokenId does not match YES token")
    if (
        polymarket_market.get("active") is not True
        or polymarket_market.get("closed") is not False
        or polymarket_market.get("acceptingOrders") is not True
    ):
        raise ShadowError("Polymarket market is not active and orderable")
    for book, token, outcome in (
        (poly_yes_book, tokens[0], "YES"),
        (poly_no_book, tokens[1], "NO"),
    ):
        if str(book.get("asset_id", "")) != str(token):
            raise ShadowError(f"Polymarket {outcome} book asset_id mismatch")
        if str(book.get("market", "")) != condition_id:
            raise ShadowError(f"Polymarket {outcome} book condition mismatch")
    if market_info.get("ao") is not True or str(market_info.get("c", "")) != condition_id:
        raise ShadowError("Polymarket compact market is not orderable or condition-bound")
    compact_tokens = market_info.get("t")
    if not isinstance(compact_tokens, list) or len(compact_tokens) != 2:
        raise ShadowError("Polymarket compact token mapping missing")
    expected = [(str(tokens[0]), "yes"), (str(tokens[1]), "no")]
    actual = [
        (str(item.get("t", "")), str(item.get("o", "")).casefold())
        for item in compact_tokens
        if isinstance(item, dict)
    ]
    if actual != expected:
        raise ShadowError("Polymarket compact token/outcome mapping mismatch")


def evaluate_route(
    name: str,
    first_levels: list[tuple[float, float]],
    second_levels: list[tuple[float, float]],
    first_venue: str,
    *,
    shares: float,
    market_info: dict[str, Any],
    gas_and_transfer_buffer_usd: float,
    certificate_status: str,
    polymarket_fees_enabled: bool | None,
) -> dict[str, Any]:
    first = walk_asks(first_levels, shares)
    second = walk_asks(second_levels, shares)
    if first_venue == "polymarket":
        poly_leg, limitless_leg = first, second
    elif first_venue == "limitless":
        limitless_leg, poly_leg = first, second
    else:
        raise ShadowError(f"unknown first venue {first_venue}")
    poly_fee, fee_schedule = polymarket_fee(
        poly_leg["fills"], market_info, polymarket_fees_enabled
    )
    # Limitless documents a 0.40%-3.00% CLOB buy-fee range paid in
    # outcome tokens. Value every possible fee token at full $1 payout for
    # a conservative upper bound until exact signed-order terms exist.
    limitless_fee_bound = shares * MAX_LIMITLESS_BUY_FEE_RATE
    total_cost = first["cost_usd"] + second["cost_usd"]
    gross_profit = shares - total_cost
    net_profit_bound = (
        gross_profit
        - poly_fee
        - limitless_fee_bound
        - gas_and_transfer_buffer_usd
    )
    capital = total_cost + poly_fee + gas_and_transfer_buffer_usd
    blockers = [
        "shadow_only_no_submit",
        "non_atomic_cross_chain_execution_not_implemented",
        "limitless_exact_fee_terms_unverified",
    ]
    if certificate_status != "certified":
        blockers.append(f"rules_certificate_{certificate_status}")
    if net_profit_bound <= 0.0:
        blockers.append("non_positive_conservative_shadow_profit")
    return {
        "route": name,
        "shares": shares,
        "first_leg": first,
        "second_leg": second,
        "limitless_leg": limitless_leg,
        "polymarket_leg": poly_leg,
        "gross_profit_usd": gross_profit,
        "gross_roi_pct": (gross_profit / total_cost * 100.0)
        if total_cost > 0.0
        else None,
        "polymarket_fee_usd": poly_fee,
        "polymarket_fee_schedule": fee_schedule,
        "limitless_max_fee_value_usd": limitless_fee_bound,
        "gas_and_transfer_buffer_usd": gas_and_transfer_buffer_usd,
        "conservative_shadow_profit_usd": net_profit_bound,
        "conservative_shadow_roi_pct": (net_profit_bound / capital * 100.0)
        if capital > 0.0
        else None,
        "blockers": blockers,
        "actionable": False,
    }


def evaluate_pair(
    pair: tuple[dict[str, Any], dict[str, Any]],
    *,
    requested_shares: float,
    gas_and_transfer_buffer_usd: float,
    timeout: float,
    certificate: dict[str, Any] | None,
) -> dict[str, Any]:
    limitless_market, polymarket_market = pair
    limitless_slug = str(limitless_market["slug"])
    polymarket_slug = str(polymarket_market["slug"])
    condition_id = str(polymarket_market.get("conditionId", "")).strip()
    if not condition_id:
        raise ShadowError("Polymarket conditionId missing")
    tokens = parse_json_array(polymarket_market.get("clobTokenIds"), "clobTokenIds")
    outcomes = parse_json_array(polymarket_market.get("outcomes"), "outcomes")
    if len(tokens) != 2 or [str(item).casefold() for item in outcomes] != ["yes", "no"]:
        raise ShadowError("Polymarket binary YES/NO token mapping missing")

    limitless_detail = get_json(
        f"{LIMITLESS_API}/markets/{urllib.parse.quote(limitless_slug, safe='')}",
        timeout=timeout,
    )
    limitless_book = get_json(
        f"{LIMITLESS_API}/markets/{urllib.parse.quote(limitless_slug, safe='')}/orderbook",
        timeout=timeout,
    )
    poly_yes_book = get_json(
        f"{POLYMARKET_CLOB_API}/book",
        params={"token_id": str(tokens[0])},
        timeout=timeout,
    )
    poly_no_book = get_json(
        f"{POLYMARKET_CLOB_API}/book",
        params={"token_id": str(tokens[1])},
        timeout=timeout,
    )
    market_info = compact_market_info(
        get_json(
            f"{POLYMARKET_CLOB_API}/clob-markets/{urllib.parse.quote(condition_id, safe='')}",
            timeout=timeout,
        )
    )
    if not isinstance(limitless_detail, dict) or not isinstance(limitless_book, dict):
        raise ShadowError("Limitless detail/orderbook schema invalid")
    if not isinstance(poly_yes_book, dict) or not isinstance(poly_no_book, dict):
        raise ShadowError("Polymarket orderbook schema invalid")
    validate_pair_identity(
        limitless_discovery=limitless_market,
        limitless_detail=limitless_detail,
        limitless_book=limitless_book,
        polymarket_market=polymarket_market,
        poly_yes_book=poly_yes_book,
        poly_no_book=poly_no_book,
        market_info=market_info,
        tokens=tokens,
        condition_id=condition_id,
    )

    minimums = [requested_shares]
    limitless_minimum = (
        limitless_detail.get("settings", {}).get("minSize")
        if isinstance(limitless_detail.get("settings"), dict)
        else None
    )
    if limitless_minimum is not None:
        minimums.append(finite_number(limitless_minimum, "Limitless minSize") / 1e6)
    for book, label in ((poly_yes_book, "yes"), (poly_no_book, "no")):
        minimums.append(
            finite_number(book.get("min_order_size"), f"Polymarket {label} min order")
        )
    shares = max(minimums)

    limitless_yes = parse_limitless_levels(limitless_book, "yes")
    limitless_no = parse_limitless_levels(limitless_book, "no")
    poly_yes = parse_polymarket_asks(poly_yes_book)
    poly_no = parse_polymarket_asks(poly_no_book)

    limitless_hash = rules_fingerprint(
        str(limitless_detail.get("title", "")),
        str(limitless_detail.get("description", "")),
        limitless_detail.get("expirationTimestamp"),
        {
            "condition_id": limitless_detail.get("conditionId"),
            "tokens": limitless_detail.get("tokens"),
            "collateral_token": limitless_detail.get("collateralToken"),
            "venue": limitless_detail.get("venue"),
            "oracle_version": limitless_detail.get("metadata", {}).get("oracleVersion")
            if isinstance(limitless_detail.get("metadata"), dict)
            else None,
        },
    )
    polymarket_hash = rules_fingerprint(
        str(polymarket_market.get("question", "")),
        str(polymarket_market.get("description", "")),
        polymarket_market.get("endDate"),
        {
            "condition_id": condition_id,
            "tokens": [str(token) for token in tokens],
            "outcomes": outcomes,
            "neg_risk": polymarket_market.get("negRisk"),
            "resolution_source": polymarket_market.get("resolutionSource"),
        },
    )
    certificate_status = pair_certificate_status(
        certificate,
        limitless_slug,
        polymarket_slug,
        limitless_hash,
        polymarket_hash,
    )

    route_specs = [
        ("limitless_yes_polymarket_no", limitless_yes, poly_no, "limitless"),
        ("polymarket_yes_limitless_no", poly_yes, limitless_no, "polymarket"),
    ]
    routes: list[dict[str, Any]] = []
    for name, first_levels, second_levels, first_venue in route_specs:
        routes.append(
            evaluate_route(
                name,
                first_levels,
                second_levels,
                first_venue,
                shares=shares,
                market_info=market_info,
                gas_and_transfer_buffer_usd=gas_and_transfer_buffer_usd,
                certificate_status=certificate_status,
                polymarket_fees_enabled=polymarket_market.get("feesEnabled")
                if isinstance(polymarket_market.get("feesEnabled"), bool)
                else None,
            )
        )

    return {
        "pair_id": hashlib.sha256(
            f"{limitless_slug}\0{polymarket_slug}".encode()
        ).hexdigest(),
        "title": str(limitless_detail.get("title", limitless_market.get("title", ""))),
        "limitless_slug": limitless_slug,
        "polymarket_slug": polymarket_slug,
        "limitless_rules_sha256": limitless_hash,
        "polymarket_rules_sha256": polymarket_hash,
        "limitless_rules_preview": strip_html(
            str(limitless_detail.get("description", ""))
        )[:240],
        "polymarket_rules_preview": str(polymarket_market.get("description", ""))[:240],
        "rules_certificate_status": certificate_status,
        "routes": routes,
    }


def load_certificate(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ShadowError(f"reading pair certificate {path}: {error}") from error
    if not isinstance(value, dict):
        raise ShadowError("pair certificate root must be an object")
    return value


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pair-certificate", type=Path)
    parser.add_argument("--shares", type=float, default=100.0)
    parser.add_argument("--gas-transfer-buffer-usd", type=float, default=2.0)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--concurrency", type=int, default=8)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not math.isfinite(args.shares) or args.shares <= 0.0:
        raise ShadowError("--shares must be positive and finite")
    if (
        not math.isfinite(args.gas_transfer_buffer_usd)
        or args.gas_transfer_buffer_usd < 0.0
    ):
        raise ShadowError("--gas-transfer-buffer-usd must be finite and non-negative")
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0.0:
        raise ShadowError("--timeout-seconds must be positive and finite")
    concurrency = max(1, min(args.concurrency, 16))
    certificate = load_certificate(args.pair_certificate)
    limitless = discover_limitless_markets(args.timeout_seconds)
    polymarket = discover_polymarket_markets(
        limitless, args.timeout_seconds, concurrency
    )
    pairs = discover_pairs(limitless, polymarket)
    evaluated: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(
                evaluate_pair,
                pair,
                requested_shares=args.shares,
                gas_and_transfer_buffer_usd=args.gas_transfer_buffer_usd,
                timeout=args.timeout_seconds,
                certificate=certificate,
            ): pair
            for pair in pairs
        }
        for future in concurrent.futures.as_completed(futures):
            pair = futures[future]
            try:
                evaluated.append(future.result())
            except Exception as error:  # keep a complete fail-closed report
                errors.append(
                    {
                        "limitless_slug": str(pair[0].get("slug", "")),
                        "polymarket_slug": str(pair[1].get("slug", "")),
                        "error": str(error),
                    }
                )
    evaluated.sort(key=lambda item: item["pair_id"])
    errors.sort(key=lambda item: (item["limitless_slug"], item["polymarket_slug"]))
    positive_routes = [
        route
        for pair in evaluated
        for route in pair["routes"]
        if route["conservative_shadow_profit_usd"] > 0.0
    ]
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "public_read_only_no_submit",
        "complete": not errors,
        "profitability_evidence_eligible": False,
        "live_submission_possible": False,
        "requested_shares": args.shares,
        "gas_and_transfer_buffer_usd": args.gas_transfer_buffer_usd,
        "sources": {
            "limitless_api": LIMITLESS_API,
            "polymarket_gamma_api": POLYMARKET_GAMMA_API,
            "polymarket_clob_api": POLYMARKET_CLOB_API,
        },
        "counts": {
            "limitless_explicit_mirrors": len(limitless),
            "polymarket_exact_title_candidates": len(polymarket),
            "unique_exact_title_pairs": len(pairs),
            "evaluated_pairs": len(evaluated),
            "evaluation_errors": len(errors),
            "positive_conservative_shadow_routes": len(positive_routes),
            "certified_pairs": sum(
                pair["rules_certificate_status"] == "certified" for pair in evaluated
            ),
        },
        "global_blockers": [
            "shadow_only_no_submit",
            "cross_venue_results_excluded_from_paper_profitability_gate",
            "non_atomic_cross_chain_execution_not_implemented",
        ],
        "pairs": evaluated,
        "errors": errors,
    }
    atomic_write_json(args.output, report)
    print(
        "cross_venue_shadow_complete={} pairs={} positive_conservative_routes={} errors={} output={}".format(
            str(report["complete"]).lower(),
            len(evaluated),
            len(positive_routes),
            len(errors),
            args.output,
        )
    )
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ShadowError as error:
        print(f"cross-venue shadow failed: {error}", file=sys.stderr)
        raise SystemExit(2) from error
