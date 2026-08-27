import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "cross_venue_shadow.py"
SPEC = importlib.util.spec_from_file_location("cross_venue_shadow", MODULE_PATH)
assert SPEC and SPEC.loader
SHADOW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SHADOW)


class CrossVenueShadowTests(unittest.TestCase):
    def test_polymarket_search_requires_terminal_page_and_exact_title(self):
        pages = [
            {
                "events": [
                    {
                        "markets": [
                            {"slug": "one", "question": "One?"},
                            {"slug": "near", "question": "One more?"},
                        ]
                    }
                ],
                "pagination": {"hasMore": True, "totalResults": 2},
            },
            {
                "events": [
                    {"markets": [{"slug": "duplicate", "question": "ONE!"}]}
                ],
                "pagination": {"hasMore": False, "totalResults": 2},
            },
        ]
        with mock.patch.object(SHADOW, "get_json", side_effect=pages) as request:
            markets = SHADOW.search_polymarket_title("One?", 1.0)
        self.assertEqual(
            [market["slug"] for market in markets], ["one", "duplicate"]
        )
        self.assertEqual(request.call_args_list[0].kwargs["params"]["page"], 1)
        self.assertEqual(request.call_args_list[1].kwargs["params"]["page"], 2)

    def test_polymarket_search_fails_closed_at_page_cap(self):
        page = {
            "events": [{"markets": []}],
            "pagination": {"hasMore": True, "totalResults": 1000},
        }
        with mock.patch.object(SHADOW, "get_json", return_value=page):
            with self.assertRaisesRegex(SHADOW.ShadowError, "no events|safety cap"):
                SHADOW.search_polymarket_title("One?", 1.0)

    def test_polymarket_search_accepts_canonical_empty_result(self):
        payload = {
            "pagination": {"hasMore": False, "totalResults": 0},
        }
        with mock.patch.object(SHADOW, "get_json", return_value=payload):
            self.assertEqual(SHADOW.search_polymarket_title("No match?", 1.0), [])

    def test_title_discovery_is_exact_normalized_and_ambiguous_titles_fail_closed(self):
        limitless = [
            {
                "slug": "mirror",
                "title": "US national Ethereum reserve before 2027?",
            }
        ]
        polymarket = [
            {
                "slug": "poly",
                "question": "US National Ethereum Reserve Before 2027",
            }
        ]
        self.assertEqual(len(SHADOW.discover_pairs(limitless, polymarket)), 1)
        polymarket.append(
            {
                "slug": "duplicate",
                "question": "US national ethereum reserve before 2027!",
            }
        )
        self.assertEqual(SHADOW.discover_pairs(limitless, polymarket), [])

    def test_limitless_depth_maps_yes_asks_and_complementary_no_asks(self):
        orderbook = {
            "asks": [
                {"price": 0.47, "size": 10_000_000},
                {"price": 0.47, "size": 2_000_000},
                {"price": 0.50, "size": 100_000_000},
            ],
            "bids": [
                {"price": 0.44, "size": 20_000_000},
                {"price": 0.44, "size": 5_000_000},
                {"price": 0.40, "size": 100_000_000},
            ],
        }
        self.assertEqual(
            SHADOW.parse_limitless_levels(orderbook, "yes"),
            [(0.47, 12.0), (0.5, 100.0)],
        )
        no_levels = SHADOW.parse_limitless_levels(orderbook, "no")
        self.assertAlmostEqual(no_levels[0][0], 0.56)
        self.assertEqual(no_levels[0][1], 25.0)
        self.assertAlmostEqual(no_levels[1][0], 0.60)
        self.assertEqual(no_levels[1][1], 100.0)

    def test_depth_walk_uses_vwap_and_refuses_shortfall(self):
        fill = SHADOW.walk_asks([(0.2, 5.0), (0.3, 10.0)], 10.0)
        self.assertAlmostEqual(fill["cost_usd"], 2.5)
        self.assertAlmostEqual(fill["vwap"], 0.25)
        self.assertEqual(fill["worst_price"], 0.3)
        self.assertEqual(
            fill["fills"],
            [
                {"price": 0.2, "shares": 5.0, "cost_usd": 1.0},
                {"price": 0.3, "shares": 5.0, "cost_usd": 1.5},
            ],
        )
        with self.assertRaisesRegex(SHADOW.ShadowError, "insufficient ask depth"):
            SHADOW.walk_asks([(0.2, 5.0)], 10.0)

    def test_route_economics_apply_verified_poly_curve_and_max_limitless_fee(self):
        route = SHADOW.evaluate_route(
            "polymarket_yes_limitless_no",
            [(0.226, 1_735.0)],
            [(0.7, 110.0)],
            "polymarket",
            shares=100.0,
            market_info={"fd": {"r": 0.07, "e": 1}},
            gas_and_transfer_buffer_usd=1.0,
            certificate_status="missing",
            polymarket_fees_enabled=True,
        )
        self.assertAlmostEqual(route["gross_profit_usd"], 7.4)
        self.assertAlmostEqual(route["polymarket_fee_usd"], 1.22447)
        self.assertAlmostEqual(route["limitless_max_fee_value_usd"], 3.0)
        self.assertAlmostEqual(route["conservative_shadow_profit_usd"], 2.17553)
        self.assertFalse(route["actionable"])
        self.assertIn("rules_certificate_missing", route["blockers"])
        self.assertIn("shadow_only_no_submit", route["blockers"])

    def test_polymarket_fee_is_summed_and_rounded_per_depth_fill(self):
        fills = [
            {"price": 0.01, "shares": 50.0},
            {"price": 0.10, "shares": 50.0},
        ]
        fee, schedule = SHADOW.polymarket_fee(
            fills, {"fd": {"r": 0.07, "e": 2}}, True
        )
        self.assertAlmostEqual(fee, 0.02869)
        self.assertEqual(schedule["rounding_decimals"], 5)

        half_tick_fee, _ = SHADOW.polymarket_fee(
            [{"price": 0.5, "shares": 0.00002}],
            {"fd": {"r": 1.0, "e": 1}},
            True,
        )
        self.assertEqual(half_tick_fee, 0.00001)

        with self.assertRaisesRegex(SHADOW.ShadowError, "not an integer"):
            SHADOW.polymarket_fee(
                [{"price": 0.5, "shares": 1.0}],
                {"fd": {"r": 0.07, "e": 1.5}},
                True,
            )

    def test_pair_certificate_is_exact_hash_bound_and_reviewed(self):
        certificate = {
            "schema_version": 1,
            "pairs": [
                {
                    "limitless_slug": "limitless",
                    "polymarket_slug": "poly",
                    "limitless_rules_sha256": "a" * 64,
                    "polymarket_rules_sha256": "b" * 64,
                    "reviewed": True,
                }
            ],
        }
        self.assertEqual(
            SHADOW.pair_certificate_status(
                certificate, "limitless", "poly", "a" * 64, "b" * 64
            ),
            "certified",
        )
        self.assertEqual(
            SHADOW.pair_certificate_status(
                certificate, "limitless", "poly", "c" * 64, "b" * 64
            ),
            "rules_drift",
        )
        certificate["pairs"][0]["reviewed"] = False
        self.assertEqual(
            SHADOW.pair_certificate_status(
                certificate, "limitless", "poly", "a" * 64, "b" * 64
            ),
            "unreviewed",
        )

    def test_pair_identity_requires_exact_tokens_conditions_and_orderability(self):
        limitless = {
            "slug": "mirror",
            "title": "Mirror?",
            "status": "FUNDED",
            "expired": False,
            "marketType": "single",
            "tradeType": "clob",
            "tokens": {"yes": "ly", "no": "ln"},
            "metadata": {"isPolyArbitrage": True},
        }
        polymarket = {
            "active": True,
            "closed": False,
            "acceptingOrders": True,
        }
        yes_book = {"asset_id": "py", "market": "condition"}
        no_book = {"asset_id": "pn", "market": "condition"}
        compact = {
            "ao": True,
            "c": "condition",
            "t": [{"t": "py", "o": "Yes"}, {"t": "pn", "o": "No"}],
        }
        SHADOW.validate_pair_identity(
            limitless_discovery=limitless,
            limitless_detail=dict(limitless),
            limitless_book={"tokenId": "ly"},
            polymarket_market=polymarket,
            poly_yes_book=yes_book,
            poly_no_book=no_book,
            market_info=compact,
            tokens=["py", "pn"],
            condition_id="condition",
        )

        paused = dict(compact)
        paused["ao"] = False
        with self.assertRaisesRegex(SHADOW.ShadowError, "not orderable"):
            SHADOW.validate_pair_identity(
                limitless_discovery=limitless,
                limitless_detail=dict(limitless),
                limitless_book={"tokenId": "ly"},
                polymarket_market=polymarket,
                poly_yes_book=yes_book,
                poly_no_book=no_book,
                market_info=paused,
                tokens=["py", "pn"],
                condition_id="condition",
            )
        with self.assertRaisesRegex(SHADOW.ShadowError, "asset_id mismatch"):
            SHADOW.validate_pair_identity(
                limitless_discovery=limitless,
                limitless_detail=dict(limitless),
                limitless_book={"tokenId": "ly"},
                polymarket_market=polymarket,
                poly_yes_book={"asset_id": "wrong", "market": "condition"},
                poly_no_book=no_book,
                market_info=compact,
                tokens=["py", "pn"],
                condition_id="condition",
            )

        changed_title = dict(limitless)
        changed_title["title"] = "Different mirror?"
        with self.assertRaisesRegex(SHADOW.ShadowError, "title mismatch"):
            SHADOW.validate_pair_identity(
                limitless_discovery=limitless,
                limitless_detail=changed_title,
                limitless_book={"tokenId": "ly"},
                polymarket_market=polymarket,
                poly_yes_book=yes_book,
                poly_no_book=no_book,
                market_info=compact,
                tokens=["py", "pn"],
                condition_id="condition",
            )

    def test_atomic_report_write_is_parseable(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "report.json"
            SHADOW.atomic_write_json(output, {"complete": True})
            self.assertEqual(json.loads(output.read_text()), {"complete": True})


if __name__ == "__main__":
    unittest.main()
