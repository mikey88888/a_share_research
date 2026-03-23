from __future__ import annotations

import os
import unittest

from fastapi.testclient import TestClient

from a_share_research.domain.market import AssetType
from a_share_research.services.market_catalog import resolve_asset
from a_share_research.services.market_views import build_stock_list_context
from a_share_research.web.app import create_app


@unittest.skipUnless(os.getenv("A_SHARE_PG_DSN"), "A_SHARE_PG_DSN is required for dashboard integration tests")
class MarketDashboardIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(create_app())

    def test_stock_search_hits_symbol_and_name(self) -> None:
        by_symbol = build_stock_list_context(q="000001", page=1, page_size=10)
        self.assertGreater(by_symbol["total"], 0)
        self.assertEqual(by_symbol["items"][0]["symbol"], "000001")

        by_name = build_stock_list_context(q="平安", page=1, page_size=10)
        symbols = [item["symbol"] for item in by_name["items"]]
        self.assertIn("000001", symbols)

    def test_stock_filter_and_membership_label(self) -> None:
        context = build_stock_list_context(exchange="SZ", sort="latest_60m", page=1, page_size=20)
        self.assertGreater(len(context["items"]), 0)
        self.assertTrue(all(item["exchange"] == "SZ" for item in context["items"]))
        self.assertTrue(all("membership_label" in item for item in context["items"]))

    def test_resolve_asset_distinguishes_index_and_stock(self) -> None:
        index_asset = resolve_asset("000300")
        stock_asset = resolve_asset("000001")
        self.assertEqual(index_asset.asset_type, AssetType.INDEX)
        self.assertEqual(stock_asset.asset_type, AssetType.STOCK)

    def test_market_pages_render(self) -> None:
        for path in ["/", "/markets", "/markets/indexes", "/markets/stocks", "/markets/indexes/000300", "/markets/stocks/000001"]:
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200, path)

        stocks_page = self.client.get("/markets/stocks")
        self.assertIn("个股列表与检索", stocks_page.text)
        self.assertIn("000001", stocks_page.text)

    def test_stock_list_htmx_partial_renders(self) -> None:
        response = self.client.get("/markets/stocks?q=000001", headers={"HX-Request": "true"})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("<html", response.text.lower())
        self.assertIn("000001", response.text)

    def test_legacy_instrument_route_redirects(self) -> None:
        response = self.client.get("/instruments/000001?timeframe=60m&range=3m", follow_redirects=False)
        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers["location"], "/markets/stocks/000001?timeframe=60m&range=3m")

    def test_api_search_and_bars(self) -> None:
        response = self.client.get("/api/stocks/search?q=000001")
        self.assertEqual(response.status_code, 200)
        items = response.json()
        self.assertTrue(any(item["symbol"] == "000001" for item in items))

        bars = self.client.get("/api/bars?symbol=000001&asset_type=stock&timeframe=1d&limit=5")
        self.assertEqual(bars.status_code, 200)
        payload = bars.json()
        self.assertEqual(len(payload), 5)
        self.assertIn("close", payload[0])

    def test_invalid_symbol_returns_not_found(self) -> None:
        response = self.client.get("/markets/stocks/not-a-symbol")
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
