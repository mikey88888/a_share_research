from __future__ import annotations

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from a_share_research.domain.market import normalize_exchange_filter
from a_share_research import sync_stock_data, webapp
from a_share_research.stock_data import IntradaySourceBlockedError
from a_share_research.web import app as web_app


class DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyDate:
    @classmethod
    def today(cls):
        return pd.Timestamp("2026-03-23").date()


class SyncStockDataTests(unittest.TestCase):
    def test_helper_functions(self) -> None:
        self.assertEqual(list(sync_stock_data._chunked(["a", "b", "c"], 2)), [["a", "b"], ["c"]])
        self.assertEqual(sync_stock_data._normalize_symbol_args(["000001,600000", "300750"]), ["000001", "600000", "300750"])
        self.assertEqual(normalize_exchange_filter("sz"), "SZ")
        self.assertEqual(normalize_exchange_filter("weird"), "ALL")

        complete_row = SimpleNamespace(
            intraday_rows=1970,
            earliest_bar_time=pd.Timestamp("2024-03-08 10:30:00"),
            latest_bar_time=pd.Timestamp("2026-03-23 15:00:00"),
            first_trade_date=pd.Timestamp("2024-03-08").date(),
        )
        self.assertTrue(
            sync_stock_data._intraday_is_complete(
                complete_row,
                min_rows=1970,
                min_latest_date=pd.Timestamp("2026-03-18").date(),
            )
        )

        new_stock_row = SimpleNamespace(
            intraday_rows=500,
            earliest_bar_time=pd.Timestamp("2025-09-01 10:30:00"),
            latest_bar_time=pd.Timestamp("2026-03-23 15:00:00"),
            first_trade_date=pd.Timestamp("2025-09-01").date(),
        )
        self.assertTrue(
            sync_stock_data._intraday_is_complete(
                new_stock_row,
                min_rows=1970,
                min_latest_date=pd.Timestamp("2026-03-18").date(),
            )
        )

    def test_sync_stock_universe_and_gap_selection(self) -> None:
        constituents = pd.DataFrame(
            [
                {"index_symbol": "000300", "stock_symbol": "000001"},
                {"index_symbol": "000905", "stock_symbol": "600000"},
            ]
        )
        stocks = pd.DataFrame(
            [
                {"symbol": "000001", "exchange": "SZ", "vendor_symbol": "sz000001", "name": "平安银行", "timezone": "Asia/Shanghai", "source": "demo"},
                {"symbol": "600000", "exchange": "SH", "vendor_symbol": "sh600000", "name": "浦发银行", "timezone": "Asia/Shanghai", "source": "demo"},
            ]
        )
        conn = DummyConn()
        with patch("a_share_research.sync_stock_data.fetch_index_constituents", side_effect=[constituents.iloc[[0]], constituents.iloc[[1]]]), patch(
            "a_share_research.sync_stock_data.build_stock_master",
            return_value=stocks,
        ) as build_master, patch("a_share_research.sync_stock_data.upsert_stocks") as upsert_stocks, patch(
            "a_share_research.sync_stock_data.replace_index_constituents_current"
        ) as replace_constituents:
            result = sync_stock_data.sync_stock_universe(conn)
        self.assertEqual(len(result), 2)
        self.assertTrue(build_master.called)
        self.assertTrue(upsert_stocks.called)
        self.assertTrue(replace_constituents.called)

        status = pd.DataFrame(
            [
                {
                    "symbol": "000001",
                    "intraday_rows": 1970,
                    "earliest_bar_time": pd.Timestamp("2024-03-08 10:30:00"),
                    "latest_bar_time": pd.Timestamp("2026-03-23 15:00:00"),
                    "intraday_fetched_at": None,
                    "first_trade_date": pd.Timestamp("2024-03-08").date(),
                },
                {
                    "symbol": "600000",
                    "intraday_rows": 0,
                    "earliest_bar_time": None,
                    "latest_bar_time": None,
                    "intraday_fetched_at": None,
                    "first_trade_date": pd.Timestamp("2024-03-08").date(),
                },
            ]
        )
        with patch("a_share_research.sync_stock_data.load_stock_bar_60m_status", return_value=status):
            missing, loaded = sync_stock_data._select_intraday_gap_symbols(
                conn,
                ["000001", "600000"],
                min_rows=1970,
                min_latest_date=pd.Timestamp("2026-03-18").date(),
            )
        self.assertEqual(missing, ["600000"])
        self.assertEqual(len(loaded), 2)

    def test_sync_batches_handle_success_failures_and_blocked(self) -> None:
        conn = DummyConn()
        with patch("a_share_research.sync_stock_data.default_daily_end", return_value="20260323"), patch(
            "a_share_research.sync_stock_data.default_daily_start",
            return_value="20200101",
        ), patch(
            "a_share_research.sync_stock_data.fetch_stock_daily",
            return_value=pd.DataFrame([{"symbol": "000001"}]),
        ), patch(
            "a_share_research.sync_stock_data.upsert_stock_bar_1d",
            return_value=3,
        ):
            rows, failures = sync_stock_data._sync_daily_batch(conn, ["000001"], mode="init", today=DummyDate.today())
        self.assertEqual(rows, 3)
        self.assertEqual(failures, [])

        with patch("a_share_research.sync_stock_data.default_daily_end", return_value="20260323"), patch(
            "a_share_research.sync_stock_data.next_daily_start",
            return_value="20260324",
        ), patch(
            "a_share_research.sync_stock_data.get_latest_stock_trade_date",
            return_value=pd.Timestamp("2026-03-20").date(),
        ):
            rows, failures = sync_stock_data._sync_daily_batch(conn, ["000001"], mode="refresh", today=DummyDate.today())
        self.assertEqual(rows, 0)
        self.assertEqual(failures, [])

        with patch("a_share_research.sync_stock_data.fetch_stock_60m", side_effect=[pd.DataFrame([{"symbol": "000001"}]), RuntimeError("boom")]), patch(
            "a_share_research.sync_stock_data.upsert_stock_bar_60m",
            return_value=5,
        ):
            rows, failures = sync_stock_data._sync_intraday_batch(conn, ["000001", "600000"])
        self.assertEqual(rows, 5)
        self.assertEqual(failures, ["600000"])

        with patch("a_share_research.sync_stock_data.fetch_stock_60m", side_effect=IntradaySourceBlockedError("blocked")):
            with self.assertRaises(IntradaySourceBlockedError):
                sync_stock_data._sync_intraday_batch(conn, ["000001"])

    def test_sync_stock_data_full_flow_and_unknown_symbol(self) -> None:
        conn = DummyConn()
        universe = pd.DataFrame([{"stock_symbol": "000001"}, {"stock_symbol": "600000"}])
        status_all = pd.DataFrame(
            [
                {
                    "symbol": "000001",
                    "intraday_rows": 1970,
                    "earliest_bar_time": pd.Timestamp("2024-03-08 10:30:00"),
                    "latest_bar_time": pd.Timestamp("2026-03-23 15:00:00"),
                    "intraday_fetched_at": None,
                    "first_trade_date": pd.Timestamp("2024-03-08").date(),
                },
                {
                    "symbol": "600000",
                    "intraday_rows": 1970,
                    "earliest_bar_time": pd.Timestamp("2024-03-08 10:30:00"),
                    "latest_bar_time": pd.Timestamp("2026-03-23 15:00:00"),
                    "intraday_fetched_at": None,
                    "first_trade_date": pd.Timestamp("2024-03-08").date(),
                },
            ]
        )
        with patch("a_share_research.sync_stock_data.date", DummyDate), patch(
            "a_share_research.sync_stock_data.connect_pg",
            return_value=conn,
        ), patch("a_share_research.sync_stock_data.ensure_schema"), patch(
            "a_share_research.sync_stock_data.sync_instruments"
        ), patch(
            "a_share_research.sync_stock_data.sync_stock_universe",
            return_value=universe,
        ), patch(
            "a_share_research.sync_stock_data._sync_daily_batch",
            return_value=(4, []),
        ) as sync_daily, patch(
            "a_share_research.sync_stock_data._sync_intraday_batch",
            return_value=(8, []),
        ) as sync_intraday, patch(
            "a_share_research.sync_stock_data._select_intraday_gap_symbols",
            side_effect=[(["600000"], status_all), ([], status_all), ([], status_all)],
        ), patch("a_share_research.sync_stock_data.time.sleep"):
            sync_stock_data.sync_stock_data("refresh", intraday_retry_rounds=2)
        self.assertTrue(sync_daily.called)
        self.assertTrue(sync_intraday.called)

        with patch("a_share_research.sync_stock_data.date", DummyDate), patch(
            "a_share_research.sync_stock_data.connect_pg",
            return_value=conn,
        ), patch("a_share_research.sync_stock_data.ensure_schema"), patch(
            "a_share_research.sync_stock_data.sync_instruments"
        ), patch(
            "a_share_research.sync_stock_data.load_current_stock_symbols",
            return_value=["000001"],
        ):
            with self.assertRaisesRegex(ValueError, "symbols not in current index universe"):
                sync_stock_data.sync_stock_data("refresh", symbols=["600000"], skip_universe=True)

    def test_sync_stock_data_blocked_short_circuit_and_cli_wrappers(self) -> None:
        conn = DummyConn()
        blocked_status = pd.DataFrame(
            [
                {
                    "symbol": "000001",
                    "intraday_rows": 0,
                    "earliest_bar_time": None,
                    "latest_bar_time": None,
                    "intraday_fetched_at": None,
                    "first_trade_date": pd.Timestamp("2024-03-08").date(),
                }
            ]
        )
        with patch("a_share_research.sync_stock_data.date", DummyDate), patch(
            "a_share_research.sync_stock_data.connect_pg",
            return_value=conn,
        ), patch("a_share_research.sync_stock_data.ensure_schema"), patch(
            "a_share_research.sync_stock_data.sync_instruments"
        ), patch(
            "a_share_research.sync_stock_data.load_current_stock_symbols",
            return_value=["000001"],
        ), patch(
            "a_share_research.sync_stock_data._select_intraday_gap_symbols",
            side_effect=[(["000001"], blocked_status), (["000001"], blocked_status)],
        ), patch(
            "a_share_research.sync_stock_data._sync_intraday_batch",
            side_effect=IntradaySourceBlockedError("blocked"),
        ), patch("a_share_research.sync_stock_data.time.sleep"):
            sync_stock_data.sync_stock_data("refresh", skip_universe=True, intraday_only=True)

        with patch.object(sys, "argv", ["sync_stock_data.py", "--mode", "refresh", "--intraday-retry-rounds", "0", "--intraday-target-rows", "0"]), patch(
            "a_share_research.sync_stock_data.sync_stock_data"
        ) as mocked_sync:
            sync_stock_data.main()
        kwargs = mocked_sync.call_args.kwargs
        self.assertEqual(kwargs["intraday_retry_rounds"], 1)
        self.assertEqual(kwargs["intraday_target_rows"], 1)

        self.assertIs(webapp.create_app, web_app.create_app)
        with patch.object(sys, "argv", ["app.py", "--host", "127.0.0.1", "--port", "9000", "--reload"]), patch(
            "a_share_research.web.app.uvicorn.run"
        ) as mocked_run:
            web_app.main()
        mocked_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
