from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from a_share_research import data, index_data, stock_data, sync_index_data


class DataAdapterTests(unittest.TestCase):
    def test_data_helpers_and_fetch_stock_history(self) -> None:
        self.assertEqual(data._normalize_symbol("sh000001"), "000001")
        self.assertEqual(data._to_sina_symbol("600000"), "sh600000")
        self.assertEqual(data._to_sina_symbol("000001"), "sz000001")
        self.assertTrue(str(data.project_root()).endswith("a_share_research"))

        raw = pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-03"],
                "open": [10, 11],
                "close": [11, 12],
                "high": [11, 12],
                "low": [9, 10],
                "volume": [1, 2],
                "amount": [100, 200],
                "turnover": [0.1, 0.2],
            }
        )
        formatted = data._format_sina_daily(raw, "000001")
        self.assertEqual(formatted.iloc[0]["股票代码"], "000001")

        with patch("a_share_research.data.ak.stock_zh_a_hist", return_value=raw) as mocked_hist:
            result = data.fetch_stock_history("000001")
        self.assertTrue(mocked_hist.called)
        self.assertIs(result, raw)

        with patch("a_share_research.data.ak.stock_zh_a_hist", side_effect=RuntimeError("boom")), patch(
            "a_share_research.data.ak.stock_zh_a_daily", return_value=raw
        ):
            fallback = data.fetch_stock_history("000001")
        self.assertEqual(fallback.iloc[0]["股票代码"], "000001")

    def test_index_data_helpers_and_fetchers(self) -> None:
        self.assertEqual(index_data.normalize_symbol("sh000300"), "000300")
        self.assertEqual(index_data.get_instrument("000905").name, "中证500")
        self.assertRegex(index_data.default_daily_start(today=pd.Timestamp("2026-03-23").date()), r"^\d{8}$")
        self.assertEqual(index_data.next_daily_start(None, today=pd.Timestamp("2026-03-23").date()), "20160101")
        self.assertEqual(index_data.next_daily_start(pd.Timestamp("2026-03-20").date()), "20260321")

        raw_daily = pd.DataFrame(
            {"date": ["2026-03-20"], "open": [1], "high": [2], "low": [0.5], "close": [1.5], "amount": [100]}
        )
        with patch("a_share_research.index_data.ak.stock_zh_a_hist_tx", return_value=raw_daily):
            frame = index_data.fetch_index_daily("000300", "20260101", "20260320")
        self.assertEqual(frame.iloc[0]["symbol"], "000300")

        raw_60m = pd.DataFrame(
            {
                "day": ["2026-03-20 15:00:00"],
                "open": [1],
                "high": [2],
                "low": [0.5],
                "close": [1.5],
                "volume": [100],
                "amount": [200],
            }
        )
        with patch("a_share_research.index_data.ak.stock_zh_a_minute", return_value=raw_60m):
            frame = index_data.fetch_index_60m("000905")
        self.assertEqual(frame.iloc[0]["symbol"], "000905")

    def test_stock_data_helpers_and_fetchers(self) -> None:
        self.assertEqual(stock_data.normalize_stock_symbol("sz000001"), "000001")
        self.assertEqual(stock_data.infer_stock_exchange("600000"), "SH")
        self.assertEqual(stock_data.to_vendor_stock_symbol("000001"), "sz000001")
        self.assertEqual(stock_data.to_baostock_stock_symbol("600000"), "sh.600000")
        self.assertTrue(stock_data._default_stock_60m_start_date(today=pd.Timestamp("2026-03-23").date()).startswith("2024"))

        blocked_response = MagicMock(status_code=456, text="拒绝访问")
        with patch("a_share_research.stock_data.requests.get", return_value=blocked_response):
            with self.assertRaises(stock_data.IntradaySourceBlockedError):
                stock_data._fetch_sina_stock_60m_frame("sh600000")

        ok_response = MagicMock(status_code=200, text='foo=([{"day":"2026-03-20 15:00:00","open":"1","high":"2","low":"0.5","close":"1.5","volume":"10","amount":"20"}]);')
        with patch("a_share_research.stock_data.requests.get", return_value=ok_response):
            frame = stock_data._fetch_sina_stock_60m_frame("sh600000")
        self.assertEqual(len(frame), 1)

        baostock_result = MagicMock(error_code="0", error_msg="success")
        rows = iter([["2026-03-20", "20260320150000000", "sh.600000", "1", "2", "0.5", "1.5", "10", "20"]])
        baostock_result.next.side_effect = [True, False]
        baostock_result.get_row_data.side_effect = lambda: next(rows)
        with patch("a_share_research.stock_data.bs.login", return_value=MagicMock(error_code="0", error_msg="success")), patch(
            "a_share_research.stock_data.bs.query_history_k_data_plus", return_value=baostock_result
        ), patch("a_share_research.stock_data.bs.logout"):
            frame = stock_data._fetch_baostock_stock_60m_frame("sh.600000", "2024-01-01", "2026-03-23")
        self.assertEqual(len(frame), 1)

        built_from_sina = stock_data._build_stock_60m_frame(
            pd.DataFrame([{"day": "2026-03-20 15:00:00", "open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "1", "amount": "2"}]),
            "000001",
            source="demo",
            from_baostock=False,
        )
        self.assertEqual(built_from_sina.iloc[0]["symbol"], "000001")

        built_from_baostock = stock_data._build_stock_60m_frame(
            pd.DataFrame([{"time": "20260320150000000", "open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "1", "amount": "2"}]),
            "000001",
            source="demo",
            from_baostock=True,
        )
        self.assertEqual(built_from_baostock.iloc[0]["symbol"], "000001")

        constituent_df = pd.DataFrame(
            {
                "指数代码": ["000300"],
                "指数名称": ["沪深300"],
                "成分券代码": ["000001"],
                "成分券名称": ["平安银行"],
                "交易所": ["深圳证券交易所"],
                "日期": ["2026-03-20"],
            }
        )
        with patch("a_share_research.stock_data.ak.index_stock_cons_csindex", return_value=constituent_df):
            frame = stock_data.fetch_index_constituents("000300")
        self.assertEqual(frame.iloc[0]["stock_symbol"], "000001")

        daily_df = pd.DataFrame({"date": ["2026-03-20"], "open": [1], "high": [2], "low": [0.5], "close": [1.5], "amount": [100]})
        with patch("a_share_research.stock_data.ak.stock_zh_a_hist_tx", return_value=daily_df):
            frame = stock_data.fetch_stock_daily("000001", "20260101", "20260320")
        self.assertEqual(frame.iloc[0]["symbol"], "000001")

        with patch("a_share_research.stock_data._fetch_sina_stock_60m_frame", return_value=pd.DataFrame([{"day": "2026-03-20 15:00:00", "open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "1", "amount": "2"}])):
            frame = stock_data.fetch_stock_60m("000001")
        self.assertEqual(frame.iloc[0]["source"], stock_data.STOCK_BAR_60M_SOURCE)

        with patch("a_share_research.stock_data._fetch_sina_stock_60m_frame", side_effect=stock_data.IntradaySourceBlockedError("blocked")), patch(
            "a_share_research.stock_data._fetch_baostock_stock_60m_frame",
            return_value=pd.DataFrame([{"time": "20260320150000000", "open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "1", "amount": "2"}]),
        ):
            frame = stock_data.fetch_stock_60m("000001")
        self.assertEqual(frame.iloc[0]["source"], stock_data.STOCK_BAR_60M_SOURCE_FALLBACK)

        master = stock_data.build_stock_master(
            pd.DataFrame(
                [
                    {"stock_symbol": "000001", "exchange": "SZ", "vendor_symbol": "sz000001", "stock_name": "平安银行", "source": "demo"},
                    {"stock_symbol": "000001", "exchange": "SZ", "vendor_symbol": "sz000001", "stock_name": "平安银行", "source": "demo"},
                ]
            )
        )
        self.assertEqual(len(master), 1)

    def test_sync_index_data_flow(self) -> None:
        class DummyDate:
            @classmethod
            def today(cls):
                return pd.Timestamp("2026-03-23").date()

        class DummyConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        conn = DummyConn()
        with patch("a_share_research.sync_index_data.date", DummyDate), patch(
            "a_share_research.sync_index_data.connect_pg", return_value=conn
        ), patch("a_share_research.sync_index_data.ensure_schema"), patch(
            "a_share_research.sync_index_data.sync_instruments"
        ), patch(
            "a_share_research.sync_index_data.fetch_index_daily", return_value=pd.DataFrame([{"symbol": "000300"}])
        ), patch(
            "a_share_research.sync_index_data.fetch_index_60m", return_value=pd.DataFrame([{"symbol": "000300"}])
        ), patch(
            "a_share_research.sync_index_data.upsert_bar_1d", return_value=1
        ) as daily_upsert, patch(
            "a_share_research.sync_index_data.upsert_bar_60m", return_value=2
        ) as intraday_upsert, patch(
            "a_share_research.sync_index_data.get_latest_trade_date", return_value=pd.Timestamp("2026-03-20").date()
        ):
            sync_index_data.sync_all("refresh")
        self.assertTrue(daily_upsert.called)
        self.assertTrue(intraday_upsert.called)


if __name__ == "__main__":
    unittest.main()
