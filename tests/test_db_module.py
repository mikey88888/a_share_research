from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from a_share_research import db


class FakeCursor:
    def __init__(self, results: list[dict] | None = None):
        self.results = list(results or [])
        self.current: dict = {}
        self.execute_calls: list[tuple[str, object]] = []
        self.executemany_calls: list[tuple[str, list[tuple]]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.execute_calls.append((query, params))
        self.current = self.results.pop(0) if self.results else {}

    def executemany(self, query, rows):
        self.executemany_calls.append((query, list(rows)))
        self.current = {}

    def fetchone(self):
        return self.current.get("one")

    def fetchall(self):
        return self.current.get("all", [])

    @property
    def description(self):
        return [SimpleNamespace(name=name) for name in self.current.get("description", [])]


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


class DbModuleTests(unittest.TestCase):
    def test_get_pg_dsn_requires_env(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "A_SHARE_PG_DSN is not set"):
                db.get_pg_dsn()

        with patch.dict(os.environ, {"A_SHARE_PG_DSN": "postgresql://demo"}):
            self.assertEqual(db.get_pg_dsn(), "postgresql://demo")

    def test_connect_pg_sets_timezone_and_jit(self) -> None:
        cursor = FakeCursor()
        conn = FakeConnection(cursor)
        with patch("a_share_research.db.psycopg.connect", return_value=conn) as mocked:
            result = db.connect_pg("postgresql://demo")
        self.assertIs(result, conn)
        mocked.assert_called_once_with("postgresql://demo")
        self.assertEqual(cursor.execute_calls[0][0], "SET TIME ZONE 'Asia/Shanghai'")
        self.assertEqual(cursor.execute_calls[1][0], "SET jit = off")

    def test_ensure_schema_executes_each_statement(self) -> None:
        cursor = FakeCursor()
        conn = FakeConnection(cursor)
        db.ensure_schema(conn)
        self.assertGreater(len(cursor.execute_calls), 3)
        self.assertEqual(conn.commits, 1)

    def test_nullable_and_fetch_dataframe(self) -> None:
        ts = pd.Timestamp("2026-03-23 10:30:00")
        self.assertIsNone(db._nullable(float("nan")))
        self.assertEqual(db._nullable(ts), ts.to_pydatetime().replace(tzinfo=None))
        self.assertEqual(db._nullable(3), 3)

        cursor = FakeCursor([{"all": [(1, "x")], "description": ["id", "name"]}])
        conn = FakeConnection(cursor)
        with patch("a_share_research.db.connect_pg", return_value=conn):
            frame = db._fetch_dataframe("select 1", params=["demo"])
        self.assertEqual(frame.to_dict(orient="records"), [{"id": 1, "name": "x"}])
        self.assertEqual(cursor.execute_calls[0], ("select 1", ["demo"]))

    def test_mutating_queries_commit_and_normalize(self) -> None:
        cursor = FakeCursor()
        conn = FakeConnection(cursor)

        stocks = pd.DataFrame(
            [
                {
                    "symbol": "000001",
                    "exchange": "SZ",
                    "vendor_symbol": "sz000001",
                    "name": "平安银行",
                    "timezone": "Asia/Shanghai",
                    "source": "demo",
                }
            ]
        )
        count = db.upsert_stocks(conn, stocks)
        self.assertEqual(count, 1)
        self.assertEqual(conn.commits, 1)
        self.assertIn("INSERT INTO market_data.stocks", cursor.executemany_calls[0][0])

        constituents = pd.DataFrame(
            [
                {
                    "index_symbol": "000300",
                    "stock_symbol": "000001",
                    "as_of_date": pd.Timestamp("2026-03-20").date(),
                    "index_name": "沪深300",
                    "stock_name": "平安银行",
                    "exchange": "SZ",
                    "source": "demo",
                }
            ]
        )
        db.replace_index_constituents_current(conn, constituents)
        self.assertEqual(conn.commits, 2)
        self.assertIn("DELETE FROM market_data.index_constituents_current", cursor.execute_calls[-1][0])

        bars = pd.DataFrame(
            [
                {
                    "symbol": "000300",
                    "trade_date": pd.Timestamp("2026-03-20").date(),
                    "bar_time": pd.Timestamp("2026-03-20 15:00:00"),
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "source": "demo",
                }
            ]
        )
        db.upsert_bar_1d(conn, bars.loc[:, ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount", "source"]])
        db.upsert_stock_bar_60m(conn, bars.loc[:, ["symbol", "bar_time", "open", "high", "low", "close", "volume", "amount", "source"]].assign(symbol="000001"))
        self.assertGreaterEqual(conn.commits, 4)

    def test_get_latest_and_load_queries_delegate_to_fetch_dataframe(self) -> None:
        cursor = FakeCursor([{"one": [pd.Timestamp("2026-03-20").date()]}])
        conn = FakeConnection(cursor)
        value = db._get_latest_date(conn, "stock_bar_1d", "000001")
        self.assertEqual(str(value), "2026-03-20")

        sample = pd.DataFrame([{"symbol": "000001"}])
        with patch("a_share_research.db._fetch_dataframe", return_value=sample) as mocked:
            self.assertIs(db.load_bar_1d("000300"), sample)
            self.assertIs(db.load_bar_60m("000300"), sample)
            self.assertIs(db.load_stock_bar_1d("000001"), sample)
            self.assertIs(db.load_stock_bar_60m("000001"), sample)
            self.assertIs(db.load_stocks(), sample)
            self.assertIs(db.load_index_constituents(), sample)
            self.assertIs(db.load_dashboard_cards(), sample)
        self.assertEqual(mocked.call_count, 7)

    def test_load_current_symbols_status_and_summaries(self) -> None:
        cursor = FakeCursor(
            [
                {"all": [("000001",), ("000002",)]},
                {
                    "all": [("000001", 10, None, None, None, pd.Timestamp("2026-03-20").date())],
                    "description": [
                        "symbol",
                        "intraday_rows",
                        "earliest_bar_time",
                        "latest_bar_time",
                        "intraday_fetched_at",
                        "first_trade_date",
                    ],
                },
                {"one": [pd.Timestamp("2026-03-23 10:00:00")]},
            ]
        )
        conn = FakeConnection(cursor)
        self.assertEqual(db.load_current_stock_symbols(conn), ["000001", "000002"])
        status = db.load_stock_bar_60m_status(conn, symbols=["000001"])
        self.assertEqual(status.iloc[0]["symbol"], "000001")

        with patch("a_share_research.db._fetch_dataframe", return_value=pd.DataFrame()) as mocked:
            summary = db.load_stock_library_summary()
        self.assertEqual(summary["stock_count"], 0)
        self.assertTrue(mocked.called)

        with patch(
            "a_share_research.db._fetch_dataframe",
            return_value=pd.DataFrame(
                [{"stock_count": 1, "daily_rows": 2, "intraday_rows": 3, "latest_sync_at": "demo"}]
            ),
        ):
            summary = db.load_stock_library_summary()
        self.assertEqual(summary["stock_count"], 1)

        with patch("a_share_research.db.connect_pg", return_value=conn):
            latest = db.get_latest_sync_time()
        self.assertEqual(str(latest), "2026-03-23 10:00:00")


if __name__ == "__main__":
    unittest.main()
