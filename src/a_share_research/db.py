from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any

import pandas as pd
import psycopg

from .index_data import INDEX_INSTRUMENTS, normalize_symbol


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.instruments (
    symbol TEXT PRIMARY KEY,
    exchange TEXT NOT NULL,
    vendor_symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai'
);

CREATE TABLE IF NOT EXISTS market_data.bar_1d (
    symbol TEXT NOT NULL REFERENCES market_data.instruments(symbol),
    trade_date DATE NOT NULL,
    open NUMERIC(18, 6) NOT NULL,
    high NUMERIC(18, 6) NOT NULL,
    low NUMERIC(18, 6) NOT NULL,
    close NUMERIC(18, 6) NOT NULL,
    volume NUMERIC(20, 4),
    amount NUMERIC(20, 4),
    source TEXT NOT NULL DEFAULT 'akshare:stock_zh_a_hist_tx',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS market_data.bar_60m (
    symbol TEXT NOT NULL REFERENCES market_data.instruments(symbol),
    bar_time TIMESTAMP NOT NULL,
    open NUMERIC(18, 6) NOT NULL,
    high NUMERIC(18, 6) NOT NULL,
    low NUMERIC(18, 6) NOT NULL,
    close NUMERIC(18, 6) NOT NULL,
    volume NUMERIC(20, 4),
    amount NUMERIC(20, 4),
    source TEXT NOT NULL DEFAULT 'akshare:stock_zh_a_minute:60',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, bar_time)
);
"""


def get_pg_dsn() -> str:
    dsn = os.environ.get("A_SHARE_PG_DSN")
    if not dsn:
        raise RuntimeError("A_SHARE_PG_DSN is not set")
    return dsn


def connect_pg(dsn: str | None = None) -> psycopg.Connection:
    conn = psycopg.connect(dsn or get_pg_dsn())
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'Asia/Shanghai'")
    return conn


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def sync_instruments(conn: psycopg.Connection) -> None:
    rows = [
        (item.symbol, item.exchange, item.vendor_symbol, item.name, item.timezone)
        for item in INDEX_INSTRUMENTS
    ]
    sql = """
        INSERT INTO market_data.instruments (symbol, exchange, vendor_symbol, name, timezone)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE
        SET exchange = EXCLUDED.exchange,
            vendor_symbol = EXCLUDED.vendor_symbol,
            name = EXCLUDED.name,
            timezone = EXCLUDED.timezone
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def _nullable(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().replace(tzinfo=None)
    return value


def upsert_bar_1d(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        (
            normalize_symbol(row.symbol),
            row.trade_date,
            _nullable(row.open),
            _nullable(row.high),
            _nullable(row.low),
            _nullable(row.close),
            _nullable(row.volume),
            _nullable(row.amount),
            row.source,
        )
        for row in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO market_data.bar_1d (
            symbol, trade_date, open, high, low, close, volume, amount, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, trade_date) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            source = EXCLUDED.source,
            fetched_at = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def upsert_bar_60m(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        (
            normalize_symbol(row.symbol),
            _nullable(row.bar_time),
            _nullable(row.open),
            _nullable(row.high),
            _nullable(row.low),
            _nullable(row.close),
            _nullable(row.volume),
            _nullable(row.amount),
            row.source,
        )
        for row in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO market_data.bar_60m (
            symbol, bar_time, open, high, low, close, volume, amount, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, bar_time) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            source = EXCLUDED.source,
            fetched_at = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def get_latest_trade_date(conn: psycopg.Connection, symbol: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(trade_date) FROM market_data.bar_1d WHERE symbol = %s",
            (normalize_symbol(symbol),),
        )
        value = cur.fetchone()[0]
    return value


def load_bar_1d(
    symbol: str,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    dsn: str | None = None,
) -> pd.DataFrame:
    query = """
        SELECT symbol, trade_date, open, high, low, close, volume, amount, source, fetched_at
        FROM market_data.bar_1d
        WHERE symbol = %s
    """
    params: list[Any] = [normalize_symbol(symbol)]
    if start_date is not None:
        query += " AND trade_date >= %s"
        params.append(start_date)
    if end_date is not None:
        query += " AND trade_date <= %s"
        params.append(end_date)
    query += " ORDER BY trade_date"

    with connect_pg(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [item.name for item in cur.description]
    return pd.DataFrame(rows, columns=columns)


def load_bar_60m(
    symbol: str,
    start_time: datetime | str | None = None,
    end_time: datetime | str | None = None,
    dsn: str | None = None,
) -> pd.DataFrame:
    query = """
        SELECT symbol, bar_time, open, high, low, close, volume, amount, source, fetched_at
        FROM market_data.bar_60m
        WHERE symbol = %s
    """
    params: list[Any] = [normalize_symbol(symbol)]
    if start_time is not None:
        query += " AND bar_time >= %s"
        params.append(start_time)
    if end_time is not None:
        query += " AND bar_time <= %s"
        params.append(end_time)
    query += " ORDER BY bar_time"

    with connect_pg(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [item.name for item in cur.description]
    return pd.DataFrame(rows, columns=columns)


def load_dashboard_cards(dsn: str | None = None) -> pd.DataFrame:
    query = """
        SELECT
            i.symbol,
            i.exchange,
            i.vendor_symbol,
            i.name,
            i.timezone,
            d.daily_rows,
            d.latest_trade_date,
            d.daily_fetched_at,
            m.intraday_rows,
            m.latest_bar_time,
            m.intraday_fetched_at,
            GREATEST(
                COALESCE(d.daily_fetched_at, '-infinity'::timestamptz),
                COALESCE(m.intraday_fetched_at, '-infinity'::timestamptz)
            ) AS latest_sync_at
        FROM market_data.instruments AS i
        LEFT JOIN (
            SELECT
                symbol,
                COUNT(*) AS daily_rows,
                MAX(trade_date) AS latest_trade_date,
                MAX(fetched_at) AS daily_fetched_at
            FROM market_data.bar_1d
            GROUP BY symbol
        ) AS d USING (symbol)
        LEFT JOIN (
            SELECT
                symbol,
                COUNT(*) AS intraday_rows,
                MAX(bar_time) AS latest_bar_time,
                MAX(fetched_at) AS intraday_fetched_at
            FROM market_data.bar_60m
            GROUP BY symbol
        ) AS m USING (symbol)
        ORDER BY i.symbol
    """
    with connect_pg(dsn) as conn, conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [item.name for item in cur.description]
    return pd.DataFrame(rows, columns=columns)


def get_latest_sync_time(dsn: str | None = None) -> datetime | None:
    query = """
        SELECT GREATEST(
            COALESCE((SELECT MAX(fetched_at) FROM market_data.bar_1d), '-infinity'::timestamptz),
            COALESCE((SELECT MAX(fetched_at) FROM market_data.bar_60m), '-infinity'::timestamptz)
        ) AS latest_sync_at
    """
    with connect_pg(dsn) as conn, conn.cursor() as cur:
        cur.execute(query)
        value = cur.fetchone()[0]
    if value == datetime.min.replace(tzinfo=value.tzinfo) if value is not None else False:
        return None
    return value
