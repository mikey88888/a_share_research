from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any

import pandas as pd
import psycopg

from .index_data import INDEX_INSTRUMENTS, normalize_symbol as normalize_index_symbol
from .stock_data import TRACKED_INDEX_SYMBOLS, normalize_stock_symbol


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

CREATE TABLE IF NOT EXISTS market_data.stocks (
    symbol TEXT PRIMARY KEY,
    exchange TEXT NOT NULL,
    vendor_symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
    source TEXT NOT NULL DEFAULT 'akshare:index_stock_cons_csindex',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_data.index_constituents_current (
    index_symbol TEXT NOT NULL REFERENCES market_data.instruments(symbol),
    stock_symbol TEXT NOT NULL REFERENCES market_data.stocks(symbol) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    index_name TEXT NOT NULL,
    stock_name TEXT NOT NULL,
    exchange TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'akshare:index_stock_cons_csindex',
    PRIMARY KEY (index_symbol, stock_symbol)
);

CREATE TABLE IF NOT EXISTS market_data.stock_bar_1d (
    symbol TEXT NOT NULL REFERENCES market_data.stocks(symbol) ON DELETE CASCADE,
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

CREATE TABLE IF NOT EXISTS market_data.stock_bar_60m (
    symbol TEXT NOT NULL REFERENCES market_data.stocks(symbol) ON DELETE CASCADE,
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
        # The local user-space PostgreSQL package may not ship LLVM JIT libraries.
        # Disabling JIT keeps ad-hoc analytical queries stable inside WSL.
        cur.execute("SET jit = off")
    return conn


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        for statement in SCHEMA_SQL.split(";"):
            sql = statement.strip()
            if sql:
                cur.execute(sql)
    conn.commit()


def _nullable(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().replace(tzinfo=None)
    return value


def _fetch_dataframe(query: str, params: list[Any] | tuple[Any, ...] | None = None, dsn: str | None = None) -> pd.DataFrame:
    with connect_pg(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, params or [])
        rows = cur.fetchall()
        columns = [item.name for item in cur.description]
    return pd.DataFrame(rows, columns=columns)


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


def upsert_stocks(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        (
            normalize_stock_symbol(row.symbol),
            row.exchange,
            row.vendor_symbol,
            row.name,
            row.timezone,
            row.source,
        )
        for row in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO market_data.stocks (symbol, exchange, vendor_symbol, name, timezone, source)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE
        SET exchange = EXCLUDED.exchange,
            vendor_symbol = EXCLUDED.vendor_symbol,
            name = EXCLUDED.name,
            timezone = EXCLUDED.timezone,
            source = EXCLUDED.source,
            fetched_at = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def replace_index_constituents_current(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    target_indices = sorted(df["index_symbol"].drop_duplicates().tolist()) if not df.empty else list(TRACKED_INDEX_SYMBOLS)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM market_data.index_constituents_current WHERE index_symbol = ANY(%s)",
            (target_indices,),
        )
        if not df.empty:
            rows = [
                (
                    normalize_index_symbol(row.index_symbol),
                    normalize_stock_symbol(row.stock_symbol),
                    row.as_of_date,
                    row.index_name,
                    row.stock_name,
                    row.exchange,
                    row.source,
                )
                for row in df.itertuples(index=False)
            ]
            cur.executemany(
                """
                INSERT INTO market_data.index_constituents_current (
                    index_symbol, stock_symbol, as_of_date, index_name, stock_name, exchange, source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
    conn.commit()
    return len(df)


def _upsert_bars(
    conn: psycopg.Connection,
    df: pd.DataFrame,
    *,
    table_name: str,
    time_column: str,
    symbol_normalizer,
) -> int:
    if df.empty:
        return 0
    rows = [
        (
            symbol_normalizer(row.symbol),
            _nullable(getattr(row, time_column)),
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
    sql = f"""
        INSERT INTO market_data.{table_name} (
            symbol, {time_column}, open, high, low, close, volume, amount, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, {time_column}) DO UPDATE
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


def upsert_bar_1d(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    return _upsert_bars(
        conn,
        df,
        table_name="bar_1d",
        time_column="trade_date",
        symbol_normalizer=normalize_index_symbol,
    )


def upsert_bar_60m(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    return _upsert_bars(
        conn,
        df,
        table_name="bar_60m",
        time_column="bar_time",
        symbol_normalizer=normalize_index_symbol,
    )


def upsert_stock_bar_1d(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    return _upsert_bars(
        conn,
        df,
        table_name="stock_bar_1d",
        time_column="trade_date",
        symbol_normalizer=normalize_stock_symbol,
    )


def upsert_stock_bar_60m(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    return _upsert_bars(
        conn,
        df,
        table_name="stock_bar_60m",
        time_column="bar_time",
        symbol_normalizer=normalize_stock_symbol,
    )


def _get_latest_date(conn: psycopg.Connection, table_name: str, symbol: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MAX(trade_date) FROM market_data.{table_name} WHERE symbol = %s",
            (symbol,),
        )
        value = cur.fetchone()[0]
    return value


def get_latest_trade_date(conn: psycopg.Connection, symbol: str) -> date | None:
    return _get_latest_date(conn, "bar_1d", normalize_index_symbol(symbol))


def get_latest_stock_trade_date(conn: psycopg.Connection, symbol: str) -> date | None:
    return _get_latest_date(conn, "stock_bar_1d", normalize_stock_symbol(symbol))


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
    params: list[Any] = [normalize_index_symbol(symbol)]
    if start_date is not None:
        query += " AND trade_date >= %s"
        params.append(start_date)
    if end_date is not None:
        query += " AND trade_date <= %s"
        params.append(end_date)
    query += " ORDER BY trade_date"
    return _fetch_dataframe(query, params=params, dsn=dsn)


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
    params: list[Any] = [normalize_index_symbol(symbol)]
    if start_time is not None:
        query += " AND bar_time >= %s"
        params.append(start_time)
    if end_time is not None:
        query += " AND bar_time <= %s"
        params.append(end_time)
    query += " ORDER BY bar_time"
    return _fetch_dataframe(query, params=params, dsn=dsn)


def load_stock_bar_1d(
    symbol: str,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    dsn: str | None = None,
) -> pd.DataFrame:
    query = """
        SELECT symbol, trade_date, open, high, low, close, volume, amount, source, fetched_at
        FROM market_data.stock_bar_1d
        WHERE symbol = %s
    """
    params: list[Any] = [normalize_stock_symbol(symbol)]
    if start_date is not None:
        query += " AND trade_date >= %s"
        params.append(start_date)
    if end_date is not None:
        query += " AND trade_date <= %s"
        params.append(end_date)
    query += " ORDER BY trade_date"
    return _fetch_dataframe(query, params=params, dsn=dsn)


def load_stock_bar_60m(
    symbol: str,
    start_time: datetime | str | None = None,
    end_time: datetime | str | None = None,
    dsn: str | None = None,
) -> pd.DataFrame:
    query = """
        SELECT symbol, bar_time, open, high, low, close, volume, amount, source, fetched_at
        FROM market_data.stock_bar_60m
        WHERE symbol = %s
    """
    params: list[Any] = [normalize_stock_symbol(symbol)]
    if start_time is not None:
        query += " AND bar_time >= %s"
        params.append(start_time)
    if end_time is not None:
        query += " AND bar_time <= %s"
        params.append(end_time)
    query += " ORDER BY bar_time"
    return _fetch_dataframe(query, params=params, dsn=dsn)


def load_stocks(dsn: str | None = None) -> pd.DataFrame:
    query = """
        SELECT symbol, exchange, vendor_symbol, name, timezone, source, fetched_at
        FROM market_data.stocks
        ORDER BY symbol
    """
    return _fetch_dataframe(query, dsn=dsn)


def load_current_stock_symbols(conn: psycopg.Connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM market_data.stocks ORDER BY symbol")
        rows = cur.fetchall()
    return [row[0] for row in rows]


def load_stock_bar_60m_status(
    conn: psycopg.Connection,
    symbols: list[str] | None = None,
) -> pd.DataFrame:
    query = """
        SELECT
            s.symbol,
            COALESCE(m.intraday_rows, 0) AS intraday_rows,
            m.earliest_bar_time,
            m.latest_bar_time,
            m.intraday_fetched_at,
            d.first_trade_date
        FROM market_data.stocks AS s
        LEFT JOIN (
            SELECT
                symbol,
                COUNT(*) AS intraday_rows,
                MIN(bar_time) AS earliest_bar_time,
                MAX(bar_time) AS latest_bar_time,
                MAX(fetched_at) AS intraday_fetched_at
            FROM market_data.stock_bar_60m
            GROUP BY symbol
        ) AS m USING (symbol)
        LEFT JOIN (
            SELECT
                symbol,
                MIN(trade_date) AS first_trade_date
            FROM market_data.stock_bar_1d
            GROUP BY symbol
        ) AS d USING (symbol)
    """
    params: list[Any] = []
    if symbols is not None:
        normalized = [normalize_stock_symbol(symbol) for symbol in symbols]
        query += " WHERE s.symbol = ANY(%s)"
        params.append(normalized)
    query += " ORDER BY s.symbol"

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [item.name for item in cur.description]
    return pd.DataFrame(rows, columns=columns)


def load_index_constituents(index_symbol: str | None = None, dsn: str | None = None) -> pd.DataFrame:
    query = """
        SELECT index_symbol, stock_symbol, as_of_date, index_name, stock_name, exchange, source
        FROM market_data.index_constituents_current
    """
    params: list[Any] = []
    if index_symbol is not None:
        query += " WHERE index_symbol = %s"
        params.append(normalize_index_symbol(index_symbol))
    query += " ORDER BY index_symbol, stock_symbol"
    return _fetch_dataframe(query, params=params, dsn=dsn)


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
    return _fetch_dataframe(query, dsn=dsn)


def load_stock_library_summary(dsn: str | None = None) -> dict[str, Any]:
    query = """
        SELECT
            (SELECT COUNT(*) FROM market_data.stocks) AS stock_count,
            (SELECT COUNT(*) FROM market_data.stock_bar_1d) AS daily_rows,
            (SELECT COUNT(*) FROM market_data.stock_bar_60m) AS intraday_rows,
            GREATEST(
                COALESCE((SELECT MAX(fetched_at) FROM market_data.stocks), '-infinity'::timestamptz),
                COALESCE((SELECT MAX(fetched_at) FROM market_data.stock_bar_1d), '-infinity'::timestamptz),
                COALESCE((SELECT MAX(fetched_at) FROM market_data.stock_bar_60m), '-infinity'::timestamptz)
            ) AS latest_sync_at
    """
    frame = _fetch_dataframe(query, dsn=dsn)
    if frame.empty:
        return {"stock_count": 0, "daily_rows": 0, "intraday_rows": 0, "latest_sync_at": None}
    row = frame.iloc[0].to_dict()
    return row


def get_latest_sync_time(dsn: str | None = None) -> datetime | None:
    query = """
        SELECT GREATEST(
            COALESCE((SELECT MAX(fetched_at) FROM market_data.bar_1d), '-infinity'::timestamptz),
            COALESCE((SELECT MAX(fetched_at) FROM market_data.bar_60m), '-infinity'::timestamptz),
            COALESCE((SELECT MAX(fetched_at) FROM market_data.stock_bar_1d), '-infinity'::timestamptz),
            COALESCE((SELECT MAX(fetched_at) FROM market_data.stock_bar_60m), '-infinity'::timestamptz)
        ) AS latest_sync_at
    """
    with connect_pg(dsn) as conn, conn.cursor() as cur:
        cur.execute(query)
        value = cur.fetchone()[0]
    return value
