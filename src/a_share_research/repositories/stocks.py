from __future__ import annotations

from typing import Any

import pandas as pd

from ..db import connect_pg, load_stocks
from ..stock_data import normalize_stock_symbol

STOCK_SORT_OPTIONS = {
    "symbol": "s.symbol ASC",
    "name": "s.name ASC, s.symbol ASC",
    "latest_trade": "daily.latest_trade_date DESC NULLS LAST, s.symbol ASC",
    "latest_60m": "intraday.latest_bar_time DESC NULLS LAST, s.symbol ASC",
}


def _fetch_dataframe(query: str, params: list[Any] | tuple[Any, ...] | None = None, dsn: str | None = None) -> pd.DataFrame:
    with connect_pg(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, params or [])
        rows = cur.fetchall()
        columns = [item.name for item in cur.description]
    return pd.DataFrame(rows, columns=columns)


def _stock_summary_ctes() -> str:
    return """
        WITH daily AS (
            SELECT
                symbol,
                COUNT(*) AS daily_rows,
                MIN(trade_date) AS first_trade_date,
                MAX(trade_date) AS latest_trade_date,
                MAX(fetched_at) AS daily_fetched_at,
                STRING_AGG(DISTINCT source, ', ') AS daily_sources
            FROM market_data.stock_bar_1d
            GROUP BY symbol
        ),
        intraday AS (
            SELECT
                symbol,
                COUNT(*) AS intraday_rows,
                MIN(bar_time) AS earliest_bar_time,
                MAX(bar_time) AS latest_bar_time,
                MAX(fetched_at) AS intraday_fetched_at,
                STRING_AGG(DISTINCT source, ', ') AS intraday_sources
            FROM market_data.stock_bar_60m
            GROUP BY symbol
        ),
        membership AS (
            SELECT
                c.stock_symbol AS symbol,
                BOOL_OR(c.index_symbol = '000300') AS in_hs300,
                BOOL_OR(c.index_symbol = '000905') AS in_csi500,
                STRING_AGG(DISTINCT i.name, ' / ') AS index_names
            FROM market_data.index_constituents_current AS c
            JOIN market_data.instruments AS i
              ON i.symbol = c.index_symbol
            GROUP BY c.stock_symbol
        )
    """


def _stock_summary_select() -> str:
    return """
        SELECT
            s.symbol,
            s.exchange,
            s.vendor_symbol,
            s.name,
            s.timezone,
            s.source,
            s.fetched_at AS stock_fetched_at,
            COALESCE(daily.daily_rows, 0) AS daily_rows,
            daily.first_trade_date,
            daily.latest_trade_date,
            daily.daily_fetched_at,
            daily.daily_sources,
            COALESCE(intraday.intraday_rows, 0) AS intraday_rows,
            intraday.earliest_bar_time,
            intraday.latest_bar_time,
            intraday.intraday_fetched_at,
            intraday.intraday_sources,
            COALESCE(membership.in_hs300, FALSE) AS in_hs300,
            COALESCE(membership.in_csi500, FALSE) AS in_csi500,
            membership.index_names,
            GREATEST(
                COALESCE(s.fetched_at, '-infinity'::timestamptz),
                COALESCE(daily.daily_fetched_at, '-infinity'::timestamptz),
                COALESCE(intraday.intraday_fetched_at, '-infinity'::timestamptz)
            ) AS latest_sync_at
        FROM market_data.stocks AS s
        LEFT JOIN daily USING (symbol)
        LEFT JOIN intraday USING (symbol)
        LEFT JOIN membership USING (symbol)
    """


def _build_stock_filters(q: str | None, exchange: str) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if q:
        clauses.append("(s.symbol ILIKE %s OR s.name ILIKE %s)")
        pattern = f"%{q.strip()}%"
        params.extend([pattern, pattern])
    if exchange != "ALL":
        clauses.append("s.exchange = %s")
        params.append(exchange)
    if not clauses:
        return "", params
    return " WHERE " + " AND ".join(clauses), params


def _membership_label(record: dict[str, Any]) -> str:
    in_hs300 = bool(record.get("in_hs300"))
    in_csi500 = bool(record.get("in_csi500"))
    if in_hs300 and in_csi500:
        return "沪深300 / 中证500"
    if in_hs300:
        return "沪深300"
    if in_csi500:
        return "中证500"
    return "非当前成分"


def search_stocks(
    *,
    q: str | None = None,
    exchange: str = "ALL",
    sort: str = "symbol",
    page: int = 1,
    page_size: int = 50,
    dsn: str | None = None,
) -> dict[str, Any]:
    normalized_page = max(page, 1)
    normalized_page_size = min(max(page_size, 1), 100)
    order_by = STOCK_SORT_OPTIONS.get(sort, STOCK_SORT_OPTIONS["symbol"])
    where_sql, params = _build_stock_filters(q=q, exchange=exchange)
    offset = (normalized_page - 1) * normalized_page_size

    count_query = f"SELECT COUNT(*) AS total FROM market_data.stocks AS s{where_sql}"
    total_frame = _fetch_dataframe(count_query, params=params, dsn=dsn)
    total = int(total_frame.iloc[0]["total"]) if not total_frame.empty else 0

    query = f"""
        {_stock_summary_ctes()}
        {_stock_summary_select()}
        {where_sql}
        ORDER BY {order_by}
        LIMIT %s OFFSET %s
    """
    frame = _fetch_dataframe(query, params=[*params, normalized_page_size, offset], dsn=dsn)
    items = frame.to_dict(orient="records")
    for item in items:
        item["membership_label"] = _membership_label(item)
        item["detail_url"] = f"/markets/stocks/{item['symbol']}"

    total_pages = max((total + normalized_page_size - 1) // normalized_page_size, 1) if total else 1
    return {
        "items": items,
        "total": total,
        "page": normalized_page,
        "page_size": normalized_page_size,
        "total_pages": total_pages,
        "sort": sort,
        "exchange": exchange,
        "q": q or "",
    }


def search_stock_suggestions(q: str, limit: int = 10, dsn: str | None = None) -> list[dict[str, Any]]:
    query = """
        SELECT symbol, exchange, name
        FROM market_data.stocks
        WHERE symbol ILIKE %s OR name ILIKE %s
        ORDER BY CASE WHEN symbol = %s THEN 0 ELSE 1 END, symbol
        LIMIT %s
    """
    term = q.strip()
    if not term:
        return []
    pattern = f"%{term}%"
    frame = _fetch_dataframe(query, params=[pattern, pattern, term, limit], dsn=dsn)
    items = frame.to_dict(orient="records")
    for item in items:
        item["detail_url"] = f"/markets/stocks/{item['symbol']}"
    return items


def get_stock_profile(symbol: str, dsn: str | None = None) -> dict[str, Any] | None:
    normalized = normalize_stock_symbol(symbol)
    query = f"""
        {_stock_summary_ctes()}
        {_stock_summary_select()}
        WHERE s.symbol = %s
    """
    frame = _fetch_dataframe(query, params=[normalized], dsn=dsn)
    if frame.empty:
        return None
    record = frame.iloc[0].to_dict()
    record["membership_label"] = _membership_label(record)
    record["detail_url"] = f"/markets/stocks/{record['symbol']}"
    return record


def get_stock_neighbors(symbol: str, dsn: str | None = None) -> dict[str, dict[str, Any] | None]:
    normalized = normalize_stock_symbol(symbol)
    query = """
        SELECT symbol, name, exchange, previous_symbol, previous_name, next_symbol, next_name
        FROM (
            SELECT
                symbol,
                name,
                exchange,
                LAG(symbol) OVER (ORDER BY symbol) AS previous_symbol,
                LAG(name) OVER (ORDER BY symbol) AS previous_name,
                LEAD(symbol) OVER (ORDER BY symbol) AS next_symbol,
                LEAD(name) OVER (ORDER BY symbol) AS next_name
            FROM market_data.stocks
        ) AS ranked
        WHERE symbol = %s
    """
    frame = _fetch_dataframe(query, params=[normalized], dsn=dsn)
    if frame.empty:
        return {"previous": None, "next": None}
    row = frame.iloc[0].to_dict()
    previous = None
    if row.get("previous_symbol"):
        previous = {
            "symbol": row["previous_symbol"],
            "name": row["previous_name"],
            "detail_url": f"/markets/stocks/{row['previous_symbol']}",
        }
    nxt = None
    if row.get("next_symbol"):
        nxt = {
            "symbol": row["next_symbol"],
            "name": row["next_name"],
            "detail_url": f"/markets/stocks/{row['next_symbol']}",
        }
    return {"previous": previous, "next": nxt}


def list_stock_basics(dsn: str | None = None) -> list[dict[str, Any]]:
    frame = load_stocks(dsn=dsn)
    items = frame.to_dict(orient="records")
    for item in items:
        item["asset_type"] = "stock"
        item["detail_url"] = f"/markets/stocks/{item['symbol']}"
    return items

