from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import pandas as pd
from fastapi import HTTPException

from ..db import load_bar_1d, load_bar_60m, load_stock_bar_1d, load_stock_bar_60m
from ..domain.market import AssetType
from ..repositories.indexes import get_index_card, list_index_cards
from ..repositories.stocks import get_stock_neighbors, get_stock_profile

TIMEFRAME_DEFAULT_RANGE = {"1d": "1y", "60m": "3m"}
TIMEFRAME_RANGE_OPTIONS = {
    "1d": {"3m": 90, "6m": 180, "1y": 365, "3y": 365 * 3, "max": None},
    "60m": {"1w": 7, "1m": 30, "3m": 90, "6m": 180, "1y": 365, "max": None},
}
TIMEFRAME_LABELS = {"1d": "日线", "60m": "60分钟"}
MAX_RETURNED_ROWS = {"1d": 5000, "60m": 3000}


def coerce_timeframe(value: str) -> str:
    if value not in TIMEFRAME_LABELS:
        raise HTTPException(status_code=400, detail=f"invalid timeframe {value!r}")
    return value


def coerce_range(timeframe: str, value: str | None) -> str:
    fallback = TIMEFRAME_DEFAULT_RANGE[timeframe]
    if value is None:
        return fallback
    if value not in TIMEFRAME_RANGE_OPTIONS[timeframe]:
        raise HTTPException(status_code=400, detail=f"invalid range {value!r} for timeframe {timeframe!r}")
    return value


def parse_api_date(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value)


def parse_api_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _load_frame(symbol: str, asset_type: AssetType, timeframe: str) -> pd.DataFrame:
    if asset_type is AssetType.INDEX:
        if timeframe == "1d":
            return load_bar_1d(symbol)
        return load_bar_60m(symbol)
    if timeframe == "1d":
        return load_stock_bar_1d(symbol)
    return load_stock_bar_60m(symbol)


def _filter_frame_for_api(
    symbol: str,
    asset_type: AssetType,
    timeframe: str,
    *,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    if asset_type is AssetType.INDEX:
        if timeframe == "1d":
            return load_bar_1d(symbol, start_date=parse_api_date(start), end_date=parse_api_date(end))
        return load_bar_60m(symbol, start_time=parse_api_datetime(start), end_time=parse_api_datetime(end))
    if timeframe == "1d":
        return load_stock_bar_1d(symbol, start_date=parse_api_date(start), end_date=parse_api_date(end))
    return load_stock_bar_60m(symbol, start_time=parse_api_datetime(start), end_time=parse_api_datetime(end))


def cut_frame_by_range(df: pd.DataFrame, timeframe: str, range_key: str) -> pd.DataFrame:
    if df.empty:
        return df
    window_days = TIMEFRAME_RANGE_OPTIONS[timeframe][range_key]
    if window_days is None:
        result = df.copy()
    else:
        time_col = "trade_date" if timeframe == "1d" else "bar_time"
        latest = pd.Timestamp(df[time_col].max())
        start = latest - pd.Timedelta(days=window_days)
        result = df[df[time_col] >= start.date()] if timeframe == "1d" else df[df[time_col] >= start]
    max_rows = MAX_RETURNED_ROWS[timeframe]
    if len(result) > max_rows:
        result = result.tail(max_rows)
    return result.reset_index(drop=True)


def _format_bar_time(value: Any, timeframe: str) -> str:
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    if isinstance(value, date):
        if timeframe == "1d":
            return value.isoformat()
        return datetime.combine(value, datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%S+08:00")
    return str(value)


def to_chart_payload(df: pd.DataFrame, timeframe: str) -> list[dict[str, Any]]:
    bars: list[dict[str, Any]] = []
    time_col = "trade_date" if timeframe == "1d" else "bar_time"
    for row in df.itertuples(index=False):
        bars.append(
            {
                "time": _format_bar_time(getattr(row, time_col), timeframe),
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "volume": None if pd.isna(getattr(row, "volume", None)) else float(row.volume),
                "amount": None if pd.isna(getattr(row, "amount", None)) else float(row.amount),
            }
        )
    return bars


def _common_detail_fields(df: pd.DataFrame, timeframe: str) -> dict[str, Any]:
    display_df = df
    table_df = display_df.tail(15).iloc[::-1].reset_index(drop=True)
    chart_bars = to_chart_payload(display_df, timeframe)

    latest_row = display_df.iloc[-1] if not display_df.empty else None
    previous_row = display_df.iloc[-2] if len(display_df) > 1 else None
    latest_close = float(latest_row["close"]) if latest_row is not None else None
    change_pct = None
    if latest_row is not None and previous_row is not None and previous_row["close"]:
        change_pct = ((float(latest_row["close"]) / float(previous_row["close"])) - 1.0) * 100.0

    time_col = "trade_date" if timeframe == "1d" else "bar_time"
    start_label = display_df.iloc[0][time_col] if not display_df.empty else None
    end_label = display_df.iloc[-1][time_col] if not display_df.empty else None
    latest_sync = display_df["fetched_at"].max() if not display_df.empty and "fetched_at" in display_df.columns else None

    return {
        "display_df": display_df,
        "table_rows": table_df.to_dict(orient="records"),
        "chart_bars": chart_bars,
        "chart_bars_json": json.dumps(chart_bars, ensure_ascii=False),
        "latest_close": latest_close,
        "change_pct": change_pct,
        "display_count": len(display_df),
        "latest_sync": latest_sync,
        "start_label": start_label,
        "end_label": end_label,
    }


def build_asset_detail_context(
    symbol: str,
    *,
    asset_type: AssetType,
    timeframe: str,
    range_key: str | None,
    dsn: str | None = None,
) -> dict[str, Any]:
    timeframe = coerce_timeframe(timeframe)
    range_key = coerce_range(timeframe, range_key)

    if asset_type is AssetType.INDEX:
        asset = get_index_card(symbol, dsn=dsn)
        frame = _load_frame(asset["symbol"], asset_type, timeframe)
        related = list_index_cards(dsn=dsn)
        detail_path = f"/markets/indexes/{asset['symbol']}"
        context = {
            "asset": asset,
            "asset_type": asset_type.value,
            "asset_type_label": "指数",
            "detail_path": detail_path,
            "back_path": "/markets/indexes",
            "back_label": "返回指数列表",
            "timeframe": timeframe,
            "timeframe_label": TIMEFRAME_LABELS[timeframe],
            "timeframe_options": TIMEFRAME_LABELS,
            "range_key": range_key,
            "range_options": list(TIMEFRAME_RANGE_OPTIONS[timeframe].keys()),
            "default_ranges": TIMEFRAME_DEFAULT_RANGE,
            "related_assets": related,
            "page_title": f"{asset['name']} · {TIMEFRAME_LABELS[timeframe]}",
            "is_index": True,
            "is_stock": False,
            "stock_navigation": None,
        }
    else:
        asset = get_stock_profile(symbol, dsn=dsn)
        if asset is None:
            raise ValueError(f"unsupported stock symbol: {symbol!r}")
        frame = _load_frame(asset["symbol"], asset_type, timeframe)
        detail_path = f"/markets/stocks/{asset['symbol']}"
        neighbors = get_stock_neighbors(asset["symbol"], dsn=dsn)
        context = {
            "asset": asset,
            "asset_type": asset_type.value,
            "asset_type_label": "个股",
            "detail_path": detail_path,
            "back_path": "/markets/stocks",
            "back_label": "返回股票列表",
            "timeframe": timeframe,
            "timeframe_label": TIMEFRAME_LABELS[timeframe],
            "timeframe_options": TIMEFRAME_LABELS,
            "range_key": range_key,
            "range_options": list(TIMEFRAME_RANGE_OPTIONS[timeframe].keys()),
            "default_ranges": TIMEFRAME_DEFAULT_RANGE,
            "related_assets": None,
            "page_title": f"{asset['name']} · {TIMEFRAME_LABELS[timeframe]}",
            "is_index": False,
            "is_stock": True,
            "stock_navigation": neighbors,
        }

    display_df = cut_frame_by_range(frame, timeframe, range_key)
    context.update(_common_detail_fields(display_df, timeframe))
    return context


def build_api_bars_payload(
    symbol: str,
    *,
    asset_type: AssetType,
    timeframe: str,
    start: str | None = None,
    end: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    timeframe = coerce_timeframe(timeframe)
    frame = _filter_frame_for_api(symbol, asset_type, timeframe, start=start, end=end)
    max_rows = limit or MAX_RETURNED_ROWS[timeframe]
    if len(frame) > max_rows:
        frame = frame.tail(max_rows)
    return to_chart_payload(frame.reset_index(drop=True), timeframe)

