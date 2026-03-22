from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .db import load_bar_1d, load_bar_60m, load_dashboard_cards
from .index_data import INDEX_INSTRUMENTS, get_instrument, normalize_symbol
from .sync_index_data import sync_all

TIMEFRAME_DEFAULT_RANGE = {"1d": "1y", "60m": "3m"}
TIMEFRAME_RANGE_OPTIONS = {
    "1d": {"3m": 90, "6m": 180, "1y": 365, "3y": 365 * 3, "max": None},
    "60m": {"1w": 7, "1m": 30, "3m": 90, "6m": 180, "1y": 365, "max": None},
}
TIMEFRAME_LABELS = {"1d": "日线", "60m": "60分钟"}
MAX_RETURNED_ROWS = {"1d": 5000, "60m": 3000}
PACKAGE_ROOT = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(PACKAGE_ROOT / "templates"))
STATIC_ROOT = PACKAGE_ROOT / "static"


def _coerce_timeframe(value: str) -> str:
    if value not in TIMEFRAME_LABELS:
        raise HTTPException(status_code=400, detail=f"invalid timeframe {value!r}")
    return value


def _coerce_range(timeframe: str, value: str | None) -> str:
    fallback = TIMEFRAME_DEFAULT_RANGE[timeframe]
    if value is None:
        return fallback
    if value not in TIMEFRAME_RANGE_OPTIONS[timeframe]:
        raise HTTPException(status_code=400, detail=f"invalid range {value!r} for timeframe {timeframe!r}")
    return value


def _parse_api_date(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value)


def _parse_api_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _frame_for_timeframe(symbol: str, timeframe: str) -> pd.DataFrame:
    if timeframe == "1d":
        return load_bar_1d(symbol)
    return load_bar_60m(symbol)


def _cut_frame_by_range(df: pd.DataFrame, timeframe: str, range_key: str) -> pd.DataFrame:
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


def _to_chart_payload(df: pd.DataFrame, timeframe: str) -> list[dict[str, Any]]:
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


def _overview_cards() -> list[dict[str, Any]]:
    cards = load_dashboard_cards()
    if cards.empty:
        return []
    records = cards.to_dict(orient="records")
    for item in records:
        item["detail_url"] = f"/instruments/{item['symbol']}"
    return records


def _latest_sync_from_cards(cards: list[dict[str, Any]]) -> Any:
    values = [item.get("latest_sync_at") for item in cards if item.get("latest_sync_at") is not None]
    return max(values) if values else None


def _build_instrument_context(symbol: str, timeframe: str, range_key: str) -> dict[str, Any]:
    instrument = get_instrument(symbol)
    timeframe = _coerce_timeframe(timeframe)
    range_key = _coerce_range(timeframe, range_key)
    df = _frame_for_timeframe(instrument.symbol, timeframe)
    display_df = _cut_frame_by_range(df, timeframe, range_key)
    table_df = display_df.tail(15).iloc[::-1].reset_index(drop=True)
    chart_bars = _to_chart_payload(display_df, timeframe)

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
        "instrument": instrument,
        "instrument_list": INDEX_INSTRUMENTS,
        "timeframe": timeframe,
        "timeframe_label": TIMEFRAME_LABELS[timeframe],
        "timeframe_options": TIMEFRAME_LABELS,
        "range_key": range_key,
        "range_options": list(TIMEFRAME_RANGE_OPTIONS[timeframe].keys()),
        "default_ranges": TIMEFRAME_DEFAULT_RANGE,
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
        "page_title": f"{instrument.name} · {TIMEFRAME_LABELS[timeframe]}",
    }


def create_app() -> FastAPI:
    app = FastAPI(title="A-Share Research Dashboard")
    app.mount("/static", StaticFiles(directory=str(STATIC_ROOT)), name="static")

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request) -> HTMLResponse:
        error = None
        cards: list[dict[str, Any]] = []
        try:
            cards = _overview_cards()
        except Exception as exc:  # pragma: no cover - defensive rendering path
            error = str(exc)
        context = {
            "request": request,
            "cards": cards,
            "latest_sync_at": _latest_sync_from_cards(cards),
            "page_title": "量化研究看板",
            "error": error,
            "message": None,
        }
        return TEMPLATES.TemplateResponse("index.html", context)

    @app.post("/actions/refresh", response_class=HTMLResponse)
    def refresh_data(request: Request) -> HTMLResponse:
        message = "数据刷新完成"
        error = None
        try:
            sync_all("refresh")
        except Exception as exc:  # pragma: no cover - surface operational issue to UI
            message = "数据刷新失败"
            error = str(exc)
        cards = []
        if error is None:
            cards = _overview_cards()
        context = {
            "request": request,
            "cards": cards,
            "latest_sync_at": _latest_sync_from_cards(cards),
            "message": message,
            "error": error,
        }
        return TEMPLATES.TemplateResponse("partials/index_content.html", context)

    @app.get("/instruments/{symbol}", response_class=HTMLResponse)
    def instrument_detail(
        request: Request,
        symbol: str,
        timeframe: str = Query("1d"),
        range_key: str | None = Query(None, alias="range"),
    ) -> HTMLResponse:
        try:
            context = _build_instrument_context(symbol, timeframe=timeframe, range_key=range_key)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        context["request"] = request
        is_htmx = request.headers.get("HX-Request", "").lower() == "true"
        template_name = "partials/instrument_panel.html" if is_htmx else "instrument.html"
        return TEMPLATES.TemplateResponse(template_name, context)

    @app.get("/api/instruments", response_class=JSONResponse)
    def api_instruments() -> list[dict[str, str]]:
        return [
            {
                "symbol": item.symbol,
                "exchange": item.exchange,
                "vendor_symbol": item.vendor_symbol,
                "name": item.name,
                "timezone": item.timezone,
            }
            for item in INDEX_INSTRUMENTS
        ]

    @app.get("/api/bars", response_class=JSONResponse)
    def api_bars(
        symbol: str,
        timeframe: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = Query(None, ge=1, le=10000),
    ) -> list[dict[str, Any]]:
        timeframe = _coerce_timeframe(timeframe)
        try:
            symbol = normalize_symbol(symbol)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        if timeframe == "1d":
            df = load_bar_1d(symbol, start_date=_parse_api_date(start), end_date=_parse_api_date(end))
        else:
            df = load_bar_60m(symbol, start_time=_parse_api_datetime(start), end_time=_parse_api_datetime(end))

        max_rows = limit or MAX_RETURNED_ROWS[timeframe]
        if len(df) > max_rows:
            df = df.tail(max_rows)
        return _to_chart_payload(df.reset_index(drop=True), timeframe)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the A-share research dashboard")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    uvicorn.run(
        "a_share_research.webapp:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
