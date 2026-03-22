from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import akshare as ak
import pandas as pd


@dataclass(frozen=True)
class IndexInstrument:
    symbol: str
    exchange: str
    vendor_symbol: str
    name: str
    timezone: str = "Asia/Shanghai"


INDEX_INSTRUMENTS: tuple[IndexInstrument, ...] = (
    IndexInstrument(symbol="000300", exchange="SH", vendor_symbol="sh000300", name="沪深300"),
    IndexInstrument(symbol="000905", exchange="SH", vendor_symbol="sh000905", name="中证500"),
)

INDEX_BY_SYMBOL = {item.symbol: item for item in INDEX_INSTRUMENTS}
DAILY_SOURCE = "akshare:stock_zh_a_hist_tx"
BAR_60M_SOURCE = "akshare:stock_zh_a_minute:60"


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.lower().replace("sh", "").replace("sz", "").strip()
    if normalized not in INDEX_BY_SYMBOL:
        raise ValueError(f"unsupported index symbol: {symbol!r}")
    return normalized


def get_instrument(symbol: str) -> IndexInstrument:
    return INDEX_BY_SYMBOL[normalize_symbol(symbol)]


def default_daily_start(today: date | None = None) -> str:
    today = today or date.today()
    return f"{today.year - 10}0101"


def default_daily_end(today: date | None = None) -> str:
    today = today or date.today()
    return today.strftime("%Y%m%d")


def next_daily_start(last_trade_date: date | None, today: date | None = None) -> str:
    if last_trade_date is None:
        return default_daily_start(today=today)
    return (last_trade_date + timedelta(days=1)).strftime("%Y%m%d")


def _empty_daily_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "source",
        ]
    )


def _empty_60m_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "bar_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "source",
        ]
    )


def fetch_index_daily(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    instrument = get_instrument(symbol)
    df = ak.stock_zh_a_hist_tx(
        symbol=instrument.vendor_symbol,
        start_date=start_date,
        end_date=end_date,
        adjust="",
        timeout=30,
    )
    if df.empty:
        return _empty_daily_frame()

    daily = pd.DataFrame(
        {
            "symbol": instrument.symbol,
            "trade_date": pd.to_datetime(df["date"], errors="coerce").dt.date,
            "open": pd.to_numeric(df["open"], errors="coerce"),
            "high": pd.to_numeric(df["high"], errors="coerce"),
            "low": pd.to_numeric(df["low"], errors="coerce"),
            "close": pd.to_numeric(df["close"], errors="coerce"),
            "volume": pd.Series([None] * len(df), dtype="object"),
            "amount": pd.to_numeric(df["amount"], errors="coerce"),
            "source": DAILY_SOURCE,
        }
    )
    return daily.dropna(subset=["trade_date", "open", "high", "low", "close"]).reset_index(drop=True)


def fetch_index_60m(symbol: str) -> pd.DataFrame:
    instrument = get_instrument(symbol)
    df = ak.stock_zh_a_minute(symbol=instrument.vendor_symbol, period="60", adjust="")
    if df.empty:
        return _empty_60m_frame()

    intraday = pd.DataFrame(
        {
            "symbol": instrument.symbol,
            "bar_time": pd.to_datetime(df["day"], errors="coerce"),
            "open": pd.to_numeric(df["open"], errors="coerce"),
            "high": pd.to_numeric(df["high"], errors="coerce"),
            "low": pd.to_numeric(df["low"], errors="coerce"),
            "close": pd.to_numeric(df["close"], errors="coerce"),
            "volume": pd.to_numeric(df["volume"], errors="coerce"),
            "amount": pd.to_numeric(df["amount"], errors="coerce"),
            "source": BAR_60M_SOURCE,
        }
    )
    return intraday.dropna(subset=["bar_time", "open", "high", "low", "close"]).reset_index(drop=True)
