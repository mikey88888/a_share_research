from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import time

import akshare as ak
import baostock as bs
import pandas as pd
import requests

from .index_data import default_daily_end, default_daily_start, next_daily_start


STOCK_CONSTITUENT_SOURCE = "akshare:index_stock_cons_csindex"
STOCK_DAILY_SOURCE = "akshare:stock_zh_a_hist_tx"
STOCK_BAR_60M_SOURCE = "sina:CN_MarketDataService.getKLineData:60"
STOCK_BAR_60M_SOURCE_FALLBACK = "baostock:query_history_k_data_plus:60"
TRACKED_INDEX_SYMBOLS = ("000300", "000905")
SINA_INTRADAY_URLS = (
    "https://quotes.sina.cn/cn/api/jsonp_v2.php/=/CN_MarketDataService.getKLineData",
    "https://quotes.sina.cn/cn/api/jsonp_v2.php/var_{symbol}_{period}_fallback=/CN_MarketDataService.getKLineData",
)
SINA_60M_COOLDOWN_SECONDS = 30 * 60
_sina_60m_blocked_until = 0.0


class IntradaySourceBlockedError(RuntimeError):
    """Raised when the upstream intraday source blocks the current IP."""


@dataclass(frozen=True)
class StockInstrument:
    symbol: str
    exchange: str
    vendor_symbol: str
    name: str
    timezone: str = "Asia/Shanghai"


def normalize_stock_symbol(symbol: str) -> str:
    normalized = symbol.lower().replace("sh", "").replace("sz", "").strip()
    if len(normalized) != 6 or not normalized.isdigit():
        raise ValueError(f"unsupported stock symbol: {symbol!r}")
    return normalized


def infer_stock_exchange(symbol: str, exchange_name: str | None = None) -> str:
    normalized = normalize_stock_symbol(symbol)
    if exchange_name:
        if "上海" in exchange_name:
            return "SH"
        if "深圳" in exchange_name:
            return "SZ"
    if normalized.startswith("6"):
        return "SH"
    return "SZ"


def to_vendor_stock_symbol(symbol: str, exchange_name: str | None = None) -> str:
    normalized = normalize_stock_symbol(symbol)
    exchange = infer_stock_exchange(normalized, exchange_name=exchange_name)
    return f"{exchange.lower()}{normalized}"


def to_baostock_stock_symbol(symbol: str, exchange_name: str | None = None) -> str:
    normalized = normalize_stock_symbol(symbol)
    exchange = infer_stock_exchange(normalized, exchange_name=exchange_name)
    return f"{exchange.lower()}.{normalized}"


def _empty_constituent_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "index_symbol",
            "index_name",
            "stock_symbol",
            "stock_name",
            "exchange",
            "vendor_symbol",
            "as_of_date",
            "source",
        ]
    )


def _empty_stock_daily_frame() -> pd.DataFrame:
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


def _empty_stock_60m_frame() -> pd.DataFrame:
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


def _fetch_sina_stock_60m_frame(symbol: str, period: str = "60") -> pd.DataFrame:
    params = {
        "symbol": symbol,
        "scale": period,
        "ma": "no",
        "datalen": "1970",
    }
    headers = {
        "Referer": f"https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        ),
    }
    last_error: Exception | None = None

    for template in SINA_INTRADAY_URLS:
        url = template.format(symbol=symbol, period=period)
        response = requests.get(url, params=params, headers=headers, timeout=20)
        text = response.text
        if response.status_code == 456 or "拒绝访问" in text or "异常访问" in text:
            raise IntradaySourceBlockedError(
                "Sina 60m source blocked the current IP; wait 5-60 minutes before retrying."
            )

        try:
            payload = json.loads(text.split("=(")[1].split(");")[0])
            frame = pd.DataFrame(payload).iloc[:, :7]
            return frame
        except Exception as exc:  # pragma: no cover - depends on upstream payload shape
            last_error = exc

    raise RuntimeError(f"unexpected Sina 60m payload for {symbol}: {last_error}") from last_error


def _default_stock_60m_start_date(today: date | None = None) -> str:
    today = today or date.today()
    return date(today.year - 2, 1, 1).isoformat()


def _fetch_baostock_stock_60m_frame(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    login_result = bs.login()
    if login_result.error_code != "0":
        raise RuntimeError(f"baostock login failed: {login_result.error_msg}")

    try:
        result = bs.query_history_k_data_plus(
            symbol,
            "date,time,code,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency="60",
            adjustflag="3",
        )
        if result.error_code != "0":
            raise RuntimeError(f"baostock query failed: {result.error_msg}")

        rows: list[list[str]] = []
        while result.next():
            rows.append(result.get_row_data())
    finally:
        bs.logout()

    if not rows:
        return _empty_stock_60m_frame()

    return pd.DataFrame(
        rows,
        columns=["date", "time", "code", "open", "high", "low", "close", "volume", "amount"],
    )


def _build_stock_60m_frame(df: pd.DataFrame, normalized: str, *, source: str, from_baostock: bool) -> pd.DataFrame:
    if from_baostock:
        bar_time = pd.to_datetime(df["time"].astype(str).str.slice(0, 14), format="%Y%m%d%H%M%S", errors="coerce")
    else:
        bar_time = pd.to_datetime(df["day"], errors="coerce")

    intraday = pd.DataFrame(
        {
            "symbol": normalized,
            "bar_time": bar_time,
            "open": pd.to_numeric(df["open"], errors="coerce"),
            "high": pd.to_numeric(df["high"], errors="coerce"),
            "low": pd.to_numeric(df["low"], errors="coerce"),
            "close": pd.to_numeric(df["close"], errors="coerce"),
            "volume": pd.to_numeric(df["volume"], errors="coerce"),
            "amount": pd.to_numeric(df["amount"], errors="coerce"),
            "source": source,
        }
    )
    return intraday.dropna(subset=["bar_time", "open", "high", "low", "close"]).reset_index(drop=True)


def fetch_index_constituents(index_symbol: str) -> pd.DataFrame:
    df = ak.index_stock_cons_csindex(symbol=index_symbol)
    if df.empty:
        return _empty_constituent_frame()

    result = pd.DataFrame(
        {
            "index_symbol": df["指数代码"].astype(str),
            "index_name": df["指数名称"].astype(str),
            "stock_symbol": df["成分券代码"].astype(str).str.zfill(6),
            "stock_name": df["成分券名称"].astype(str),
            "exchange": [
                infer_stock_exchange(symbol, exchange_name=exchange_name)
                for symbol, exchange_name in zip(df["成分券代码"], df["交易所"], strict=True)
            ],
            "vendor_symbol": [
                to_vendor_stock_symbol(symbol, exchange_name=exchange_name)
                for symbol, exchange_name in zip(df["成分券代码"], df["交易所"], strict=True)
            ],
            "as_of_date": pd.to_datetime(df["日期"], errors="coerce").dt.date,
            "source": STOCK_CONSTITUENT_SOURCE,
        }
    )
    return result.dropna(subset=["stock_symbol", "as_of_date"]).drop_duplicates().reset_index(drop=True)


def fetch_stock_daily(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    normalized = normalize_stock_symbol(symbol)
    df = ak.stock_zh_a_hist_tx(
        symbol=to_vendor_stock_symbol(normalized),
        start_date=start_date,
        end_date=end_date,
        adjust="",
        timeout=30,
    )
    if df.empty:
        return _empty_stock_daily_frame()

    daily = pd.DataFrame(
        {
            "symbol": normalized,
            "trade_date": pd.to_datetime(df["date"], errors="coerce").dt.date,
            "open": pd.to_numeric(df["open"], errors="coerce"),
            "high": pd.to_numeric(df["high"], errors="coerce"),
            "low": pd.to_numeric(df["low"], errors="coerce"),
            "close": pd.to_numeric(df["close"], errors="coerce"),
            "volume": pd.Series([None] * len(df), dtype="object"),
            "amount": pd.to_numeric(df["amount"], errors="coerce"),
            "source": STOCK_DAILY_SOURCE,
        }
    )
    return daily.dropna(subset=["trade_date", "open", "high", "low", "close"]).reset_index(drop=True)


def fetch_stock_60m(symbol: str) -> pd.DataFrame:
    global _sina_60m_blocked_until
    normalized = normalize_stock_symbol(symbol)
    last_exc: Exception | None = None
    if time.monotonic() >= _sina_60m_blocked_until:
        for attempt in range(1, 4):
            try:
                df = _fetch_sina_stock_60m_frame(symbol=to_vendor_stock_symbol(normalized), period="60")
                if df.empty:
                    return _empty_stock_60m_frame()
                return _build_stock_60m_frame(
                    df,
                    normalized,
                    source=STOCK_BAR_60M_SOURCE,
                    from_baostock=False,
                )
            except IntradaySourceBlockedError:
                _sina_60m_blocked_until = time.monotonic() + SINA_60M_COOLDOWN_SECONDS
                break
            except Exception as exc:  # pragma: no cover - depends on remote source state
                last_exc = exc
                time.sleep(min(3.0, float(attempt)))

    start_date = _default_stock_60m_start_date()
    end_date = date.today().isoformat()
    for attempt in range(1, 4):
        try:
            df = _fetch_baostock_stock_60m_frame(
                symbol=to_baostock_stock_symbol(normalized),
                start_date=start_date,
                end_date=end_date,
            )
            if df.empty:
                return _empty_stock_60m_frame()
            return _build_stock_60m_frame(
                df,
                normalized,
                source=STOCK_BAR_60M_SOURCE_FALLBACK,
                from_baostock=True,
            )
        except Exception as exc:  # pragma: no cover - depends on remote source state
            last_exc = exc
            time.sleep(min(3.0, float(attempt)))

    raise RuntimeError(f"failed to fetch 60m bars for {normalized}: {last_exc}") from last_exc


def build_stock_master(constituents: pd.DataFrame) -> pd.DataFrame:
    if constituents.empty:
        return pd.DataFrame(columns=["symbol", "exchange", "vendor_symbol", "name", "timezone", "source"])
    stocks = (
        constituents.loc[:, ["stock_symbol", "exchange", "vendor_symbol", "stock_name", "source"]]
        .rename(columns={"stock_symbol": "symbol", "stock_name": "name"})
        .drop_duplicates(subset=["symbol"])
        .reset_index(drop=True)
    )
    stocks["timezone"] = "Asia/Shanghai"
    return stocks.loc[:, ["symbol", "exchange", "vendor_symbol", "name", "timezone", "source"]]


__all__ = [
    "STOCK_BAR_60M_SOURCE",
    "STOCK_CONSTITUENT_SOURCE",
    "STOCK_DAILY_SOURCE",
    "TRACKED_INDEX_SYMBOLS",
    "StockInstrument",
    "build_stock_master",
    "default_daily_end",
    "default_daily_start",
    "fetch_index_constituents",
    "fetch_stock_60m",
    "fetch_stock_daily",
    "infer_stock_exchange",
    "next_daily_start",
    "normalize_stock_symbol",
    "to_baostock_stock_symbol",
    "to_vendor_stock_symbol",
]
