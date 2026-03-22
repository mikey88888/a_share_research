from __future__ import annotations

from pathlib import Path
import warnings

import akshare as ak
import pandas as pd


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _normalize_symbol(symbol: str) -> str:
    return symbol.split(".")[-1].lower().replace("sh", "").replace("sz", "").replace("bj", "")


def _to_sina_symbol(symbol: str) -> str:
    normalized = _normalize_symbol(symbol)
    if symbol.startswith(("sh", "sz", "bj")):
        return symbol.lower()
    if normalized.startswith(("8", "4")):
        return f"bj{normalized}"
    if normalized.startswith("6"):
        return f"sh{normalized}"
    return f"sz{normalized}"


def _format_sina_daily(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return df

    prev_close = df["close"].shift(1).fillna(df["open"])
    formatted = pd.DataFrame(
        {
            "日期": pd.to_datetime(df["date"], errors="coerce").dt.date,
            "股票代码": _normalize_symbol(symbol),
            "开盘": pd.to_numeric(df["open"], errors="coerce"),
            "收盘": pd.to_numeric(df["close"], errors="coerce"),
            "最高": pd.to_numeric(df["high"], errors="coerce"),
            "最低": pd.to_numeric(df["low"], errors="coerce"),
            "成交量": pd.to_numeric(df["volume"], errors="coerce"),
            "成交额": pd.to_numeric(df["amount"], errors="coerce"),
            "振幅": ((df["high"] - df["low"]) / prev_close * 100).round(2),
            "涨跌幅": ((df["close"] / prev_close - 1) * 100).round(2),
            "涨跌额": (df["close"] - prev_close).round(2),
            "换手率": (pd.to_numeric(df["turnover"], errors="coerce") * 100).round(2),
        }
    )
    return formatted.dropna(subset=["日期", "开盘", "收盘"]).reset_index(drop=True)


def fetch_stock_history(
    symbol: str = "000001",
    start_date: str = "20240101",
    end_date: str = "20240201",
    adjust: str = "qfq",
) -> pd.DataFrame:
    try:
        df = ak.stock_zh_a_hist(
            symbol=_normalize_symbol(symbol),
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )
        if not df.empty:
            return df
    except Exception as exc:
        warnings.warn(
            f"Eastmoney history request failed for {symbol}, falling back to Sina: {exc}",
            stacklevel=2,
        )

    df = ak.stock_zh_a_daily(
        symbol=_to_sina_symbol(symbol),
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    return _format_sina_daily(df=df, symbol=symbol)
