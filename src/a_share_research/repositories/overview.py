from __future__ import annotations

from typing import Any

from ..db import get_latest_sync_time, load_stock_library_summary


def load_market_summary(dsn: str | None = None) -> dict[str, Any]:
    return load_stock_library_summary(dsn=dsn)


def load_market_latest_sync(dsn: str | None = None):
    return get_latest_sync_time(dsn=dsn)

