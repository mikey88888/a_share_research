from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..db import load_dashboard_cards, load_index_constituents
from ..index_data import INDEX_INSTRUMENTS, get_instrument, normalize_symbol as normalize_index_symbol


def _index_constituent_counts(dsn: str | None = None) -> dict[str, int]:
    frame = load_index_constituents(dsn=dsn)
    if frame.empty:
        return {}
    return frame.groupby("index_symbol")["stock_symbol"].nunique().astype(int).to_dict()


def list_index_cards(dsn: str | None = None) -> list[dict[str, Any]]:
    frame = load_dashboard_cards(dsn=dsn)
    by_symbol = {
        row["symbol"]: row
        for row in frame.to_dict(orient="records")
    }
    constituent_counts = _index_constituent_counts(dsn=dsn)

    cards: list[dict[str, Any]] = []
    for instrument in INDEX_INSTRUMENTS:
        record = dict(by_symbol.get(instrument.symbol, {}))
        record.update(
            {
                "symbol": instrument.symbol,
                "exchange": instrument.exchange,
                "vendor_symbol": instrument.vendor_symbol,
                "name": instrument.name,
                "timezone": instrument.timezone,
                "constituent_count": constituent_counts.get(instrument.symbol, 0),
                "detail_url": f"/markets/indexes/{instrument.symbol}",
                "asset_type": "index",
            }
        )
        cards.append(record)
    return cards


def get_index_card(symbol: str, dsn: str | None = None) -> dict[str, Any]:
    normalized = normalize_index_symbol(symbol)
    for item in list_index_cards(dsn=dsn):
        if item["symbol"] == normalized:
            return item
    instrument = get_instrument(normalized)
    return {
        "symbol": instrument.symbol,
        "exchange": instrument.exchange,
        "vendor_symbol": instrument.vendor_symbol,
        "name": instrument.name,
        "timezone": instrument.timezone,
        "constituent_count": 0,
        "detail_url": f"/markets/indexes/{instrument.symbol}",
        "asset_type": "index",
    }


def list_index_symbols() -> Iterable[str]:
    return [item.symbol for item in INDEX_INSTRUMENTS]

