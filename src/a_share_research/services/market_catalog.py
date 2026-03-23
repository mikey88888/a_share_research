from __future__ import annotations

from typing import Any

from ..domain.market import AssetIdentity, AssetType
from ..index_data import get_instrument
from ..repositories.indexes import list_index_cards
from ..repositories.stocks import get_stock_profile, list_stock_basics
from ..stock_data import normalize_stock_symbol


def resolve_asset(symbol: str, dsn: str | None = None) -> AssetIdentity:
    try:
        instrument = get_instrument(symbol)
    except ValueError:
        instrument = None
    if instrument is not None:
        return AssetIdentity(
            asset_type=AssetType.INDEX,
            symbol=instrument.symbol,
            name=instrument.name,
            exchange=instrument.exchange,
            vendor_symbol=instrument.vendor_symbol,
            timezone=instrument.timezone,
        )

    normalized = normalize_stock_symbol(symbol)
    stock = get_stock_profile(normalized, dsn=dsn)
    if stock is None:
        raise ValueError(f"unsupported symbol: {symbol!r}")
    return AssetIdentity(
        asset_type=AssetType.STOCK,
        symbol=stock["symbol"],
        name=stock["name"],
        exchange=stock["exchange"],
        vendor_symbol=stock["vendor_symbol"],
        timezone=stock["timezone"],
    )


def list_all_assets(dsn: str | None = None) -> list[dict[str, Any]]:
    assets = [
        {
            "asset_type": "index",
            "symbol": item["symbol"],
            "name": item["name"],
            "exchange": item["exchange"],
            "vendor_symbol": item["vendor_symbol"],
            "timezone": item["timezone"],
            "detail_url": item["detail_url"],
        }
        for item in list_index_cards(dsn=dsn)
    ]
    assets.extend(list_stock_basics(dsn=dsn))
    return assets

