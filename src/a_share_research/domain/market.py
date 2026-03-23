from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class AssetType(StrEnum):
    INDEX = "index"
    STOCK = "stock"


@dataclass(frozen=True)
class AssetIdentity:
    asset_type: AssetType
    symbol: str
    name: str
    exchange: str
    vendor_symbol: str
    timezone: str = "Asia/Shanghai"


def normalize_exchange_filter(value: str | None) -> str:
    if not value:
        return "ALL"
    normalized = value.upper()
    if normalized not in {"ALL", "SH", "SZ"}:
        return "ALL"
    return normalized

