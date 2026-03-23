from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ...domain.market import AssetType
from ...repositories.indexes import list_index_cards
from ...repositories.stocks import search_stock_suggestions
from ...services.market_catalog import list_all_assets, resolve_asset
from ...services.research import build_api_bars_payload

router = APIRouter(prefix="/api")


@router.get("/indexes", response_class=JSONResponse)
def api_indexes() -> list[dict[str, Any]]:
    return list_index_cards()


@router.get("/stocks/search", response_class=JSONResponse)
def api_stock_search(
    q: str = Query(""),
    limit: int = Query(10, ge=1, le=20),
) -> list[dict[str, Any]]:
    return search_stock_suggestions(q=q, limit=limit)


@router.get("/instruments", response_class=JSONResponse)
def api_instruments() -> list[dict[str, Any]]:
    return list_all_assets()


@router.get("/bars", response_class=JSONResponse)
def api_bars(
    symbol: str,
    asset_type: str = Query("index"),
    timeframe: str = Query("1d"),
    start: str | None = None,
    end: str | None = None,
    limit: int | None = Query(None, ge=1, le=10000),
) -> list[dict[str, Any]]:
    try:
        resolved = resolve_asset(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        requested_type = AssetType(asset_type)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid asset_type {asset_type!r}") from exc
    if requested_type is not resolved.asset_type:
        raise HTTPException(status_code=400, detail="asset_type does not match symbol")
    return build_api_bars_payload(
        resolved.symbol,
        asset_type=requested_type,
        timeframe=timeframe,
        start=start,
        end=end,
        limit=limit,
    )
