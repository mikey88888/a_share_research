from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ...domain.market import AssetType
from ...services.market_catalog import resolve_asset
from ...services.market_views import build_index_list_context, build_market_home_context, build_stock_list_context
from ...services.research import build_asset_detail_context
from ...sync_index_data import sync_all
from ..templating import TEMPLATES, is_htmx_request

router = APIRouter()


def _render(request: Request, *, page_template: str, partial_template: str, context: dict) -> HTMLResponse:
    context = {"request": request, **context}
    template_name = partial_template if is_htmx_request(request) else page_template
    return TEMPLATES.TemplateResponse(request, template_name, context)


@router.get("/", response_class=HTMLResponse)
@router.get("/markets", response_class=HTMLResponse)
def market_home(request: Request) -> HTMLResponse:
    context = build_market_home_context()
    return _render(
        request,
        page_template="pages/market_home.html",
        partial_template="partials/market_home_content.html",
        context=context,
    )


@router.post("/actions/refresh", response_class=HTMLResponse)
def refresh_data(request: Request) -> HTMLResponse:
    context = build_market_home_context()
    context["message"] = "指数数据刷新完成"
    context["error"] = None
    try:
        sync_all("refresh")
        context = build_market_home_context()
        context["message"] = "指数数据刷新完成"
        context["error"] = None
    except Exception as exc:  # pragma: no cover - surface operational issue to UI
        context["message"] = "数据刷新失败"
        context["error"] = str(exc)
    return _render(
        request,
        page_template="pages/market_home.html",
        partial_template="partials/market_home_content.html",
        context=context,
    )


@router.get("/markets/indexes", response_class=HTMLResponse)
def index_list(request: Request) -> HTMLResponse:
    context = build_index_list_context()
    return _render(
        request,
        page_template="pages/index_list.html",
        partial_template="partials/index_list_panel.html",
        context=context,
    )


@router.get("/markets/stocks", response_class=HTMLResponse)
def stock_list(
    request: Request,
    q: str | None = None,
    exchange: str = Query("ALL"),
    sort: str = Query("symbol"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
) -> HTMLResponse:
    context = build_stock_list_context(q=q, exchange=exchange, sort=sort, page=page, page_size=page_size)
    return _render(
        request,
        page_template="pages/stock_list.html",
        partial_template="partials/stock_list_panel.html",
        context=context,
    )


@router.get("/markets/indexes/{symbol}", response_class=HTMLResponse)
def index_detail(
    request: Request,
    symbol: str,
    timeframe: str = Query("1d"),
    range_key: str | None = Query(None, alias="range"),
) -> HTMLResponse:
    try:
        context = build_asset_detail_context(
            symbol,
            asset_type=AssetType.INDEX,
            timeframe=timeframe,
            range_key=range_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _render(
        request,
        page_template="pages/asset_detail.html",
        partial_template="partials/asset_detail_panel.html",
        context=context,
    )


@router.get("/markets/stocks/{symbol}", response_class=HTMLResponse)
def stock_detail(
    request: Request,
    symbol: str,
    timeframe: str = Query("1d"),
    range_key: str | None = Query(None, alias="range"),
) -> HTMLResponse:
    try:
        context = build_asset_detail_context(
            symbol,
            asset_type=AssetType.STOCK,
            timeframe=timeframe,
            range_key=range_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _render(
        request,
        page_template="pages/asset_detail.html",
        partial_template="partials/asset_detail_panel.html",
        context=context,
    )


@router.get("/instruments/{symbol}")
def instrument_redirect(
    symbol: str,
    timeframe: str = Query("1d"),
    range_key: str | None = Query(None, alias="range"),
):
    try:
        asset = resolve_asset(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    route = "/markets/indexes" if asset.asset_type is AssetType.INDEX else "/markets/stocks"
    query = f"?timeframe={timeframe}"
    if range_key:
        query += f"&range={range_key}"
    return RedirectResponse(url=f"{route}/{asset.symbol}{query}", status_code=307)
