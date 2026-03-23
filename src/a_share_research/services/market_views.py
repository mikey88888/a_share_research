from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from ..repositories.indexes import list_index_cards
from ..repositories.overview import load_market_latest_sync, load_market_summary
from ..repositories.stocks import search_stocks

STOCK_SORT_LABELS = {
    "symbol": "代码",
    "name": "名称",
    "latest_trade": "最新日线",
    "latest_60m": "最新60分钟",
}


def _latest_sync_from_overview(index_cards: list[dict[str, Any]], stock_summary: dict[str, Any] | None):
    values = [item.get("latest_sync_at") for item in index_cards if item.get("latest_sync_at") is not None]
    if stock_summary and stock_summary.get("latest_sync_at") is not None:
        values.append(stock_summary["latest_sync_at"])
    return max(values) if values else None


def build_market_home_context(*, dsn: str | None = None) -> dict[str, Any]:
    index_cards = list_index_cards(dsn=dsn)
    stock_summary = load_market_summary(dsn=dsn)
    featured_stocks = search_stocks(page=1, page_size=12, dsn=dsn)
    latest_sync = _latest_sync_from_overview(index_cards, stock_summary) or load_market_latest_sync(dsn=dsn)
    return {
        "page_title": "统一市场看板",
        "latest_sync_at": latest_sync,
        "index_cards": index_cards,
        "stock_summary": stock_summary,
        "featured_stocks": featured_stocks,
        "sort_labels": STOCK_SORT_LABELS,
    }


def build_index_list_context(*, dsn: str | None = None) -> dict[str, Any]:
    index_cards = list_index_cards(dsn=dsn)
    return {
        "page_title": "指数研究",
        "index_cards": index_cards,
        "latest_sync_at": max(
            [item.get("latest_sync_at") for item in index_cards if item.get("latest_sync_at") is not None],
            default=None,
        ),
    }


def build_stock_list_context(
    *,
    q: str | None = None,
    exchange: str = "ALL",
    sort: str = "symbol",
    page: int = 1,
    page_size: int = 50,
    dsn: str | None = None,
) -> dict[str, Any]:
    result = search_stocks(q=q, exchange=exchange, sort=sort, page=page, page_size=page_size, dsn=dsn)
    result.update(
        {
            "page_title": "股票研究",
            "exchange_options": [
                {"value": "ALL", "label": "全部"},
                {"value": "SH", "label": "上海"},
                {"value": "SZ", "label": "深圳"},
            ],
            "sort_options": [
                {"value": key, "label": label}
                for key, label in STOCK_SORT_LABELS.items()
            ],
            "query_string": (q or "").strip(),
            "encoded_query_string": quote_plus((q or "").strip()),
        }
    )
    return result
