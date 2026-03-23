from __future__ import annotations

import argparse
import time
from collections.abc import Iterable
from datetime import date, timedelta

import pandas as pd

from .db import (
    connect_pg,
    ensure_schema,
    get_latest_stock_trade_date,
    load_stock_bar_60m_status,
    load_current_stock_symbols,
    replace_index_constituents_current,
    sync_instruments,
    upsert_stock_bar_1d,
    upsert_stock_bar_60m,
    upsert_stocks,
)
from .stock_data import (
    IntradaySourceBlockedError,
    TRACKED_INDEX_SYMBOLS,
    build_stock_master,
    default_daily_end,
    default_daily_start,
    fetch_index_constituents,
    fetch_stock_60m,
    fetch_stock_daily,
    next_daily_start,
    normalize_stock_symbol,
)


DAILY_BATCH_SIZE = 20
INTRADAY_BATCH_SIZE = 10
BATCH_SLEEP_SECONDS = 0.2
INTRADAY_TARGET_ROWS = 1970
INTRADAY_FRESHNESS_LAG_DAYS = 5


def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _normalize_symbol_args(symbols: list[str] | None) -> list[str] | None:
    if not symbols:
        return None
    expanded: list[str] = []
    for item in symbols:
        expanded.extend(part.strip() for part in item.split(",") if part.strip())
    return [normalize_stock_symbol(item) for item in expanded]


def sync_stock_universe(conn) -> pd.DataFrame:
    frames = [fetch_index_constituents(index_symbol) for index_symbol in TRACKED_INDEX_SYMBOLS]
    constituents = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
    stocks = build_stock_master(constituents)
    upsert_stocks(conn, stocks)
    replace_index_constituents_current(conn, constituents)
    return constituents


def _sync_daily_batch(conn, symbols: list[str], mode: str, today: date) -> tuple[int, list[str]]:
    success_rows = 0
    failures: list[str] = []
    daily_end = default_daily_end(today=today)

    for symbol in symbols:
        try:
            if mode == "init":
                start_date = default_daily_start(today=today)
            else:
                start_date = next_daily_start(get_latest_stock_trade_date(conn, symbol), today=today)
            if start_date > daily_end:
                continue
            df = fetch_stock_daily(symbol, start_date=start_date, end_date=daily_end)
            success_rows += upsert_stock_bar_1d(conn, df)
        except Exception as exc:
            print(f"daily failure symbol={symbol} error={exc}")
            failures.append(symbol)
    return success_rows, failures


def _sync_intraday_batch(conn, symbols: list[str]) -> tuple[int, list[str]]:
    success_rows = 0
    failures: list[str] = []
    for symbol in symbols:
        try:
            df = fetch_stock_60m(symbol)
            success_rows += upsert_stock_bar_60m(conn, df)
        except IntradaySourceBlockedError:
            raise
        except Exception as exc:
            print(f"60m failure symbol={symbol} error={exc}")
            failures.append(symbol)
    return success_rows, failures


def _intraday_is_complete(row, *, min_rows: int, min_latest_date: date) -> bool:
    earliest_bar_time = getattr(row, "earliest_bar_time", None)
    first_trade_date = getattr(row, "first_trade_date", None)
    latest_bar_time = getattr(row, "latest_bar_time", None)
    latest_bar_date = None
    if latest_bar_time is not None and not pd.isna(latest_bar_time):
        latest_bar_date = latest_bar_time.date()
    intraday_rows = int(getattr(row, "intraday_rows", 0) or 0)
    is_fresh = latest_bar_date is not None and latest_bar_date >= min_latest_date
    if intraday_rows >= min_rows and is_fresh:
        return True
    if first_trade_date is None or earliest_bar_time is None or pd.isna(earliest_bar_time) or not is_fresh:
        return False
    return earliest_bar_time.date() <= first_trade_date


def _select_intraday_gap_symbols(
    conn,
    symbols: list[str],
    *,
    min_rows: int,
    min_latest_date: date,
) -> tuple[list[str], pd.DataFrame]:
    status = load_stock_bar_60m_status(conn, symbols=symbols)
    if status.empty:
        return symbols, status

    missing_symbols = [
        row.symbol
        for row in status.itertuples(index=False)
        if not _intraday_is_complete(row, min_rows=min_rows, min_latest_date=min_latest_date)
    ]
    return missing_symbols, status


def sync_stock_data(
    mode: str,
    symbols: list[str] | None = None,
    skip_universe: bool = False,
    intraday_retry_rounds: int = 1,
    intraday_target_rows: int = INTRADAY_TARGET_ROWS,
    intraday_only: bool = False,
) -> None:
    today = date.today()
    requested_symbols = _normalize_symbol_args(symbols)

    with connect_pg() as conn:
        ensure_schema(conn)
        sync_instruments(conn)
        if skip_universe:
            constituents = pd.DataFrame()
            universe_symbols = load_current_stock_symbols(conn)
        else:
            constituents = sync_stock_universe(conn)
            universe_symbols = sorted(constituents["stock_symbol"].drop_duplicates().tolist())

        if requested_symbols is not None:
            unknown = sorted(set(requested_symbols) - set(universe_symbols))
            if unknown:
                raise ValueError(f"symbols not in current index universe: {unknown}")
            target_symbols = requested_symbols
        else:
            target_symbols = universe_symbols

        print(
            f"universe_rows={len(constituents)} unique_stocks={len(universe_symbols)} "
            f"target_stocks={len(target_symbols)} skip_universe={skip_universe}"
        )

        daily_failures: list[str] = []
        intraday_failures: list[str] = []
        total_daily_rows = 0
        total_intraday_rows = 0

        if not intraday_only:
            daily_batches = list(_chunked(target_symbols, DAILY_BATCH_SIZE))
            for idx, batch in enumerate(daily_batches, start=1):
                batch_rows, batch_failures = _sync_daily_batch(conn, batch, mode=mode, today=today)
                total_daily_rows += batch_rows
                daily_failures.extend(batch_failures)
                print(
                    f"daily batch {idx}/{len(daily_batches)} symbols={len(batch)} "
                    f"rows={batch_rows} failures={len(batch_failures)}"
                )
                time.sleep(BATCH_SLEEP_SECONDS)

        min_latest_date = today - timedelta(days=INTRADAY_FRESHNESS_LAG_DAYS)
        intraday_source_blocked = False
        for round_idx in range(1, intraday_retry_rounds + 1):
            remaining_symbols, status = _select_intraday_gap_symbols(
                conn,
                target_symbols,
                min_rows=intraday_target_rows,
                min_latest_date=min_latest_date,
            )
            complete_count = len(status) - len(remaining_symbols) if not status.empty else 0
            print(
                f"60m round {round_idx}/{intraday_retry_rounds} "
                f"complete={complete_count} remaining={len(remaining_symbols)} "
                f"target_rows={intraday_target_rows} freshness>={min_latest_date.isoformat()}"
            )
            if not remaining_symbols:
                break

            intraday_batches = list(_chunked(remaining_symbols, INTRADAY_BATCH_SIZE))
            round_failures: list[str] = []
            round_rows = 0
            for idx, batch in enumerate(intraday_batches, start=1):
                try:
                    batch_rows, batch_failures = _sync_intraday_batch(conn, batch)
                except IntradaySourceBlockedError as exc:
                    intraday_source_blocked = True
                    print(f"60m source blocked during round {round_idx} batch {idx}: {exc}")
                    break
                total_intraday_rows += batch_rows
                round_rows += batch_rows
                intraday_failures.extend(batch_failures)
                round_failures.extend(batch_failures)
                print(
                    f"60m round {round_idx} batch {idx}/{len(intraday_batches)} symbols={len(batch)} "
                    f"rows={batch_rows} failures={len(batch_failures)}"
                )
                time.sleep(BATCH_SLEEP_SECONDS)

            print(
                f"60m round {round_idx} summary rows={round_rows} "
                f"failed_symbols={len(sorted(set(round_failures)))}"
            )
            if intraday_source_blocked:
                break

        final_remaining, final_status = _select_intraday_gap_symbols(
            conn,
            target_symbols,
            min_rows=intraday_target_rows,
            min_latest_date=min_latest_date,
        )
        print(
            f"60m final complete={len(final_status) - len(final_remaining)} "
            f"incomplete={len(final_remaining)}"
        )
        if final_remaining:
            print(f"60m incomplete_symbols={final_remaining}")
        if intraday_source_blocked:
            print("60m synchronization stopped early because the upstream source blocked the current IP")

    print(
        f"mode={mode} total_daily_rows={total_daily_rows} total_intraday_rows={total_intraday_rows} "
        f"daily_failures={len(daily_failures)} intraday_failures={len(intraday_failures)}"
    )
    if daily_failures:
        print(f"daily_failure_symbols={sorted(set(daily_failures))}")
    if intraday_failures:
        print(f"intraday_failure_symbols={sorted(set(intraday_failures))}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync current CSI 300 + CSI 500 constituent stock data")
    parser.add_argument("--mode", choices=["init", "refresh"], required=True)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--skip-universe", action="store_true")
    parser.add_argument("--intraday-retry-rounds", type=int, default=1)
    parser.add_argument("--intraday-target-rows", type=int, default=INTRADAY_TARGET_ROWS)
    parser.add_argument("--intraday-only", action="store_true")
    args = parser.parse_args()
    sync_stock_data(
        mode=args.mode,
        symbols=args.symbols,
        skip_universe=args.skip_universe,
        intraday_retry_rounds=max(args.intraday_retry_rounds, 1),
        intraday_target_rows=max(args.intraday_target_rows, 1),
        intraday_only=args.intraday_only,
    )


if __name__ == "__main__":
    main()
