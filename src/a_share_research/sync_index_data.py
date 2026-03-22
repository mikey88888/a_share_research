from __future__ import annotations

import argparse
from datetime import date

from .db import connect_pg, ensure_schema, get_latest_trade_date, sync_instruments, upsert_bar_1d, upsert_bar_60m
from .index_data import INDEX_INSTRUMENTS, default_daily_end, default_daily_start, fetch_index_60m, fetch_index_daily, next_daily_start


def sync_all(mode: str) -> None:
    today = date.today()
    daily_end = default_daily_end(today=today)

    with connect_pg() as conn:
        ensure_schema(conn)
        sync_instruments(conn)

        for instrument in INDEX_INSTRUMENTS:
            if mode == "init":
                daily_start = default_daily_start(today=today)
            else:
                daily_start = next_daily_start(get_latest_trade_date(conn, instrument.symbol), today=today)

            daily_rows = 0
            if daily_start <= daily_end:
                daily_rows = upsert_bar_1d(
                    conn,
                    fetch_index_daily(instrument.symbol, start_date=daily_start, end_date=daily_end),
                )

            intraday_rows = upsert_bar_60m(conn, fetch_index_60m(instrument.symbol))
            print(
                f"{instrument.symbol} mode={mode} daily_rows={daily_rows} "
                f"intraday_rows={intraday_rows}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync HS300 and CSI500 index data to PostgreSQL")
    parser.add_argument("--mode", choices=["init", "refresh"], required=True)
    args = parser.parse_args()
    sync_all(mode=args.mode)


if __name__ == "__main__":
    main()
