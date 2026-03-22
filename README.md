# A-Share Research

WSL + VS Code quant research workspace.

## Quickstart

Install dependencies:

```bash
uv sync
```

Run the existing smoke test:

```bash
uv run python scripts/smoke_test.py
```

## PostgreSQL Index Store

This repo can persist HS300 (`000300`) and CSI500 (`000905`) bars into PostgreSQL.

Start a local user-space PostgreSQL 16 instance:

```bash
./scripts/start_local_postgres.sh
```

Set the connection string:

```bash
export A_SHARE_PG_DSN="postgresql://thinkpad@localhost:5432/a_share_research"
```

Initialize schema + backfill 10 years of daily bars + current 60 minute window:

```bash
uv run python -m a_share_research.sync_index_data --mode init
```

Refresh incrementally:

```bash
uv run python -m a_share_research.sync_index_data --mode refresh
```

Run the web dashboard:

```bash
uv run python -m a_share_research.webapp --host 0.0.0.0 --port 8000
```

Then open `http://127.0.0.1:8000` inside WSL or use the forwarded address from Windows.

For background usage:

```bash
export A_SHARE_PG_DSN="postgresql://thinkpad@127.0.0.1:5432/a_share_research"
./scripts/start_dashboard.sh
./scripts/stop_dashboard.sh
```

Load bars from Python:

```python
from a_share_research import load_bar_1d, load_bar_60m

daily = load_bar_1d("000300")
intraday = load_bar_60m("000905")
```

Stop the local PostgreSQL instance:

```bash
./scripts/stop_local_postgres.sh
```



## Layout

- : exploratory notebooks
- : reusable research code
- : raw downloads/cache
- : processed datasets
- : charts and exports
- : smoke tests and utilities
