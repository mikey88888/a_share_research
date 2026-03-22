from __future__ import annotations

import matplotlib.pyplot as plt
import pyarrow

from a_share_research import fetch_stock_history, project_root


def main() -> None:
    root = project_root()
    raw_dir = root / "data" / "raw"
    reports_dir = root / "reports"
    raw_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    df = fetch_stock_history()
    if df.empty:
        raise RuntimeError("akshare returned an empty dataframe")

    csv_path = raw_dir / "000001_202401_daily.csv"
    parquet_path = raw_dir / "000001_202401_daily.parquet"
    plot_path = reports_dir / "000001_close.png"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    close_col = "收盘"
    if close_col not in df.columns:
        raise RuntimeError(f"expected column {close_col!r}, got {list(df.columns)!r}")

    ax = df[close_col].plot(title="000001 Daily Close")
    ax.figure.tight_layout()
    ax.figure.savefig(plot_path)
    plt.close(ax.figure)

    print(f"rows={len(df)}")
    print(f"csv={csv_path}")
    print(f"parquet={parquet_path}")
    print(f"plot={plot_path}")
    print(f"pyarrow={pyarrow.__version__}")


if __name__ == "__main__":
    main()
