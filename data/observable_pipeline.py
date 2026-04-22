"""Backward-compatible wrapper around spark_jobs.observable_pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path

from spark_jobs.observable_pipeline import run_observable_pipeline
from utils.config import DATA_DIR, RAW_DATA_PATH


def main() -> None:
    parser = argparse.ArgumentParser(description="Run observable PySpark pipeline for Spark UI DAG demos")
    parser.add_argument("--input", type=str, default=str(RAW_DATA_PATH), help="Input CSV path")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DATA_DIR / "processed" / "spark_ui_observable"),
        help="Output directory for parquet/json outputs",
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Disable intermediate dataframe persistence",
    )
    parser.add_argument(
        "--keep-ui-alive-seconds",
        type=int,
        default=30,
        help="Keep Spark session alive after actions so UI remains inspectable",
    )
    args = parser.parse_args()

    run_observable_pipeline(
        input_csv=Path(args.input),
        output_dir=Path(args.output_dir),
        persist_intermediate=not args.no_persist,
        keep_ui_alive_seconds=args.keep_ui_alive_seconds,
    )


if __name__ == "__main__":
    main()