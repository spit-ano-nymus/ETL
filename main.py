"""
main.py
-------
Command-line entry point.

One-click run for one table (YAML with pipeline.table_key — see config/tables/):
    python main.py --pipeline config/tables/customers.yaml
    python main.py --pipeline config/tables/customers.yaml --file C:/data/customers_2026_11.csv
    python main.py --pipeline config/tables/customers.yaml --setup-only
    python main.py --pipeline config/tables/customers.yaml --final-only

Legacy YAML pipelines (destination / transform blocks, see config/pipeline.yaml)
still run through core/pipeline_runner.py, unchanged.

Exit codes:
    0  – completed successfully
    1  – failed (the error is logged)
"""

import argparse
import sys


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="etl",
        description="YAML-driven ETL pipeline runner",
    )
    parser.add_argument(
        "--pipeline", required=True,
        metavar="PATH",
        help="Path to the pipeline YAML config file",
    )
    parser.add_argument(
        "--file", default=None,
        metavar="PATH",
        help="Override the source file path defined in the YAML",
    )
    parser.add_argument(
        "--setup-only", action="store_true",
        help="One-click YAML only: apply the sql/ files to Postgres and stop",
    )
    parser.add_argument(
        "--final-only", action="store_true",
        help="One-click YAML only: skip the fetch, rebuild the final table from raw "
             "(after changing the mapper or exclude_where)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity (default: INFO)",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    from utils.logging_utils import configure_logging
    configure_logging(args.log_level)

    from core.table_pipeline import is_table_pipeline, load_yaml, run_table_pipeline

    try:
        cfg = load_yaml(args.pipeline)
        if is_table_pipeline(cfg):
            run_table_pipeline(
                cfg,
                file_override=args.file,
                setup_only=args.setup_only,
                final_only=args.final_only,
            )
        else:
            if args.setup_only or args.final_only:
                raise ValueError("--setup-only / --final-only need a YAML with pipeline.table_key")
            from core.pipeline_runner import run_pipeline
            run_pipeline(yaml_path=args.pipeline, file_override=args.file)
        return 0
    except Exception:
        import logging
        logging.getLogger("main").exception("Run failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
