"""
main.py
-------
Command-line entry point.

Run one table (YAML in config/tables/, one file per table):
    python main.py --pipeline config/tables/customers.yaml
    python main.py --pipeline config/tables/customers.yaml --file C:/data/customers_2026_11.csv
    python main.py --pipeline config/tables/customers.yaml --setup-only
    python main.py --pipeline config/tables/customers.yaml --final-only

Run a .sql file from the project (table config, checks, maintenance) and print
what it returns — no pgAdmin needed:
    python main.py --pipeline config/tables/customers.yaml --sql config/tables/customers_setup.sql

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
        "--sql", action="append", default=None,
        metavar="PATH",
        help="Run a .sql file against the Postgres in the YAML and print what it returns, "
             "then stop (no pgAdmin needed). Repeatable; files run in the order given.",
    )
    parser.add_argument(
        "--autocommit", action="store_true",
        help="With --sql: run each statement on its own instead of one transaction "
             "(needed for VACUUM, CREATE DATABASE, CREATE INDEX CONCURRENTLY)",
    )
    parser.add_argument(
        "--setup-only", action="store_true",
        help="Apply the sql/ files to Postgres and stop (first-time setup)",
    )
    parser.add_argument(
        "--final-only", action="store_true",
        help="Skip the fetch, rebuild the final table from raw "
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

        if args.sql:
            from core.table_pipeline import postgres_engine
            from db.migrations import run_migrations
            from db.sql_runner import run_sql_file
            engine = postgres_engine(cfg.get("postgres") or {})
            # apply sql/ first, so a table's _setup.sql works on a fresh database
            run_migrations(engine)
            for path in args.sql:
                run_sql_file(engine, path, autocommit=args.autocommit)
            return 0

        if not is_table_pipeline(cfg):
            raise ValueError(
                f"{args.pipeline} has no pipeline.table_key — see config/tables/example_*.yaml"
            )

        run_table_pipeline(
            cfg,
            file_override=args.file,
            setup_only=args.setup_only,
            final_only=args.final_only,
        )
        return 0
    except Exception:
        import logging
        logging.getLogger("main").exception("Run failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
