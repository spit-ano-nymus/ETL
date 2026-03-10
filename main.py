"""
main.py
-------
CLI entry point for the ETL pipeline framework.

Usage:
    python main.py --pipeline config/pipeline.yaml
    python main.py --pipeline config/pipeline.yaml --file C:/data/other.csv
    python main.py --pipeline config/pipeline.yaml --log-level DEBUG

Exit codes:
    0  – pipeline completed successfully
    1  – pipeline failed (exception logged to stderr)
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
        help="Override the source CSV path defined in the pipeline config",
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

    from core.pipeline_runner import run_pipeline

    try:
        run_pipeline(yaml_path=args.pipeline, file_override=args.file)
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    sys.exit(main())
