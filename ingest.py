"""Load raw robot log files into DuckDB.

Usage:
    python ingest.py                      # every .xlsx in data/raw/
    python ingest.py --file path/to.xlsx  # one file
    python ingest.py --db other.duckdb    # a different database file

Safe to run repeatedly: unchanged files are skipped, changed files are
replaced, never duplicated. All files load in one transaction.
    python ingest.py --force              # reload even unchanged files
Exit code 0 on success, 1 with a clear message on any problem.
"""

import argparse
import logging
import sys
from pathlib import Path

from robot_pipeline import config
from robot_pipeline.loader import LoaderError, find_source_files, load_files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--file", type=Path, help="load only this file")
    parser.add_argument("--raw-dir", type=Path, default=config.raw_dir())
    parser.add_argument("--db", type=Path, default=config.db_path())
    parser.add_argument("--force", action="store_true", help="reload unchanged files too")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        files = [args.file] if args.file else find_source_files(args.raw_dir)
        for result in load_files(files, args.db, force=args.force):
            logging.info(
                "%s %s: %d rows in raw_logs (replaced %d) in %s",
                result.action.capitalize(),
                result.source_file,
                result.rows_loaded,
                result.rows_replaced,
                args.db,
            )
    except LoaderError as exc:
        logging.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
