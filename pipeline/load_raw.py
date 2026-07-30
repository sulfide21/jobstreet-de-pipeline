#!/usr/bin/env python3
"""
Load raw JobStreet JSON from the data lake into the DuckDB warehouse.

Data lake layout (one folder per scrape date = one "partition"):
    data_lake/raw/jobstreet/date=YYYY-MM-DD/jobs.json

Target: table `raw.jobs` in warehouse/jobstreet.duckdb, with a `scrape_date`
column added so history accumulates instead of being overwritten.

The load is IDEMPOTENT: re-running it for the same date first deletes that
date's rows, then re-inserts them. Running the script twice never creates
duplicates -- pipelines get re-run all the time (retries, backfills), so
every load step must be safe to repeat.

Usage:
    python pipeline/load_raw.py            # load every partition found
    python pipeline/load_raw.py 2026-06-28 # load just one date
"""

import re
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data_lake" / "raw" / "jobstreet"
DB_PATH = PROJECT_ROOT / "warehouse" / "jobstreet.duckdb"


def find_partitions(only_date=None):
    """Yield (date_string, json_path) for each partition folder in the lake."""
    for folder in sorted(LAKE_DIR.glob("date=*")):
        m = re.fullmatch(r"date=(\d{4}-\d{2}-\d{2})", folder.name)
        if not m:
            continue
        date = m.group(1)
        if only_date and date != only_date:
            continue
        json_path = folder / "jobs.json"
        if json_path.exists():
            yield date, json_path


def load_partition(con, date, json_path):
    """Load one partition into raw.jobs (delete-then-insert = idempotent)."""
    # union_by_name lets partitions with extra columns (e.g. --full scrapes
    # that include full_description) still load into one table.
    # hive_partitioning=false: DuckDB would otherwise auto-add a `date` column
    # from the `date=YYYY-MM-DD` folder name, duplicating scrape_date.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.jobs AS
        SELECT CAST(? AS DATE) AS scrape_date, *
        FROM read_json_auto(?, union_by_name=true, hive_partitioning=false)
        LIMIT 0
        """,
        [date, str(json_path)],
    )
    con.execute("DELETE FROM raw.jobs WHERE scrape_date = ?", [date])
    con.execute(
        """
        INSERT INTO raw.jobs BY NAME
        SELECT CAST(? AS DATE) AS scrape_date, *
        FROM read_json_auto(?, union_by_name=true, hive_partitioning=false)
        """,
        [date, str(json_path)],
    )
    n = con.execute(
        "SELECT count(*) FROM raw.jobs WHERE scrape_date = ?", [date]
    ).fetchone()[0]
    print(f"  {date}: loaded {n} rows from {json_path.relative_to(PROJECT_ROOT)}")


def main():
    only_date = sys.argv[1] if len(sys.argv) > 1 else None

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    partitions = list(find_partitions(only_date))
    if not partitions:
        sys.exit(f"No partitions found in {LAKE_DIR}" + (f" for {only_date}" if only_date else ""))

    print(f"Loading {len(partitions)} partition(s) into {DB_PATH.name} ...")
    for date, json_path in partitions:
        load_partition(con, date, json_path)

    total, dates = con.execute(
        "SELECT count(*), count(DISTINCT scrape_date) FROM raw.jobs"
    ).fetchone()
    print(f"raw.jobs now holds {total} rows across {dates} scrape date(s).")


if __name__ == "__main__":
    main()
