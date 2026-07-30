import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "jobstreet.duckdb"
EXPORT_DIR = PROJECT_ROOT / "exports"
EXPORT_DIR.mkdir(exist_ok=True)

con = duckdb.connect(str(DB_PATH), read_only=True)
tables = ["mart_skill_trend", "stg_jobs", "fact_job_posting", "dim_company", "dim_location"]
for table in tables:
    out = (EXPORT_DIR / f"{table}.parquet").as_posix()
    con.execute(f"COPY main.{table} TO '{out}' (FORMAT PARQUET)")
    print(f"wrote {out}")

con.close()
