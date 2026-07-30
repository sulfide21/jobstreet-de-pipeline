#!/usr/bin/env python3
"""
SQL-style UI for the scraped JobStreet data.

Loads jobstreet_jobs.json (produced by jobstreet_scraper.py) into an in-memory
DuckDB table called `jobs`, then lets you query it two ways:

  * Filters tab   -- point-and-click filters (location, salary, work type, ...)
  * SQL tab       -- write any SELECT against the `jobs` table

Run it:
    streamlit run app.py

The salary text (e.g. "Rp 10jt - 12jt per month") is parsed into numeric
`salary_min` / `salary_max` columns so you can do things like:
    SELECT * FROM jobs WHERE salary_min >= 7000000 ORDER BY salary_min DESC
"""

import json
import os
import re

import duckdb
import pandas as pd
import streamlit as st

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = os.path.join(HERE, "jobstreet_jobs.json")


# --------------------------------------------------------------------------- #
# Salary parsing
# --------------------------------------------------------------------------- #
_SAL_TOKEN = re.compile(r"([\d][\d.,]*)\s*(jt|juta|m|rb|ribu|k)?", re.I)


def _to_number(num_text, suffix):
    """Convert one Indonesian-formatted number token to a float.

    Indonesian uses '.' as the thousands separator and ',' as the decimal
    point, e.g. "4.500.000" -> 4500000 and "10,5jt" -> 10500000.
    """
    suffix = (suffix or "").lower()
    cleaned = num_text.replace(".", "").replace(",", ".")
    try:
        value = float(cleaned)
    except ValueError:
        return None
    if suffix in ("jt", "juta", "m"):
        value *= 1_000_000
    elif suffix in ("rb", "ribu", "k"):
        value *= 1_000
    return value


def parse_salary(label):
    """Return (min, max, period) parsed from a salary label, any may be None."""
    if not label:
        return None, None, None
    low = label.lower()
    if "hour" in low or "jam" in low:
        period = "hour"
    elif "year" in low or "tahun" in low or "annum" in low:
        period = "year"
    elif "month" in low or "bulan" in low:
        period = "month"
    else:
        period = None

    numbers = []
    for m in _SAL_TOKEN.finditer(label):
        # skip stray standalone digits with no real magnitude
        val = _to_number(m.group(1), m.group(2))
        if val is not None and val >= 1:
            numbers.append(val)

    if not numbers:
        return None, None, period
    if len(numbers) == 1:
        return numbers[0], numbers[0], period
    return min(numbers[0], numbers[1]), max(numbers[0], numbers[1]), period


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #
@st.cache_data(show_spinner=False)
def load_data(path):
    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)
    df = pd.DataFrame(records)

    # derived numeric salary columns
    parsed = df["salary"].apply(parse_salary) if "salary" in df else []
    df["salary_min"] = [p[0] for p in parsed]
    df["salary_max"] = [p[1] for p in parsed]
    df["salary_period"] = [p[2] for p in parsed]
    return df


def run_sql(df, query):
    """Run a read-only SQL query against the dataframe registered as `jobs`."""
    con = duckdb.connect(database=":memory:")
    con.register("jobs", df)
    try:
        return con.execute(query).fetchdf()
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
st.set_page_config(page_title="JobStreet SQL Explorer", layout="wide")
st.title("JobStreet SQL Explorer")

with st.sidebar:
    st.header("Data source")
    data_path = st.text_input("Path to jobstreet_jobs.json", value=DEFAULT_DATA)

if not os.path.exists(data_path):
    st.warning(
        f"Data file not found: {data_path}\n\n"
        "Run the scraper first, e.g.\n"
        '`python jobstreet_scraper.py --keywords "data" --where "Surabaya Jawa Timur" --full`'
    )
    st.stop()

df = load_data(data_path)
st.caption(f"Loaded **{len(df):,}** jobs · columns: {', '.join(df.columns)}")

filters_tab, sql_tab = st.tabs(["Filters", "SQL"])


def show_results(result):
    st.write(f"**{len(result):,}** rows")
    st.dataframe(result, use_container_width=True, hide_index=True)
    if not result.empty:
        st.download_button(
            "Download results as CSV",
            result.to_csv(index=False).encode("utf-8-sig"),
            file_name="query_results.csv",
            mime="text/csv",
        )


# ---- Filters tab ---------------------------------------------------------- #
with filters_tab:
    col1, col2, col3 = st.columns(3)

    def options(colname):
        if colname not in df:
            return []
        vals = sorted(v for v in df[colname].dropna().unique() if str(v).strip())
        return vals

    with col1:
        kw = st.text_input("Title / company contains")
        locs = st.multiselect("Location", options("location"))
    with col2:
        work_types = st.multiselect("Work type", options("work_type"))
        arrangements = st.multiselect("Work arrangement", options("work_arrangement"))
    with col3:
        classifs = st.multiselect("Classification", options("classification"))
        min_salary = st.number_input(
            "Min salary (per month, IDR)", min_value=0, value=0, step=1_000_000
        )

    clauses = []
    if kw:
        safe = kw.replace("'", "''")
        clauses.append(f"(lower(title) LIKE lower('%{safe}%') OR lower(company) LIKE lower('%{safe}%'))")
    if locs:
        joined = ", ".join("'" + v.replace("'", "''") + "'" for v in locs)
        clauses.append(f"location IN ({joined})")
    if work_types:
        joined = ", ".join("'" + v.replace("'", "''") + "'" for v in work_types)
        clauses.append(f"work_type IN ({joined})")
    if arrangements:
        joined = ", ".join("'" + v.replace("'", "''") + "'" for v in arrangements)
        clauses.append(f"work_arrangement IN ({joined})")
    if classifs:
        joined = ", ".join("'" + v.replace("'", "''") + "'" for v in classifs)
        clauses.append(f"classification IN ({joined})")
    if min_salary > 0:
        clauses.append(f"salary_max >= {min_salary}")

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    query = f"SELECT * FROM jobs{where} ORDER BY salary_max DESC NULLS LAST"
    st.code(query, language="sql")
    show_results(run_sql(df, query))


# ---- SQL tab -------------------------------------------------------------- #
with sql_tab:
    st.markdown("Query the table **`jobs`**. Example queries:")
    st.code(
        "SELECT title, company, salary, location\n"
        "FROM jobs\n"
        "WHERE salary_min >= 7000000\n"
        "ORDER BY salary_min DESC;\n\n"
        "SELECT classification, COUNT(*) AS n, AVG(salary_max) AS avg_max\n"
        "FROM jobs\n"
        "GROUP BY classification\n"
        "ORDER BY n DESC;",
        language="sql",
    )
    default_q = "SELECT title, company, salary, salary_min, salary_max, location\nFROM jobs\nORDER BY salary_max DESC NULLS LAST\nLIMIT 100;"
    user_q = st.text_area("SQL query", value=default_q, height=160)
    if st.button("Run query", type="primary"):
        stripped = user_q.strip().rstrip(";").lower()
        if not stripped.startswith(("select", "with")):
            st.error("Only SELECT / WITH queries are allowed.")
        else:
            try:
                show_results(run_sql(df, user_q))
            except Exception as exc:  # noqa: BLE001 - surface SQL errors to user
                st.error(f"Query error: {exc}")
