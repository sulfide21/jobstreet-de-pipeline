# DE Build Plan — JobStreet Pipeline Portfolio

One project, six stages. Each stage adds a real data-engineering skill on top of the
scraper that already exists in this folder. **Everything runs locally and free** —
no AWS, no GCP, no credit card. DuckDB is the warehouse, Docker is the "cloud".

Rule: don't start a stage until the previous one runs end-to-end.

---

## Stage 1 — Orchestrated ELT pipeline  ⬅ CURRENT

**Skills:** Airflow, Docker, ELT layering (raw → staging → mart), data modeling.

**Target architecture:**

```
Airflow DAG (daily)
  └─ task 1: run jobstreet_scraper.py
       → land raw JSON in  data_lake/raw/jobstreet/date=YYYY-MM-DD/jobs.json
  └─ task 2: load raw JSON into DuckDB table  raw.jobs  (append, keep history)
  └─ task 3: transform into a star schema:
       fact_job_posting   (one row per job per scrape date)
       dim_company
       dim_location
       dim_skill          (exploded from requirements text)
```

**Checklist:**
- [x] Restructure the repo: `pipeline/`, `data_lake/`, `warehouse/` (dags/ + sql/ come later)
- [x] Write `load_raw.py` — JSON → DuckDB `raw.jobs`, partitioned by scrape date (understood line by line ✔)
- [x] Design the star schema (fact_job_posting + dim_company + dim_location)
- [x] Write the transform SQL that builds the fact/dim tables (test.py/test2.py/test3.py — consolidate into one build_marts.py)
- [x] Install Docker Desktop (verified: hello-world runs on WSL2 backend)
- [x] Run Airflow with the official `docker-compose.yaml` (UI live at localhost:8080)
- [x] Wrap the three steps in one Airflow DAG with retries + a daily schedule
      (jobstreet_pipeline: scrape >> load_raw >> build_marts, @daily, max_active_runs=1;
      ran green 2026-07-10 — incl. one idempotent rerun to repair a bad scrape)
- [x] Let it run 3+ days so the warehouse actually accumulates history (started 2026-07-10;
      query = "data engineer", all Indonesia — keep it constant so dates stay comparable)
      4 scrape_dates as of 2026-07-20: 06-28=1380, 07-10=847, 07-11=830, 07-20=830.
      RESOLVED: 07-11 vs 07-20 share 828 of 830 job ids (INTERSECT) — NOT a dup scrape,
      just a near-static market (postings live for weeks). Real lesson: the same job
      recurs across scrape_dates, so row count != distinct-job count → dedup is a Stage 3 must.

**Done when:** you can `SELECT` skill demand per scrape date from the star schema,
and the whole thing ran on schedule without you touching it.

---

## Stage 2 — dbt transforms + skill-trend mart

**Skills:** dbt (the industry-standard transform tool), incremental models, docs.

- [x] Replace the hand-written transform SQL with **dbt-duckdb** models
      (transform/ project; stg_jobs, dim_company, dim_location, fact_job_posting; dbt run PASS=4;
      verified staged=fact=3887, no rows dropped by the LEFT JOINs)
- [x] Layers: `staging/` (clean raw) → `marts/` (fact/dims) — dbt convention
- [x] Make `fact_job_posting` an **incremental** model (only new scrape dates)
      FIXED the gotcha: switched dims from row_number() to md5() hash keys (stable across
      rebuilds), then config(materialized='incremental') + is_incremental()/{{ this }} filter.
      Proven 2026-07-21: Airflow-triggered dbt appended only 07-21 to main.fact (now 5 dates).
- [x] Build `mart_skill_trend`: skill demand % per week — the roadmap.md, but longitudinal
      (skills seed CSV from roadmap.py taxonomy → dbt seed; mart cross-joins jobs x skills,
      regexp_matches on title+teaser+bullet_points, % per scrape_date. Built 2026-07-21.)
      KNOWN LIMITATION: % deflated because (a) short fields only, no full_description, and
      (b) no DE-relevance filter — denominator includes non-DE jobs. Ranking valid, magnitude low.
      Possible fixes: add is_de_relevant filter (roadmap.py logic) and/or --full scrapes.
- [x] Generate dbt docs (`dbt docs generate` + `docs serve --port 8081`) — lineage graph confirmed
- [x] Retire build_marts.py; Airflow build_marts task now runs `dbt run` inside the container
      (dbt-duckdb added to .env; old `marts` schema dropped; build_marts.py deleted)

**Done when:** `dbt build` rebuilds the warehouse from raw, and you can chart
"is Airflow demand rising in Surabaya?" from `mart_skill_trend`.

---

## Stage 3 — Data quality layer

**Skills:** testing, schema-drift handling, deduplication.

- [x] dbt tests: `not_null` / `unique` on keys (schema.yml, 8 tests); `accepted_values` on
      work_type done as a singular test (assert_work_type_known.sql)
- [x] Custom test: salary parsing sanity (no negative or absurd values)
      DONE (2026-07-27): stg_jobs parses salary_raw → salary_min/salary_max
      (regexp_extract_all(salary_raw,'[0-9.,]+') — commas AND dots; regexp_replace strip; cast bigint),
      gated to "per month" only via case-when (per hour/year/no-unit → NULL). Added is_salary_sane
      boolean flag = single source of truth (NULL, or min>=1M & max<=1B & max>=min). Quarantine:
      mart_salary_anomalies = stg_jobs where not is_salary_sane (raw/staging untouched — split, not delete;
      table rebuilt each run so it captures all history & grows daily). Test assert_salary_sane reuses
      the flag + config(warn_if='>0', error_if='>10') → WARN 2 now (the $15k USD row + a sub-min-wage
      Rp500k-700k row), errors only on a spike = "something broke upstream". Build green: PASS=16 WARN=1.
- [x] Dedupe within-scrape duplicates (pagination overlap): row_number() partition by
      (id, scrape_date) in stg_jobs, keep rn=1. Caught by assert_fact_grain_unique (found 6),
      fixed, all 16 dbt nodes green 2026-07-21. NOTE: cross-day repost dedup still TODO.
- [x] Fail loudly when JobStreet changes its API shape: source tests on raw.jobs
      (not_null on id + scrape_date in sources.yml). 18 dbt nodes total, all green 2026-07-21.
- [x] Tests run in the pipeline: Airflow build_marts task now `dbt build` (not just run)

**Done when:** a bad scrape *fails the pipeline* instead of silently polluting the mart.
      → Mostly there: dbt build runs all tests every run; a failing test reds the DAG.

---

## Stage 4 — Streaming (Kafka)  ✅ DONE 2026-07-28

**Skills:** Kafka producers/consumers, upserts, at-least-once thinking.

- [x] Run Kafka locally via Docker Compose (single broker, KRaft mode — no Zookeeper;
      apache/kafka:3.8.0 on localhost:9092). GOTCHA HIT: first compose had NO volume, so a
      container recreate silently wiped all 850 messages (topic showed LOG-END-OFFSET 0 and every
      read hung). Fixed with a named volume on /tmp/kraft-combined-logs.
- [x] Producer: replay scraped JSON into a `jobs` topic (streaming/producer.py — reads one
      date partition's jobs.json, sends each job keyed by job id; value_serializer json→bytes;
      flush() after the loop. 850 messages confirmed via kafka-get-offsets.sh)
- [x] Consumer: upsert into DuckDB as messages arrive (streaming/consumer.py → stream.jobs_live;
      group_id + auto_offset_reset=earliest + consumer_timeout_ms so it exits; parameterized
      insert, never f-strings)
- [x] Handle duplicates on the consumer side (idempotent upsert by job id)
      PROVEN BY EXPERIENCE: plain insert + offset reset → 1700 rows from 850 messages.
      Fixed with `id varchar primary key` + `on conflict (id) do update set ... = excluded....`
      → replay twice, 802 rows both times. 802 not 850 because the source file itself has 48
      pagination-overlap dupes — so ONE mechanism (PK on job id) handles BOTH Kafka's
      at-least-once redelivery AND dirty source data.

**Done when:** you can explain why the consumer must be idempotent, from experience.
      → DONE 2026-07-28. Kafka guarantees at-least-once, never exactly-once: any rebalance,
      crash, or network blip before the offset commit redelivers messages. So processing the
      same message twice must leave the DB identical to processing it once.

---

## Stage 5 — Dashboard  🟡 IN PROGRESS

**Skills:** BI on top of a mart (this is a week, not a phase).

- [x] BI tool chosen: **Power BI Desktop**, not Metabase. WHY: Metabase has no native DuckDB
      driver (community .jar only) AND would hold the warehouse's single write lock, colliding
      with the nightly Airflow/dbt run. Power BI reads Parquet instead — no driver, no lock.
- [x] Serving layer: `pipeline/export_marts.py` opens DuckDB **read_only** and COPYs each mart
      to `exports/*.parquet` (Parquet not CSV so types survive — dates stay dates, salary_min
      stays an int). Paths anchored to PROJECT_ROOT like load_raw.py, not the cwd.
- [x] Chart 1: skill demand over time (line; X=scrape_date as a plain field NOT the Date
      Hierarchy, Y=demand_pct, legend=skill, filtered to ~5 skills). SQL ~4%, Python ~2.5%.
- [x] Scheduled refresh: `export_marts` is now a 4th Airflow task
      (scrape >> load_raw >> build_marts >> export_marts). Ordering matters — the export opens
      DuckDB read_only and DuckDB won't allow a reader while dbt holds it as writer, so the
      `>>` chain guarantees dbt has exited first. Power BI now just needs Refresh.
      TWO BUGS HIT AND FIXED: (1) `cd {PROJ} python ...` missing `&&` — and use `&&` not `;`
      so a failed cd stops the task instead of silently running python in the wrong dir;
      (2) export_marts.py used relative "../warehouse/..." which resolved to /opt/warehouse
      under Airflow's cwd — fixed with PROJECT_ROOT = Path(__file__).resolve().parent.parent,
      the same pattern load_raw.py already used. LESSON: relative paths break the moment
      something else chooses your working directory.
- [ ] Charts 2-4: demand by city, salary by skill, experience asked vs salary offered
- [x] BONUS — live dashboard: `streaming/dashboard.py` (Streamlit) is a SECOND Kafka consumer
      (group_id="dashboard", auto_offset_reset="latest", @st.cache_resource for the connection,
      st.session_state for accumulated rows, time.sleep + st.rerun to refresh). It touches no
      DuckDB, so it can't fight the lock — and it's the concrete answer to "why Kafka?":
      two consumers on one topic, neither aware of the other.
- [x] BONUS — event time vs processing time: producer now stamps each message with
      `scrape_date` (when JobStreet had the job) and `ingested_at` (when it was produced);
      consumer stores both, and deliberately does NOT update ingested_at on conflict so it
      means "first seen". Consumer uses job.get() not job[] so messages produced before the
      schema change still consume (land as NULL) instead of raising KeyError = schema evolution.

**Done when:** someone who's never seen the repo understands the job market in 30 seconds.

---

## Stage 6 — CI/CD + polish  ✅ DONE 2026-07-30

**Skills:** GitHub Actions, Docker Compose, portfolio presentation.

- [x] `git init`, push to GitHub (public): https://github.com/sulfide21/jobstreet-de-pipeline
      SECURITY CATCH before first commit: `airflow/config/airflow.cfg` had a real
      auto-generated fernet_key + secret_key — Airflow writes this file on first run if
      missing (confirmed in docker-compose.yaml's airflow-init step), so it's safe to
      gitignore and let it regenerate. `transform/.user.yml` (dbt's local anonymous
      telemetry id) gitignored too — machine-specific noise, not project code.
      Renamed `powerBI/ligma.pbix` -> `jobstreet_dashboard.pbix` before committing
      (blocked once by Power BI Desktop holding the file open — do this manually if it
      wasn't renamed by the time this was committed; check `powerBI/` on GitHub).
- [x] One `docker compose up` starts everything containerized (Airflow + Kafka).
      Root `docker-compose.yml` uses Compose's `include:` directive to pull in both
      `airflow/docker-compose.yaml` and `streaming/docker-compose.yml` as one project —
      no duplication, verified with `docker compose config --services` listing both
      stacks' services together. Streamlit stays a separate `streamlit run` command
      (not containerized) — documented honestly in the README rather than overclaimed.
- [x] GitHub Actions (`.github/workflows/dbt-tests.yml`): on every push/PR — checkout,
      py_compile lint, load `ci/sample_jobs.json` (a small SYNTHETIC fixture, 7 rows,
      committed to the repo) into a throwaway warehouse via load_raw.py, then
      `dbt build --profiles-dir .`. Deliberately does NOT touch the real
      warehouse/jobstreet.duckdb (gitignored, never shared). Sample fixture
      double-duty: includes a comma-formatted salary, a per-hour non-month salary,
      a USD row (triggers assert_salary_sane WARN on purpose), and one job_id posted
      twice (proves the pagination dedup). Dry-ran locally in an isolated scratch copy
      first, then confirmed green on the actual GitHub Actions runner (39s).
- [x] README.md: Mermaid architecture diagram at the top (Extract/Load/Transform/Serve/
      Streaming subgraphs), skills list, repo layout, quickstart, and an explicit
      "Honest limitations" section (short trend window, streaming replays a file rather
      than watching a live feed, skill-demand % is a deflated lower bound). Links to
      this file for the full stage-by-stage log.

**Done when:** a stranger can clone the repo and have the pipeline running in one command.
      → Containerized parts (Airflow + Kafka) yes, via root docker-compose.yml. Streamlit
      dashboard and the first dbt build still need manual follow-up steps (documented).

---

## Later (when the bank cooperates)

- Swap DuckDB → **BigQuery** (GCP free tier) and local folder → GCS. The dbt models
  barely change — that's the point of the layered design.
- Terraform for provisioning = the last Zoomcamp skill not covered here.

## Study alongside

- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) —
  modules map almost 1:1 to these stages (skip the GCP parts until "Later")
- Kimball's star-schema basics before Stage 1's modeling step
