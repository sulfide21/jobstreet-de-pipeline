#!/usr/bin/env python3
"""
Turn scraped JobStreet data into a data-engineering learning roadmap.

Idea: SQL queries answer "which jobs match X". A roadmap needs the opposite --
the *aggregate* signal: across all the data-engineering job requirements, which
skills/tools are actually asked for, and how often. We:

  1. select the data-engineering-relevant jobs (by title + description signal),
  2. count how many of them mention each skill (from a curated DE taxonomy),
  3. rank skills by demand (% of jobs that ask for them),
  4. lay them out in a sensible learning order, annotated with that demand.

The skill counting is plain keyword matching -- transparent and instant, and it
works across the bilingual (Indonesian/English) text because tech names like
Python / SQL / Spark / AWS are the same in both.

Usage:
    python roadmap.py
    python roadmap.py --all              # analyse every job, no DE filter
    python roadmap.py --min-pct 8        # only list skills asked in >=8% of jobs
    python roadmap.py --out roadmap.md
"""

import argparse
import json
import os
import re
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = os.path.join(HERE, "jobstreet_jobs.json")
DEFAULT_OUT = os.path.join(HERE, "data_engineering_roadmap.md")

# --------------------------------------------------------------------------- #
# Skill taxonomy: canonical name -> regex alias matched case-insensitively.
# Grouped into the learning PHASES used to lay out the roadmap.
# Keep aliases tight (word boundaries) so "go"/"r"/"s3" don't match noise.
# --------------------------------------------------------------------------- #
PHASES = [
    ("1. Foundations", [
        ("SQL",          r"\bsql\b"),
        ("Python",       r"\bpython\b"),
        ("Linux / Bash", r"\b(linux|bash|shell script|unix)\b"),
        ("Git",          r"\b(git|github|gitlab|version control)\b"),
        ("Excel",        r"\b(excel|spreadsheet)\b"),
    ]),
    ("2. Databases & Data Modeling", [
        ("PostgreSQL",   r"\b(postgre?s|postgresql)\b"),
        ("MySQL",        r"\bmysql\b"),
        ("SQL Server",   r"\b(sql server|ms sql|t-sql|tsql)\b"),
        ("Oracle",       r"\boracle\b"),
        ("MongoDB",      r"\bmongo ?db\b"),
        ("NoSQL",        r"\bnosql\b"),
        ("Redis",        r"\bredis\b"),
        ("Elasticsearch",r"\b(elastic ?search|elk)\b"),
        ("Data Modeling",r"\b(data model|dimensional model|star schema|normali[sz])"),
    ]),
    ("3. Data Warehousing", [
        ("Data Warehouse", r"\b(data ?warehous|dwh|datamart|data mart)"),
        ("Snowflake",      r"\bsnowflake\b"),
        ("BigQuery",       r"\b(big ?query)\b"),
        ("Redshift",       r"\bredshift\b"),
        ("Synapse",        r"\bsynapse\b"),
        ("Databricks",     r"\bdatabricks\b"),
    ]),
    ("4. ETL / ELT & Orchestration", [
        ("ETL / ELT",    r"\b(etl|elt)\b"),
        ("Data Pipeline",r"\bdata ?pipeline"),
        ("Airflow",      r"\bairflow\b"),
        ("dbt",          r"\bdbt\b"),
        ("SSIS",         r"\bssis\b"),
        ("Talend",       r"\btalend\b"),
        ("Informatica",  r"\binformatica\b"),
        ("Pentaho",      r"\bpentaho\b"),
        ("NiFi",         r"\bnifi\b"),
    ]),
    ("5. Big Data Processing", [
        ("Spark",        r"\b(spark|pyspark)\b"),
        ("Hadoop",       r"\bhadoop\b"),
        ("Hive",         r"\bhive\b"),
        ("Flink",        r"\bflink\b"),
        ("Scala",        r"\bscala\b"),
        ("Java",         r"\bjava\b"),
    ]),
    ("6. Streaming", [
        ("Kafka",        r"\bkafka\b"),
        ("Kinesis",      r"\bkinesis\b"),
        ("Pub/Sub",      r"\bpub/?sub\b"),
        ("RabbitMQ",     r"\brabbit ?mq\b"),
    ]),
    ("7. Cloud Platforms", [
        ("AWS",          r"\b(aws|amazon web services)\b"),
        ("Azure",        r"\bazure\b"),
        ("GCP",          r"\b(gcp|google cloud)\b"),
    ]),
    ("8. DevOps & Deployment", [
        ("Docker",       r"\bdocker\b"),
        ("Kubernetes",   r"\b(kubernetes|k8s)\b"),
        ("CI/CD",        r"\b(ci/cd|cicd|jenkins|continuous integration)\b"),
        ("Terraform",    r"\bterraform\b"),
    ]),
    ("9. BI & Visualization", [
        ("Power BI",     r"\bpower ?bi\b"),
        ("Tableau",      r"\btableau\b"),
        ("Looker",       r"\blooker\b"),
        ("Metabase",     r"\bmetabase\b"),
    ]),
]

# Signals that mark a job as data-engineering relevant.
DE_TITLE = re.compile(
    r"data engineer|data engineering|\betl\b|data ?warehous|big ?data|"
    r"data ?platform|data ?infrastructure|\bdba\b|database (engineer|administrator)|"
    r"data ?pipeline", re.I)
DE_SIGNALS = [re.compile(p, re.I) for p in [
    r"data ?pipeline", r"\betl\b|\belt\b", r"data ?warehous", r"\bspark\b",
    r"\bairflow\b", r"\bhadoop\b", r"data engineer", r"big ?data",
]]

EXP_RE = re.compile(r"(\d+)\s*\+?\s*(?:tahun|years?|thn)", re.I)


def job_text(job):
    return " ".join(str(job.get(k, "")) for k in
                    ("title", "teaser", "bullet_points", "full_description")).lower()


def is_de_relevant(job, text):
    if DE_TITLE.search(job.get("title", "")):
        return True
    return sum(1 for s in DE_SIGNALS if s.search(text)) >= 2


def analyze(jobs, use_all):
    selected = []
    for job in jobs:
        text = job_text(job)
        if use_all or is_de_relevant(job, text):
            selected.append((job, text))

    total = len(selected)
    skill_counts = Counter()
    compiled = {name: re.compile(pat, re.I)
                for _, skills in PHASES for name, pat in skills}

    exp_years = []
    for job, text in selected:
        for name, rx in compiled.items():
            if rx.search(text):
                skill_counts[name] += 1
        m = EXP_RE.search(text)
        if m:
            y = int(m.group(1))
            if 0 < y <= 20:
                exp_years.append(y)

    return total, skill_counts, exp_years


def pct(n, total):
    return (100.0 * n / total) if total else 0.0


def build_markdown(total, skill_counts, exp_years, min_pct, source_note):
    lines = []
    lines.append("# Data Engineering Learning Roadmap")
    lines.append("")
    lines.append(f"_Generated from **{total}** data-engineering job listings "
                 f"({source_note})._")
    lines.append("")
    lines.append("Each skill shows its **demand** = the share of those jobs that "
                 "mention it. Learn top-down within each phase; phases are ordered "
                 "so earlier skills are prerequisites for later ones.")
    lines.append("")

    # Overall ranked table
    lines.append("## Most in-demand skills (overall)")
    lines.append("")
    lines.append("| Rank | Skill | Demand | Jobs |")
    lines.append("|-----:|-------|-------:|-----:|")
    for i, (name, cnt) in enumerate(skill_counts.most_common(25), 1):
        lines.append(f"| {i} | {name} | {pct(cnt, total):.0f}% | {cnt} |")
    lines.append("")

    # Experience
    if exp_years:
        exp_years.sort()
        mid = exp_years[len(exp_years) // 2]
        lines.append(f"**Typical experience asked:** ~{mid} years "
                     f"(median of {len(exp_years)} postings that stated a number).")
        lines.append("")

    # Phased roadmap
    lines.append("## The roadmap (in learning order)")
    lines.append("")
    for phase_name, skills in PHASES:
        ranked = sorted(
            ((name, skill_counts.get(name, 0)) for name, _ in skills),
            key=lambda x: x[1], reverse=True)
        shown = [(n, c) for n, c in ranked if pct(c, total) >= min_pct]
        if not shown:
            continue
        lines.append(f"### {phase_name}")
        for name, cnt in shown:
            bar = "#" * max(1, round(pct(cnt, total) / 5))
            lines.append(f"- **{name}** — {pct(cnt, total):.0f}% `{bar}`")
        lines.append("")

    lines.append("---")
    lines.append("_Skills below the demand threshold are hidden; lower "
                 "`--min-pct` to see the long tail._")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Build a DE roadmap from scraped jobs.")
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--all", action="store_true",
                        help="analyse every job instead of only DE-relevant ones")
    parser.add_argument("--min-pct", type=float, default=5.0,
                        help="hide skills mentioned in fewer than this %% of jobs")
    args = parser.parse_args()

    with open(args.data, encoding="utf-8") as f:
        jobs = json.load(f)

    total, skill_counts, exp_years = analyze(jobs, args.all)
    if total == 0:
        print("No matching jobs found.")
        return

    note = "all jobs" if args.all else "filtered to data-engineering roles"
    md = build_markdown(total, skill_counts, exp_years, args.min_pct, note)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(md)

    # console summary
    print(f"Analysed {total} jobs ({note}).")
    print("Top 15 skills by demand:")
    for name, cnt in skill_counts.most_common(15):
        print(f"  {pct(cnt, total):5.1f}%  {name}")
    print(f"\nRoadmap written to {args.out}")


if __name__ == "__main__":
    main()
