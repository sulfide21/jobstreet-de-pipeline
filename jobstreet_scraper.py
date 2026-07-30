#!/usr/bin/env python3
"""
JobStreet (Indonesia) job scraper.

JobStreet ID runs on SEEK's platform, which exposes a JSON search API behind
the HTML pages. We hit that API directly instead of parsing HTML or driving a
browser -- it is faster, more stable, and returns clean structured data.

Example page this mirrors:
    https://id.jobstreet.com/id/data-jobs/in-Surabaya-Jawa-Timur?page=6
        -> keywords="data", where="Surabaya Jawa Timur"

Usage:
    python jobstreet_scraper.py --keywords "data" --where "Surabaya Jawa Timur"
    python jobstreet_scraper.py --keywords "data engineer" --where "Jakarta" --max-pages 5
    python jobstreet_scraper.py --keywords "data" --where "Surabaya Jawa Timur" --full

Output:
    jobstreet_jobs.csv and jobstreet_jobs.json in the current directory
    (override with --out-prefix).
"""

import argparse
import csv
import html as html_lib
import json
import re
import sys
import time

import requests

API_URL = "https://id.jobstreet.com/api/jobsearch/v5/search"
# The full description isn't in the search API; it lives in the redux blob
# embedded in each job's HTML page.
JOB_PAGE_URL = "https://id.jobstreet.com/id/job/{job_id}"

# A realistic browser-ish header set keeps the API from rejecting the request.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://id.jobstreet.com/",
}

# Fixed params that identify the JobStreet ID site to SEEK's backend.
BASE_PARAMS = {
    "siteKey": "ID-Main",
    "sourcesystem": "houston",
    "locale": "id-ID",
}


def fetch_page(session, keywords, where, page, page_size):
    """Fetch one page of search results. Returns the parsed JSON dict."""
    params = dict(BASE_PARAMS)
    params.update(
        {
            "keywords": keywords,
            "where": where,
            "page": page,
            "pageSize": page_size,
        }
    )
    resp = session.get(API_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _extract_redux(page_html):
    """Pull the `window.SEEK_REDUX_DATA = {...}` object out of a job page and
    return it parsed. Brace-matches the JSON literal so we don't depend on
    where the assignment ends."""
    marker = "SEEK_REDUX_DATA = "
    start = page_html.find(marker)
    if start == -1:
        return None
    start += len(marker)

    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(page_html)):
        c = page_html[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(page_html[start : i + 1])
                    except json.JSONDecodeError:
                        return None
    return None


def _html_to_text(raw):
    """Turn the description HTML into readable plain text, keeping bullets and
    paragraph breaks as newlines."""
    if not raw:
        return ""
    text = re.sub(r"(?is)<style.*?</style>", "", raw)
    text = re.sub(r"(?is)<(br|/p|/li|/div|/h[1-6])\s*>", "\n", text)
    text = re.sub(r"(?is)<li[^>]*>", "\n- ", text)
    text = re.sub(r"(?s)<[^>]+>", "", text)
    text = html_lib.unescape(text)
    # collapse runs of blank lines / trailing spaces
    lines = [ln.strip() for ln in text.splitlines()]
    out = []
    for ln in lines:
        if ln or (out and out[-1]):
            out.append(ln)
    return "\n".join(out).strip()


def fetch_full_description(session, job_id):
    """Fetch a job's full description + requirements from its HTML page.

    Returns (text, html). `text` is readable plain text; `html` is the raw
    description markup in case you want to keep formatting."""
    try:
        resp = session.get(
            JOB_PAGE_URL.format(job_id=job_id),
            params=BASE_PARAMS,
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        redux = _extract_redux(resp.text)
        job = (((redux or {}).get("jobdetails") or {}).get("result") or {}).get("job") or {}
        raw = job.get("content") or ""
        return _html_to_text(raw), raw
    except Exception as exc:  # noqa: BLE001 - best-effort enrichment
        print(f"  ! could not fetch detail for {job_id}: {exc}", file=sys.stderr)
        return "", ""


def parse_job(job):
    """Flatten one job object from the API into a plain dict for output."""
    advertiser = job.get("advertiser") or {}
    classifications = job.get("classifications") or []
    classification = classifications[0] if classifications else {}

    locations = job.get("locations") or []
    location = ", ".join(
        loc.get("label", "") for loc in locations if loc.get("label")
    ) or job.get("locationLabel", "")

    work_types = job.get("workTypes") or []
    work_arrangements = job.get("workArrangements") or {}
    wa_data = work_arrangements.get("data") if isinstance(work_arrangements, dict) else None
    arrangement = ""
    if wa_data:
        arrangement = ", ".join(d.get("label", {}).get("text", "") for d in wa_data)

    bullets = job.get("bulletPoints") or []

    job_id = job.get("id", "")
    return {
        "id": job_id,
        "title": job.get("title", ""),
        "company": job.get("companyName") or advertiser.get("description", ""),
        "salary": job.get("salaryLabel", ""),
        "location": location,
        "work_type": ", ".join(work_types),
        "work_arrangement": arrangement,
        "classification": (classification.get("classification") or {}).get("description", ""),
        "sub_classification": (classification.get("subclassification") or {}).get("description", ""),
        "listing_date": job.get("listingDate", ""),
        "teaser": job.get("teaser", ""),
        "bullet_points": " | ".join(bullets),
        "url": f"https://id.jobstreet.com/id/job/{job_id}" if job_id else "",
    }


def scrape(keywords, where, max_pages, page_size, delay, full):
    session = requests.Session()
    all_jobs = []
    page = 1

    while True:
        if max_pages and page > max_pages:
            break

        print(f"Fetching page {page} ...", file=sys.stderr)
        payload = fetch_page(session, keywords, where, page, page_size)

        jobs = payload.get("data") or []
        if not jobs:
            print("No more results.", file=sys.stderr)
            break

        total = payload.get("totalCount")
        for job in jobs:
            record = parse_job(job)
            if full and record["id"]:
                text, raw = fetch_full_description(session, record["id"])
                record["full_description"] = text
                record["full_description_html"] = raw
                time.sleep(delay)
            all_jobs.append(record)

        print(
            f"  page {page}: {len(jobs)} jobs (collected {len(all_jobs)}"
            f"{' / ' + str(total) if total else ''})",
            file=sys.stderr,
        )

        # Stop once we've collected everything the API reports.
        if total and len(all_jobs) >= total:
            break

        page += 1
        time.sleep(delay)

    return all_jobs


def save(jobs, out_prefix):
    json_path = f"{out_prefix}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)

    csv_path = f"{out_prefix}.csv"
    if jobs:
        # Union of all keys, preserving first-seen order; drop the raw-HTML
        # column from CSV (it's huge and unreadable -- it stays in the JSON).
        fieldnames = []
        for job in jobs:
            for key in job:
                if key not in fieldnames and key != "full_description_html":
                    fieldnames.append(key)
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for job in jobs:
                writer.writerow(job)

    print(f"\nSaved {len(jobs)} jobs to {json_path} and {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="Scrape JobStreet Indonesia jobs.")
    parser.add_argument("--keywords", default="data", help='search keywords, e.g. "data engineer"')
    parser.add_argument("--where", default="Surabaya Jawa Timur", help="location")
    parser.add_argument("--max-pages", type=int, default=0, help="0 = all pages")
    parser.add_argument("--page-size", type=int, default=30, help="results per page (max ~30)")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests")
    parser.add_argument("--full", action="store_true", help="also fetch each job's full description (slow)")
    parser.add_argument("--out-prefix", default="jobstreet_jobs", help="output file prefix")
    args = parser.parse_args()

    jobs = scrape(
        keywords=args.keywords,
        where=args.where,
        max_pages=args.max_pages,
        page_size=args.page_size,
        delay=args.delay,
        full=args.full,
    )
    save(jobs, args.out_prefix)


if __name__ == "__main__":
    main()
