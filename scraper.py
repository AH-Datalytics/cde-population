"""
FBI CDE Population Scraper (summarized-API based)
==================================================
Fetches the population for every agency listed in the FBI CDE API.

Why the API instead of the RETA master file: RETA header populations are
split across up to three MSA fields and undercount multi-county agencies
(e.g. Hoover PD: RETA 65,283 vs actual 93,094). The summarized API returns
the full agency population. ORIs from the API are already 9-character
(RTCI format), so no suffix padding is needed.

One request per agency (~19,600). Uses requests.Session with per-thread
connection pooling — the CDE server aggressively resets fresh TLS
connections, so pooled keep-alive connections are required for throughput
(a urllib version with one connection per request ran 6+ hours and hit
the GitHub Actions job limit).

Usage:
    python scraper.py                    # default output: cde_populations.csv
    python scraper.py --output my.csv
"""

import argparse
import csv
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

CDE_API_BASE = "https://cde.ucr.cjis.gov/LATEST"

STATES = (
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN "
    "MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA "
    "WV WI WY PR GU VI AS"
).split()

# Keys in the API's population series that are NOT the agency itself
NON_AGENCY_KEYS = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
    "Puerto Rico", "Guam", "Virgin Islands", "American Samoa", "United States",
}


_local = threading.local()


def _session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers["User-Agent"] = "cde-population-scraper"
        _local.session = s
    return _local.session


def get_json(url, timeout=30, retries=4):
    for attempt in range(retries):
        try:
            r = _session().get(url, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError):
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def get_agencies():
    """Return list of {ori, agency_name, state} for every agency in the API."""
    agencies = []
    seen = set()
    for st in STATES:
        try:
            data = get_json(f"{CDE_API_BASE}/agency/byStateAbbr/{st}")
        except Exception as e:
            print(f"  {st}: FAILED ({e})")
            continue
        n = 0
        for county, lst in data.items():
            if not isinstance(lst, list):
                continue
            for a in lst:
                if not isinstance(a, dict):
                    continue
                ori = a.get("ori")
                if ori and ori not in seen:
                    seen.add(ori)
                    agencies.append({
                        "ori": ori,
                        "agency_name": a.get("agency_name", ""),
                        "state": a.get("state_abbr", st),
                    })
                    n += 1
        print(f"  {st}: {n} agencies")
    return agencies


def fetch_population(ori, from_str, to_str):
    """Return the latest population for one agency, or None if unavailable."""
    url = (f"{CDE_API_BASE}/summarized/agency/{ori}/HOM"
           f"?from={from_str}&to={to_str}&type=counts")
    try:
        j = get_json(url)
    except Exception:
        return None
    pops = j.get("populations", {}).get("population", {})
    series = None
    for k, v in pops.items():
        if k not in NON_AGENCY_KEYS and isinstance(v, dict):
            series = v
            break
    if not series:
        return None
    # take the latest month that has a value
    best = None
    for mmyyyy, val in series.items():
        if val is None:
            continue
        mm, yyyy = mmyyyy.split("-")
        key = (int(yyyy), int(mm))
        if best is None or key > best[0]:
            best = (key, int(val))
    return best[1] if best else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="cde_populations.csv")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int, default=0,
                    help="only fetch first N agencies (testing)")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    from_str = f"01-{now.year - 1}"
    to_str = f"12-{now.year}"

    print("Fetching agency list...")
    agencies = get_agencies()
    print(f"Total agencies: {len(agencies)}")
    if args.limit:
        agencies = agencies[:args.limit]

    print(f"Fetching populations ({from_str} to {to_str}, {args.workers} threads)...")
    results = {}
    todo = [a["ori"] for a in agencies]
    for rnd in range(1, 4):
        if not todo:
            break
        if rnd > 1:
            print(f"Retry round {rnd}: {len(todo)} agencies")
            time.sleep(30)
        remaining = []
        done = 0
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(fetch_population, ori, from_str, to_str): ori
                       for ori in todo}
            for fut in as_completed(futures):
                ori = futures[fut]
                pop = fut.result()
                if pop is not None:
                    results[ori] = pop
                else:
                    remaining.append(ori)
                done += 1
                if done % 1000 == 0:
                    print(f"  {done}/{len(todo)} ({len(results)} with population)")
        todo = remaining

    print(f"Done: {len(results)} with population, {len(todo)} without")

    # Agencies that still failed keep their value from the previous CSV —
    # a transient API error must not drop an agency from the file
    carried = 0
    still_failed = set(todo)
    try:
        with open(args.output, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["ori"] in still_failed and r["population"]:
                    results[r["ori"]] = int(r["population"])
                    carried += 1
    except FileNotFoundError:
        pass
    if carried:
        print(f"Carried forward {carried} populations from previous CSV")

    # Sanity gate: a partial/failed run must not clobber good data
    if not args.limit and len(results) < 15000:
        print(f"ERROR: only {len(results)} populations fetched (expected ~17k+). "
              "Not writing output.")
        sys.exit(1)

    agencies.sort(key=lambda a: (a["state"], a["agency_name"]))
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ori", "agency_name", "state", "population"])
        n = 0
        for a in agencies:
            pop = results.get(a["ori"])
            if pop is None:
                continue
            w.writerow([a["ori"], a["agency_name"], a["state"], pop])
            n += 1
    print(f"Wrote {n} rows to {args.output}")


if __name__ == "__main__":
    main()
