"""
FBI CDE Population Scraper (RETA-based)
========================================
Downloads the Return-A master file from the FBI CDE bulk download API,
parses agency populations from the fixed-width records, and outputs a CSV.

For any ORIs missing from RETA, falls back to the CDE summarized API.

Usage:
    python scraper.py                    # default: RETA 2026
    python scraper.py --year 2025        # specific year
    python scraper.py --output my.csv    # custom filename
"""

import argparse
import csv
import json
import os
import sys
import zipfile
import tempfile
from datetime import datetime
from io import BytesIO
from urllib.request import urlopen, Request

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

CDE_SIGNED_URL = "https://cde.ucr.cjis.gov/LATEST/s3/signedurl?key=master_files/reta/reta-{year}.zip"
CDE_API_BASE = "https://cde.ucr.cjis.gov/LATEST"
API_KEY = os.environ.get("CDE_API_KEY", "BPkjHOgf6hpOoYlRm7GOaHbSQqlx87IfiXP3QTJg")

STATE_CODES = {
    "50": "AK", "01": "AL", "03": "AR", "54": "AS", "02": "AZ", "04": "CA",
    "05": "CO", "06": "CT", "52": "CZ", "08": "DC", "07": "DE", "09": "FL",
    "10": "GA", "55": "GU", "51": "HI", "14": "IA", "11": "ID", "12": "IL",
    "13": "IN", "15": "KS", "16": "KY", "17": "LA", "20": "MA", "19": "MD",
    "18": "ME", "21": "MI", "22": "MN", "24": "MO", "23": "MS", "25": "MT",
    "32": "NC", "33": "ND", "26": "NE", "28": "NH", "29": "NJ", "30": "NM",
    "27": "NV", "31": "NY", "34": "OH", "35": "OK", "36": "OR", "37": "PA",
    "53": "PR", "38": "RI", "39": "SC", "40": "SD", "41": "TN", "42": "TX",
    "43": "UT", "62": "VI", "45": "VA", "44": "VT", "46": "WA", "48": "WI",
    "47": "WV", "49": "WY",
}

STATE_NAMES_SET = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
    "District of Columbia", "United States",
}

# ORIs to backfill via API if missing from RETA (none currently)
BACKFILL_ORIS = []


def download_reta(year):
    """Download and return RETA zip contents."""
    print(f"Downloading RETA {year}...")
    url = CDE_SIGNED_URL.format(year=year)
    resp = urlopen(Request(url), timeout=30)
    signed_data = json.loads(resp.read())
    signed_url = list(signed_data.values())[0]

    resp2 = urlopen(signed_url, timeout=120)
    content = resp2.read()
    print(f"  Downloaded {len(content):,} bytes")
    return content


def parse_reta(zip_bytes):
    """Parse RETA fixed-width file, return dict of {ori7: {ori, agency_name, state, population}}."""
    z = zipfile.ZipFile(BytesIO(zip_bytes))
    txt_file = [n for n in z.namelist() if n.endswith(".txt") or n.endswith(".dat")][0]
    data = z.read(txt_file).decode("ascii", errors="replace")
    lines = data.split("\n")

    agencies = {}
    for line in lines:
        if len(line) < 305 or line[0] != "1":
            continue
        state_code = line[1:3]
        ori = line[3:10].strip()
        pop_str = line[44:53].strip()
        name = line[120:144].strip()
        state = STATE_CODES.get(state_code, state_code)

        try:
            pop = int(pop_str) if pop_str else 0
        except ValueError:
            pop = 0

        if ori and pop > 0:
            ori9 = ori + "00" if len(ori) == 7 else ori
            agencies[ori] = {
                "ori": ori9,
                "agency_name": name,
                "state": state,
                "population": pop,
            }

    print(f"  Parsed {len(agencies):,} agencies with population")
    return agencies


def backfill_api(oris):
    """Fetch population for specific ORIs via CDE summarized API."""
    if not oris:
        return {}

    print(f"Backfilling {len(oris)} ORIs via API...")
    now = datetime.now()
    from_date = "01-2020"
    to_date = f"{now.month:02d}-{now.year}"
    results = {}

    for ori in oris:
        try:
            url = f"{CDE_API_BASE}/summarized/agency/{ori}/homicide?from={from_date}&to={to_date}&api_key={API_KEY}"
            resp = urlopen(Request(url), timeout=30)
            data = json.loads(resp.read())

            pop_dict = data.get("populations", {}).get("population", {})
            for key, vals in pop_dict.items():
                if key in STATE_NAMES_SET:
                    continue
                if isinstance(vals, dict) and vals:
                    sorted_months = sorted(vals.keys(), key=lambda d: (
                        int(d.split('-')[1]) * 100 + int(d.split('-')[0])
                    ))
                    for m in reversed(sorted_months):
                        v = vals[m]
                        if v and v > 0:
                            results[ori[:7]] = {
                                "ori": ori[:7],
                                "agency_name": key,
                                "state": ori[:2],
                                "population": int(v),
                            }
                            print(f"  {ori}: {key} = {v:,}")
                            break
                break
        except Exception as e:
            print(f"  {ori}: API error — {e}")

    return results


def main():
    parser = argparse.ArgumentParser(description="CDE Population Scraper (RETA-based)")
    parser.add_argument("--year", type=int, default=datetime.now().year,
                        help="RETA year to download (default: current year)")
    parser.add_argument("--output", default="cde_populations.csv")
    args = parser.parse_args()

    print(f"FBI CDE Population Scraper")
    print(f"{'=' * 50}")

    # Step 1: Download and parse RETA
    zip_bytes = download_reta(args.year)
    agencies = parse_reta(zip_bytes)

    # Step 2: Try previous year if current year has fewer agencies
    if args.year == datetime.now().year:
        prev_zip = download_reta(args.year - 1)
        prev_agencies = parse_reta(prev_zip)
        # Merge: use current year pop if available, fall back to previous year
        for ori, info in prev_agencies.items():
            if ori not in agencies:
                agencies[ori] = info
        print(f"  After merging {args.year - 1}: {len(agencies):,} total agencies")

    # Step 3: Backfill missing ORIs via API
    if BACKFILL_ORIS:
        missing = [ori for ori in BACKFILL_ORIS if ori[:7] not in agencies]
        if missing:
            api_results = backfill_api(missing)
            agencies.update(api_results)
            print(f"  After API backfill: {len(agencies):,} total agencies")

    # Step 4: Write CSV
    print(f"\nWriting {args.output}...")
    fieldnames = ["ori", "agency_name", "state", "population"]
    rows = sorted(agencies.values(), key=lambda r: (r["state"], r["agency_name"]))

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  {len(rows):,} agencies written to {args.output}")


if __name__ == "__main__":
    main()
