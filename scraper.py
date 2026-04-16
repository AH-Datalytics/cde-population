"""
FBI CDE Population Scraper
==========================
Pulls the latest population for every ORI in the FBI Crime Data Explorer.

1. Fetches all agencies by state from /agency/byStateAbbr/{state}
2. For each ORI, hits the summarized endpoint (homicide, latest month)
   to extract the population field
3. Outputs cde_populations.csv: ori, agency_name, state, county, agency_type, population

Usage:
    python scraper.py                    # full run
    python scraper.py --concurrency 20   # faster
    python scraper.py --output my.csv    # custom filename
"""

import asyncio
import aiohttp
import argparse
import csv
import json
import time
import os
import sys
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

API_KEY = os.environ.get("CDE_API_KEY", "BPkjHOgf6hpOoYlRm7GOaHbSQqlx87IfiXP3QTJg")
BASE = "https://cde.ucr.cjis.gov/LATEST"

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
    "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT",
    "VT","VA","WA","WV","WI","WY",
]

MAX_RETRIES = 3
RETRY_DELAY = 2.0


async def fetch_agencies_for_state(session, state):
    """Get all agencies for a state."""
    url = f"{BASE}/agency/byStateAbbr/{state}"
    params = {"api_key": API_KEY}
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
                    continue
                resp.raise_for_status()
                data = await resp.json()
                agencies = []
                for county_name, agency_list in data.items():
                    for a in agency_list:
                        agencies.append({
                            'ori': a['ori'],
                            'agency_name': a.get('agency_name', ''),
                            'state': a.get('state_abbr', state),
                            'state_name': a.get('state_name', ''),
                            'county': county_name,
                            'agency_type': a.get('agency_type_name', ''),
                        })
                return agencies
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == MAX_RETRIES - 1:
                print(f"  WARNING: Failed to fetch agencies for {state}")
                return []
            await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
    return []


async def fetch_population(session, sem, ori, agency_info, results, progress):
    """Fetch population for one ORI from the summarized homicide endpoint."""
    now = datetime.now()
    from_date = f"{now.month:02d}-{now.year}"
    to_date = from_date
    url = f"{BASE}/summarized/agency/{ori}/homicide"
    params = {"from": from_date, "to": to_date, "api_key": API_KEY}

    for attempt in range(MAX_RETRIES):
        async with sem:
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
                        continue
                    if resp.status != 200:
                        progress["done"] += 1
                        return
                    data = await resp.json()

                    pop_dict = data.get("populations", {}).get("population", {})
                    # Find the agency population (not state or US)
                    state_names = {
                        "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
                        "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
                        "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
                        "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
                        "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
                        "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
                        "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
                        "Virginia","Washington","West Virginia","Wisconsin","Wyoming",
                        "District of Columbia","United States",
                    }
                    pop = None
                    for key, vals in pop_dict.items():
                        if key not in state_names:
                            # Get the most recent value
                            if isinstance(vals, dict) and vals:
                                pop = list(vals.values())[-1]
                            elif isinstance(vals, (int, float)):
                                pop = int(vals)
                            break

                    if pop and pop > 0:
                        results[ori] = {**agency_info, 'population': pop}

                    progress["done"] += 1
                    if progress["done"] % 500 == 0 or progress["done"] == progress["total"]:
                        pct = progress["done"] / progress["total"] * 100
                        elapsed = time.time() - progress["start"]
                        rate = progress["done"] / elapsed if elapsed > 0 else 0
                        eta = (progress["total"] - progress["done"]) / rate if rate > 0 else 0
                        print(f"\r  {progress['done']:,}/{progress['total']:,} ({pct:.0f}%) "
                              f"- {rate:.0f} req/s, ETA {eta:.0f}s", end="", flush=True)
                    return

            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == MAX_RETRIES - 1:
                    progress["done"] += 1
                else:
                    await asyncio.sleep(RETRY_DELAY * (2 ** attempt))


async def main_async(concurrency, output):
    print(f"FBI CDE Population Scraper")
    print(f"{'='*50}")

    # Step 1: Fetch all agencies by state
    print(f"\nStep 1: Fetching agency lists for {len(STATES)} states...")
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_agencies_for_state(session, st) for st in STATES]
        state_results = await asyncio.gather(*tasks)

    all_agencies = {}
    for agencies in state_results:
        for a in agencies:
            all_agencies[a['ori']] = a

    print(f"  Found {len(all_agencies):,} unique ORIs across {len(STATES)} states")

    # Step 2: Fetch population for each ORI
    print(f"\nStep 2: Fetching populations (concurrency={concurrency})...")
    results = {}
    progress = {"done": 0, "total": len(all_agencies), "start": time.time()}

    sem = asyncio.Semaphore(concurrency)
    connector2 = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=connector2) as session:
        tasks = [
            fetch_population(session, sem, ori, info, results, progress)
            for ori, info in all_agencies.items()
        ]
        await asyncio.gather(*tasks)

    print(f"\n  Got population for {len(results):,}/{len(all_agencies):,} agencies")

    # Step 3: Write CSV
    print(f"\nStep 3: Writing {output}...")
    fieldnames = ['ori', 'agency_name', 'state', 'state_name', 'county', 'agency_type', 'population']
    rows = sorted(results.values(), key=lambda r: (r['state'], r['agency_name']))

    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  {len(rows):,} agencies written to {output}")
    print(f"\nTotal time: {time.time() - progress['start']:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="CDE Population Scraper")
    parser.add_argument("--concurrency", type=int, default=15)
    parser.add_argument("--output", default="cde_populations.csv")
    args = parser.parse_args()

    asyncio.run(main_async(args.concurrency, args.output))


if __name__ == "__main__":
    main()
