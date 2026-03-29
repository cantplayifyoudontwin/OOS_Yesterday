#!/usr/bin/env python3
"""
FMCSA Failed Inspection Lead Generator
---------------------------------------
Pulls daily inspection data from the DOT/FMCSA Open Data Portal (Socrata API),
filters for out-of-service (OOS) violations, cross-references with the Company
Census to find carriers with 20+ power units, and outputs a clean CSV for sales.

Data sources (all free, no auth required for basic use):
  - Vehicle Inspection File:        data.transportation.gov :: fx4q-ay7w
  - Vehicle Inspections & Violations: data.transportation.gov :: niy2-gm2b
  - Company Census File:            data.transportation.gov :: az4p-a2qs

Usage:
  python fetch_inspections.py                 # defaults to yesterday
  python fetch_inspections.py --date 2026-03-28
  python fetch_inspections.py --days-back 7   # pull last 7 days
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library required. Install with: pip install requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Socrata dataset identifiers on data.transportation.gov
# ---------------------------------------------------------------------------
BASE_URL = "https://data.transportation.gov/resource"
INSPECTION_DATASET = "fx4q-ay7w"      # Vehicle Inspection File
VIOLATION_DATASET  = "niy2-gm2b"      # Vehicle Inspections and Violations
CENSUS_DATASET     = "az4p-a2qs"      # Company Census File

# Socrata caps at 50k rows per request by default; we paginate if needed
PAGE_SIZE = 50000

# Minimum number of power units for a carrier to be included
MIN_POWER_UNITS = 20

# Output paths (relative to repo root)
SCRIPT_DIR  = Path(__file__).resolve().parent
REPO_ROOT   = SCRIPT_DIR.parent
OUTPUT_DIR  = REPO_ROOT / "docs" / "data"
META_FILE   = OUTPUT_DIR / "meta.json"


def socrata_get(dataset_id: str, params: dict, app_token: str | None = None) -> list[dict]:
    """
    Query a Socrata dataset with automatic pagination.
    Pass an app_token to avoid throttling (optional).
    """
    url = f"{BASE_URL}/{dataset_id}.json"
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token

    all_rows = []
    offset = 0

    while True:
        params_page = {**params, "$limit": PAGE_SIZE, "$offset": offset}
        resp = requests.get(url, params=params_page, headers=headers, timeout=120)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.5)  # be polite

    return all_rows


def fetch_inspections(date_from: str, date_to: str, app_token: str | None = None) -> list[dict]:
    """Fetch inspection records within a date range (inclusive)."""
    print(f"  Fetching inspections from {date_from} to {date_to}...")
    where = f"inspection_date >= '{date_from}' AND inspection_date <= '{date_to}'"
    rows = socrata_get(INSPECTION_DATASET, {"$where": where}, app_token)
    print(f"  → {len(rows):,} inspection records found")
    return rows


def fetch_oos_violations(inspection_ids: list[str], app_token: str | None = None) -> dict:
    """
    Fetch violations for given inspection IDs that have OOS (out-of-service) flag.
    Returns a dict: inspection_id -> list of violation dicts.
    """
    print(f"  Fetching violations for {len(inspection_ids):,} inspections...")
    oos_by_inspection: dict[str, list[dict]] = {}

    # Socrata has a URL length limit, so batch the inspection IDs
    batch_size = 100
    for i in range(0, len(inspection_ids), batch_size):
        batch = inspection_ids[i : i + batch_size]
        id_list = ",".join(f"'{iid}'" for iid in batch)
        where = f"inspection_id IN ({id_list}) AND oos = 'Y'"
        rows = socrata_get(VIOLATION_DATASET, {"$where": where}, app_token)
        for row in rows:
            iid = row.get("inspection_id", "")
            oos_by_inspection.setdefault(iid, []).append(row)
        time.sleep(0.3)

    total_violations = sum(len(v) for v in oos_by_inspection.values())
    print(f"  → {total_violations:,} OOS violations across {len(oos_by_inspection):,} inspections")
    return oos_by_inspection


def fetch_census_for_dots(dot_numbers: list[str], app_token: str | None = None) -> dict:
    """
    Look up census data for a list of DOT numbers.
    Returns dict: dot_number -> census record.
    """
    print(f"  Looking up census data for {len(dot_numbers):,} carriers...")
    census: dict[str, dict] = {}

    batch_size = 100
    for i in range(0, len(dot_numbers), batch_size):
        batch = dot_numbers[i : i + batch_size]
        dot_list = ",".join(f"'{d}'" for d in batch)
        where = f"dot_number IN ({dot_list})"
        rows = socrata_get(CENSUS_DATASET, {"$where": where}, app_token)
        for row in rows:
            dot = row.get("dot_number", "")
            census[dot] = row
        time.sleep(0.3)

    print(f"  → {len(census):,} census records retrieved")
    return census


def safe_int(val, default=0) -> int:
    """Safely convert a value to int."""
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def build_lead_list(date_from: str, date_to: str, app_token: str | None = None) -> list[dict]:
    """
    Full pipeline: inspections → OOS violations → census filter → lead list.
    """
    # Step 1: Get inspections in date range
    inspections = fetch_inspections(date_from, date_to, app_token)
    if not inspections:
        print("  No inspections found for this date range.")
        return []

    # Build lookup: inspection_id -> inspection record
    insp_lookup = {}
    for insp in inspections:
        iid = insp.get("inspection_id", "")
        if iid:
            insp_lookup[iid] = insp

    # Step 2: Get OOS violations for those inspections
    oos_violations = fetch_oos_violations(list(insp_lookup.keys()), app_token)
    if not oos_violations:
        print("  No OOS violations found.")
        return []

    # Collect unique DOT numbers that had OOS violations
    dot_numbers_with_oos = set()
    for iid in oos_violations:
        insp = insp_lookup.get(iid, {})
        dot = insp.get("dot_number", "")
        if dot:
            dot_numbers_with_oos.add(dot)

    print(f"  {len(dot_numbers_with_oos):,} unique carriers had OOS violations")

    # Step 3: Census lookup — get carrier info and filter by power units
    census = fetch_census_for_dots(list(dot_numbers_with_oos), app_token)

    # Step 4: Build the final lead list
    leads = []
    for iid, violations in oos_violations.items():
        insp = insp_lookup.get(iid, {})
        dot = insp.get("dot_number", "")
        company = census.get(dot, {})

        power_units = safe_int(company.get("nbr_power_unit", 0))
        if power_units < MIN_POWER_UNITS:
            continue

        # Summarize violation codes
        violation_codes = [v.get("violation_code", "N/A") for v in violations]
        violation_descriptions = [v.get("description", "") for v in violations]

        leads.append({
            "dot_number":           dot,
            "legal_name":           company.get("legal_name", "N/A"),
            "dba_name":             company.get("dba_name", ""),
            "phone":                company.get("telephone", "N/A"),
            "email":                company.get("email_address", ""),
            "physical_address":     company.get("phy_street", ""),
            "physical_city":        company.get("phy_city", ""),
            "physical_state":       company.get("phy_state", ""),
            "physical_zip":         company.get("phy_zip", ""),
            "mailing_address":      company.get("m_street", ""),
            "mailing_city":         company.get("m_city", ""),
            "mailing_state":        company.get("m_state", ""),
            "mailing_zip":          company.get("m_zip", ""),
            "power_units":          power_units,
            "drivers":              safe_int(company.get("drivers", 0)),
            "carrier_operation":    company.get("carrier_operation", ""),
            "inspection_id":        iid,
            "inspection_date":      insp.get("inspection_date", "")[:10],
            "inspection_state":     insp.get("insp_state", ""),
            "inspection_level":     insp.get("level", ""),
            "oos_violation_count":  len(violations),
            "violation_codes":      "; ".join(violation_codes),
            "violation_descriptions": "; ".join(
                d for d in violation_descriptions if d
            )[:500],  # truncate for CSV readability
            "safer_link":           f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnap&query_param=USDOT&query_string={dot}",
        })

    # Deduplicate: if a carrier had multiple inspections, keep all (they're separate events)
    print(f"  ✓ {len(leads):,} leads generated (carriers with 20+ power units & OOS violations)")
    return leads


def write_csv(leads: list[dict], date_from: str, date_to: str):
    """Write leads to CSV and update metadata JSON."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if date_from == date_to:
        filename = f"fmcsa_leads_{date_from}.csv"
    else:
        filename = f"fmcsa_leads_{date_from}_to_{date_to}.csv"

    filepath = OUTPUT_DIR / filename

    if not leads:
        print(f"\n  No leads to write for {date_from} — creating empty marker file.")
        filepath.write_text("No carriers with 20+ power units had OOS violations on this date.\n")
    else:
        fieldnames = list(leads[0].keys())
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(leads)
        print(f"\n  ✓ CSV written: {filepath} ({len(leads)} rows)")

    # Also write/update a "latest" symlink-style copy
    latest_path = OUTPUT_DIR / "latest.csv"
    if leads:
        with open(latest_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(leads[0].keys()))
            writer.writeheader()
            writer.writerows(leads)

    # Update metadata for the website
    meta = {
        "last_updated":  datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "date_from":     date_from,
        "date_to":       date_to,
        "lead_count":    len(leads),
        "latest_file":   filename,
    }

    # Load existing history
    history = []
    if META_FILE.exists():
        try:
            existing = json.loads(META_FILE.read_text())
            history = existing.get("history", [])
        except (json.JSONDecodeError, KeyError):
            pass

    # Prepend today's run (keep last 30 days of history)
    history.insert(0, {
        "date":       date_from if date_from == date_to else f"{date_from} to {date_to}",
        "file":       filename,
        "lead_count": len(leads),
        "generated":  meta["last_updated"],
    })
    history = history[:30]

    meta["history"] = history
    META_FILE.write_text(json.dumps(meta, indent=2))
    print(f"  ✓ Metadata updated: {META_FILE}")


def main():
    parser = argparse.ArgumentParser(description="FMCSA Failed Inspection Lead Generator")
    parser.add_argument("--date", help="Specific date to pull (YYYY-MM-DD). Defaults to yesterday.")
    parser.add_argument("--days-back", type=int, default=1,
                        help="Number of days back to pull (default: 1 = yesterday only)")
    parser.add_argument("--app-token", default=None,
                        help="Socrata app token (optional, avoids throttling). "
                             "Can also set SOCRATA_APP_TOKEN env var.")
    args = parser.parse_args()

    app_token = args.app_token or os.environ.get("SOCRATA_APP_TOKEN")

    if args.date:
        date_from = args.date
        date_to   = args.date
    else:
        today     = datetime.utcnow().date()
        date_to   = (today - timedelta(days=1)).isoformat()
        date_from = (today - timedelta(days=args.days_back)).isoformat()

    print("=" * 60)
    print("  FMCSA Failed Inspection Lead Generator")
    print("=" * 60)
    print(f"  Date range: {date_from} → {date_to}")
    print(f"  Min power units: {MIN_POWER_UNITS}")
    print(f"  App token: {'set' if app_token else 'not set (may be throttled)'}")
    print("-" * 60)

    leads = build_lead_list(date_from, date_to, app_token)
    write_csv(leads, date_from, date_to)

    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
