#!/usr/bin/env python3
"""
FMCSA Failed Inspection Lead Generator (v2 — corrected)
---------------------------------------------------------
Pulls daily inspection data from the DOT/FMCSA Open Data Portal (Socrata API),
filters for out-of-service (OOS) violations, cross-references with the Company
Census to find carriers with 20+ power units, and outputs a clean CSV.
 
Data sources (all free, no auth required for basic use):
  - Vehicle Inspection File:          data.transportation.gov :: fx4q-ay7w
  - Company Census File:              data.transportation.gov :: 4a2k-zf79
 
Usage:
  python fetch_inspections.py                 # defaults to yesterday
  python fetch_inspections.py --date 2026-03-28
  python fetch_inspections.py --days-back 7
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
    print("ERROR: 'requests' library required.  pip install requests")
    sys.exit(1)
 
# ---------------------------------------------------------------------------
# Socrata dataset IDs on data.transportation.gov
# ---------------------------------------------------------------------------
BASE_URL           = "https://data.transportation.gov/resource"
INSPECTION_DATASET = "fx4q-ay7w"   # Vehicle Inspection File
CENSUS_DATASET     = "4a2k-zf79"   # Motor Carrier Registrations — Census
 
PAGE_SIZE       = 50000
MIN_POWER_UNITS = 20
 
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
OUTPUT_DIR = REPO_ROOT / "docs" / "data"
META_FILE  = OUTPUT_DIR / "meta.json"
 
 
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def socrata_get(dataset_id, params, app_token=None):
    """Query a Socrata dataset with pagination."""
    url = f"{BASE_URL}/{dataset_id}.json"
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token
 
    all_rows = []
    offset = 0
    while True:
        p = {**params, "$limit": PAGE_SIZE, "$offset": offset}
        resp = requests.get(url, params=p, headers=headers, timeout=120)
        if resp.status_code != 200:
            print(f"  ⚠  API returned {resp.status_code}: {resp.text[:300]}")
            resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.5)
    return all_rows
 
 
def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default
 
 
def discover_columns(dataset_id, app_token=None):
    """Fetch 1 row to discover available column names."""
    rows = socrata_get(dataset_id, {"$limit": 1}, app_token)
    if rows:
        return sorted(rows[0].keys())
    return []
 
 
def date_to_insp_format(iso_date):
    """Convert YYYY-MM-DD  ->  YYYYMMDD  (the format INSP_DATE uses)."""
    return iso_date.replace("-", "")
 
 
# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def fetch_oos_inspections(date_from, date_to, app_token=None):
    """
    Pull inspections that have OOS violations.
    Uses change_date for the daily window (this is a proper timestamp field).
    Falls back to insp_date text matching if change_date doesn't work.
    """
    # Try change_date first (proper timestamp, works with ISO dates)
    where = (
        f"change_date >= '{date_from}T00:00:00.000' "
        f"AND change_date <= '{date_to}T23:59:59.999' "
        f"AND oos_total > '0'"
    )
    print(f"  Fetching OOS inspections (change_date {date_from} to {date_to})...")
    try:
        rows = socrata_get(INSPECTION_DATASET, {"$where": where}, app_token)
        print(f"  → {len(rows):,} OOS inspection records found")
        if rows:
            return rows
    except Exception as e:
        print(f"  ⚠  change_date query failed: {e}")
        print(f"  Trying fallback with insp_date...")
 
    # Fallback: use insp_date (text field in YYYYMMDD format)
    d_from = date_to_insp_format(date_from)
    d_to   = date_to_insp_format(date_to)
    where = (
        f"insp_date >= '{d_from}' "
        f"AND insp_date <= '{d_to}' "
        f"AND oos_total > '0'"
    )
    try:
        rows = socrata_get(INSPECTION_DATASET, {"$where": where}, app_token)
        print(f"  → {len(rows):,} OOS inspection records found (via insp_date)")
        return rows
    except Exception as e:
        print(f"  ⚠  insp_date query also failed: {e}")
 
        # Last resort: discover columns and print them for debugging
        print("  Discovering available columns for debugging...")
        cols = discover_columns(INSPECTION_DATASET, app_token)
        print(f"  Available columns: {cols}")
        return []
 
 
def fetch_census_for_dots(dot_numbers, app_token=None):
    """Look up census data for a list of DOT numbers. Returns dict dot->record."""
    print(f"  Looking up census data for {len(dot_numbers):,} carriers...")
    census = {}
 
    # The census DOT number column may be 'dot_number' or 'usdot_number'
    # We'll try 'dot_number' first since that's the common Socrata field name
    dot_col = "dot_number"
 
    batch_size = 80  # keep URL length reasonable
    for i in range(0, len(dot_numbers), batch_size):
        batch = dot_numbers[i:i + batch_size]
        dot_list = " OR ".join(f"{dot_col}='{d}'" for d in batch)
        where = f"({dot_list})"
        try:
            rows = socrata_get(CENSUS_DATASET, {"$where": where}, app_token)
            for row in rows:
                dot = row.get("dot_number", row.get("usdot_number", ""))
                if dot:
                    census[str(dot)] = row
        except Exception as e:
            # If dot_number fails, try usdot_number
            if i == 0 and "dot_number" in str(e):
                dot_col = "usdot_number"
                dot_list = " OR ".join(f"{dot_col}='{d}'" for d in batch)
                where = f"({dot_list})"
                rows = socrata_get(CENSUS_DATASET, {"$where": where}, app_token)
                for row in rows:
                    dot = row.get("usdot_number", row.get("dot_number", ""))
                    if dot:
                        census[str(dot)] = row
            else:
                print(f"  ⚠  Census batch error: {e}")
        time.sleep(0.3)
 
    print(f"  → {len(census):,} census records retrieved")
    return census
 
 
def find_power_units(record):
    """Try multiple possible column names for power units count."""
    for key in ["nbr_power_unit", "tot_pwr_units", "total_power_units",
                "nbr_pwr_unit", "power_units", "total_pwr_units"]:
        if key in record:
            return safe_int(record[key])
    # If nothing found, check all keys for anything with 'pwr' or 'power'
    for key, val in record.items():
        if "pwr" in key.lower() or "power" in key.lower():
            return safe_int(val)
    return 0
 
 
def find_field(record, candidates, default=""):
    """Return the first matching field from a list of candidate column names."""
    for c in candidates:
        if c in record and record[c]:
            return str(record[c]).strip()
    return default
 
 
def build_lead_list(date_from, date_to, app_token=None):
    """Full pipeline: inspections with OOS → census filter → lead list."""
 
    # Step 1: Get inspections with OOS violations
    inspections = fetch_oos_inspections(date_from, date_to, app_token)
    if not inspections:
        print("  No OOS inspections found for this date range.")
        return []
 
    # Collect unique DOT numbers
    dot_set = set()
    for insp in inspections:
        dot = str(insp.get("dot_number", "")).strip()
        if dot and dot != "0":
            dot_set.add(dot)
 
    print(f"  {len(dot_set):,} unique carriers had OOS violations")
 
    # Step 2: Census lookup for fleet size + contact info
    census = fetch_census_for_dots(list(dot_set), app_token)
 
    # If census lookup completely failed, still output what we have from inspections
    if not census:
        print("  ⚠  Census lookup returned no data — outputting inspection data only")
        # Still produce leads using inspection-embedded carrier info
        leads = []
        for insp in inspections:
            dot = str(insp.get("dot_number", "")).strip()
            leads.append({
                "dot_number":          dot,
                "legal_name":          find_field(insp, ["insp_carrier_name", "carrier_name"]),
                "phone":               "",
                "email":               "",
                "physical_address":    find_field(insp, ["insp_carrier_street"]),
                "physical_city":       find_field(insp, ["insp_carrier_city"]),
                "physical_state":      find_field(insp, ["insp_carrier_state"]),
                "physical_zip":        find_field(insp, ["insp_carrier_zip_code"]),
                "power_units":         "unknown",
                "drivers":             "unknown",
                "inspection_date":     find_field(insp, ["insp_date"]),
                "oos_total":           safe_int(insp.get("oos_total", 0)),
                "vehicle_oos":         safe_int(insp.get("vehicle_oos_total", 0)),
                "driver_oos":          safe_int(insp.get("driver_oos_total", 0)),
                "inspection_state":    find_field(insp, ["report_state"]),
                "inspection_level":    find_field(insp, ["insp_level_id", "level"]),
                "safer_link":          f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnap&query_param=USDOT&query_string={dot}",
            })
        print(f"  ✓ {len(leads):,} leads generated (census data unavailable — no fleet size filter applied)")
        return leads
 
    # Step 3: Build filtered lead list
    leads = []
    for insp in inspections:
        dot = str(insp.get("dot_number", "")).strip()
        company = census.get(dot, {})
 
        power_units = find_power_units(company)
        if power_units < MIN_POWER_UNITS:
            continue
 
        leads.append({
            "dot_number":          dot,
            "legal_name":          find_field(company, ["legal_name", "name"]),
            "dba_name":            find_field(company, ["dba_name", "dba"]),
            "phone":               find_field(company, ["telephone", "phone", "phone_number"]),
            "email":               find_field(company, ["email_address", "email"]),
            "physical_address":    find_field(company, ["phy_street", "physical_address"]),
            "physical_city":       find_field(company, ["phy_city", "physical_city"]),
            "physical_state":      find_field(company, ["phy_state", "physical_state"]),
            "physical_zip":        find_field(company, ["phy_zip", "physical_zip"]),
            "mailing_address":     find_field(company, ["m_street", "mailing_address"]),
            "mailing_city":        find_field(company, ["m_city", "mailing_city"]),
            "mailing_state":       find_field(company, ["m_state", "mailing_state"]),
            "mailing_zip":         find_field(company, ["m_zip", "mailing_zip"]),
            "power_units":         power_units,
            "drivers":             safe_int(find_field(company, ["drivers", "nbr_drivers", "total_drivers"], "0")),
            "carrier_operation":   find_field(company, ["carrier_operation", "carrop"]),
            "inspection_date":     find_field(insp, ["insp_date"]),
            "oos_total":           safe_int(insp.get("oos_total", 0)),
            "vehicle_oos":         safe_int(insp.get("vehicle_oos_total", 0)),
            "driver_oos":          safe_int(insp.get("driver_oos_total", 0)),
            "total_violations":    safe_int(insp.get("viol_total", 0)),
            "inspection_state":    find_field(insp, ["report_state"]),
            "inspection_level":    find_field(insp, ["insp_level_id", "level"]),
            "safer_link":          f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnap&query_param=USDOT&query_string={dot}",
        })
 
    print(f"  ✓ {len(leads):,} leads generated (carriers with {MIN_POWER_UNITS}+ power units & OOS violations)")
    return leads
 
 
def write_csv(leads, date_from, date_to):
    """Write leads to CSV and update metadata JSON."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
 
    filename = f"fmcsa_leads_{date_from}.csv" if date_from == date_to else f"fmcsa_leads_{date_from}_to_{date_to}.csv"
    filepath = OUTPUT_DIR / filename
 
    if not leads:
        print(f"\n  No leads to write for {date_from}.")
        filepath.write_text("No carriers with 20+ power units had OOS violations on this date.\n")
    else:
        fieldnames = list(leads[0].keys())
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(leads)
        print(f"\n  ✓ CSV written: {filepath} ({len(leads)} rows)")
 
    # Write latest copy
    latest_path = OUTPUT_DIR / "latest.csv"
    if leads:
        fieldnames = list(leads[0].keys())
        with open(latest_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(leads)
 
    # Update metadata
    meta = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "date_from":    date_from,
        "date_to":      date_to,
        "lead_count":   len(leads),
        "latest_file":  filename,
    }
 
    history = []
    if META_FILE.exists():
        try:
            existing = json.loads(META_FILE.read_text())
            history = existing.get("history", [])
        except (json.JSONDecodeError, KeyError):
            pass
 
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
    parser.add_argument("--date", help="Specific date (YYYY-MM-DD). Default: yesterday.")
    parser.add_argument("--days-back", type=int, default=1, help="Days back (default 1)")
    parser.add_argument("--app-token", default=None, help="Socrata app token")
    parser.add_argument("--debug-columns", action="store_true",
                        help="Print available column names and exit (for troubleshooting)")
    args = parser.parse_args()
 
    app_token = args.app_token or os.environ.get("SOCRATA_APP_TOKEN")
 
    # Debug mode: just print column names
    if args.debug_columns:
        print("Inspection file columns:")
        cols = discover_columns(INSPECTION_DATASET, app_token)
        for c in cols:
            print(f"  {c}")
        print("\nCensus file columns:")
        cols = discover_columns(CENSUS_DATASET, app_token)
        for c in cols:
            print(f"  {c}")
        return
 
    if args.date:
        date_from = args.date
        date_to   = args.date
    else:
        today     = datetime.utcnow().date()
        date_to   = (today - timedelta(days=1)).isoformat()
        date_from = (today - timedelta(days=args.days_back)).isoformat()
 
    print("=" * 60)
    print("  FMCSA Failed Inspection Lead Generator v2")
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
