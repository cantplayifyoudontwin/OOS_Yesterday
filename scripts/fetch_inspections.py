#!/usr/bin/env python3
"""
FMCSA Failed Inspection Lead Generator (v3)
--------------------------------------------
Pulls daily inspection data from the DOT/FMCSA Open Data Portal,
filters for out-of-service (OOS) violations, and enriches with carrier
details from the FMCSA QCMobile API to filter by fleet size.
 
Data sources:
  - Vehicle Inspection File: data.transportation.gov :: fx4q-ay7w  (Socrata, free, no auth)
  - Carrier details:         mobile.fmcsa.dot.gov QCMobile API     (free, requires webkey)
 
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
# Config
# ---------------------------------------------------------------------------
SOCRATA_BASE     = "https://data.transportation.gov/resource"
INSPECTION_DS    = "fx4q-ay7w"
QCMOBILE_BASE    = "https://mobile.fmcsa.dot.gov/qc/services/carriers"
 
PAGE_SIZE        = 50000
MIN_POWER_UNITS  = 20
 
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
OUTPUT_DIR = REPO_ROOT / "docs" / "data"
META_FILE  = OUTPUT_DIR / "meta.json"
 
 
# ---------------------------------------------------------------------------
# Socrata helpers
# ---------------------------------------------------------------------------
def socrata_get(dataset_id, params, app_token=None):
    url = f"{SOCRATA_BASE}/{dataset_id}.json"
    headers = {"X-App-Token": app_token} if app_token else {}
    all_rows, offset = [], 0
    while True:
        p = {**params, "$limit": PAGE_SIZE, "$offset": offset}
        resp = requests.get(url, params=p, headers=headers, timeout=120)
        if resp.status_code != 200:
            print(f"  ⚠  Socrata {resp.status_code}: {resp.text[:200]}")
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
 
 
# ---------------------------------------------------------------------------
# FMCSA QCMobile API helpers
# ---------------------------------------------------------------------------
def qcmobile_get_carrier(dot_number, webkey):
    """Look up a single carrier by DOT number. Returns dict or None."""
    url = f"{QCMOBILE_BASE}/{dot_number}?webKey={webkey}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            # Response wraps carrier in a "content" key with a list
            if "content" in data and isinstance(data["content"], list) and data["content"]:
                carrier = data["content"][0].get("carrier", {})
                return carrier
            elif "carrier" in data:
                return data["carrier"]
            return data
        else:
            return None
    except Exception:
        return None
 
 
def batch_carrier_lookup(dot_numbers, webkey):
    """
    Look up multiple carriers via QCMobile API.
    Returns dict: dot_number_str -> carrier_record
    """
    results = {}
    total = len(dot_numbers)
    print(f"  Looking up {total:,} carriers via FMCSA QCMobile API...")
    print(f"  (This may take a few minutes for large batches)")
 
    for i, dot in enumerate(dot_numbers):
        carrier = qcmobile_get_carrier(dot, webkey)
        if carrier:
            results[str(dot)] = carrier
 
        # Progress update every 200
        if (i + 1) % 200 == 0:
            print(f"    ... {i+1:,}/{total:,} looked up ({len(results):,} found)")
 
        # Rate limit: ~5 per second to be safe
        time.sleep(0.2)
 
    print(f"  → {len(results):,} carrier records retrieved")
    return results
 
 
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default
 
 
def get_field(record, key, default=""):
    val = record.get(key, default)
    return str(val).strip() if val else default
 
 
# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def fetch_oos_inspections(date_from, date_to, app_token=None):
    where = (
        f"change_date >= '{date_from}T00:00:00.000' "
        f"AND change_date <= '{date_to}T23:59:59.999' "
        f"AND oos_total > '0'"
    )
    print(f"  Fetching OOS inspections ({date_from} to {date_to})...")
    rows = socrata_get(INSPECTION_DS, {"$where": where}, app_token)
    print(f"  → {len(rows):,} OOS inspection records found")
    return rows
 
 
def build_lead_list(date_from, date_to, app_token=None, fmcsa_webkey=None):
    # Step 1: Get inspections with OOS violations
    inspections = fetch_oos_inspections(date_from, date_to, app_token)
    if not inspections:
        print("  No OOS inspections found.")
        return []
 
    # Collect unique DOT numbers
    dot_set = set()
    for insp in inspections:
        dot = str(insp.get("dot_number", "")).strip()
        if dot and dot != "0":
            dot_set.add(dot)
    print(f"  {len(dot_set):,} unique carriers had OOS violations")
 
    # Step 2: Carrier lookup
    carrier_data = {}
    use_fleet_filter = False
 
    if fmcsa_webkey:
        use_fleet_filter = True
        carrier_data = batch_carrier_lookup(list(dot_set), fmcsa_webkey)
        if not carrier_data:
            print("  ⚠  QCMobile API returned no data. Outputting without fleet filter.")
            use_fleet_filter = False
    else:
        print("  ℹ  No FMCSA_WEBKEY set — skipping fleet size filter.")
        print("     All OOS inspections will be included. Set FMCSA_WEBKEY to filter by power units.")
 
    # Step 3: Build leads
    leads = []
    skipped_small = 0
 
    for insp in inspections:
        dot = str(insp.get("dot_number", "")).strip()
        if not dot or dot == "0":
            continue
 
        carrier = carrier_data.get(dot, {})
 
        # Fleet size filter (only if we have carrier data)
        if use_fleet_filter:
            power_units = safe_int(carrier.get("totalPowerUnits", 0))
            if power_units < MIN_POWER_UNITS:
                skipped_small += 1
                continue
        else:
            power_units = safe_int(carrier.get("totalPowerUnits", ""))
 
        # Use QCMobile data if available, fall back to inspection-embedded data
        lead = {
            "dot_number":       dot,
            "legal_name":       get_field(carrier, "legalName") or get_field(insp, "insp_carrier_name"),
            "dba_name":         get_field(carrier, "dbaName"),
            "phone":            get_field(carrier, "phyPhone") or get_field(carrier, "telephone"),
            "email":            get_field(carrier, "email"),
            "physical_address": get_field(carrier, "phyStreet") or get_field(insp, "insp_carrier_street"),
            "physical_city":    get_field(carrier, "phyCity") or get_field(insp, "insp_carrier_city"),
            "physical_state":   get_field(carrier, "phyState") or get_field(insp, "insp_carrier_state"),
            "physical_zip":     get_field(carrier, "phyZipcode") or get_field(insp, "insp_carrier_zip_code"),
            "power_units":      power_units if power_units else "",
            "drivers":          safe_int(carrier.get("totalDrivers", "")) or "",
            "inspection_date":  get_field(insp, "insp_date"),
            "oos_total":        safe_int(insp.get("oos_total", 0)),
            "vehicle_oos":      safe_int(insp.get("vehicle_oos_total", 0)),
            "driver_oos":       safe_int(insp.get("driver_oos_total", 0)),
            "total_violations": safe_int(insp.get("viol_total", 0)),
            "inspection_state": get_field(insp, "report_state"),
            "inspection_level": get_field(insp, "insp_level_id"),
            "safer_link":       f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnap&query_param=USDOT&query_string={dot}",
        }
        leads.append(lead)
 
    if use_fleet_filter:
        print(f"  Skipped {skipped_small:,} inspections (carriers with < {MIN_POWER_UNITS} power units)")
 
    print(f"  ✓ {len(leads):,} leads generated")
    return leads
 
 
def write_csv(leads, date_from, date_to):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
 
    filename = f"fmcsa_leads_{date_from}.csv" if date_from == date_to else f"fmcsa_leads_{date_from}_to_{date_to}.csv"
    filepath = OUTPUT_DIR / filename
 
    if not leads:
        filepath.write_text("No OOS violations found on this date.\n")
        print(f"\n  No leads to write.")
    else:
        fieldnames = list(leads[0].keys())
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(leads)
        print(f"\n  ✓ CSV written: {filepath} ({len(leads)} rows)")
 
    # Latest copy
    latest_path = OUTPUT_DIR / "latest.csv"
    if leads:
        with open(latest_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(leads[0].keys()))
            writer.writeheader()
            writer.writerows(leads)
 
    # Metadata
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
    meta["history"] = history[:30]
    META_FILE.write_text(json.dumps(meta, indent=2))
    print(f"  ✓ Metadata updated")
 
 
def main():
    parser = argparse.ArgumentParser(description="FMCSA OOS Lead Generator v3")
    parser.add_argument("--date", help="Specific date (YYYY-MM-DD). Default: yesterday.")
    parser.add_argument("--days-back", type=int, default=1, help="Days back (default 1)")
    parser.add_argument("--app-token", default=None, help="Socrata app token")
    parser.add_argument("--fmcsa-webkey", default=None, help="FMCSA QCMobile API webkey")
    args = parser.parse_args()
 
    app_token    = args.app_token or os.environ.get("SOCRATA_APP_TOKEN")
    fmcsa_webkey = args.fmcsa_webkey or os.environ.get("FMCSA_WEBKEY")
 
    if args.date:
        date_from = date_to = args.date
    else:
        today     = datetime.utcnow().date()
        date_to   = (today - timedelta(days=1)).isoformat()
        date_from = (today - timedelta(days=args.days_back)).isoformat()
 
    print("=" * 60)
    print("  FMCSA OOS Lead Generator v3")
    print("=" * 60)
    print(f"  Date range:   {date_from} → {date_to}")
    print(f"  Min power:    {MIN_POWER_UNITS}")
    print(f"  Socrata token: {'yes' if app_token else 'no'}")
    print(f"  FMCSA webkey:  {'yes → will filter by fleet size' if fmcsa_webkey else 'no → all OOS inspections included'}")
    print("-" * 60)
 
    leads = build_lead_list(date_from, date_to, app_token, fmcsa_webkey)
    write_csv(leads, date_from, date_to)
 
    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)
 
 
if __name__ == "__main__":
    main()
