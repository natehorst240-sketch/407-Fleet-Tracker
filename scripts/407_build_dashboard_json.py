import json
from pathlib import Path
from datetime import datetime

import pandas as pd

# ----------------------------
# Repo paths (portable)
# ----------------------------
INPUT_CSV = Path("data/407_daily_due_list.csv")
OUTPUT_JSON = Path("dist/data/dashboard.json")

# ----------------------------
# Inspections to track
# ----------------------------
TRACKED_INSPECTIONS = [
    {"label": "12 Month",             "match": "12MO-INSPECTION",             "mode": "contains"},
    {"label": "24 Month",             "match": "24MO.INSPECTION",             "mode": "contains"},
    {"label": "300HR/12M Airframe",   "match": "300HR-PERIODIC INSPECTION",   "mode": "contains"},
    {"label": "300HR/12M Engine",     "match": "72/300",                      "mode": "exact"},   # important
    {"label": "IFR Certs 91.411",     "match": "91.411",                      "mode": "contains"},
    {"label": "IFR Certs 91.413",     "match": "91.413",                      "mode": "contains"},
    {"label": "MR Mast Interim",      "match": "11-20 INTERIM",               "mode": "contains"},
    {"label": "Freewheel Interim",    "match": "13-11 INTERIM",               "mode": "contains"},
    {"label": "Transmission Interim", "match": "21-10 INTERIM",               "mode": "contains"},
    {"label": "TRGB Interim",         "match": "10-11 INTERIM",               "mode": "contains"},
    {"label": "Spring Link Interim",  "match": "20-12 INTERIM",               "mode": "contains"},
]

# ----------------------------
# Thresholds (date-first)
# ----------------------------
CRITICAL_DAYS = 7
COMING_DUE_DAYS = 30

CRITICAL_HOURS = 25
COMING_DUE_HOURS = 100


def _norm(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    return str(x).strip().upper()


def matches_rule(ata_value, rule) -> bool:
    ata = _norm(ata_value)
    target = _norm(rule["match"])
    if not ata:
        return False
    if rule["mode"] == "exact":
        return ata == target
    return target in ata  # contains


def parse_date_maybe(val):
    """Return ISO date (YYYY-MM-DD) or None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None

    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass

    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date().isoformat()


def classify_date_first(remaining_days, remaining_hours):
    """
    Prefer remaining_days for urgency.
    Fall back to remaining_hours if days are missing.
    """
    if remaining_days is not None:
        d = float(remaining_days)
        if d < 0:
            return "OVERDUE"
        if d <= CRITICAL_DAYS:
            return "CRITICAL"
        if d <= COMING_DUE_DAYS:
            return "COMING_DUE"
        return "OK"

    if remaining_hours is not None:
        h = float(remaining_hours)
        if h < 0:
            return "OVERDUE"
        if h <= CRITICAL_HOURS:
            return "CRITICAL"
        if h <= COMING_DUE_HOURS:
            return "COMING_DUE"
        return "OK"

    return "UNKNOWN"


def urgency_sort_key(item):
    bucket_order = {"OVERDUE": 0, "CRITICAL": 1, "COMING_DUE": 2, "OK": 3, "UNKNOWN": 4}
    bucket = bucket_order.get(item.get("status", "UNKNOWN"), 9)
    d = item.get("remaining_days")
    h = item.get("remaining_hours")
    if d is not None:
        return (bucket, d)
    if h is not None:
        return (bucket, h)
    return (bucket, 999999)


def build():
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Missing input CSV: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)

    # Defensive column names (match CAMP export)
    # Required: Item Type, ATA and Code, Registration Number
    df["Item Type"] = df["Item Type"].astype(str)
    df = df[df["Item Type"].str.upper() == "INSPECTION"].copy()

    aircraft = {}

    for _, row in df.iterrows():
        ata_value = row.get("ATA and Code")

    tail = str(row.get("Registration Number", "")).strip()
    if not tail:
        continue

    item = {
        "label": str(row.get("Description")),
        "ata": str(ata_value),
        "description": row.get("Description"),
        "next_due_date": parse_date_maybe(row.get("Next Due Date")),
        "remaining_days": remaining_days,
        "remaining_hours": remaining_hours,
        "next_due_status": row.get("Next Due Status"),
        "status": classify_date_first(remaining_days, remaining_hours),
}

            # Remaining fields
            rd = row.get("Remaining Days")
            rh = row.get("Remaining Hours")

            remaining_days = float(rd) if pd.notna(rd) else None
            remaining_hours = float(rh) if pd.notna(rh) else None

            item = {
                "label": rule["label"],
                "ata": str(ata_value) if pd.notna(ata_value) else "",
                "description": row.get("Description"),
                "next_due_date": parse_date_maybe(row.get("Next Due Date")),
                "remaining_days": remaining_days,
                "remaining_hours": remaining_hours,
                "next_due_status": row.get("Next Due Status"),
                "status": classify_date_first(remaining_days, remaining_hours),
            }

            if tail not in aircraft:
                ah = row.get("Airframe Hours")
                aircraft[tail] = {
                    "airframe_report_date": parse_date_maybe(row.get("Airframe Report Date")),
                    "airframe_hours": float(ah) if pd.notna(ah) else None,
                    "items": [],
                }

            aircraft[tail]["items"].append(item)

    # Sort items per aircraft
    for tail in aircraft:
        aircraft[tail]["items"].sort(key=urgency_sort_key)

    out = {
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "fleet": "Bell 407",
        "aircraft_count": len(aircraft),
        "aircraft": aircraft,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {OUTPUT_JSON} ({len(aircraft)} aircraft)")


if __name__ == "__main__":
    build()