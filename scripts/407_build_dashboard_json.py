import json
from pathlib import Path
from datetime import datetime
import re

import pandas as pd

# ----------------------------
# Repo paths (portable)
# ----------------------------
INPUT_CSV = Path("data/407_daily_due_list.csv")
OUTPUT_JSON = Path(data/dashboard.json")

# ----------------------------
# (Optional) Inspections to "flag" as tracked
# NOTE: This does NOT filter anything out.
# ----------------------------
TRACKED_INSPECTIONS = [
    {"label": "12 Month",             "match": "12MO-INSPECTION",             "mode": "contains"},
    {"label": "24 Month",             "match": "24MO-INSPECTION",             "mode": "contains"},  # fixed from dot to dash
    {"label": "300HR/12M Airframe",   "match": "300HR-PERIODIC INSPECTION",   "mode": "contains"},
    {"label": "300HR/12M Engine",     "match": "72/300",                      "mode": "exact"},
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
    """
    Normalize strings for matching:
    - uppercase
    - collapse whitespace
    - remove spaces around punctuation like '-', '.', '/'
    """
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"\s+", " ", s)                 # collapse whitespace
    s = re.sub(r"\s*([\-./])\s*", r"\1", s)    # remove spaces around - . /
    return s

def matches_rule(ata_value, rule) -> bool:
    ata = _norm(ata_value)
    target = _norm(rule["match"])
    if not ata:
        return False

    if rule["mode"] == "exact":
        # exact = match whole token OR exact string
        tokens = ata.split()
        return ata == target or target in tokens

    # contains
    return target in ata


def tracked_label_for(ata_value):
    """Return a tracked label if the ATA matches one of the tracked rules, else None."""
    for rule in TRACKED_INSPECTIONS:
        if matches_rule(ata_value, rule):
            return rule["label"]
    return None


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
    """Prefer remaining_days for urgency; fall back to remaining_hours."""
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
    df.columns = [c.strip() for c in df.columns]  # normalize headers

    # Must-have columns for this script
    required = ["Item Type", "Registration Number", "Description"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    # Keep ALL inspections (more forgiving than == "INSPECTION")
    df["Item Type"] = df["Item Type"].astype(str).str.strip().str.upper()
    df = df[df["Item Type"].str.contains("INSPECTION", na=False)].copy()

    aircraft = {}

    for _, row in df.iterrows():
        tail = str(row.get("Registration Number", "")).strip()
        if not tail:
            continue

        ata_value = row.get("ATA and Code") if "ATA and Code" in df.columns else row.get("ATA")  # fallback

        rd = row.get("Remaining Days")
        rh = row.get("Remaining Hours")
        remaining_days = float(rd) if pd.notna(rd) else None
        remaining_hours = float(rh) if pd.notna(rh) else None

        tracked_label = tracked_label_for(ata_value)

        item = {
            # Keep your original row description as the primary label
            "label": str(row.get("Description", "")).strip(),
            "ata": str(ata_value).strip() if pd.notna(ata_value) else "",
            "description": row.get("Description"),
            "next_due_date": parse_date_maybe(row.get("Next Due Date")),
            "remaining_days": remaining_days,
            "remaining_hours": remaining_hours,
            "next_due_status": row.get("Next Due Status"),
            "status": classify_date_first(remaining_days, remaining_hours),

            # Optional flags for UI highlighting
            "tracked": tracked_label is not None,
            "tracked_label": tracked_label,
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
