"""
Bell 407 Dashboard Builder
==========================
Reads:
  data/407_daily_due_list.csv         — daily Veryon export (primary)
  data/407_Due-List_weekly.csv        — weekly Veryon export (merged into history)
  data/407_flight_hours_history.json  — accumulated daily snapshots (auto-created)

Writes:
  dist/data/dashboard.json            — consumed by public/index.html
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# ---- Paths ----------------------------------------------------------------
DAILY_CSV    = Path(os.getenv("VERYON_407_CSV",    "data/407_daily_due_list.csv"))
WEEKLY_CSV   = Path(os.getenv("VERYON_407_WEEKLY", "data/407_Due-List_weekly.csv"))
HISTORY_JSON = Path("data/407_flight_hours_history.json")
OUTPUT_JSON  = Path("dist/data/dashboard.json")

# ---- Inspections to track -------------------------------------------------
TRACKED_INSPECTIONS = [
    {"label": "12 Month",           "match": "05 12MO- INSPECTION",          "mode": "exact"},
    {"label": "24 Month",           "match": "05 24MO. INSPECTION",          "mode": "exact"},
    {"label": "300HR/12M Airframe", "match": "05 300HR- PERIODIC INSPECTION","mode": "exact"},
    {"label": "300HR/12M Engine",   "match": "72 72/300",                    "mode": "exact"},
    {"label": "600HR/12M Engine",   "match": "72 INSP 600HR/12MO",           "mode": "exact"},
    {"label": "TRGB Interim",       "match": "65 10-11 INTERIM",             "mode": "exact"},
]

# ---- Thresholds -----------------------------------------------------------
CRITICAL_DAYS    = 7
COMING_DUE_DAYS  = 30
CRITICAL_HOURS   = 25
COMING_DUE_HOURS = 100


# ---- Helpers --------------------------------------------------------------

def _norm(s) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip()


def _read_csv(path: Path) -> pd.DataFrame:
    """Read a Veryon CSV, trying utf-8-sig first then cp1252."""
    for enc in ("utf-8-sig", "cp1252"):
        try:
            df = pd.read_csv(path, encoding=enc)
            df.columns = [c.lstrip("﻿") for c in df.columns]
            return df
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode {path}")
def matches_rule(ata_value, rule) -> bool:
    ata    = _norm(ata_value)
    target = _norm(rule["match"])
    if not ata:
        return False
    if rule["mode"] == "exact":
        return ata == target
    return target.upper() in ata.upper()


def parse_date_iso(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    dt = pd.to_datetime(s, errors="coerce")
    return None if pd.isna(dt) else dt.date().isoformat()


def classify(remaining_days, remaining_hours) -> str:
    if remaining_days is not None:
        d = float(remaining_days)
        if d < 0:                return "OVERDUE"
        if d <= CRITICAL_DAYS:   return "CRITICAL"
        if d <= COMING_DUE_DAYS: return "COMING DUE"
        return "OK"
    if remaining_hours is not None:
        h = float(remaining_hours)
        if h < 0:                 return "OVERDUE"
        if h <= CRITICAL_HOURS:   return "CRITICAL"
        if h <= COMING_DUE_HOURS: return "COMING DUE"
        return "OK"
    return "UNKNOWN"


def urgency_key(item):
    order = {"OVERDUE": 0, "CRITICAL": 1, "COMING DUE": 2, "OK": 3, "UNKNOWN": 4}
    bucket = order.get(item.get("status", "UNKNOWN"), 9)
    d = item.get("remaining_days")
    h = item.get("remaining_hours")
    if d is not None: return (bucket, d)
    if h is not None: return (bucket, h)
    return (bucket, 999_999)


# ---- History --------------------------------------------------------------

def load_history() -> dict:
    if HISTORY_JSON.exists():
        try:
            with open(HISTORY_JSON, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_history(history: dict):
    HISTORY_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_JSON, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)


def collect_snapshots(df: pd.DataFrame) -> list:
    """Extract (tail, date_iso, airframe_hours) tuples from a DataFrame."""
    seen = set()
    result = []
    rows = df[["Registration Number", "Airframe Report Date", "Airframe Hours"]].drop_duplicates()
    for _, row in rows.iterrows():
        tail     = _norm(row["Registration Number"])
        date_iso = parse_date_iso(row["Airframe Report Date"])
        try:
            hours = float(row["Airframe Hours"])
        except (ValueError, TypeError):
            continue
        if not tail or not date_iso or pd.isna(hours):
            continue
        key = (tail, date_iso)
        if key not in seen:
            seen.add(key)
            result.append((tail, date_iso, hours))
    return result


def update_history(history: dict, snapshots: list) -> dict:
    """Merge snapshots into history without overwriting existing dates."""
    for tail, date_iso, hours in snapshots:
        history.setdefault(tail, {})
        if date_iso not in history[tail]:
            history[tail][date_iso] = {"hours": hours}
    return history


# ---- Utilization stats ----------------------------------------------------

def calculate_utilization(history: dict) -> dict:
    """
    Calculate avg_daily from accumulated history using best available span:
    30-day → 7-day → any span with >= 2 data points.
    """
    today           = datetime.today()
    seven_ago_str   = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    thirty_ago_str  = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    stats = {}

    for tail, snapshots in history.items():
        sorted_dates = sorted(snapshots.keys(), reverse=True)  # newest first

        # Daily data for chart (last 7 entries, chronological order)
        daily_data = [
            {"date": d, "hours": snapshots[d]["hours"]}
            for d in sorted(sorted_dates[:7])
        ]

        if len(sorted_dates) < 2:
            stats[tail] = {"avg_daily": None, "daily_data": daily_data,
                           "current_hours": snapshots[sorted_dates[0]]["hours"] if sorted_dates else None}
            continue

        latest_hours = snapshots[sorted_dates[0]]["hours"]

        # Try 30-day span
        monthly_hours = None
        for d in sorted_dates:
            if d <= thirty_ago_str:
                monthly_hours = latest_hours - snapshots[d]["hours"]
                break

        # Try 7-day span
        weekly_hours = None
        for d in sorted_dates:
            if d <= seven_ago_str:
                weekly_hours = latest_hours - snapshots[d]["hours"]
                break

        avg_daily = None
        if monthly_hours is not None:
            span = (today - datetime.strptime(thirty_ago_str, "%Y-%m-%d")).days
            if span > 0:
                avg_daily = monthly_hours / span
        elif weekly_hours is not None:
            span = (today - datetime.strptime(seven_ago_str, "%Y-%m-%d")).days
            if span > 0:
                avg_daily = weekly_hours / span
        else:
            # Use whatever span exists
            oldest = sorted_dates[-1]
            newest = sorted_dates[0]
            span = (datetime.strptime(newest, "%Y-%m-%d") -
                    datetime.strptime(oldest, "%Y-%m-%d")).days
            if span > 0:
                avg_daily = (latest_hours - snapshots[oldest]["hours"]) / span

        stats[tail] = {
            "avg_daily":          avg_daily,
            "projection_weekly":  avg_daily * 7  if avg_daily is not None else None,
            "projection_monthly": avg_daily * 30 if avg_daily is not None else None,
            "daily_data":         daily_data,
            "current_hours":      latest_hours,
        }

    return stats


# ---- Main build -----------------------------------------------------------

def build():
    if not DAILY_CSV.exists():
        raise FileNotFoundError(
            f"Missing daily Veryon CSV: {DAILY_CSV}\n"
            "Set VERYON_407_CSV env var to override."
        )

    daily_df = _read_csv(DAILY_CSV)
    insp_df  = daily_df[daily_df["Item Type"].astype(str).str.upper() == "INSPECTION"].copy()

    # Accumulate flight hours history from both CSVs
    history   = load_history()
    snapshots = collect_snapshots(daily_df)
    if WEEKLY_CSV.exists():
        weekly_df = _read_csv(WEEKLY_CSV)
        snapshots += collect_snapshots(weekly_df)
    history = update_history(history, snapshots)
    save_history(history)

    utilization = calculate_utilization(history)

    # Report date from daily file
    report_date = None
    try:
        dates = pd.to_datetime(daily_df["Airframe Report Date"], errors="coerce").dropna()
        if not dates.empty:
            report_date = dates.max().date().isoformat()
    except Exception:
        pass

    # Build per-aircraft inspection data
    aircraft_dict: dict = {}

    for _, row in insp_df.iterrows():
        ata_value = row.get("ATA and Code")
        for rule in TRACKED_INSPECTIONS:
            if not matches_rule(ata_value, rule):
                continue
            tail = _norm(row.get("Registration Number", ""))
            if not tail:
                continue

            rd = row.get("Remaining Days")
            rh = row.get("Remaining Hours")
            remaining_days  = float(rd) if pd.notna(rd) else None
            remaining_hours = float(rh) if pd.notna(rh) else None

            item = {
                "inspection":      rule["label"],
                "ata":             _norm(ata_value),
                "description":     _norm(row.get("Description", "")),
                "due_date":        parse_date_iso(row.get("Next Due Date")),
                "remaining_days":  remaining_days,
                "remaining_hours": remaining_hours,
                "next_due_status": _norm(row.get("Next Due Status", "")),
                "status":          classify(remaining_days, remaining_hours),
            }

            if tail not in aircraft_dict:
                aircraft_dict[tail] = {
                    "airframe_report_date": parse_date_iso(row.get("Airframe Report Date")),
                    "airframe_hours": float(row["Airframe Hours"]) if pd.notna(row.get("Airframe Hours")) else None,
                    "items": [],
                }
            aircraft_dict[tail]["items"].append(item)

    for data in aircraft_dict.values():
        data["items"].sort(key=urgency_key)

    # Assemble output array with utilization merged in
    aircraft_list = []
    for tail in sorted(aircraft_dict.keys()):
        data = aircraft_dict[tail]
        util = utilization.get(tail, {})
        aircraft_list.append({
            "tail":               tail,
            "airframe_hours":     data["airframe_hours"],
            "airframe_report_date": data["airframe_report_date"],
            "avg_daily":          util.get("avg_daily"),
            "projection_weekly":  util.get("projection_weekly"),
            "projection_monthly": util.get("projection_monthly"),
            "daily_data":         util.get("daily_data", []),
            "items":              data["items"],
        })

    # ---- Build components list (PART items + OVERHAUL inspections) ----------
    COMPONENT_WINDOW = 200
    components = []

    # PART items — use the full daily CSV (not inspection-filtered)
    parts = daily_df[daily_df["Item Type"].astype(str).str.upper() == "PART"].copy()
    parts["Remaining Hours"] = pd.to_numeric(parts["Remaining Hours"], errors="coerce")
    for _, row in parts.iterrows():
        rh = row["Remaining Hours"]
        if pd.isna(rh) or rh > COMPONENT_WINDOW:
            continue
        components.append({
            "tail":            _norm(row.get("Registration Number","")),
            "ata":             _norm(row.get("ATA and Code","")),
            "description":     _norm(row.get("Description","")),
            "item_type":       "PART",
            "remaining_hours": float(rh),
        })

    # INSPECTION items with Requirement Type = OVERHAUL
    insp_oh = daily_df[
        (daily_df["Item Type"].astype(str).str.upper() == "INSPECTION") &
        (daily_df["Requirement Type"].astype(str).str.upper() == "OVERHAUL")
    ].copy()
    insp_oh["Remaining Hours"] = pd.to_numeric(insp_oh["Remaining Hours"], errors="coerce")
    for _, row in insp_oh.iterrows():
        rh = row["Remaining Hours"]
        if pd.isna(rh) or rh > COMPONENT_WINDOW:
            continue
        components.append({
            "tail":            _norm(row.get("Registration Number","")),
            "ata":             _norm(row.get("ATA and Code","")),
            "description":     _norm(row.get("Description","")),
            "item_type":       "OVERHAUL",
            "remaining_hours": float(rh),
        })

    components.sort(key=lambda c: c["remaining_hours"])

    out = {
        "meta": {
            "report_date":    report_date,
            "generated_utc":  datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "fleet_name":     "Bell 407",
            "source":         str(DAILY_CSV),
            "aircraft_count": len(aircraft_list),
        },
        "aircraft": aircraft_list,
        "components": components,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Read:    {DAILY_CSV} ({len(insp_df)} inspection rows)")
    if WEEKLY_CSV.exists():
        print(f"Weekly:  {WEEKLY_CSV} (merged into history)")
    print(f"History: {HISTORY_JSON} ({sum(len(v) for v in history.values())} total snapshots)")
    print(f"Built:   {OUTPUT_JSON} ({len(aircraft_list)} aircraft)")
    print("\nUtilization summary:")
    for a in aircraft_list:
        avg = a["avg_daily"]
        pts = len(a["daily_data"])
        avg_str = f"{avg:.3f} hrs/day" if avg is not None else "no data yet"
        print(f"  {a['tail']:12s}  {avg_str}  ({pts} history pts)")


if __name__ == "__main__":
    build()
