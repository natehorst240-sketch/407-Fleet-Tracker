"""Build dashboard JSON for the Bell 407 fleet page.

Inputs:
- data/407_daily_due_list.csv   (daily update - wins on any item it contains)
- data/407_Due-List_weekly.csv  (long range baseline - fills in what daily misses)

Output:
- public/data/dashboard.json
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

DAILY_CSV  = Path("data/407_daily_due_list.csv")
WEEKLY_CSV = Path("data/407_Due-List_weekly.csv")
HISTORY_JSON = Path("data/flight_hours_history.json")
OUTPUT_JSON  = Path("public/data/dashboard.json")

COMPONENT_WINDOW_HOURS = 200

INSPECTION_RULES = [
    ("12 Month",             "05 12MO- INSPECTION"),
    ("24 Month",             "05 24MO. INSPECTION"),
    ("24 Month",             "05 24.MO. INSPECTION"),
    ("300HR/12M Airframe",   "05 300HR- PERIODIC INSPECTION"),
    ("300HR/12M Engine",     "72 72/300"),
    ("600HR/12M Engine",     "72 INSP 600HR/12MO"),
    ("MR Mast Interim",      "63 11-20 INTERIM"),
    ("Freewheel Interim",    "63 13-11 INTERIM"),
    ("Transmission Interim", "63 21-10 INTERIM"),
    ("TRGB Interim",         "65 10-11 INTERIM"),
    ("TRGB Interim",         "65-10-11 INTERIM"),
    ("Spring Link Interim",  "67 20-12 INTERIM"),
]


@dataclass
class InspectionItem:
    tail: str
    inspection: str
    ata: str
    description: str
    due_date: str | None
    remaining_days: float | None
    remaining_hours: float | None


def _clean(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _norm_ata(value: Any) -> str:
    return "".join(ch for ch in _clean(value).upper() if ch.isalnum())


def _to_date(value: Any) -> str | None:
    if pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _to_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "cp1252"):
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Unable to decode {path}")


def _load_history() -> dict[str, dict[str, float]]:
    if not HISTORY_JSON.exists():
        return {}
    try:
        return json.loads(HISTORY_JSON.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_history(history: dict[str, dict[str, float]]) -> None:
    HISTORY_JSON.write_text(json.dumps(history, indent=2), encoding="utf-8")


def _update_history(
    history: dict[str, dict[str, float]], frame: pd.DataFrame
) -> dict[str, dict[str, float]]:
    """Add the latest daily CSV flight-hour readings into persisted history."""
    cols = ["Registration Number", "Airframe Report Date", "Airframe Hours"]
    missing = [c for c in cols if c not in frame.columns]
    if missing:
        return history

    for _, row in frame[cols].drop_duplicates().iterrows():
        tail = _clean(row["Registration Number"])
        dt = _to_date(row["Airframe Report Date"])
        hours = _to_float(row["Airframe Hours"])
        if not tail or not dt or hours is None:
            continue
        history.setdefault(tail, {})
        # Always accept the latest reading for the report date in case corrections are made.
        history[tail][dt] = hours
    return history


def _hour_deltas(points: list[tuple[str, float]]) -> list[dict[str, float | str | int]]:
    """Build per-update flight-hour deltas from ordered (date, hours) points."""
    deltas: list[dict[str, float | str | int]] = []
    for idx in range(1, len(points)):
        prev_date, prev_hours = points[idx - 1]
        curr_date, curr_hours = points[idx]
        days_between = max((date.fromisoformat(curr_date) - date.fromisoformat(prev_date)).days, 1)
        hours_delta = curr_hours - prev_hours
        deltas.append(
            {
                "from_date": prev_date,
                "to_date": curr_date,
                "days_between": days_between,
                "hours_delta": round(hours_delta, 3),
                "daily_rate": round(hours_delta / days_between, 3),
            }
        )
    return deltas


def _utilization(history: dict[str, dict[str, float]]) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for tail, points in history.items():
        ordered = sorted(points.items(), key=lambda x: x[0])
        deltas = _hour_deltas(ordered)
        if not deltas:
            stats[tail] = {
                "avg_daily_hours": None,
                "avg_weekly_hours": None,
                "daily_points": [],
                "hour_deltas": [],
            }
            continue

        total_hours = sum(float(item["hours_delta"]) for item in deltas)
        total_days = sum(int(item["days_between"]) for item in deltas)
        avg_daily = total_hours / total_days if total_days else 0.0

        days = [{"date": dt, "hours": hrs} for dt, hrs in ordered[-30:]]
        stats[tail] = {
            "avg_daily_hours":  round(avg_daily, 3),
            "avg_weekly_hours": round(avg_daily * 7, 3),
            "daily_points":     days,
            "hour_deltas":      deltas[-30:],
        }
    return stats


def _collect_inspections(df: pd.DataFrame) -> list[InspectionItem]:
    """
    Parse inspection items from a dataframe.
    Filters to Item Type == INSPECTION.
    Uses substring match on normalised ATA code.
    """
    inspections: list[InspectionItem] = []

    if "Item Type" in df.columns:
        insp_df = df[df["Item Type"].astype(str).str.upper() == "INSPECTION"]
    else:
        insp_df = df

    rule_pairs = [(_norm_ata(ata), label) for label, ata in INSPECTION_RULES]

    for _, row in insp_df.iterrows():
        raw_ata  = _clean(row.get("ATA and Code", ""))
        norm_ata = _norm_ata(raw_ata)

        matched_labels: list[str] = []
        for rule_ata, label in rule_pairs:
            if rule_ata and rule_ata in norm_ata:
                if label not in matched_labels:
                    matched_labels.append(label)

        if not matched_labels:
            continue

        tail           = _clean(row.get("Registration Number", ""))
        due_date       = _to_date(row.get("Next Due Date"))
        remaining_days = _to_float(row.get("Remaining Days"))
        if due_date:
            remaining_days = float((date.fromisoformat(due_date) - date.today()).days)

        for label in matched_labels:
            inspections.append(
                InspectionItem(
                    tail=tail,
                    inspection=label,
                    ata=raw_ata,
                    description=_clean(row.get("Description", "")),
                    due_date=due_date,
                    remaining_days=remaining_days,
                    remaining_hours=_to_float(row.get("Remaining Hours")),
                )
            )
    return inspections


def _merge_inspections(
    weekly: list[InspectionItem],
    daily: list[InspectionItem],
) -> list[InspectionItem]:
    """
    Merge weekly (long-range baseline) and daily (current update).
    Daily wins for any (tail, inspection) key it contains.
    Weekly fills in everything else.
    """
    merged: dict[tuple[str, str], InspectionItem] = {}
    for item in weekly:
        merged[(item.tail, item.inspection)] = item
    for item in daily:
        merged[(item.tail, item.inspection)] = item  # daily overwrites
    return list(merged.values())


def _collect_components(df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Components due within COMPONENT_WINDOW_HOURS — daily CSV only."""
    required_columns = {"Requirement Type", "Remaining Hours"}
    if not required_columns.issubset(df.columns):
        return {}

    subset = df[
        df["Requirement Type"].astype(str).str.upper().isin(["RETIRE", "OVERHAUL"])
    ].copy()
    subset["Remaining Hours"] = pd.to_numeric(subset["Remaining Hours"], errors="coerce")
    subset = subset[
        subset["Remaining Hours"].notna()
        & (subset["Remaining Hours"] <= COMPONENT_WINDOW_HOURS)
    ]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for _, row in subset.sort_values("Remaining Hours").iterrows():
        tail = _clean(row.get("Registration Number", "")) or "UNKNOWN"
        grouped.setdefault(tail, []).append(
            {
                "description":      _clean(row.get("Description", "")),
                "ata":              _clean(row.get("ATA and Code", "")),
                "requirement_type": _clean(row.get("Requirement Type", "")).upper(),
                "remaining_hours":  float(row["Remaining Hours"]),
            }
        )
    return grouped


def build() -> None:
    missing = [p for p in (DAILY_CSV, WEEKLY_CSV) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing CSV files: {missing}")

    print(f"Reading daily CSV:  {DAILY_CSV}")
    daily_df = _read_csv(DAILY_CSV)
    print(f"Reading weekly CSV: {WEEKLY_CSV}")
    weekly_df = _read_csv(WEEKLY_CSV)

    history = _load_history()
    # Seed with weekly baseline first so long-range history exists immediately,
    # then overlay the latest daily feed for freshest values.
    history = _update_history(history, weekly_df)
    history = _update_history(history, daily_df)
    _save_history(history)

    utilization = _utilization(history)

    weekly_inspections = _collect_inspections(weekly_df)
    daily_inspections  = _collect_inspections(daily_df)
    print(f"Weekly inspections parsed: {len(weekly_inspections)}")
    print(f"Daily inspections parsed:  {len(daily_inspections)}")
    inspections = _merge_inspections(weekly_inspections, daily_inspections)
    print(f"Merged inspections total:  {len(inspections)}")

    components = _collect_components(daily_df)

    aircraft: dict[str, dict[str, Any]] = {}
    for item in inspections:
        aircraft.setdefault(item.tail, {"tail": item.tail, "inspections": []})
        aircraft[item.tail]["inspections"].append(item.__dict__)

    # Ensure aircraft that only appear in utilization history still render,
    # even if they currently have no tracked inspections.
    for tail in utilization:
        aircraft.setdefault(tail, {"tail": tail, "inspections": []})

    for tail, ac in aircraft.items():
        ac["utilization"] = utilization.get(
            tail,
            {"avg_daily_hours": None, "avg_weekly_hours": None, "daily_points": [], "hour_deltas": []},
        )

    output = {
        "meta": {
            "generated_utc":  datetime.now(timezone.utc).isoformat(),
            "daily_csv":      str(DAILY_CSV),
            "weekly_csv":     str(WEEKLY_CSV),
            "aircraft_count": len(aircraft),
        },
        "aircraft":               sorted(aircraft.values(), key=lambda x: x["tail"]),
        "components_by_aircraft": components,
        "inspection_names":       sorted({item.inspection for item in inspections}),
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"Built {OUTPUT_JSON} — {len(output['aircraft'])} aircraft, "
          f"{len(output['inspection_names'])} inspection types")
    print(f"Inspection types found: {output['inspection_names']}")


if __name__ == "__main__":
    build()
