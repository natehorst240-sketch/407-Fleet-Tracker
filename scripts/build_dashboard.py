"""Build dashboard JSON for the Bell 407 fleet page.

Inputs:
- data/407_daily_due_list.csv
- data/407_Due-List_weekly.csv

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

DAILY_CSV = Path("data/407_daily_due_list.csv")
WEEKLY_CSV = Path("data/407_Due-List_weekly.csv")
HISTORY_JSON = Path("data/flight_hours_history.json")
OUTPUT_JSON = Path("public/data/dashboard.json")

COMPONENT_WINDOW_HOURS = 200

INSPECTION_RULES = [
    ("12 Month", "05 12MO- INSPECTION"),
    ("24 Month", "05 24MO. INSPECTION"),
    ("24 Month", "05 24.MO. INSPECTION"),
    ("300HR/12M Airframe", "05 300HR- PERIODIC INSPECTION"),
    ("300HR/12M Engine", "72 72/300"),
    ("600HR/12M Engine", "72 INSP 600HR/12MO"),
    ("MR Mast Interim", "63 11-20 INTERIM"),
    ("Freewheel Interim", "63 13-11 INTERIM"),
    ("Transmission Interim", "63 21-10 INTERIM"),
    ("TRGB Interim", "65 10-11 INTERIM"),
    ("TRGB Interim", "65-10-11 INTERIM"),
    ("Spring Link Interim", "67 20-12 INTERIM"),
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
            return pd.read_csv(path, encoding=enc)
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


def _update_history(history: dict[str, dict[str, float]], frames: list[pd.DataFrame]) -> dict[str, dict[str, float]]:
    for frame in frames:
        cols = ["Registration Number", "Airframe Report Date", "Airframe Hours"]
        for _, row in frame[cols].drop_duplicates().iterrows():
            tail = _clean(row["Registration Number"])
            dt = _to_date(row["Airframe Report Date"])
            hours = _to_float(row["Airframe Hours"])
            if not tail or not dt or hours is None:
                continue
            history.setdefault(tail, {})
            history[tail].setdefault(dt, hours)
    return history


def _utilization(history: dict[str, dict[str, float]]) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for tail, points in history.items():
        ordered = sorted(points.items(), key=lambda x: x[0])
        if len(ordered) < 2:
            stats[tail] = {"avg_daily_hours": None, "avg_weekly_hours": None, "daily_points": []}
            continue

        days = []
        for dt, hrs in ordered[-30:]:
            days.append({"date": dt, "hours": hrs})

        first_date = date.fromisoformat(ordered[0][0])
        last_date = date.fromisoformat(ordered[-1][0])
        span_days = max((last_date - first_date).days, 1)
        flown = ordered[-1][1] - ordered[0][1]
        avg_daily = flown / span_days
        stats[tail] = {
            "avg_daily_hours": round(avg_daily, 3),
            "avg_weekly_hours": round(avg_daily * 7, 3),
            "daily_points": days,
        }
    return stats


def _collect_inspections(df: pd.DataFrame) -> list[InspectionItem]:
    inspections: list[InspectionItem] = []
    insp_df = df[df["Item Type"].astype(str).str.upper() == "INSPECTION"]

    rule_map = {(_norm_ata(ata), label) for label, ata in INSPECTION_RULES}

    for _, row in insp_df.iterrows():
        ata = _clean(row.get("ATA and Code"))
        key_matches = [(label, rule_ata) for rule_ata, label in rule_map if _norm_ata(ata) == rule_ata]
        if not key_matches:
            continue

        tail = _clean(row.get("Registration Number"))
        due_date = _to_date(row.get("Next Due Date"))
        remaining_days = _to_float(row.get("Remaining Days"))
        if due_date:
            remaining_days = float((date.fromisoformat(due_date) - date.today()).days)

        for label, _ in key_matches:
            inspections.append(
                InspectionItem(
                    tail=tail,
                    inspection=label,
                    ata=ata,
                    description=_clean(row.get("Description")),
                    due_date=due_date,
                    remaining_days=remaining_days,
                    remaining_hours=_to_float(row.get("Remaining Hours")),
                )
            )
    return inspections


def _collect_components(df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    subset = df[df["Requirement Type"].astype(str).str.upper().isin(["RETIRE", "OVERHAUL"])].copy()
    subset["Remaining Hours"] = pd.to_numeric(subset["Remaining Hours"], errors="coerce")
    subset = subset[subset["Remaining Hours"].notna() & (subset["Remaining Hours"] <= COMPONENT_WINDOW_HOURS)]

    grouped: dict[str, list[dict[str, Any]]] = {}
    for _, row in subset.sort_values("Remaining Hours").iterrows():
        tail = _clean(row.get("Registration Number")) or "UNKNOWN"
        grouped.setdefault(tail, []).append(
            {
                "description": _clean(row.get("Description")),
                "ata": _clean(row.get("ATA and Code")),
                "requirement_type": _clean(row.get("Requirement Type")).upper(),
                "remaining_hours": float(row["Remaining Hours"]),
            }
        )
    return grouped


def build() -> None:
    if not DAILY_CSV.exists() or not WEEKLY_CSV.exists():
        raise FileNotFoundError("Expected both daily and weekly CSV files in data/.")

    daily_df = _read_csv(DAILY_CSV)
    weekly_df = _read_csv(WEEKLY_CSV)

    history = _update_history(_load_history(), [weekly_df, daily_df])
    _save_history(history)

    utilization = _utilization(history)
    inspections = _collect_inspections(daily_df)
    components = _collect_components(daily_df)

    aircraft: dict[str, dict[str, Any]] = {}
    for item in inspections:
        aircraft.setdefault(item.tail, {"tail": item.tail, "inspections": []})
        aircraft[item.tail]["inspections"].append(item.__dict__)

    for tail, ac in aircraft.items():
        ac["utilization"] = utilization.get(tail, {"avg_daily_hours": None, "avg_weekly_hours": None, "daily_points": []})

    output = {
        "meta": {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "daily_csv": str(DAILY_CSV),
            "weekly_csv": str(WEEKLY_CSV),
            "aircraft_count": len(aircraft),
        },
        "aircraft": sorted(aircraft.values(), key=lambda x: x["tail"]),
        "components_by_aircraft": components,
        "inspection_names": sorted({item.inspection for item in inspections}),
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"Built {OUTPUT_JSON} for {len(output['aircraft'])} aircraft")


if __name__ == "__main__":
    build()
