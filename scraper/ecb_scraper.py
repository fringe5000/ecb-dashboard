#!/usr/bin/env python3
"""
ECB Data Portal Scraper
Downloads bulk CSV files from the ECB SDMX API and filters to the
series we want. This is more reliable than querying individual series keys.
"""

import csv
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://data-api.ecb.europa.eu/service/data"
DATA_DIR = Path(__file__).parent.parent / "data"
TIMEOUT  = 120
START_YEAR = "2000"

# ---------------------------------------------------------------------------
# What we want to extract from each dataset bulk download
# ---------------------------------------------------------------------------

# BSI — we pull the full quarterly NFC NACE breakdown
# Series key pattern: BSI.Q.U2.N.A.A20.A.1.U2.{sector}.Z01.E
# The sector dimension (position 9) contains the NACE codes
BSI_NACE_SECTORS = {
    "2240A":   "Agriculture, forestry & fishing",
    "2240B":   "Mining & quarrying",
    "2240C":   "Manufacturing",
    "2240DE":  "Electricity, gas, water & waste",
    "2240F":   "Construction",
    "2240G":   "Wholesale & retail trade",
    "2240HJ":  "Transport, storage & communication",
    "2240LMN": "Real estate & professional services",
    "2240Z":   "Other services",
    "2240":    "Total NFC loans",
}

# Monthly aggregate BSI series keys to pull individually
BSI_INDIVIDUAL = [
    ("M.U2.N.A.A20.A.1.U2.2240.Z01.E",  "NFC loans — outstanding",        "NFC_total",  "EUR", "Money, credit & banking"),
    ("M.U2.N.A.A20.A.I.U2.2240.Z01.A",  "NFC loans — annual growth rate",  "NFC_growth", "PCT", "Money, credit & banking"),
    ("M.U2.N.A.A20.A.1.U2.2250.Z01.E",  "Household loans — outstanding",   "HH_total",   "EUR", "Money, credit & banking"),
    ("M.U2.N.A.A20.A.I.U2.2250.Z01.A",  "Household loans — growth rate",   "HH_growth",  "PCT", "Money, credit & banking"),
    ("M.U2.Y.V.M10.X.1.U2.2300.Z0Z.E",  "M1 — outstanding",                "M1",         "EUR", "Money, credit & banking"),
    ("M.U2.Y.V.M30.X.1.U2.2300.Z0Z.E",  "M3 — outstanding",                "M3",         "EUR", "Money, credit & banking"),
    ("M.U2.Y.V.M30.X.1.U2.2300.Z0Z.I",  "M3 — annual growth rate",         "M3_growth",  "PCT", "Money, credit & banking"),
]

MIR_INDIVIDUAL = [
    ("M.U2.B.A2I.AM.R.A.2240.EUR.N", "NFC lending rate — new business",         "NFC_rate_new",   "PCT", "Interest rates"),
    ("M.U2.B.A2O.AM.R.A.2240.EUR.N", "NFC lending rate — outstanding",          "NFC_rate_out",   "PCT", "Interest rates"),
    ("M.U2.B.A2C.AM.R.A.2250.EUR.N", "Household mortgage rate — new business",  "HH_mortgage_new","PCT", "Interest rates"),
    ("M.U2.B.A2D.AM.R.A.2250.EUR.N", "Household consumer rate — new business",  "HH_consumer_new","PCT", "Interest rates"),
    ("M.U2.B.L21.A.R.A.2250.EUR.N",  "Overnight deposit rate — households",     "HH_deposit_ON",  "PCT", "Interest rates"),
]

FM_INDIVIDUAL = [
    ("M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA", "Euribor 1-month",           "EURIBOR1M",  "PCT", "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", "Euribor 3-month",           "EURIBOR3M",  "PCT", "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA", "Euribor 6-month",           "EURIBOR6M",  "PCT", "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA", "Euribor 12-month",          "EURIBOR12M", "PCT", "Interest rates"),
    ("M.U2.EUR.4F.KR.MRR_FR.LEV",       "ECB main refinancing rate", "ECB_MRR",    "PCT", "ECB policy rates"),
    ("M.U2.EUR.4F.KR.DFR.LEV",          "ECB deposit facility rate", "ECB_DFR",    "PCT", "ECB policy rates"),
    ("M.U2.EUR.4F.KR.MLFR.LEV",         "ECB marginal lending rate", "ECB_MLF",    "PCT", "ECB policy rates"),
]

ICP_INDIVIDUAL = [
    ("M.U2.N.000000.4.ANR", "HICP — all items", "HICP_all",      "PCT", "Inflation"),
    ("M.U2.N.01.4.ANR",     "HICP — food",      "HICP_food",     "PCT", "Inflation"),
    ("M.U2.N.045.4.ANR",    "HICP — energy",    "HICP_energy",   "PCT", "Inflation"),
    ("M.U2.N.S.4.ANR",      "HICP — services",  "HICP_services", "PCT", "Inflation"),
    ("M.U2.N.G.4.ANR",      "HICP — goods",     "HICP_goods",    "PCT", "Inflation"),
]

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch_url(url: str) -> Optional[str]:
    """Fetch a URL and return text content, or None on failure."""
    log.info(f"  GET {url}")
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"Accept": "text/csv"})
        if r.status_code in (400, 404):
            log.warning(f"  HTTP {r.status_code} — not found")
            return None
        r.raise_for_status()
        log.info(f"  → {len(r.content):,} bytes")
        return r.text
    except requests.RequestException as e:
        log.error(f"  Request failed: {e}")
        return None


def parse_csv_text(text: str) -> list[dict]:
    """Parse ECB CSV into list of {period, value}."""
    rows = []
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return rows
    for line in lines[1:]:
        cols = line.split(",")
        if len(cols) < 2:
            continue
        period = cols[-2].strip()
        raw    = cols[-1].strip()
        if not raw or raw in ("NaN", ""):
            continue
        try:
            rows.append({"period": period, "value": float(raw)})
        except ValueError:
            continue
    return [r for r in rows if r["period"] >= START_YEAR]


def parse_bulk_csv(text: str) -> dict[str, list[dict]]:
    """
    Parse a bulk ECB CSV download into a dict of {series_key: [{period, value}]}.
    ECB bulk CSV has a KEY_DIM_N column for the series key and TIME_PERIOD / OBS_VALUE.
    """
    result: dict[str, list] = {}
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        # Find the series key column — ECB uses different names
        key = row.get("KEY") or row.get("SERIES_KEY") or row.get("key") or ""
        period = row.get("TIME_PERIOD") or row.get("time_period") or ""
        value  = row.get("OBS_VALUE")  or row.get("obs_value")  or ""
        if not key or not period or not value:
            continue
        if period < START_YEAR:
            continue
        try:
            v = float(value)
        except ValueError:
            continue
        result.setdefault(key, []).append({"period": period, "value": v})
    return result


# ---------------------------------------------------------------------------
# Fetch individual series
# ---------------------------------------------------------------------------

def fetch_individual(dataset: str, key: str) -> list[dict]:
    url = f"{BASE_URL}/{dataset}/{key}?startPeriod={START_YEAR}&detail=dataonly&format=csvdata"
    text = fetch_url(url)
    if not text:
        return []
    rows = parse_csv_text(text)
    log.info(f"  → {len(rows)} observations")
    return rows


# ---------------------------------------------------------------------------
# Fetch BSI NACE sector data via bulk download with key filter
# ---------------------------------------------------------------------------

def fetch_bsi_nace() -> list[dict]:
    """
    Fetch quarterly BSI loans to NFCs by NACE sector.
    Uses a wildcard key to get all NACE sector series at once.
    Pattern: Q.U2.N.A.A20.A.1.U2.*.Z01.E  (wildcard on counterpart sector)
    """
    log.info("Fetching BSI NACE sector loans (bulk wildcard query)...")

    # Wildcard fetch — omit the counterpart sector dimension to get all sectors
    url = f"{BASE_URL}/BSI/Q.U2.N.A.A20.A.1.U2..Z01.E?startPeriod={START_YEAR}&detail=dataonly&format=csvdata"
    text = fetch_url(url)

    if not text:
        # Fallback: try each sector individually
        log.warning("Bulk wildcard failed, trying sectors individually...")
        return fetch_bsi_nace_individual()

    # Parse and filter to our NACE codes
    series = []
    bulk = parse_bulk_csv(text)

    if not bulk:
        # The bulk parse may fail if format differs — try line-by-line
        log.warning("Bulk parse returned empty, trying individual series...")
        return fetch_bsi_nace_individual()

    for full_key, obs in bulk.items():
        # Extract counterpart sector from key position 9 (0-indexed)
        parts = full_key.replace("BSI.", "").split(".")
        if len(parts) < 9:
            continue
        sector_code = parts[8]
        if sector_code not in BSI_NACE_SECTORS:
            continue
        series.append({
            "dataset":      "BSI",
            "key":          full_key,
            "country_code": "U2",
            "country":      "Euro area",
            "series":       BSI_NACE_SECTORS[sector_code],
            "sector_code":  sector_code,
            "unit":         "EUR",
            "unit_label":   "Outstanding amounts (€ millions)",
            "frequency":    "Q",
            "theme":        "Bank lending by sector",
            "observations": sorted(obs, key=lambda x: x["period"]),
        })
        log.info(f"  ✓ {BSI_NACE_SECTORS[sector_code]} ({sector_code}): {len(obs)} obs")

    log.info(f"BSI NACE: {len(series)} sectors retrieved")
    return series


def fetch_bsi_nace_individual() -> list[dict]:
    """Fallback: fetch each NACE sector series individually."""
    series = []
    for sector_code, sector_name in BSI_NACE_SECTORS.items():
        log.info(f"  Trying sector {sector_code} — {sector_name}")
        # Try both with and without Z01 currency dimension
        for key_variant in [
            f"Q.U2.N.A.A20.A.1.U2.{sector_code}.Z01.E",
            f"Q.U2.N.A.A20.A.1.U2.{sector_code}.EUR.E",
            f"Q.U2.N.A.A20.A.1.U2.{sector_code}.Z0Z.E",
        ]:
            obs = fetch_individual("BSI", key_variant)
            if obs:
                series.append({
                    "dataset":      "BSI",
                    "key":          f"BSI.{key_variant}",
                    "country_code": "U2",
                    "country":      "Euro area",
                    "series":       sector_name,
                    "sector_code":  sector_code,
                    "unit":         "EUR",
                    "unit_label":   "Outstanding amounts (€ millions)",
                    "frequency":    "Q",
                    "theme":        "Bank lending by sector",
                    "observations": obs,
                })
                log.info(f"  ✓ {sector_name}: {len(obs)} obs (key: {key_variant})")
                break
        else:
            log.warning(f"  ✗ No data for {sector_name} ({sector_code})")
        time.sleep(0.3)
    return series


# ---------------------------------------------------------------------------
# Build all series records
# ---------------------------------------------------------------------------

def build_individual_series(dataset, definitions, country_code="U2", country="Euro area"):
    results = []
    for key, label, sector_code, unit, theme in definitions:
        log.info(f"[{dataset}] {label}")
        obs = fetch_individual(dataset, key)
        if obs:
            results.append({
                "dataset":      dataset,
                "key":          f"{dataset}.{key}",
                "country_code": country_code,
                "country":      country,
                "series":       label,
                "sector_code":  sector_code,
                "unit":         unit,
                "unit_label":   "Rate (%)" if unit == "PCT" else "Outstanding (€ millions)",
                "frequency":    key[0],
                "theme":        theme,
                "observations": obs,
            })
            log.info(f"  ✓ {len(obs)} obs")
        else:
            log.warning(f"  ✗ No data")
        time.sleep(0.2)
    return results


# ---------------------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------------------

def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def save_outputs(all_series: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    now_iso = datetime.now(timezone.utc).isoformat()

    # Group by dataset
    by_dataset: dict[str, list] = {}
    by_theme:   dict[str, list] = {}
    flat_rows = []

    for s in all_series:
        by_dataset.setdefault(s["dataset"], []).append(s)
        by_theme.setdefault(s["theme"], []).append(s)
        for obs in s.get("observations", []):
            flat_rows.append({
                "dataset":      s["dataset"],
                "country_code": s["country_code"],
                "country":      s["country"],
                "series":       s["series"],
                "sector_code":  s["sector_code"],
                "unit":         s["unit"],
                "theme":        s["theme"],
                "period":       obs["period"],
                "value":        obs["value"],
            })

    # Per-dataset JSON
    for ds, records in by_dataset.items():
        save_json(DATA_DIR / f"{ds}.json", {
            "dataset": ds, "generated_at": now_iso,
            "series_count": len(records), "series": records,
        })
        log.info(f"Saved data/{ds}.json ({len(records)} series)")

    # Per-theme JSON
    theme_dir = DATA_DIR / "themes"
    for theme, records in by_theme.items():
        slug = theme.lower().replace(" ", "_").replace(",", "").replace("&", "and")
        save_json(theme_dir / f"{slug}.json", {
            "theme": theme, "generated_at": now_iso,
            "series_count": len(records), "series": records,
        })

    # Master CSV
    if flat_rows:
        import csv as csv_mod
        csv_path = DATA_DIR / "ecb_all_data.csv"
        fieldnames = ["dataset","country_code","country","series","sector_code","unit","theme","period","value"]
        with open(csv_path, "w", newline="") as f:
            writer = csv_mod.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_rows)
        log.info(f"Saved data/ecb_all_data.csv ({len(flat_rows):,} rows)")

    # Manifest
    save_json(DATA_DIR / "manifest.json", {
        "generated_at":  now_iso,
        "total_series":  len(all_series),
        "total_obs":     len(flat_rows),
        "datasets":      list(by_dataset.keys()),
        "themes":        list(by_theme.keys()),
    })
    log.info(f"Manifest: {len(all_series)} series, {len(flat_rows):,} observations")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    all_series = []

    # 1. BSI NACE sector loans (bulk wildcard, with individual fallback)
    all_series += fetch_bsi_nace()

    # 2. BSI aggregates
    log.info("--- BSI individual series ---")
    all_series += build_individual_series("BSI", BSI_INDIVIDUAL)

    # 3. MIR interest rates
    log.info("--- MIR series ---")
    all_series += build_individual_series("MIR", MIR_INDIVIDUAL)

    # 4. FM — Euribor + policy rates
    log.info("--- FM series ---")
    all_series += build_individual_series("FM", FM_INDIVIDUAL)

    # 5. ICP — Inflation
    log.info("--- ICP series ---")
    all_series += build_individual_series("ICP", ICP_INDIVIDUAL)

    save_outputs(all_series)
