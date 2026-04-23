#!/usr/bin/env python3
"""
ECB Data Portal Scraper
Fetches all configured series from the ECB SDMX API.
NACE sector lending uses a targeted OR-operator query.
All other series are fetched individually.
"""

import csv
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from datasets import (
    ALL_SERIES, THEMES, DATASETS, COUNTRIES,
    NACE_SECTOR_LABELS, START_PERIOD, BASE_URL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR    = Path(__file__).parent.parent / "data"
TIMEOUT     = 120
RETRY_LIMIT = 3
RETRY_DELAY = 5

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def fetch_url(url: str) -> Optional[str]:
    log.info(f"  GET {url}")
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers={"Accept": "text/csv"})
            if r.status_code in (400, 404):
                log.warning(f"  HTTP {r.status_code} — not found")
                return None
            r.raise_for_status()
            log.info(f"  → {len(r.content):,} bytes")
            return r.text
        except requests.RequestException as e:
            if attempt < RETRY_LIMIT:
                log.warning(f"  Attempt {attempt} failed: {e} — retrying in {RETRY_DELAY}s")
                time.sleep(RETRY_DELAY)
            else:
                log.error(f"  All retries failed: {e}")
                return None

# ---------------------------------------------------------------------------
# CSV parsers
# ---------------------------------------------------------------------------

def parse_csv_individual(text: str) -> list[dict]:
    """Parse a single-series ECB CSV response into [{period, value}]."""
    rows = []
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return rows
    for line in lines[1:]:
        cols = line.split(",")
        if len(cols) < 2:
            continue
        period = cols[-2].strip().strip('"')
        raw    = cols[-1].strip().strip('"')
        if not raw or raw in ("NaN", "NA", ""):
            continue
        try:
            rows.append({"period": period, "value": float(raw)})
        except ValueError:
            continue
    return [r for r in rows if r["period"] >= START_PERIOD]


def parse_csv_bulk(text: str) -> dict[str, list[dict]]:
    """
    Parse an ECB bulk/multi-series CSV into {full_series_key: [{period, value}]}.
    ECB returns wide format: rows = series, columns = time periods.
    First column is the full series key (e.g. BSI.Q.U2.N.A.A20.A.1.U2.2240A.Z01.E).
    """
    result: dict[str, list] = {}
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return result

    reader = csv.reader(lines)
    headers = next(reader)

    # Identify period columns by pattern YYYY-Qn or YYYY-MM
    period_pattern = re.compile(r'^\d{4}[-Q]\d')
    period_cols = []
    for i, h in enumerate(headers):
        h_clean = h.strip().strip('"')
        if period_pattern.match(h_clean):
            period_cols.append((i, h_clean))

    if not period_cols:
        log.warning("  No period columns found in bulk CSV — falling back to long format")
        return parse_csv_bulk_long(text)

    # Column 0 is the series key in ECB wide-format CSV
    for row in reader:
        if not row:
            continue
        full_key = row[0].strip().strip('"')
        if not full_key:
            continue
        obs = []
        for col_i, period in period_cols:
            if col_i >= len(row):
                continue
            val = row[col_i].strip().strip('"')
            if not val or val in ("NaN", "NA", ""):
                continue
            try:
                if period >= START_PERIOD:
                    obs.append({"period": period, "value": float(val)})
            except ValueError:
                continue
        if obs:
            result[full_key] = obs

    return result


def parse_csv_bulk_long(text: str) -> dict[str, list[dict]]:
    """Fallback for ECB long-format CSV."""
    result: dict[str, list] = {}
    try:
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            key    = (row.get("KEY") or row.get("SERIES_KEY") or "").strip()
            period = (row.get("TIME_PERIOD") or "").strip()
            value  = (row.get("OBS_VALUE") or "").strip()
            if not key or not period or not value or period < START_PERIOD:
                continue
            try:
                result.setdefault(key, []).append({"period": period, "value": float(value)})
            except ValueError:
                continue
    except Exception as e:
        log.error(f"  Long-format parse failed: {e}")
    return result

# ---------------------------------------------------------------------------
# NACE sector loans — bulk fetch using OR operator
# ---------------------------------------------------------------------------

def fetch_bsi_nace() -> list[dict]:
    """
    Fetch quarterly BSI loans to NFCs by NACE sector.
    Uses the SDMX OR operator (+) to request all NACE codes in one call.
    """
    log.info("Fetching BSI NACE sector loans...")
    nace_codes = "+".join(NACE_SECTOR_LABELS.keys())
    url = (
        f"{BASE_URL}/BSI/Q.U2.N.A.A20.A.1.U2.{nace_codes}.Z01.E"
        f"?startPeriod={START_PERIOD}&detail=dataonly&format=csvdata"
    )
    text = fetch_url(url)

    if not text:
        log.warning("  OR-operator fetch failed, trying sectors individually...")
        return fetch_bsi_nace_individual()

    bulk = parse_csv_bulk(text)

    if not bulk:
        log.warning("  Bulk parse returned empty, trying sectors individually...")
        return fetch_bsi_nace_individual()

    series = []
    for full_key, obs in bulk.items():
        # Strip dataset prefix: BSI.Q.U2.N.A.A20.A.1.U2.{SECTOR}.Z01.E
        clean = full_key[4:] if full_key.startswith("BSI.") else full_key
        parts = clean.split(".")
        if len(parts) < 9:
            log.warning(f"  Key too short: {full_key}")
            continue
        sector_code = parts[8]
        if sector_code not in NACE_SECTOR_LABELS:
            log.info(f"  Skipping unmapped sector: {sector_code}")
            continue
        series.append({
            "dataset":      "BSI",
            "key":          full_key,
            "country_code": "U2",
            "country":      "Euro area",
            "series":       NACE_SECTOR_LABELS[sector_code],
            "sector_code":  sector_code,
            "unit":         "EUR",
            "unit_label":   "Outstanding amounts (€ millions)",
            "frequency":    "Q",
            "theme":        "Bank lending by sector",
            "observations": sorted(obs, key=lambda x: x["period"]),
        })
        log.info(f"  ✓ {NACE_SECTOR_LABELS[sector_code]} ({sector_code}): {len(obs)} obs")

    log.info(f"BSI NACE: {len(series)} sectors retrieved")
    if not series:
        log.warning("  No sectors matched — trying individual fallback...")
        return fetch_bsi_nace_individual()
    return series


def fetch_bsi_nace_individual() -> list[dict]:
    """Fallback: fetch each NACE sector series individually."""
    series = []
    for sector_code, sector_name in NACE_SECTOR_LABELS.items():
        log.info(f"  Trying sector {sector_code} — {sector_name}")
        for key_variant in [
            f"Q.U2.N.A.A20.A.1.U2.{sector_code}.Z01.E",
            f"Q.U2.N.A.A20.A.1.U2.{sector_code}.EUR.E",
        ]:
            text = fetch_url(f"{BASE_URL}/BSI/{key_variant}?startPeriod={START_PERIOD}&detail=dataonly&format=csvdata")
            if text:
                obs = parse_csv_individual(text)
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
                    log.info(f"  ✓ {sector_name}: {len(obs)} obs")
                    break
        else:
            log.warning(f"  ✗ No data for {sector_name} ({sector_code})")
        time.sleep(0.3)
    return series

# ---------------------------------------------------------------------------
# Individual series fetch
# ---------------------------------------------------------------------------

def fetch_individual(dataset: str, key: str) -> list[dict]:
    url = f"{BASE_URL}/{dataset}/{key}?startPeriod={START_PERIOD}&detail=dataonly&format=csvdata"
    text = fetch_url(url)
    if not text:
        return []
    obs = parse_csv_individual(text)
    log.info(f"  → {len(obs)} observations")
    return obs


def build_series_list(definitions: list[dict]) -> list[dict]:
    """Fetch all individual series from a list of series definitions."""
    results = []
    for meta in definitions:
        log.info(f"[{meta['dataset']}] {meta['series']} | {meta['country']}")
        obs = fetch_individual(meta["dataset"], meta["key"])
        if obs:
            results.append({**meta, "observations": obs})
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

    by_dataset: dict[str, list] = {}
    by_theme:   dict[str, list] = {}
    flat_rows:  list[dict]      = []

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
            "dataset":      ds,
            "generated_at": now_iso,
            "series_count": len(records),
            "series":       records,
        })
        log.info(f"Saved data/{ds}.json ({len(records)} series)")

    # Per-theme JSON
    theme_dir = DATA_DIR / "themes"
    for theme, records in by_theme.items():
        slug = theme.lower().replace(" ", "_").replace(",", "").replace("&", "and")
        save_json(theme_dir / f"{slug}.json", {
            "theme":        theme,
            "generated_at": now_iso,
            "series_count": len(records),
            "series":       records,
        })

    # Master CSV
    if flat_rows:
        csv_path = DATA_DIR / "ecb_all_data.csv"
        fieldnames = [
            "dataset", "country_code", "country", "series",
            "sector_code", "unit", "theme", "period", "value",
        ]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_rows)
        log.info(f"Saved data/ecb_all_data.csv ({len(flat_rows):,} rows)")

    # Manifest
    save_json(DATA_DIR / "manifest.json", {
        "generated_at": now_iso,
        "total_series": len(all_series),
        "total_obs":    len(flat_rows),
        "datasets":     list(by_dataset.keys()),
        "themes":       list(by_theme.keys()),
    })
    log.info(f"Manifest: {len(all_series)} series, {len(flat_rows):,} observations")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    all_series = []

    # 1. NACE sector loans (BSI bulk fetch)
    log.info("=== BSI NACE sector loans ===")
    all_series += fetch_bsi_nace()

    # 2. All individually-defined series (BSI, FM, MIR, ICP, CBD2, GFS)
    log.info("=== Individual series ===")
    all_series += build_series_list(ALL_SERIES)

    save_outputs(all_series)
