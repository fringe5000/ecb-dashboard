#!/usr/bin/env python3
"""
ECB Data Portal Scraper
Fetches all configured series from the ECB SDMX 2.1 REST API,
stores results as structured JSON and CSV in /data.

Usage:
    python scraper/ecb_scraper.py              # fetch everything
    python scraper/ecb_scraper.py --dataset BSI  # single dataset
    python scraper/ecb_scraper.py --dry-run      # print series count, no fetch
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from datasets import ALL_SERIES, THEMES, DATASETS, COUNTRIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL    = "https://data-api.ecb.europa.eu/service/data"
START_YEAR  = "2000"
DATA_DIR    = Path(__file__).parent.parent / "data"
TIMEOUT     = 30
RETRY_LIMIT = 3
RETRY_DELAY = 5  # seconds between retries


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch_series(key: str, dataset: str, start: str = START_YEAR) -> Optional[list[dict]]:
    """Fetch one series from the ECB SDMX API. Returns list of {period, value} or None."""
    url = f"{BASE_URL}/{dataset}/{key}?startPeriod={start}-01&detail=dataonly&format=csvdata"
    log.info(f"  GET {url}")
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers={"Accept": "text/csv"})
            if r.status_code in (400, 404):
                log.warning(f"  HTTP {r.status_code} — key not found: {dataset}/{key}")
                return None
            r.raise_for_status()
            rows = parse_csv(r.text)
            log.info(f"  → {len(rows)} observations")
            return rows
        except requests.RequestException as e:
            if attempt < RETRY_LIMIT:
                log.warning(f"  Attempt {attempt} failed: {e} — retrying in {RETRY_DELAY}s")
                time.sleep(RETRY_DELAY)
            else:
                log.error(f"  All retries failed: {e}")
                return None

def parse_csv(text: str) -> list[dict]:
    """Parse ECB CSV response into [{period, value}]."""
    rows = []
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return rows
    reader = csv.reader(lines)
    headers = next(reader)
    # ECB CSV: last two cols are TIME_PERIOD and OBS_VALUE
    for cols in reader:
        if len(cols) < 2:
            continue
        period = cols[-2].strip()
        raw    = cols[-1].strip()
        if not raw or raw == "NaN":
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        rows.append({"period": period, "value": value})
    return rows


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def save_csv(path: Path, rows: list[dict], extra_cols: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(extra_cols.keys()) + ["period", "value"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({**extra_cols, **row})


# ---------------------------------------------------------------------------
# Core scrape logic
# ---------------------------------------------------------------------------

def scrape_all(filter_dataset: Optional[str] = None) -> dict:
    series_to_fetch = ALL_SERIES
    if filter_dataset:
        series_to_fetch = [s for s in ALL_SERIES if s["dataset"] == filter_dataset]

    log.info(f"Fetching {len(series_to_fetch)} series...")

    all_observations = []
    dataset_buckets: dict[str, list] = {ds: [] for ds in DATASETS}
    theme_buckets:   dict[str, list] = {th: [] for th in THEMES}

    ok = fail = 0

    for i, meta in enumerate(series_to_fetch, 1):
        log.info(f"[{i}/{len(series_to_fetch)}] {meta['dataset']} | {meta['country']} | {meta['series']} | key={meta['key']}")
        rows = fetch_series(meta["key"], meta["dataset"])

        if not rows:
            fail += 1
            log.warning(f"  FAILED — {meta['dataset']}/{meta['key']}")
            continue

        ok += 1
        log.info(f"  OK — {len(rows)} obs, first={rows[0]['period']}, last={rows[-1]['period']}")

        record = {
            "dataset":      meta["dataset"],
            "country_code": meta["country_code"],
            "country":      meta["country"],
            "series":       meta["series"],
            "sector_code":  meta["sector_code"],
            "unit":         meta["unit"],
            "unit_label":   meta["unit_label"],
            "frequency":    meta["frequency"],
            "theme":        meta["theme"],
            "observations": rows,
        }

        dataset_buckets[meta["dataset"]].append(record)
        theme_buckets[meta["theme"]].append(record)

        for obs in rows:
            all_observations.append({
                "dataset":      meta["dataset"],
                "country_code": meta["country_code"],
                "country":      meta["country"],
                "series":       meta["series"],
                "sector_code":  meta["sector_code"],
                "unit":         meta["unit"],
                "frequency":    meta["frequency"],
                "theme":        meta["theme"],
                "period":       obs["period"],
                "value":        obs["value"],
            })

        time.sleep(0.2)

    log.info(f"Done — {ok} series fetched, {fail} empty/failed")
    return {
        "dataset_buckets":  dataset_buckets,
        "theme_buckets":    theme_buckets,
        "all_observations": all_observations,
        "ok":               ok,
        "fail":             fail,
    }

# ---------------------------------------------------------------------------
# Save all outputs
# ---------------------------------------------------------------------------

def save_outputs(result: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. Per-dataset JSON (e.g. data/BSI.json)
    for ds, records in result["dataset_buckets"].items():
        if records:
            save_json(DATA_DIR / f"{ds}.json", {
                "dataset":       ds,
                "generated_at":  now_iso,
                "series_count":  len(records),
                "series":        records,
            })
            log.info(f"Saved data/{ds}.json ({len(records)} series)")

    # 2. Per-theme JSON (e.g. data/themes/interest_rates.json)
    theme_dir = DATA_DIR / "themes"
    for theme, records in result["theme_buckets"].items():
        if records:
            slug = theme.lower().replace(" ", "_").replace(",", "").replace("&", "and")
            save_json(theme_dir / f"{slug}.json", {
                "theme":         theme,
                "generated_at":  now_iso,
                "series_count":  len(records),
                "series":        records,
            })

    # 3. Master flat CSV
    csv_path = DATA_DIR / "ecb_all_data.csv"
    obs = result["all_observations"]
    if obs:
        fieldnames = ["dataset", "country_code", "country", "series", "sector_code",
                      "unit", "frequency", "theme", "period", "value"]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(obs)
        log.info(f"Saved data/ecb_all_data.csv ({len(obs):,} observations)")

    # 4. Manifest / metadata file (used by the dashboard)
    manifest = {
        "generated_at":    now_iso,
        "total_series":    result["ok"],
        "total_obs":       len(result["all_observations"]),
        "failed_series":   result["fail"],
        "datasets":        [ds for ds, r in result["dataset_buckets"].items() if r],
        "themes":          [th for th, r in result["theme_buckets"].items() if r],
        "countries":       COUNTRIES,
    }
    save_json(DATA_DIR / "manifest.json", manifest)
    log.info("Saved data/manifest.json")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ECB Data Portal scraper")
    parser.add_argument("--dataset",  help="Fetch only one dataset (e.g. BSI, MIR)")
    parser.add_argument("--dry-run",  action="store_true", help="Print series count without fetching")
    args = parser.parse_args()

    if args.dry_run:
        subset = [s for s in ALL_SERIES if not args.dataset or s["dataset"] == args.dataset]
        print(f"Would fetch {len(subset)} series across {len(set(s['dataset'] for s in subset))} datasets")
        sys.exit(0)

    result = scrape_all(filter_dataset=args.dataset)
    save_outputs(result)
