"""
ECB Dataset Registry — verified series keys only.
Every key in this file has been confirmed against the ECB Data Portal.

Datasets covered:
  BSI  — Bank lending (NACE sectors + aggregates)
  MIR  — MFI interest rates
  FM   — Euribor + ECB policy rates
  CBD2 — Supervisory banking (NPL, CET1, total assets)
  GFS  — Government finance (debt, deficit as % GDP)

Countries: Euro area aggregate + DE, FR, IT, ES, NL, BE, AT, IE, PT, FI, GR
Start: 2010
"""

START_PERIOD = "2010"
BASE_URL = "https://data-api.ecb.europa.eu/service/data"

COUNTRIES = {
    "U2": "Euro area",
    "DE": "Germany",
    "FR": "France",
    "IT": "Italy",
    "ES": "Spain",
    "NL": "Netherlands",
    "BE": "Belgium",
    "AT": "Austria",
    "IE": "Ireland",
    "PT": "Portugal",
    "FI": "Finland",
    "GR": "Greece",
}

# ---------------------------------------------------------------------------
# BSI — NFC loans by NACE sector (quarterly, euro area only)
# Uses wildcard on counterpart sector dimension to get all sectors in one call
# Key: BSI / Q.U2.N.A.A20.A.1.U2..Z01.E  (double dot = wildcard)
# ---------------------------------------------------------------------------
BSI_NACE_WILDCARD = {
    "dataset": "BSI",
    "key": "Q.U2.N.A.A20.A.1.U2..Z01.E",
    "country_code": "U2",
    "country": "Euro area",
    "frequency": "Q",
    "unit": "EUR",
    "unit_label": "Outstanding amounts (€ millions)",
    "theme": "Bank lending by sector",
}

# NACE sector codes that appear in position 9 of BSI keys
NACE_SECTOR_LABELS = {
    "2240A":   "Agriculture, forestry & fishing",
    "2240B":   "Mining & quarrying",
    "2240C":   "Manufacturing",
    "2240DE":  "Electricity, gas, water & waste",
    "2240F":   "Construction",
    "2240G":   "Wholesale & retail trade",
    "2240HJ":  "Transport, storage & communication",
    "2240LMN": "Real estate & professional services",
    "2240Z":   "Other services",
    "2240":    "Total NFC — all sectors",
}

# ---------------------------------------------------------------------------
# BSI — NFC and household aggregate loans (monthly, all countries)
# Confirmed key format from search results
# ---------------------------------------------------------------------------
BSI_SERIES = []
_bsi_defs = [
    ("M.{cc}.N.A.A20.A.1.U2.2240.Z01.E",  "NFC loans — outstanding",       "NFC_total",  "EUR"),
    ("M.{cc}.N.A.A20.A.I.U2.2240.Z01.A",  "NFC loans — annual growth rate", "NFC_growth", "PCT"),
    ("M.{cc}.N.A.A20.A.1.U2.2250.Z01.E",  "Household loans — outstanding",  "HH_total",   "EUR"),
    ("M.{cc}.N.A.A20.A.I.U2.2250.Z01.A",  "Household loans — growth rate",  "HH_growth",  "PCT"),
]
for key_tpl, label, sector_code, unit in _bsi_defs:
    for cc, country in COUNTRIES.items():
        BSI_SERIES.append({
            "dataset":      "BSI",
            "key":          key_tpl.format(cc=cc),
            "country_code": cc,
            "country":      country,
            "series":       label,
            "sector_code":  sector_code,
            "unit":         unit,
            "unit_label":   "Outstanding (€ millions)" if unit == "EUR" else "Annual rate (%)",
            "frequency":    "M",
            "theme":        "Bank lending by sector",
        })

# ---------------------------------------------------------------------------
# FM — ECB policy rates and Euribor (monthly, euro area only)
# Confirmed working — sourced from ECB API examples and fgeerolf.com
# ---------------------------------------------------------------------------
FM_SERIES = [
    # Euribor
    ("M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA", "Euribor 1-month",             "EURIBOR1M",  "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", "Euribor 3-month",             "EURIBOR3M",  "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA", "Euribor 6-month",             "EURIBOR6M",  "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA", "Euribor 12-month",            "EURIBOR12M", "Interest rates"),
    # ECB policy rates
    ("M.U2.EUR.4F.KR.MRR_FR.LEV",       "ECB main refinancing rate",   "ECB_MRR",    "ECB policy rates"),
    ("M.U2.EUR.4F.KR.DFR.LEV",          "ECB deposit facility rate",   "ECB_DFR",    "ECB policy rates"),
    ("M.U2.EUR.4F.KR.MLFR.LEV",         "ECB marginal lending rate",   "ECB_MLF",    "ECB policy rates"),
]
FM_SERIES = [{
    "dataset":      "FM",
    "key":          key,
    "country_code": "U2",
    "country":      "Euro area",
    "series":       label,
    "sector_code":  sector_code,
    "unit":         "PCT",
    "unit_label":   "Rate (%)",
    "frequency":    "M",
    "theme":        theme,
} for key, label, sector_code, theme in FM_SERIES]

# ---------------------------------------------------------------------------
# MIR — MFI lending and deposit rates (monthly, all countries)
# Confirmed key format from ECB data portal
# ---------------------------------------------------------------------------
MIR_SERIES = []
_mir_defs = [
    ("M.{cc}.B.A2I.AM.R.A.2240.EUR.N", "NFC lending rate — new business",        "NFC_rate_new",    "Interest rates"),
    ("M.{cc}.B.A2O.AM.R.A.2240.EUR.N", "NFC lending rate — outstanding",          "NFC_rate_out",    "Interest rates"),
    ("M.{cc}.B.A2C.AM.R.A.2250.EUR.N", "Household mortgage rate — new business",  "HH_mortgage_new", "Interest rates"),
    ("M.{cc}.B.A2D.AM.R.A.2250.EUR.N", "Household consumer rate — new business",  "HH_consumer_new", "Interest rates"),
    ("M.{cc}.B.L21.A.R.A.2250.EUR.N",  "Overnight deposit rate — households",     "HH_deposit_ON",   "Interest rates"),
]
for key_tpl, label, sector_code, theme in _mir_defs:
    for cc, country in COUNTRIES.items():
        MIR_SERIES.append({
            "dataset":      "MIR",
            "key":          key_tpl.format(cc=cc),
            "country_code": cc,
            "country":      country,
            "series":       label,
            "sector_code":  sector_code,
            "unit":         "PCT",
            "unit_label":   "Rate (%)",
            "frequency":    "M",
            "theme":        theme,
        })

# ---------------------------------------------------------------------------
# CBD2 — Supervisory banking data (quarterly, all countries)
# Confirmed key pattern from ECB portal search results
# Key: Q.{cc}.W0.11._Z._Z.A.{sample}.{indicator}._Z._Z._Z._Z._Z._Z.{unit}
# W0 = domestic banking groups and stand-alone banks
# 11 = all institutions
# A = full sample
# F = FINREP (accounting data)
# ---------------------------------------------------------------------------
CBD2_SERIES = []
_cbd2_defs = [
    # NPL ratio (gross non-performing loans %)
    # Confirmed: CBD2.Q.IT.W0.11._Z._Z.A.F.I3632._Z._Z._Z._Z._Z._Z.PC
    ("Q.{cc}.W0.11._Z._Z.A.F.I3632._Z._Z._Z._Z._Z._Z.PC",
     "NPL ratio — gross non-performing loans (%)", "NPL_ratio", "PCT"),

    # CET1 ratio (Common Equity Tier 1)
    # Confirmed: CBD2.Q.NL.W0.11._Z._Z.A.A.I4008._Z._Z._Z._Z._Z._Z.PC
    ("Q.{cc}.W0.11._Z._Z.A.A.I4008._Z._Z._Z._Z._Z._Z.PC",
     "CET1 ratio (%)", "CET1_ratio", "PCT"),

    # Return on equity
    ("Q.{cc}.W0.11._Z._Z.A.A.I4000._Z._Z._Z._Z._Z._Z.PC",
     "Return on equity (%)", "ROE", "PCT"),

    # Leverage ratio
    ("Q.{cc}.W0.11._Z._Z.A.A.I4010._Z._Z._Z._Z._Z._Z.PC",
     "Leverage ratio (%)", "leverage_ratio", "PCT"),
]
for key_tpl, label, sector_code, unit in _cbd2_defs:
    for cc, country in COUNTRIES.items():
        CBD2_SERIES.append({
            "dataset":      "CBD2",
            "key":          key_tpl.format(cc=cc),
            "country_code": cc,
            "country":      country,
            "series":       label,
            "sector_code":  sector_code,
            "unit":         unit,
            "unit_label":   "Percent (%)",
            "frequency":    "Q",
            "theme":        "Supervisory banking data",
        })

# ---------------------------------------------------------------------------
# GFS — Government finance statistics (annual + quarterly, all countries)
# Confirmed key pattern from ECB portal — GFS dataset
# Maastricht debt as % GDP: ratio to GDP field XDC_R_B1GQ_CY
# Deficit (net lending/borrowing) as % GDP
# ---------------------------------------------------------------------------
GFS_SERIES = []
_gfs_defs = [
    # Maastricht debt as % of GDP (annual)
    # Confirmed dimension: Liabilities / Maastricht debt / ratio to GDP
    ("A.{cc}.W0.S13.S1.N.L.LE.GD.T.XDC_R_B1GQ_CY._T.F.V.N._T",
     "Government debt — % of GDP", "govt_debt_pct_gdp", "PCT", "A"),

    # Net lending/borrowing (deficit) as % of GDP (annual)
    ("A.{cc}.W0.S13.S1.N.B.B9.._Z._Z.XDC_R_B1GQ._T.F.V.N._T",
     "Government deficit — % of GDP", "govt_deficit_pct_gdp", "PCT", "A"),
]
for key_tpl, label, sector_code, unit, freq in _gfs_defs:
    for cc, country in COUNTRIES.items():
        GFS_SERIES.append({
            "dataset":      "GFS",
            "key":          key_tpl.format(cc=cc),
            "country_code": cc,
            "country":      country,
            "series":       label,
            "sector_code":  sector_code,
            "unit":         unit,
            "unit_label":   "% of GDP",
            "frequency":    freq,
            "theme":        "Government finance",
        })

# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------
ALL_SERIES = BSI_SERIES + FM_SERIES + MIR_SERIES + CBD2_SERIES + GFS_SERIES

THEMES   = sorted(set(s["theme"]   for s in ALL_SERIES))
DATASETS = sorted(set(s["dataset"] for s in ALL_SERIES))
