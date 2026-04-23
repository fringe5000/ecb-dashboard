"""
ECB Dataset Registry
Defines every series to fetch from the ECB SDMX API.
Each entry maps to one or more API calls.
"""

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
# LBSI — Estimated MFI Loans by Sector of Economic Activity (quarterly)
# Key: Q.{country}.N.R.A20.A.1.{nace}.Z0Z.EUR.E
# ---------------------------------------------------------------------------
LBSI_SECTORS = {
    "A":        "Agriculture, forestry & fishing",
    "BC":       "Industry (excl. construction)",
    "F":        "Construction",
    "G":        "Wholesale & retail trade",
    "H":        "Transport & storage",
    "I":        "Accommodation & food services",
    "JA":       "Publishing, broadcasting & telecoms",
    "JB":       "IT & information services",
    "K":        "Financial & insurance activities",
    "L":        "Real estate",
    "MN":       "Professional & support services",
    "OPQRSTU":  "Other services",
}

LBSI_SERIES = []
for country_code, country_name in COUNTRIES.items():
    for nace_code, nace_name in LBSI_SECTORS.items():
        key = f"Q.{country_code}.N.R.A20.A.1.{nace_code}.Z0Z.EUR.E"
        LBSI_SERIES.append({
            "dataset":      "LBSI",
            "key":          key,
            "country_code": country_code,
            "country":      country_name,
            "series":       nace_name,
            "sector_code":  nace_code,
            "unit":         "EUR",
            "unit_label":   "Outstanding amounts (€)",
            "frequency":    "Q",
            "theme":        "Bank lending by sector",
        })

# ---------------------------------------------------------------------------
# BSI — MFI Balance Sheet Items
# Total NFC loans: outstanding + annual growth rate (monthly)
# ---------------------------------------------------------------------------
BSI_SERIES = []
BSI_DEFINITIONS = [
    {
        "label":        "NFC loans — outstanding amounts",
        "key_tpl":      "M.{cc}.N.R.LRE.X.1.A1.2240.Z01.E",
        "unit":         "EUR",
        "unit_label":   "Outstanding amounts (€)",
        "sector_code":  "NFC_total",
    },
    {
        "label":        "NFC loans — annual growth rate",
        "key_tpl":      "M.{cc}.N.R.LRE.X.1.A1.2240.Z01.I",
        "unit":         "PCT",
        "unit_label":   "Annual growth rate (%)",
        "sector_code":  "NFC_growth",
    },
    {
        "label":        "Household loans — outstanding amounts",
        "key_tpl":      "M.{cc}.N.R.LRE.X.1.A1.2250.Z01.E",
        "unit":         "EUR",
        "unit_label":   "Outstanding amounts (€)",
        "sector_code":  "HH_total",
    },
    {
        "label":        "Household loans — annual growth rate",
        "key_tpl":      "M.{cc}.N.R.LRE.X.1.A1.2250.Z01.I",
        "unit":         "PCT",
        "unit_label":   "Annual growth rate (%)",
        "sector_code":  "HH_growth",
    },
    {
        "label":        "M1 — outstanding amounts",
        "key_tpl":      "M.{cc}.Y.V.M10.X.1.U2.2300.Z0Z.E",
        "unit":         "EUR",
        "unit_label":   "Outstanding amounts (€)",
        "sector_code":  "M1",
    },
    {
        "label":        "M3 — outstanding amounts",
        "key_tpl":      "M.{cc}.Y.V.M30.X.1.U2.2300.Z0Z.E",
        "unit":         "EUR",
        "unit_label":   "Outstanding amounts (€)",
        "sector_code":  "M3",
    },
    {
        "label":        "M3 — annual growth rate",
        "key_tpl":      "M.{cc}.Y.V.M30.X.1.U2.2300.Z0Z.I",
        "unit":         "PCT",
        "unit_label":   "Annual growth rate (%)",
        "sector_code":  "M3_growth",
    },
]

for defn in BSI_DEFINITIONS:
    for country_code, country_name in COUNTRIES.items():
        key = defn["key_tpl"].format(cc=country_code)
        BSI_SERIES.append({
            "dataset":      "BSI",
            "key":          key,
            "country_code": country_code,
            "country":      country_name,
            "series":       defn["label"],
            "sector_code":  defn["sector_code"],
            "unit":         defn["unit"],
            "unit_label":   defn["unit_label"],
            "frequency":    "M",
            "theme":        "Money, credit & banking",
        })

# ---------------------------------------------------------------------------
# MIR — MFI Interest Rates (monthly)
# ---------------------------------------------------------------------------
MIR_SERIES = []
MIR_DEFINITIONS = [
    {
        "label":       "NFC lending rate — new business (all maturities)",
        "key_tpl":     "M.{cc}.B.A2I.AM.R.A.2240.EUR.N",
        "sector_code": "NFC_rate_new",
        "unit":        "PCT",
        "unit_label":  "Interest rate (%)",
    },
    {
        "label":       "NFC lending rate — outstanding amounts",
        "key_tpl":     "M.{cc}.B.A2O.AM.R.A.2240.EUR.N",
        "sector_code": "NFC_rate_out",
        "unit":        "PCT",
        "unit_label":  "Interest rate (%)",
    },
    {
        "label":       "Household mortgage rate — new business",
        "key_tpl":     "M.{cc}.B.A2C.AM.R.A.2250.EUR.N",
        "sector_code": "HH_mortgage_new",
        "unit":        "PCT",
        "unit_label":  "Interest rate (%)",
    },
    {
        "label":       "Household consumer credit rate — new business",
        "key_tpl":     "M.{cc}.B.A2D.AM.R.A.2250.EUR.N",
        "sector_code": "HH_consumer_new",
        "unit":        "PCT",
        "unit_label":  "Interest rate (%)",
    },
    {
        "label":       "Overnight deposit rate — households",
        "key_tpl":     "M.{cc}.B.L21.A.R.A.2250.EUR.N",
        "sector_code": "HH_deposit_ON",
        "unit":        "PCT",
        "unit_label":  "Interest rate (%)",
    },
]

for defn in MIR_DEFINITIONS:
    for country_code, country_name in COUNTRIES.items():
        key = defn["key_tpl"].format(cc=country_code)
        MIR_SERIES.append({
            "dataset":      "MIR",
            "key":          key,
            "country_code": country_code,
            "country":      country_name,
            "series":       defn["label"],
            "sector_code":  defn["sector_code"],
            "unit":         defn["unit"],
            "unit_label":   defn["unit_label"],
            "frequency":    "M",
            "theme":        "Interest rates",
        })

# ---------------------------------------------------------------------------
# FM — Financial Market Data (daily/monthly)
# ---------------------------------------------------------------------------
FM_SERIES = [
    {
        "dataset":      "FM",
        "key":          "M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
        "country_code": "U2",
        "country":      "Euro area",
        "series":       "Euribor 1-month",
        "sector_code":  "EURIBOR1M",
        "unit":         "PCT",
        "unit_label":   "Rate (%)",
        "frequency":    "M",
        "theme":        "Interest rates",
    },
    {
        "dataset":      "FM",
        "key":          "M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
        "country_code": "U2",
        "country":      "Euro area",
        "series":       "Euribor 3-month",
        "sector_code":  "EURIBOR3M",
        "unit":         "PCT",
        "unit_label":   "Rate (%)",
        "frequency":    "M",
        "theme":        "Interest rates",
    },
    {
        "dataset":      "FM",
        "key":          "M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
        "country_code": "U2",
        "country":      "Euro area",
        "series":       "Euribor 6-month",
        "sector_code":  "EURIBOR6M",
        "unit":         "PCT",
        "unit_label":   "Rate (%)",
        "frequency":    "M",
        "theme":        "Interest rates",
    },
    {
        "dataset":      "FM",
        "key":          "M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA",
        "country_code": "U2",
        "country":      "Euro area",
        "series":       "Euribor 12-month",
        "sector_code":  "EURIBOR12M",
        "unit":         "PCT",
        "unit_label":   "Rate (%)",
        "frequency":    "M",
        "theme":        "Interest rates",
    },
]

# ---------------------------------------------------------------------------
# ICP — Inflation / HICP (monthly)
# ---------------------------------------------------------------------------
ICP_SERIES = []
ICP_DEFINITIONS = [
    {"label": "HICP — all items", "key_tpl": "M.{cc}.N.000000.4.ANR", "sector_code": "HICP_all"},
    {"label": "HICP — food",      "key_tpl": "M.{cc}.N.01.4.ANR",     "sector_code": "HICP_food"},
    {"label": "HICP — energy",    "key_tpl": "M.{cc}.N.045.4.ANR",    "sector_code": "HICP_energy"},
    {"label": "HICP — services",  "key_tpl": "M.{cc}.N.S.4.ANR",      "sector_code": "HICP_services"},
    {"label": "HICP — goods",     "key_tpl": "M.{cc}.N.G.4.ANR",      "sector_code": "HICP_goods"},
]

for defn in ICP_DEFINITIONS:
    for country_code, country_name in COUNTRIES.items():
        key = defn["key_tpl"].format(cc=country_code)
        ICP_SERIES.append({
            "dataset":      "ICP",
            "key":          key,
            "country_code": country_code,
            "country":      country_name,
            "series":       defn["label"],
            "sector_code":  defn["sector_code"],
            "unit":         "PCT",
            "unit_label":   "Annual rate of change (%)",
            "frequency":    "M",
            "theme":        "Inflation",
        })

# ---------------------------------------------------------------------------
# ECB Policy Rates (monthly) — from FM dataset
# ---------------------------------------------------------------------------
POLICY_SERIES = [
    {
        "dataset":      "FM",
        "key":          "M.U2.EUR.4F.KR.MRR_FR.LEV",
        "country_code": "U2",
        "country":      "Euro area",
        "series":       "ECB main refinancing rate",
        "sector_code":  "ECB_MRR",
        "unit":         "PCT",
        "unit_label":   "Rate (%)",
        "frequency":    "M",
        "theme":        "ECB policy rates",
    },
    {
        "dataset":      "FM",
        "key":          "M.U2.EUR.4F.KR.DFR.LEV",
        "country_code": "U2",
        "country":      "Euro area",
        "series":       "ECB deposit facility rate",
        "sector_code":  "ECB_DFR",
        "unit":         "PCT",
        "unit_label":   "Rate (%)",
        "frequency":    "M",
        "theme":        "ECB policy rates",
    },
    {
        "dataset":      "FM",
        "key":          "M.U2.EUR.4F.KR.MLFR.LEV",
        "country_code": "U2",
        "country":      "Euro area",
        "series":       "ECB marginal lending facility rate",
        "sector_code":  "ECB_MLF",
        "unit":         "PCT",
        "unit_label":   "Rate (%)",
        "frequency":    "M",
        "theme":        "ECB policy rates",
    },
]

# ---------------------------------------------------------------------------
# All series combined
# ---------------------------------------------------------------------------
ALL_SERIES = LBSI_SERIES + BSI_SERIES + MIR_SERIES + FM_SERIES + ICP_SERIES + POLICY_SERIES

THEMES = sorted(set(s["theme"] for s in ALL_SERIES))
DATASETS = sorted(set(s["dataset"] for s in ALL_SERIES))
