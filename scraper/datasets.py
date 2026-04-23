"""
ECB Dataset Registry — corrected series keys based on actual BSI DSD.
NACE sector loans use the BSI dataset with counterpart sector codes 2240A, 2240F etc.
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
# NACE sector loans — BSI dataset, quarterly
# Key: Q.{country}.N.A.A20.A.1.U2.{nace_code}.Z01.E
# NACE codes are embedded in the BS counterpart sector dimension
# Euro area (U2) only — country breakdowns not published at NACE level
# ---------------------------------------------------------------------------
NACE_SECTORS = {
    "2240A":   "Agriculture, forestry & fishing",
    "2240B":   "Mining & quarrying",
    "2240C":   "Manufacturing",
    "2240DE":  "Electricity, gas, water & waste",
    "2240F":   "Construction",
    "2240G":   "Wholesale & retail trade",
    "2240HJ":  "Transport, storage & communication",
    "2240LMN": "Real estate & professional services",
    "2240Z":   "Other services",
}

LBSI_SERIES = []
for nace_code, nace_name in NACE_SECTORS.items():
    key = f"Q.U2.N.A.A20.A.1.U2.{nace_code}.Z01.E"
    LBSI_SERIES.append({
        "dataset":      "BSI",
        "key":          key,
        "country_code": "U2",
        "country":      "Euro area",
        "series":       nace_name,
        "sector_code":  nace_code,
        "unit":         "EUR",
        "unit_label":   "Outstanding amounts (€ millions)",
        "frequency":    "Q",
        "theme":        "Bank lending by sector",
    })

# ---------------------------------------------------------------------------
# BSI — aggregate NFC and household loans + money supply (monthly)
# ---------------------------------------------------------------------------
BSI_DEFINITIONS = [
    {
        "label":       "NFC loans — outstanding amounts",
        "key_tpl":     "M.{cc}.N.A.A20.A.1.U2.2240.Z01.E",
        "unit":        "EUR",
        "unit_label":  "Outstanding amounts (€ millions)",
        "sector_code": "NFC_total",
    },
    {
        "label":       "NFC loans — annual growth rate",
        "key_tpl":     "M.{cc}.N.A.A20.A.I.U2.2240.Z01.A",
        "unit":        "PCT",
        "unit_label":  "Annual growth rate (%)",
        "sector_code": "NFC_growth",
    },
    {
        "label":       "Household loans — outstanding amounts",
        "key_tpl":     "M.{cc}.N.A.A20.A.1.U2.2250.Z01.E",
        "unit":        "EUR",
        "unit_label":  "Outstanding amounts (€ millions)",
        "sector_code": "HH_total",
    },
    {
        "label":       "Household loans — annual growth rate",
        "key_tpl":     "M.{cc}.N.A.A20.A.I.U2.2250.Z01.A",
        "unit":        "PCT",
        "unit_label":  "Annual growth rate (%)",
        "sector_code": "HH_growth",
    },
    {
        "label":       "M1 — outstanding amounts",
        "key_tpl":     "M.{cc}.Y.V.M10.X.1.U2.2300.Z0Z.E",
        "unit":        "EUR",
        "unit_label":  "Outstanding amounts (€ millions)",
        "sector_code": "M1",
    },
    {
        "label":       "M3 — outstanding amounts",
        "key_tpl":     "M.{cc}.Y.V.M30.X.1.U2.2300.Z0Z.E",
        "unit":        "EUR",
        "unit_label":  "Outstanding amounts (€ millions)",
        "sector_code": "M3",
    },
    {
        "label":       "M3 — annual growth rate",
        "key_tpl":     "M.{cc}.Y.V.M30.X.1.U2.2300.Z0Z.I",
        "unit":        "PCT",
        "unit_label":  "Annual growth rate (%)",
        "sector_code": "M3_growth",
    },
]

BSI_SERIES = []
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
MIR_DEFINITIONS = [
    {
        "label":       "NFC lending rate — new business",
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

MIR_SERIES = []
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
# FM — Euribor + ECB policy rates (monthly)
# ---------------------------------------------------------------------------
FM_SERIES = [
    { "key": "M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA", "series": "Euribor 1-month",               "sector_code": "EURIBOR1M",  "theme": "Interest rates" },
    { "key": "M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", "series": "Euribor 3-month",               "sector_code": "EURIBOR3M",  "theme": "Interest rates" },
    { "key": "M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA", "series": "Euribor 6-month",               "sector_code": "EURIBOR6M",  "theme": "Interest rates" },
    { "key": "M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA", "series": "Euribor 12-month",              "sector_code": "EURIBOR12M", "theme": "Interest rates" },
    { "key": "M.U2.EUR.4F.KR.MRR_FR.LEV",       "series": "ECB main refinancing rate",     "sector_code": "ECB_MRR",    "theme": "ECB policy rates" },
    { "key": "M.U2.EUR.4F.KR.DFR.LEV",          "series": "ECB deposit facility rate",     "sector_code": "ECB_DFR",    "theme": "ECB policy rates" },
    { "key": "M.U2.EUR.4F.KR.MLFR.LEV",         "series": "ECB marginal lending rate",     "sector_code": "ECB_MLF",    "theme": "ECB policy rates" },
]

FM_SERIES = [{
    "dataset":      "FM",
    "key":          s["key"],
    "country_code": "U2",
    "country":      "Euro area",
    "series":       s["series"],
    "sector_code":  s["sector_code"],
    "unit":         "PCT",
    "unit_label":   "Rate (%)",
    "frequency":    "M",
    "theme":        s["theme"],
} for s in FM_SERIES]

# ---------------------------------------------------------------------------
# ICP — HICP Inflation (monthly)
# ---------------------------------------------------------------------------
ICP_DEFINITIONS = [
    { "label": "HICP — all items", "key_tpl": "M.{cc}.N.000000.4.ANR", "sector_code": "HICP_all" },
    { "label": "HICP — food",      "key_tpl": "M.{cc}.N.01.4.ANR",     "sector_code": "HICP_food" },
    { "label": "HICP — energy",    "key_tpl": "M.{cc}.N.045.4.ANR",    "sector_code": "HICP_energy" },
    { "label": "HICP — services",  "key_tpl": "M.{cc}.N.S.4.ANR",      "sector_code": "HICP_services" },
    { "label": "HICP — goods",     "key_tpl": "M.{cc}.N.G.4.ANR",      "sector_code": "HICP_goods" },
]

ICP_SERIES = []
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
# Combined
# ---------------------------------------------------------------------------
ALL_SERIES = LBSI_SERIES + BSI_SERIES + MIR_SERIES + FM_SERIES + ICP_SERIES

THEMES   = sorted(set(s["theme"]   for s in ALL_SERIES))
DATASETS = sorted(set(s["dataset"] for s in ALL_SERIES))
