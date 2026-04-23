"""
ECB Dataset Registry — verified series keys only.
Datasets: BSI (lending + money supply), MIR (rates), FM (Euribor + policy), ICP (inflation)
Countries: Euro area + 11 member states
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
# NACE sector codes in the BSI counterpart sector dimension
# Used by fetch_bsi_nace() in ecb_scraper.py
# ---------------------------------------------------------------------------
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
}

# ---------------------------------------------------------------------------
# BSI — aggregate NFC and household loans + money supply (monthly, all countries)
# ---------------------------------------------------------------------------
BSI_SERIES = []
_bsi_defs = [
    ("M.{cc}.N.A.A20.A.1.U2.2240.Z01.E", "NFC loans — outstanding",        "NFC_total",  "EUR", "M"),
    ("M.{cc}.N.A.A20.A.I.U2.2240.Z01.A", "NFC loans — annual growth rate", "NFC_growth", "PCT", "M"),
    ("M.{cc}.N.A.A20.A.1.U2.2250.Z01.E", "Household loans — outstanding",  "HH_total",   "EUR", "M"),
    ("M.{cc}.N.A.A20.A.I.U2.2250.Z01.A", "Household loans — growth rate",  "HH_growth",  "PCT", "M"),
]
for _key_tpl, _label, _sector_code, _unit, _freq in _bsi_defs:
    for _cc, _country in COUNTRIES.items():
        BSI_SERIES.append({
            "dataset":      "BSI",
            "key":          _key_tpl.format(cc=_cc),
            "country_code": _cc,
            "country":      _country,
            "series":       _label,
            "sector_code":  _sector_code,
            "unit":         _unit,
            "unit_label":   "Outstanding (€ millions)" if _unit == "EUR" else "Annual rate (%)",
            "frequency":    _freq,
            "theme":        "Bank lending by sector",
        })

# M1 and M3 — euro area only
_money_defs = [
    ("M.U2.Y.V.M10.X.1.U2.2300.Z01.E", "M1 — outstanding",        "M1",        "EUR", "M"),
    ("M.U2.Y.V.M30.X.1.U2.2300.Z01.E", "M3 — outstanding",        "M3",        "EUR", "M"),
    ("M.U2.Y.V.M30.X.1.U2.2300.Z01.I", "M3 — annual growth rate", "M3_growth", "PCT", "M"),
]
for _key, _label, _sector_code, _unit, _freq in _money_defs:
    BSI_SERIES.append({
        "dataset":      "BSI",
        "key":          _key,
        "country_code": "U2",
        "country":      "Euro area",
        "series":       _label,
        "sector_code":  _sector_code,
        "unit":         _unit,
        "unit_label":   "Outstanding (€ millions)" if _unit == "EUR" else "Annual rate (%)",
        "frequency":    _freq,
        "theme":        "Money, credit & banking",
    })

# ---------------------------------------------------------------------------
# FM — Euribor (monthly) + ECB policy rates (daily)
# ---------------------------------------------------------------------------
FM_SERIES = []
_fm_defs = [
    ("M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA", "Euribor 1-month",           "EURIBOR1M",  "M", "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", "Euribor 3-month",           "EURIBOR3M",  "M", "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA", "Euribor 6-month",           "EURIBOR6M",  "M", "Interest rates"),
    ("M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA", "Euribor 12-month",          "EURIBOR12M", "M", "Interest rates"),
    ("D.U2.EUR.4F.KR.MRR_RT.LEV",       "ECB main refinancing rate", "ECB_MRR",    "D", "ECB policy rates"),
    ("D.U2.EUR.4F.KR.DFR.LEV",          "ECB deposit facility rate", "ECB_DFR",    "D", "ECB policy rates"),
    ("D.U2.EUR.4F.KR.MLFR.LEV",         "ECB marginal lending rate", "ECB_MLF",    "D", "ECB policy rates"),
]
for _key, _label, _sector_code, _freq, _theme in _fm_defs:
    FM_SERIES.append({
        "dataset":      "FM",
        "key":          _key,
        "country_code": "U2",
        "country":      "Euro area",
        "series":       _label,
        "sector_code":  _sector_code,
        "unit":         "PCT",
        "unit_label":   "Rate (%)",
        "frequency":    _freq,
        "theme":        _theme,
    })

# ---------------------------------------------------------------------------
# MIR — MFI lending and deposit rates (monthly, all countries)
# ---------------------------------------------------------------------------
MIR_SERIES = []
_mir_defs = [
    ("M.{cc}.B.A2I.AM.R.A.2240.EUR.N", "NFC lending rate — new business",       "NFC_rate_new",    "Interest rates"),
    ("M.{cc}.B.A2C.AM.R.A.2250.EUR.N", "Household mortgage rate — new business", "HH_mortgage_new", "Interest rates"),
    ("M.{cc}.B.L21.A.R.A.2250.EUR.N",  "Overnight deposit rate — households",    "HH_deposit_ON",   "Interest rates"),
]
for _key_tpl, _label, _sector_code, _theme in _mir_defs:
    for _cc, _country in COUNTRIES.items():
        MIR_SERIES.append({
            "dataset":      "MIR",
            "key":          _key_tpl.format(cc=_cc),
            "country_code": _cc,
            "country":      _country,
            "series":       _label,
            "sector_code":  _sector_code,
            "unit":         "PCT",
            "unit_label":   "Rate (%)",
            "frequency":    "M",
            "theme":        _theme,
        })

# ---------------------------------------------------------------------------
# ICP — HICP inflation (monthly, all countries)
# Using confirmed special aggregate codes from ECB portal
# ---------------------------------------------------------------------------
ICP_SERIES = []
_icp_defs = [
    ("M.{cc}.N.000000.4.ANR", "HICP — all items",          "HICP_all",      "Inflation"),
    ("M.{cc}.N.011000.4.ANR", "HICP — food",               "HICP_food",     "Inflation"),
    ("M.{cc}.N.NRGY00.4.ANR", "HICP — energy",             "HICP_energy",   "Inflation"),
    ("M.{cc}.N.SERV00.4.ANR", "HICP — services",           "HICP_services", "Inflation"),
    ("M.{cc}.N.IGXE00.4.ANR", "HICP — goods excl. energy", "HICP_goods",    "Inflation"),
]
for _key_tpl, _label, _sector_code, _theme in _icp_defs:
    for _cc, _country in COUNTRIES.items():
        ICP_SERIES.append({
            "dataset":      "ICP",
            "key":          _key_tpl.format(cc=_cc),
            "country_code": _cc,
            "country":      _country,
            "series":       _label,
            "sector_code":  _sector_code,
            "unit":         "PCT",
            "unit_label":   "Annual rate of change (%)",
            "frequency":    "M",
            "theme":        _theme,
        })

# ---------------------------------------------------------------------------
# CBD2 — Supervisory banking data (quarterly, all countries)
# ---------------------------------------------------------------------------
CBD2_SERIES = []
_cbd2_defs = [
    ("Q.{cc}.W0.11._Z._Z.A.F.I3632._Z._Z._Z._Z._Z._Z.PC",
     "NPL ratio — gross non-performing loans (%)", "NPL_ratio",    "PCT"),
    ("Q.{cc}.W0.11._Z._Z.A.A.I4008._Z._Z._Z._Z._Z._Z.PC",
     "CET1 ratio (%)",                             "CET1_ratio",   "PCT"),
    ("Q.{cc}.W0.11._Z._Z.A.A.I4000._Z._Z._Z._Z._Z._Z.PC",
     "Return on equity (%)",                       "ROE",          "PCT"),
    ("Q.{cc}.W0.11._Z._Z.A.A.I4010._Z._Z._Z._Z._Z._Z.PC",
     "Leverage ratio (%)",                         "leverage_ratio","PCT"),
]
for _key_tpl, _label, _sector_code, _unit in _cbd2_defs:
    for _cc, _country in COUNTRIES.items():
        CBD2_SERIES.append({
            "dataset":      "CBD2",
            "key":          _key_tpl.format(cc=_cc),
            "country_code": _cc,
            "country":      _country,
            "series":       _label,
            "sector_code":  _sector_code,
            "unit":         _unit,
            "unit_label":   "Percent (%)",
            "frequency":    "Q",
            "theme":        "Supervisory banking data",
        })

# ---------------------------------------------------------------------------
# GFS — Government finance statistics (annual, all countries)
# ---------------------------------------------------------------------------
GFS_SERIES = []
_gfs_defs = [
    ("A.{cc}.W0.S13.S1.N.L.LE.GD.T.XDC_R_B1GQ_CY._T.F.V.N._T",
     "Government debt — % of GDP",    "govt_debt_pct_gdp",   "PCT"),
    ("A.{cc}.W0.S13.S1.N.B.B9.._Z._Z.XDC_R_B1GQ._T.F.V.N._T",
     "Government deficit — % of GDP", "govt_deficit_pct_gdp","PCT"),
]
for _key_tpl, _label, _sector_code, _unit in _gfs_defs:
    for _cc, _country in COUNTRIES.items():
        GFS_SERIES.append({
            "dataset":      "GFS",
            "key":          _key_tpl.format(cc=_cc),
            "country_code": _cc,
            "country":      _country,
            "series":       _label,
            "sector_code":  _sector_code,
            "unit":         _unit,
            "unit_label":   "% of GDP",
            "frequency":    "A",
            "theme":        "Government finance",
        })

# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------
ALL_SERIES = BSI_SERIES + FM_SERIES + MIR_SERIES + ICP_SERIES + CBD2_SERIES + GFS_SERIES

THEMES   = sorted(set(s["theme"]   for s in ALL_SERIES))
DATASETS = sorted(set(s["dataset"] for s in ALL_SERIES))
