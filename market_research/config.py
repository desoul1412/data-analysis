"""
Market Research Pipeline Configuration
All settings for the local DuckDB-based pipeline.
"""
import os
from pathlib import Path

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT.parent  # d:\BaoLN2\Personal\data-analysis
DB_PATH = PROJECT_ROOT / "market_research.duckdb"

# Company data files
GAME_PERFORMANCE_CSV = DATA_DIR / "Game Performance.csv"
CHANGELOG_JAN_CSV = DATA_DIR / "2026_Jan_changelog.csv"
CHANGELOG_FEB_CSV = DATA_DIR / "2026_Feb_changelog.csv"

# ──────────────────────────────────────────────
# Sensor Tower API
# ──────────────────────────────────────────────
ST_BASE_URL = "https://api.sensortower.com"
ST_AUTH_TOKEN = os.environ.get("SENSORTOWER_AUTH_TOKEN", "")

# ──────────────────────────────────────────────
# Markets
# ──────────────────────────────────────────────
SEA_MARKETS = ["VN", "TH", "PH", "ID", "SG", "MY"]

# Mapping: company data market name → ST country code (primary, legacy)
COMPANY_MARKET_TO_ST = {
    "Vietnam": "VN",
    "ThaiLand": "TH",
    "Philippines": "PH",
    "Indonesia": "ID",
    "Sing-Malay": "SG",  # SG+MY combined in company data
    "TW-HK": "TW",
    "Global": "WW",
}

# Full ST country list per company market (used for multi-country aggregation)
COMPANY_MARKET_ST_COUNTRIES: dict[str, list[str]] = {
    "Vietnam":     ["VN"],
    "ThaiLand":    ["TH"],
    "Philippines": ["PH"],
    "Indonesia":   ["ID"],
    "Sing-Malay":  ["SG", "MY"],   # aggregate both countries
    "TW-HK":       ["TW", "HK"],   # aggregate both countries
    "Global":      ["WW"],
}

# IAP fraction per (genre_lower, company_market) — ST only captures App Store/Play revenue.
# Source: empirically calibrated 2026-04-06 from implied_iap_pct = median(ST / company_gross).
# Original manual estimates kept as comments; data-driven values applied where N >= 2.
IAP_PCT: dict[tuple[str, str], float] = {
    # MMORPG — all markets show ST captures only 0.5–4% of gross (web payment dominance)
    ("mmorpg",        "Vietnam"):     0.0065,  # was 0.10, implied=0.65%, N=43
    ("mmorpg",        "ThaiLand"):    0.0051,  # was 0.40, implied=0.51%, N=8
    ("mmorpg",        "Indonesia"):   0.0379,  # was 0.25, implied=3.79%, N=11
    ("mmorpg",        "Philippines"): 0.0379,  # no data, use Indonesia proxy
    ("mmorpg",        "Sing-Malay"):  0.0183,  # was 0.35, implied=1.83%, N=9
    # Idle RPG / Idler
    ("idle rpg",      "Vietnam"):     0.0237,  # implied=2.37%, N=4
    ("idle rpg",      "ThaiLand"):    0.0141,
    ("idle rpg",      "Indonesia"):   0.0254,
    ("idle rpg",      "Philippines"): 0.0300,
    ("idle rpg",      "Sing-Malay"):  0.0183,
    ("idler",         "Vietnam"):     0.0237,
    ("idler",         "ThaiLand"):    0.0141,  # implied=1.41%, N=2
    ("idler",         "Indonesia"):   0.0254,  # implied=2.54%, N=5
    ("idler",         "Philippines"): 0.0030,  # implied=0.30%, N=1
    ("idler",         "Sing-Malay"):  0.0183,
    # Tycoon / Crafting
    ("tycoon / crafting", "Vietnam"):     0.0336,  # implied=3.36%, N=4
    ("tycoon / crafting", "ThaiLand"):    0.0300,
    ("tycoon / crafting", "Indonesia"):   0.0350,
    ("tycoon / crafting", "Philippines"): 0.0300,
    ("tycoon / crafting", "Sing-Malay"):  0.0300,
    # Turn-based RPG
    ("turn-based rpg", "Vietnam"):    0.0434,  # implied=4.34%, N=2
    ("turn-based rpg", "ThaiLand"):   0.2677,  # implied=26.77%, N=1 (higher — possibly more IAP)
    ("turn-based rpg", "Indonesia"):  0.0909,  # implied=9.09%, N=1
    ("turn-based rpg", "Sing-Malay"): 0.0389,  # implied=3.89%, N=1
    ("turn-based rpg", "Philippines"):0.0500,
    # Platformer / Runner (e.g. YS in Sing-Malay — higher IAP%)
    ("platformer / runner", "Vietnam"):    0.0700,
    ("platformer / runner", "ThaiLand"):   0.0700,
    ("platformer / runner", "Indonesia"):  0.0700,
    ("platformer / runner", "Philippines"):0.0700,
    ("platformer / runner", "Sing-Malay"): 0.2167,  # implied=21.67%, N=17
    # Squad RPG
    ("squad rpg",     "Vietnam"):     0.0150,  # implied=1.50%, N=1
    ("squad rpg",     "ThaiLand"):    0.0150,
    ("squad rpg",     "Indonesia"):   0.0150,
    ("squad rpg",     "Philippines"): 0.0150,
    ("squad rpg",     "Sing-Malay"):  0.0150,
    # Shoot 'em up / Action shooters
    ("shoot 'em up",  "Vietnam"):     0.0007,  # implied=0.07%, N=1 (extremely low)
    ("shoot 'em up",  "ThaiLand"):    0.0013,  # implied=0.13%, N=2
    ("shoot 'em up",  "Indonesia"):   0.0350,
    ("shoot 'em up",  "Philippines"): 0.0300,
    ("shoot 'em up",  "Sing-Malay"):  0.0300,
    # Legacy keys (kept for backward compat with manual genre tagging)
    ("team battle",   "Vietnam"):     0.0200,
    ("team battle",   "ThaiLand"):    0.0200,
    ("team battle",   "Indonesia"):   0.0300,
    ("team battle",   "Philippines"): 0.0500,
    ("team battle",   "Sing-Malay"):  0.0300,
    ("battle royale", "Vietnam"):     0.0200,
    ("battle royale", "ThaiLand"):    0.0200,
    ("battle royale", "Indonesia"):   0.0300,
    ("battle royale", "Philippines"): 0.0300,
    ("battle royale", "Sing-Malay"):  0.0300,
    ("shooting",      "Vietnam"):     0.0200,
    ("shooting",      "ThaiLand"):    0.0200,
    ("shooting",      "Indonesia"):   0.0300,
    ("shooting",      "Philippines"): 0.0300,
    ("shooting",      "Sing-Malay"):  0.0300,
    ("moba",          "Vietnam"):     0.0300,
    ("moba",          "ThaiLand"):    0.0300,
    ("moba",          "Indonesia"):   0.0300,
    ("moba",          "Philippines"): 0.0300,
    ("moba",          "Sing-Malay"):  0.0300,
}
# Fallback IAP % by market when genre is unknown.
# Updated 2026-04-06: data shows SEA mobile games are predominantly web-payment driven.
IAP_PCT_MARKET_DEFAULT: dict[str, float] = {
    "Vietnam":     0.0100,  # was 0.40 — ST captures ~1% of VN gross on average
    "ThaiLand":    0.0150,  # was 0.45
    "Indonesia":   0.0350,  # was 0.35 — already close; confirmed by data
    "Philippines": 0.0300,  # was 0.30 — limited data, conservative
    "Sing-Malay":  0.0300,  # was 0.40
    "TW-HK":       0.85,    # TW/HK are predominantly IAP-driven — unchanged
}
IAP_PCT_FALLBACK = 0.02  # global fallback (was 0.40)

# ──────────────────────────────────────────────
# API rate limiting
# ──────────────────────────────────────────────
API_REQUEST_DELAY_SECONDS = 0.5   # Minimum delay between API calls
API_MAX_RETRIES = 3
API_TIMEOUT_SECONDS = 30

# ──────────────────────────────────────────────
# DuckDB schemas (used as table prefixes in DuckDB)
# ──────────────────────────────────────────────
SCHEMA_RAW = "raw"
SCHEMA_DIM = "dim"
SCHEMA_FACT = "fact"
SCHEMA_ANALYTICS = "analytics"
