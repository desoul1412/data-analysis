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

# Mapping: company data market name → ST country code
COMPANY_MARKET_TO_ST = {
    "Vietnam": "VN",
    "ThaiLand": "TH",
    "Philippines": "PH",
    "Indonesia": "ID",
    "Sing-Malay": "SG",  # SG+MY combined in company data
    "TW-HK": "TW",
    "Global": "WW",
}

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
