"""
Sensor Tower API client — extraction functions for all endpoints.
Each function fetches from the ST API and returns structured data
ready for DuckDB insertion.
"""
import time
import httpx
from typing import Any
from config import ST_BASE_URL, ST_AUTH_TOKEN, API_REQUEST_DELAY_SECONDS, API_MAX_RETRIES, API_TIMEOUT_SECONDS


# ──────────────────────────────────────────────
# HTTP Client
# ──────────────────────────────────────────────

def _get(endpoint: str, params: dict[str, Any] | None = None) -> dict | list:
    """Make an authenticated GET request to the Sensor Tower API."""
    url = f"{ST_BASE_URL}{endpoint}"
    params = params or {}
    params["auth_token"] = ST_AUTH_TOKEN

    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            resp = httpx.get(url, params=params, timeout=API_TIMEOUT_SECONDS)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 2 ** attempt
                print(f"  ⚠ Rate limited (429), waiting {wait}s (attempt {attempt}/{API_MAX_RETRIES})")
                time.sleep(wait)
                continue
            else:
                print(f"  ✗ HTTP {resp.status_code}: {resp.text[:200]}")
                return {}
        except httpx.TimeoutException:
            print(f"  ✗ Timeout (attempt {attempt}/{API_MAX_RETRIES})")
            time.sleep(2)
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return {}

    print(f"  ✗ Exhausted {API_MAX_RETRIES} retries for {endpoint}")
    return {}


def _delay():
    """Respect API rate limits."""
    time.sleep(API_REQUEST_DELAY_SECONDS)


# ══════════════════════════════════════════════
# ENDPOINT FUNCTIONS
# ══════════════════════════════════════════════

# --- #1. App Metadata ---
def fetch_app_metadata(app_ids: list[str], os: str = "unified", country: str = "US") -> list[dict]:
    """GET /v1/{os}/apps — Fetch metadata for up to 100 apps."""
    data = _get(f"/v1/{os}/apps", {
        "app_ids": ",".join(app_ids),
        "country": country,
    })
    _delay()
    return data if isinstance(data, list) else []


# --- #3. Sales Report Estimates ---
def fetch_sales_estimates(
    app_ids: list[str] | None = None,
    publisher_ids: list[str] | None = None,
    countries: list[str] | None = None,
    os: str = "unified",
    date_granularity: str = "monthly",
    start_date: str = "",
    end_date: str = "",
) -> list[dict]:
    """GET /v1/{os}/sales_report_estimates"""
    params: dict[str, Any] = {
        "date_granularity": date_granularity,
        "start_date": start_date,
        "end_date": end_date,
    }
    if app_ids:
        params["app_ids"] = ",".join(app_ids)
    if publisher_ids:
        params["publisher_ids"] = ",".join(publisher_ids)
    if countries:
        params["countries"] = ",".join(countries)
    
    data = _get(f"/v1/{os}/sales_report_estimates", params)
    _delay()
    return data if isinstance(data, list) else []


# --- #4. Active Users ---
def fetch_active_users(
    app_ids: list[str],
    os: str = "unified",
    time_period: str = "month",
    start_date: str = "",
    end_date: str = "",
) -> list[dict]:
    """GET /v1/{os}/usage/active_users"""
    data = _get(f"/v1/{os}/usage/active_users", {
        "app_ids": ",".join(app_ids),
        "time_period": time_period,
        "start_date": start_date,
        "end_date": end_date,
    })
    _delay()
    return data if isinstance(data, list) else []


# --- #6. Retention Metrics ---
def fetch_retention(
    app_ids: list[str],
    bundle: str = "retention_monthly",
    start_date: str = "",
    end_date: str = "",
) -> list | dict:
    """GET /v1/facets/metrics — retention bundle. Returns list or dict depending on ST response."""
    data = _get("/v1/facets/metrics", {
        "bundle": bundle,
        "breakdown": "date,app_id",
        "app_ids": ",".join(app_ids),
        "start_date": start_date,
        "end_date": end_date,
    })
    _delay()
    return data


# --- #11. Top Charts (Ranking) ---
def fetch_top_charts(
    os: str,
    category: str,
    chart_type: str,
    country: str,
    date: str,
    limit: int = 100,
) -> list[dict]:
    """GET /v1/{os}/ranking — top free/paid/grossing charts by category×country."""
    data = _get(f"/v1/{os}/ranking", {
        "chart_type": chart_type,
        "category": category,
        "country": country,
        "date": date,
        "limit": limit,
    })
    _delay()
    # API returns {"ranking": [app_id, ...], ...} — normalise to list of dicts
    if isinstance(data, dict):
        return [{"app_id": str(aid)} for aid in data.get("ranking", [])]
    return data if isinstance(data, list) else []


# --- #12. Top Apps by Active Users ---
def fetch_top_apps_by_active_users(
    os: str = "ios",
    comparison_attribute: str = "absolute",
    time_range: str = "month",
    measure: str = "MAU",
    date: str = "",
    category: int = 0,
    limit: int = 100,
    regions: list[str] | None = None,
) -> list[dict]:
    """GET /v1/{os}/top_and_trending/active_users"""
    params: dict[str, Any] = {
        "comparison_attribute": comparison_attribute,
        "time_range": time_range,
        "measure": measure,
        "date": date,
        "category": category,
        "limit": limit,
    }
    if regions:
        params["regions"] = ",".join(regions)
    
    data = _get(f"/v1/{os}/top_and_trending/active_users", params)
    _delay()
    return data if isinstance(data, list) else []


# --- #14. Store Summary ---
def fetch_store_summary(
    categories: list[str],
    os: str = "ios",
    date_granularity: str = "monthly",
    start_date: str = "",
    end_date: str = "",
    countries: list[str] | None = None,
) -> list[dict]:
    """GET /v1/{os}/store_summary"""
    params: dict[str, Any] = {
        "categories": ",".join(categories),
        "date_granularity": date_granularity,
        "start_date": start_date,
        "end_date": end_date,
    }
    if countries:
        params["countries"] = ",".join(countries)
    
    data = _get(f"/v1/{os}/store_summary", params)
    _delay()
    return data if isinstance(data, list) else []


# --- #18. Games Breakdown ---
def fetch_games_breakdown(
    categories: list[str],
    os: str = "ios",
    date_granularity: str = "monthly",
    start_date: str = "",
    end_date: str = "",
    countries: list[str] | None = None,
) -> list[dict]:
    """GET /v1/{os}/games_breakdown"""
    params: dict[str, Any] = {
        "categories": ",".join(categories),
        "date_granularity": date_granularity,
        "start_date": start_date,
        "end_date": end_date,
    }
    if countries:
        params["countries"] = ",".join(countries)
    
    data = _get(f"/v1/{os}/games_breakdown", params)
    _delay()
    return data if isinstance(data, list) else []


# --- #19. App IDs Discovery ---
def fetch_app_ids_by_category(
    os: str = "ios",
    category: str = "6014",
    start_date: str | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> dict:
    """GET /v1/{os}/apps/app_ids"""
    params: dict[str, Any] = {
        "category": category,
        "limit": limit,
        "offset": offset,
    }
    if start_date:
        params["start_date"] = start_date
    
    data = _get(f"/v1/{os}/apps/app_ids", params)
    _delay()
    return data if isinstance(data, dict) else {}


# --- #24. Entity Search ---
def search_entities(
    term: str,
    os: str = "unified",
    entity_type: str = "app",
    limit: int = 100,
) -> list[dict]:
    """GET /v1/{os}/search_entities"""
    data = _get(f"/v1/{os}/search_entities", {
        "entity_type": entity_type,
        "term": term,
        "limit": limit,
    })
    _delay()
    return data if isinstance(data, list) else []


# --- #28. Rating Metrics ---
def fetch_ratings(
    app_ids: list[str],
    bundle: str = "ratings_cumulative",
    breakdown: str = "app_id,date",
    start_date: str = "",
    end_date: str = "",
    date_granularity: str = "month",
) -> dict:
    """GET /v1/facets/metrics — ratings bundle."""
    data = _get("/v1/facets/metrics", {
        "bundle": bundle,
        "breakdown": breakdown,
        "app_ids": ",".join(app_ids),
        "start_date": start_date,
        "end_date": end_date,
        "date_granularity": date_granularity,
    })
    _delay()
    return data if isinstance(data, dict) else {}


# --- #29A. Review Details ---
def fetch_reviews(
    app_id: str,
    os: str = "unified",
    start_date: str = "",
    end_date: str = "",
) -> list[dict]:
    """GET /v1/{os}/review/get_reviews"""
    data = _get(f"/v1/{os}/review/get_reviews", {
        "app_ids": app_id,
        "start_date": start_date,
        "end_date": end_date,
    })
    _delay()
    return data if isinstance(data, list) else []


# --- #15. Ad Intel Top Apps ---
def fetch_ad_intel_top_apps(
    os: str = "unified",
    role: str = "advertisers",
    period: str = "month",
    date: str = "",
    category: int = 6014,
    network: str = "Facebook",
    country: str = "US",
) -> list[dict]:
    """GET /v1/{os}/ad_intel/top_apps"""
    data = _get(f"/v1/{os}/ad_intel/top_apps", {
        "role": role,
        "period": period,
        "date": date,
        "category": category,
        "network": network,
        "country": country,
    })
    _delay()
    return data if isinstance(data, list) else []


# --- #25. ASO Top Keywords ---
def fetch_aso_keywords(
    app_ids: list[str],
    regions: list[str],
    os: str = "ios",
    limit: int = 50,
) -> dict:
    """GET /v1/facets/metrics — aso_top_keywords bundle."""
    data = _get("/v1/facets/metrics", {
        "os": os,
        "bundle": "aso_top_keywords",
        "breakdown": "keyword",
        "app_ids": ",".join(app_ids),
        "regions": ",".join(regions),
        "limit": limit,
    })
    _delay()
    return data if isinstance(data, dict) else {}
