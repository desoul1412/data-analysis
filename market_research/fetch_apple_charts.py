"""
Apple RSS Charts fetcher — free public data, no API key needed.
Fetches top-grossing iOS games for SEA + TW/HK markets from Apple's
marketing tools RSS feed and loads into fact.fact_apple_public_charts.

Usage:
    python fetch_apple_charts.py
"""
import time
import httpx
import duckdb
import pandas as pd
from datetime import date
from config import DB_PATH

COUNTRIES = ['vn', 'th', 'ph', 'id', 'sg', 'my', 'tw', 'hk']

# URL candidates in priority order (first one that returns 200 wins)
_APPLE_URL_CANDIDATES = [
    # New domain (rss.marketingtools.apple.com was renamed to rss.applemarketingtools.com)
    "https://rss.applemarketingtools.com/api/v2/{country}/apps/top-grossing/200/apps.json",
    # Original URL (returns 404 as of 2026-04 but kept as fallback for future)
    "https://rss.marketingtools.apple.com/api/v2/{country}/apps/top-grossing/200/apps.json",
    # Legacy iTunes RSS endpoint (returns XML-in-JSON via limit param)
    "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/RSS/"
    "topgrossingapplications/limit=200/json?cc={country}",
]


def _try_apple_urls(country: str) -> tuple[list[dict], str | None]:
    """
    Try each URL candidate in order; return (results, url_used) for the first success.
    Returns ([], None) if all fail.
    """
    for url_tmpl in _APPLE_URL_CANDIDATES:
        url = url_tmpl.format(country=country)
        try:
            resp = httpx.get(url, timeout=30, follow_redirects=True)
            if resp.status_code != 200:
                continue
            data = resp.json()
            # applemarketingtools format: {"feed": {"results": [...]}}
            if 'feed' in data:
                results = data['feed'].get('results', [])
                if results:
                    return results, url
            # iTunes RSS format: {"feed": {"entry": [...]}}
            if 'feed' in data and 'entry' in data['feed']:
                raw_entries = data['feed']['entry']
                # Normalise to same shape as applemarketingtools results
                results = [
                    {
                        'id': e.get('id', {}).get('attributes', {}).get('im:id', ''),
                        'name': e.get('im:name', {}).get('label', ''),
                        'artistName': e.get('im:artist', {}).get('label', ''),
                    }
                    for e in raw_entries if isinstance(e, dict)
                ]
                if results:
                    return results, url
        except Exception:
            continue
    return [], None


def fetch_apple_top_grossing(country: str) -> list[dict]:
    """Fetch top-grossing iOS apps for a country, trying multiple URL formats."""
    results, url_used = _try_apple_urls(country)
    if not results:
        print(f"  ✗ Apple RSS [{country.upper()}]: all URL candidates failed")
    return results


def load_apple_charts(db_path_str: str | None = None):
    """Fetch and upsert Apple top-grossing charts for all configured countries."""
    path = db_path_str or str(DB_PATH)
    con = duckdb.connect(path)
    today = date.today().isoformat()
    total = 0

    for country in COUNTRIES:
        print(f"  Fetching Apple charts [{country.upper()}]...")
        results = fetch_apple_top_grossing(country)
        if not results:
            continue

        rows = []
        for rank, item in enumerate(results, start=1):
            app_id = item.get('id', '')
            if not app_id:
                continue
            rows.append({
                'app_id': str(app_id),
                'country': country.upper(),
                'date': today,
                'chart_type': 'grossing',
                'rank': rank,
                'name': item.get('name', ''),
                'publisher_name': item.get('artistName', ''),
            })

        if not rows:
            continue

        df = pd.DataFrame(rows)
        con.execute(
            f"DELETE FROM fact.fact_apple_public_charts "
            f"WHERE country = '{country.upper()}' AND date = '{today}' AND chart_type = 'grossing'"
        )
        con.execute(
            "INSERT INTO fact.fact_apple_public_charts "
            "(app_id, country, date, chart_type, rank, name, publisher_name, loaded_at) "
            "SELECT app_id, country, date, chart_type, rank, name, publisher_name, "
            "current_timestamp FROM df"
        )
        total += len(rows)
        print(f"    ✓ {len(rows)} apps for {country.upper()}")
        time.sleep(0.5)

    con.close()
    print(f"  ✓ Apple charts: {total} total rows across {len(COUNTRIES)} countries")


if __name__ == "__main__":
    load_apple_charts()
