"""
ST Load — API response → DuckDB fact tables.
Handles upsert (INSERT OR REPLACE) logic for all HIGH-priority endpoints.
"""
import time
import duckdb
import pandas as pd
from datetime import date
from config import DB_PATH


def _con(db_path: str | None = None, retries: int = 10, delay: float = 2.0) -> duckdb.DuckDBPyConnection:
    """Open DuckDB connection with retry+backoff for transient file-lock errors."""
    path = db_path or str(DB_PATH)
    for attempt in range(retries):
        try:
            return duckdb.connect(path)
        except duckdb.IOException as e:
            if attempt < retries - 1:
                print(f"  ⚠ DB locked (attempt {attempt+1}/{retries}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise


# ── Helper: upsert via DELETE + INSERT ──────────────────────────────────────

def _upsert(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame,
            key_cols: list[str]):
    """Delete matching keys then insert — safe UPSERT for DuckDB."""
    if df.empty:
        return

    # Build WHERE clause from key columns
    key_vals = df[key_cols].drop_duplicates()
    for _, row in key_vals.iterrows():
        conditions = ' AND '.join(
            f"{k} = '{row[k]}'" for k in key_cols
        )
        con.execute(f"DELETE FROM {table} WHERE {conditions}")

    col_list = ', '.join(f'"{c}"' for c in df.columns)
    con.execute(f"INSERT INTO {table} ({col_list}) SELECT * FROM df")


# ── 2.1 Load Active Users ────────────────────────────────────────────────────

def load_active_users(data: list[dict], os: str, db_path: str | None = None):
    """
    Load active_users API response into fact.fact_active_users.

    Expected data item keys:
      app_id, date, country, dau, mau, wau (from ST /usage/active_users)
    """
    if not data:
        print("  ⚠ load_active_users: no data")
        return

    rows = []
    for item in data:
        base = {
            'app_id': item.get('app_id') or item.get('id', ''),
            'country': item.get('country', ''),
            'os': os,
        }
        for period, field in [('day', 'dau'), ('week', 'wau'), ('month', 'mau')]:
            val = item.get(field)
            if val is not None:
                rows.append({
                    **base,
                    'date': item.get('date', str(date.today())),
                    'time_period': period,
                    'active_users': int(val),
                })

    df = pd.DataFrame(rows)
    con = _con(db_path)
    _upsert(con, 'fact.fact_active_users', df,
            ['app_id', 'country', 'date', 'time_period', 'os'])
    con.close()
    print(f"  ✓ fact_active_users: {len(df)} rows loaded")


# ── 2.2 Load Retention ───────────────────────────────────────────────────────

def load_retention(data: list[dict], os: str, db_path: str | None = None):
    """
    Load retention API response into fact.fact_retention.

    Expected data item keys:
      app_id, date (cohort month), day_1, day_7, day_30, day_90, month_1, month_3
    """
    if not data:
        print("  ⚠ load_retention: no data")
        return

    rows = []
    for item in data:
        rows.append({
            'app_id': item.get('app_id') or item.get('id', ''),
            'date': item.get('date', str(date.today())),
            'retention_d1': item.get('day_1') or item.get('d1'),
            'retention_d7': item.get('day_7') or item.get('d7'),
            'retention_d30': item.get('day_30') or item.get('d30'),
            'retention_d90': item.get('day_90') or item.get('d90'),
            'retention_m1': item.get('month_1') or item.get('m1'),
            'retention_m3': item.get('month_3') or item.get('m3'),
            'os': os,
        })

    df = pd.DataFrame(rows)
    con = _con(db_path)
    _upsert(con, 'fact.fact_retention', df, ['app_id', 'date', 'os'])
    con.close()
    print(f"  ✓ fact_retention: {len(df)} rows loaded")


# ── 2.3 Load Store Summary ───────────────────────────────────────────────────

def load_store_summary(data: list[dict], os: str, date_granularity: str = 'monthly',
                       db_path: str | None = None):
    """
    Load store_summary API response into fact.fact_store_summary.

    Expected data item keys:
      category, country, date, revenue (cents), units (downloads)
    """
    if not data:
        print("  ⚠ load_store_summary: no data")
        return

    rows = []
    for item in data:
        units = item.get('units') or item.get('downloads') or sum(item.get(k, 0) for k in ['iu', 'au', 'u'] if item.get(k) is not None)
        rev = item.get('revenue') or item.get('revenue_cents') or sum(item.get(k, 0) for k in ['ir', 'ar', 'r'] if item.get(k) is not None)
        
        cat = str(item.get('category') or item.get('category_id') or item.get('ca', ''))
        cc = item.get('country') or item.get('cc', '')
        if not cat or not cc:
            continue
            
        rows.append({
            'category_id': cat,
            'country': cc,
            'date': item.get('date') or item.get('d', str(date.today())),
            'date_granularity': date_granularity,
            'revenue_cents': rev,
            'downloads': units,
            'os': os,
        })

    df = pd.DataFrame(rows)
    con = _con(db_path)
    _upsert(con, 'fact.fact_store_summary', df,
            ['category_id', 'country', 'date', 'date_granularity', 'os'])
    con.close()
    print(f"  ✓ fact_store_summary: {len(df)} rows loaded")


# ── 2.4 Load Games Breakdown (Genre Summary) ─────────────────────────────────

def load_genre_summary(data: list[dict], os: str, date_granularity: str = 'monthly',
                       db_path: str | None = None):
    """
    Load games_breakdown API response into fact.fact_genre_summary.

    Expected data item keys:
      category_id, category_name, country, date, revenue (cents), units, num_apps
    """
    if not data:
        print("  ⚠ load_genre_summary: no data")
        return

    rows = []
    for item in data:
        units = item.get('units') or item.get('downloads') or sum(item.get(k, 0) for k in ['iu', 'au', 'u'] if item.get(k) is not None)
        rev = item.get('revenue') or item.get('revenue_cents') or sum(item.get(k, 0) for k in ['ir', 'ar', 'r'] if item.get(k) is not None)

        cat = str(item.get('category_id') or item.get('category') or item.get('ca', ''))
        cc = item.get('country') or item.get('cc', '')
        if not cat or not cc:
            continue

        rows.append({
            'category_id': cat,
            'category_name': item.get('category_name', ''),
            'country': cc,
            'date': item.get('date') or item.get('d', str(date.today())),
            'date_granularity': date_granularity,
            'revenue_cents': rev,
            'downloads': units,
            'num_apps': item.get('num_apps') or item.get('na'),
            'os': os,
        })

    df = pd.DataFrame(rows)
    con = _con(db_path)
    _upsert(con, 'fact.fact_genre_summary', df,
            ['category_id', 'country', 'date', 'date_granularity', 'os'])
    con.close()
    print(f"  ✓ fact_genre_summary: {len(df)} rows loaded")


# ── 2.5 Load Sales Estimates ─────────────────────────────────────────────────

def load_sales_estimates(data: list[dict], os: str, date_granularity: str = 'monthly',
                         db_path: str | None = None):
    """
    Load sales_report_estimates API response into fact.fact_market_insights.

    Expected data item keys from /v1/{os}/sales_report_estimates:
      app_id (or id), country, date, revenue (int cents), units (int downloads)
    """
    if not data:
        print("  ⚠ load_sales_estimates: no data")
        return

    rows = []
    for item in data:
        units = item.get('units') or item.get('downloads') or sum(item.get(k, 0) for k in ['iu', 'au', 'u'] if item.get(k) is not None)
        rev = item.get('revenue') or item.get('revenue_cents') or sum(item.get(k, 0) for k in ['ir', 'ar', 'r'] if item.get(k) is not None)
        
        if rev is not None and not isinstance(rev, int):
            rev = int(float(rev) * 100)
            
        app_id_val = str(item.get('app_id') or item.get('id') or item.get('ai', ''))
        cc = item.get('country') or item.get('cc', '')
        if not app_id_val or not cc:
            continue

        rows.append({
            'app_id': app_id_val,
            'country': cc,
            'date': item.get('date') or item.get('d', str(date.today())),
            'date_granularity': date_granularity,
            'downloads': units,
            'revenue_cents': rev,
            'os': os,
            'source': 'sensortower',
        })

    df = pd.DataFrame(rows)
    con = _con(db_path)
    _upsert(con, 'fact.fact_market_insights', df,
            ['app_id', 'country', 'date', 'date_granularity', 'os'])
    con.close()
    print(f"  ✓ fact_market_insights: {len(df)} rows loaded")


# ── 2.6 Load App Metadata ────────────────────────────────────────────────────

def load_app_metadata(data: list[dict], os: str, country: str,
                      db_path: str | None = None):
    """
    Upsert app metadata into dim.dim_apps (from /v1/{os}/apps/[id] or search results).
    """
    if not data:
        return

    rows = []
    for item in data:
        rows.append({
            'app_id': str(item.get('id') or item.get('app_id', '')),
            'unified_app_id': item.get('unified_app_id'),
            'name': item.get('name'),
            'publisher_name': item.get('publisher_name'),
            'publisher_id': str(item.get('publisher_id', '') or ''),
            'os': os,
            'category_id': str(item.get('category_id', '') or ''),
            'release_date': item.get('release_date'),
            'icon_url': item.get('icon_url'),
            'price': item.get('price'),
            'rating': item.get('rating'),
            'ratings_count': item.get('ratings_count'),
            'country': country,
        })

    df = pd.DataFrame(rows)
    con = _con(db_path)
    _upsert(con, 'dim.dim_apps', df, ['app_id', 'country', 'os'])
    con.close()
    print(f"  ✓ dim_apps: {len(df)} rows loaded")


# ── Quick validation ─────────────────────────────────────────────────────────

def validate_tables(db_path: str | None = None):
    """Print row counts for all fact + analytics tables."""
    con = _con(db_path)
    tables = [
        ('fact', 'fact_market_insights'),
        ('fact', 'fact_active_users'),
        ('fact', 'fact_retention'),
        ('fact', 'fact_store_summary'),
        ('fact', 'fact_genre_summary'),
        ('analytics', 'benchmark_accuracy'),
        ('analytics', 'cohort_retention'),
        ('analytics', 'ltv_model'),
        ('analytics', 'genre_pnl_template'),
        ('analytics', 'revenue_forecast'),
    ]
    print("\n── Table Status ──────────────────────")
    for schema, table in tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        status = "✓" if cnt > 0 else "○"
        print(f"  {status} {schema}.{table}: {cnt:,} rows")
    con.close()


if __name__ == "__main__":
    validate_tables()
