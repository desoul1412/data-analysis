"""
Pipeline Runner — CLI orchestrator for the Market Research Pipeline.
Coordinates initialization, data import, ST API extraction, and analytics.

Usage:
    python pipeline_runner.py init             # Initialize database
    python pipeline_runner.py import           # Import company CSV data
    python pipeline_runner.py extract          # Run ST HIGH-priority extraction (gap-fill)
    python pipeline_runner.py extract-charts   # Fetch ST top grossing charts
    python pipeline_runner.py extract-apple    # Fetch Apple RSS public charts (no token needed)
    python pipeline_runner.py analytics        # Run derived analytics models (phases 3-6)
    python pipeline_runner.py analytics-v2     # Run benchmark accuracy improvement (tasks 1,4,5,6)
    python pipeline_runner.py status           # Show table row counts
    python pipeline_runner.py all              # Run everything
"""
import sys
import argparse
from datetime import date, timedelta
from config import DB_PATH, SEA_MARKETS, ST_AUTH_TOKEN


# ── Helpers ──────────────────────────────────────────────────────────────────

def _require_token():
    if not ST_AUTH_TOKEN:
        print("  ✗ SENSORTOWER_AUTH_TOKEN environment variable not set")
        print("    Set it with: $env:SENSORTOWER_AUTH_TOKEN='your_token'  (PowerShell)")
        sys.exit(1)


def _default_dates(months_back: int = 12) -> tuple[str, str]:
    end = date.today().replace(day=1) - timedelta(days=1)
    start = end.replace(month=1, day=1) if end.month > months_back else \
        (end - timedelta(days=30 * months_back)).replace(day=1)
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


# ── Phase 1: Initialize ───────────────────────────────────────────────────────

def cmd_init():
    print("\n=== P1: Database Initialization ===")
    from db_init import init_db
    con = init_db()
    # Summary
    for schema in ['dim', 'fact', 'analytics']:
        tables = con.execute(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{schema}' ORDER BY table_name
        """).fetchall()
        print(f"  {schema}/")
        for (t,) in tables:
            print(f"    └─ {t}")
    con.close()


# ── Phase 1: Import Company Data ─────────────────────────────────────────────

def cmd_import():
    print("\n=== P1: Company Data Import ===")
    from import_company_data import run_all
    run_all()


# ── Phase 1b: Import pre-pulled ST flat files ─────────────────────────────────

def cmd_import_st_files():
    print("\n=== P1b: Historical ST Files Import ===")
    print("  Covers 2019-01 to 2026-01 — skips API calls for this range")
    from import_st_files import run_all
    run_all()


# ── Phase 2: ST API Extraction (HIGH priority) ───────────────────────────────

def cmd_extract():
    print("\n=== P2: Sensor Tower API Extraction (gap-filling only) ===")
    _require_token()

    import duckdb
    from config import DB_PATH

    # Determine date gaps — only pull what's not already in market_insights
    con = duckdb.connect(str(DB_PATH))
    result = con.execute("""
        SELECT MAX(date) AS latest_date
        FROM fact.fact_market_insights
        WHERE source = 'sensortower_file'
    """).fetchone()
    con.close()

    latest_str = str(result[0]) if result and result[0] else None
    if latest_str:
        from datetime import datetime, date
        latest = datetime.strptime(latest_str[:10], '%Y-%m-%d').date()
        today = date.today().replace(day=1)
        if latest >= today:
            print(f"  ✓ historical files cover up to {latest_str} — no API extraction needed")
            return
        # Only pull data AFTER the flat file coverage
        start = latest_str[:8] + '01'
        end = today.strftime('%Y-%m-%d')
        print(f"  📅 Pulling gap only: {start} → {end}")
    else:
        start, end = _default_dates(months_back=12)
        print(f"  📅 No files found — pulling: {start} → {end}")

    import st_extract as st
    import st_load as loader

    ios_game_cat = ['6014']
    android_game_cat = ['GAME']

    print(f"  🌏 Markets: {SEA_MARKETS}")

    for os_type, categories in [('ios', ios_game_cat), ('android', android_game_cat)]:
        print(f"\n── {os_type.upper()} ──")
        for country in SEA_MARKETS:
            print(f"  Fetching store_summary [{country}]...")
            data = st.fetch_store_summary(
                categories=categories, os=os_type,
                date_granularity='monthly', start_date=start, end_date=end,
                countries=[country],
            )
            loader.load_store_summary(data, os=os_type)

            print(f"  Fetching games_breakdown [{country}]...")
            data = st.fetch_games_breakdown(
                categories=categories, os=os_type,
                date_granularity='monthly', start_date=start, end_date=end,
                countries=[country],
            )
            loader.load_genre_summary(data, os=os_type)

    # NEW: Fetch sales estimates for company games to keep benchmarks fresh
    con = duckdb.connect(str(DB_PATH))
    mapped_apps = con.execute("""
        SELECT DISTINCT unified_app_id
        FROM dim.dim_company_games
        WHERE unified_app_id IS NOT NULL 
          AND unified_app_id != ''
    """).fetchall()
    con.close()

    if mapped_apps:
        # Get just the string ids
        app_list = [row[0] for row in mapped_apps]
        print(f"\n── Fetching Gap Sales for {len(app_list)} Company Games ──")
        for os_type in ['ios', 'android']:
            for uid in app_list:
                print(f"  Gap sales/AU/retention for {uid} [{os_type}]...")
                
                sales_data = st.fetch_sales_estimates(
                    app_ids=[uid], os=os_type, 
                    start_date=start, end_date=end,
                    date_granularity='monthly',
                    countries=SEA_MARKETS
                )
                loader.load_sales_estimates(sales_data, os=os_type)

                au_data = st.fetch_active_users(
                    app_ids=[uid], os=os_type,
                    start_date=start, end_date=end,
                    time_period='month',
                    countries=SEA_MARKETS
                )
                loader.load_active_users(au_data, os=os_type)

                ret_data = st.fetch_retention(
                    app_ids=[uid], os=os_type,
                    start_date=start, end_date=end,
                    time_period='month',
                    countries=SEA_MARKETS
                )
                loader.load_retention(ret_data, os=os_type)

    print("\n  ✓ Gap-fill extraction complete")


# ── Extract: ST Top Charts ────────────────────────────────────────────────────

def cmd_extract_charts():
    print("\n=== Extract: ST Top Charts (grossing) ===")
    _require_token()
    import st_extract as st
    import st_load as loader

    ios_cat = '6014'
    android_cat = 'game'
    report_date = date.today().replace(day=1).strftime('%Y-%m-%d')
    all_countries = SEA_MARKETS + ['TW', 'HK']

    # Chart type is OS-specific
    os_chart_type = {'ios': 'topgrossingapplications', 'android': 'topgrossing'}

    for os_type, category in [('ios', ios_cat), ('android', android_cat)]:
        ct = os_chart_type[os_type]
        for country in all_countries:
            print(f"  Fetching top_charts [{os_type}, {country}]...")
            data = st.fetch_top_charts(
                os=os_type, category=category, chart_type=ct,
                country=country, date=report_date, limit=100,
            )
            loader.load_top_charts(
                data, os=os_type, country=country, date=report_date,
                chart_type=ct, category_id=category,
            )

    print("\n  ✓ Top charts extraction complete")


# ── Extract: Apple Public RSS Charts ─────────────────────────────────────────

def cmd_extract_apple():
    print("\n=== Extract: Apple Public RSS Charts ===")
    from fetch_apple_charts import load_apple_charts
    load_apple_charts()


# ── Genre Auto-Assignment ────────────────────────────────────────────────────

def cmd_genre_assign():
    print("\n=== Genre Auto-Assignment ===")
    from analytics import auto_assign_genres
    auto_assign_genres()


# ── Analytics v2 (benchmark accuracy improvement) ────────────────────────────

def cmd_analytics_v2():
    print("\n=== Analytics v2: Benchmark Accuracy Improvement ===")
    from analytics import (
        validate_mappings_via_apple_charts,
        auto_assign_genres,
        investigate_ys_singmalay,
        investigate_high_variance_genres,
        compute_iap_sensitivity,
        compute_rpd,
        compute_rpd_benchmark,
        compute_download_triangulation,
        get_calibration_factors_v2,
        compute_composite_benchmark,
    )
    print("── T16: Genre Auto-Assignment ──")
    auto_assign_genres()

    print("── T15: Apple Chart Mapping Validator ──")
    validate_mappings_via_apple_charts()

    print("── Task 1: IAP% Sensitivity ──")
    compute_iap_sensitivity()

    print("── RPD Model ──")
    compute_rpd()
    compute_rpd_benchmark()

    print("── T12: YS Sing-Malay Cutoff ──")
    investigate_ys_singmalay()

    print("── T13: High-Variance Game Investigation ──")
    investigate_high_variance_genres()

    print("── Task 4: Download Triangulation ──")
    compute_download_triangulation()

    print("── Task 5: Enhanced Calibration ──")
    get_calibration_factors_v2()

    print("── Task 6: Composite Benchmark ──")
    compute_composite_benchmark()

    print("\n✓ Analytics v2 completed.")


# ── Phase 3-6: Analytics ─────────────────────────────────────────────────────

def cmd_analytics():
    print("\n=== P3–6: Analytics Models ===")
    from analytics import run_all_analytics
    report_month = date.today().replace(day=1).strftime('%Y-%m-01')
    run_all_analytics(report_month=report_month)


# ── Status ────────────────────────────────────────────────────────────────────

def cmd_status():
    print("\n=== Pipeline Status ===")
    import duckdb
    con = duckdb.connect(str(DB_PATH))

    all_tables = con.execute("""
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_schema IN ('dim','fact','analytics')
        ORDER BY table_schema, table_name
    """).fetchall()

    current_schema = None
    for schema, table in all_tables:
        if schema != current_schema:
            print(f"\n  {schema}/")
            current_schema = schema
        cnt = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        icon = "✓" if cnt > 0 else "○"
        print(f"    {icon} {table}: {cnt:,} rows")

    print(f"\n  DB: {DB_PATH}")
    print(f"  ST Token: {'✓ set' if ST_AUTH_TOKEN else '✗ NOT SET'}")
    con.close()


# ── Full pipeline ─────────────────────────────────────────────────────────────

def cmd_all():
    cmd_init()
    cmd_import()
    cmd_import_st_files()   # load historical flat files before any API calls
    cmd_extract()
    cmd_extract_charts()
    cmd_extract_apple()
    cmd_analytics()
    cmd_analytics_v2()
    cmd_status()


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Market Research Pipeline Runner")
    parser.add_argument('command', choices=[
                            'init', 'import', 'st-files', 'extract',
                            'extract-charts', 'extract-apple',
                            'analytics', 'analytics-v2', 'genre-assign',
                            'status', 'all',
                        ],
                        help="Command to run")
    args = parser.parse_args()

    dispatch = {
        'init': cmd_init,
        'import': cmd_import,
        'st-files': cmd_import_st_files,
        'extract': cmd_extract,
        'extract-charts': cmd_extract_charts,
        'extract-apple': cmd_extract_apple,
        'analytics': cmd_analytics,
        'analytics-v2': cmd_analytics_v2,
        'genre-assign': cmd_genre_assign,
        'status': cmd_status,
        'all': cmd_all,
    }
    dispatch[args.command]()


if __name__ == "__main__":
    main()
