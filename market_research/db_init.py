"""
Database initialization — creates schemas and tables in DuckDB.
Run once to set up, safe to re-run (uses IF NOT EXISTS).
"""
import duckdb
from config import DB_PATH, SCHEMA_RAW, SCHEMA_DIM, SCHEMA_FACT, SCHEMA_ANALYTICS


def init_db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Create or connect to the DuckDB database and initialize all schemas + tables."""
    path = db_path or str(DB_PATH)
    con = duckdb.connect(path)

    # --- Schemas ---
    for schema in [SCHEMA_RAW, SCHEMA_DIM, SCHEMA_FACT, SCHEMA_ANALYTICS]:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # ══════════════════════════════════════════
    # DIMENSION TABLES
    # ══════════════════════════════════════════

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim.dim_apps (
            app_id              VARCHAR NOT NULL,
            unified_app_id      VARCHAR,
            name                VARCHAR,
            publisher_name      VARCHAR,
            publisher_id        VARCHAR,
            os                  VARCHAR,          -- 'ios' or 'android'
            category_id         VARCHAR,
            release_date        DATE,
            icon_url            VARCHAR,
            price               DOUBLE,
            rating              DOUBLE,
            ratings_count       BIGINT,
            country             VARCHAR,          -- country queried
            last_updated        TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (app_id, country, os)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim.dim_company_games (
            id                  INTEGER PRIMARY KEY,
            product_code        VARCHAR NOT NULL,  -- e.g. 'C06 : Ballistic Hero-ID'
            product_name        VARCHAR,           -- parsed: 'Ballistic Hero'
            market              VARCHAR,           -- e.g. 'Indonesia'
            st_country_code     VARCHAR,           -- e.g. 'ID'
            unified_app_id      VARCHAR,           -- manual ST mapping (nullable until mapped)
            UNIQUE(product_code, market)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim.dim_genre_taxonomy (
            id                  INTEGER PRIMARY KEY,
            unified_id          VARCHAR NOT NULL,
            unified_name        VARCHAR,
            class_from          VARCHAR,
            class_to            VARCHAR,
            genre_from          VARCHAR,
            genre_to            VARCHAR,
            subgenre_from       VARCHAR,
            subgenre_to         VARCHAR,
            changelog_source    VARCHAR,           -- e.g. '2026_Feb'
            UNIQUE(unified_id, changelog_source)
        )
    """)

    # ══════════════════════════════════════════
    # FACT TABLES (Phase 1 — core pipeline)
    # ══════════════════════════════════════════

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_market_insights (
            app_id              VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            date                DATE NOT NULL,
            date_granularity    VARCHAR,           -- 'daily', 'monthly', etc.
            downloads           BIGINT,
            revenue_cents       BIGINT,
            os                  VARCHAR,
            source              VARCHAR DEFAULT 'sensortower',
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, country, date, date_granularity, os)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_performance_daily (
            app_id              VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            date                DATE NOT NULL,
            downloads           BIGINT,
            revenue_cents       BIGINT,
            os                  VARCHAR,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, country, date, os)
        )
    """)

    # ══════════════════════════════════════════
    # FACT TABLES (Phase 2 — engagement + TAM)
    # ══════════════════════════════════════════

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_active_users (
            app_id              VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            date                DATE NOT NULL,
            time_period         VARCHAR NOT NULL,  -- 'day', 'week', 'month'
            active_users        BIGINT,
            os                  VARCHAR,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, country, date, time_period, os)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_retention (
            app_id              VARCHAR NOT NULL,
            date                DATE NOT NULL,      -- cohort install month
            retention_d1        DOUBLE,
            retention_d7        DOUBLE,
            retention_d30       DOUBLE,
            retention_d90       DOUBLE,
            retention_m1        DOUBLE,
            retention_m3        DOUBLE,
            os                  VARCHAR,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, date, os)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_store_summary (
            category_id         VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            date                DATE NOT NULL,
            date_granularity    VARCHAR,
            revenue_cents       BIGINT,
            downloads           BIGINT,
            os                  VARCHAR,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(category_id, country, date, date_granularity, os)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_genre_summary (
            category_id         VARCHAR NOT NULL,
            category_name       VARCHAR,
            country             VARCHAR NOT NULL,
            date                DATE NOT NULL,
            date_granularity    VARCHAR,
            revenue_cents       BIGINT,
            downloads           BIGINT,
            num_apps            INTEGER,
            os                  VARCHAR,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(category_id, country, date, date_granularity, os)
        )
    """)

    # ══════════════════════════════════════════
    # COMPANY DATA (revenue actuals)
    # ══════════════════════════════════════════

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_company_revenue (
            product_code        VARCHAR NOT NULL,
            market              VARCHAR NOT NULL,
            ob_date             DATE,
            grand_total         DOUBLE,
            m0  DOUBLE, m1  DOUBLE, m2  DOUBLE, m3  DOUBLE,
            m4  DOUBLE, m5  DOUBLE, m6  DOUBLE, m7  DOUBLE,
            m8  DOUBLE, m9  DOUBLE, m10 DOUBLE, m11 DOUBLE,
            m12 DOUBLE, m13 DOUBLE, m14 DOUBLE, m15 DOUBLE,
            m16 DOUBLE, m17 DOUBLE, m18 DOUBLE, m19 DOUBLE,
            m20 DOUBLE, m21 DOUBLE, m22 DOUBLE, m23 DOUBLE,
            m24 DOUBLE, m25 DOUBLE, m26 DOUBLE, m27 DOUBLE,
            m28 DOUBLE, m29 DOUBLE, m30 DOUBLE, m31 DOUBLE,
            m32 DOUBLE, m33 DOUBLE, m34 DOUBLE, m35 DOUBLE,
            m36 DOUBLE,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(product_code, market, ob_date)  -- multiple monthly snapshots per game+market
        )
    """)

    # ══════════════════════════════════════════
    # FACT TABLES (Phase 7 — medium-priority)
    # ══════════════════════════════════════════

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_ratings (
            app_id              VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            date                DATE NOT NULL,
            rating_avg          DOUBLE,
            ratings_count       BIGINT,
            stars_1             BIGINT,
            stars_2             BIGINT,
            stars_3             BIGINT,
            stars_4             BIGINT,
            stars_5             BIGINT,
            os                  VARCHAR,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, country, date, os)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_ad_intel (
            app_id              VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            date                DATE NOT NULL,
            network             VARCHAR,           -- 'facebook', 'admob', etc.
            ad_count            INTEGER,
            impression_share    DOUBLE,
            os                  VARCHAR,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, country, date, network, os)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_aso_keywords (
            app_id              VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            date                DATE NOT NULL,
            keyword             VARCHAR NOT NULL,
            rank                INTEGER,
            traffic_score       DOUBLE,
            difficulty          DOUBLE,
            os                  VARCHAR,
            loaded_at           TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, country, date, keyword, os)
        )
    """)

    # ══════════════════════════════════════════
    # ANALYTICS (derived tables — Phase 3-6)
    # ══════════════════════════════════════════

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.benchmark_accuracy (
            product_code        VARCHAR,
            product_name        VARCHAR,
            country             VARCHAR,
            month               DATE,
            st_estimate_usd     DOUBLE,
            actual_usd          DOUBLE,
            variance_pct        DOUBLE,
            accuracy_tier       VARCHAR,           -- '✅ Accurate', '⚠️ Acceptable', '❌ Unreliable'
            computed_at         TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.cohort_retention (
            genre               VARCHAR NOT NULL,
            subgenre            VARCHAR,
            country             VARCHAR NOT NULL,
            os                  VARCHAR NOT NULL,
            cohort_month        DATE NOT NULL,     -- install month (M0)
            period              INTEGER NOT NULL,  -- lifecycle month (0..12)
            retention_pct       DOUBLE,            -- % of M0 cohort still active
            cohort_size         INTEGER,           -- number of apps in cohort
            computed_at         TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(genre, country, os, cohort_month, period)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.ltv_model (
            genre               VARCHAR NOT NULL,
            subgenre            VARCHAR,
            country             VARCHAR NOT NULL,
            os                  VARCHAR NOT NULL,
            period              INTEGER NOT NULL,  -- month n (0..36)
            arpdau              DOUBLE,            -- avg revenue per DAU
            retention_pct       DOUBLE,
            ltv_usd             DOUBLE,            -- cumulative LTV at period n
            computed_at         TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(genre, country, os, period)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.genre_pnl_template (
            genre               VARCHAR NOT NULL,
            subgenre            VARCHAR,
            country             VARCHAR NOT NULL,
            os                  VARCHAR,
            report_month        DATE NOT NULL,
            tam_usd             DOUBLE,            -- total addressable market
            genre_revenue_share DOUBLE,            -- genre % of category TAM
            genre_tam_usd       DOUBLE,            -- = tam * genre_revenue_share
            top10_revenue_usd   DOUBLE,
            top10_dau           BIGINT,
            avg_rating          DOUBLE,
            num_advertisers     INTEGER,
            hhi_score           DOUBLE,            -- Herfindahl concentration index
            arpdau              DOUBLE,
            d1_retention        DOUBLE,
            d30_retention       DOUBLE,
            ltv_30              DOUBLE,
            ltv_90              DOUBLE,
            opportunity_score   VARCHAR,           -- 'HIGH', 'MEDIUM', 'LOW'
            computed_at         TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(genre, country, os, report_month)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.revenue_forecast (
            genre               VARCHAR NOT NULL,
            subgenre            VARCHAR,
            country             VARCHAR NOT NULL,
            os                  VARCHAR,
            forecast_month      DATE NOT NULL,     -- calendar month being forecast
            period              INTEGER NOT NULL,  -- months from launch (M0=0)
            revenue_mid_usd     DOUBLE,            -- central forecast
            revenue_low_usd     DOUBLE,            -- lower bound
            revenue_high_usd    DOUBLE,            -- upper bound
            calibration_factor  DOUBLE,
            computed_at         TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(genre, country, os, forecast_month, period)
        )
    """)

    print(f"✓ Database initialized at: {path}")
    return con


if __name__ == "__main__":
    con = init_db()
    # Print summary
    for schema in [SCHEMA_DIM, SCHEMA_FACT, SCHEMA_ANALYTICS]:
        tables = con.execute(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{schema}'
            ORDER BY table_name
        """).fetchall()
        print(f"  {schema}/")
        for (t,) in tables:
            print(f"    └─ {t}")
    con.close()
