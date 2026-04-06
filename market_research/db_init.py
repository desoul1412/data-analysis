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
            st_country_code     VARCHAR,           -- e.g. 'ID' (primary ST country, legacy)
            unified_app_id      VARCHAR,           -- manual ST mapping (nullable until mapped)
            genre               VARCHAR,           -- e.g. 'MMORPG', 'MOBA' — for IAP% lookup
            iap_pct_override    DOUBLE,            -- per-game IAP% override (NULL = use config default)
            benchmark_valid_to  DATE,              -- exclude benchmark rows after this date (NULL = no cutoff)
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
    # FACT TABLES (benchmark accuracy improvement)
    # ══════════════════════════════════════════

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_top_charts (
            app_id          VARCHAR NOT NULL,
            country         VARCHAR NOT NULL,
            date            DATE NOT NULL,
            os              VARCHAR NOT NULL,
            chart_type      VARCHAR NOT NULL,   -- 'free', 'paid', 'grossing'
            category_id     VARCHAR NOT NULL,
            rank            INTEGER,
            loaded_at       TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, country, date, os, chart_type, category_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact.fact_apple_public_charts (
            app_id          VARCHAR NOT NULL,
            country         VARCHAR NOT NULL,
            date            DATE NOT NULL,
            chart_type      VARCHAR NOT NULL,   -- 'grossing', 'free', 'paid'
            rank            INTEGER,
            name            VARCHAR,
            publisher_name  VARCHAR,
            loaded_at       TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(app_id, country, date, chart_type)
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
            country             VARCHAR,           -- company market name (e.g. 'Sing-Malay')
            month               DATE,
            st_estimate_usd     DOUBLE,
            actual_usd          DOUBLE,            -- IAP-only portion of company gross revenue
            variance_pct        DOUBLE,
            accuracy_tier       VARCHAR,           -- 'Accurate', 'Acceptable', 'Unreliable'
            iap_pct             DOUBLE,            -- IAP fraction applied (e.g. 0.35 for MMORPG/SGMY)
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

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.download_triangulation (
            unified_app_id      VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            month               DATE NOT NULL,
            os                  VARCHAR NOT NULL,
            st_revenue          DOUBLE,
            synth_revenue       DOUBLE,            -- downloads × retention_m1 × arpdau × 30
            actual_iap_usd      DOUBLE,
            st_vs_actual_pct    DOUBLE,
            synth_vs_actual_pct DOUBLE,
            best_estimate_usd   DOUBLE,            -- weighted avg of ST + synthetic
            confidence          VARCHAR,           -- 'HIGH', 'MEDIUM', 'LOW'
            computed_at         TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(unified_app_id, country, month, os)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.calibration_factors_v2 (
            genre                   VARCHAR NOT NULL,
            market                  VARCHAR NOT NULL,
            rank_bucket             VARCHAR NOT NULL,  -- 'top10', 'top50', 'top100', 'longtail'
            calibration_factor      DOUBLE,
            sample_size             INTEGER,
            confidence_interval_low  DOUBLE,
            confidence_interval_high DOUBLE,
            computed_at             TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(genre, market, rank_bucket)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.rpd_model (
            unified_app_id      VARCHAR NOT NULL,
            country             VARCHAR NOT NULL,
            os                  VARCHAR NOT NULL,
            month               DATE NOT NULL,
            downloads           BIGINT,
            revenue_usd         DOUBLE,
            rpd_usd             DOUBLE,            -- revenue per download
            cumulative_downloads BIGINT,
            cumulative_revenue_usd DOUBLE,
            ltv_rpd_usd         DOUBLE,            -- cumulative rev / cumulative dl (lifetime RPD proxy)
            arpu_usd            DOUBLE,            -- revenue / active_users (NULL if no AU data)
            computed_at         TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(unified_app_id, country, os, month)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.rpd_benchmark (
            product_code        VARCHAR,
            product_name        VARCHAR,
            country             VARCHAR,
            month               DATE,
            st_estimate_usd     DOUBLE,
            rpd_estimate_usd    DOUBLE,            -- downloads × genre-median RPD
            actual_iap_usd      DOUBLE,
            rpd_vs_actual_pct   DOUBLE,
            st_vs_actual_pct    DOUBLE,
            best_estimate_usd   DOUBLE,
            accuracy_tier       VARCHAR,
            computed_at         TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.composite_benchmark (
            product_code        VARCHAR,
            product_name        VARCHAR,
            country             VARCHAR,
            month               DATE,
            st_estimate_usd     DOUBLE,
            synth_estimate_usd  DOUBLE,
            rank_estimate_usd   DOUBLE,
            composite_mid_usd   DOUBLE,
            composite_low_usd   DOUBLE,
            composite_high_usd  DOUBLE,
            actual_iap_usd      DOUBLE,
            composite_variance_pct DOUBLE,
            accuracy_tier       VARCHAR,           -- 'Accurate', 'Acceptable', 'Unreliable'
            confidence_level    VARCHAR,           -- 'HIGH', 'MEDIUM', 'LOW'
            signals_used        VARCHAR,           -- e.g. 'st_revenue,rank,downloads'
            computed_at         TIMESTAMP DEFAULT current_timestamp
        )
    """)

    # Safe column migrations for existing databases
    for stmt in [
        "ALTER TABLE dim.dim_company_games ADD COLUMN IF NOT EXISTS genre VARCHAR",
        "ALTER TABLE dim.dim_company_games ADD COLUMN IF NOT EXISTS iap_pct_override DOUBLE",
        "ALTER TABLE dim.dim_company_games ADD COLUMN IF NOT EXISTS benchmark_valid_to DATE",
        "ALTER TABLE analytics.benchmark_accuracy ADD COLUMN IF NOT EXISTS iap_pct DOUBLE",
    ]:
        con.execute(stmt)

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
