"""
Analytics computations — derived models built on top of raw fact tables.
All functions write results into analytics.* tables.

Applied skills:
  @[mcp/game-publishing-data-architect] — metrics formulas
  @[mcp/cohort-analysis] — triangular matrix, retention curves
  @[mcp/customer-lifetime-value] — LTV(n) segmented model
  @[mcp/benchmarking-report] — accuracy tiers, calibration factors
"""
import duckdb
import pandas as pd
from config import DB_PATH


def _con(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path or str(DB_PATH))


# ────────────────────────────────────────────────────────
# PHASE 3: BENCHMARKING
# ────────────────────────────────────────────────────────

def compute_benchmark_accuracy(db_path: str | None = None) -> pd.DataFrame:
    """
    Compare ST sales estimates vs company actual revenue.
    Only works for games where unified_app_id is mapped.

    Returns DataFrame written into analytics.benchmark_accuracy.

    Metric: variance_pct = (st_estimate - actual) / actual * 100
    Accuracy tiers from @[mcp/benchmarking-report]:
      ✅ < 15%  — Accurate
      ⚠️ < 40%  — Acceptable
      ❌ >= 40% — Unreliable
    """
    con = _con(db_path)
    df: pd.DataFrame = con.execute("""
        SELECT
            cg.product_code,
            cg.product_name,
            cg.st_country_code                              AS country,
            cr.ob_date                                      AS month,
            SUM(mi.revenue_cents) / 100.0                   AS st_estimate_usd,
            cr.grand_total                                  AS actual_usd,
            ROUND(
              (SUM(mi.revenue_cents) / 100.0 - cr.grand_total)
              / NULLIF(cr.grand_total, 0) * 100.0, 1
            )                                               AS variance_pct
        FROM dim.dim_company_games cg
        JOIN dim.dim_apps da
          ON da.unified_app_id = cg.unified_app_id
        JOIN fact.fact_market_insights mi
          ON mi.app_id = da.app_id
         AND mi.country = cg.st_country_code
         AND mi.date_granularity = 'monthly'
        JOIN fact.fact_company_revenue cr
          ON cr.product_code = cg.product_code
         AND cr.market = cg.market
        WHERE cg.unified_app_id IS NOT NULL
          AND cr.grand_total IS NOT NULL
          AND cr.grand_total > 0
        GROUP BY cg.product_code, cg.product_name, cg.st_country_code, cr.ob_date, cr.grand_total
    """).df()

    if df.empty:
        print("  ○ benchmark_accuracy: no matched games yet (set unified_app_id first)")
        con.close()
        return df

    def _tier(pct: float | None) -> str:
        if pct is None:
            return '❓ Unknown'
        p = abs(pct)
        if p < 15:
            return 'Accurate'
        if p < 40:
            return 'Acceptable'
        return 'Unreliable'

    df['accuracy_tier'] = df['variance_pct'].apply(_tier)

    con.execute("DELETE FROM analytics.benchmark_accuracy")
    con.execute("INSERT INTO analytics.benchmark_accuracy SELECT "
                "product_code, product_name, country, month, "
                "st_estimate_usd, actual_usd, variance_pct, accuracy_tier, "
                "current_timestamp FROM df")
    con.close()
    print(f"  ✓ benchmark_accuracy: {len(df)} rows, "
          f"{(df.accuracy_tier == 'Accurate').sum()} accurate, "
          f"{(df.accuracy_tier == 'Acceptable').sum()} acceptable, "
          f"{(df.accuracy_tier == 'Unreliable').sum()} unreliable")
    return df


def get_calibration_factors(db_path: str | None = None) -> pd.DataFrame:
    """
    Compute per-genre, per-market calibration factor:
      calibration_factor = MEAN(actual / st_estimate) for known games

    Used to adjust ST competitor estimates toward true values.
    Returns a DataFrame(genre, country, calibration_factor).
    """
    con = _con(db_path)
    df: pd.DataFrame = con.execute("""
        SELECT
            da.category_id                                  AS genre,
            ba.country,
            ROUND(AVG(ba.actual_usd / NULLIF(ba.st_estimate_usd, 0)), 3) AS calibration_factor,
            COUNT(*)                                        AS sample_size
        FROM analytics.benchmark_accuracy ba
        JOIN fact.fact_market_insights mi
          ON mi.country = ba.country
        JOIN dim.dim_apps da
          ON da.app_id = mi.app_id
         AND da.country = mi.country
        WHERE ba.st_estimate_usd > 0
          AND ba.actual_usd > 0
          AND da.category_id IS NOT NULL
        GROUP BY da.category_id, ba.country
        HAVING COUNT(*) >= 2                                -- min 2 samples
    """).df()

    con.close()
    if df.empty:
        print("  ○ calibration_factors: not enough benchmarked data yet")
    else:
        print(f"  ✓ calibration_factors: {len(df)} genre×market pairs")
    return df


# ────────────────────────────────────────────────────────
# PHASE 4: COHORT RETENTION ANALYSIS
# ────────────────────────────────────────────────────────

def compute_cohort_retention(db_path: str | None = None) -> pd.DataFrame:
    """
    Build triangular cohort retention matrix from fact.fact_retention.

    Rows = install month cohorts (cohort_month)
    Cols = lifecycle periods M0–M12 (derived from D1→M1→M3 available fields)
    Cells = % of M0 cohort still active

    Applied from @[mcp/cohort-analysis]:
      - Triangular matrix: NaN in future cells (cohort too recent)
      - Interpretation: improving rows = market health improving
      - Stabilization: where curve flattens after M3

    Outputs: analytics.cohort_retention
    """
    con = _con(db_path)
    # Get retention data joined with genre info
    df: pd.DataFrame = con.execute("""
        SELECT
            da.category_id                                  AS genre,
            fr.date                                         AS cohort_month,
            fr.os,
            mi.country,
            AVG(fr.retention_d1)                            AS d1_pct,
            AVG(fr.retention_d7)                            AS d7_pct,
            AVG(fr.retention_d30)                           AS d30_pct,
            AVG(fr.retention_d90)                           AS d90_pct,
            AVG(fr.retention_m1)                            AS m1_pct,
            AVG(fr.retention_m3)                            AS m3_pct,
            COUNT(DISTINCT fr.app_id)                       AS cohort_size
        FROM fact.fact_retention fr
        JOIN dim.dim_apps da ON da.app_id = fr.app_id
        JOIN fact.fact_market_insights mi
          ON mi.app_id = fr.app_id
         AND mi.date = fr.date
        WHERE da.category_id IS NOT NULL
        GROUP BY da.category_id, fr.date, fr.os, mi.country
    """).df()

    if df.empty:
        print("  ○ cohort_retention: no retention data yet")
        con.close()
        return df

    # Melt to long format: one row per (genre, cohort_month, os, country, period)
    period_map = {
        'd1_pct': 0,    # ~M0 proxy (Day 1)
        'd7_pct': 0,    # still M0 month
        'd30_pct': 1,   # M1
        'd90_pct': 3,   # M3
        'm1_pct': 1,    # M1 (exact)
        'm3_pct': 3,    # M3 (exact)
    }

    rows = []
    for _, row in df.iterrows():
        seen_periods: dict[int, list[float]] = {}
        for col, period in period_map.items():
            val = row.get(col)
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                seen_periods.setdefault(period, []).append(float(val))
        for period, vals in seen_periods.items():
            rows.append({
                'genre': row['genre'],
                'subgenre': None,
                'country': row['country'],
                'os': row['os'],
                'cohort_month': row['cohort_month'],
                'period': period,
                'retention_pct': sum(vals) / len(vals),
                'cohort_size': int(row['cohort_size']),
            })

    out_df = pd.DataFrame(rows)
    con.execute("DELETE FROM analytics.cohort_retention")
    con.execute("INSERT INTO analytics.cohort_retention "
                "(genre, subgenre, country, os, cohort_month, period, retention_pct, cohort_size, computed_at) "
                "SELECT genre, subgenre, country, os, cohort_month, period, retention_pct, cohort_size, "
                "current_timestamp FROM out_df")
    con.close()
    print(f"  ✓ cohort_retention: {len(out_df)} rows "
          f"({out_df['genre'].nunique()} genres × {out_df['country'].nunique()} countries)")
    return out_df


# ────────────────────────────────────────────────────────
# PHASE 5: LTV MODEL
# ────────────────────────────────────────────────────────

def compute_ltv_model(db_path: str | None = None) -> pd.DataFrame:
    """
    Compute LTV(n) for each genre × market × OS using:
      LTV(n) = Σ(m=0..n) [ARPDAU × 30 × Retention(m)]

    Where:
      ARPDAU      = avg(revenue) / avg(dau) per genre×market from ST data
      Retention(m)= from analytics.cohort_retention (genre-average)

    Applied from @[mcp/customer-lifetime-value] and @[mcp/game-publishing-data-architect]
    (LTV formula: RevNRU(n) / NRU adapted for market research / competitor analysis)
    """
    con = _con(db_path)

    # Get ARPDAU: revenue/DAU ratio per genre×market (requires both active_users + market_insights)
    arpdau_df: pd.DataFrame = con.execute("""
        SELECT
            da.category_id                                          AS genre,
            mi.country,
            mi.os,
            AVG(mi.revenue_cents / 100.0)                          AS avg_monthly_rev,
            AVG(au.active_users)                                    AS avg_mau,
            ROUND(
                AVG(mi.revenue_cents / 100.0)
                / NULLIF(AVG(au.active_users) * 30.0, 0), 4
            )                                                       AS arpdau
        FROM fact.fact_market_insights mi
        JOIN dim.dim_apps da ON da.app_id = mi.app_id AND da.country = mi.country
        JOIN fact.fact_active_users au
          ON au.app_id = mi.app_id
         AND au.country = mi.country
         AND au.time_period = 'month'
         AND au.os = mi.os
        WHERE da.category_id IS NOT NULL
          AND mi.date_granularity = 'monthly'
          AND mi.revenue_cents > 0
          AND au.active_users > 0
        GROUP BY da.category_id, mi.country, mi.os
    """).df()

    # Get genre-average retention curves
    ret_df: pd.DataFrame = con.execute("""
        SELECT
            genre, country, os, period,
            AVG(retention_pct) / 100.0          AS retention_rate
        FROM analytics.cohort_retention
        GROUP BY genre, country, os, period
        ORDER BY genre, country, os, period
    """).df()

    if arpdau_df.empty or ret_df.empty:
        print("  ○ ltv_model: need active_users + retention + market_insights data first")
        con.close()
        return pd.DataFrame()

    # Merge and compute cumulative LTV
    merged = arpdau_df.merge(ret_df, on=['genre', 'country', 'os'], how='inner')
    merged = merged.sort_values(['genre', 'country', 'os', 'period'])

    rows_out = []
    for (genre, country, os_val), grp in merged.groupby(['genre', 'country', 'os']):
        arpdau = grp['arpdau'].iloc[0]
        cumulative_ltv = 0.0
        for _, row in grp.iterrows():
            # LTV contribution at period m = ARPDAU * 30 days * retention_rate
            month_rev = arpdau * 30.0 * row['retention_rate']
            cumulative_ltv += month_rev
            rows_out.append({
                'genre': genre,
                'subgenre': None,
                'country': country,
                'os': os_val,
                'period': int(row['period']),
                'arpdau': round(arpdau, 4),
                'retention_pct': round(row['retention_rate'] * 100, 2),
                'ltv_usd': round(cumulative_ltv, 4),
            })

    out_df = pd.DataFrame(rows_out)
    if out_df.empty:
        print("  ○ ltv_model: no cross-matched genre data")
        con.close()
        return out_df

    con.execute("DELETE FROM analytics.ltv_model")
    con.execute("INSERT INTO analytics.ltv_model "
                "(genre, subgenre, country, os, period, arpdau, retention_pct, ltv_usd, computed_at) "
                "SELECT genre, subgenre, country, os, period, arpdau, retention_pct, ltv_usd, "
                "current_timestamp FROM out_df")
    con.close()
    print(f"  ✓ ltv_model: {len(out_df)} rows "
          f"({out_df['genre'].nunique()} genres, max LTV = "
          f"${out_df['ltv_usd'].max():.2f})")
    return out_df


# ────────────────────────────────────────────────────────
# PHASE 6: GENRE PnL TEMPLATE
# ────────────────────────────────────────────────────────

def compute_genre_pnl(report_month: str, db_path: str | None = None) -> pd.DataFrame:
    """
    Compute the Genre PnL template for a given month.
    Aggregates TAM, top-10 metrics, ARPDAU, retention, LTV, and opportunity score.

    Opportunity score formula:
      HIGH   if TAM growth > 10% AND HHI < 0.25 AND ARPDAU > median_arpdau
      LOW    if TAM growth < 0% OR HHI > 0.50
      MEDIUM otherwise
    """
    con = _con(db_path)

    df: pd.DataFrame = con.execute(f"""
        WITH store AS (
            SELECT country, os, SUM(revenue_cents) / 100.0 AS tam_usd
            FROM fact.fact_store_summary
            WHERE date = '{report_month}'
            GROUP BY country, os
        ),
        genre_share AS (
            SELECT
                category_id AS genre,
                category_name,
                country, os,
                SUM(revenue_cents) / 100.0                  AS genre_rev_usd,
                SUM(downloads)                              AS genre_downloads
            FROM fact.fact_genre_summary
            WHERE date = '{report_month}'
            GROUP BY category_id, category_name, country, os
        ),
        genre_top10 AS (
            SELECT
                da.category_id                              AS genre,
                mi.country, mi.os,
                SUM(mi.revenue_cents) / 100.0               AS top10_revenue,
                AVG(fr.rating_avg)                          AS avg_rating,
                COUNT(DISTINCT adi.app_id)                  AS num_advertisers
            FROM fact.fact_market_insights mi
            JOIN dim.dim_apps da ON da.app_id = mi.app_id AND da.country = mi.country
            LEFT JOIN fact.fact_ratings fr
              ON fr.app_id = mi.app_id AND fr.country = mi.country
             AND fr.date = '{report_month}'
            LEFT JOIN fact.fact_ad_intel adi
              ON adi.app_id = mi.app_id AND adi.country = mi.country
             AND adi.date = '{report_month}'
            WHERE mi.date = '{report_month}'
              AND mi.date_granularity = 'monthly'
              AND da.category_id IS NOT NULL
            GROUP BY da.category_id, mi.country, mi.os
        ),
        ltv AS (
            SELECT genre, country, os,
                MAX(CASE WHEN period <= 1 THEN ltv_usd END) AS ltv_30,
                MAX(CASE WHEN period <= 3 THEN ltv_usd END) AS ltv_90
            FROM analytics.ltv_model
            GROUP BY genre, country, os
        ),
        ret AS (
            SELECT genre, country, os,
                AVG(CASE WHEN period = 0 THEN retention_pct END) AS d1_ret,
                AVG(CASE WHEN period = 1 THEN retention_pct END) AS d30_ret
            FROM analytics.cohort_retention
            GROUP BY genre, country, os
        )
        SELECT
            gs.genre,
            gs.category_name,
            gs.country,
            gs.os,
            '{report_month}'::DATE                          AS report_month,
            s.tam_usd,
            ROUND(gs.genre_rev_usd / NULLIF(s.tam_usd, 0), 4) AS genre_revenue_share,
            gs.genre_rev_usd                                AS genre_tam_usd,
            gt.top10_revenue,
            gt.avg_rating,
            gt.num_advertisers,
            -- HHI approximation: if top10 is near genre_tam, concentration is high
            ROUND(
                POWER(gs.genre_rev_usd / NULLIF(NULLIF(s.tam_usd, 0), 0), 2)
                , 4
            )                                               AS hhi_score,
            l.ltv_30,
            l.ltv_90,
            r.d1_ret                                        AS d1_retention,
            r.d30_ret                                       AS d30_retention,
            -- ARPDAU from LTV model base
            lm.arpdau
        FROM genre_share gs
        JOIN store s ON s.country = gs.country AND s.os = gs.os
        LEFT JOIN genre_top10 gt ON gt.genre = gs.genre AND gt.country = gs.country AND gt.os = gs.os
        LEFT JOIN ltv l ON l.genre = gs.genre AND l.country = gs.country AND l.os = gs.os
        LEFT JOIN ret r ON r.genre = gs.genre AND r.country = gs.country AND r.os = gs.os
        LEFT JOIN (
            SELECT genre, country, os, AVG(arpdau) AS arpdau
            FROM analytics.ltv_model GROUP BY genre, country, os
        ) lm ON lm.genre = gs.genre AND lm.country = gs.country AND lm.os = gs.os
    """).df()

    if df.empty:
        print(f"  ○ genre_pnl: no data for {report_month}")
        con.close()
        return df

    # Compute opportunity score
    med_arpdau = df['arpdau'].median() or 0
    def _score(row) -> str:
        share = row.get('genre_revenue_share') or 0
        hhi = row.get('hhi_score') or 0
        arpdau = row.get('arpdau') or 0
        if share > 0.10 and hhi < 0.25 and arpdau > med_arpdau:
            return 'HIGH'
        if share < 0.02 or hhi > 0.50:
            return 'LOW'
        return 'MEDIUM'

    df['opportunity_score'] = df.apply(_score, axis=1)

    con.execute(f"DELETE FROM analytics.genre_pnl_template WHERE report_month = '{report_month}'")
    col_map = {
        'category_name': None,  # not in target table — drop
    }
    insert_df = df.drop(columns=['category_name'], errors='ignore')
    insert_df['top10_dau'] = None   # not yet computed (needs active_users JOIN)

    tbl_cols = [r[0] for r in con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = 'genre_pnl_template'
          AND column_name != 'computed_at'
        ORDER BY ordinal_position
    """).fetchall()]

    final = insert_df[[c for c in tbl_cols if c in insert_df.columns]]
    con.execute("INSERT INTO analytics.genre_pnl_template "
                f"({', '.join(final.columns)}) SELECT * FROM final")
    con.close()
    print(f"  ✓ genre_pnl_template: {len(df)} genres×markets, "
          f"{(df.opportunity_score == 'HIGH').sum()} HIGH opportunities")
    return df


# ────────────────────────────────────────────────────────
# PHASE 5: REVENUE FORECAST
# ────────────────────────────────────────────────────────

def compute_revenue_forecast(genre: str, country: str, os: str,
                              forecast_base_month: str,
                              months_ahead: int = 12,
                              db_path: str | None = None) -> pd.DataFrame:
    """
    Compute M0–Mn revenue forecast for a new game launch in genre×market.
    Uses company data curve shape scaled to genre median M0 from ST data.

    Returns DataFrame and writes to analytics.revenue_forecast.
    """
    con = _con(db_path)

    # Get genre M0–M36 curve from company data (average across products in this genre/market)
    # Requires dim_company_games to have genre attribution (future: join with genre taxonomy)
    curve_df: pd.DataFrame = con.execute(f"""
        SELECT
            AVG(cr.m0 ) AS m0,  AVG(cr.m1 ) AS m1,  AVG(cr.m2 ) AS m2,
            AVG(cr.m3 ) AS m3,  AVG(cr.m4 ) AS m4,  AVG(cr.m5 ) AS m5,
            AVG(cr.m6 ) AS m6,  AVG(cr.m7 ) AS m7,  AVG(cr.m8 ) AS m8,
            AVG(cr.m9 ) AS m9,  AVG(cr.m10) AS m10, AVG(cr.m11) AS m11,
            AVG(cr.m12) AS m12, AVG(cr.m13) AS m13, AVG(cr.m14) AS m14,
            AVG(cr.m15) AS m15,
            STDDEV(cr.m1) / NULLIF(AVG(cr.m1), 0) AS volatility
        FROM fact.fact_company_revenue cr
        JOIN dim.dim_company_games cg
          ON cg.product_code = cr.product_code AND cg.market = cr.market
        WHERE cg.st_country_code = '{country}'
    """).df()

    if curve_df.empty or curve_df['m0'].isna().all():
        print(f"  ○ forecast: no company curve data for {country}")
        con.close()
        return pd.DataFrame()

    row = curve_df.iloc[0]
    m0_val = row['m0'] or 1.0
    volatility = row['volatility'] or 0.3  # default 30% uncertainty

    rows_out = []
    for m in range(min(months_ahead, 16)):
        col = f'm{m}'
        val = row.get(col) or 0.0
        mid = float(val)
        rows_out.append({
            'genre': genre,
            'subgenre': None,
            'country': country,
            'os': os,
            'forecast_month': forecast_base_month,
            'period': m,
            'revenue_mid_usd': round(mid, 2),
            'revenue_low_usd': round(mid * max(0, 1 - volatility), 2),
            'revenue_high_usd': round(mid * (1 + volatility), 2),
            'calibration_factor': 1.0,  # update after benchmarking
        })

    out_df = pd.DataFrame(rows_out)

    con.execute(f"""DELETE FROM analytics.revenue_forecast
                    WHERE genre = '{genre}' AND country = '{country}'
                    AND os = '{os}'""")
    con.execute("INSERT INTO analytics.revenue_forecast "
                "(genre, subgenre, country, os, forecast_month, period, "
                "revenue_mid_usd, revenue_low_usd, revenue_high_usd, "
                "calibration_factor, computed_at) "
                "SELECT genre, subgenre, country, os, forecast_month, period, "
                "revenue_mid_usd, revenue_low_usd, revenue_high_usd, "
                "calibration_factor, current_timestamp FROM out_df")
    con.close()
    print(f"  ✓ revenue_forecast({genre}, {country}): {len(out_df)} periods, "
          f"M0=${out_df.iloc[0]['revenue_mid_usd']:,.0f} → "
          f"M{len(out_df)-1}=${out_df.iloc[-1]['revenue_mid_usd']:,.0f}")
    return out_df


# ────────────────────────────────────────────────────────
# ORCHESTRATOR
# ────────────────────────────────────────────────────────

def run_all_analytics(report_month: str, db_path: str | None = None):
    """Run the full analytics computation pipeline for a given report month."""
    print(f"\n=== Analytics: {report_month} ===")
    print("── Phase 3: Benchmarking ──")
    compute_benchmark_accuracy(db_path)

    print("── Phase 4: Cohort Retention ──")
    compute_cohort_retention(db_path)

    print("── Phase 5: LTV Model ──")
    compute_ltv_model(db_path)

    print("── Phase 6: Genre PnL ──")
    compute_genre_pnl(report_month, db_path)

    print("\n✓ All analytics completed.")


if __name__ == "__main__":
    from datetime import date
    run_all_analytics(report_month=date.today().strftime('%Y-%m-01'))
