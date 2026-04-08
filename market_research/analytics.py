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
from config import DB_PATH, IAP_PCT, IAP_PCT_MARKET_DEFAULT, IAP_PCT_FALLBACK, COMPANY_MARKET_ST_COUNTRIES


def _con(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path or str(DB_PATH))


# ────────────────────────────────────────────────────────
# PHASE 3: BENCHMARKING
# ────────────────────────────────────────────────────────

def _get_iap_pct(genre, market: str) -> float:
    """Return IAP fraction for (genre, market). Falls back to market default, then global."""
    if genre:
        iap = IAP_PCT.get((str(genre).lower().strip(), market))
        if iap is not None:
            return iap
    return IAP_PCT_MARKET_DEFAULT.get(market, IAP_PCT_FALLBACK)


def compute_iap_sensitivity(db_path: str | None = None) -> pd.DataFrame:
    """
    Run benchmark at IAP% multipliers [0.25, 0.50, 0.75, 1.0] to find the
    empirically optimal IAP% per genre×market.

    Also back-calculates implied_iap_pct = median(ST / company_gross) per
    genre×market as a data-driven recalibration signal.

    Prints a comparison table and flags pairs where implied differs >20%
    from current config values.

    Returns DataFrame(genre, market, current_iap_pct, implied_iap_pct,
                       best_multiplier, optimal_iap_pct, median_abs_variance_pct, sample_size)
    """
    con = _con(db_path)

    rev_wide: pd.DataFrame = con.execute("""
        SELECT
            cr.product_code, cr.market, cr.ob_date,
            cg.unified_app_id, cg.genre,
            cr.m0,  cr.m1,  cr.m2,  cr.m3,  cr.m4,  cr.m5,
            cr.m6,  cr.m7,  cr.m8,  cr.m9,  cr.m10, cr.m11,
            cr.m12, cr.m13, cr.m14, cr.m15, cr.m16, cr.m17,
            cr.m18, cr.m19, cr.m20, cr.m21, cr.m22, cr.m23,
            cr.m24, cr.m25, cr.m26, cr.m27, cr.m28, cr.m29,
            cr.m30, cr.m31, cr.m32, cr.m33, cr.m34, cr.m35, cr.m36
        FROM fact.fact_company_revenue cr
        JOIN dim.dim_company_games cg
          ON cg.product_code = cr.product_code AND cg.market = cr.market
        WHERE cg.unified_app_id IS NOT NULL AND cr.ob_date IS NOT NULL
    """).df()

    if rev_wide.empty:
        print("  ○ iap_sensitivity: no mapped games")
        con.close()
        return rev_wide

    id_cols = ['product_code', 'market', 'ob_date', 'unified_app_id', 'genre']
    present_m = [f'm{i}' for i in range(37) if f'm{i}' in rev_wide.columns]
    rev_long = rev_wide.melt(id_vars=id_cols, value_vars=present_m,
                              var_name='period_label', value_name='gross_usd')
    rev_long = rev_long[rev_long['gross_usd'].notna() & (rev_long['gross_usd'] > 0)].copy()
    rev_long['period_n'] = rev_long['period_label'].str[1:].astype(int)
    rev_long['ob_date'] = pd.to_datetime(rev_long['ob_date'], errors='coerce')
    rev_long = rev_long[rev_long['ob_date'].notna()].copy()
    rev_long['calendar_month'] = (
        rev_long['ob_date'].dt.to_period('M') + rev_long['period_n']
    ).dt.to_timestamp().dt.date

    country_map_df = pd.DataFrame(
        [(market, country) for market, countries in COMPANY_MARKET_ST_COUNTRIES.items()
         for country in countries],
        columns=['market', 'st_country'],
    )
    rev_join = rev_long[['product_code', 'market', 'unified_app_id',
                          'calendar_month', 'gross_usd', 'genre']].copy()
    con.register('_sens_rev', rev_join)
    con.register('_sens_cmap', country_map_df)

    try:
        joined: pd.DataFrame = con.execute("""
            WITH st_monthly AS (
                SELECT
                    da.unified_app_id,
                    cm.market,
                    mi.date                        AS calendar_month,
                    SUM(mi.revenue_cents) / 100.0  AS st_estimate_usd
                FROM fact.fact_market_insights mi
                JOIN dim.dim_apps da
                  ON da.app_id = mi.app_id AND da.country = mi.country AND da.os = mi.os
                JOIN _sens_cmap cm ON mi.country = cm.st_country
                WHERE mi.date_granularity = 'monthly' AND mi.revenue_cents > 0
                GROUP BY da.unified_app_id, cm.market, mi.date
            )
            SELECT r.genre, r.market, r.gross_usd, s.st_estimate_usd
            FROM _sens_rev r
            JOIN st_monthly s
              ON s.unified_app_id = r.unified_app_id
             AND s.market = r.market
             AND s.calendar_month = r.calendar_month
        """).df()
    finally:
        con.unregister('_sens_rev')
        con.unregister('_sens_cmap')
    con.close()

    if joined.empty:
        print("  ○ iap_sensitivity: no ST data matches")
        return joined

    joined['genre_key'] = joined['genre'].astype(str).replace('None', '').str.lower().str.strip()
    joined['implied_iap_pct'] = (
        joined['st_estimate_usd'] / joined['gross_usd'].replace(0, float('nan'))
    )

    multipliers = [0.25, 0.50, 0.75, 1.00]
    results = []
    for (genre_key, market), grp in joined.groupby(['genre_key', 'market']):
        current_iap = IAP_PCT.get(
            (genre_key, market), IAP_PCT_MARKET_DEFAULT.get(market, IAP_PCT_FALLBACK)
        )
        implied_median = grp['implied_iap_pct'].dropna().median()

        best_mult, best_mad = None, float('inf')
        for mult in multipliers:
            iap_actual = grp['gross_usd'] * (current_iap * mult)
            variance = (
                (grp['st_estimate_usd'] - iap_actual)
                / iap_actual.replace(0, float('nan')) * 100
            )
            mad = variance.abs().median()
            if mad < best_mad:
                best_mad, best_mult = mad, mult

        results.append({
            'genre': genre_key,
            'market': market,
            'current_iap_pct': current_iap,
            'implied_iap_pct': round(implied_median, 4) if not pd.isna(implied_median) else None,
            'best_multiplier': best_mult,
            'optimal_iap_pct': round(current_iap * best_mult, 4),
            'median_abs_variance_pct': round(best_mad, 1),
            'sample_size': len(grp),
        })

    out_df = pd.DataFrame(results)
    if out_df.empty:
        print("  ○ iap_sensitivity: no results")
        return out_df

    # Print comparison table
    header = f"  {'Genre':<15} {'Market':<15} {'Current':>8} {'Implied':>8} {'Optimal':>8} {'Mult':>6} {'Med|Var|%':>10} {'N':>5}"
    print("\n── IAP% Sensitivity Analysis ──────────────────────────────────")
    print(header)
    print("  " + "-" * (len(header) - 2))
    for _, row in out_df.sort_values(['market', 'genre']).iterrows():
        imp = row['implied_iap_pct']
        implied_str = f"{imp:.2%}" if imp is not None else "    N/A"
        flag = ""
        if imp is not None and row['current_iap_pct']:
            diff_ratio = abs(imp - row['current_iap_pct']) / row['current_iap_pct']
            if diff_ratio > 0.20:
                flag = " ← UPDATE"
        print(f"  {row['genre']:<15} {row['market']:<15} "
              f"{row['current_iap_pct']:>7.2%} {implied_str:>8} "
              f"{row['optimal_iap_pct']:>7.2%} {row['best_multiplier']:>6.2f} "
              f"{row['median_abs_variance_pct']:>10.1f} "
              f"{row['sample_size']:>5}{flag}")
    print()
    return out_df


# Confirmed false-positive mappings: product_name → reason
# These are games that were auto-mapped to wrong ST apps (identified by extreme variance)
_FALSE_POSITIVE_MAPPINGS = [
    "Thánh Quang Thiên Sứ",       # 465% variance, single row — wrong ST app matched
    "Thần Ma Đại Lục",             # 8 months, 7 Unreliable, +147.6% median — mapped to wrong VN MMORPG
    "Võ Lâm Truyền Kỳ 1 Mobile",  # 3 months, 3 Unreliable, −100% (ST ≈ $0) — app delisted / wrong mapping
    "Hello Cafe",                  # 4 months, 4 Unreliable, 0 Accurate, 88.7% median |var| — wrong mapping
    "Metal Slug",                  # 2 months, 2 Unreliable, 0 Accurate, 70.9% median |var| — wrong mapping
    "KON",                         # Mapped to Mobile Legends: Bang Bang (unified=57955d280211a6718a000002)
                                   # Confirmed by Apple chart validator — KON is a different company game
    # NOTE: "YS" Sing-Malay is a PARTIAL false-positive (valid 2022-07–2023-03, then product
    # discontinued). Uses benchmark_valid_to date-cutoff — do NOT NULL the whole mapping.
]


def fix_false_positive_mappings(db_path: str | None = None) -> None:
    """
    NULL out unified_app_id for confirmed false-positive game mappings.
    Run before compute_benchmark_accuracy() to avoid contaminating the benchmark.

    False positives are identified by extreme variance (>200%) on a single game
    where manual inspection confirms the ST app doesn't match the company game.
    """
    con = _con(db_path)
    total_fixed = 0
    for product_name in _FALSE_POSITIVE_MAPPINGS:
        rows = con.execute("""
            SELECT COUNT(*) FROM dim.dim_company_games
            WHERE product_name = ? AND unified_app_id IS NOT NULL
        """, [product_name]).fetchone()[0]
        if rows > 0:
            con.execute("""
                UPDATE dim.dim_company_games
                SET unified_app_id = NULL
                WHERE product_name = ?
            """, [product_name])
            print(f"  ✓ Nulled mapping for '{product_name}' ({rows} row(s))")
            total_fixed += rows
        else:
            print(f"  ○ '{product_name}' already NULL or not found")
    con.close()
    if total_fixed:
        print(f"  ✓ fix_false_positive_mappings: {total_fixed} mapping(s) cleared")
    else:
        print("  ✓ fix_false_positive_mappings: nothing to fix")


def compute_benchmark_accuracy(db_path: str | None = None) -> pd.DataFrame:
    """
    Compare ST sales estimates vs company IAP revenue month by month.

    M0-M36 cohort columns are unpivoted to calendar months via ob_date + n months.
    Multi-country markets (Sing-Malay, TW-HK) aggregate ST revenue across all
    constituent countries. Company gross revenue is scaled by IAP% before comparison
    because ST only captures App Store/Google Play (not web/direct payments).

    Accuracy tiers: Accurate <15%, Acceptable <40%, Unreliable >=40%
    """
    con = _con(db_path)

    # Load company revenue (wide) joined with game dimensions
    rev_wide: pd.DataFrame = con.execute("""
        SELECT
            cr.product_code,
            cr.market,
            cr.ob_date,
            cg.product_name,
            cg.unified_app_id,
            cg.genre,
            cg.iap_pct_override,
            cg.benchmark_valid_to,
            cr.m0,  cr.m1,  cr.m2,  cr.m3,  cr.m4,  cr.m5,
            cr.m6,  cr.m7,  cr.m8,  cr.m9,  cr.m10, cr.m11,
            cr.m12, cr.m13, cr.m14, cr.m15, cr.m16, cr.m17,
            cr.m18, cr.m19, cr.m20, cr.m21, cr.m22, cr.m23,
            cr.m24, cr.m25, cr.m26, cr.m27, cr.m28, cr.m29,
            cr.m30, cr.m31, cr.m32, cr.m33, cr.m34, cr.m35, cr.m36
        FROM fact.fact_company_revenue cr
        JOIN dim.dim_company_games cg
          ON cg.product_code = cr.product_code AND cg.market = cr.market
        WHERE cg.unified_app_id IS NOT NULL
          AND cr.ob_date IS NOT NULL
    """).df()

    if rev_wide.empty:
        print("  ○ benchmark_accuracy: no mapped games yet (set unified_app_id first)")
        con.close()
        return rev_wide

    # Unpivot M0-M36 → long format (one row per product × market × cohort month)
    id_cols = ['product_code', 'product_name', 'market', 'ob_date', 'unified_app_id', 'genre',
               'iap_pct_override', 'benchmark_valid_to']
    m_cols = [f'm{i}' for i in range(37)]
    present_m = [c for c in m_cols if c in rev_wide.columns]
    rev_long = rev_wide.melt(
        id_vars=id_cols, value_vars=present_m,
        var_name='period_label', value_name='actual_usd',
    )
    rev_long = rev_long[rev_long['actual_usd'].notna() & (rev_long['actual_usd'] > 0)].copy()
    rev_long['period_n'] = rev_long['period_label'].str[1:].astype(int)

    # Compute calendar month: ob_date month + n months (vectorized via Period arithmetic)
    rev_long['ob_date'] = pd.to_datetime(rev_long['ob_date'], errors='coerce')
    rev_long = rev_long[rev_long['ob_date'].notna()].copy()
    rev_long['calendar_month'] = (
        rev_long['ob_date'].dt.to_period('M') + rev_long['period_n'].astype(int)
    ).dt.to_timestamp().dt.date

    # Apply benchmark_valid_to cutoff: exclude rows where calendar_month > cutoff date
    has_cutoff = rev_long['benchmark_valid_to'].notna()
    if has_cutoff.any():
        rev_long['benchmark_valid_to'] = pd.to_datetime(
            rev_long['benchmark_valid_to'], errors='coerce'
        ).dt.date
        beyond_cutoff = has_cutoff & (rev_long['calendar_month'] > rev_long['benchmark_valid_to'])
        n_excluded = beyond_cutoff.sum()
        if n_excluded:
            print(f"  ↳ benchmark_valid_to: excluded {n_excluded} rows beyond game cutoff dates")
        rev_long = rev_long[~beyond_cutoff].copy()

    # Apply IAP%: use per-game override if set, else config lookup
    genre_norm = rev_long['genre'].fillna('').str.lower().str.strip()
    config_iap = [
        IAP_PCT.get((g, m), IAP_PCT_MARKET_DEFAULT.get(m, IAP_PCT_FALLBACK))
        for g, m in zip(genre_norm, rev_long['market'])
    ]
    rev_long['iap_pct'] = rev_long['iap_pct_override'].where(
        rev_long['iap_pct_override'].notna(), other=pd.Series(config_iap, index=rev_long.index)
    )
    rev_long['iap_actual_usd'] = rev_long['actual_usd'] * rev_long['iap_pct']

    # Prepare join frame
    rev_join = rev_long[
        ['product_code', 'product_name', 'market', 'unified_app_id',
         'calendar_month', 'iap_actual_usd', 'iap_pct']
    ].copy()

    # Register temp views for the ST join
    country_map_df = pd.DataFrame(
        [(market, country) for market, countries in COMPANY_MARKET_ST_COUNTRIES.items()
         for country in countries],
        columns=['market', 'st_country'],
    )
    con.register('_rev_long', rev_join)
    con.register('_country_map', country_map_df)

    # Join against ST monthly data, aggregating multi-country regions
    try:
        df: pd.DataFrame = con.execute("""
            WITH st_monthly AS (
                SELECT
                    da.unified_app_id,
                    cm.market,
                    mi.date                        AS calendar_month,
                    SUM(mi.revenue_cents) / 100.0  AS st_estimate_usd
                FROM fact.fact_market_insights mi
                JOIN dim.dim_apps da
                  ON da.app_id = mi.app_id
                 AND da.country = mi.country
                 AND da.os = mi.os
                JOIN _country_map cm ON mi.country = cm.st_country
                WHERE mi.date_granularity = 'monthly'
                  AND mi.revenue_cents > 0
                GROUP BY da.unified_app_id, cm.market, mi.date
            )
            SELECT
                r.product_code,
                r.product_name,
                r.market                           AS country,
                r.calendar_month                   AS month,
                s.st_estimate_usd,
                r.iap_actual_usd                   AS actual_usd,
                r.iap_pct,
                ROUND(
                    (s.st_estimate_usd - r.iap_actual_usd)
                    / NULLIF(r.iap_actual_usd, 0) * 100.0, 1
                )                                  AS variance_pct
            FROM _rev_long r
            JOIN st_monthly s
              ON s.unified_app_id = r.unified_app_id
             AND s.market = r.market
             AND s.calendar_month = r.calendar_month
            ORDER BY r.product_code, r.market, r.calendar_month
        """).df()
    finally:
        con.unregister('_rev_long')
        con.unregister('_country_map')

    if df.empty:
        print("  ○ benchmark_accuracy: no ST date matches (check ob_date alignment or ST coverage)")
        con.close()
        return df

    def _tier(pct: float | None) -> str:
        if pd.isna(pct):
            return 'Unknown'
        p = abs(pct)
        if p < 15:
            return 'Accurate'
        if p < 40:
            return 'Acceptable'
        return 'Unreliable'

    df['accuracy_tier'] = df['variance_pct'].apply(_tier)

    con.execute("DELETE FROM analytics.benchmark_accuracy")
    con.execute("""
        INSERT INTO analytics.benchmark_accuracy
            (product_code, product_name, country, month,
             st_estimate_usd, actual_usd, variance_pct, accuracy_tier, iap_pct, computed_at)
        SELECT product_code, product_name, country, month,
               st_estimate_usd, actual_usd, variance_pct, accuracy_tier, iap_pct,
               current_timestamp
        FROM df
    """)
    con.close()
    print(f"  ✓ benchmark_accuracy: {len(df)} rows, "
          f"{(df.accuracy_tier == 'Accurate').sum()} accurate, "
          f"{(df.accuracy_tier == 'Acceptable').sum()} acceptable, "
          f"{(df.accuracy_tier == 'Unreliable').sum()} unreliable")
    return df


def get_calibration_factors(db_path: str | None = None) -> pd.DataFrame:
    """
    Compute per-genre, per-market calibration factor:
      calibration_factor = MEDIAN(actual_iap / st_estimate) for benchmarked games

    Used to scale ST competitor estimates toward true IAP values.
    Returns a DataFrame(genre, market, calibration_factor, sample_size).
    """
    con = _con(db_path)
    df: pd.DataFrame = con.execute("""
        SELECT
            COALESCE(cg.genre, 'Unknown')                       AS genre,
            ba.country                                          AS market,
            ROUND(MEDIAN(ba.actual_usd / NULLIF(ba.st_estimate_usd, 0)), 2) AS calibration_factor,
            ROUND(AVG(ba.actual_usd / NULLIF(ba.st_estimate_usd, 0)), 2)    AS avg_calibration_factor,
            COUNT(*)                                            AS sample_size
        FROM analytics.benchmark_accuracy ba
        JOIN dim.dim_company_games cg
          ON cg.product_code = ba.product_code
         AND cg.market = ba.country
        WHERE ba.st_estimate_usd > 0
          AND ba.actual_usd > 0
        GROUP BY COALESCE(cg.genre, 'Unknown'), ba.country
        HAVING COUNT(*) >= 2
        ORDER BY genre, market
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


def compute_ltv_model_v2(db_path: str | None = None) -> pd.DataFrame:
    """
    Compute LTV(n) using RPD-based ARPDAU proxy instead of active_users.

    ARPDAU proxy = median(ltv_rpd_usd) per genre×country from rpd_model.
    LTV(n) = Σ(m=0..n) [arpdau_proxy × retention_rate(m)]

    Joins rpd_model → dim_apps (via unified_app_id) for ST category_id.
    Joins cohort_retention for genre-average retention curves.
    """
    con = _con(db_path)

    # Step 1: Genre-level RPD (ARPDAU proxy) from rpd_model + dim_apps
    rpd_df = con.execute("""
        WITH app_cat AS (
            SELECT app_id, unified_app_id,
                   FIRST(category_id) AS genre
            FROM dim.dim_apps
            WHERE category_id IS NOT NULL
              AND category_id NOT IN ('', 'NaN', '0')
              AND unified_app_id IS NOT NULL
            GROUP BY app_id, unified_app_id
        )
        SELECT ac.genre, rpm.country, rpm.os,
               MEDIAN(rpm.ltv_rpd_usd) AS median_rpd,
               COUNT(DISTINCT rpm.unified_app_id) AS sample_size
        FROM analytics.rpd_model rpm
        JOIN app_cat ac ON ac.unified_app_id = rpm.unified_app_id
        WHERE rpm.ltv_rpd_usd IS NOT NULL AND rpm.ltv_rpd_usd > 0
        GROUP BY ac.genre, rpm.country, rpm.os
    """).df()

    # Step 2: Genre-average retention curves
    ret_df = con.execute("""
        SELECT genre, country, os, period,
               AVG(retention_pct) / 100.0 AS retention_rate
        FROM analytics.cohort_retention
        GROUP BY genre, country, os, period
        ORDER BY genre, country, os, period
    """).df()

    if rpd_df.empty or ret_df.empty:
        print("  ○ ltv_model_v2: need rpd_model + cohort_retention data")
        con.close()
        return pd.DataFrame()

    # Step 3: Merge and compute cumulative LTV
    merged = rpd_df.merge(ret_df, on=['genre', 'country', 'os'], how='inner')
    merged = merged.sort_values(['genre', 'country', 'os', 'period'])

    rows_out = []
    for (genre, country, os_val), grp in merged.groupby(['genre', 'country', 'os']):
        arpdau = float(grp['median_rpd'].iloc[0])
        cumulative_ltv = 0.0
        for _, row in grp.iterrows():
            month_rev = arpdau * row['retention_rate']
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
        print("  ○ ltv_model_v2: no cross-matched genre data")
        con.close()
        return out_df

    con.execute("DELETE FROM analytics.ltv_model")
    con.execute("""
        INSERT INTO analytics.ltv_model
            (genre, subgenre, country, os, period, arpdau, retention_pct, ltv_usd, computed_at)
        SELECT genre, subgenre, country, os, period, arpdau, retention_pct, ltv_usd,
               current_timestamp
        FROM out_df
    """)

    n = len(out_df)
    genres = out_df['genre'].nunique()
    max_ltv = out_df['ltv_usd'].max()
    print(f"  ✓ ltv_model_v2: {n} rows ({genres} genres, max LTV=${max_ltv:.2f})")
    con.close()
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


def compute_genre_pnl_v2(report_month: str | None = None,
                          db_path: str | None = None) -> pd.DataFrame:
    """
    Genre PnL v2 — uses fact_market_insights for TAM instead of sparse fact_genre_summary.

    For each ST category × country × month:
      - TAM from MI aggregated by dim_apps.category_id
      - Genre revenue share = genre_tam / store_tam
      - Growth = TAM delta vs 3 months prior
      - HHI + concentration tier from analytics.genre_concentration
      - LTV/retention from analytics.ltv_model + analytics.cohort_retention

    Writes to analytics.genre_pnl_template.
    """
    con = _con(db_path)

    month_filter = f"AND gt.month = '{report_month}'::DATE" if report_month else ""

    df = con.execute(f"""
        WITH app_cat AS (
            SELECT app_id,
                   FIRST(category_id) AS st_category
            FROM dim.dim_apps
            WHERE category_id IS NOT NULL
              AND category_id NOT IN ('', 'NaN', '0')
            GROUP BY app_id
        ),
        genre_tam AS (
            SELECT ac.st_category AS genre,
                   mi.country,
                   mi.date AS month,
                   SUM(mi.revenue_cents) / 100.0  AS genre_tam_usd,
                   SUM(mi.downloads)               AS genre_downloads,
                   COUNT(DISTINCT mi.app_id)        AS app_count
            FROM fact.fact_market_insights mi
            JOIN app_cat ac ON mi.app_id = ac.app_id
            WHERE mi.revenue_cents > 0
            GROUP BY ac.st_category, mi.country, mi.date
        ),
        store_tam AS (
            SELECT country, date AS month,
                   SUM(revenue_cents) / 100.0 AS store_tam_usd
            FROM fact.fact_market_insights
            WHERE revenue_cents > 0
            GROUP BY country, date
        ),
        conc AS (
            SELECT st_category, country, month,
                   hhi_top10, concentration_tier,
                   top1_share_pct, top10_share_pct
            FROM analytics.genre_concentration
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
        ),
        arpdau_tbl AS (
            SELECT genre, country, os, AVG(arpdau) AS arpdau
            FROM analytics.ltv_model GROUP BY genre, country, os
        )
        SELECT
            gt.genre,
            gt.country,
            'unified' AS os,
            gt.month AS report_month,
            st.store_tam_usd AS tam_usd,
            ROUND(gt.genre_tam_usd / NULLIF(st.store_tam_usd, 0), 4) AS genre_revenue_share,
            gt.genre_tam_usd,
            c.hhi_top10 AS hhi_score,
            c.concentration_tier,
            c.top1_share_pct,
            c.top10_share_pct,
            l.ltv_30,
            l.ltv_90,
            r.d1_ret AS d1_retention,
            r.d30_ret AS d30_retention,
            a.arpdau,
            gt.genre_downloads,
            gt.app_count
        FROM genre_tam gt
        JOIN store_tam st ON st.country = gt.country AND st.month = gt.month
        LEFT JOIN conc c ON c.st_category = gt.genre
            AND c.country = gt.country AND c.month = gt.month
        LEFT JOIN (
            SELECT genre, country, AVG(ltv_30) AS ltv_30, AVG(ltv_90) AS ltv_90
            FROM ltv GROUP BY genre, country
        ) l ON l.genre = gt.genre AND l.country = gt.country
        LEFT JOIN (
            SELECT genre, country, AVG(d1_ret) AS d1_ret, AVG(d30_ret) AS d30_ret
            FROM ret GROUP BY genre, country
        ) r ON r.genre = gt.genre AND r.country = gt.country
        LEFT JOIN (
            SELECT genre, country, AVG(arpdau) AS arpdau
            FROM arpdau_tbl GROUP BY genre, country
        ) a ON a.genre = gt.genre AND a.country = gt.country
        WHERE gt.genre_tam_usd > 0
        {month_filter}
    """).df()

    if df.empty:
        print(f"  ○ genre_pnl_v2: no data")
        con.close()
        return df

    # Compute TAM growth (3 months prior)
    df = df.sort_values(['genre', 'country', 'report_month'])
    df['tam_growth_3m'] = (
        df.groupby(['genre', 'country'])['genre_tam_usd']
          .transform(lambda s: (s - s.shift(3)) / s.shift(3).replace(0, float('nan')))
    )

    # Opportunity score
    med_ltv = df['ltv_90'].median() or 0
    def _score_v2(row) -> str:
        growth = row.get('tam_growth_3m')
        hhi = row.get('hhi_score') or 0
        ltv = row.get('ltv_90') or 0
        if pd.notna(growth) and growth > 0.10 and hhi < 0.25 and ltv > med_ltv:
            return 'HIGH'
        if (pd.notna(growth) and growth < -0.05) or hhi > 0.50:
            return 'LOW'
        return 'MEDIUM'

    df['opportunity_score'] = df.apply(_score_v2, axis=1)

    # Prepare for insert — map columns to existing table schema
    df['subgenre'] = None
    df['top10_revenue_usd'] = None
    df['top10_dau'] = None
    df['num_advertisers'] = None

    # Upsert
    con.execute("DELETE FROM analytics.genre_pnl_template")
    tbl_cols = [r[0] for r in con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = 'genre_pnl_template'
          AND column_name != 'computed_at'
        ORDER BY ordinal_position
    """).fetchall()]

    available = [c for c in tbl_cols if c in df.columns]
    insert_df = df[available].copy()
    con.register('_pnl_in', insert_df)
    con.execute(f"""
        INSERT INTO analytics.genre_pnl_template ({', '.join(available)})
        SELECT {', '.join(available)} FROM _pnl_in
    """)
    con.unregister('_pnl_in')

    n = len(df)
    high = (df['opportunity_score'] == 'HIGH').sum()
    med = (df['opportunity_score'] == 'MEDIUM').sum()
    low = (df['opportunity_score'] == 'LOW').sum()
    print(f"  ✓ genre_pnl_v2: {n:,} rows, {high} HIGH / {med} MEDIUM / {low} LOW")
    con.close()
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


def compute_revenue_forecast_v2(db_path: str | None = None) -> pd.DataFrame:
    """
    Auto-generate revenue forecasts for all genre × market combinations.

    Uses company M0-M24 revenue curves normalized to M1, with calibration
    from composite_benchmark. Produces mid/low/high estimates with CV-based
    confidence bands.
    """
    import numpy as np
    con = _con(db_path)

    # Step 1: Extract company revenue curves by genre × market
    curves = con.execute("""
        SELECT cg.genre, cg.market,
               cr.m0,  cr.m1,  cr.m2,  cr.m3,  cr.m4,  cr.m5,
               cr.m6,  cr.m7,  cr.m8,  cr.m9,  cr.m10, cr.m11,
               cr.m12, cr.m13, cr.m14, cr.m15, cr.m16, cr.m17,
               cr.m18, cr.m19, cr.m20, cr.m21, cr.m22, cr.m23, cr.m24
        FROM fact.fact_company_revenue cr
        JOIN dim.dim_company_games cg
          ON cg.product_code = cr.product_code AND cg.market = cr.market
        WHERE cg.genre IS NOT NULL AND cr.ob_date IS NOT NULL
    """).df()

    if curves.empty:
        print("  ○ revenue_forecast_v2: no company revenue curves")
        con.close()
        return pd.DataFrame()

    # Step 2: Get calibration factors from composite_benchmark
    cal_df = con.execute("""
        SELECT cg.genre, cb.country AS market,
               AVG(cb.composite_mid_usd / NULLIF(cb.st_estimate_usd, 0)) AS cal_factor,
               COUNT(*) AS sample_size
        FROM analytics.composite_benchmark cb
        JOIN dim.dim_company_games cg
          ON cg.product_code = cb.product_code AND cg.market = cb.country
        WHERE cb.accuracy_tier IN ('Accurate', 'Acceptable')
          AND cb.st_estimate_usd > 0
        GROUP BY cg.genre, cb.country
    """).df()

    cal_map = {}
    for _, row in cal_df.iterrows():
        cal_map[(row['genre'], row['market'])] = float(row['cal_factor'])

    # Step 3: Determine forecast base month (latest data month + 1)
    latest_month = con.execute("""
        SELECT MAX(ob_date) FROM fact.fact_company_revenue WHERE ob_date IS NOT NULL
    """).fetchone()[0]
    if latest_month:
        forecast_base = (pd.Timestamp(latest_month) + pd.DateOffset(months=1)).strftime('%Y-%m-%d')
    else:
        forecast_base = '2026-01-01'

    # Step 4: Build genre-average curves
    m_cols = [f'm{i}' for i in range(25)]
    rows_out = []

    for (genre, market), grp in curves.groupby(['genre', 'market']):
        # Compute median of each period
        medians = {}
        for col in m_cols:
            vals = grp[col].dropna()
            vals = vals[vals > 0]
            if len(vals) > 0:
                medians[col] = float(vals.median())

        if 'm1' not in medians or medians['m1'] <= 0:
            continue

        m1_base = medians['m1']
        # Normalize shape to M1
        shape = {}
        for col in m_cols:
            if col in medians:
                shape[col] = medians[col] / m1_base

        # CV for confidence bands
        m1_vals = grp['m1'].dropna()
        m1_vals = m1_vals[m1_vals > 0]
        if len(m1_vals) >= 2:
            cv = float(m1_vals.std() / m1_vals.mean())
            cv = max(0.15, min(cv, 0.60))
        else:
            cv = 0.50  # wide band for sparse data

        # Calibration factor
        cal = cal_map.get((genre, market), 1.0)

        # Get IAP% for this genre × market
        iap = _get_iap_pct(genre, market)

        for m in range(min(25, len(shape))):
            col = f'm{m}'
            if col not in shape:
                break
            rev_mid = m1_base * shape[col] * iap * cal
            rows_out.append({
                'genre': genre,
                'subgenre': None,
                'country': market,
                'os': 'unified',
                'forecast_month': forecast_base,
                'period': m,
                'revenue_mid_usd': round(rev_mid, 2),
                'revenue_low_usd': round(rev_mid * max(0, 1 - cv), 2),
                'revenue_high_usd': round(rev_mid * (1 + cv), 2),
                'calibration_factor': round(cal, 4),
            })

    if not rows_out:
        print("  ○ revenue_forecast_v2: no viable curves")
        con.close()
        return pd.DataFrame()

    out_df = pd.DataFrame(rows_out)
    con.execute("DELETE FROM analytics.revenue_forecast")
    con.execute("""
        INSERT INTO analytics.revenue_forecast
            (genre, subgenre, country, os, forecast_month, period,
             revenue_mid_usd, revenue_low_usd, revenue_high_usd,
             calibration_factor, computed_at)
        SELECT genre, subgenre, country, os, forecast_month, period,
               revenue_mid_usd, revenue_low_usd, revenue_high_usd,
               calibration_factor, current_timestamp
        FROM out_df
    """)

    n = len(out_df)
    genres = out_df['genre'].nunique()
    markets = out_df['country'].nunique()
    print(f"  ✓ revenue_forecast_v2: {n} rows ({genres} genres × {markets} markets)")
    con.close()
    return out_df


# ────────────────────────────────────────────────────────
# BENCHMARK ACCURACY IMPROVEMENT — TASKS 4, 5, 6
# ────────────────────────────────────────────────────────

def compute_rpd(db_path: str | None = None) -> pd.DataFrame:
    """
    Compute Revenue Per Download (RPD) per app × country × OS.

    For SEA MMORPGs, downloads and revenue are decoupled month-by-month:
    - Downloads appear at launch/update months (new installs)
    - Revenue appears in subsequent months (existing users paying via web)
    So point-in-time RPD is meaningless. We use:

      lifetime_rpd = total_cumulative_revenue / total_cumulative_downloads
                     (taken at the last available month per app×country×os)

    This is the primary RPD signal for benchmarking:
      rpd_estimate = competitor_monthly_downloads × genre_median_lifetime_rpd

    Also stores monthly rows for time-series reference.
    Writes to analytics.rpd_model.
    """
    con = _con(db_path)

    # Pull ALL months with downloads OR revenue for mapped games (SEA + TW/HK)
    df: pd.DataFrame = con.execute("""
        SELECT
            da.unified_app_id,
            rst.sub_genre                  AS genre,
            mi.country,
            mi.os,
            mi.date                        AS month,
            SUM(mi.downloads)              AS downloads,
            SUM(mi.revenue_cents) / 100.0  AS revenue_usd
        FROM fact.fact_market_insights mi
        JOIN dim.dim_apps da
          ON da.app_id = mi.app_id AND da.country = mi.country AND da.os = mi.os
        LEFT JOIN raw.raw_st_dim_app rst ON rst.unified_app_id = da.unified_app_id
        WHERE mi.date_granularity = 'monthly'
          AND mi.country IN ('VN','TH','PH','ID','SG','MY','TW','HK')
          AND da.unified_app_id IN (
              SELECT unified_app_id FROM dim.dim_company_games
              WHERE unified_app_id IS NOT NULL
          )
        GROUP BY da.unified_app_id, rst.sub_genre, mi.country, mi.os, mi.date
        HAVING SUM(mi.downloads) > 0 OR SUM(mi.revenue_cents) > 0
        ORDER BY da.unified_app_id, mi.country, mi.os, mi.date
    """).df()

    con.close()

    if df.empty:
        print("  ○ rpd_model: no download/revenue data")
        return df

    # Sort and compute cumulative totals per app × country × os
    df = df.sort_values(['unified_app_id', 'country', 'os', 'month'])
    grp_keys = ['unified_app_id', 'country', 'os']
    df['cumulative_downloads'] = df.groupby(grp_keys)['downloads'].cumsum()
    df['cumulative_revenue_usd'] = df.groupby(grp_keys)['revenue_usd'].cumsum()

    # Point-in-time RPD (useful only where both are > 0 in the same month)
    mask_both = (df['downloads'] > 0) & (df['revenue_usd'] > 0)
    df['rpd_usd'] = float('nan')
    df.loc[mask_both, 'rpd_usd'] = (
        df.loc[mask_both, 'revenue_usd'] / df.loc[mask_both, 'downloads']
    ).round(4)

    # Lifetime RPD = cumulative rev / cumulative downloads (running)
    df['ltv_rpd_usd'] = (
        df['cumulative_revenue_usd']
        / df['cumulative_downloads'].replace(0, float('nan'))
    ).round(4)

    df['arpu_usd'] = float('nan')  # no active_users data from subscription

    con = _con(db_path)
    con.execute("DELETE FROM analytics.rpd_model")
    out_cols = ['unified_app_id', 'country', 'os', 'month', 'downloads',
                'revenue_usd', 'rpd_usd', 'cumulative_downloads',
                'cumulative_revenue_usd', 'ltv_rpd_usd', 'arpu_usd']
    out_df = df[out_cols].copy()
    con.execute(
        "INSERT INTO analytics.rpd_model "
        "(unified_app_id, country, os, month, downloads, revenue_usd, rpd_usd, "
        "cumulative_downloads, cumulative_revenue_usd, ltv_rpd_usd, arpu_usd, computed_at) "
        "SELECT unified_app_id, country, os, month, downloads, revenue_usd, rpd_usd, "
        "cumulative_downloads, cumulative_revenue_usd, ltv_rpd_usd, arpu_usd, "
        "current_timestamp FROM out_df"
    )
    con.close()

    # Summarise lifetime RPD (last row per app×country×os = final cumulative)
    last_rows = df.sort_values('month').groupby(grp_keys).last().reset_index()
    valid_ltv = last_rows['ltv_rpd_usd'].dropna()
    valid_rpd = df['rpd_usd'].dropna()
    print(f"  ✓ rpd_model: {len(out_df):,} rows, "
          f"{int(mask_both.sum())} months with both dl+rev | "
          f"point-in-time RPD median=${valid_rpd.median():.3f} | "
          f"lifetime RPD median=${valid_ltv.median():.3f} "
          f"(p25=${valid_ltv.quantile(0.25):.3f}, p75=${valid_ltv.quantile(0.75):.3f})")
    return out_df


def compute_rpd_benchmark(db_path: str | None = None) -> pd.DataFrame:
    """
    Use genre-median RPD as an independent revenue estimate for benchmarking.

    For each benchmark row (company game × market × month):
      1. Find the genre-median RPD from own-game data in fact_market_insights
      2. Fetch competitor downloads from fact_market_insights
      3. rpd_estimate = competitor_downloads × genre_median_rpd
      4. Compare rpd_estimate vs actual_iap_usd

    Also computes a blended best_estimate:
      best = weighted avg(st_estimate × 1/st_mad, rpd_estimate × 1/rpd_mad)

    Writes to analytics.rpd_benchmark.
    """
    con = _con(db_path)

    # Genre-median LIFETIME RPD per country (last cumulative value per app×country×os)
    # Using sub_genre from raw_st_dim_app for finer segmentation
    genre_rpd: pd.DataFrame = con.execute("""
        WITH last_row AS (
            -- Take the final (most recent) cumulative value per app×country×os
            SELECT
                unified_app_id, country, os,
                LAST(ltv_rpd_usd ORDER BY month) AS lifetime_rpd
            FROM analytics.rpd_model
            WHERE ltv_rpd_usd IS NOT NULL AND ltv_rpd_usd > 0
            GROUP BY unified_app_id, country, os
        )
        SELECT
            rst.sub_genre                                  AS genre,
            lr.country,
            MEDIAN(lr.lifetime_rpd)                        AS median_rpd,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY lr.lifetime_rpd) AS p25_rpd,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY lr.lifetime_rpd) AS p75_rpd,
            COUNT(*)                                       AS sample_size
        FROM last_row lr
        JOIN raw.raw_st_dim_app rst ON rst.unified_app_id = lr.unified_app_id
        WHERE rst.sub_genre IS NOT NULL AND TRIM(rst.sub_genre) != ''
        GROUP BY rst.sub_genre, lr.country
        HAVING COUNT(*) >= 1
    """).df()

    # Base benchmark rows
    ba_df: pd.DataFrame = con.execute("""
        SELECT
            ba.product_code, ba.product_name, ba.country, ba.month,
            ba.st_estimate_usd, ba.actual_usd AS actual_iap_usd,
            ba.variance_pct AS st_variance_pct,
            cg.unified_app_id
        FROM analytics.benchmark_accuracy ba
        JOIN dim.dim_company_games cg
          ON cg.product_code = ba.product_code AND cg.market = ba.country
        WHERE ba.st_estimate_usd > 0 AND ba.actual_usd > 0
    """).df()

    if ba_df.empty or genre_rpd.empty:
        print("  ○ rpd_benchmark: need benchmark_accuracy + rpd_model data first")
        con.close()
        return pd.DataFrame()

    # Get monthly downloads for each benchmarked app × country
    dl_df: pd.DataFrame = con.execute("""
        SELECT
            da.unified_app_id,
            mi.country,
            mi.date                        AS month,
            SUM(mi.downloads)              AS downloads
        FROM fact.fact_market_insights mi
        JOIN dim.dim_apps da
          ON da.app_id = mi.app_id AND da.country = mi.country AND da.os = mi.os
        WHERE mi.date_granularity = 'monthly'
          AND mi.downloads > 0
        GROUP BY da.unified_app_id, mi.country, mi.date
    """).df()

    # Country map: company market → ST countries
    country_map_df = pd.DataFrame(
        [(market, ctry) for market, countries in COMPANY_MARKET_ST_COUNTRIES.items()
         for ctry in countries],
        columns=['market', 'st_country'],
    )

    # Aggregate downloads to company-market level (multi-country markets)
    ba_joined = ba_df.merge(
        country_map_df.rename(columns={'market': 'country'}),
        on='country', how='left'
    )
    dl_agg = dl_df.merge(
        country_map_df.rename(columns={'st_country': 'country'}),
        on='country', how='left'
    ).rename(columns={'country': 'st_country', 'market': 'country'})
    dl_market = dl_agg.groupby(['unified_app_id', 'country', 'month'])['downloads'].sum().reset_index()

    ba_with_dl = ba_joined.merge(dl_market, on=['unified_app_id', 'country', 'month'], how='left')

    # Get sub_genre for each app from raw ST data
    cat_df: pd.DataFrame = con.execute("""
        SELECT DISTINCT unified_app_id, sub_genre AS genre
        FROM raw.raw_st_dim_app
        WHERE unified_app_id IS NOT NULL AND sub_genre IS NOT NULL AND TRIM(sub_genre) != ''
    """).df()
    ba_with_dl = ba_with_dl.merge(cat_df, on='unified_app_id', how='left')

    # Match RPD using primary ST country (st_country from country_map)
    # For multi-country markets (Sing-Malay → SG, TW-HK → TW), use the primary country
    primary_country = country_map_df.groupby('market')['st_country'].first().reset_index()
    ba_with_dl = ba_with_dl.merge(
        primary_country.rename(columns={'market': 'country', 'st_country': 'st_country_primary'}),
        on='country', how='left'
    )
    ba_with_dl = ba_with_dl.merge(
        genre_rpd[['genre', 'country', 'median_rpd']].rename(
            columns={'country': 'st_country_primary'}),
        on=['genre', 'st_country_primary'], how='left'
    )

    # RPD estimate = downloads × genre median RPD
    ba_with_dl['rpd_estimate_usd'] = (
        ba_with_dl['downloads'] * ba_with_dl['median_rpd']
    ).round(2)

    # Variance metrics
    ba_with_dl['rpd_vs_actual_pct'] = (
        (ba_with_dl['rpd_estimate_usd'] - ba_with_dl['actual_iap_usd'])
        / ba_with_dl['actual_iap_usd'].replace(0, float('nan')) * 100
    ).round(1)

    # Blended best estimate (only when RPD estimate available)
    # ST historical MAD ~50%, RPD ~40% (downloads more accurate signal)
    w_st, w_rpd = 1 / 0.50, 1 / 0.40
    has_rpd = ba_with_dl['rpd_estimate_usd'].notna() & (ba_with_dl['rpd_estimate_usd'] > 0)
    ba_with_dl['best_estimate_usd'] = ba_with_dl['st_estimate_usd'].copy()
    ba_with_dl.loc[has_rpd, 'best_estimate_usd'] = (
        (ba_with_dl.loc[has_rpd, 'st_estimate_usd'] * w_st
         + ba_with_dl.loc[has_rpd, 'rpd_estimate_usd'] * w_rpd)
        / (w_st + w_rpd)
    ).round(2)

    def _tier(pct) -> str:
        if pd.isna(pct):
            return 'Unknown'
        p = abs(pct)
        return 'Accurate' if p < 15 else ('Acceptable' if p < 40 else 'Unreliable')

    ba_with_dl['accuracy_tier'] = ba_with_dl['rpd_vs_actual_pct'].apply(_tier)

    out_cols = ['product_code', 'product_name', 'country', 'month',
                'st_estimate_usd', 'rpd_estimate_usd', 'actual_iap_usd',
                'rpd_vs_actual_pct', 'st_variance_pct' if 'st_variance_pct' in ba_with_dl.columns else 'st_vs_actual_pct',
                'best_estimate_usd', 'accuracy_tier']
    # normalise column name
    if 'st_variance_pct' in ba_with_dl.columns:
        ba_with_dl = ba_with_dl.rename(columns={'st_variance_pct': 'st_vs_actual_pct'})
    out_df = ba_with_dl[[c for c in out_cols if c in ba_with_dl.columns]].drop_duplicates(
        subset=['product_code', 'country', 'month']
    ).copy()

    con.execute("DELETE FROM analytics.rpd_benchmark")
    col_list = ', '.join(out_df.columns)
    con.execute(
        f"INSERT INTO analytics.rpd_benchmark ({col_list}, computed_at) "
        f"SELECT {col_list}, current_timestamp FROM out_df"
    )
    con.close()

    has_est = out_df['rpd_estimate_usd'].notna() & (out_df['rpd_estimate_usd'] > 0)
    accurate = (out_df.loc[has_est, 'accuracy_tier'] == 'Accurate').sum()
    acceptable = (out_df.loc[has_est, 'accuracy_tier'] == 'Acceptable').sum()
    total_est = has_est.sum()
    print(f"  ✓ rpd_benchmark: {len(out_df)} rows, "
          f"{total_est} with RPD estimate — "
          f"{accurate} Accurate ({accurate/max(total_est,1)*100:.1f}%), "
          f"{acceptable} Acceptable")
    return out_df


def compute_download_triangulation(db_path: str | None = None) -> pd.DataFrame:
    """
    Build an independent revenue estimate from: Downloads × Genre-Median Lifetime RPD.

    Replaces the original ARPDAU × retention approach (blocked on active_users from ST).
    Instead uses lifetime RPD (cumulative_revenue / cumulative_downloads) from rpd_model
    as an ARPU proxy — validated against known company games.

    synth_revenue = monthly_competitor_downloads × genre_median_lifetime_rpd

    Confidence:
      HIGH   — ST and synthetic agree within ±20%
      MEDIUM — agree within ±50%
      LOW    — disagree >50% or one signal missing

    Writes to analytics.download_triangulation.
    """
    con = _con(db_path)

    # Genre-median lifetime RPD — same source as rpd_benchmark
    genre_rpd: pd.DataFrame = con.execute("""
        WITH last_row AS (
            SELECT unified_app_id, country, os,
                   LAST(ltv_rpd_usd ORDER BY month) AS lifetime_rpd
            FROM analytics.rpd_model
            WHERE ltv_rpd_usd IS NOT NULL AND ltv_rpd_usd > 0
            GROUP BY unified_app_id, country, os
        )
        SELECT
            rst.sub_genre   AS genre,
            lr.country,
            MEDIAN(lr.lifetime_rpd) AS median_rpd
        FROM last_row lr
        JOIN raw.raw_st_dim_app rst ON rst.unified_app_id = lr.unified_app_id
        WHERE rst.sub_genre IS NOT NULL AND TRIM(rst.sub_genre) != ''
        GROUP BY rst.sub_genre, lr.country
        HAVING COUNT(*) >= 1
    """).df()

    if genre_rpd.empty:
        print("  ○ download_triangulation: rpd_model empty — run compute_rpd() first")
        con.close()
        return pd.DataFrame()

    # Downloads from fact_market_insights for company-mapped apps
    dl_df: pd.DataFrame = con.execute("""
        SELECT
            da.unified_app_id,
            mi.country,
            mi.date                        AS month,
            mi.os,
            SUM(mi.downloads)              AS downloads,
            SUM(mi.revenue_cents) / 100.0  AS st_revenue
        FROM fact.fact_market_insights mi
        JOIN dim.dim_apps da
          ON da.app_id = mi.app_id AND da.country = mi.country AND da.os = mi.os
        WHERE mi.date_granularity = 'monthly'
          AND mi.downloads > 0
          AND da.unified_app_id IS NOT NULL
        GROUP BY da.unified_app_id, mi.country, mi.date, mi.os
    """).df()

    if dl_df.empty:
        print("  ○ download_triangulation: no download data in fact_market_insights")
        con.close()
        return dl_df

    # Sub-genre lookup per app
    genre_df: pd.DataFrame = con.execute("""
        SELECT DISTINCT unified_app_id, sub_genre AS genre
        FROM raw.raw_st_dim_app
        WHERE unified_app_id IS NOT NULL AND sub_genre IS NOT NULL AND TRIM(sub_genre) != ''
    """).df()

    # Country map: ST country → company market (for RPD lookup by primary country)
    country_map_df = pd.DataFrame(
        [(market, ctry) for market, countries in COMPANY_MARKET_ST_COUNTRIES.items()
         for ctry in countries],
        columns=['market', 'st_country'],
    )
    primary_country = country_map_df.groupby('market')['st_country'].first().reset_index()

    merged = dl_df.merge(genre_df, on='unified_app_id', how='left')
    # Map ST country to primary ST country (for multi-country markets SG→SG, MY→SG)
    merged = merged.merge(
        country_map_df.rename(columns={'st_country': 'country', 'market': 'mkt'}),
        on='country', how='left'
    )
    merged = merged.merge(
        primary_country.rename(columns={'market': 'mkt', 'st_country': 'st_country_primary'}),
        on='mkt', how='left'
    )
    merged['rpd_lookup_country'] = merged['st_country_primary'].fillna(merged['country'])

    # Join genre-median RPD
    merged = merged.merge(
        genre_rpd.rename(columns={'country': 'rpd_lookup_country'}),
        on=['genre', 'rpd_lookup_country'], how='left'
    )

    # Synthetic revenue estimate
    merged['synth_revenue'] = (merged['downloads'] * merged['median_rpd']).round(2)

    # Pull company actuals from benchmark_accuracy (where available)
    actual_df: pd.DataFrame = con.execute("""
        SELECT ba.product_code, ba.country, ba.month, ba.actual_usd AS actual_iap_usd
        FROM analytics.benchmark_accuracy ba
    """).df()
    game_map: pd.DataFrame = con.execute("""
        SELECT DISTINCT cg.product_code, cg.market AS country, cg.unified_app_id
        FROM dim.dim_company_games cg
        WHERE cg.unified_app_id IS NOT NULL
    """).df()
    merged = merged.merge(
        game_map[['unified_app_id', 'country', 'product_code']].drop_duplicates()
        .rename(columns={'country': 'mkt_for_actual'}),
        on='unified_app_id', how='left'
    )
    # mkt column holds company market name; actual_df uses company market as country
    merged = merged.rename(columns={'mkt': 'company_market'})
    actual_join = merged.merge(
        actual_df.rename(columns={'country': 'company_market'}),
        on=['product_code', 'company_market', 'month'], how='left'
    )
    merged['actual_iap_usd'] = actual_join['actual_iap_usd'].values

    # Variance pcts
    actual_base = merged['actual_iap_usd'].replace(0, float('nan'))
    merged['st_vs_actual_pct'] = (
        (merged['st_revenue'] - merged['actual_iap_usd']) / actual_base * 100
    ).round(1)
    merged['synth_vs_actual_pct'] = (
        (merged['synth_revenue'] - merged['actual_iap_usd']) / actual_base * 100
    ).round(1)

    # Confidence based on agreement between ST and synthetic
    def _confidence(row) -> str:
        st, syn = row['st_revenue'], row['synth_revenue']
        if pd.isna(syn) or st <= 0 or syn <= 0:
            return 'LOW'
        diff = abs(st - syn) / max(st, syn)
        if diff < 0.20:
            return 'HIGH'
        if diff < 0.50:
            return 'MEDIUM'
        return 'LOW'

    merged['confidence'] = merged.apply(_confidence, axis=1)

    # Inverse-variance weighted best estimate: ST ~60% MAD, RPD ~80% MAD
    w_st, w_syn = 1 / 0.60, 1 / 0.80
    has_synth = merged['synth_revenue'].notna() & (merged['synth_revenue'] > 0)
    merged['best_estimate_usd'] = merged['st_revenue'].copy()
    merged.loc[has_synth, 'best_estimate_usd'] = (
        (merged.loc[has_synth, 'st_revenue'] * w_st
         + merged.loc[has_synth, 'synth_revenue'] * w_syn)
        / (w_st + w_syn)
    ).round(2)

    out_cols = ['unified_app_id', 'country', 'month', 'os',
                'st_revenue', 'synth_revenue', 'actual_iap_usd',
                'st_vs_actual_pct', 'synth_vs_actual_pct',
                'best_estimate_usd', 'confidence']
    out_df = (
        merged[out_cols]
        .dropna(subset=['unified_app_id', 'month'])
        .drop_duplicates(subset=['unified_app_id', 'country', 'month', 'os'])
        .copy()
    )

    con.execute("DELETE FROM analytics.download_triangulation")
    con.execute(
        "INSERT INTO analytics.download_triangulation "
        "(unified_app_id, country, month, os, st_revenue, synth_revenue, actual_iap_usd, "
        "st_vs_actual_pct, synth_vs_actual_pct, best_estimate_usd, confidence, computed_at) "
        "SELECT unified_app_id, country, month, os, st_revenue, synth_revenue, actual_iap_usd, "
        "st_vs_actual_pct, synth_vs_actual_pct, best_estimate_usd, confidence, "
        "current_timestamp FROM out_df"
    )
    con.close()

    high = (out_df['confidence'] == 'HIGH').sum()
    has_synth_rows = out_df['synth_revenue'].notna() & (out_df['synth_revenue'] > 0)
    print(f"  ✓ download_triangulation: {len(out_df)} rows, "
          f"{has_synth_rows.sum()} with RPD estimate — "
          f"{high} HIGH confidence, "
          f"{(out_df['confidence'] == 'MEDIUM').sum()} MEDIUM, "
          f"{(out_df['confidence'] == 'LOW').sum()} LOW")
    return out_df


def get_calibration_factors_v2(db_path: str | None = None) -> pd.DataFrame:
    """
    Enhanced calibration model replacing the simple median approach.

    Improvements over v1:
      - Time weighting: recent 6 months get 2× weight vs. older months
      - Outlier exclusion: drop M0-M1 cohort months and |variance| > 200%
      - Rank bucketing (when fact_top_charts is available):
        top10 / top50 / top100 / longtail
      - Outputs 95% confidence intervals via bootstrapped IQR approximation

    Writes to analytics.calibration_factors_v2.
    Returns DataFrame(genre, market, rank_bucket, calibration_factor,
                       sample_size, confidence_interval_low, confidence_interval_high)
    """
    con = _con(db_path)

    # Pull benchmark data joined with cohort month metadata
    ba_df: pd.DataFrame = con.execute("""
        SELECT
            COALESCE(cg.genre, 'Unknown') AS genre,
            ba.country                    AS market,
            ba.month,
            ba.actual_usd,
            ba.st_estimate_usd,
            ba.variance_pct,
            -- cohort period_n: months since ob_date
            DATEDIFF('month', cr.ob_date, ba.month) AS period_n
        FROM analytics.benchmark_accuracy ba
        JOIN dim.dim_company_games cg ON cg.product_code = ba.product_code AND cg.market = ba.country
        JOIN fact.fact_company_revenue cr ON cr.product_code = ba.product_code AND cr.market = ba.country
        WHERE ba.st_estimate_usd > 0 AND ba.actual_usd > 0
    """).df()

    if ba_df.empty:
        print("  ○ calibration_factors_v2: no benchmark data")
        con.close()
        return ba_df

    # Outlier exclusion: drop M0-M1 cohorts and extreme variance
    ba_df = ba_df[
        (ba_df['period_n'].fillna(99) >= 2) &
        (ba_df['variance_pct'].abs() <= 200)
    ].copy()

    if ba_df.empty:
        print("  ○ calibration_factors_v2: all rows filtered as outliers")
        con.close()
        return ba_df

    # Time weighting: months within last 6 months get weight 2, older get weight 1
    cutoff = ba_df['month'].max() - pd.DateOffset(months=6)
    ba_df['month'] = pd.to_datetime(ba_df['month'])
    ba_df['weight'] = ba_df['month'].apply(lambda m: 2.0 if m >= cutoff else 1.0)
    ba_df['calib_ratio'] = ba_df['actual_usd'] / ba_df['st_estimate_usd']

    # Try to attach rank bucket from fact_top_charts
    try:
        rank_df: pd.DataFrame = con.execute("""
            SELECT
                da.unified_app_id,
                tc.country,
                tc.date AS month,
                tc.os,
                MIN(tc.rank) AS best_rank
            FROM fact.fact_top_charts tc
            JOIN dim.dim_apps da ON da.app_id = tc.app_id AND da.country = tc.country AND da.os = tc.os
            WHERE tc.chart_type = 'grossing'
            GROUP BY da.unified_app_id, tc.country, tc.date, tc.os
        """).df()
        has_rank = not rank_df.empty
    except Exception:
        has_rank = False

    def _rank_bucket(rank) -> str:
        if pd.isna(rank):
            return 'longtail'
        if rank <= 10:
            return 'top10'
        if rank <= 50:
            return 'top50'
        if rank <= 100:
            return 'top100'
        return 'longtail'

    if has_rank:
        # Join product_code → unified_app_id via dim_company_games + dim_apps
        game_uid: pd.DataFrame = con.execute("""
            SELECT cg.product_code, cg.market, da.unified_app_id
            FROM dim.dim_company_games cg
            JOIN dim.dim_apps da ON da.unified_app_id = cg.unified_app_id
            WHERE cg.unified_app_id IS NOT NULL
        """).df()
        ba_df = ba_df.merge(game_uid[['product_code', 'market']].rename(columns={'market': 'market_x'}),
                            left_on=['market'], right_on=['market_x'], how='left')
        rank_df['month'] = pd.to_datetime(rank_df['month'])
        ba_df['month_dt'] = pd.to_datetime(ba_df['month'])
        ba_df = ba_df.merge(rank_df[['unified_app_id', 'country', 'month', 'best_rank']].rename(
                                columns={'country': 'market', 'month': 'month_dt'}),
                            on=['market', 'month_dt'], how='left')
        ba_df['rank_bucket'] = ba_df['best_rank'].apply(_rank_bucket)
    else:
        ba_df['rank_bucket'] = 'all'

    # Weighted calibration factor per genre × market × rank_bucket
    results = []
    for (genre, market, bucket), grp in ba_df.groupby(['genre', 'market', 'rank_bucket']):
        if len(grp) < 2:
            continue
        weights = grp['weight'].values
        ratios = grp['calib_ratio'].values
        w_sum = weights.sum()
        w_mean = (ratios * weights).sum() / w_sum

        # Bootstrap CI approximation via weighted quantiles
        sorted_idx = ratios.argsort()
        sorted_r = ratios[sorted_idx]
        sorted_w = weights[sorted_idx]
        cumw = sorted_w.cumsum() / w_sum
        p025 = sorted_r[cumw >= 0.025][0] if (cumw >= 0.025).any() else sorted_r[0]
        p975 = sorted_r[cumw >= 0.975][0] if (cumw >= 0.975).any() else sorted_r[-1]

        results.append({
            'genre': genre,
            'market': market,
            'rank_bucket': bucket,
            'calibration_factor': round(w_mean, 4),
            'sample_size': len(grp),
            'confidence_interval_low': round(float(p025), 4),
            'confidence_interval_high': round(float(p975), 4),
        })

    out_df = pd.DataFrame(results)
    if out_df.empty:
        print("  ○ calibration_factors_v2: no groups with ≥2 samples")
        con.close()
        return out_df

    con.execute("DELETE FROM analytics.calibration_factors_v2")
    con.execute(
        "INSERT INTO analytics.calibration_factors_v2 "
        "(genre, market, rank_bucket, calibration_factor, sample_size, "
        "confidence_interval_low, confidence_interval_high, computed_at) "
        "SELECT genre, market, rank_bucket, calibration_factor, sample_size, "
        "confidence_interval_low, confidence_interval_high, current_timestamp FROM out_df"
    )
    con.close()

    ci_width = (out_df['confidence_interval_high'] - out_df['confidence_interval_low']).mean()
    print(f"  ✓ calibration_factors_v2: {len(out_df)} genre×market×bucket combinations, "
          f"avg CI width = {ci_width:.2f}x")
    return out_df


def compute_composite_benchmark(db_path: str | None = None) -> pd.DataFrame:
    """
    Combine all available signals into a single weighted benchmark estimate
    with explicit confidence intervals.

    Signals (used when available):
      st_revenue  — ST sales estimate (always present when benchmark rows exist)
      downloads   — synthetic estimate from download_triangulation
      rank        — rank-implied revenue band from top_charts + historical data

    composite_mid = weighted average, weights = 1 / median_abs_variance of each signal
    composite_low/high = apply ±30% band on low-signal rows, ±15% on high-signal rows

    Accuracy tiers: <15% Accurate, <40% Acceptable, >=40% Unreliable (unchanged)
    Writes to analytics.composite_benchmark.
    """
    con = _con(db_path)

    # Base: benchmark_accuracy (ST revenue signal)
    ba_df: pd.DataFrame = con.execute("""
        SELECT
            ba.product_code, ba.product_name, ba.country, ba.month,
            ba.st_estimate_usd, ba.actual_usd AS actual_iap_usd, ba.variance_pct AS st_variance_pct,
            cf.calibration_factor
        FROM analytics.benchmark_accuracy ba
        LEFT JOIN (
            SELECT genre, market, AVG(calibration_factor) AS calibration_factor
            FROM analytics.calibration_factors_v2
            GROUP BY genre, market
        ) cf ON cf.market = ba.country
        JOIN dim.dim_company_games cg ON cg.product_code = ba.product_code AND cg.market = ba.country
    """).df()

    if ba_df.empty:
        print("  ○ composite_benchmark: no benchmark_accuracy data")
        con.close()
        return ba_df

    # Apply calibration factor to ST estimate
    ba_df['st_calibrated_usd'] = (
        ba_df['st_estimate_usd'] * ba_df['calibration_factor'].fillna(1.0)
    )

    # Synthetic estimates from download_triangulation
    try:
        tri_df: pd.DataFrame = con.execute("""
            SELECT unified_app_id, country, month, os, best_estimate_usd AS synth_estimate_usd
            FROM analytics.download_triangulation
        """).df()
        game_map: pd.DataFrame = con.execute("""
            SELECT cg.product_code, cg.market AS country, da.unified_app_id
            FROM dim.dim_company_games cg
            JOIN dim.dim_apps da ON da.unified_app_id = cg.unified_app_id
            WHERE cg.unified_app_id IS NOT NULL
        """).df()
        tri_df = tri_df.merge(game_map[['product_code', 'country', 'unified_app_id']].drop_duplicates(),
                              on=['unified_app_id', 'country'], how='left')
        tri_df = tri_df.groupby(['product_code', 'country', 'month'])['synth_estimate_usd'].mean().reset_index()
        ba_df = ba_df.merge(tri_df, on=['product_code', 'country', 'month'], how='left')
    except Exception:
        ba_df['synth_estimate_usd'] = float('nan')

    # Rank-implied estimates from top charts (use simple rank-tier revenue floors)
    try:
        # Derive rank-tier medians from own benchmark data
        rank_df: pd.DataFrame = con.execute("""
            SELECT
                da.unified_app_id, tc.country, tc.date AS month,
                MIN(tc.rank) AS best_rank
            FROM fact.fact_top_charts tc
            JOIN dim.dim_apps da ON da.app_id = tc.app_id AND da.country = tc.country AND da.os = tc.os
            WHERE tc.chart_type = 'grossing'
            GROUP BY da.unified_app_id, tc.country, tc.date
        """).df()
        game_map2: pd.DataFrame = con.execute("""
            SELECT cg.product_code, cg.market AS country, da.unified_app_id
            FROM dim.dim_company_games cg
            JOIN dim.dim_apps da ON da.unified_app_id = cg.unified_app_id
            WHERE cg.unified_app_id IS NOT NULL
        """).df()
        rank_df = rank_df.merge(game_map2[['product_code', 'country', 'unified_app_id']].drop_duplicates(),
                                on=['unified_app_id', 'country'], how='left')
        # Join benchmark actuals to build rank → revenue mapping
        rank_act = rank_df.merge(ba_df[['product_code', 'country', 'month', 'actual_iap_usd']],
                                 on=['product_code', 'country', 'month'], how='inner')
        tier_medians = rank_act.groupby(
            rank_act['best_rank'].apply(
                lambda r: 'top10' if r <= 10 else ('top50' if r <= 50 else 'top100')
            )
        )['actual_iap_usd'].median().to_dict()

        def _rank_estimate(rank) -> float | None:
            if pd.isna(rank):
                return None
            bucket = 'top10' if rank <= 10 else ('top50' if rank <= 50 else 'top100')
            return tier_medians.get(bucket)

        rank_df['rank_estimate_usd'] = rank_df['best_rank'].apply(_rank_estimate)
        rank_df2 = rank_df.groupby(['product_code', 'country', 'month'])['rank_estimate_usd'].mean().reset_index()
        ba_df = ba_df.merge(rank_df2, on=['product_code', 'country', 'month'], how='left')
    except Exception:
        ba_df['rank_estimate_usd'] = float('nan')

    # Compute composite estimate
    def _composite(row):
        signals = []
        weights = []
        labels = []
        # ST signal: weight by inverse of historical MAD (~60%)
        if pd.notna(row['st_calibrated_usd']) and row['st_calibrated_usd'] > 0:
            signals.append(row['st_calibrated_usd'])
            weights.append(1 / 0.60)
            labels.append('st_revenue')
        # Synthetic signal: weight by inverse MAD (~80%)
        if pd.notna(row.get('synth_estimate_usd')) and row.get('synth_estimate_usd', 0) > 0:
            signals.append(row['synth_estimate_usd'])
            weights.append(1 / 0.80)
            labels.append('downloads')
        # Rank signal: weight by inverse MAD (~50% when rank is known)
        if pd.notna(row.get('rank_estimate_usd')) and row.get('rank_estimate_usd', 0) > 0:
            signals.append(row['rank_estimate_usd'])
            weights.append(1 / 0.50)
            labels.append('rank')

        if not signals:
            return pd.Series({'composite_mid_usd': None, 'composite_low_usd': None,
                               'composite_high_usd': None, 'confidence_level': 'LOW',
                               'signals_used': ''})

        w_total = sum(weights)
        mid = sum(s * w for s, w in zip(signals, weights)) / w_total
        band = 0.15 if len(signals) >= 3 else (0.20 if len(signals) == 2 else 0.30)
        conf = 'HIGH' if len(signals) >= 3 else ('MEDIUM' if len(signals) == 2 else 'LOW')

        return pd.Series({
            'composite_mid_usd': round(mid, 2),
            'composite_low_usd': round(mid * (1 - band), 2),
            'composite_high_usd': round(mid * (1 + band), 2),
            'confidence_level': conf,
            'signals_used': ','.join(labels),
        })

    composite_cols = ba_df.apply(_composite, axis=1)
    ba_df = pd.concat([ba_df, composite_cols], axis=1)

    # Composite variance vs actuals
    ba_df['composite_variance_pct'] = (
        (ba_df['composite_mid_usd'] - ba_df['actual_iap_usd'])
        / ba_df['actual_iap_usd'].replace(0, float('nan')) * 100
    ).round(1)

    def _tier(pct) -> str:
        if pd.isna(pct):
            return 'Unknown'
        p = abs(pct)
        if p < 15:
            return 'Accurate'
        if p < 40:
            return 'Acceptable'
        return 'Unreliable'

    ba_df['accuracy_tier'] = ba_df['composite_variance_pct'].apply(_tier)

    out_cols = ['product_code', 'product_name', 'country', 'month',
                'st_estimate_usd', 'synth_estimate_usd', 'rank_estimate_usd',
                'composite_mid_usd', 'composite_low_usd', 'composite_high_usd',
                'actual_iap_usd', 'composite_variance_pct', 'accuracy_tier',
                'confidence_level', 'signals_used']
    out_df = ba_df[[c for c in out_cols if c in ba_df.columns]].copy()

    con.execute("DELETE FROM analytics.composite_benchmark")
    col_list = ', '.join(out_df.columns)
    con.execute(
        f"INSERT INTO analytics.composite_benchmark ({col_list}, computed_at) "
        f"SELECT {col_list}, current_timestamp FROM out_df"
    )
    con.close()

    accurate = (out_df['accuracy_tier'] == 'Accurate').sum()
    acceptable = (out_df['accuracy_tier'] == 'Acceptable').sum()
    unreliable = (out_df['accuracy_tier'] == 'Unreliable').sum()
    total = len(out_df)
    print(f"  ✓ composite_benchmark: {total} rows — "
          f"{accurate} Accurate ({accurate/total*100:.1f}%), "
          f"{acceptable} Acceptable, "
          f"{unreliable} Unreliable ({unreliable/total*100:.1f}%)")
    return out_df


# ────────────────────────────────────────────────────────
# TASK 9: PER-GAME OVERRIDE UTILITIES
# ────────────────────────────────────────────────────────

def investigate_game_mapping(product_name: str, db_path: str | None = None) -> pd.DataFrame:
    """
    Diagnostic: show benchmark variance per month for a specific game,
    alongside the ST app it's mapped to (unified_app_id, app name).
    Useful for spotting anomalous revenue spikes that indicate bad mappings
    or months where ST data is wrong.

    Returns DataFrame(month, st_estimate_usd, actual_usd, variance_pct, accuracy_tier).
    """
    con = _con(db_path)
    df: pd.DataFrame = con.execute("""
        SELECT
            ba.month,
            cg.unified_app_id,
            da.name                AS st_app_name,
            ba.st_estimate_usd,
            ba.actual_usd,
            ba.variance_pct,
            ba.accuracy_tier,
            ba.iap_pct
        FROM analytics.benchmark_accuracy ba
        JOIN dim.dim_company_games cg
          ON cg.product_code = ba.product_code AND cg.market = ba.country
        LEFT JOIN dim.dim_apps da
          ON da.unified_app_id = cg.unified_app_id
        WHERE ba.product_name = ?
        ORDER BY ba.month
    """, [product_name]).df()

    if df.empty:
        print(f"  ○ No benchmark rows found for '{product_name}'")
    else:
        print(f"\n  Game: {product_name}")
        print(f"  ST mapping: {df['unified_app_id'].iloc[0]} → '{df['st_app_name'].iloc[0]}'")
        print(f"  {len(df)} months | variance range: "
              f"{df['variance_pct'].min():.1f}% to {df['variance_pct'].max():.1f}%")
        unreliable = (df['accuracy_tier'] == 'Unreliable').sum()
        print(f"  Unreliable: {unreliable}/{len(df)} months")
        print(df[['month', 'st_estimate_usd', 'actual_usd', 'variance_pct',
                   'accuracy_tier']].to_string(index=False))
    con.close()
    return df


def set_benchmark_cutoff(product_name: str, market: str, valid_to: str | None,
                         db_path: str | None = None) -> None:
    """
    Set (or clear) a benchmark_valid_to cutoff date for a product×market.
    Rows with calendar_month > valid_to are excluded from benchmark_accuracy.

    Use when a game's ST mapping becomes invalid after a specific date
    (e.g., product discontinued, ownership transferred, game rebranded).

    valid_to format: 'YYYY-MM-DD'. Pass None to clear the cutoff.
    """
    con = _con(db_path)
    rows = con.execute("""
        SELECT COUNT(*) FROM dim.dim_company_games
        WHERE product_name = ? AND market = ?
    """, [product_name, market]).fetchone()[0]

    if rows == 0:
        print(f"  ✗ No game found: '{product_name}' / {market}")
        con.close()
        return

    con.execute("""
        UPDATE dim.dim_company_games
        SET benchmark_valid_to = ?
        WHERE product_name = ? AND market = ?
    """, [valid_to, product_name, market])
    con.close()

    if valid_to is None:
        print(f"  ✓ Cleared benchmark_valid_to for '{product_name}' / {market}")
    else:
        print(f"  ✓ Set benchmark_valid_to = {valid_to} for '{product_name}' / {market}")


def set_game_iap_override(product_name: str, market: str, iap_pct: float | None,
                           db_path: str | None = None) -> None:
    """
    Set (or clear) a per-game IAP% override for a specific product×market.
    Use this when a game's ST revenue systematically over/under-estimates
    due to unusual payment mix or wrong mapping.

    iap_pct=None clears the override (falls back to config default).
    """
    con = _con(db_path)
    rows = con.execute("""
        SELECT COUNT(*) FROM dim.dim_company_games
        WHERE product_name = ? AND market = ?
    """, [product_name, market]).fetchone()[0]

    if rows == 0:
        print(f"  ✗ No game found: '{product_name}' / {market}")
        con.close()
        return

    con.execute("""
        UPDATE dim.dim_company_games
        SET iap_pct_override = ?
        WHERE product_name = ? AND market = ?
    """, [iap_pct, product_name, market])
    con.close()

    if iap_pct is None:
        print(f"  ✓ Cleared iap_pct_override for '{product_name}' / {market}")
    else:
        print(f"  ✓ Set iap_pct_override = {iap_pct:.4f} ({iap_pct:.2%}) "
              f"for '{product_name}' / {market}")


def investigate_high_variance_genres(db_path: str | None = None) -> pd.DataFrame:
    """
    T13: Print per-game breakdown for genres with median |variance| > 60%.
    Helps identify bad mappings or games that need iap_pct_override / date cutoffs.
    Returns DataFrame sorted by variance_pct descending.
    """
    con = _con(db_path)
    df: pd.DataFrame = con.execute("""
        SELECT
            ba.product_name,
            ba.country,
            cg.genre,
            COUNT(*)                                  AS months,
            ROUND(MEDIAN(ABS(ba.variance_pct)), 1)    AS med_abs_var_pct,
            ROUND(MEDIAN(ba.variance_pct), 1)         AS med_var_pct,
            SUM(CASE WHEN ba.accuracy_tier = 'Unreliable' THEN 1 ELSE 0 END) AS unreliable,
            SUM(CASE WHEN ba.accuracy_tier = 'Accurate'   THEN 1 ELSE 0 END) AS accurate,
            cg.unified_app_id
        FROM analytics.benchmark_accuracy ba
        JOIN dim.dim_company_games cg
          ON cg.product_code = ba.product_code AND cg.market = ba.country
        GROUP BY ba.product_name, ba.country, cg.genre, cg.unified_app_id
        HAVING MEDIAN(ABS(ba.variance_pct)) > 60
        ORDER BY med_abs_var_pct DESC
    """).df()
    con.close()

    if df.empty:
        print("  ✓ No genres with median |variance| > 60%")
        return df

    print(f"\n  High-variance games (med |var| > 60%): {len(df)} games")
    print(df[['product_name', 'country', 'genre', 'months',
               'med_abs_var_pct', 'med_var_pct', 'unreliable', 'accurate',
               'unified_app_id']].to_string(index=False))

    # Flag games with 100% unreliable — likely bad mappings
    all_bad = df[df['accurate'] == 0].copy()
    if not all_bad.empty:
        print(f"\n  → {len(all_bad)} game(s) with 0 accurate months — likely bad mappings:")
        for _, r in all_bad.iterrows():
            print(f"    • {r['product_name']} / {r['country']} "
                  f"({r['months']} months, {r['unreliable']} Unreliable)")
            print(f"      unified_app_id: {r['unified_app_id']}")
    return df


def investigate_ys_singmalay(db_path: str | None = None) -> None:
    """
    Task 9 / T12: Investigate YS Sing-Malay outlier and auto-apply benchmark_valid_to cutoff.

    Pattern: company actual collapses post-2023-03 (~$75) while ST stays ~$1-3K.
    Root cause: product discontinued/transferred; mapped ST app is still active.
    Fix: set benchmark_valid_to = 2023-03-01 to exclude the 9 invalid rows.
    unified_app_id = 64870d70b3ae27253f16c069
    """
    con = _con(db_path)

    ys_df: pd.DataFrame = con.execute("""
        SELECT
            ba.month,
            ba.st_estimate_usd,
            ba.actual_usd,
            ba.variance_pct,
            ba.accuracy_tier,
            cg.benchmark_valid_to
        FROM analytics.benchmark_accuracy ba
        JOIN dim.dim_company_games cg
          ON cg.product_code = ba.product_code AND cg.market = ba.country
        WHERE cg.unified_app_id = '64870d70b3ae27253f16c069'
          AND ba.country = 'Sing-Malay'
        ORDER BY ba.month
    """).df()
    con.close()

    if ys_df.empty:
        print("  ○ YS Sing-Malay: no benchmark rows")
        return

    current_cutoff = ys_df['benchmark_valid_to'].iloc[0]
    median_st = ys_df['st_estimate_usd'].median()
    # Detect discontinuation: company actual drops to <20% of its own median for the first time
    median_actual = ys_df['actual_usd'].median()
    collapsed = ys_df['actual_usd'] < 0.20 * median_actual
    first_collapse = ys_df.loc[collapsed, 'month'].min() if collapsed.any() else None

    print(f"\n  YS Sing-Malay | {len(ys_df)} months | median ST: ${median_st:,.0f}")
    print(f"  Current benchmark_valid_to: {current_cutoff}")
    print(f"  Detected company-actual collapse from: {first_collapse}")
    print(ys_df[['month', 'st_estimate_usd', 'actual_usd',
                  'variance_pct', 'accuracy_tier']].to_string(index=False))

    # Auto-apply cutoff: one month before first detected collapse
    # pd.isna covers both None and NaT (column returns NaT for NULL DATE values)
    if first_collapse and pd.isna(current_cutoff):
        cutoff_date = (pd.Timestamp(first_collapse) - pd.DateOffset(months=1)).date()
        print(f"\n  → Auto-applying benchmark_valid_to = {cutoff_date} (month before collapse)")
        set_benchmark_cutoff("YS", "Sing-Malay", str(cutoff_date), db_path)
    elif not pd.isna(current_cutoff):
        print(f"  ✓ benchmark_valid_to already set to {current_cutoff}")


# ────────────────────────────────────────────────────────
# T15: APPLE RSS MAPPING VALIDATOR
# ────────────────────────────────────────────────────────

def validate_mappings_via_apple_charts(db_path: str | None = None) -> pd.DataFrame:
    """
    Cross-reference company game mappings against Apple's top-grossing chart.

    For each company game mapped to an ST unified_app_id:
      - Look up the iOS app_id in fact_apple_public_charts
      - If found, compare the Apple chart app name to what we expect
      - Flag as MISMATCH if the Apple name differs significantly from the company game name

    Catches cases like KON → "Mobile Legends: Bang Bang" where the ST mapping
    is pointing to the wrong (popular) app.

    Returns DataFrame of flagged mismatches.
    """
    con = _con(db_path)

    df: pd.DataFrame = con.execute("""
        SELECT DISTINCT
            cg.product_name,
            cg.market,
            cg.unified_app_id,
            da.app_id,
            da.name                AS st_app_name,
            ac.name                AS apple_chart_name,
            ac.rank                AS apple_rank,
            ac.country             AS apple_country
        FROM dim.dim_company_games cg
        JOIN dim.dim_apps da
          ON da.unified_app_id = cg.unified_app_id AND da.os = 'ios'
        JOIN fact.fact_apple_public_charts ac
          ON ac.app_id = da.app_id
        WHERE cg.unified_app_id IS NOT NULL
        ORDER BY cg.product_name, ac.country
    """).df()
    con.close()

    if df.empty:
        print("  ○ validate_mappings: no company games found in Apple charts "
              "(most SEA MMORPGs route through web payments — expected)")
        return df

    # Flag potential mismatches: Apple chart name doesn't share any token with product name
    def _is_mismatch(row) -> bool:
        product_tokens = set(row['product_name'].lower().split())
        apple_tokens = set(row['apple_chart_name'].lower().split())
        # Remove common noise words
        noise = {'mobile', 'game', 'vng', 'sea', 'mini', 'm', 'the', 'of', 'a'}
        product_tokens -= noise
        apple_tokens -= noise
        return len(product_tokens & apple_tokens) == 0

    df['mismatch_flag'] = df.apply(_is_mismatch, axis=1)
    mismatches = df[df['mismatch_flag']].copy()

    print(f"\n  Apple chart mapping validation:")
    print(f"  {len(df['unified_app_id'].unique())} company games appear in Apple top-100")
    if mismatches.empty:
        print("  ✓ No obvious name mismatches detected")
    else:
        print(f"  ⚠ {len(mismatches['product_name'].unique())} potential false positives (name mismatch):")
        for _, r in mismatches.drop_duplicates('product_name').iterrows():
            print(f"    • {r['product_name']} ({r['market']}) → "
                  f"Apple: '{r['apple_chart_name']}' [rank {r['apple_rank']} in {r['apple_country']}]")
            print(f"      unified_app_id: {r['unified_app_id']}")
    return mismatches


# ────────────────────────────────────────────────────────
# T16: GENRE AUTO-ASSIGNMENT
# ────────────────────────────────────────────────────────

# Curated name → genre lookup for known games in the company portfolio.
# Priority: non-MMORPG genres first (those have distinct IAP% configs).
# Names are matched case-insensitively as substrings against product_name.
_GENRE_NAME_LOOKUP: list[tuple[str, str]] = [
    # Turn-based RPG — App Store IAP-dominant, <10% variance when correctly mapped
    ("Lethe Record",         "Turn-based RPG"),
    ("Azure Fantasy",        "Turn-based RPG"),
    ("Crown of Heroes",      "Turn-based RPG"),
    ("The Play of Genesis",  "Turn-based RPG"),
    ("Samurai Spirit",       "Turn-based RPG"),  # SNK fighter — closer to Turn-based RPG than Shoot
    ("SNK All Star",         "Turn-based RPG"),  # SNK collection RPG
    ("Dynasty Warrior",      "Turn-based RPG"),  # action but monetises like Turn-based RPG
    # Shoot 'em Up — has VN-specific config (0.07%)
    ("Call of Duty",         "Shoot 'em Up"),
    ("Metal Slug",           "Shoot 'em Up"),
    # Idle RPG — distinct IAP% in config
    ("OMG3",                 "Idle RPG"),        # Onmyoji — gacha/idle collect RPG
    ("Lucky Fishing",        "Idle RPG"),        # idle fishing genre
    # Tycoon / Crafting
    ("Hello Cafe",           "Tycoon / Crafting"),
    ("Plant War",            "Tycoon / Crafting"),
    # MOBA
    ("KON",                  "MOBA"),
    ("AutoChess",            "MOBA"),            # closest genre in config to strategy
    # MMORPG — Vietnamese MMORPGs (catch-all for remaining)
    ("Cloud Song",           "MMORPG"),
    ("Ghost Story",          "MMORPG"),
    ("Perfect World",        "MMORPG"),
    ("Ngọa Long",            "MMORPG"),
    ("Tan Thien Long",       "MMORPG"),
    ("Tay Du",               "MMORPG"),
    ("Kiem Vu",              "MMORPG"),
    ("JX1M",                 "MMORPG"),
    ("MU Angel",             "MMORPG"),
    ("Pure 3Q",              "MMORPG"),
    ("3Q Phản Kích",         "MMORPG"),
    ("Thiếu Niên",           "MMORPG"),
    ("Tuyết Ưng",            "MMORPG"),
    ("Nhất Mộng",            "MMORPG"),
    ("Thần Điêu",            "MMORPG"),
    ("DauLa",                "MMORPG"),
    ("Ngoalong",             "MMORPG"),
    ("Đại Chiến",            "MMORPG"),
    ("Đại Đạo",              "MMORPG"),
    ("TuTienLenLuon",        "MMORPG"),
    ("Dũng Giả",             "MMORPG"),
    ("360mobi",              "MMORPG"),
    ("JX1",                  "MMORPG"),
    # Remaining 13 unmatched — added after first auto-assign run
    ("YS",                   "Platformer / Runner"),  # same game as YS Sing-Malay
    ("CheonSangBi",          "MMORPG"),               # Korean MMORPG (천상비)
    ("Justice",              "MMORPG"),               # Justice Mobile — CN MMORPG
    ("Phong Than",           "MMORPG"),               # Vietnamese MMORPG
    ("Thiên Khởi",           "MMORPG"),               # Vietnamese MMORPG
    ("Tân Tiếu Ngạo",        "MMORPG"),               # Vietnamese MMORPG (Laughing Proud Wanderer)
    ("KTO",                  "MMORPG"),               # abbreviated Vietnamese MMORPG
    ("City Sun",             "Tycoon / Crafting"),    # city building / simulation
]


def auto_assign_genres(dry_run: bool = False, db_path: str | None = None) -> pd.DataFrame:
    """
    Assign genres to company games with NULL genre using _GENRE_NAME_LOOKUP.
    Matches product_name case-insensitively as a substring.

    dry_run=True: prints assignments without writing to DB.
    Returns DataFrame of games that would be updated.
    """
    con = _con(db_path)
    null_genre: pd.DataFrame = con.execute("""
        SELECT id, product_name, market, unified_app_id, genre
        FROM dim.dim_company_games
        WHERE genre IS NULL
        ORDER BY product_name, market
    """).df()
    con.close()

    if null_genre.empty:
        print("  ✓ auto_assign_genres: all games already have genres")
        return null_genre

    assignments = []
    for _, row in null_genre.iterrows():
        name_lower = str(row['product_name']).lower()
        assigned = None
        for pattern, genre in _GENRE_NAME_LOOKUP:
            if pattern.lower() in name_lower:
                assigned = genre
                break
        if assigned:
            assignments.append({
                'id': row['id'], 'product_name': row['product_name'],
                'market': row['market'], 'new_genre': assigned,
            })

    result_df = pd.DataFrame(assignments) if assignments else pd.DataFrame(
        columns=['id', 'product_name', 'market', 'new_genre'])

    unmatched = null_genre[~null_genre['id'].isin(result_df['id'] if not result_df.empty else [])].copy()

    print(f"\n  Genre auto-assignment: {len(null_genre)} NULL-genre games")
    print(f"  Matched: {len(result_df)} | Unmatched: {len(unmatched)}")

    if not result_df.empty:
        print("\n  Assignments:")
        for _, r in result_df.iterrows():
            print(f"    {r['product_name']} / {r['market']} → {r['new_genre']}")

    if not unmatched.empty:
        print("\n  Unmatched (will keep NULL genre):")
        for _, r in unmatched.iterrows():
            print(f"    {r['product_name']} / {r['market']}")

    if not dry_run and not result_df.empty:
        con = _con(db_path)
        for _, r in result_df.iterrows():
            con.execute(
                "UPDATE dim.dim_company_games SET genre = ? WHERE id = ?",
                [r['new_genre'], int(r['id'])]
            )
        con.close()
        print(f"\n  ✓ auto_assign_genres: {len(result_df)} genres written to DB")

    return result_df


# ────────────────────────────────────────────────────────
# PHASE 7: MARKET SHARE & COMPETITIVE POSITION
# ────────────────────────────────────────────────────────

# Maps company genre labels (lowercase) → ST dim_apps.category_id values.
# Used to compute genre-level denominators from fact_market_insights.
_GENRE_TO_ST_CATEGORY: dict[str, str] = {
    'mmorpg':               'RPG',
    'turn-based rpg':       'RPG',
    'idle rpg':             'RPG',
    'squad rpg':            'RPG',
    'idler':                'Simulation',
    'moba':                 'Action',
    "shoot 'em up":         'Shooter',
    'shooting':             'Shooter',
    'artillery shooter':    'Shooter',
    'fps / 3ps':            'Shooter',
    'platformer / runner':  'Arcade',
    'other arcade':         'Arcade',
    'tycoon / crafting':    'Simulation',
    'battle royale':        'Action',
    'team battle':          'Action',
    'avatar life':          'Social',
    'sports manager':       'Sports',
}


def compute_market_share(db_path: str | None = None) -> pd.DataFrame:
    """
    Compute each company game's % share of its genre's total IAP-equivalent market.

    Numerator:   company gross × IAP_PCT per calendar month (all 155 games).
    Denominator: sum of fact_market_insights revenue for apps in matching ST
                 category per market per calendar month (ST flat-file universe).
    Store share: company_iap / fact_store_summary total (where available, 5-month window).

    Writes to analytics.market_share.
    """
    con = _con(db_path)

    # ── Step 1: Unpivot all company revenue to calendar months ───────────────
    rev_wide = con.execute("""
        SELECT cr.product_code, cr.market, cr.ob_date,
               cg.product_name, cg.genre, cg.iap_pct_override,
               cr.m0,  cr.m1,  cr.m2,  cr.m3,  cr.m4,  cr.m5,
               cr.m6,  cr.m7,  cr.m8,  cr.m9,  cr.m10, cr.m11,
               cr.m12, cr.m13, cr.m14, cr.m15, cr.m16, cr.m17,
               cr.m18, cr.m19, cr.m20, cr.m21, cr.m22, cr.m23,
               cr.m24, cr.m25, cr.m26, cr.m27, cr.m28, cr.m29,
               cr.m30, cr.m31, cr.m32, cr.m33, cr.m34, cr.m35, cr.m36
        FROM fact.fact_company_revenue cr
        JOIN dim.dim_company_games cg
          ON cg.product_code = cr.product_code AND cg.market = cr.market
        WHERE cr.ob_date IS NOT NULL
    """).df()

    if rev_wide.empty:
        print("  ○ compute_market_share: no company revenue found")
        con.close()
        return rev_wide

    id_cols = ['product_code', 'product_name', 'market', 'genre', 'iap_pct_override', 'ob_date']
    m_cols = [f'm{i}' for i in range(37) if f'm{i}' in rev_wide.columns]
    rev_long = rev_wide.melt(id_vars=id_cols, value_vars=m_cols,
                              var_name='period_label', value_name='gross_usd')
    rev_long = rev_long[rev_long['gross_usd'].notna() & (rev_long['gross_usd'] > 0)].copy()
    rev_long['period_n'] = rev_long['period_label'].str[1:].astype(int)
    rev_long['ob_date'] = pd.to_datetime(rev_long['ob_date'], errors='coerce')
    rev_long = rev_long[rev_long['ob_date'].notna()].copy()
    rev_long['calendar_month'] = (
        rev_long['ob_date'].dt.to_period('M') + rev_long['period_n']
    ).dt.to_timestamp().dt.date

    # ── Step 2: Apply IAP_PCT per game × market ───────────────────────────────
    def _iap(row) -> float:
        if pd.notna(row['iap_pct_override']) and row['iap_pct_override'] > 0:
            return float(row['iap_pct_override'])
        return _get_iap_pct(row['genre'], row['market'])

    rev_long['iap_pct'] = rev_long.apply(_iap, axis=1)
    rev_long['company_iap_usd'] = rev_long['gross_usd'] * rev_long['iap_pct']

    # ── Step 3: Genre → ST category mapping ──────────────────────────────────
    rev_long['st_category'] = (
        rev_long['genre'].str.lower().str.strip().map(_GENRE_TO_ST_CATEGORY)
    )

    # ── Step 4: Genre IAP totals from fact_market_insights + dim_apps ────────
    # Pre-build for all SEA countries at once; we'll aggregate by market after.
    genre_totals = con.execute("""
        SELECT da.category_id  AS st_category,
               mi.country,
               mi.date         AS month,
               SUM(mi.revenue_cents) / 100.0 AS genre_iap_usd
        FROM fact.fact_market_insights mi
        JOIN dim.dim_apps da ON mi.app_id = da.app_id
        WHERE mi.country IN ('VN','TH','ID','PH','SG','MY','TW','HK','WW')
          AND da.category_id IS NOT NULL
          AND da.category_id NOT IN ('', 'NaN')
        GROUP BY 1, 2, 3
    """).df()
    genre_totals['month'] = pd.to_datetime(genre_totals['month']).dt.date

    # ── Step 5: Store IAP totals from fact_store_summary ─────────────────────
    store_totals = con.execute("""
        SELECT country, date AS month, revenue_cents / 100.0 AS store_iap_usd
        FROM fact.fact_store_summary
    """).df()
    store_totals['month'] = pd.to_datetime(store_totals['month']).dt.date

    # ── Step 6: Join company revenue to genre and store denominators ──────────
    rows_out = []
    for _, row in rev_long.iterrows():
        market   = row['market']
        st_cat   = row['st_category']
        month    = row['calendar_month']
        countries = COMPANY_MARKET_ST_COUNTRIES.get(market, [])

        # Genre denominator: sum across all constituent countries
        genre_iap = None
        if st_cat and countries:
            subset = genre_totals[
                (genre_totals['st_category'] == st_cat) &
                (genre_totals['country'].isin(countries)) &
                (genre_totals['month'] == month)
            ]
            if not subset.empty:
                genre_iap = float(subset['genre_iap_usd'].sum())

        # Store denominator: sum across constituent countries
        store_iap = None
        if countries:
            sub_store = store_totals[
                (store_totals['country'].isin(countries)) &
                (store_totals['month'] == month)
            ]
            if not sub_store.empty:
                store_iap = float(sub_store['store_iap_usd'].sum())

        rows_out.append({
            'product_code':     row['product_code'],
            'product_name':     row['product_name'],
            'genre':            row['genre'],
            'market':           market,
            'month':            month,
            'company_gross_usd': float(row['gross_usd']),
            'company_iap_usd':  float(row['company_iap_usd']),
            'iap_pct':          float(row['iap_pct']),
            'genre_iap_usd':    genre_iap,
            'market_share_pct': (float(row['company_iap_usd']) / genre_iap * 100)
                                 if genre_iap and genre_iap > 0 else None,
            'store_iap_usd':    store_iap,
            'store_share_pct':  (float(row['company_iap_usd']) / store_iap * 100)
                                 if store_iap and store_iap > 0 else None,
        })

    if not rows_out:
        print("  ○ compute_market_share: no rows produced")
        con.close()
        return pd.DataFrame()

    df = pd.DataFrame(rows_out)

    # ── Step 7: Portfolio rank within genre × market × month ─────────────────
    df['portfolio_rank'] = (
        df.groupby(['genre', 'market', 'month'])['company_iap_usd']
          .rank(method='dense', ascending=False)
          .astype(int)
    )

    # ── Step 8: 3-month trend (pp change in market_share_pct) ─────────────────
    df = df.sort_values(['product_code', 'market', 'month']).reset_index(drop=True)
    df['_grp'] = df['product_code'] + '|' + df['market']
    df['trend_3m_pp'] = (
        df.groupby('_grp', sort=False)['market_share_pct']
          .transform(lambda s: s - s.shift(3))
    )
    df = df.drop(columns=['_grp'])

    # ── Step 9: Upsert into analytics.market_share ───────────────────────────
    out_cols = [
        'product_code', 'product_name', 'genre', 'market', 'month',
        'company_gross_usd', 'company_iap_usd', 'iap_pct',
        'genre_iap_usd', 'market_share_pct',
        'store_iap_usd', 'store_share_pct',
        'portfolio_rank', 'trend_3m_pp',
    ]
    df_out = df[out_cols].copy()
    con.register('_ms_in', df_out)
    con.execute("DELETE FROM analytics.market_share")
    con.execute("""
        INSERT INTO analytics.market_share
            (product_code, product_name, genre, market, month,
             company_gross_usd, company_iap_usd, iap_pct,
             genre_iap_usd, market_share_pct,
             store_iap_usd, store_share_pct,
             portfolio_rank, trend_3m_pp)
        SELECT product_code, product_name, genre, market, month,
               company_gross_usd, company_iap_usd, iap_pct,
               genre_iap_usd, market_share_pct,
               store_iap_usd, store_share_pct,
               portfolio_rank, trend_3m_pp
        FROM _ms_in
    """)
    con.unregister('_ms_in')

    # Null out implausible shares (>100%) — genre denominator too small for reliable estimate
    con.execute("""
        UPDATE analytics.market_share
        SET market_share_pct = NULL
        WHERE market_share_pct > 100
    """)

    n = con.execute("SELECT COUNT(*) FROM analytics.market_share").fetchone()[0]
    games_with_share = con.execute(
        "SELECT COUNT(*) FROM analytics.market_share WHERE market_share_pct IS NOT NULL"
    ).fetchone()[0]
    print(f"  ✓ market_share: {n:,} rows ({games_with_share:,} with genre denominator)")
    con.close()
    return df_out


def compute_portfolio_summary(db_path: str | None = None) -> pd.DataFrame:
    """
    Aggregate company portfolio by genre × market × month.

    Summarises total company IAP, portfolio market share, game count,
    and the top-revenue game per cell. Reads from analytics.market_share.

    Writes to analytics.portfolio_summary.
    """
    con = _con(db_path)

    check = con.execute("SELECT COUNT(*) FROM analytics.market_share").fetchone()[0]
    if check == 0:
        print("  ○ portfolio_summary: market_share table empty — run compute_market_share() first")
        con.close()
        return pd.DataFrame()

    df = con.execute("""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY genre, market, month
                       ORDER BY company_iap_usd DESC
                   ) AS rn
            FROM analytics.market_share
        ),
        top_game AS (
            SELECT genre, market, month,
                   product_code AS top_game_code,
                   product_name AS top_game_name,
                   market_share_pct AS top_game_share_pct
            FROM ranked WHERE rn = 1
        ),
        agg AS (
            SELECT
                genre,
                market,
                month,
                SUM(company_gross_usd)       AS total_company_gross_usd,
                SUM(company_iap_usd)         AS total_company_iap_usd,
                MAX(genre_iap_usd)           AS genre_iap_usd,
                CASE WHEN MAX(genre_iap_usd) > 0
                     THEN SUM(company_iap_usd) / MAX(genre_iap_usd) * 100
                     ELSE NULL END            AS portfolio_share_pct,
                COUNT(*)                     AS game_count
            FROM analytics.market_share
            GROUP BY 1, 2, 3
        )
        SELECT a.*, t.top_game_code, t.top_game_name, t.top_game_share_pct
        FROM agg a
        LEFT JOIN top_game t USING (genre, market, month)
        ORDER BY a.genre, a.market, a.month
    """).df()

    if df.empty:
        con.close()
        return df

    # 3-month trend on portfolio_share_pct
    df = df.sort_values(['genre', 'market', 'month'])
    df['trend_3m_pp'] = (
        df.groupby(['genre', 'market'])['portfolio_share_pct']
          .transform(lambda s: s - s.shift(3))
    )

    con.register('_ps_in', df)
    con.execute("DELETE FROM analytics.portfolio_summary")
    con.execute("""
        INSERT INTO analytics.portfolio_summary
            (genre, market, month,
             total_company_gross_usd, total_company_iap_usd,
             genre_iap_usd, portfolio_share_pct, game_count,
             top_game_code, top_game_name, top_game_share_pct,
             trend_3m_pp)
        SELECT genre, market, month,
               total_company_gross_usd, total_company_iap_usd,
               genre_iap_usd, portfolio_share_pct, game_count,
               top_game_code, top_game_name, top_game_share_pct,
               trend_3m_pp
        FROM _ps_in
    """)
    con.unregister('_ps_in')

    n = con.execute("SELECT COUNT(*) FROM analytics.portfolio_summary").fetchone()[0]
    print(f"  ✓ portfolio_summary: {n:,} rows")
    con.close()
    return df


def compute_genre_concentration(db_path: str | None = None) -> pd.DataFrame:
    """
    Compute genre-level concentration metrics from fact_market_insights.

    For each ST category × country × month, ranks apps by revenue and computes:
      - top1/5/10 share of total category revenue
      - HHI proxy (sum of squared shares for top 10)
      - concentration tier: HIGH (>0.25), MEDIUM (>0.10), LOW

    Uses fact_market_insights + dim_apps (deduplicated by app_id).
    Writes to analytics.genre_concentration.
    """
    con = _con(db_path)

    # Build concentration from MI revenue, joining dim_apps for category.
    # Deduplicate dim_apps: one category_id per app_id (take first non-null).
    df = con.execute("""
        WITH app_cat AS (
            SELECT app_id,
                   FIRST(category_id) AS st_category,
                   FIRST(name)        AS app_name
            FROM dim.dim_apps
            WHERE category_id IS NOT NULL
              AND category_id NOT IN ('', 'NaN', '0')
            GROUP BY app_id
        ),
        rev AS (
            SELECT ac.st_category,
                   mi.country,
                   mi.date       AS month,
                   mi.app_id,
                   ac.app_name,
                   SUM(mi.revenue_cents) / 100.0 AS rev_usd
            FROM fact.fact_market_insights mi
            JOIN app_cat ac ON mi.app_id = ac.app_id
            WHERE mi.revenue_cents > 0
            GROUP BY 1, 2, 3, 4, 5
        ),
        genre_total AS (
            SELECT st_category, country, month,
                   SUM(rev_usd) AS genre_iap_usd,
                   COUNT(*)     AS app_count
            FROM rev
            GROUP BY 1, 2, 3
        ),
        ranked AS (
            SELECT r.*,
                   gt.genre_iap_usd,
                   gt.app_count,
                   r.rev_usd / gt.genre_iap_usd AS share,
                   ROW_NUMBER() OVER (
                       PARTITION BY r.st_category, r.country, r.month
                       ORDER BY r.rev_usd DESC
                   ) AS rk
            FROM rev r
            JOIN genre_total gt
              ON r.st_category = gt.st_category
             AND r.country     = gt.country
             AND r.month       = gt.month
            WHERE gt.genre_iap_usd > 0
        )
        SELECT
            st_category,
            country,
            month,
            ANY_VALUE(genre_iap_usd)  AS genre_iap_usd,
            ANY_VALUE(app_count)      AS app_count,
            MAX(CASE WHEN rk = 1 THEN share END)                        AS top1_share,
            SUM(CASE WHEN rk <= 5  THEN share ELSE 0 END)               AS top5_share,
            SUM(CASE WHEN rk <= 10 THEN share ELSE 0 END)               AS top10_share,
            SUM(CASE WHEN rk <= 10 THEN share * share ELSE 0 END)       AS hhi_top10,
            MAX(CASE WHEN rk = 1 THEN app_id END)                       AS top1_app_id,
            MAX(CASE WHEN rk = 1 THEN app_name END)                     AS top1_app_name
        FROM ranked
        GROUP BY st_category, country, month
        ORDER BY st_category, country, month
    """).df()

    if df.empty:
        print("  ○ genre_concentration: no data produced")
        con.close()
        return df

    # Convert shares to percentages and assign tier
    for col in ['top1_share', 'top5_share', 'top10_share']:
        df[col + '_pct'] = df[col] * 100
    df['concentration_tier'] = df['hhi_top10'].apply(
        lambda h: 'HIGH' if h > 0.25 else ('MEDIUM' if h > 0.10 else 'LOW')
    )

    # Upsert
    out = df[['st_category', 'country', 'month', 'genre_iap_usd',
              'top1_share_pct', 'top5_share_pct', 'top10_share_pct',
              'hhi_top10', 'concentration_tier', 'app_count',
              'top1_app_id', 'top1_app_name']].copy()
    con.register('_gc_in', out)
    con.execute("DELETE FROM analytics.genre_concentration")
    con.execute("""
        INSERT INTO analytics.genre_concentration
            (st_category, country, month, genre_iap_usd,
             top1_share_pct, top5_share_pct, top10_share_pct,
             hhi_top10, concentration_tier, app_count,
             top1_app_id, top1_app_name)
        SELECT st_category, country, month, genre_iap_usd,
               top1_share_pct, top5_share_pct, top10_share_pct,
               hhi_top10, concentration_tier, app_count,
               top1_app_id, top1_app_name
        FROM _gc_in
    """)
    con.unregister('_gc_in')

    n = con.execute("SELECT COUNT(*) FROM analytics.genre_concentration").fetchone()[0]
    cats = con.execute(
        "SELECT COUNT(DISTINCT st_category) FROM analytics.genre_concentration"
    ).fetchone()[0]
    countries = con.execute(
        "SELECT COUNT(DISTINCT country) FROM analytics.genre_concentration"
    ).fetchone()[0]
    print(f"  ✓ genre_concentration: {n:,} rows ({cats} categories × {countries} countries)")
    con.close()
    return out


def compute_game_pnl(db_path: str | None = None) -> pd.DataFrame:
    """
    Per-game PnL combining market share position, genre TAM context,
    concentration, forecast, and LTV into a single actionable view.

    Reads from analytics.market_share + genre_concentration + genre_pnl_template
    + revenue_forecast + ltv_model. Writes to analytics.game_pnl.
    """
    con = _con(db_path)

    check = con.execute("SELECT COUNT(*) FROM analytics.market_share").fetchone()[0]
    if check == 0:
        print("  ○ game_pnl: market_share empty — run compute_market_share() first")
        con.close()
        return pd.DataFrame()

    # Build genre → ST category mapping as SQL CASE (escape single quotes)
    genre_case = "CASE LOWER(TRIM(ms.genre))\n"
    for g, cat in _GENRE_TO_ST_CATEGORY.items():
        g_escaped = g.replace("'", "''")
        genre_case += f"            WHEN '{g_escaped}' THEN '{cat}'\n"
    genre_case += "            ELSE NULL END"

    df = con.execute(f"""
        WITH ms_data AS (
            SELECT ms.*,
                   {genre_case} AS st_category
            FROM analytics.market_share ms
        ),
        launch AS (
            SELECT product_code, market, MIN(ob_date) AS launch_date
            FROM fact.fact_company_revenue
            WHERE ob_date IS NOT NULL
            GROUP BY product_code, market
        ),
        -- Map company market → primary ST country for concentration join
        market_country AS (
            SELECT DISTINCT market,
                   CASE market
                       WHEN 'Vietnam'     THEN 'VN'
                       WHEN 'ThaiLand'    THEN 'TH'
                       WHEN 'Philippines' THEN 'PH'
                       WHEN 'Indonesia'   THEN 'ID'
                       WHEN 'Sing-Malay'  THEN 'SG'
                       WHEN 'TW-HK'       THEN 'TW'
                       WHEN 'Global'      THEN 'VN'
                   END AS primary_country
            FROM analytics.market_share
        ),
        gc AS (
            SELECT st_category, country, month,
                   hhi_top10, concentration_tier
            FROM analytics.genre_concentration
        ),
        ltv AS (
            SELECT genre, country,
                   AVG(CASE WHEN period <= 3 THEN ltv_usd END) AS ltv_90
            FROM analytics.ltv_model
            GROUP BY genre, country
        )
        SELECT
            md.product_code,
            md.product_name,
            md.genre,
            md.market,
            md.month            AS report_month,
            md.company_gross_usd,
            md.company_iap_usd,
            md.market_share_pct,
            md.portfolio_rank,
            md.genre_iap_usd    AS genre_tam_usd,
            gc.hhi_top10        AS hhi_score,
            gc.concentration_tier,
            DATEDIFF('month', l.launch_date, md.month) AS months_since_launch,
            md.trend_3m_pp      AS share_trend_3m_pp,
            ltv.ltv_90          AS ltv_90_usd
        FROM ms_data md
        LEFT JOIN market_country mc ON mc.market = md.market
        LEFT JOIN gc ON gc.st_category = md.st_category
            AND gc.country = mc.primary_country
            AND gc.month = md.month
        LEFT JOIN launch l ON l.product_code = md.product_code AND l.market = md.market
        LEFT JOIN ltv ON ltv.genre = md.st_category AND ltv.country = mc.primary_country
    """).df()

    if df.empty:
        print("  ○ game_pnl: no rows produced")
        con.close()
        return df

    # Revenue trend 3m (% change in own gross vs 3 months prior)
    df = df.sort_values(['product_code', 'market', 'report_month'])
    df['revenue_trend_3m'] = (
        df.groupby(['product_code', 'market'])['company_gross_usd']
          .transform(lambda s: (s - s.shift(3)) / s.shift(3).replace(0, float('nan')))
    )

    # TAM growth from genre_pnl_template (latest available)
    tam_growth = con.execute("""
        SELECT genre AS st_category, country,
               AVG(tam_growth_3m) AS tam_growth_3m
        FROM analytics.genre_pnl_template
        WHERE tam_growth_3m IS NOT NULL
        GROUP BY genre, country
    """).df()

    # Map market → primary country for TAM growth join
    market_map = {
        'Vietnam': 'VN', 'ThaiLand': 'TH', 'Philippines': 'PH',
        'Indonesia': 'ID', 'Sing-Malay': 'SG', 'TW-HK': 'TW', 'Global': 'VN'
    }
    df['_primary_country'] = df['market'].map(market_map)
    df['_st_cat'] = df['genre'].str.lower().str.strip().map(_GENRE_TO_ST_CATEGORY)

    if not tam_growth.empty:
        tam_map = tam_growth.set_index(['st_category', 'country'])['tam_growth_3m'].to_dict()
        df['tam_growth_3m'] = df.apply(
            lambda r: tam_map.get((r['_st_cat'], r['_primary_country'])), axis=1
        )
    else:
        df['tam_growth_3m'] = None

    # Join forecast data
    forecast = con.execute("""
        SELECT genre, country, period,
               revenue_mid_usd AS forecast_mid_usd,
               revenue_low_usd AS forecast_low_usd,
               revenue_high_usd AS forecast_high_usd
        FROM analytics.revenue_forecast
    """).df()

    if not forecast.empty:
        fc_map = {}
        for _, r in forecast.iterrows():
            fc_map[(r['genre'], r['country'], int(r['period']))] = (
                r['forecast_mid_usd'], r['forecast_low_usd'], r['forecast_high_usd']
            )
        df['_msl'] = df['months_since_launch'].fillna(-1).astype(int)
        df['forecast_mid_usd'] = df.apply(
            lambda r: fc_map.get((r['genre'], r['market'], r['_msl']), (None,))[0], axis=1
        )
        df['forecast_low_usd'] = df.apply(
            lambda r: fc_map.get((r['genre'], r['market'], r['_msl']), (None, None))[1], axis=1
        )
        df['forecast_high_usd'] = df.apply(
            lambda r: fc_map.get((r['genre'], r['market'], r['_msl']), (None, None, None))[2], axis=1
        )
    else:
        df['forecast_mid_usd'] = None
        df['forecast_low_usd'] = None
        df['forecast_high_usd'] = None

    # Opportunity score per game
    def _game_score(row) -> str:
        share_trend = row.get('share_trend_3m_pp')
        tam_growth = row.get('tam_growth_3m')
        hhi = row.get('hhi_score') or 0
        if (pd.notna(share_trend) and share_trend > 0
                and pd.notna(tam_growth) and tam_growth > 0
                and hhi < 0.25):
            return 'HIGH'
        if ((pd.notna(share_trend) and share_trend < -2)
                or (pd.notna(tam_growth) and tam_growth < -0.05)
                or hhi > 0.50):
            return 'LOW'
        return 'MEDIUM'

    df['opportunity_score'] = df.apply(_game_score, axis=1)

    # Write to analytics.game_pnl
    out_cols = [
        'product_code', 'product_name', 'genre', 'market', 'report_month',
        'company_gross_usd', 'company_iap_usd',
        'market_share_pct', 'portfolio_rank',
        'genre_tam_usd', 'tam_growth_3m', 'hhi_score', 'concentration_tier',
        'forecast_mid_usd', 'forecast_low_usd', 'forecast_high_usd',
        'months_since_launch', 'revenue_trend_3m', 'share_trend_3m_pp',
        'ltv_90_usd', 'opportunity_score',
    ]
    out_df = df[out_cols].copy()

    con.register('_gpnl_in', out_df)
    con.execute("DELETE FROM analytics.game_pnl")
    con.execute(f"""
        INSERT INTO analytics.game_pnl ({', '.join(out_cols)})
        SELECT {', '.join(out_cols)} FROM _gpnl_in
    """)
    con.unregister('_gpnl_in')

    n = len(out_df)
    games = out_df['product_code'].nunique()
    high = (out_df['opportunity_score'] == 'HIGH').sum()
    med = (out_df['opportunity_score'] == 'MEDIUM').sum()
    low = (out_df['opportunity_score'] == 'LOW').sum()
    print(f"  ✓ game_pnl: {n:,} rows ({games} games), "
          f"{high} HIGH / {med} MEDIUM / {low} LOW")
    con.close()
    return out_df


# ────────────────────────────────────────────────────────
# REPORTING
# ────────────────────────────────────────────────────────

def generate_reports(output_dir: str | None = None, db_path: str | None = None) -> str:
    """
    Generate a multi-sheet Excel report from all analytics tables.

    Sheets:
      1. Executive Summary — top games, genre opportunities, key metrics
      2. Game PnL — per-game PnL with opportunity scores (latest 6 months)
      3. Market Share — per-game market share trends
      4. Genre PnL — genre-level TAM, growth, concentration (SEA, latest 12 months)
      5. Genre Concentration — HHI and structure per genre×country
      6. Revenue Forecast — M0-M24 curves per genre×market
      7. Portfolio Summary — genre×market aggregation
      8. Data Quality — mapping coverage and gaps

    Returns path to the generated file.
    """
    from pathlib import Path
    from datetime import datetime

    con = _con(db_path)
    out_dir = Path(output_dir) if output_dir else Path(__file__).parent / 'reports'
    out_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    out_path = out_dir / f'market_intelligence_{timestamp}.xlsx'

    sea_countries = ('VN', 'TH', 'ID', 'PH', 'SG', 'MY', 'TW', 'HK')

    # ── Sheet 1: Executive Summary ──────────────────────────
    exec_summary = con.execute(f"""
        SELECT
            product_name AS "Game",
            market AS "Market",
            genre AS "Genre",
            COUNT(*) FILTER (WHERE opportunity_score = 'HIGH') AS "HIGH Months",
            ROUND(AVG(company_gross_usd)) AS "Avg Monthly Gross ($)",
            ROUND(AVG(market_share_pct), 2) AS "Avg Market Share (%)",
            ROUND(AVG(hhi_score), 4) AS "Avg HHI",
            MAX(concentration_tier) AS "Concentration",
            ROUND(AVG(tam_growth_3m) * 100, 1) AS "Avg TAM Growth (%)"
        FROM analytics.game_pnl
        WHERE company_gross_usd > 0
        GROUP BY 1, 2, 3
        ORDER BY "Avg Monthly Gross ($)" DESC NULLS LAST
    """).df()

    # ── Sheet 2: Game PnL (latest 6 months) ─────────────────
    game_pnl = con.execute("""
        SELECT
            product_name AS "Game",
            genre AS "Genre",
            market AS "Market",
            report_month AS "Month",
            ROUND(company_gross_usd) AS "Gross ($)",
            ROUND(company_iap_usd) AS "IAP ($)",
            ROUND(market_share_pct, 2) AS "Market Share (%)",
            portfolio_rank AS "Rank",
            ROUND(genre_tam_usd) AS "Genre TAM ($)",
            ROUND(tam_growth_3m * 100, 1) AS "TAM Growth 3m (%)",
            ROUND(hhi_score, 4) AS "HHI",
            concentration_tier AS "Concentration",
            ROUND(share_trend_3m_pp, 2) AS "Share Trend 3m (pp)",
            ROUND(revenue_trend_3m * 100, 1) AS "Rev Trend 3m (%)",
            months_since_launch AS "Months Live",
            ROUND(forecast_mid_usd) AS "Forecast Mid ($)",
            opportunity_score AS "Opportunity"
        FROM analytics.game_pnl
        WHERE report_month >= (
            SELECT MAX(report_month) - INTERVAL '6 months' FROM analytics.game_pnl
        )
        ORDER BY report_month DESC, company_gross_usd DESC NULLS LAST
    """).df()

    # ── Sheet 3: Market Share ───────────────────────────────
    market_share = con.execute("""
        SELECT
            product_name AS "Game",
            genre AS "Genre",
            market AS "Market",
            month AS "Month",
            ROUND(company_gross_usd) AS "Gross ($)",
            ROUND(company_iap_usd) AS "IAP ($)",
            ROUND(market_share_pct, 2) AS "Genre Share (%)",
            ROUND(store_share_pct, 4) AS "Store Share (%)",
            portfolio_rank AS "Rank",
            ROUND(trend_3m_pp, 2) AS "Trend 3m (pp)"
        FROM analytics.market_share
        ORDER BY month DESC, company_gross_usd DESC NULLS LAST
    """).df()

    # ── Sheet 4: Genre PnL (SEA, latest 12 months) ─────────
    genre_pnl = con.execute(f"""
        SELECT
            genre AS "Genre",
            country AS "Country",
            report_month AS "Month",
            ROUND(tam_usd) AS "Store TAM ($)",
            ROUND(genre_tam_usd) AS "Genre TAM ($)",
            ROUND(genre_revenue_share * 100, 2) AS "Genre Share of Store (%)",
            ROUND(tam_growth_3m * 100, 1) AS "TAM Growth 3m (%)",
            ROUND(hhi_score, 4) AS "HHI",
            concentration_tier AS "Concentration",
            ROUND(ltv_30, 4) AS "LTV 30d ($)",
            ROUND(ltv_90, 4) AS "LTV 90d ($)",
            ROUND(d1_retention, 1) AS "D1 Ret (%)",
            ROUND(d30_retention, 1) AS "D30 Ret (%)",
            opportunity_score AS "Opportunity"
        FROM analytics.genre_pnl_template
        WHERE country IN {sea_countries}
          AND report_month >= (
              SELECT MAX(report_month) - INTERVAL '12 months'
              FROM analytics.genre_pnl_template
          )
        ORDER BY country, genre, report_month DESC
    """).df()

    # ── Sheet 5: Genre Concentration ────────────────────────
    concentration = con.execute(f"""
        SELECT
            st_category AS "Genre",
            country AS "Country",
            month AS "Month",
            ROUND(genre_iap_usd) AS "Genre Revenue ($)",
            app_count AS "Apps",
            ROUND(top1_share_pct, 1) AS "Top 1 Share (%)",
            ROUND(top5_share_pct, 1) AS "Top 5 Share (%)",
            ROUND(top10_share_pct, 1) AS "Top 10 Share (%)",
            ROUND(hhi_top10, 4) AS "HHI (Top 10)",
            concentration_tier AS "Tier",
            top1_app_name AS "Top App"
        FROM analytics.genre_concentration
        WHERE country IN {sea_countries}
          AND month >= (
              SELECT MAX(month) - INTERVAL '6 months'
              FROM analytics.genre_concentration
          )
        ORDER BY country, st_category, month DESC
    """).df()

    # ── Sheet 6: Revenue Forecast ───────────────────────────
    forecast = con.execute("""
        SELECT
            genre AS "Genre",
            country AS "Market",
            period AS "Period (M)",
            ROUND(revenue_mid_usd) AS "Forecast Mid ($)",
            ROUND(revenue_low_usd) AS "Forecast Low ($)",
            ROUND(revenue_high_usd) AS "Forecast High ($)",
            calibration_factor AS "Calibration Factor"
        FROM analytics.revenue_forecast
        ORDER BY genre, country, period
    """).df()

    # ── Sheet 7: Portfolio Summary ──────────────────────────
    portfolio = con.execute("""
        SELECT
            genre AS "Genre",
            market AS "Market",
            month AS "Month",
            game_count AS "Games",
            ROUND(total_company_gross_usd) AS "Portfolio Gross ($)",
            ROUND(total_company_iap_usd) AS "Portfolio IAP ($)",
            ROUND(genre_iap_usd) AS "Genre TAM ($)",
            ROUND(portfolio_share_pct, 2) AS "Portfolio Share (%)",
            top_game_name AS "Top Game",
            ROUND(top_game_share_pct, 2) AS "Top Game Share (%)",
            ROUND(trend_3m_pp, 2) AS "Trend 3m (pp)"
        FROM analytics.portfolio_summary
        ORDER BY genre, market, month DESC
    """).df()

    # ── Sheet 8: Data Quality ───────────────────────────────
    data_quality = con.execute("""
        SELECT
            'Mapping Coverage' AS "Metric",
            CONCAT(
                COUNT(CASE WHEN unified_app_id IS NOT NULL THEN 1 END),
                '/', COUNT(*), ' games mapped (',
                ROUND(COUNT(CASE WHEN unified_app_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1),
                '%)'
            ) AS "Value"
        FROM dim.dim_company_games
        UNION ALL
        SELECT 'Market Share Rows', CONCAT(COUNT(*), ' total') FROM analytics.market_share
        UNION ALL
        SELECT 'With Genre Share', CONCAT(COUNT(*), ' rows')
        FROM analytics.market_share WHERE market_share_pct IS NOT NULL
        UNION ALL
        SELECT 'Null Genre Denom', CONCAT(COUNT(*), ' rows')
        FROM analytics.market_share WHERE genre_iap_usd IS NULL
        UNION ALL
        SELECT 'Over 100% (nulled)', CONCAT(COUNT(*), ' rows')
        FROM analytics.market_share WHERE market_share_pct IS NULL AND genre_iap_usd IS NOT NULL
        UNION ALL
        SELECT 'MI Data Latest', MAX(date)::VARCHAR
        FROM fact.fact_market_insights WHERE revenue_cents > 0
        UNION ALL
        SELECT 'Genre Concentration Rows', CONCAT(COUNT(*), ' rows')
        FROM analytics.genre_concentration
        UNION ALL
        SELECT 'Game PnL Rows', CONCAT(COUNT(*), ' rows') FROM analytics.game_pnl
        UNION ALL
        SELECT 'Revenue Forecast Rows', CONCAT(COUNT(*), ' rows') FROM analytics.revenue_forecast
        UNION ALL
        SELECT 'LTV Model Rows', CONCAT(COUNT(*), ' rows') FROM analytics.ltv_model
    """).df()

    con.close()

    # ── Write Excel ─────────────────────────────────────────
    with pd.ExcelWriter(str(out_path), engine='openpyxl') as writer:
        exec_summary.to_excel(writer, sheet_name='Executive Summary', index=False)
        game_pnl.to_excel(writer, sheet_name='Game PnL', index=False)
        market_share.to_excel(writer, sheet_name='Market Share', index=False)
        genre_pnl.to_excel(writer, sheet_name='Genre PnL', index=False)
        concentration.to_excel(writer, sheet_name='Genre Concentration', index=False)
        forecast.to_excel(writer, sheet_name='Revenue Forecast', index=False)
        portfolio.to_excel(writer, sheet_name='Portfolio Summary', index=False)
        data_quality.to_excel(writer, sheet_name='Data Quality', index=False)

    print(f"  ✓ Report generated: {out_path}")
    print(f"    Sheets: Executive Summary | Game PnL | Market Share | Genre PnL")
    print(f"            Genre Concentration | Revenue Forecast | Portfolio Summary | Data Quality")
    return str(out_path)


# ────────────────────────────────────────────────────────
# ORCHESTRATOR
# ────────────────────────────────────────────────────────

def run_all_analytics(report_month: str, db_path: str | None = None):
    """Run the full analytics computation pipeline for a given report month."""
    print(f"\n=== Analytics: {report_month} ===")
    print("── Fix: False-Positive Mappings ──")
    fix_false_positive_mappings(db_path)
    print("── Phase 3: Benchmarking ──")
    compute_benchmark_accuracy(db_path)

    print("── Phase 3b: RPD Model ──")
    compute_rpd(db_path)
    compute_rpd_benchmark(db_path)

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
