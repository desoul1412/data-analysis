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
