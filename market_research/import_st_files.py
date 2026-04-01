"""
Import pre-pulled Sensor Tower data files into DuckDB.
Covers 2019-01 to 2026-01 — use this instead of calling the ST API
for historical data to preserve API quota.

Files (pipe-delimited, in workspace root d:/BaoLN2/Personal/data-analysis/):
  dim_app.txt          — 243k apps  (unified_app_id, name, genre, sub_genre, theme, art_style)
  dim_app_variants.txt — 1.1M rows  (variant_id, unified_app_id, app_id, os, country, publisher_name, release_date)
  market_insights.txt  — 6.4M rows  (app_id, country, os, month, downloads, revenue)

Strategy:
  - Use DuckDB's native read_csv() for bulk load — avoids pandas overhead on large files
  - Chunk market_insights in 500k-row batches (keeps RAM under ~1GB peak)
  - Deduplicate before insert using INSERT OR IGNORE pattern
"""
import duckdb
import pandas as pd
from pathlib import Path
from config import DB_PATH

# File locations — txt files sit in workspace root, one level above market_research/
ROOT = Path(DB_PATH).parent.parent
DIM_APP         = ROOT / 'dim_app.txt'
DIM_VARIANTS    = ROOT / 'dim_app_variants.txt'
MARKET_INSIGHTS = ROOT / 'market_insights.txt'

CHUNK_SIZE = 500_000   # rows per batch for market_insights


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Strip fixed-width padding from column names and string values."""
    df.columns = [c.strip() for c in df.columns]
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
    return df


def _read_pipe(path: Path, nrows: int | None = None) -> pd.DataFrame:
    kw = dict(sep='|', dtype=str, low_memory=False, on_bad_lines='skip')
    if nrows:
        kw['nrows'] = nrows
    df = pd.read_csv(path, **kw)
    df = _strip_cols(df)
    # Drop always-empty prefix/suffix columns produced by leading/trailing pipes
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
    # Drop header-separator row (all dashes)
    if len(df) > 0:
        df = df[~df.iloc[:, 0].str.match(r'^-+$', na=False)]
    return df


# ── 1. dim.dim_apps (from dim_app + dim_app_variants) ────────────────────────

def import_dim_apps(con: duckdb.DuckDBPyConnection):
    """
    Load app dimension tables.
    dim_app.txt       → lookup: unified_app_id → genre/sub_genre
    dim_app_variants  → dim.dim_apps (one row per app_id × os × country)
    """
    print("── Loading dim_app.txt …")
    app_df = _read_pipe(DIM_APP)
    print(f"   {len(app_df):,} apps — cols: {list(app_df.columns)}")

    print("── Loading dim_app_variants.txt …")
    var_df = _read_pipe(DIM_VARIANTS)
    print(f"   {len(var_df):,} variants — cols: {list(var_df.columns)}")

    # Merge genre info onto variants
    merged = var_df.merge(
        app_df[['unified_app_id', 'name', 'genre', 'sub_genre', 'theme', 'art_style']],
        on='unified_app_id',
        how='left',
    )

    # Map to dim.dim_apps schema
    dim = pd.DataFrame({
        'app_id':           merged.get('app_id'),
        'unified_app_id':   merged.get('unified_app_id'),
        'name':             merged.get('name'),
        'publisher_name':   merged.get('publisher_name'),
        'publisher_id':     None,
        'os':               merged.get('os'),
        'category_id':      merged.get('genre'),      # genre → category_id
        'release_date':     pd.to_datetime(merged.get('release_date'), errors='coerce'),
        'icon_url':         merged.get('icon_url'),
        'price':            None,
        'rating':           None,
        'ratings_count':    None,
        'country':          merged.get('country'),
    }).dropna(subset=['app_id', 'country', 'os'])

    # Deduplicate by (app_id, country, os)
    dim = dim.drop_duplicates(subset=['app_id', 'country', 'os'], keep='last')
    print(f"   After dedup: {len(dim):,} rows")

    con.execute("DELETE FROM dim.dim_apps")
    con.register('dim_df', dim)
    con.execute("""
        INSERT INTO dim.dim_apps
         (app_id, unified_app_id, name, publisher_name, publisher_id,
          os, category_id, release_date, icon_url, price, rating, ratings_count, country)
        SELECT app_id, unified_app_id, name, publisher_name, publisher_id,
               os, category_id, release_date, icon_url, price, rating, ratings_count, country
        FROM dim_df
    """)
    con.unregister('dim_df')
    print(f"  ✓ dim.dim_apps: {len(dim):,} rows inserted")


# ── 2. fact.fact_market_insights (from market_insights.txt) ──────────────────

def import_market_insights(con: duckdb.DuckDBPyConnection):
    """
    Bulk-load market_insights.txt (6.4M rows) in 500k-row chunks.

    Source columns: app_id, country, os, month, downloads, revenue
    Target: fact.fact_market_insights
      - date          ← month
      - date_granularity = 'monthly'
      - downloads     ← downloads (integer)
      - revenue_cents ← revenue * 100  (ST returns $ not cents)
      - source        = 'sensortower_file'
    """
    print("── Loading market_insights.txt (chunked) …")

    total_chunks = 0
    total_rows   = 0
    skipped      = 0

    # Use pandas chunked reader
    reader = pd.read_csv(
        MARKET_INSIGHTS,
        sep='|',
        dtype=str,
        low_memory=False,
        on_bad_lines='skip',
        chunksize=CHUNK_SIZE,
    )

    con.execute("DELETE FROM fact.fact_market_insights")

    for chunk_df in reader:
        chunk_df = _strip_cols(chunk_df)
        chunk_df = chunk_df.loc[:, ~chunk_df.columns.str.startswith('Unnamed')]
        # Drop separator rows
        if len(chunk_df) > 0:
            chunk_df = chunk_df[~chunk_df.iloc[:, 0].str.match(r'^-+$', na=False)]

        if chunk_df.empty:
            continue

        # Coerce types
        chunk_df['downloads']     = pd.to_numeric(chunk_df.get('downloads', 0), errors='coerce').fillna(0).astype(int)
        chunk_df['revenue_cents'] = (
            pd.to_numeric(chunk_df.get('revenue', 0), errors='coerce').fillna(0) * 100
        ).round(0).astype(int)
        chunk_df['date']              = chunk_df.get('month', '')
        chunk_df['date_granularity']  = 'monthly'
        chunk_df['source']            = 'sensortower_file'

        # Filter: drop rows without key fields
        chunk_df = chunk_df.dropna(subset=['app_id', 'country', 'os', 'date'])
        chunk_df = chunk_df[chunk_df['date'].str.match(r'^\d{4}-\d{2}', na=False)]

        # Drop in-chunk duplicates
        before = len(chunk_df)
        chunk_df = chunk_df.drop_duplicates(
            subset=['app_id', 'country', 'os', 'date', 'date_granularity'], keep='last'
        )
        skipped += before - len(chunk_df)

        if chunk_df.empty:
            total_chunks += 1
            continue

        ins = chunk_df[['app_id', 'country', 'os', 'date', 'date_granularity',
                         'downloads', 'revenue_cents', 'source']].copy()

        con.register('ins_chunk', ins)
        con.execute("""
            INSERT INTO fact.fact_market_insights
             (app_id, country, os, date, date_granularity, downloads, revenue_cents, source)
            SELECT app_id, country, os, date, date_granularity, downloads, revenue_cents, source
            FROM ins_chunk
            ON CONFLICT (app_id, country, date, date_granularity, os)
            DO NOTHING
        """)
        con.unregister('ins_chunk')

        total_rows   += len(ins)
        total_chunks += 1
        print(f"   chunk {total_chunks}: {len(ins):,} rows  (total so far: {total_rows:,})", end='\r')

    print(f"\n  ✓ fact.fact_market_insights: {total_rows:,} rows inserted ({skipped:,} in-chunk dupes skipped)")


# ── Also: register dim_app → raw.raw_sensortower_apps ─────────────────────────
# (for future reference, the raw genre taxonomy from ST before we normalise it)

def import_raw_dim_app(con: duckdb.DuckDBPyConnection):
    """Store the raw dim_app.txt into a raw staging table for posterity."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_st_dim_app (
            unified_app_id  VARCHAR,
            name            VARCHAR,
            genre           VARCHAR,
            sub_genre       VARCHAR,
            theme           VARCHAR,
            art_style       VARCHAR,
            loaded_at       TIMESTAMP DEFAULT current_timestamp
        )
    """)
    app_df = _read_pipe(DIM_APP)
    app_df = app_df.rename(columns={
        'unified_app_id': 'unified_app_id',
        'name': 'name',
        'genre': 'genre',
        'sub_genre': 'sub_genre',
        'theme': 'theme',
        'art_style': 'art_style',
    })
    keep = ['unified_app_id', 'name', 'genre', 'sub_genre', 'theme', 'art_style']
    app_df = app_df[[c for c in keep if c in app_df.columns]]
    con.execute("DELETE FROM raw.raw_st_dim_app")
    con.register('raw_app_df', app_df)
    con.execute("INSERT INTO raw.raw_st_dim_app (unified_app_id, name, genre, sub_genre, theme, art_style) "
                "SELECT unified_app_id, name, genre, sub_genre, theme, art_style FROM raw_app_df")
    con.unregister('raw_app_df')
    print(f"  ✓ raw.raw_st_dim_app: {len(app_df):,} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_all():
    import time
    t0 = time.time()
    con = duckdb.connect(str(DB_PATH))

    print("\n=== Importing Historical ST Files ===")
    print(f"  dim_app.txt:         {DIM_APP}")
    print(f"  dim_app_variants.txt:{DIM_VARIANTS}")
    print(f"  market_insights.txt: {MARKET_INSIGHTS}")
    print()

    import_raw_dim_app(con)
    import_dim_apps(con)
    import_market_insights(con)

    elapsed = time.time() - t0
    print(f"\n✓ Done in {elapsed:.1f}s")

    # Quick sanity check
    for schema, table in [('dim', 'dim_apps'), ('fact', 'fact_market_insights'),
                           ('raw', 'raw_st_dim_app')]:
        cnt = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        print(f"  {schema}.{table}: {cnt:,}")

    con.close()


if __name__ == '__main__':
    run_all()
