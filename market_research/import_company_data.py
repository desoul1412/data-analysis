"""
Import company data files (Game Performance + Changelogs) into DuckDB.
"""
import re
import duckdb
import pandas as pd
from config import (
    DB_PATH, GAME_PERFORMANCE_CSV,
    CHANGELOG_JAN_CSV, CHANGELOG_FEB_CSV,
    COMPANY_MARKET_TO_ST,
)


def _parse_currency(val) -> float | None:
    """Parse currency strings like '$1,234' or '$8.35' to float."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or str(val).strip() in ('', '-', 'nan'):
        return None
    cleaned = str(val).replace('$', '').replace(',', '').strip()
    if cleaned in ('', '-'):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_product_name(product_code: str) -> str:
    """Extract human-readable name from codes like 'C06 : Ballistic Hero-ID'."""
    match = re.match(r'^\w+\s*:\s*(.+)$', product_code)
    if match:
        name = match.group(1).strip()
        name = re.sub(r'-(VN|ID|TH|PH|SM|MY|TW|Glob(al)?)$', '', name).strip()
        return name
    return product_code


def import_game_performance(con: duckdb.DuckDBPyConnection):
    """Import Game Performance.csv into dim.dim_company_games + fact.fact_company_revenue."""
    print("── Importing Game Performance.csv ──")

    df = pd.read_csv(str(GAME_PERFORMANCE_CSV), sep='\t', encoding='utf-16', dtype=str)
    df = df.dropna(how='all')
    df = df[df['Product'].notna() & (df['Product'].str.strip() != '')]

    print(f"  Read {len(df)} rows, {df['Product'].nunique()} products, {df['Market'].nunique()} markets")

    # ── dim_company_games ──
    dim_rows = []
    seen = set()
    for _, row in df.iterrows():
        key = (row['Product'], row['Market'])
        if key in seen:
            continue
        seen.add(key)
        dim_rows.append({
            'product_code': row['Product'],
            'product_name': _parse_product_name(row['Product']),
            'market': row['Market'],
            'st_country_code': COMPANY_MARKET_TO_ST.get(row['Market'], ''),
            'unified_app_id': None,
            'genre': None,   # fill manually: MMORPG, MOBA, Team Battle, Battle Royale, Shooting
        })

    dim_df = pd.DataFrame(dim_rows)
    dim_df.insert(0, 'id', range(1, len(dim_df) + 1))  # id must be FIRST column

    con.execute("DELETE FROM dim.dim_company_games")
    con.execute("INSERT INTO dim.dim_company_games SELECT * FROM dim_df")
    print(f"  ✓ dim.dim_company_games: {len(dim_df)} rows")

    # ── fact_company_revenue ──
    month_cols = [f'M{i}' for i in range(37)]

    # Get column names from table DDL order
    tbl_cols = [r[0] for r in con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'fact' AND table_name = 'fact_company_revenue'
        ORDER BY ordinal_position
    """).fetchall()]
    # Remove loaded_at — it has a default
    tbl_cols = [c for c in tbl_cols if c != 'loaded_at']

    # Filter to only the revenue rows for fact table
    rev_source_df = df[df['Unnamed: 3'] == 'Game Revenue (5)']

    rev_rows = []
    for _, row in rev_source_df.iterrows():
        rev_row = {
            'product_code': row['Product'],
            'market': row['Market'],
            'ob_date': row.get('Ob Date')
        }
        monthly_total = 0.0
        for col in month_cols:
            val = _parse_currency(row.get(col, ''))
            rev_row[col.lower()] = val
            if val is not None:
                monthly_total += val
                
        # Use provided Grand Total or fallback to calculated total
        csv_grand_total = _parse_currency(row.get('Grand Total', ''))
        rev_row['grand_total'] = csv_grand_total if csv_grand_total is not None else monthly_total
        
        rev_rows.append(rev_row)

    rev_df = pd.DataFrame(rev_rows)

    # Deduplicate: CSV may have duplicate (product_code, market, ob_date) rows — keep last
    rev_df = rev_df.drop_duplicates(subset=['product_code', 'market', 'ob_date'], keep='last')
    print(f"  After dedup: {len(rev_df)} revenue rows ({len(rev_rows) - len(rev_df)} removed)")

    # Reorder to match table columns (excluding loaded_at)
    rev_df = rev_df[[c for c in tbl_cols if c in rev_df.columns]]

    con.execute("DELETE FROM fact.fact_company_revenue")
    col_list = ', '.join(f'"{c}"' for c in rev_df.columns)
    con.execute(f"INSERT INTO fact.fact_company_revenue ({col_list}) SELECT * FROM rev_df")
    print(f"  ✓ fact.fact_company_revenue: {len(rev_df)} rows")


def import_changelogs(con: duckdb.DuckDBPyConnection):
    """Import genre changelog CSVs into dim.dim_genre_taxonomy."""
    print("── Importing Changelogs ──")

    changelog_files = [
        (CHANGELOG_JAN_CSV, '2026_Jan'),
        (CHANGELOG_FEB_CSV, '2026_Feb'),
    ]

    con.execute("DELETE FROM dim.dim_genre_taxonomy")
    total = 0

    for csv_path, source_label in changelog_files:
        if not csv_path.exists():
            print(f"  ⚠ Skipping {csv_path.name} (not found)")
            continue

        df = pd.read_csv(str(csv_path), sep='\t', encoding='utf-16', dtype=str)
        df = df.dropna(how='all')

        if df.empty:
            print(f"  ⚠ {csv_path.name} is empty")
            continue

        # Normalize column names
        col_map = {}
        for c in df.columns:
            cl = c.strip()
            if 'Unified ID' in cl:       col_map[c] = 'unified_id'
            elif 'Unified Name' in cl:   col_map[c] = 'unified_name'
            elif 'Class (From' in cl:    col_map[c] = 'class_from'
            elif 'Class (To' in cl:      col_map[c] = 'class_to'
            elif 'Genre (From' in cl:    col_map[c] = 'genre_from'
            elif 'Genre (To' in cl:      col_map[c] = 'genre_to'
            elif 'Sub-genre (From' in cl or 'Subgenre (From' in cl: col_map[c] = 'subgenre_from'
            elif 'Sub-genre (To' in cl or 'Subgenre (To' in cl:     col_map[c] = 'subgenre_to'

        df = df.rename(columns=col_map)
        df['changelog_source'] = source_label
        df['id'] = range(total + 1, total + 1 + len(df))

        needed = ['id', 'unified_id', 'unified_name', 'class_from', 'class_to',
                  'genre_from', 'genre_to', 'subgenre_from', 'subgenre_to', 'changelog_source']
        for col in needed:
            if col not in df.columns:
                df[col] = None

        insert_df = df[needed]
        con.execute("INSERT INTO dim.dim_genre_taxonomy SELECT * FROM insert_df")
        total += len(insert_df)
        print(f"  ✓ {csv_path.name}: {len(insert_df)} rows")

    print(f"  ✓ dim.dim_genre_taxonomy total: {total} rows")


def run_all(db_path: str | None = None):
    """Run full company data import."""
    path = db_path or str(DB_PATH)
    con = duckdb.connect(path)
    import_game_performance(con)
    import_changelogs(con)
    con.close()
    print("\n✓ All company data imported successfully.")


if __name__ == "__main__":
    run_all()
