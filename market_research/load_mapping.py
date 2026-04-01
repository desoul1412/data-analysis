import duckdb
from config import DB_PATH
import os

def load_mapping():
    print("=== Loading Company Games Mapping ===")
    
    mapping_file = "company_game_mapping.csv"
    if not os.path.exists(mapping_file):
        print(f"❌ Error: {mapping_file} not found. Did you fill it out?")
        return
        
    con = duckdb.connect(str(DB_PATH))
    
    # Check if there are any non-empty mappings
    df = con.execute(f"SELECT * FROM read_csv_auto('{mapping_file}') WHERE unified_app_id IS NOT NULL AND unified_app_id != ''").fetchdf()
    if df.empty:
        print("ℹ️ No valid unified_app_ids found in mapping file to update.")
        return
        
    print(f"Loading {len(df)} mapped products...")
    
    # First, import mapping into a temp table
    con.execute("DROP TABLE IF EXISTS temp_mapping")
    con.execute(f"CREATE TEMP TABLE temp_mapping AS SELECT * FROM read_csv_auto('{mapping_file}') WHERE unified_app_id IS NOT NULL AND unified_app_id != ''")
    
    # Update dim_company_games using temp_mapping
    con.execute("""
        UPDATE dim.dim_company_games
        SET unified_app_id = t.unified_app_id
        FROM temp_mapping t
        WHERE dim.dim_company_games.product_code = t.product_code
    """)
    
    print("✓ Successfully updated dim.dim_company_games with new mappings.")
    
if __name__ == '__main__':
    load_mapping()
