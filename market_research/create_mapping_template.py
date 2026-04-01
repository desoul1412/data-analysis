import duckdb
from config import DB_PATH
import os

def create_mapping_template():
    print("=== Creating Company Games Mapping Template ===")
    
    con = duckdb.connect(str(DB_PATH))
    out_file = "company_game_mapping.csv"
    
    # Export missing to CSV
    con.execute(f"""
        COPY (
            SELECT DISTINCT product_code, product_name, '' as unified_app_id
            FROM dim.dim_company_games 
            WHERE unified_app_id IS NULL
            ORDER BY product_name
        ) TO '{out_file}' (HEADER, DELIMITER ',')
    """)
    
    print(f"✓ Exported mapping template to '{out_file}'")
    print("  Please open this file, fill in the correct ST 'unified_app_id' values,")
    print("  and save it. Then we can load it to update dim_company_games.")

if __name__ == '__main__':
    create_mapping_template()
