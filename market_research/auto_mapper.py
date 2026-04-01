import duckdb
from config import DB_PATH
import re

def auto_map_games():
    """Maps internal company products to ST unified_app_ids based on string similarity."""
    print("=== Auto Mapping Internal Games to Sensor Tower ===")
    
    con = duckdb.connect(str(DB_PATH))
    
    # Get all missing standardizations
    missing = con.execute("""
        SELECT DISTINCT product_code, product_name 
        FROM dim.dim_company_games 
        WHERE unified_app_id IS NULL
    """).fetchdf()

    if missing.empty:
        print("✓ All company games are mapped!")
        return

    print(f"Found {len(missing)} internal products to map...")

    # We will query ST dim_apps looking for fuzzy matches.
    for _, row in missing.iterrows():
        pcode = row['product_code']
        pname = row['product_name']
        
        # Simple keywords from pname
        # Clean string: remove generic words and punctuation
        clean_name = re.sub(r'[^a-zA-Z0-9\s]', ' ', pname).lower()
        keywords = [k for k in clean_name.split() if len(k) > 2]
        
        if not keywords:
            continue
            
        print(f"\n🔍 Searching for '{pname}' ({pcode})...")
        
        likes = " OR ".join(["LOWER(name) LIKE ?" for _ in keywords])
        params = [f"%{k}%" for k in keywords]
        
        candidates = con.execute(f"""
            SELECT DISTINCT unified_app_id, name, publisher_name
            FROM dim.dim_apps
            WHERE ({likes})
            ORDER BY name
            LIMIT 5
        """, params).fetchall()
        
        if not candidates:
            print("  [No candidates found in Sensor Tower DB]")
            continue
            
        for idx, cand in enumerate(candidates):
            uid, cname, pub = cand
            print(f"  [{idx+1}] {cname} (By: {pub}) -> {uid}")
        
        print("💡 Recommendation: Use SQL to UPDATE dim.dim_company_games SET unified_app_id = 'xxx'...")

if __name__ == '__main__':
    auto_map_games()
