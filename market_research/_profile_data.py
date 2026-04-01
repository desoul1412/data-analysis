"""Profile company data - clean output."""
import pandas as pd

# --- Game Performance ---
gp = pd.read_csv('D:/BaoLN2/Personal/data-analysis/Game Performance.csv',
                  sep='\t', encoding='utf-16', dtype=str)
gp = gp.dropna(how='all')
gp = gp[gp['Product'].notna() & (gp['Product'].str.strip() != '')]

with open('/tmp/profile_output.txt', 'w', encoding='utf-8') as f:
    f.write("=== Game Performance.csv ===\n")
    f.write(f"Columns ({len(gp.columns)}): {gp.columns.tolist()}\n")
    f.write(f"Shape: {gp.shape}\n\n")
    
    f.write("Sample (first 3 rows):\n")
    f.write(gp[['Product', 'Market', 'Ob Date', 'Grand Total', 'M0', 'M1', 'M2']].head(3).to_string() + "\n\n")
    
    f.write(f"Unique Products ({gp['Product'].nunique()}):\n")
    for p in sorted(gp['Product'].unique()):
        f.write(f"  {p}\n")
    f.write(f"\nUnique Markets ({gp['Market'].nunique()}):\n")
    for m in sorted(gp['Market'].unique()):
        f.write(f"  {m}\n")
    f.write(f"\nOb Date samples: {gp['Ob Date'].head(5).tolist()}\n")
    
    # --- Changelog ---
    f.write("\n=== 2026_Feb_changelog.csv ===\n")
    cl = pd.read_csv('D:/BaoLN2/Personal/data-analysis/2026_Feb_changelog.csv',
                      sep='\t', encoding='utf-16', dtype=str)
    cl = cl.dropna(how='all')
    f.write(f"Columns: {cl.columns.tolist()}\n")
    f.write(f"Shape: {cl.shape}\n\n")
    f.write(cl.head(5).to_string() + "\n\n")
    
    # Unique classes
    for col_pair in [('Class (From)', 'Class (To)'), ('Genre (From)', 'Genre (To)'), ('Sub-genre (From)', 'Sub-genre (To)')]:
        vals = set()
        for c in col_pair:
            if c in cl.columns:
                vals |= set(cl[c].dropna().unique())
        f.write(f"Unique {col_pair[0].split(' ')[0]} values ({len(vals)}):\n")
        for v in sorted(vals):
            f.write(f"  {v}\n")
        f.write("\n")

print("Output written to /tmp/profile_output.txt")
