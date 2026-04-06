"""
Polls fetch_au_retention.log until the DONE line appears in NEW content,
then runs all analytics phases (with DB lock retry).
"""
import time
import sys
import duckdb
from pathlib import Path

LOG      = Path(__file__).parent / "fetch_au_retention.log"
DB_PATH  = Path(__file__).parent / "market_research.duckdb"
POLL_SEC = 60

print("Watching fetch_au_retention.log for completion...", flush=True)

# Record current log size — only scan content written after this point
offset = LOG.stat().st_size if LOG.exists() else 0
print(f"  Log offset start: {offset} bytes", flush=True)

while True:
    time.sleep(POLL_SEC)
    if not LOG.exists():
        continue
    with open(LOG, encoding="utf-8", errors="replace") as f:
        f.seek(offset)
        new_text = f.read()

    if not new_text:
        continue

    # Show last progress line
    lines = [l for l in new_text.splitlines() if "Running totals" in l]
    if lines:
        print(f"  {lines[-1].split('INFO')[-1].strip()}", flush=True)

    if "DONE -" in new_text and "API calls" in new_text:
        print("Fetch DONE. Running analytics...", flush=True)
        break

# ── Run analytics with DB lock retry ─────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from analytics import run_all_analytics

for attempt in range(10):
    try:
        con = duckdb.connect(str(DB_PATH))
        con.close()
        break
    except Exception as e:
        wait = 15 * (attempt + 1)
        print(f"  DB locked (attempt {attempt+1}), retrying in {wait}s: {e}", flush=True)
        time.sleep(wait)
else:
    print("ERROR: Could not acquire DB lock after 10 attempts. Run analytics manually.", flush=True)
    sys.exit(1)

run_all_analytics("2026-04-01")
print("Analytics complete.", flush=True)
