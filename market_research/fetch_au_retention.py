"""
fetch_au_retention.py — Resumable, rate-limited fetcher for:
  - Active Users (MAU) per app × OS
  - Cohort Retention per app

Design principles:
  - Year-chunked requests (avoids ST date-range limits)
  - Checkpoint file (.fetch_progress.json) — resume on restart
  - Configurable delay with jitter to stay under rate limits
  - 429 handling: 60s back-off, then retry
  - Logs to fetch_au_retention.log for unattended runs

Usage:
  python fetch_au_retention.py            # run with defaults
  python fetch_au_retention.py --delay 3  # 3s between calls
  python fetch_au_retention.py --reset    # clear checkpoint and restart
"""
import argparse
import json
import logging
import random
import sys
import time
from datetime import date
from pathlib import Path

import duckdb

from config import DB_PATH, ST_AUTH_TOKEN
from st_extract import fetch_active_users, fetch_retention
from st_load import load_active_users, load_retention

# ── Config ────────────────────────────────────────────────────────────────────

CHECKPOINT_FILE = Path(__file__).parent / ".fetch_progress.json"
LOG_FILE        = Path(__file__).parent / "fetch_au_retention.log"

# Full historical range available in ST
HISTORY_START = date(2019, 1, 1)
HISTORY_END   = date(2026, 3, 31)

# Year chunks to split large date ranges (avoids ST per-call limits)
def _year_chunks(start: date, end: date) -> list[tuple[str, str]]:
    """Split [start, end] into year-boundary slices. Returns [(start_str, end_str), ...]"""
    chunks = []
    cur_year = start.year
    while cur_year <= end.year:
        chunk_start = max(start, date(cur_year, 1, 1))
        chunk_end   = min(end,   date(cur_year, 12, 31))
        chunks.append((chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur_year += 1
    return chunks


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logging():
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

log = logging.getLogger(__name__)


# ── Checkpoint ────────────────────────────────────────────────────────────────

def _load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"au_done": [], "ret_done": []}


def _save_checkpoint(cp: dict):
    CHECKPOINT_FILE.write_text(json.dumps(cp, indent=2), encoding="utf-8")


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_platform_app_ids() -> list[tuple[str, str, str]]:
    """Return (platform_app_id, unified_app_id, os) for all mapped company games.

    Uses dim_apps to get the platform-specific app_id (iOS numeric / Android package)
    required by /v1/ios/ and /v1/android/ ST endpoints.
    """
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = con.execute("""
        SELECT DISTINCT da.app_id, da.unified_app_id, da.os
        FROM dim.dim_apps da
        JOIN dim.dim_company_games cg ON cg.unified_app_id = da.unified_app_id
        WHERE da.unified_app_id IS NOT NULL
          AND da.unified_app_id != ''
          AND da.app_id IS NOT NULL
          AND da.app_id != ''
          AND da.os IN ('ios', 'android')
        ORDER BY da.unified_app_id, da.os
    """).fetchall()
    con.close()
    return rows  # list of (platform_app_id, unified_app_id, os)


# ── Rate-limited sleep ────────────────────────────────────────────────────────

def _sleep(base_delay: float):
    """Sleep base_delay + small random jitter to avoid burst patterns."""
    jitter = random.uniform(0.0, base_delay * 0.3)
    time.sleep(base_delay + jitter)


def _on_rate_limit(attempt: int):
    """Called when a 429 is detected. Waits progressively longer."""
    wait = min(60 * (attempt + 1), 300)   # 60s, 120s, 180s … max 300s
    log.warning(f"  Rate-limited — waiting {wait}s before retry (attempt {attempt+1})")
    time.sleep(wait)


# ── Fetch helpers (with 429 retry) ───────────────────────────────────────────

def _safe_fetch_au(app_id: str, os_type: str, start: str, end: str,
                   base_delay: float) -> list[dict]:
    """Fetch active_users with 429 back-off. Returns empty list on error."""
    for attempt in range(3):
        try:
            data = fetch_active_users(
                app_ids=[app_id],
                os=os_type,
                time_period="month",
                start_date=start,
                end_date=end,
            )
            _sleep(base_delay)
            return data or []
        except Exception as e:
            msg = str(e)
            if "429" in msg:
                _on_rate_limit(attempt)
            else:
                log.warning(f"  fetch_au error ({app_id} {os_type} {start}–{end}): {e}")
                return []
    return []


def _safe_fetch_ret(app_id: str, start: str, end: str,
                    base_delay: float) -> list | dict:
    """Fetch retention with 429 back-off. Returns empty list on error."""
    for attempt in range(3):
        try:
            data = fetch_retention(
                app_ids=[app_id],
                bundle="retention_monthly",
                start_date=start,
                end_date=end,
            )
            _sleep(base_delay)
            return data if data else []
        except Exception as e:
            msg = str(e)
            if "429" in msg:
                _on_rate_limit(attempt)
            else:
                log.warning(f"  fetch_ret error ({app_id} {start}–{end}): {e}")
                return []
    return []


# ── Parse retention response → list[dict] ────────────────────────────────────

def _parse_retention(raw: list | dict) -> list[dict]:
    """
    Normalize ST /v1/facets/metrics retention response to a flat list.

    API field names: est_retention_d1, est_retention_d7, est_retention_d30,
    est_retention_d90, est_retention_m1, est_retention_m3.
    Response may be a bare list or wrapped in {"data": [...]}.
    """
    if not raw:
        return []

    # Unwrap {"data": [...]} envelope or accept bare list
    if isinstance(raw, list):
        items = raw
    else:
        items = raw.get("data") or raw.get("results") or []
        if not items and raw.get("app_id"):
            items = [raw]  # single flat record

    rows = []
    for item in items:
        rows.append({
            "app_id":  item.get("app_id") or item.get("id", ""),
            "date":    item.get("date", ""),
            "day_1":   item.get("est_retention_d1") or item.get("day_1") or item.get("d1"),
            "day_7":   item.get("est_retention_d7") or item.get("day_7") or item.get("d7"),
            "day_30":  item.get("est_retention_d30") or item.get("day_30") or item.get("d30"),
            "day_90":  item.get("est_retention_d90") or item.get("day_90") or item.get("d90"),
            "month_1": item.get("est_retention_m1") or item.get("month_1") or item.get("m1"),
            "month_3": item.get("est_retention_m3") or item.get("month_3") or item.get("m3"),
        })
    return rows


# ── Main runner ───────────────────────────────────────────────────────────────

def run(base_delay: float = 2.0, reset: bool = False):
    if not ST_AUTH_TOKEN:
        log.error("SENSORTOWER_AUTH_TOKEN not set. Aborting.")
        sys.exit(1)

    _setup_logging()
    log.info("=" * 60)
    log.info(f"fetch_au_retention  started  delay={base_delay}s")
    log.info(f"range: {HISTORY_START} → {HISTORY_END}")
    log.info("=" * 60)

    cp = {} if reset else _load_checkpoint()
    au_done:  set[str] = set(cp.get("au_done", []))   # "platform_app_id|os"
    ret_done: set[str] = set(cp.get("ret_done", []))  # "unified_app_id"

    if reset and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        log.info("Checkpoint cleared — starting from scratch")

    # platform_rows: list of (platform_app_id, unified_app_id, os)
    platform_rows = _get_platform_app_ids()
    log.info(f"Found {len(platform_rows)} platform app entries "
             f"({sum(1 for r in platform_rows if r[2]=='ios')} iOS, "
             f"{sum(1 for r in platform_rows if r[2]=='android')} Android)")

    chunks = _year_chunks(HISTORY_START, HISTORY_END)
    log.info(f"Date chunks: {len(chunks)} years | "
             f"max AU calls: {len(platform_rows) * len(chunks)} | "
             f"max RET calls: {len(platform_rows) * len(chunks)}")

    total_au_rows   = 0
    total_ret_rows  = 0
    call_count      = 0

    # ── Active Users — iterate by (platform_app_id, os) ─────────────────────
    for idx, (platform_app_id, unified_app_id, os_type) in enumerate(platform_rows, 1):
        log.info(f"── AU {idx}/{len(platform_rows)}: {platform_app_id} ({os_type}) [unified={unified_app_id}] ──")

        key = f"{platform_app_id}|{os_type}"
        if key in au_done:
            log.info(f"  [AU/{os_type}] already done — skip")
            continue

        all_au: list[dict] = []
        for chunk_start, chunk_end in chunks:
            log.info(f"  [AU/{os_type}] {chunk_start} -> {chunk_end}")
            data = _safe_fetch_au(platform_app_id, os_type, chunk_start, chunk_end, base_delay)
            call_count += 1
            if data:
                all_au.extend(data)

        if all_au:
            try:
                load_active_users(all_au, os=os_type)
                total_au_rows += len(all_au)
                log.info(f"  [AU/{os_type}] loaded {len(all_au)} rows")
            except Exception as e:
                log.error(f"  [AU/{os_type}] load error: {e}")
        else:
            log.info(f"  [AU/{os_type}] no data returned")

        au_done.add(key)
        _save_checkpoint({"au_done": list(au_done), "ret_done": list(ret_done)})
        log.info(f"  Running totals — calls={call_count}  AU_rows={total_au_rows}  RET_rows={total_ret_rows}")

    # ── Retention — iterate by (platform_app_id, os) like AU ────────────────
    for idx, (platform_app_id, unified_app_id, os_type) in enumerate(platform_rows, 1):
        log.info(f"── RET {idx}/{len(platform_rows)}: {platform_app_id} ({os_type}) ──")

        key = f"{platform_app_id}|{os_type}"
        if key in ret_done:
            log.info(f"  [RET] already done — skip")
            continue

        all_ret: list[dict] = []
        for chunk_start, chunk_end in chunks:
            log.info(f"  [RET] {chunk_start} -> {chunk_end}")
            raw = _safe_fetch_ret(platform_app_id, chunk_start, chunk_end, base_delay)
            call_count += 1
            rows = _parse_retention(raw)
            if rows:
                all_ret.extend(rows)

        if all_ret:
            try:
                load_retention(all_ret, os=os_type)
                total_ret_rows += len(all_ret)
                log.info(f"  [RET] loaded {len(all_ret)} rows")
            except Exception as e:
                log.error(f"  [RET] load error: {e}")
        else:
            log.info(f"  [RET] no data returned")

        ret_done.add(key)
        _save_checkpoint({"au_done": list(au_done), "ret_done": list(ret_done)})
        log.info(f"  Running totals — calls={call_count}  AU_rows={total_au_rows}  RET_rows={total_ret_rows}")

    log.info("=" * 60)
    log.info(f"DONE — {call_count} API calls, {total_au_rows} AU rows, {total_ret_rows} retention rows")
    log.info(f"Checkpoint: {CHECKPOINT_FILE}")
    log.info(f"Log: {LOG_FILE}")
    log.info("=" * 60)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch active_users + retention for all mapped games")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Base delay in seconds between API calls (default 2.0)")
    parser.add_argument("--reset", action="store_true",
                        help="Ignore checkpoint and restart from scratch")
    args = parser.parse_args()
    run(base_delay=args.delay, reset=args.reset)
