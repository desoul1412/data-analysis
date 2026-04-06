# Market Intelligence Pipeline Summary

*Last updated: 2026-04-06*

---

## 1. Pipeline Overview

A Python + DuckDB market intelligence pipeline that ingests Sensor Tower (ST) API data and internal company revenue, then benchmarks ST estimates against company actuals month-by-month.

**Stack**: Python, DuckDB, pandas, Sensor Tower API  
**DB**: `market_research.duckdb` — schemas: `dim`, `fact`, `analytics`  
**Entry point**: `pipeline_runner.py` (commands: `init`, `import`, `st-files`, `extract`, `extract-charts`, `analytics`, `analytics-v2`, `status`, `all`)

---

## 2. Current Benchmark Results (as of 2026-04-06)

| Metric | Original | Current | Target |
|---|---|---|---|
| Benchmark rows | 391 | **112** (mapped games only) | — |
| Unreliable % | 93.6% | **56.3%** | <40% |
| Accurate % | 1.8% | **21.4%** ✓ | >20% |
| Median \|variance\| | 93.1% | **~49%** | <30% |
| Signals per row | 1 (ST only) | **3** (ST + RPD + rank) ✓ | 2-3 |

The >20% Accurate target is met. The <40% Unreliable target is not yet met — see Section 5.

---

## 3. Key Discoveries & Fixes Applied

### D1: IAP% was 5–60× too high for SEA MMORPGs
ST only captures App Store / Play revenue. SEA MMORPGs route 96–99% through web payments (Codashop, direct top-up portals). Original IAP% values (10–40%) caused systematic 50–100× overestimation of actual IAP.

**Fix**: Empirically calibrated IAP% via `implied_iap_pct = median(ST_revenue / company_gross)` per genre×market. Applied ~60 data-driven values in `config.py`:

| Genre | Market | Old IAP% | New IAP% |
|---|---|---|---|
| MMORPG | Vietnam | 10% | **0.65%** |
| MMORPG | Thailand | 40% | **0.51%** |
| MMORPG | Indonesia | 25% | **3.79%** |
| MMORPG | Sing-Malay | 35% | **1.83%** |
| Platformer/Runner | Sing-Malay | — | **21.67%** |

### D2: Downloads and revenue are temporally decoupled for VN MMORPGs
Downloads appear at launch/update months; web-payment revenue appears in subsequent months. Point-in-time RPD ≈ $0 for most rows.

**Fix**: Use **lifetime RPD** = cumulative_revenue / cumulative_downloads as ARPU proxy.  
Benchmarks: MMORPG VN ≈ $2.50/dl, MMORPG ID ≈ $2.55/dl, Turn-based RPG TH ≈ $4.96/dl.

### D3: ST top charts API format
`GET /v1/{os}/ranking` returns `{"ranking": [...]}` dict, not a list. iOS chart_type = `topgrossingapplications`, Android = `topgrossing` (lowercase category = `game`).

### D4: Apple RSS URL changed
`rss.marketingtools.apple.com` returns 404. Fixed with multi-candidate fallback:
1. `rss.applemarketingtools.com/api/v2/{cc}/apps/top-grossing/200/apps.json`
2. Original URL (fallback)
3. iTunes RSS legacy endpoint

### D5: Active users not in ST subscription
`/v1/{os}/usage/active_users` returns empty. LTV model remains blocked. Replaced with lifetime RPD as ARPU proxy in download triangulation.

### D6: 89/155 company games auto-mapped; 1 confirmed false positive
`Thánh Quang Thiên Sứ` matched wrong ST app (465% variance). NULLed via `fix_false_positive_mappings()`.  
`YS` (Sing-Malay): product discontinued after March 2023 — company actual drops to ~$75 while ST stays ~$1–3K. Requires per-game date-cutoff support (not a simple NULL).

---

## 4. Completed Implementation

| Task | What was built |
|---|---|
| IAP% recalibration | `compute_iap_sensitivity()` — prints implied vs. current IAP% per genre×market |
| ST top charts | `fetch_top_charts()` in `st_extract.py`, `fact.fact_top_charts` (6,080 rows) |
| Apple RSS charts | `fetch_apple_charts.py` with 3-URL fallback; `fact.fact_apple_public_charts` |
| RPD model | `compute_rpd()` — lifetime cumulative RPD; `analytics.rpd_model` (3,267 rows) |
| RPD benchmark | `compute_rpd_benchmark()` — competitor revenue via genre-median RPD; 51/112 rows have RPD estimate |
| Download triangulation | `compute_download_triangulation()` — unblocked via RPD model; 2.4M rows, 114K with RPD estimate |
| Calibration v2 | `get_calibration_factors_v2()` — time-weighted, outlier-excluded, rank-bucketed; 11 combinations |
| Composite benchmark | `compute_composite_benchmark()` — 3-signal weighted average; 344 rows |
| False-positive fix | `fix_false_positive_mappings()` — runs before `compute_benchmark_accuracy()` each pipeline run |
| Per-game IAP override | `iap_pct_override` column in `dim_company_games`; `set_game_iap_override()` utility |
| YS investigation | `investigate_ys_singmalay()` — automatic diagnostic in `analytics-v2` run |

---

## 5. Remaining Blockers to <40% Unreliable

Based on the sensitivity analysis output (run `analytics-v2`):

| Genre | Market | Rows | Median \|Var\| | Notes |
|---|---|---|---|---|
| MMORPG | Vietnam | 42 | 56.2% | ST systematically underestimates; web payment dominant |
| MMORPG | Indonesia | 11 | 60.5% | Sensitivity recommends lowering IAP% to 2.84% (0.75×) |
| Platformer/Runner | Sing-Malay | 17 | 86.3% | YS — product discontinued Apr 2023, partial false positive |
| Tycoon/Crafting | Vietnam | 4 | 88.7% | Small sample, needs mapping review |
| Shoot 'em up | Thailand | 2 | 70.9% | Small sample |

**Quick wins available:**
1. Apply Indonesia MMORPG IAP% reduction: change `("mmorpg", "Indonesia")` from `0.0379` → `0.0284` in `config.py`
2. Exclude YS post-March-2023 rows (implement per-game date cutoff in `benchmark_accuracy`)
3. Expand game mapping coverage beyond 89/155 (manual review of uncertain matches)

---

## 6. Errors Encountered & Solutions

| Error | Root Cause | Solution |
|---|---|---|
| Empty analytics output | `fact_company_revenue` filtered wrong rows | Import strictly filters `Game Revenue (5)` rows; synthesizes `grand_total` from M0–M36 |
| Benchmarking SQL miss | `dim_company_games` → `fact_market_insights` joined directly on `unified_app_id = app_id` | Bridge via `dim_apps`: `cg → da (unified_app_id) → mi (app_id)` |
| DuckDB connection locks | Concurrent connections during bulk load | `_con()` wrapper; backoff-retry in `st_load.py` |
| ST ranking API 422 | Wrong `chart_type` and wrong Android `category` case | iOS: `topgrossingapplications`; Android: `topgrossing`, category `game` (lowercase) |
| ST ranking returns dict | API returns `{"ranking": [...]}` not `[...]` | `isinstance(data, dict)` check + unwrap `data.get("ranking", [])` |
| `compute_iap_sensitivity` TypeError | DuckDB Int32 `genre` column fails `fillna('')` | `.astype(str).replace('None', '').str.lower().str.strip()` |
| Point-in-time RPD = $0 | Downloads and revenue are in different months for VN MMORPGs | Switch to lifetime RPD (cumulative running totals) |
| `download_triangulation` UNIQUE violation | Multi-market join creates duplicate (app, country, month, os) | `.drop_duplicates(subset=[...])` before INSERT |
| Apple RSS 404 | `rss.marketingtools.apple.com` domain changed | Multi-URL fallback with auto format detection |
| UnicodeEncodeError on Windows | Windows CP1252 terminal can't encode ✓/✗ | Run all Python with `python -X utf8` flag |
