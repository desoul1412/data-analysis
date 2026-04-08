# Market Share & Competitive Position Analytics Plan

## Goal

Build a market share analytics layer on top of the existing benchmark and genre data. Answer: *"What % of each genre's total SEA market does each company game hold, and how concentrated is each genre?"*

---

## Why This Stage

The benchmark accuracy module established **how well ST estimates match company actuals**. This module extends that foundation to produce **competitive intelligence**:

- `benchmark_accuracy` → "Is our revenue estimate reliable?"
- `market_share` → "What share of the total genre pie do our games hold?"
- `genre_pnl_template` → "What is the total addressable market by genre?"

All required data is already in the DB — no new API calls needed.

---

## Data Inputs (Already Available)

| Source | Table | What it provides |
|---|---|---|
| Company actuals | `fact_company_revenue` | Actual gross revenue per game × market × month |
| IAP config | `config.IAP_PCT` | Genre × market IAP fraction (calibrated) |
| Genre market total | `fact_genre_summary` | Total downloads + revenue per genre-category × country × month |
| Store total | `fact_store_summary` | Total game store revenue per country × month (TAM) |
| Top charts | `fact_top_charts` | Top 100 app rankings per market (for HHI proxy) |
| Mapped games | `dim_company_games` + `dim_apps` | unified_app_id for mapped games |
| ST estimates | `fact_market_insights` | Per-app revenue estimates for mapped games |

**Known gap**: `fact_genre_summary.category_id` uses ST game sub-category codes (e.g. `7014` for RPG), not the company genre labels (e.g. `MMORPG`). A mapping table bridges this.

---

## New Tables

### `analytics.market_share`

```sql
CREATE TABLE analytics.market_share (
    product_code        VARCHAR,
    product_name        VARCHAR,
    genre               VARCHAR,           -- company genre label
    market              VARCHAR,           -- company market name (e.g. 'Vietnam')
    month               DATE,
    company_gross_usd   DOUBLE,            -- from fact_company_revenue (m-column unpivoted)
    company_iap_usd     DOUBLE,            -- gross × IAP_PCT (ST-comparable portion)
    genre_iap_usd       DOUBLE,            -- genre total IAP revenue from fact_genre_summary
    market_share_pct    DOUBLE,            -- company_iap_usd / genre_iap_usd × 100
    rank_in_genre       INTEGER,           -- rank vs other company games in same genre×market×month
    trend_3m            DOUBLE,            -- share delta vs 3 months prior (pp)
    computed_at         TIMESTAMP DEFAULT current_timestamp,
    UNIQUE(product_code, market, month)
)
```

### `analytics.genre_concentration`

```sql
CREATE TABLE analytics.genre_concentration (
    genre               VARCHAR,           -- company genre label
    market              VARCHAR,
    month               DATE,
    genre_iap_usd       DOUBLE,            -- total genre revenue (IAP-equivalent)
    top1_share_pct      DOUBLE,            -- top game's share
    top5_share_pct      DOUBLE,            -- top 5 combined share
    top10_share_pct     DOUBLE,            -- top 10 combined share
    hhi_proxy           DOUBLE,            -- sum of squares of top-chart rank weights (HHI proxy)
    concentration_tier  VARCHAR,           -- 'HIGH', 'MEDIUM', 'LOW'
    computed_at         TIMESTAMP DEFAULT current_timestamp,
    UNIQUE(genre, market, month)
)
```

---

## New Functions in `analytics.py`

### 1. `compute_market_share(db_path=None)`

**Logic:**

1. Unpivot `fact_company_revenue` from wide M-columns to long (product_code, market, month, gross_usd)
2. Join `dim_company_games` to get `genre`, apply `IAP_PCT[genre, market]` → `company_iap_usd`
3. Join `fact_genre_summary` via **genre→category_id mapping** (see §Genre Mapping below), aggregate iOS+Android revenue → `genre_iap_usd`
4. Compute `market_share_pct = company_iap_usd / genre_iap_usd * 100`
5. Rank within genre × market × month (`rank_in_genre`)
6. Compute `trend_3m` = current share − share 3 months prior
7. Upsert into `analytics.market_share`

**Edge cases:**
- `genre_iap_usd = 0` → NULL share (avoid divide by zero)
- `company_iap_usd > genre_iap_usd` → cap at 100% and flag (data quality issue — genre mapping may be wrong)
- Games without `unified_app_id` still get market share via company actuals (doesn't require ST mapping)

### 2. `compute_genre_concentration(db_path=None)`

**Logic:**

1. For each genre × market × month: pull `fact_top_charts` top 10 app_ids
2. Join `fact_market_insights` to get ST revenue for each top-chart game
3. Sum top 1/5/10 revenues as % of total genre revenue
4. HHI proxy: `SUM((rev_i / total_rev)^2)` for top 10
5. Tier: HHI > 0.25 → 'HIGH', > 0.10 → 'MEDIUM', else 'LOW'
6. Upsert into `analytics.genre_concentration`

**Note:** This uses ST revenue directly (not IAP-adjusted), so it measures ST-visible concentration, not total market concentration. Document this caveat.

### 3. `_genre_to_st_category()` — internal helper

Maps company genre labels to ST game sub-category IDs. Used to join `fact_genre_summary`.

```python
_GENRE_TO_ST_CATEGORY = {
    "MMORPG":             ["7003"],   # Role Playing
    "Turn-based RPG":     ["7003"],
    "Idle RPG":           ["7003"],
    "Squad RPG":          ["7003"],
    "MOBA":               ["7001"],   # Action
    "Shoot 'em Up":       ["7001"],
    "Platformer / Runner":["7015"],   # Racing / Action
    "Tycoon / Crafting":  ["7009"],   # Simulation
    "Battle Royale":      ["7001"],
}
```

*(Exact ST sub-category IDs to verify against actual `fact_genre_summary.category_id` values in DB before finalizing.)*

---

## Genre Mapping Verification Task

Before implementation: query the DB to see what `category_id` and `category_name` values actually exist in `fact_genre_summary`, then finalize `_GENRE_TO_ST_CATEGORY`.

```sql
SELECT DISTINCT category_id, category_name, COUNT(*) as rows
FROM fact.fact_genre_summary
GROUP BY 1, 2
ORDER BY 3 DESC
```

---

## Pipeline Integration

New command: `analytics-market`

```
python pipeline_runner.py analytics-market
```

Runs:
1. `compute_market_share()`
2. `compute_genre_concentration()`

Also add both calls to `cmd_analytics_v2()` after composite benchmark.

---

## Success Criteria

| Metric | Target |
|---|---|
| `analytics.market_share` populated | All games × months with company revenue |
| Market share % reasonable | 0–100%, no NULL genre_iap_usd for MMORPG/VN/TH/ID |
| Genre concentration computed | At least 6 markets × 5+ genres |
| Top company game identified | Highest market_share_pct game per genre × market surfaced |

---

## Dependency Graph

```
fact_company_revenue (already populated)
fact_genre_summary   (already populated)     → compute_market_share() → analytics.market_share
dim_company_games    (155/155 genres set)

fact_top_charts      (already populated)     → compute_genre_concentration() → analytics.genre_concentration
fact_market_insights (already populated)
```

No new API calls. No ST token required.

---

## Implementation Order

| Step | File | Task |
|---|---|---|
| 1 | `db_init.py` | Add `analytics.market_share` + `analytics.genre_concentration` tables |
| 2 | `analytics.py` | Add `_GENRE_TO_ST_CATEGORY` mapping (after DB query verification) |
| 3 | `analytics.py` | `compute_market_share()` |
| 4 | `analytics.py` | `compute_genre_concentration()` |
| 5 | `pipeline_runner.py` | `cmd_analytics_market()` + wire into `analytics-v2` |
| 6 | `benchmark-accuracy-improvement.md` → rename/replace with `market-intel-status.md` | Update project status |
