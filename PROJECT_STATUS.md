# Mobile Game Market Intelligence Platform — Project Status

**Last updated**: 2026-04-08
**Stack**: Python, DuckDB, pandas, Sensor Tower API

---

## 1. Project Overview

This platform provides competitive intelligence for a mobile game portfolio operating across Southeast Asia (VN, TH, ID, PH, SG, MY, TW, HK). It answers three key questions:

1. **How accurate are our market estimates?** (Benchmarking)
2. **What share of each genre do our games hold?** (Market Share & Concentration)
3. **Where should we invest next?** (PnL / Forecasting)

---

## 2. Implementation Phases

| Phase | Name | Status | Key Output |
|---|---|---|---|
| P1 | Database Init & Company Data Import | Done | 155 games, M0-M36 revenue curves |
| P2 | Sensor Tower API Extraction | Done | 4.9M market insight rows |
| P2+ | Historical ST Flat Files (2019-01 to 2026-01) | Done | 1.1M dim_apps entries |
| P3 | Benchmark Accuracy | Done | 86 accuracy comparisons |
| P3b | RPD Model (Revenue Per Download) | Done | 2,700 RPD data points |
| P4 | Cohort Retention | Done | 16,282 retention curve rows |
| P5 | LTV Model | Done | 34 LTV rows (RPD-based) |
| P6 | Genre PnL Template | Done | 20,382 genre-level PnL rows |
| P7 | Market Share & Concentration | Done | 2,811 share + 20,382 concentration rows |
| P8 | PnL / Forecasting Core | Done | 126 forecast + 2,811 game PnL rows |
| P9 | Reporting Layer | Done | 8-sheet Excel report generator |

---

## 3. Data Inventory

### 3.1 Dimension Tables

| Table | Rows | Description |
|---|---|---|
| `dim.dim_apps` | 1,133,986 | Sensor Tower app metadata (name, publisher, category, ratings) |
| `dim.dim_company_games` | 155 | Internal game registry with genre, market, IAP% override, ST mapping |
| `dim.dim_genre_taxonomy` | 22,936 | ST genre/sub-genre classification |

### 3.2 Fact Tables

| Table | Rows | Description |
|---|---|---|
| `fact.fact_market_insights` | 4,909,799 | Per-app revenue + downloads, 20 countries, Jan 2019 - Mar 2026 |
| `fact.fact_company_revenue` | 155 | Company actuals in M0-M36 wide format per product x market |
| `fact.fact_top_charts` | 6,080 | Top 100 grossing apps per country (Apr 2026 snapshot) |
| `fact.fact_apple_public_charts` | 800 | Apple RSS public chart data for mapping validation |
| `fact.fact_retention` | 4,302 | App-level retention data from ST |
| `fact.fact_store_summary` | 30 | Total app store revenue per country (5 months) |
| `fact.fact_genre_summary` | 30 | Genre-level revenue from ST (5 months, Games overall only) |

### 3.3 Analytics Tables

| Table | Rows | Description |
|---|---|---|
| `analytics.benchmark_accuracy` | 86 | ST estimate vs company actual per game x month |
| `analytics.composite_benchmark` | 209 | 3-signal weighted estimate (ST + synthetic + rank) |
| `analytics.calibration_factors_v2` | 9 | Genre x market calibration multipliers |
| `analytics.rpd_model` | 2,700 | Revenue-per-download per app x country x month |
| `analytics.rpd_benchmark` | 86 | RPD-derived revenue estimates vs actuals |
| `analytics.download_triangulation` | 2,400,158 | Synthetic revenue signal from download x RPD |
| `analytics.cohort_retention` | 16,282 | Genre-average retention curves (RPG, Action, Shooter, Simulation, Strategy) |
| `analytics.ltv_model` | 34 | Cumulative LTV per genre x country (RPD-based ARPDAU proxy) |
| `analytics.genre_pnl_template` | 20,382 | Genre TAM, growth, HHI, opportunity score per country x month |
| `analytics.market_share` | 2,811 | Per-game % share of genre IAP market + total store |
| `analytics.portfolio_summary` | 1,712 | Genre x market x month portfolio aggregation |
| `analytics.genre_concentration` | 20,382 | HHI, top1/5/10 share per ST category x country x month |
| `analytics.revenue_forecast` | 126 | M0-M24 calibrated revenue curves per genre x market |
| `analytics.game_pnl` | 2,811 | Per-game PnL with opportunity scoring |

---

## 4. Benchmarking Methodology

### 4.1 Problem

Company revenue includes all channels (web, direct, IAP), but Sensor Tower only tracks in-app purchases (IAP). A direct comparison is meaningless without calibration.

### 4.2 IAP Percentage Calibration

Each (genre, market) pair has an empirically calibrated `IAP_PCT` — the fraction of company gross revenue that is IAP-comparable:

```
company_iap_usd = company_gross_usd * IAP_PCT[genre, market]
```

Examples:
- MMORPG / Vietnam: 0.65% (web payment dominance — ST only sees a tiny slice)
- Platformer / Sing-Malay: 21.7% (higher IAP fraction)
- TW-HK default: 85% (predominantly IAP-driven market)

Calibrated from `implied_iap_pct = median(ST_estimate / company_gross)` per genre x market, with N >= 2 observations required.

### 4.3 Three-Signal Composite Benchmark

Revenue estimates use three independent signals, weighted by availability and reliability:

| Signal | Source | Method |
|---|---|---|
| **ST Revenue** | `fact_market_insights` | Direct ST revenue estimate for mapped apps |
| **Synthetic (RPD)** | `analytics.download_triangulation` | `downloads x median_RPD` per genre x country — creates a revenue proxy from download volume |
| **Rank-based** | `fact_top_charts` | Position in top-grossing chart implies revenue range |

The composite estimate:
```
composite_mid = weighted_average(st_rev, synth_rev, rank_rev)
composite_variance = |composite_mid - actual_iap| / actual_iap * 100
```

**Accuracy tiers** (209 benchmarks):
- Accurate (|variance| <= 25%): 33 observations (16%)
- Acceptable (25-50%): 54 observations (26%)
- Unreliable (>50%): 122 observations (58%)

### 4.4 Calibration Factors

Per genre x market correction multipliers derived from the composite benchmark:

| Genre | Market | Factor | Sample |
|---|---|---|---|
| MMORPG | Vietnam | 1.38x | 29 obs |
| MMORPG | Indonesia | 4.01x | 10 obs |
| MMORPG | ThaiLand | 1.23x | 7 obs |
| Turn-based RPG | Vietnam | 1.10x | 2 obs |
| Platformer / Runner | Sing-Malay | 3.62x | 9 obs |

---

## 5. Forecasting Methodology

### 5.1 Revenue Forecast (M0-M24 Curves)

**Approach**: Company historical revenue curves (M0-M36) normalized to M1, then calibrated.

Steps:
1. Extract all company revenue by genre x market from `fact_company_revenue` (wide M0-M36 format)
2. Compute **median** of each period across all products in that genre x market
3. Normalize to M1: `shape[m] = median(Mm) / median(M1)` — produces a decay curve
4. Apply **IAP_PCT** to convert to ST-comparable revenue
5. Apply **calibration factor** from composite benchmark
6. Generate confidence bands using coefficient of variation (CV):
   - `forecast_low = mid * (1 - CV)`, `forecast_high = mid * (1 + CV)`
   - CV capped at [0.15, 0.60]; sparse genres (< 3 obs) use 0.50

**Example — MMORPG Vietnam** (calibration factor = 1.33x):
- M1: $3,827 | M6: $1,010 | M12: $765 | M24: $378

### 5.2 LTV Model

**Problem**: Standard ARPDAU requires active user data (not available at scale).
**Solution**: Use RPD (Revenue Per Download) as ARPDAU proxy.

```
LTV(n) = Σ(m=0..n) [median_RPD × retention_rate(m)]
```

- `median_RPD` per genre x country from `analytics.rpd_model` (13 apps, 8 countries)
- `retention_rate(m)` from `analytics.cohort_retention` (16K rows, 5 ST categories)
- Produces cumulative LTV at 30-day and 90-day horizons

### 5.3 Genre PnL / Opportunity Scoring

For each ST category x country x month:

| Metric | Source |
|---|---|
| **Genre TAM** | Sum of MI revenue by `dim_apps.category_id` (87 months of history) |
| **TAM Growth** | Genre TAM vs 3 months prior |
| **HHI** | From `genre_concentration` — sum of squared revenue shares for top 10 apps |
| **Concentration Tier** | HIGH (HHI > 0.25), MEDIUM (> 0.10), LOW |
| **LTV** | From LTV model (30d and 90d) |
| **Retention** | D1 and D30 from cohort retention |

**Opportunity score**:
- `HIGH`: TAM growth > 10% AND HHI < 0.25 AND LTV > median
- `LOW`: TAM growth < -5% OR HHI > 0.50
- `MEDIUM`: otherwise

### 5.4 Game-Level PnL

Per-game view combining all signals (2,811 rows, 154 games):

| Field | Source |
|---|---|
| Revenue actuals | `market_share` (company gross + IAP) |
| Market share | `market_share` (% of genre + % of store) |
| Genre TAM + growth | `genre_pnl_template` |
| Concentration | `genre_concentration` (HHI, tier) |
| Revenue forecast | `revenue_forecast` (M0-M24 curves) |
| LTV | `ltv_model` |
| Months since launch | `fact_company_revenue` min(ob_date) |

**Game opportunity score**:
- `HIGH`: share trend > 0 AND TAM growing AND HHI < 0.25
- `LOW`: share trend < -2pp OR TAM declining OR HHI > 0.50

---

## 6. Data Coverage & Quality

### 6.1 Game Mapping

| Metric | Value |
|---|---|
| Total games | 155 |
| Mapped to ST | 105 (67.7%) |
| Revenue coverage | 94.9% of company revenue |
| Unmapped with revenue | 3 games ($1.8K/month — negligible) |

### 6.2 Market Insights Coverage

- **Countries**: 20 (8 SEA + 12 other)
- **Date range**: Jan 2019 - Dec 2025 (87 months with good data)
- **Apps tracked**: ~17K unique per country
- **Revenue data quality**: Good through Nov 2025; Dec 2025 partial; Jan-Mar 2026 sparse

### 6.3 Known Limitations

1. **IAP-only visibility**: ST only tracks App Store / Play Store IAP — web/direct payments invisible. IAP_PCT calibration bridges this gap but introduces uncertainty for MMORPG (0.65% IAP fraction).
2. **TW-HK market**: Company revenue far exceeds ST genre totals for some games (Artillery Shooter, Shooter). 76 share rows nulled due to >100% share.
3. **"Global" market**: Maps to WW country code with no MI data — 123 rows without genre denominator.
4. **Active users**: `fact_active_users` is empty — LTV uses RPD proxy instead of true ARPDAU.
5. **Revenue forecast sparsity**: Only 3 genres (MMORPG, Turn-based RPG, Tycoon/Crafting) have company curve data. Other genres need more launch cohorts.

---

## 7. Pipeline Commands

```bash
# Core pipeline
python pipeline_runner.py init              # Database initialization
python pipeline_runner.py import            # Company CSV data import
python pipeline_runner.py st-files          # Import ST historical flat files
python pipeline_runner.py extract           # ST API gap-fill extraction
python pipeline_runner.py extract-charts    # ST top charts extraction
python pipeline_runner.py extract-apple     # Apple RSS chart validation
python pipeline_runner.py genre-assign      # Auto-assign genres to games

# Analytics
python pipeline_runner.py analytics         # Phase 3-6 (benchmark, RPD, retention, LTV, genre PnL)
python pipeline_runner.py analytics-v2      # Full pipeline (P3-8, includes market share + PnL)
python pipeline_runner.py analytics-market  # Phase 7 only (market share + concentration)
python pipeline_runner.py pnl              # Phase 8 only (LTV + genre PnL + forecast + game PnL)

# Reporting
python pipeline_runner.py report           # Generate Excel report (8 sheets)

# Utilities
python pipeline_runner.py status           # Show table row counts
python pipeline_runner.py all              # Run everything sequentially
```

---

## 8. Next Applications

### 8.1 Immediate (data already available)

- **New game launch scoring**: Use genre PnL opportunity scores to evaluate potential new titles by genre x market. RPG/Vietnam is consistently HIGH — fragmented market with growing TAM.
- **Portfolio rebalancing**: Game PnL identifies games with declining share in growing markets (action needed) vs growing share in declining markets (ride the wave).
- **Competitive alerts**: Monitor `genre_concentration` trends — if top1_share_pct suddenly spikes, a competitor may have launched a breakout title.

### 8.2 Medium-term (needs additional data)

- **Active user-based LTV**: If ST active users endpoint becomes available, replace RPD proxy with true ARPDAU for more precise LTV curves.
- **Ad intelligence integration**: `fact_ad_intel` is empty — when populated, can correlate marketing spend with market share gains.
- **Regional expansion analysis**: Use genre_concentration to identify under-served markets (LOW concentration + growing TAM) for geographic expansion.

### 8.3 Long-term (platform evolution)

- **Real-time dashboards**: Connect DuckDB analytics to BI tool (Metabase, Superset, or Streamlit) for auto-refreshing views.
- **Automated monthly reports**: Schedule `pipeline_runner.py all && pipeline_runner.py report` as a monthly cron job with ST data refresh.
- **Predictive churn signals**: Combine retention decay curves with revenue trends to predict when games will become unprofitable, enabling proactive sunsetting decisions.

---

## 9. Architecture

```
                    ┌─────────────────┐
                    │  Company CSVs   │
                    │ (Game Perf.csv) │
                    └────────┬────────┘
                             │ import
                             ▼
┌───────────────┐   ┌────────────────┐   ┌───────────────────┐
│ Sensor Tower  │──▶│   DuckDB       │◀──│ ST Flat Files     │
│   API         │   │ (market_       │   │ (2019-2026)       │
│ (live fill)   │   │  research.     │   │                   │
└───────────────┘   │  duckdb)       │   └───────────────────┘
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────────┐
        │   dim.*  │  │  fact.*  │  │ analytics.*  │
        │ (apps,   │  │ (MI,    │  │ (benchmark,  │
        │  games)  │  │  rev,   │  │  share, PnL, │
        │          │  │  charts)│  │  forecast)   │
        └──────────┘  └─────────┘  └──────┬───────┘
                                          │
                                          ▼
                                   ┌──────────────┐
                                   │ Excel Report │
                                   │ (8 sheets)   │
                                   └──────────────┘
```
