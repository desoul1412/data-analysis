# Market Intelligence Pipeline Summary

## 1. Accomplishments & Pipeline Fixes

The primary goal was to complete the automated ingestion of historical Sensor Tower data into our DuckDB-based market intelligence system, map internal company games to Sensor Tower's `unified_app_id`, and execute the analytics pipeline (benchmarking, retention, LTV, and PnL).

We achieved the following:
*   **Fixed API Extraction**: Stabilized the pipeline by introducing retry-with-backoff logic to handle transient DuckDB lock contention during bulk data loads. Skips were added for malformed API responses (missing required keys).
*   **Resolved Benchmarking Join Issue**: The initial `analytics.py` benchmarking model failed to match games. We identified a key mismatch: `fact_market_insights` uses OS-specific bundle IDs (e.g., `com.example.app`), whereas `dim_company_games` stores Sensor Tower's `unified_app_id`. An intermediary JOIN via `dim_apps` was implemented to bridge `unified_app_id` to the respective `app_id` correctly.
*   **Internal Data Import Fixes**:
    *   **Filtered Revenue Rows**: The `import_company_data.py` script was importing all metric rows from `Game Performance.csv` (e.g., Installs, MKT/Revenue, Cost). Updated the script to strictly filter for `Unnamed: 3 == 'Game Revenue (5)'` to ensure `fact_company_revenue` only holds revenue figures.
    *   **Grand Total Recalculation**: Discovered that `Grand Total` was often missing (`NaN`) in the CSV exports. Implemented a fallback algorithm to dynamically sum months `M0` through `M36` during the import phase to synthesize the `grand_total` metric.
*   **Pipeline Enrichment**: Updated `pipeline_runner.py`'s company game extraction loop to fetch not only Sales Estimates but also Active Users (DAU/MAU) and Cohort Retention, providing the required inputs to feed the LTV and retention analytic models.

## 2. Errors Encountered & Solutions

| Error / Issue | Root Cause | Solution Applied |
| :--- | :--- | :--- |
| **Empty Analytics Output** | The `fact_company_revenue` table had empty data or `NaN` values for `grand_total`. | Updated `import_company_data.py` to filter for specifically `Game Revenue (5)` rows and manually aggregate `M0 - M36` to synthesize `grand_total`. |
| **Benchmarking SQL Miss** | `compute_benchmark_accuracy` tried to join `dim_company_games` to `fact_market_insights` directly on `unified_app_id = app_id`. | Altered the JOIN clause to `dim_company_games cg JOIN dim_apps da ON cg.unified_app_id = da.unified_app_id JOIN fact_market_insights mi ON da.app_id = mi.app_id`. |
| **DuckDB Connection Locks** | Concurrent connections competing for the `.duckdb` lock during extraction looping (`BinderException` / Lock errors). | Standardized connection handling via a `_con()` wrapper using `duckdb.connect` with robust backoff-and-retry logic inside `st_load.py`. |
| **Pipeline Runner LTV/Retention Missing** | The gap filler was omitting critical user data that models rely on. | Appended `fetch_active_users` and `fetch_retention` calls with inner-loop iterations inside `pipeline_runner.py`. |
| **Key Error in API loading** | Short-form keys (`ca`, `cc`, `d`) occasionally missing from sparse ST responses. | Subbed with `.get('key', default)` safety patterns inside `st_load.py`. |

## 3. Optimizing Benchmarking Results

Currently, out of **93 benchmarked models**, only **2 are accurate, 1 is acceptable, and 90 are flagged as unreliable**. To optimize and improve the correlation between Sensor Tower estimates and internal actuals, consider the following targeted adjustments:

1.  **Platform Revenue Mapping (PC/Web Top-ups vs. Store IAP)**:
    Sensor Tower estimates *only* track revenue flowing through App Store and Google Play channels. If your internal revenue (`grand_total`) includes Direct-to-Consumer (DTC) web top-ups, PC clients, or third-party stores (e.g., Huawei, Galaxy Store, Codashop), the internal actuals will vastly outpace ST estimates. 
    *Recommendation*: Align internal actuals by splitting out App Store / Google Play specific net revenue before benchmarking.
2.  **Gross vs. Net Revenue Matching**:
    Sensor Tower presents estimates as *Gross Revenue* (what the consumer paid, including platform cuts and taxes). Your internal dataset's `Grand Total` might be *Net Revenue* (after 30% cuts). 
    *Recommendation*: Ensure you are converting ST's Gross Revenue to an estimated Net equivalent (e.g., `ST_Estimate * 0.7`) to do an apples-to-apples variance calculation.
3.  **Market/Country Granularity Splitting**:
    Internal data maps regions (e.g., `TW-HK`, `Sing-Malay`), whereas ST provides ISO country data (`TW`, `HK`, `SG`, `MY`). Grouping must account accurately for exactly the matching regions without double counting duplicates across Android vs iOS.
4.  **Historical Date Horizon Misalignment**:
    Ensure the `fact_market_insights` database has full historic API extractions mapping the entirety of your internal games' lifetimes (since soft-launch). If internal revenue accounts for 3 years, but ST API was only pulled for the last 1 year, the benchmark will severely under-index. Use `pipeline_runner.py extract` forcing an older `start_date` for newly mapped items.
