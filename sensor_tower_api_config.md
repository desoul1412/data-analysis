# Sensor Tower API Configuration Summary

This document summarizes the API configuration for Sensor Tower endpoints, including parameters, response schemas, and recommended use cases for game market research.

**Base URL**: `https://api.sensortower.com`
**Authentication**: Use `auth_token` as a query parameter in all requests.

---

## 1. App Metadata
**Endpoint**: `GET /v1/{os}/apps`
**Description**: Fetches detailed metadata for specific applications (name, publisher, ratings, etc.).

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `app_ids` | array[string] | Query | Yes | Comma-separated App IDs (Limit: 100) |
| `country` | string | Query | No | Country Code (Default: `US`) |
| `include_sdk_data` | boolean | Query | No | Include SDK Insights (Default: `false`) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | Platform-specific app ID |
| `unified_app_id` | string | Cross-platform unified ID |
| `name` | string | App display name |
| `publisher_name` | string | Developer/publisher name |
| `publisher_id` | string | Publisher identifier |
| `release_date` | date | App release date (country-specific) |
| `country_release_date` | date | Release date in requested country |
| `rating` | float | Average star rating |
| `ratings_count` | integer | Total number of ratings |
| `description` | string | App store description |
| `category_id` | string | Primary category ID |
| `icon_url` | string | URL to app icon |
| `price` | float | Current price (0 = free) |
| `in_app_purchases` | boolean | Whether IAPs exist |
| `publisher_country` | string | Publisher's registered country |

### Use Cases
- **`dim_apps` population**: Seed the app dimension table with name, publisher, genre, and release date for all discovered apps.
- **Publisher profiling**: Map apps to publishers; identify which studios build which genres.
- **Competitor research**: Pull metadata for top competitor games to cross-reference with your company's `dim_company_games`.
- **Localization tracking**: Query the same `app_id` across multiple `country` values to detect market-specific adaptations (language, pricing, icon).

---

## 2. Top In-App Purchases
**Endpoint**: `GET /v1/ios/apps/top_in_app_purchases`
**Description**: Retrieves the top performing in-app purchases for the requested App IDs.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `app_ids` | array[string] | Query | Yes | Comma-separated App IDs (Limit: 100) |
| `country` | string | Query | No | Country Code (Default: `US`) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `iap_name` | string | Name of the in-app purchase |
| `iap_type` | string | IAP type (e.g., `Consumable`, `Subscription`) |
| `price` | float | IAP price in USD |
| `rank` | integer | Rank among top IAPs |

### Use Cases
- **Monetization model analysis**: Determine whether top games in a genre monetize via consumables, subscriptions, or one-time purchases.
- **Pricing benchmarking**: Compare IAP price points of competitor games to your own; inform pricing strategy.
- **Genre PnL input**: Estimate ARPU ceiling by analyzing the price distribution of top IAPs in a genre/subgenre.

---

## 3. Sales Report Estimates
**Endpoint**: `GET /v1/{os}/sales_report_estimates`
**Description**: Fetches download and revenue estimates of apps and publishers.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `app_ids` | array[string] | Query | * | IDs of apps (Separated by commas) |
| `publisher_ids` | array[string] | Query | * | Publisher IDs (Use `[]` format for Android IDs with commas) |
| `countries` | array[string] | Query | No | Country codes (use `WW` for Worldwide) |
| `date_granularity` | string | Query | Yes | `daily`, `weekly`, `monthly`, or `quarterly` |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |
| `data_model` | string | Query | No | `DM_2025_Q1` (Legacy) or `DM_2025_Q2` (New) |

*\*At least one `app_ids` or `publisher_ids` is required.*

### Key Response Fields (iOS)
| Field | Type | Description |
| :--- | :--- | :--- |
| `aid` | string | App ID |
| `cc` | string | Country code |
| `d` | date | Date (YYYY-MM-DD) |
| `iu` | long | iPhone downloads |
| `au` | long | iPad downloads |
| `ir` | long | iPhone revenue (cents) |
| `ar` | long | iPad revenue (cents) |

### Key Response Fields (Android)
| Field | Type | Description |
| :--- | :--- | :--- |
| `aid` | string | App ID |
| `c` | string | Country code |
| `d` | date | Date (YYYY-MM-DD) |
| `u` | long | Downloads |
| `r` | long | Revenue (cents) |

### Use Cases
- **Core pipeline extraction**: The primary revenue & download source for `fact_market_insights` and `performance_3m` tables.
- **Launch-period benchmarking**: Fetch M0–M3 daily data for new game launches and compare against your company's `Game Performance.csv` M0–M3 actuals.
- **Revenue forecasting input**: Monthly time series used to fit M0–M36 revenue curve templates per genre.
- **Publisher-level analysis**: Use `publisher_ids` to aggregate total revenue/downloads across a competitor studio's entire portfolio.

---

## 4. Active Users (Usage)
**Endpoint**: `GET /v1/{os}/usage/active_users`
**Description**: Retrieves active user estimates (DAU, WAU, MAU) per country and date.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`, `unified`) |
| `app_ids` | array[string] | Query | Yes | IDs of apps (Max 500) |
| `time_period` | string | Query | Yes | `day` (DAU), `week` (WAU), `month` (MAU) |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `country` | string | Country code |
| `date` | date | Date of the measurement |
| `active_users` | long | Estimated active user count (DAU/WAU/MAU depending on `time_period`) |

### Use Cases
- **ARPDAU calculation**: `revenue_from_ST / active_users` → monetization efficiency per app.
- **User base forecasting**: Fit MAU growth curves → project future engagement for genre PnL.
- **DAU/MAU ratio (stickiness)**: `DAU / MAU` reveals how often users return; a key health metric for competitive benchmarking.
- **Engagement ranking**: Rank games in a genre by MAU to find user-base leaders vs. revenue leaders (identify monetization gap opportunities).

---

## 5. Category Ranking History
**Endpoint**: `GET /v1/{os}/category/category_history`
**Description**: Fetches historical category ranking for specific apps, categories, and chart types.

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `category_id` | string | Category identifier |
| `chart_type` | string | Chart type (e.g., `topfreeapplications`) |
| `country` | string | Country code |
| `date` | date | Date of ranking |
| `rank` | integer | App's rank on that date |

### Use Cases
- **Visibility tracking**: Monitor how a competitor game climbs or falls in the top charts over time.
- **Launch spike detection**: Identify when a new game entered the top 100 and how quickly it rose/fell.
- **Seasonality analysis**: Detect recurring rank patterns around holidays or game events.
- **Correlation analysis**: Join with `fact_market_insights` to quantify the revenue impact of ranking movements.

---

## 6. Retention Metrics
**Endpoint**: `GET /v1/facets/metrics?retention`
**Description**: Retrieve retention metrics (D1..D360, W1..W52, M1..M12) for specific apps.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `bundle` | string | Query | Yes | `retention_daily`, `retention_weekly`, or `retention_monthly` |
| `breakdown` | array[string] | Query | Yes | Fields to break down (e.g., `date,app_id`) |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |
| `app_ids` | array[string] | Query | * | Filter by app IDs |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `date` | date | Cohort install month |
| `est_retention_d1` | float | Day-1 retention rate (0–1) |
| `est_retention_d7` | float | Day-7 retention rate |
| `est_retention_d30` | float | Day-30 retention rate |
| `est_retention_d90` | float | Day-90 retention rate |
| `est_retention_w1` | float | Week-1 retention rate |
| `est_retention_m1` | float | Month-1 retention rate |
| `est_retention_m3` | float | Month-3 retention rate |

### Use Cases
- **LTV modeling input**: D1/D7/D30 retention rates are core inputs for LTV formula: `LTV ≈ ARPDAU × f(retention_curve)`.
- **Genre retention benchmarks**: Compare D30 retention of top RPG games vs. Puzzle games → understand structural differences for PnL.
- **Competitive quality signal**: High D7 retention in a competitor signals strong early-game hook; a red flag for similar genre games you operate.
- **Revenue curve forecasting**: Retention decay shape directly predicts how the M0–M36 revenue curve will flatten.

---

## 7. App Updates & Version History

### A. App Update Timeline
**Endpoint**: `GET /v1/{os}/app_update/get_app_update_history`
**Description**: Fetches detailed update history (metadata changes, pricing, descriptions, screenshots, events).

| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `app_id` | string | Query | Yes | Single App ID |
| `country` | string | Query | No | Country Code (Default: `US`) |
| `date_limit` | string | Query | No | Number of days from today to start timeline |

### B. Version History (Release Notes)
**Endpoint**: `GET /v1/{os}/apps/version_history`
**Description**: Fetches the chronological version history including release notes ("What's New").

| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `app_id` | string | Query | Yes | Single App ID |
| `country` | string | Query | No | Country Code (Default: `US`) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `version` | string | App version string (e.g., `2.4.1`) |
| `release_date` | date | Version release date |
| `release_notes` | string | "What's New" text |
| `update_type` | string | Type of update (metadata, price, screenshot, description) |
| `price_change` | object | `from` and `to` price if changed |
| `icon_change` | boolean | Whether icon was updated |
| `screenshot_change` | boolean | Whether screenshots changed |

### Use Cases
- **Competitor update cadence**: How often does a top game ship updates? Frequent updates correlate with higher engagement.
- **Monetization event detection**: Detect when a competitor changed price or added IAP — correlate with `fact_market_insights` revenue spikes.
- **Feature discovery**: Parse release notes to identify new features (events, modes, content) before they show in revenue data.
- **Marketing event timeline**: Screenshot/icon changes often signal a UA campaign — cross-reference with `fact_ad_intel`.

---

## 8. Downloads by Sources (Unified Data)
**Endpoint**: `GET /v1/{os}/downloads_by_sources`
**Description**: Fetches breakdown of download channels (Organic Browse/Search, Paid Ads/Search, Browser).

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | `unified`, `ios`, or `android` |
| `app_ids` | array[string] | Query | Yes | Unified App IDs |
| `countries` | array[string] | Query | Yes | Country codes (e.g., `WW`, `US`) |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | Unified App ID |
| `country` | string | Country code |
| `date` | date | Date |
| `organic_browse` | long | Downloads from organic browsing |
| `organic_search` | long | Downloads from organic search |
| `paid_ads` | long | Downloads attributed to paid advertising |
| `paid_search` | long | Downloads from paid search (Apple Search Ads) |
| `web_referral` | long | Downloads from web/browser referral |

### Use Cases
- **UA efficiency assessment**: High `paid_ads` share → game is dependent on paid acquisition; assess sustainability.
- **Organic health**: High `organic_search` share → strong ASO or brand recognition; less marketing cost risk.
- **SEA market comparison**: Compare organic vs. paid mix across VN/TH/ID/PH to tailor UA spend by market.
- **Paid search impact**: Quantify Apple Search Ads contribution → benchmark against competitor spend from #30.

---

## 9. Demographics
**Endpoint**: `GET /v1/{os}/usage/demographics`
**Description**: Retrieve gender and age range breakdown of app users.

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `country` | string | Country code |
| `gender` | string | `male` or `female` |
| `age_range` | string | Age bucket (e.g., `18-24`, `25-34`) |
| `share` | float | Proportion of users in this segment (0–1) |

### Use Cases
- **Target audience validation**: Confirm your game actually reaches the demographic it was designed for.
- **Cross-genre comparison**: RPG games skew male 25–34; Puzzle games skew female 25–44 — use to plan genre entry.
- **SEA market profiling**: Demographics vary significantly between VN, ID, and PH — important for localization and UA creative targeting.
- **Creative brief input**: Feed `gender` + `age_range` data into UA creative briefs for competitor genre targeting.

---

## 10. Session Metrics (Time Spent / Session Count)
**Endpoints**:
- Regular Apps: `GET /v1/apps/timeseries`
- Unified Apps: `GET /v1/apps/timeseries/unified_apps`

**Description**: Retrieve engagement metrics like `time_spent`, `session_duration`, and `session_count`.

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `date` | date | Date |
| `country` | string | Country code |
| `time_spent` | long | Total time spent in-app (seconds) per day |
| `session_count` | long | Total number of sessions per day |
| `session_duration` | float | Average session duration (seconds) |
| `sessions_per_dau` | float | Average sessions per daily active user |

### Use Cases
- **Engagement quality**: `session_duration` and `sessions_per_dau` reveal how immersive a game is → competitive depth indicator.
- **Genre DAU comparison**: A strategy game with 30-min sessions vs. a casual game with 3-min sessions → different monetization models.
- **Retention predictor**: Declining `time_spent` per DAU is an early churn signal before it shows in revenue.
- **PnL input**: `time_spent × ARPDAU_per_hour` can proxy monetization efficiency for premium/IAP games.

---

## 11. Ad Intel (Creatives & SOV)
**Endpoints**:
- Creatives: `GET /v1/{os}/ad_intel/creatives`
- SOV: `GET /v1/{os}/ad_intel/network_analysis`
- Rank: `GET /v1/{os}/ad_intel/network_analysis/rank`

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `network` | string | Ad network name (e.g., `Admob`, `Facebook`) |
| `sov` | float | Share of Voice (0–1) |
| `impressions` | long | Estimated ad impressions |
| `creative_url` | string | URL to the creative asset |
| `creative_type` | string | `video`, `image`, `html`, `banner` |
| `first_seen` | date | Date creative first appeared |
| `last_seen` | date | Date creative last appeared |
| `country` | string | Country code |

### Use Cases
- **Competitive UA intelligence**: Identify when a competitor significantly increases ad spend (SOV spike) → likely precedes a revenue spike in `fact_market_insights`.
- **Creative trend analysis**: What messaging/visual styles dominate in a genre → brief your own UA team.
- **Network strategy**: Which networks do top games in your genre prioritize? (Facebook vs. Admob vs. Unity) → inform media mix.
- **Silence detection**: Competitors going dark on ads may signal game decline → opportunity to capture market share.

---

## 12. Top Apps by Active Users
**Endpoint**: `GET /v1/{os}/top_and_trending/active_users`
**Description**: Retrieve top apps ranked by absolute active users, growth, or growth percentage.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `comparison_attribute` | string | Query | Yes | `absolute`, `delta`, or `transformed_delta` |
| `time_range` | string | Query | Yes | `week`, `month`, or `quarter` |
| `measure` | string | Query | Yes | `DAU`, `WAU`, or `MAU` |
| `date` | Date | Query | Yes | YYYY-MM-DD (Should match beginning of range) |
| `category` | integer | Query | No | Category ID (Default: `0` for All) |
| `limit` | integer | Query | No | Apps per call (Default: `25`) |
| `data_model` | string | Query | No | `DM_2025_Q1` or `DM_2025_Q2` |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `name` | string | App display name |
| `active_users` | long | Absolute DAU/WAU/MAU |
| `delta` | long | Change vs. prior period |
| `transformed_delta` | float | % change vs. prior period |
| `rank` | integer | Rank within the result set |
| `country` | string | Country code |

### Use Cases
- **Fast-growing game detection**: Use `transformed_delta` to find games whose MAU is growing fastest → early signal of an emerging hit.
- **Market leader tracking**: Regular pull of top MAU games per genre → build a leaderboard over time.
- **SEA engagement leaders**: Who leads MAU in VN/TH/ID — may not be the same as revenue leaders (different monetization efficiency).

---

## 13. Top Publishers (Revenue & Downloads)
**Endpoint**: `GET /v1/{os}/top_and_trending/publishers`
**Description**: Retrieve top app publishers ranked by downloads (`units`) or revenue.
**Note**: All revenues are returned in **cents**.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `measure` | string | Query | Yes | `units` (Downloads) or `revenue` |
| `time_range` | string | Query | Yes | `day`, `week`, `month`, or `quarter` |
| `date` | Date | Query | Yes | Start date of range (YYYY-MM-DD) |
| `end_date` | Date | Query | No | End date for multi-period aggregation |
| `category` | integer | Query | Yes | Category ID |
| `country` | string | Query | No | Country Code |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `publisher_id` | string | Publisher identifier |
| `publisher_name` | string | Publisher/studio name |
| `revenue` | long | Total revenue across all publisher apps (cents) |
| `units` | long | Total downloads across all apps |
| `rank` | integer | Publisher rank |
| `country` | string | Country code |

### Use Cases
- **Studio market share**: Track VNG's rank among all game publishers in VN, TH, ID — trend over quarterly intervals.
- **Competitive studio watch**: Monitor top studios (e.g., Garena, Moonton) monthly volume to detect expansion patterns.
- **Publisher PnL proxy**: Revenue / downloads per publisher gives a crude ARPD estimate for studio-level benchmarking.

---

## 14. Store Summary (Aggregated Category Data)
**Endpoint**: `GET /v1/{os}/store_summary`
**Description**: Fetches aggregated download and revenue estimates for store categories.
**Note**: All revenues are returned in **cents**.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `categories` | array[string] | Query | Yes | Comma-separated Category IDs |
| `date_granularity` | string | Query | Yes | `daily`, `weekly`, `monthly`, or `quarterly` |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |
| `countries` | array[string] | Query | No | Country codes (Default: `US`, use `WW` for WorldWide) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `category_id` | string | Category identifier |
| `country` | string | Country code |
| `date` | date | Date |
| `revenue` | long | Category total revenue (cents) |
| `units` | long | Category total downloads |

### Use Cases
- **TAM sizing**: Total Addressable Market for a genre in a target country → core input for genre PnL template.
- **Market growth rate**: Monthly category revenue trend → project future TAM for revenue forecasting.
- **Genre mix analysis**: Compare Games vs. Apps revenue share over time within a market.
- **Seasonal adjustments**: Detect Q4 holiday spikes or Lunar New Year effects in SEA markets for PnL forecasting.

---

## 15. Ad Intel Top Apps (Advertisers/Publishers)
**Endpoint**: `GET /v1/{os}/ad_intel/top_apps`
**Description**: Fetches the top advertisers or publishers by Share of Voice (SOV) over a time period.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (usually `unified`) |
| `role` | string | Query | Yes | `advertisers` or `publishers` |
| `period` | string | Query | Yes | `week`, `month`, `quarter`, or `year` |
| `date` | Date | Query | Yes | Start date (YYYY-MM-DD) |
| `category` | integer | Query | Yes | Category ID |
| `network` | string | Query | Yes | Ad Network name (e.g., `Admob`, `Facebook`) |
| `country` | string | Query | Yes | Primary country for results |
| `countries` | array[string] | Query | No | Multi-country filter (Priority over `country` if role is advertisers) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App/Publisher identifier |
| `name` | string | App/Publisher name |
| `sov` | float | Share of Voice (0–1) |
| `rank` | integer | Rank among top advertisers/publishers |
| `impressions` | long | Estimated impressions |
| `networks` | array | Networks where this app advertises |
| `country` | string | Country code |

### Use Cases
- **UA landscape map**: Who are the biggest spenders in a genre in each SEA country? Monthly snapshot builds a competitive UA spending leaderboard.
- **Genre entry cost signal**: High SOV concentration in few players → genre is UA-cost-heavy to penetrate.
- **Network prioritization**: Which network has highest advertiser count in your genre/market → where the audience is.

---

## 16. Ad Intel Rank Search
**Endpoint**: `GET /v1/{os}/ad_intel/top_apps/search`
**Description**: Fetches the specific rank and SOV of an advertiser or publisher matching provided filters.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (usually `unified`) |
| `app_id` | string | Query | Yes | App ID to search for |
| `role` | string | Query | Yes | `advertisers` or `publishers` |
| `date` | Date | Query | Yes | YYYY-MM-DD |
| `period` | string | Query | Yes | `week`, `month`, etc. |
| `category` | integer | Query | Yes | Category ID |
| `country` | string | Query | Yes | Country Code |
| `network` | string | Query | Yes | Ad Network name |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `rank` | integer | Rank in the top advertiser list |
| `sov` | float | Share of Voice for this app |
| `impressions` | long | Estimated impressions |
| `total_advertisers` | integer | Total number of advertisers in the list |

### Use Cases
- **Self-benchmarking**: Check where your own game ranks in SOV for a given genre/network/market → track UA share over time.
- **Specific competitor tracking**: Monitor one competitor's exact UA rank monthly → detect when they scale up or pull back.
- **SOV trend report**: Build a time series of SOV rank for a game → include in competitive intelligence monthly report.

---

## 17. Top Creatives (Advertising)
**Endpoint**: `GET /v1/{os}/ad_intel/creatives/top`
**Description**: Fetches the top-performing ad creatives over a given time period with detailed filtering.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (usually `unified`) |
| `date` | Date | Query | Yes | Start date (YYYY-MM-DD) |
| `period` | string | Query | Yes | `week`, `month`, etc. |
| `category` | integer | Query | Yes | Category ID |
| `country` | string | Query | Yes | Country Code |
| `network` | string | Query | Yes | Ad Network name |
| `ad_types` | array | Query | Yes | Comma-separated (e.g., `video`, `image`, `banner`) |
| `new_creative` | boolean | Query | No | If `true`, returns only creatives first seen in range |
| `limit` | integer | Query | No | Max 250 (Default: 25) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `creative_id` | string | Unique creative identifier |
| `app_id` | string | App this creative belongs to |
| `creative_url` | string | URL to the creative asset |
| `creative_type` | string | `video`, `image`, `html`, `banner` |
| `impressions` | long | Estimated impressions for this creative |
| `sov` | float | Share of impressions this creative holds |
| `first_seen` | date | Date creative first appeared |
| `last_seen` | date | Most recent date seen |
| `duration_seconds` | integer | Video duration (if video type) |
| `network` | string | Network where creative ran |

### Use Cases
- **Creative intelligence**: Download top-performing creatives of genre leaders → understand visual and narrative hooks that resonate.
- **Long-running creative signals**: Creatives with `first_seen` far in the past still running → high-performing "evergreen" assets to study.
- **New creative detection**: `new_creative=true` → monitor competitor fresh creative launches; signals new campaigns or seasonal pushes.
- **Ad format trends**: Are top games shifting toward video over banner? Informs your UA format strategy.

---

## 18. Game Summary (Games Breakdown)
**Endpoint**: `GET /v1/{os}/games_breakdown`
**Description**: Retrieve aggregated download and revenue estimates specifically for game categories.
**Note**: All revenues are returned in **cents**.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `categories` | array | Query | Yes | Comma-separated Game Category IDs |
| `date_granularity` | string | Query | Yes | `daily`, `weekly`, `monthly`, or `quarterly` |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |
| `countries` | array | Query | No | Country codes (Default: `US`, use `WW` for WorldWide) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `category_id` | string | Game subcategory ID |
| `category_name` | string | Category name (e.g., `RPG`, `Puzzle`) |
| `country` | string | Country code |
| `date` | date | Date |
| `revenue` | long | Total revenue for the category (cents) |
| `units` | long | Total downloads for the category |
| `num_apps` | integer | Number of apps contributing |

### Use Cases
- **Genre PnL foundation**: Monthly revenue per genre/subgenre per SEA market → direct input to the genre PnL template.
- **Genre growth ranking**: Which subgenre is growing fastest in revenue in VN/TH/ID? → prioritize genre entry decisions.
- **Market share calculation**: `company_game_revenue / genre_revenue` → your game's market share within its genre.
- **Genre Mix report**: Visualize how revenue is distributed across RPG, Puzzle, Action, Strategy in each SEA market.

---

## 19. App IDs Discovery (by Category/Date)
**Endpoint**: `GET /v1/{os}/apps/app_ids`
**Description**: Retrieve a list of App IDs (up to 10,000) starting from a specific release or update date within a category.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `category` | string | Query | Yes | Category ID (e.g., `6005` for Social Networking) |
| `start_date` | Date | Query | No | YYYY-MM-DD (Minimum Release Date) |
| `updated_date` | Date | Query | No | YYYY-MM-DD (Minimum Updated Date) |
| `limit` | integer | Query | No | Max 10,000 (Default: 1,000) |
| `offset` | integer | Query | No | Results offset |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `ids` | array[string] | List of App IDs matching the filters |
| `date_range` | array[date] | Release date range covered by the result |
| `total` | integer | Total matching apps (for pagination) |

### Use Cases
- **New game discovery**: Core pipeline step — paginate through all new Game IDs released in a month to feed into `extract_new_games`.
- **Category universe building**: Pull all-time game IDs in a category → universe for bulk metadata or performance pulls.
- **Updated app detection**: Use `updated_date` to find recently refreshed apps (app updates can trigger chart movement).

---

## 20. Publisher Apps
**Endpoint**: `GET /v1/{os}/publisher/publisher_apps`
**Description**: Retrieves a collection of apps associated with a specific Publisher ID.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Operating System (`ios`, `android`) |
| `publisher_id` | string | Query | Yes | ID of the Publisher |
| `limit` | integer | Query | No | Max 100 apps per call |
| `offset` | integer | Query | No | Results offset |
| `include_count` | boolean | Query | No | If `true`, returns total count (changes JSON structure) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `publisher_id` | string | Publisher identifier |
| `publisher_name` | string | Publisher display name |
| `apps` | array | List of app objects |
| `apps[].app_id` | string | Platform-specific App ID |
| `apps[].name` | string | App name |
| `apps[].category_id` | string | Category ID |
| `total_count` | integer | Total apps for this publisher (if `include_count=true`) |

### Use Cases
- **Competitor portfolio mapping**: List all games a rival studio publishes → identify their genre diversification strategy.
- **Publisher-level risk**: A studio with 10 games in the same genre is over-exposed; spot market concentration risks.
- **Acquisition target research**: Identify studios with strong game portfolios but weak monetization for potential partnership.

---

## 21. Unified Publisher & Apps
**Endpoint**: `GET /v1/unified/publishers/apps`
**Description**: Retrieve a unified publisher and all its associated unified and platform-specific apps.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `unified_id` | string | Query | Yes | Unified App ID or Unified Publisher ID |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `unified_publisher_id` | string | Cross-platform publisher ID |
| `publisher_name` | string | Publisher name |
| `unified_apps` | array | List of unified app objects |
| `unified_apps[].unified_app_id` | string | Cross-platform app ID |
| `unified_apps[].ios_app_id` | string | iOS App Store ID |
| `unified_apps[].android_app_id` | string | Google Play package name |
| `unified_apps[].name` | string | App name |

### Use Cases
- **Cross-platform publisher view**: Single call gets both iOS and Android apps for a publisher → efficient for building `dim_company_games` mapping.
- **Unified ID resolution**: Given a unified publisher ID, get all their unified app IDs → expand competitor coverage to both platforms.

---

## 22. Unified Apps (Mapping Discovery)
**Endpoint**: `GET /v1/unified/apps`
**Description**: Retrieve the mapping between unified app IDs and their platform-specific (iOS/Android) counterparts, including canonical app IDs and cohort IDs.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `app_id_type` | string | Query | Yes | Operating System (`ios`, `android`, `unified`) |
| `app_ids` | array | Query | Yes | Comma-separated App IDs (Max 100) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `unified_app_id` | string | Cross-platform unified ID |
| `ios_app_id` | string | iOS numeric App ID |
| `android_app_id` | string | Android package name |
| `unified_publisher_id` | string | Cross-platform publisher ID |
| `name` | string | Canonical app name |
| `canonical_country` | string | App's primary market |

### Use Cases
- **ID translation**: Convert a known iOS ID to its Android counterpart (or vice versa) → essential for cross-platform `fact_market_insights` joins.
- **`dim_apps` linkage**: Populate the `unified_app_id` → `ios_app_id` / `android_app_id` mapping table that every other fact table references.

---

## 23. Unified Publishers (Mapping Discovery)
**Endpoint**: `GET /v1/unified/publishers`
**Description**: Retrieve the mapping between unified publisher IDs and their platform-specific (iTunes/Android) publisher IDs.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `publisher_id_type` | string | Query | Yes | Operating System (`ios`, `android`, `unified`) |
| `publisher_ids` | array | Query | Yes | Comma-separated Publisher IDs (Max 100) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `unified_publisher_id` | string | Cross-platform publisher ID |
| `ios_publisher_id` | string | iTunes Connect publisher ID |
| `android_publisher_id` | string | Google Play publisher ID |
| `publisher_name` | string | Publisher display name |
| `publisher_country` | string | Publisher's registered country |

### Use Cases
- **Cross-platform publisher analytics**: Aggregate a publisher's total downloads/revenue across both iOS and Android.
- **Country-of-origin analysis**: Identify SEA-based publishers vs. Chinese or Korean publishers entering the SEA market.

---

## 24. Entity Search
**Endpoint**: `GET /v1/{os}/search_entities`
**Description**: Search for apps and publishers containing certain words in their name, description, subtitle, promo text, and in-app purchases. Also searchable by App ID or Unified ID.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Path | Yes | Platform (`ios`, `android`, `unified`) |
| `entity_type` | string | Query | Yes | Type of entity (`app`, `publisher`) |
| `term` | string | Query | Yes | Search keyword (Min 3 Latin or 2 non-Latin chars) |
| `limit` | integer | Query | No | Limit results per call (Max 250, Default 100) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `unified_app_id` | string | Cross-platform ID |
| `name` | string | App display name |
| `publisher_name` | string | Publisher name |
| `category_id` | string | Category |
| `release_date` | date | Release date |
| `rating` | float | Average rating |
| `ratings_count` | integer | Total ratings count |
| `entity_type` | string | `app` or `publisher` |

### Use Cases
- **App ID discovery from name**: When you know a game's name but not its ST ID → translate to `app_id` for all downstream calls.
- **Keyword landscape scan**: Search a genre keyword (e.g., "idle RPG") → discover competitive set beyond the top charts.
- **New entrant detection**: Weekly search on genre keywords to find newly published games that haven't yet ranked.

---

## 25. ASO Top Keywords
**Endpoint**: `GET /v1/facets/metrics?aso_top_keywords`
**Description**: Fetches top keywords metrics for an app over the last 30 days, including rank, traffic, estimated downloads, and opportunity scores.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `os` | string | Query | Yes | Operating System (`ios`, `android`) |
| `app_ids` | array | Query | Yes | Comma-separated App IDs |
| `regions` | array | Query | Yes | Comma-separated Country Codes |
| `breakdown` | string | Query | Yes | `keyword` |
| `bundle` | string | Query | Yes | `aso_top_keywords` |
| `limit` | integer | Query | No | Max results (Default: 25) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `keyword` | string | The search keyword |
| `keyword_rank` | integer | App's current position for this keyword |
| `keyword_traffic` | float | Traffic Score (higher = more searched) |
| `est_keyword_downloads` | long | Estimated downloads driven by this keyword |
| `keyword_opportunity_score` | float | Blended score of traffic, difficulty, and relevance |
| `keyword_difficulty` | float | How hard it is to rank for this keyword |
| `region` | string | Country code |

### Use Cases
- **Organic acquisition audit**: Which keywords drive most downloads for top genre competitors? → benchmark your own ASO against them.
- **Keyword gap analysis**: Keywords a top competitor ranks for that you don't → gap to exploit in metadata optimization.
- **SEA localization**: Are Thai/Vietnamese keywords driving downloads in those markets? → insight for local metadata strategy.
- **Opportunity scoring**: Surface keywords with high traffic + low difficulty → prioritize for ASO quick wins.

---

## 26. Featured Apps & Creatives
**Endpoints**:
- Apps: `GET /v1/ios/featured/apps`
- Creatives: `GET /v1/{os}/featured/creatives`
- Impacts: `GET /v1/{os}/featured/impacts`

**Description**: Tracks app store featuring across the Today, Apps, and Games pages. Retrieves entries, creative assets, and performance impact data.
**Note**: Non-App Performance Insights users are limited to the last 3 days of data.

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | Featured app identifier |
| `feature_type` | string | Page type (`today`, `apps`, `games`) |
| `position` | integer | Position on the featured page |
| `start_date` | date | Feature start date |
| `end_date` | date | Feature end date |
| `country` | string | Country where featured |
| `creative_url` | string | URL to the featured creative asset |
| `impact_downloads` | long | Estimated incremental downloads from featuring |
| `impact_revenue` | long | Estimated incremental revenue from featuring |

### Use Cases
- **Feature lift quantification**: From `impacts` — quantify the download/revenue spike from being featured → input for PnL "featuring bonus" row.
- **Editorial trend analysis**: What genres/themes are Apple featuring? → informs a game's editorial pitch and metadata.
- **Competitor alert**: When a direct competitor gets featured, preemptively analyze their momentum spike.

---

## 27. Featured Today Stories
**Endpoint**: `GET /v1/ios/featured/today/stories`
**Description**: Retrieve featured today story metadata (title, label, style, position), featured apps, and relevant app metadata.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `country` | string | Query | No | Country Code (Default: `US`) |
| `start_date` | Date | Query | No | YYYY-MM-DD (Default: 3 days ago) |
| `end_date` | Date | Query | No | YYYY-MM-DD (Default: Today) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `story_id` | string | Unique story identifier |
| `title` | string | Story headline |
| `label` | string | Story label (e.g., `Game of the Day`) |
| `style` | string | Layout/presentation style |
| `position` | integer | Story position in the Today tab |
| `date` | date | Feature date |
| `country` | string | Country code |
| `featured_apps` | array | List of apps in the story |
| `featured_apps[].app_id` | string | App identifier |
| `featured_apps[].name` | string | App name |

### Use Cases
- **Competitor featuring tracker**: Track when a competitor game is featured → correlate with `fact_market_insights` download spikes.
- **Editorial calendar intelligence**: Identify patterns in Apple's featuring choices per genre/season → anticipate future featuring windows.
- **Story type analysis**: "Game of the Day" vs. "Tips & Tricks" stories have different download impacts → inform your pitching strategy.

---

## 28. Rating Metrics (Facets)
**Endpoint**: `GET /v1/facets/metrics?ratings`
**Description**: Get star rating metrics including total count, star-level breakdowns, and averages at incremental or cumulative levels.

### Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `bundle` | string | Query | Yes | `ratings_incremental` or `ratings_cumulative` |
| `breakdown` | string | Query | Yes | Combination of `app_id`, `date`, `region`, `app_version` |
| `app_ids` | array | Query | Yes | Comma-separated App IDs (Max 1,000) |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |
| `date_granularity` | string | Query | * | Required for `date` breakdown (`day`, `week`, `month`) |

### Key Response Fields
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `date` | date | Date |
| `region` | string | Country code |
| `app_version` | string | App version at time of rating |
| `average_rating` | float | Average star rating (1–5) |
| `ratings_count` | integer | Total ratings in this period |
| `star_1` | integer | Count of 1-star ratings |
| `star_2` | integer | Count of 2-star ratings |
| `star_3` | integer | Count of 3-star ratings |
| `star_4` | integer | Count of 4-star ratings |
| `star_5` | integer | Count of 5-star ratings |

### Use Cases
- **Quality benchmark**: Compare average ratings of top genre games vs. your own → quantify quality gap.
- **Update impact**: `app_version` breakdown → does a competitor's rating spike or drop after major updates? Predict churn risk.
- **PnL risk signal**: Games with declining average ratings show predictable revenue decay 2–3 months later → leading indicator for forecasting.
- **Genre baseline**: Average rating across all top-20 RPG games → set a quality bar target for new game development.

---

## 29. Review Analysis (Details & Metrics)

### A. Review Details
**Endpoint**: `GET /v1/{os}/review/get_reviews`
**Description**: Retrieve detailed user reviews including content, rating, version, tags, and sentiment.

### B. Aggregated Review Metrics
**Endpoint**: `GET /v1/facets/metrics?{bundle}`
**Bundles**: `reviews_by_rating`, `reviews_by_sentiment`, `reviews_by_tag`
**Description**: Time series or snapshot data for reviews broken down by stars, sentiment, or specific categories (tags).

### Shared Parameters
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `app_ids` | string | Query | Yes | App ID to retrieve data for |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |
| `rating_filters` | array | Query | No | Comma-separated star ratings (1-5) |
| `review_sentiments` | array | Query | No | Comma-separated (`happy`, `neutral`, `unhappy`, `mixed`) |

### Key Response Fields (Review Details)
| Field | Type | Description |
| :--- | :--- | :--- |
| `review_id` | string | Unique review identifier |
| `app_id` | string | App identifier |
| `author` | string | Reviewer username |
| `rating` | integer | Star rating (1–5) |
| `title` | string | Review title |
| `body` | string | Full review text |
| `version` | string | App version reviewed |
| `date` | date | Review date |
| `sentiment` | string | `happy`, `neutral`, `unhappy`, `mixed` |
| `tags` | array[string] | Sensor Tower auto-classified tags (e.g., `gameplay`, `monetization`, `bugs`) |
| `country` | string | Country code |

### Key Response Fields (Aggregated Metrics)
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `date` | date | Date |
| `sentiment` | string | Sentiment category |
| `review_count` | integer | Number of reviews matching filters |
| `tag` | string | Review tag (for `reviews_by_tag`) |

### Use Cases
- **Competitor pain point mining**: Filter 1- and 2-star reviews of top competitors → build a list of problems your game can solve.
- **Sentiment trend**: `reviews_by_sentiment` over time → early warning of user dissatisfaction before it shows in churn/revenue.
- **Feature gap discovery**: `reviews_by_tag` for `monetization` tag → are users complaining about aggressive IAP? Monetization opportunity or risk.
- **Version-specific feedback**: Post-update review analysis on competitor's latest version → understand what they shipped and how users reacted.

---

## 30. Apple Search Ads
**Endpoints**:
- Keyword Search: `GET /v1/ios/search_ads/apps`
- App Keywords: `GET /v1/ios/search_ads/terms`

**Description**:
1. Finds apps bidding on a specific keyword and their Share of Voice (SOV).
2. Lists all keywords an app is bidding on with SOV and competitor counts.

### Parameters (App Keywords)
| Name | Type | Location | Required | Description |
| :--- | :--- | :--- | :--- | :--- |
| `app_id` | string | Query | Yes | App ID to lookup |
| `country` | string | Query | Yes | Country Code |
| `start_date` | Date | Query | Yes | YYYY-MM-DD |
| `end_date` | Date | Query | Yes | YYYY-MM-DD |

### Key Response Fields (App Keywords — `/search_ads/terms`)
| Field | Type | Description |
| :--- | :--- | :--- |
| `app_id` | string | App identifier |
| `keyword` | string | Keyword being bid on |
| `sov` | float | Share of Voice for this keyword (0–1) |
| `competitor_count` | integer | Number of other apps bidding on this keyword |
| `country` | string | Country code |
| `date` | date | Date |

### Key Response Fields (Keyword Search — `/search_ads/apps`)
| Field | Type | Description |
| :--- | :--- | :--- |
| `keyword` | string | The searched keyword |
| `app_id` | string | App bidding on this keyword |
| `name` | string | App name |
| `sov` | float | This app's SOV for the keyword |
| `rank` | integer | Ad rank for this keyword |

### Use Cases
- **Keyword competition analysis**: How many apps bid on a genre keyword (e.g., "RPG game")? High `competitor_count` → expensive CPI bucket.
- **Competitor keyword strategy**: Discover all keywords a top competitor bids on → reveals their acquisition strategy and target audience.
- **SOV gap tracking**: Find keywords where you have 0 SOV but competitors are active → prioritize for Apple Search Ads bidding.
- **Genre keyword pricing**: Combine with ASO data (#25) to map paid vs. organic performance per keyword → optimize combined paid+organic strategy.

---

## Common HTTP Responses

| Code | Status | Description |
| :--- | :--- | :--- |
| **200** | Success | Request was successful and returned data. |
| **401** | Unauthorized | Invalid authentication token. |
| **403** | Forbidden | API token is not authorized for the requested product. |
| **422** | Unprocessable | Invalid Query Parameter. Check required params. |
