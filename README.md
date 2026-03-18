# DTC Brand Analytics

End-to-end analytics engineering project built on anonymized data from a direct-to-consumer e-commerce brand. Demonstrates a production-style dbt workflow: ingestion, layered modeling, testing, CI/CD, and data observability — ready for BI dashboards.

## Tech Stack

| Layer | Tool |
|-------|------|
| Warehouse | Google BigQuery |
| Transformation | dbt Core 1.11 |
| CI/CD | GitHub Actions |
| Data Observability | Elementary |
| Ingestion | Python |
| BI (Phase 6) | Looker Studio |

## Data Model

Three-layer dbt architecture following the [dbt best practices guide](https://docs.getdbt.com/best-practices):

| Layer | Count | Materialization | Purpose |
|-------|-------|-----------------|---------|
| Staging | 10 models | View | Clean, rename, cast from raw sources |
| Intermediate | 6 models | View | Business logic: spines, enrichments, unions |
| Marts | 13 models | Table | Analyst-facing facts and dimensions |
| Seed | 1 | Table | Product mapping reference data |

**Total: 29 dbt models, 81+ data tests, all passing.**

See the full data lineage diagram: [`docs/erd.md`](docs/erd.md)

## Key Metrics

- **Blended ROAS** — Shopify revenue / total ad spend (Facebook, Google, Bing, Pinterest)
- **Blended CAC** — Total ad spend / new customer orders
- **Channel mix** — Shopify DTC vs. retail revenue share over time
- **Customer lifetime value** — Order count, AOV, lifetime revenue via customer spine
- **Monthly channel overview** — Capstone mart joining revenue, ad performance, GA traffic, and retail

## Project Structure

```
dtc-brand-analytics/
├── .github/workflows/
│   └── dbt_ci.yml              # GitHub Actions: dbt build on every PR
├── dbt_project/
│   ├── models/
│   │   ├── staging/            # 10 models across 6 source groups
│   │   │   ├── shopify/        #   orders, order items
│   │   │   ├── facebook_ads/   #   daily account, daily campaigns, monthly adset
│   │   │   ├── google_ads/     #   search keywords, shopping products
│   │   │   ├── google_analytics/ # source/medium monthly
│   │   │   ├── ad_spend/       #   daily consolidated ad spend
│   │   │   └── retail/         #   weekly retail sales
│   │   ├── intermediate/       # 6 models — spines, enrichments, unions
│   │   └── marts/
│   │       ├── core/           # dim_dates, dim_customers, dim_products,
│   │       │                   # fct_orders, fct_order_items
│   │       ├── marketing/      # 7 fact tables — ad performance, channel overview
│   │       └── retail/         # fct_retail_weekly_sales
│   ├── seeds/
│   │   └── product_mapping.csv
│   ├── dbt_project.yml
│   └── packages.yml            # dbt-utils, elementary
├── ingestion/
│   ├── load_to_bigquery.py     # CSV → BigQuery loader
│   └── validate_bigquery.py    # Post-load validation checks
├── docs/
│   └── erd.md                  # Mermaid data lineage diagram
└── README.md
```

## Setup

### Prerequisites

- Python 3.10+
- [dbt-bigquery](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup) installed
- GCP project with BigQuery enabled
- Service account or OAuth credentials for BigQuery

### Quick Start

```bash
# Clone
git clone https://github.com/thomasc1126/dtc-brand-analytics.git
cd dtc-brand-analytics/dbt_project

# Configure your profiles.yml (not committed — see template below)
# Install packages
dbt deps

# Load seed data and build all models + run tests
dbt build
```

### profiles.yml Template

Create `dbt_project/profiles.yml` (gitignored):

```yaml
dtc_brand_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth            # or service-account
      project: your-gcp-project
      dataset: dtc_analytics_dev
      threads: 4
      timeout_seconds: 300
      location: US
```

## Data Sources

10 anonymized datasets from a DTC e-commerce brand, covering January 2019 through December 2020:

| Source | Description |
|--------|-------------|
| Shopify Orders | Core commerce orders with customer, financial, and fulfillment data |
| Shopify Order Items | Line-item detail with product and variant info |
| Facebook Ads (3 tables) | Daily account totals, daily campaign breakdowns, monthly adset performance |
| Google Ads Search | Monthly keyword-level search campaign metrics |
| Google Ads Shopping | Monthly product-level shopping campaign metrics |
| Google Analytics | Monthly source/medium traffic and conversion data |
| Daily Ad Spend | Consolidated daily spend across all 4 ad platforms |
| Retail Weekly Sales | Weekly retail point-of-sale data by UPC |

All brand-identifying information was removed during ingestion (Phase 1). Product names, brand references, and store identifiers are anonymized.

## CI/CD

GitHub Actions runs `dbt build` (models + tests) on every pull request to `main`.

- **Workflow:** [`.github/workflows/dbt_ci.yml`](.github/workflows/dbt_ci.yml)
- **Dataset:** CI runs use a separate `dtc_analytics_ci` dataset to avoid interfering with dev
- **Secret:** `DBT_BIGQUERY_KEYFILE` (base64-encoded GCP service account JSON)
- **Manual trigger:** Also supports `workflow_dispatch` for on-demand runs

## Data Observability

[Elementary](https://www.elementary-data.com/) is installed for data observability. It captures dbt artifacts, test results, and run metadata into dedicated tables for monitoring data pipeline health.

## Status

**Phase 5** — CI/CD, observability, documentation, and repo polish (current).

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Data ingestion and anonymization | Complete |
| 2 | BigQuery loading and dbt project init | Complete |
| 3 | BigQuery data validation | Complete |
| 4 | Intermediate and mart models | Complete |
| 5 | CI/CD, observability, docs, polish | In progress |
| 6 | Looker Studio dashboards | Planned |
