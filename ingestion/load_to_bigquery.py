"""
Phase 2.1 — Load all 11 Phase 1 CSVs into BigQuery as raw tables.

Usage:
    python load_to_bigquery.py --project analytics-eng-project --dataset dtc_analytics_raw

Prerequisites:
    pip install google-cloud-bigquery
    gcloud auth application-default login
"""

import argparse
import os
from pathlib import Path

from google.cloud import bigquery

# ---------- configuration ----------

# Resolve paths relative to this script's location
SCRIPT_DIR = Path(__file__).resolve().parent
PHASE1_DIR = SCRIPT_DIR.parent.parent / "phase1"

TABLES = [
    # ---------- Shopify ----------
    {
        "table": "raw_shopify_orders",
        "csv": PHASE1_DIR / "1.01-shopify-orders/output/raw_shopify_orders.csv",
        "expected_rows": 610176,
        "partition_field": "timestamp",
        "schema": [
            ("order_id", "STRING"),
            ("timestamp", "TIMESTAMP"),
            ("customer_email", "STRING"),
            ("revenue", "FLOAT64"),
            ("quantity", "INT64"),
            ("cost", "FLOAT64"),
            ("shipping", "FLOAT64"),
            ("tax", "FLOAT64"),
            ("discount", "FLOAT64"),
            ("status", "STRING"),
            ("day_of_week", "STRING"),
            ("device", "STRING"),
            ("channel", "STRING"),
            ("source", "STRING"),
            ("metro", "STRING"),
            ("campaign", "STRING"),
            ("amount_refunded", "FLOAT64"),
            ("current_device", "STRING"),
            ("current_channel", "STRING"),
            ("current_source", "STRING"),
            ("current_metro", "STRING"),
            ("current_campaign", "STRING"),
            ("transaction_id", "STRING"),
            ("gift_card_revenue", "FLOAT64"),
            ("is_gift_card", "BOOLEAN"),
            ("order_source", "STRING"),
            ("state", "STRING"),
            ("country_code", "STRING"),
            ("line_item_count", "INT64"),
            ("highest_priced_product", "FLOAT64"),
            ("lowest_priced_product", "FLOAT64"),
            ("currency", "STRING"),
            ("fulfillment_status", "STRING"),
            ("user_id", "STRING"),
            ("user", "STRING"),
            ("location_id", "STRING"),
            ("pos_id", "STRING"),
            ("processiong_method", "STRING"),  # typo in original data
            ("source_identifier", "STRING"),
            ("gateway", "STRING"),
            ("location", "STRING"),
            ("pos", "STRING"),
            ("app_id", "STRING"),
            ("cancelled_at", "TIMESTAMP"),
            ("closed_at", "TIMESTAMP"),
            ("cancel_reason", "STRING"),
            ("sales_channel_id", "STRING"),
            ("sales_channel", "STRING"),
            ("customer_id", "STRING"),
        ],
    },
    {
        "table": "raw_shopify_orders_core_commerce",
        "csv": PHASE1_DIR / "1.01-shopify-orders/output/raw_shopify_orders_core_commerce.csv",
        "expected_rows": 583550,
        "partition_field": "timestamp",
        "schema": "SAME_AS:raw_shopify_orders",
    },
    {
        "table": "raw_shopify_order_items",
        "csv": PHASE1_DIR / "1.02-shopify-order-items/output/raw_shopify_order_items.csv",
        "expected_rows": 650515,
        "partition_field": "timestamp",
        "schema": [
            ("timestamp", "TIMESTAMP"),
            ("order_id", "STRING"),
            ("transaction_id", "STRING"),
            ("order_source", "STRING"),
            ("status", "STRING"),
            ("fulfillment_status", "STRING"),
            ("customer_email", "STRING"),
            ("base_product_id", "STRING"),
            ("variant_id", "STRING"),
            ("sku", "STRING"),
            ("product", "STRING"),
            ("quantity", "INT64"),
            ("revenue", "FLOAT64"),
            ("pre_tax_price", "FLOAT64"),
            ("tax", "FLOAT64"),
            ("shipping", "FLOAT64"),
            ("discount", "FLOAT64"),
            ("amount_refunded", "FLOAT64"),
            ("quantity_refunded", "INT64"),
            ("normalized_base_product_id", "STRING"),
            ("normalized_variant_id", "STRING"),
            ("normalized_sku", "STRING"),
            ("normalized_product_name", "STRING"),
            ("product_family", "STRING"),
            ("product_form", "STRING"),
            ("pack_size_months", "INT64"),
            ("is_bundle", "BOOLEAN"),
            ("product_mapping_status", "STRING"),
            ("mapping_key_used", "STRING"),
            ("mapping_notes", "STRING"),
        ],
    },
    # ---------- Facebook ----------
    {
        "table": "raw_facebook_ads_daily_campaigns",
        "csv": PHASE1_DIR / "1.03-facebook-daily-campaigns/output/facebook_ads_daily_campaigns_2020.csv",
        "expected_rows": 3563,
        "schema": [
            ("reporting_starts", "DATE"),
            ("reporting_ends", "DATE"),
            ("campaign_name", "STRING"),
            ("ad_set_budget", "FLOAT64"),
            ("ad_set_budget_type", "STRING"),
            ("amount_spent_usd", "FLOAT64"),
            ("campaign_delivery", "STRING"),
            ("reach", "INT64"),
            ("impressions", "INT64"),
            ("frequency", "FLOAT64"),
            ("website_purchases", "INT64"),
            ("website_purchases_conversion_value", "FLOAT64"),
            ("website_purchase_roas", "FLOAT64"),
            ("link_clicks", "INT64"),
            ("cost_per_purchase_usd", "FLOAT64"),
            ("cpm_usd", "FLOAT64"),
        ],
    },
    {
        "table": "raw_facebook_ads_daily_account",
        "csv": PHASE1_DIR / "1.04-facebook-daily-account/output/facebook_ads_daily_account_2019.csv",
        "expected_rows": 365,
        "schema": [
            ("reporting_starts", "DATE"),
            ("reporting_ends", "DATE"),
            ("account_name", "STRING"),
            ("reach", "INT64"),
            ("impressions", "INT64"),
            ("frequency", "FLOAT64"),
            ("amount_spent_usd", "FLOAT64"),
            ("link_clicks", "INT64"),
            ("website_purchases", "INT64"),
            ("website_purchases_conversion_value", "FLOAT64"),
            ("cost_per_purchase_usd", "FLOAT64"),
            ("website_purchase_roas", "FLOAT64"),
        ],
    },
    {
        "table": "raw_facebook_ads_monthly_adset",
        "csv": PHASE1_DIR / "1.05-facebook-monthly-adset/output/facebook_ads_monthly_adset_2019_2020.csv",
        "expected_rows": 1749,
        "schema": [
            ("month_start_end", "STRING"),
            ("month", "STRING"),
            ("date", "DATE"),
            ("ad_set_id", "STRING"),
            ("campaign_id", "STRING"),
            ("impressions", "INT64"),
            ("amount_spent_usd", "FLOAT64"),
            ("starts", "STRING"),
            ("ends", "STRING"),
            ("cpm", "FLOAT64"),
            ("website_purchases", "INT64"),
            ("website_purchases_conversion_value", "FLOAT64"),
            ("reporting_starts", "DATE"),
            ("reporting_ends", "DATE"),
            ("audience_type", "STRING"),
        ],
    },
    # ---------- Google Ads ----------
    {
        "table": "raw_google_ads_search_keywords",
        "csv": PHASE1_DIR / "1.06-google-ads-search/output/google_ads_search_keywords_2019_2020.csv",
        "expected_rows": 1032,
        "schema": [
            ("month", "DATE"),
            ("account", "STRING"),
            ("campaign_type", "STRING"),
            ("keyword_category", "STRING"),
            ("currency", "STRING"),
            ("cost", "FLOAT64"),
            ("impressions", "INT64"),
            ("ctr", "FLOAT64"),
            ("avg_cpc", "FLOAT64"),
            ("conversions", "FLOAT64"),
            ("conv_rate", "FLOAT64"),
            ("conv_value", "FLOAT64"),
            ("conv_value_per_cost", "FLOAT64"),
            ("cost_per_conv", "FLOAT64"),
        ],
    },
    {
        "table": "raw_google_ads_shopping_products",
        "csv": PHASE1_DIR / "1.07-google-ads-shopping/output/google_ads_shopping_products_2019_2020.csv",
        "expected_rows": 261,
        "schema": [
            ("month", "DATE"),
            ("account", "STRING"),
            ("campaign_type", "STRING"),
            ("item_id", "STRING"),
            ("product_type_level1", "STRING"),
            ("currency", "STRING"),
            ("cost", "FLOAT64"),
            ("impressions", "INT64"),
            ("ctr", "FLOAT64"),
            ("avg_cpc", "FLOAT64"),
            ("conversions", "FLOAT64"),
            ("conv_rate", "FLOAT64"),
            ("conv_value", "FLOAT64"),
            ("conv_value_per_cost", "FLOAT64"),
            ("cost_per_conv", "FLOAT64"),
        ],
    },
    # ---------- Cross-platform ----------
    {
        "table": "raw_daily_ad_spend",
        "csv": PHASE1_DIR / "1.08-daily-ad-spend/output/daily_ad_spend_2019_2020.csv",
        "expected_rows": 1829,
        "schema": [
            ("platform", "STRING"),
            ("date", "DATE"),
            ("spend", "FLOAT64"),
        ],
    },
    # ---------- Google Analytics ----------
    {
        "table": "raw_ga_source_medium_monthly",
        "csv": PHASE1_DIR / "1.09-ga-source-medium/output/ga_source_medium_monthly_2019_2020.csv",
        "expected_rows": 2301,
        "schema": [
            ("source_medium", "STRING"),
            ("month_of_year", "DATE"),
            ("users", "INT64"),
            ("sessions", "INT64"),
            ("revenue", "FLOAT64"),
            ("transactions", "INT64"),
            ("avg_order_value", "FLOAT64"),
            ("ecommerce_conversion_rate", "FLOAT64"),
            ("per_session_value", "FLOAT64"),
        ],
    },
    # ---------- Retail ----------
    {
        "table": "raw_retail_weekly_sales",
        "csv": PHASE1_DIR / "1.10-retail-weekly/output/retail_weekly_sales_2019_2020.csv",
        "expected_rows": 1272,
        "schema": [
            ("week_ending_date", "DATE"),
            ("fiscal_week", "STRING"),
            ("file_date", "DATE"),
            ("upc", "STRING"),
            ("retailer_item_number", "STRING"),
            ("item_description", "STRING"),
            ("retail_price", "FLOAT64"),
            ("total_sales_ty_units", "FLOAT64"),
            ("total_sales_ly_units", "FLOAT64"),
            ("total_sales_units_chg_pct", "FLOAT64"),
            ("total_sales_ty_dollars", "FLOAT64"),
            ("total_sales_ly_dollars", "FLOAT64"),
            ("total_sales_dollars_chg_pct", "FLOAT64"),
            ("comp_sales_ty_units", "FLOAT64"),
            ("comp_sales_ly_units", "FLOAT64"),
            ("comp_sales_units_chg_pct", "FLOAT64"),
            ("comp_sales_ty_dollars", "FLOAT64"),
            ("comp_sales_ly_dollars", "FLOAT64"),
            ("comp_sales_dollars_chg_pct", "FLOAT64"),
            ("noncomp_sales_ty_units", "FLOAT64"),
            ("noncomp_sales_ly_units", "FLOAT64"),
            ("noncomp_sales_units_chg_pct", "FLOAT64"),
            ("noncomp_sales_ty_dollars", "FLOAT64"),
            ("noncomp_sales_ly_dollars", "FLOAT64"),
            ("noncomp_sales_dollars_chg_pct", "FLOAT64"),
            ("dotcom_sales_ty_units", "FLOAT64"),
            ("dotcom_sales_ly_units", "FLOAT64"),
            ("dotcom_sales_units_chg_pct", "FLOAT64"),
            ("dotcom_sales_ty_dollars", "FLOAT64"),
            ("dotcom_sales_ly_dollars", "FLOAT64"),
            ("dotcom_sales_dollars_chg_pct", "FLOAT64"),
            ("total_eoh_ty_units", "FLOAT64"),
            ("total_eoh_ly_units", "FLOAT64"),
            ("dc_eoh_ty_units", "FLOAT64"),
            ("dc_eoh_ly_units", "FLOAT64"),
            ("store_eoh_ty_units", "FLOAT64"),
            ("store_eoh_ly_units", "FLOAT64"),
            ("store_in_stock_pct", "FLOAT64"),
        ],
    },
]

# ---------- helpers ----------

def resolve_schemas(tables: list[dict]) -> None:
    """Resolve SAME_AS references so every entry has its own schema list."""
    lookup = {t["table"]: t["schema"] for t in tables if isinstance(t.get("schema"), list)}
    for t in tables:
        if isinstance(t.get("schema"), str) and t["schema"].startswith("SAME_AS:"):
            ref = t["schema"].split(":", 1)[1]
            t["schema"] = lookup[ref]


def build_bq_schema(schema_tuples: list[tuple]) -> list[bigquery.SchemaField]:
    return [bigquery.SchemaField(name, dtype, mode="NULLABLE") for name, dtype in schema_tuples]


def load_table(
    client: bigquery.Client,
    dataset_ref: str,
    table_cfg: dict,
    dry_run: bool = False,
) -> int:
    table_id = f"{dataset_ref}.{table_cfg['table']}"
    csv_path = str(table_cfg["csv"])

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    schema = build_bq_schema(table_cfg["schema"])

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        max_bad_records=0,
    )

    # Add time-based partitioning if specified
    if "partition_field" in table_cfg:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field=table_cfg["partition_field"],
        )

    if dry_run:
        print(f"  [DRY RUN] Would load {csv_path} -> {table_id}")
        return 0

    print(f"  Loading {csv_path} -> {table_id} ...")

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)

    job.result()  # wait for completion

    dest_table = client.get_table(table_id)
    actual_rows = dest_table.num_rows
    expected = table_cfg["expected_rows"]

    status = "PASS" if actual_rows == expected else "MISMATCH"
    print(f"  {table_cfg['table']}: {actual_rows:,} rows loaded (expected {expected:,}) [{status}]")

    return actual_rows


# ---------- main ----------

def main():
    parser = argparse.ArgumentParser(description="Load Phase 1 CSVs to BigQuery")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="dtc_analytics_raw", help="BigQuery dataset name")
    parser.add_argument("--location", default="US", help="Dataset location")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen without loading")
    parser.add_argument("--table", help="Load a single table by name (optional)")
    args = parser.parse_args()

    resolve_schemas(TABLES)

    client = bigquery.Client(project=args.project)
    dataset_ref = f"{args.project}.{args.dataset}"

    # Create dataset if it doesn't exist
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = args.location
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset ready: {dataset_ref}")

    # Filter to single table if specified
    to_load = TABLES
    if args.table:
        to_load = [t for t in TABLES if t["table"] == args.table]
        if not to_load:
            print(f"ERROR: table '{args.table}' not found. Available: {[t['table'] for t in TABLES]}")
            return

    # Load tables
    results = {}
    errors = []
    for table_cfg in to_load:
        try:
            rows = load_table(client, dataset_ref, table_cfg, dry_run=args.dry_run)
            results[table_cfg["table"]] = rows
        except Exception as e:
            print(f"  ERROR loading {table_cfg['table']}: {e}")
            errors.append(table_cfg["table"])

    # Summary
    print("\n--- Summary ---")
    for name, rows in results.items():
        print(f"  {name}: {rows:,} rows")
    if errors:
        print(f"\n  ERRORS: {errors}")
    else:
        print("\n  All tables loaded successfully.")


if __name__ == "__main__":
    main()
