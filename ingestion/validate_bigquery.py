"""
Phase 3 — BigQuery Data Validation

Runs validation checks (3.1–3.8) against all 11 raw BQ tables and the
product_mapping seed. Outputs results to console and writes a structured
validation log.

Usage:
    python validate_bigquery.py \
        --project analytics-eng-project \
        --raw-dataset dtc_analytics_raw \
        --dev-dataset dtc_analytics_dev

Prerequisites:
    pip install google-cloud-bigquery
    gcloud auth application-default login
"""

import argparse
import re
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

# ---------- constants ----------

RAW_DATASET = "dtc_analytics_raw"
DEV_DATASET = "dtc_analytics_dev"

EXPECTED_ROW_COUNTS = {
    "raw_shopify_orders": 610_176,
    "raw_shopify_orders_core_commerce": 583_550,
    "raw_shopify_order_items": 650_515,
    "raw_facebook_ads_daily_campaigns": 3_563,
    "raw_facebook_ads_daily_account": 365,
    "raw_facebook_ads_monthly_adset": 1_749,
    "raw_google_ads_search_keywords": 1_032,
    "raw_google_ads_shopping_products": 261,
    "raw_daily_ad_spend": 1_829,
    "raw_ga_source_medium_monthly": 2_301,
    "raw_retail_weekly_sales": 1_272,
}

DATE_RANGE_CHECKS = {
    "raw_shopify_orders": ("timestamp", "2019-01-01", "2020-12-31"),
    "raw_shopify_orders_core_commerce": ("timestamp", "2019-01-01", "2020-12-31"),
    "raw_shopify_order_items": ("timestamp", "2019-01-01", "2020-12-31"),
    "raw_facebook_ads_daily_campaigns": ("reporting_starts", "2020-01-01", "2020-12-31"),
    "raw_facebook_ads_daily_account": ("reporting_starts", "2019-01-01", "2019-12-31"),
    "raw_facebook_ads_monthly_adset": ("date", "2019-01-01", "2020-12-31"),
    "raw_google_ads_search_keywords": ("month", "2019-01-01", "2020-12-31"),
    "raw_google_ads_shopping_products": ("month", "2019-01-01", "2020-12-31"),
    "raw_daily_ad_spend": ("date", "2019-01-01", "2020-12-31"),
    "raw_ga_source_medium_monthly": ("month_of_year", "2019-01-01", "2020-12-31"),
    "raw_retail_weekly_sales": ("week_ending_date", "2019-01-05", "2021-01-02"),
}

NULL_CHECK_COLUMNS = {
    "raw_shopify_orders": ["order_id", "timestamp"],
    "raw_shopify_orders_core_commerce": ["order_id", "timestamp"],
    "raw_shopify_order_items": ["order_id", "timestamp"],
    "raw_facebook_ads_daily_campaigns": ["reporting_starts", "campaign_name"],
    "raw_facebook_ads_daily_account": ["reporting_starts"],
    "raw_facebook_ads_monthly_adset": ["date", "ad_set_id"],
    "raw_google_ads_search_keywords": ["month", "keyword_category"],
    "raw_google_ads_shopping_products": ["month", "item_id"],
    "raw_daily_ad_spend": ["date", "platform"],
    "raw_ga_source_medium_monthly": ["month_of_year", "source_medium"],
    "raw_retail_weekly_sales": ["week_ending_date", "upc"],
}

FORBIDDEN_TERMS = [
    "sugarbear", "sugar bear", "sbh", "besweet",
    "sugarbearhair", "133650600343472", "ulta",
]

EXCLUDED_ORDER_SOURCES = ["1622947", "shopify_draft_order", "1647733"]
EXCLUDED_ORDER_IDS = [
    "2752249135188", "2752349405268", "2753189380180", "2754392490068",
]

EXPECTED_PRODUCT_FAMILIES = {
    "hair_vitamins", "womens_multi", "sleep_vitamins", "bundle",
    "accessory", "promo", "case", "sampler", "display",
}

EXPECTED_PLATFORMS = {"bing", "facebook", "google_ads", "pinterest"}


# ---------- result tracking ----------

class ValidationResult:
    def __init__(self, check_id: str, name: str):
        self.check_id = check_id
        self.name = name
        self.status = "PASS"
        self.details: list[str] = []

    def fail(self, msg: str):
        self.status = "FAIL"
        self.details.append(f"FAIL: {msg}")

    def warn(self, msg: str):
        self.details.append(f"WARNING: {msg}")

    def info(self, msg: str):
        self.details.append(msg)


class ValidationRunner:
    def __init__(self, project: str, raw_dataset: str, dev_dataset: str):
        self.client = bigquery.Client(project=project)
        self.project = project
        self.raw_dataset = raw_dataset
        self.dev_dataset = dev_dataset
        self.results: list[ValidationResult] = []

    def raw_table(self, table: str) -> str:
        return f"`{self.project}.{self.raw_dataset}.{table}`"

    def dev_table(self, table: str) -> str:
        return f"`{self.project}.{self.dev_dataset}.{table}`"

    def query(self, sql: str):
        return list(self.client.query(sql).result())

    def query_scalar(self, sql: str):
        rows = self.query(sql)
        return rows[0][0] if rows else None

    # ---------- 3.1 Row Count Verification ----------

    def check_row_counts(self):
        result = ValidationResult("3.1", "Row Count Verification")
        for table, expected in EXPECTED_ROW_COUNTS.items():
            actual = self.query_scalar(f"SELECT COUNT(*) FROM {self.raw_table(table)}")
            if actual == expected:
                result.info(f"{table}: {actual:,} rows (expected {expected:,}) — OK")
            else:
                result.fail(f"{table}: {actual:,} rows (expected {expected:,})")
        self.results.append(result)

    # ---------- 3.2 Date Range Checks ----------

    def check_date_ranges(self):
        result = ValidationResult("3.2", "Date Range Checks")
        for table, (col, min_date, max_date) in DATE_RANGE_CHECKS.items():
            rows = self.query(
                f"SELECT MIN(CAST({col} AS DATE)) AS mn, MAX(CAST({col} AS DATE)) AS mx "
                f"FROM {self.raw_table(table)}"
            )
            mn, mx = rows[0].mn, rows[0].mx
            mn_str, mx_str = str(mn), str(mx)

            if mn_str < min_date:
                result.fail(f"{table}.{col} min={mn_str} < expected {min_date}")
            if mx_str > max_date:
                result.fail(f"{table}.{col} max={mx_str} > expected {max_date}")

            if result.status == "PASS" or mn_str >= min_date and mx_str <= max_date:
                result.info(f"{table}.{col}: {mn_str} to {mx_str} — OK")
            else:
                result.info(f"{table}.{col}: {mn_str} to {mx_str}")
        self.results.append(result)

    # ---------- 3.3 Null Checks on Key Columns ----------

    def check_nulls(self):
        result = ValidationResult("3.3", "Null Checks on Key Columns")
        for table, columns in NULL_CHECK_COLUMNS.items():
            for col in columns:
                null_count = self.query_scalar(
                    f"SELECT COUNTIF({col} IS NULL) FROM {self.raw_table(table)}"
                )
                if null_count > 0:
                    result.fail(f"{table}.{col}: {null_count:,} nulls")
                else:
                    result.info(f"{table}.{col}: 0 nulls — OK")
        self.results.append(result)

    # ---------- 3.4 Brand Leak Scan ----------

    def check_brand_leaks(self):
        result = ValidationResult("3.4", "Brand Leak Scan")
        total_matches = 0

        for table in EXPECTED_ROW_COUNTS:
            # Get string columns from INFORMATION_SCHEMA
            schema_rows = self.query(
                f"SELECT column_name FROM "
                f"`{self.project}.{self.raw_dataset}.INFORMATION_SCHEMA.COLUMNS` "
                f"WHERE table_name = '{table}' AND data_type = 'STRING'"
            )
            string_cols = [r.column_name for r in schema_rows]
            if not string_cols:
                result.info(f"{table}: no string columns — skipped")
                continue

            # Build LOWER(CONCAT(...)) and check all forbidden terms
            concat_expr = "LOWER(CONCAT(" + ", ' ', ".join(
                f"IFNULL({c}, '')" for c in string_cols
            ) + "))"

            conditions = " OR ".join(
                f"{concat_expr} LIKE '%{term}%'" for term in FORBIDDEN_TERMS
            )

            count = self.query_scalar(
                f"SELECT COUNTIF({conditions}) FROM {self.raw_table(table)}"
            )
            total_matches += count
            if count > 0:
                result.fail(f"{table}: {count:,} rows contain forbidden terms")
            else:
                result.info(f"{table}: 0 matches — OK")

        result.info(f"Total matches across all tables: {total_matches}")
        self.results.append(result)

    # ---------- 3.5 Product Mapping Validation ----------

    def check_product_mapping(self):
        result = ValidationResult("3.5", "Product Mapping Validation")

        # 3.5.1 Seed integrity
        seed_rows = self.query_scalar(
            f"SELECT COUNT(*) FROM {self.dev_table('product_mapping')}"
        )
        distinct_codes = self.query_scalar(
            f"SELECT COUNT(DISTINCT anonymized_product_code) "
            f"FROM {self.dev_table('product_mapping')}"
        )
        if seed_rows == 73:
            result.info(f"Seed row count: {seed_rows} — OK")
        else:
            result.fail(f"Seed row count: {seed_rows} (expected 73)")
        if distinct_codes == 42:
            result.info(f"Distinct anonymized_product_code: {distinct_codes} — OK")
        else:
            result.fail(f"Distinct anonymized_product_code: {distinct_codes} (expected 42)")

        # 3.5.2 Order items mapping coverage
        oi = self.raw_table("raw_shopify_order_items")
        mapping_rows = self.query(
            f"SELECT product_mapping_status, COUNT(*) AS cnt "
            f"FROM {oi} GROUP BY product_mapping_status"
        )
        status_map = {r.product_mapping_status: r.cnt for r in mapping_rows}
        mapped = status_map.get("mapped", 0)
        unmapped = status_map.get("unmapped", 0)
        review = status_map.get("review_required", 0)
        if mapped == 650_515 and unmapped == 0 and review == 0:
            result.info(f"Mapping coverage: {mapped:,} mapped, {unmapped} unmapped, {review} review — OK")
        else:
            result.fail(
                f"Mapping coverage: {mapped:,} mapped, {unmapped:,} unmapped, "
                f"{review:,} review (expected 650,515 / 0 / 0)"
            )

        # 3.5.3 Product family distribution
        # Some seed families (case, display) may have zero order volume — check
        # that all order item families are valid (exist in seed), and note any
        # seed-only families as expected zero-volume entries.
        oi_families = self.query(
            f"SELECT DISTINCT product_family FROM {oi}"
        )
        actual_families = {r.product_family for r in oi_families}
        seed_families = self.query(
            f"SELECT DISTINCT product_family FROM {self.dev_table('product_mapping')}"
        )
        seed_family_set = {r.product_family for r in seed_families}
        unexpected = actual_families - seed_family_set
        zero_volume = seed_family_set - actual_families
        if not unexpected:
            result.info(
                f"Product families: all {len(actual_families)} order item families "
                f"exist in seed — OK"
            )
        else:
            result.fail(f"Unexpected product families not in seed: {unexpected}")
        if zero_volume:
            result.info(f"Seed families with zero order volume (expected): {sorted(zero_volume)}")

        # 3.5.4 Anonymized field consistency (no orphans)
        orphan_count = self.query_scalar(
            f"SELECT COUNT(DISTINCT oi.normalized_product_name) "
            f"FROM {oi} oi "
            f"LEFT JOIN {self.dev_table('product_mapping')} pm "
            f"  ON oi.normalized_product_name = pm.anonymized_product_name "
            f"WHERE pm.anonymized_product_name IS NULL"
        )
        if orphan_count == 0:
            result.info("Orphan check: 0 orphaned normalized_product_name values — OK")
        else:
            result.fail(f"Orphan check: {orphan_count} normalized_product_name values not in seed")

        # 3.5.5 No raw product names — only anonymized patterns
        anon_patterns = [
            "sku", "product", "normalized_sku", "normalized_product_name",
        ]
        # Check for values that DON'T match expected anonymized patterns
        # Patterns: SKU-###, PRODUCT-###, Product-###, VARIANT-###, etc.
        # or NULL
        for col in anon_patterns:
            bad_count = self.query_scalar(
                f"SELECT COUNTIF("
                f"  {col} IS NOT NULL "
                f"  AND NOT REGEXP_CONTAINS({col}, r'^(SKU-\\d+|PRODUCT-\\d+|Product-\\d+|VARIANT-\\d+|PRODUCT_ID-\\d+|VARIANT_ID-\\d+|UNMAPPED-ITEM-\\d+)$')"
                f") FROM {oi}"
            )
            if bad_count == 0:
                result.info(f"Anonymized patterns ({col}): all match — OK")
            else:
                result.fail(f"Anonymized patterns ({col}): {bad_count:,} values don't match expected patterns")

        # 3.5.6 Bundle flag consistency with seed
        # The seed defines which products are bundles across multiple families
        # (bundle, accessory, sampler, case, promo). Verify order item is_bundle
        # values match what the seed defines for each normalized_product_name.
        bundle_mismatch = self.query_scalar(
            f"SELECT COUNT(*) FROM {oi} oi "
            f"JOIN {self.dev_table('product_mapping')} pm "
            f"  ON oi.normalized_product_name = pm.anonymized_product_name "
            f"WHERE oi.is_bundle != pm.is_bundle"
        )
        if bundle_mismatch == 0:
            result.info("Bundle flag consistency (vs seed): OK")
        else:
            result.fail(f"Bundle flag mismatch vs seed: {bundle_mismatch:,} rows")

        # Also verify: all bundle-family items have is_bundle=TRUE
        bundle_family_not_flagged = self.query_scalar(
            f"SELECT COUNTIF(product_family = 'bundle' AND (is_bundle IS NULL OR is_bundle = FALSE)) "
            f"FROM {oi}"
        )
        if bundle_family_not_flagged == 0:
            result.info("All bundle-family items have is_bundle=TRUE — OK")
        else:
            result.fail(f"bundle-family items without is_bundle=TRUE: {bundle_family_not_flagged:,}")

        # 3.5.7 Google Shopping item_id patterns
        gs = self.raw_table("raw_google_ads_shopping_products")
        bad_item_ids = self.query_scalar(
            f"SELECT COUNTIF("
            f"  NOT REGEXP_CONTAINS(item_id, r'^(PRODUCT_ID-\\d+|VARIANT_ID-\\d+|SKU-\\d+|UNMAPPED-ITEM-\\d+)$')"
            f") FROM {gs}"
        )
        if bad_item_ids == 0:
            result.info("Google Shopping item_id patterns: all match — OK")
        else:
            result.fail(f"Google Shopping item_id: {bad_item_ids:,} values don't match expected patterns")

        self.results.append(result)

    # ---------- 3.6 Order Exclusion Verification ----------

    def check_order_exclusions(self):
        result = ValidationResult("3.6", "Order Exclusion Verification")

        for table in ["raw_shopify_orders_core_commerce", "raw_shopify_order_items"]:
            tbl = self.raw_table(table)

            # Check excluded order sources
            sources_str = ", ".join(f"'{s}'" for s in EXCLUDED_ORDER_SOURCES)
            bad_sources = self.query_scalar(
                f"SELECT COUNTIF(order_source IN ({sources_str})) FROM {tbl}"
            )
            if bad_sources == 0:
                result.info(f"{table}: no excluded order_source values — OK")
            else:
                result.fail(f"{table}: {bad_sources:,} rows with excluded order_source")

            # Check excluded order IDs
            ids_str = ", ".join(f"'{i}'" for i in EXCLUDED_ORDER_IDS)
            bad_ids = self.query_scalar(
                f"SELECT COUNTIF(order_id IN ({ids_str})) FROM {tbl}"
            )
            if bad_ids == 0:
                result.info(f"{table}: no excluded order_id values — OK")
            else:
                result.fail(f"{table}: {bad_ids:,} rows with excluded order_id")

        self.results.append(result)

    # ---------- 3.7 PII Check ----------

    def check_pii(self):
        result = ValidationResult("3.7", "PII Check")

        # Check customer_email — should be SHA-256 (64-char hex) or empty/null
        for table in ["raw_shopify_orders", "raw_shopify_orders_core_commerce", "raw_shopify_order_items"]:
            at_count = self.query_scalar(
                f"SELECT COUNTIF(customer_email LIKE '%@%') "
                f"FROM {self.raw_table(table)}"
            )
            if at_count == 0:
                result.info(f"{table}.customer_email: no @ symbols — OK")
            else:
                result.fail(f"{table}.customer_email: {at_count:,} rows contain @")

        # Check no state = 'SBH'
        for table in ["raw_shopify_orders", "raw_shopify_orders_core_commerce"]:
            sbh_count = self.query_scalar(
                f"SELECT COUNTIF(UPPER(state) = 'SBH') "
                f"FROM {self.raw_table(table)}"
            )
            if sbh_count == 0:
                result.info(f"{table}.state: no 'SBH' values — OK")
            else:
                result.fail(f"{table}.state: {sbh_count:,} rows with state='SBH'")

        self.results.append(result)

    # ---------- 3.8 Cross-Table Consistency ----------

    def check_cross_table(self):
        result = ValidationResult("3.8", "Cross-Table Consistency")

        # Core commerce ≤ orders
        orders_count = self.query_scalar(
            f"SELECT COUNT(*) FROM {self.raw_table('raw_shopify_orders')}"
        )
        cc_count = self.query_scalar(
            f"SELECT COUNT(*) FROM {self.raw_table('raw_shopify_orders_core_commerce')}"
        )
        if cc_count <= orders_count:
            result.info(
                f"Core commerce ({cc_count:,}) ≤ orders ({orders_count:,}) — OK"
            )
        else:
            result.fail(
                f"Core commerce ({cc_count:,}) > orders ({orders_count:,})"
            )

        # Referential integrity: order_items.order_id ∈ orders.order_id
        orphan_orders = self.query_scalar(
            f"SELECT COUNT(DISTINCT oi.order_id) "
            f"FROM {self.raw_table('raw_shopify_order_items')} oi "
            f"LEFT JOIN {self.raw_table('raw_shopify_orders')} o "
            f"  ON oi.order_id = o.order_id "
            f"WHERE o.order_id IS NULL"
        )
        if orphan_orders == 0:
            result.info("Order items referential integrity: all order_ids found in orders — OK")
        else:
            result.fail(f"Referential integrity: {orphan_orders:,} order_ids in items but not in orders")

        # Daily ad spend platforms
        platforms = self.query(
            f"SELECT DISTINCT platform FROM {self.raw_table('raw_daily_ad_spend')}"
        )
        actual_platforms = {r.platform for r in platforms}
        if actual_platforms == EXPECTED_PLATFORMS:
            result.info(f"Ad spend platforms: {sorted(actual_platforms)} — OK")
        else:
            missing = EXPECTED_PLATFORMS - actual_platforms
            extra = actual_platforms - EXPECTED_PLATFORMS
            if missing:
                result.fail(f"Missing platforms: {missing}")
            if extra:
                result.warn(f"Extra platforms: {extra}")

        self.results.append(result)

    # ---------- run all ----------

    def run_all(self):
        checks = [
            ("3.1", self.check_row_counts),
            ("3.2", self.check_date_ranges),
            ("3.3", self.check_nulls),
            ("3.4", self.check_brand_leaks),
            ("3.5", self.check_product_mapping),
            ("3.6", self.check_order_exclusions),
            ("3.7", self.check_pii),
            ("3.8", self.check_cross_table),
        ]
        for check_id, fn in checks:
            print(f"\n{'='*60}")
            print(f"Running check {check_id}...")
            print(f"{'='*60}")
            try:
                fn()
                print(f"  -> {self.results[-1].status}")
                for d in self.results[-1].details:
                    print(f"  {d}")
            except Exception as e:
                r = ValidationResult(check_id, f"Check {check_id}")
                r.fail(f"Exception: {e}")
                self.results.append(r)
                print(f"  -> FAIL (exception: {e})")

    # ---------- output ----------

    def write_log(self, output_path: str):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [
            "# Phase 3 — BigQuery Data Validation Log",
            "",
            f"Generated: {now}",
            "",
            "## Summary",
            "",
            "| Check | Name | Status |",
            "|---|---|---|",
        ]

        all_pass = True
        for r in self.results:
            lines.append(f"| {r.check_id} | {r.name} | {r.status} |")
            if r.status != "PASS":
                all_pass = False

        lines.append("")
        overall = "ALL CHECKS PASSED" if all_pass else "SOME CHECKS FAILED"
        lines.append(f"**Overall: {overall}**")
        lines.append("")

        # Detailed results
        lines.append("## Detailed Results")
        lines.append("")
        for r in self.results:
            lines.append(f"### {r.check_id} — {r.name}")
            lines.append("")
            lines.append(f"**Status: {r.status}**")
            lines.append("")
            for d in r.details:
                lines.append(f"- {d}")
            lines.append("")

        content = "\n".join(lines)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(content)

        print(f"\nValidation log written to: {output_path}")
        return all_pass


# ---------- main ----------

def main():
    parser = argparse.ArgumentParser(description="Phase 3 — BigQuery Data Validation")
    parser.add_argument("--project", default="analytics-eng-project", help="GCP project ID")
    parser.add_argument("--raw-dataset", default=RAW_DATASET, help="Raw dataset name")
    parser.add_argument("--dev-dataset", default=DEV_DATASET, help="Dev dataset name (for seeds)")
    parser.add_argument(
        "--output", default=None,
        help="Validation log output path (default: phase3/validation_log.md)",
    )
    args = parser.parse_args()

    output_path = args.output
    if output_path is None:
        # Default: write to phase3/validation_log.md relative to repo root
        script_dir = Path(__file__).resolve().parent
        output_path = str(script_dir.parent.parent / "phase3" / "validation_log.md")

    runner = ValidationRunner(args.project, args.raw_dataset, args.dev_dataset)
    runner.run_all()

    print(f"\n{'='*60}")
    print("VALIDATION COMPLETE")
    print(f"{'='*60}")

    all_pass = runner.write_log(output_path)
    if not all_pass:
        print("\n⚠ Some checks FAILED — review the log for details.")
        raise SystemExit(1)
    else:
        print("\nAll checks PASSED.")


if __name__ == "__main__":
    main()
