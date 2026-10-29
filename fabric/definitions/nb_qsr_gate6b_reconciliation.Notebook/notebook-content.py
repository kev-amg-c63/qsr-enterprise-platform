# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5005d5ba-270c-4a6c-92bc-2fa6bc8c01bb",
# META       "default_lakehouse_name": "lh_qsr_bronze",
# META       "default_lakehouse_workspace_id": "9dcbaac9-96ea-46aa-90c3-bc75f9de65ac",
# META       "known_lakehouses": [
# META         {
# META           "id": "5005d5ba-270c-4a6c-92bc-2fa6bc8c01bb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# ============================================================
# GATE 6B — SOURCE-TO-BRONZE RECONCILIATION
# READ-ONLY VALIDATION
# ============================================================

import json

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------

WORKSPACE = "qsr-dev-data-platform"
LAKEHOUSE = "lh_qsr_bronze"

ONELAKE_ROOT = (
    f"abfss://{WORKSPACE}@"
    "onelake.dfs.fabric.microsoft.com/"
    f"{LAKEHOUSE}.Lakehouse/Files/"
)

# Existing ADLS-backed OneLake shortcut.
# Configure the actual shortcut name before execution.
# Do not create or modify shortcuts in this task.
SOURCE_SHORTCUT = "adls_qsr_source"

SOURCE_ROOT = ONELAKE_ROOT + SOURCE_SHORTCUT + "/"
BRONZE_ROOT = ONELAKE_ROOT

DIMENSIONS = [
    "DimAccount",
    "DimCustomer",
    "DimDate",
    "DimEmployee",
    "DimFranchise",
    "DimProduct",
    "DimPromotion",
    "DimStore",
    "DimSupplier",
]

MAPPINGS = [
    (name, f"master-data/{name}.csv")
    for name in DIMENSIONS
] + [
    ("FactSales", "pos/FactSales.csv"),
    ("FactSalesLine", "pos/FactSalesLine.csv"),
    ("FactInventory", "inventory/FactInventory.csv"),
    ("FactLabor", "labor/FactLabor.csv"),
    ("FactPurchaseOrders", "purchasing/FactPurchaseOrders.csv"),
    ("FactFinancials", "finance/FactFinancials.csv"),
    ("FactStorePerformance", "finance/FactStorePerformance.csv"),
    ("FactMarketing", "marketing/FactMarketing.csv"),
]

# ------------------------------------------------------------
# FILE ACCESS
# ------------------------------------------------------------

def read_csv(path):
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .load(path)
    )


def file_exists(path):
    try:
        notebookutils.fs.head(path, 1)
        return True, None
    except Exception as exc:
        return False, str(exc)


# ------------------------------------------------------------
# SOURCE-TO-BRONZE RECONCILIATION
# ------------------------------------------------------------

results = []

if SOURCE_SHORTCUT.startswith("<"):
    raise RuntimeError(
        "BLOCKED: Existing ADLS-backed OneLake shortcut "
        "name has not been configured."
    )

for dataset, relative_path in MAPPINGS:

    source_path = SOURCE_ROOT + relative_path
    bronze_path = BRONZE_ROOT + relative_path

    result = {
        "dataset": dataset,
        "source_path": source_path,
        "bronze_path": bronze_path,
        "source_rows": None,
        "bronze_rows": None,
        "row_count_match": None,
        "schema_match": None,
        "status": "BLOCKED",
        "error": None,
    }

    source_exists, source_error = file_exists(source_path)
    bronze_exists, bronze_error = file_exists(bronze_path)

    if not source_exists or not bronze_exists:
        result["error"] = json.dumps({
            "source_error": source_error,
            "bronze_error": bronze_error,
        })
        results.append(result)
        continue

    try:
        source_df = read_csv(source_path)
        bronze_df = read_csv(bronze_path)

        source_count = source_df.count()
        bronze_count = bronze_df.count()

        schema_match = (
            source_df.columns == bronze_df.columns
        )

        result["source_rows"] = source_count
        result["bronze_rows"] = bronze_count
        result["row_count_match"] = (
            source_count == bronze_count
        )
        result["schema_match"] = schema_match

        result["status"] = (
            "PASS"
            if result["row_count_match"] and schema_match
            else "FAIL"
        )

    except Exception as exc:
        result["status"] = "BLOCKED"
        result["error"] = str(exc)

    results.append(result)

# ------------------------------------------------------------
# RESULTS
# ------------------------------------------------------------

results_df = spark.createDataFrame(
    [
        (
            r["dataset"],
            r["source_rows"],
            r["bronze_rows"],
            r["row_count_match"],
            r["schema_match"],
            r["status"],
            r["error"],
        )
        for r in results
    ],
    schema="""
        dataset STRING,
        source_rows LONG,
        bronze_rows LONG,
        row_count_match BOOLEAN,
        schema_match BOOLEAN,
        status STRING,
        error STRING
    """,
)

display(results_df)

passed = sum(r["status"] == "PASS" for r in results)
failed = sum(r["status"] == "FAIL" for r in results)
blocked = sum(r["status"] == "BLOCKED" for r in results)

summary = {
    "gate": "6B",
    "datasets_expected": len(MAPPINGS),
    "datasets_passed": passed,
    "datasets_failed": failed,
    "datasets_blocked": blocked,
    "source_rows": sum(
        r["source_rows"] or 0 for r in results
    ),
    "bronze_rows": sum(
        r["bronze_rows"] or 0 for r in results
    ),
    "schema_validation": "CSV COLUMN COMPARISON ONLY",
    "typed_schema_validation": "NOT IMPLEMENTED",
    "integrity_validation": "NOT IMPLEMENTED",
    "sha256": "SKIPPED",
    "status": (
        "BLOCKED" if blocked
        else "FAIL" if failed
        else "ROW_COUNT_AND_HEADER_CHECKS_PASSED"
    ),
}

print(json.dumps(summary, indent=2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
