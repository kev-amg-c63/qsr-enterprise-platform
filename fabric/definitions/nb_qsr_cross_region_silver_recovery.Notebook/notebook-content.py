# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e3ec98e0-fa95-48b0-888e-8d62ab495adb",
# META       "default_lakehouse_name": "lh_qsr_silver",
# META       "default_lakehouse_workspace_id": "cfa2ca8d-4cc6-4458-871f-701a40996ce3",
# META       "known_lakehouses": [
# META         {
# META           "id": "e3ec98e0-fa95-48b0-888e-8d62ab495adb"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Gate 1 — Test cross-region source access (read-only)

source_workspace_id = "9dcbaac9-96ea-46aa-90c3-bc75f9de65ac"
source_lakehouse_id = "f4ca4f37-0c23-4d7a-8f02-13a582c7ca4c"

source_path = (
    f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{source_lakehouse_id}/Tables/dbo/DimDate"
)

try:
    df = spark.read.format("delta").load(source_path)
    print("SOURCE ACCESS: PASS")
    print(f"DimDate rows: {df.count():,}")
except Exception as exc:
    print("SOURCE ACCESS: FAILED")
    print(f"{type(exc).__name__}: {str(exc)[:1500]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gate 2 — Verify Central US destination (read-only)

target_workspace_id = "cfa2ca8d-4cc6-4458-871f-701a40996ce3"
target_lakehouse_id = "e3ec98e0-fa95-48b0-888e-8d62ab495adb"

target_path = (
    f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{target_lakehouse_id}/Tables/dbo/DimDate"
)

try:
    df_target = spark.read.format("delta").load(target_path)
    print("DESTINATION ACCESS: PASS")
    print(f"DimDate rows: {df_target.count():,}")
except Exception as exc:
    print("DESTINATION ACCESS: FAILED")
    print(f"{type(exc).__name__}: {str(exc)[:1500]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gate 3 — Recover FactMarketing (one table only)

table_name = "FactMarketing"

source = (
    f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{source_lakehouse_id}/Tables/dbo/{table_name}"
)

destination = (
    f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{target_lakehouse_id}/Tables/dbo/{table_name}"
)

# Read source
df = spark.read.format("delta").load(source)
source_count = df.count()

print(f"Source {table_name}: {source_count:,} rows")
df.printSchema()

# Safety check: do not overwrite an existing destination
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, destination):
    print("SKIPPED: Destination Delta table already exists.")
else:
    df.write.format("delta").mode("errorifexists").save(destination)

    destination_count = spark.read.format("delta").load(destination).count()

    print(f"Destination {table_name}: {destination_count:,} rows")
    assert source_count == destination_count, "Row count mismatch"
    print("GATE 3: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gate 4 — Recover remaining failed Silver tables
# Existing destination tables will NOT be overwritten.

from delta.tables import DeltaTable

tables = [
    "DimProduct",
    "DimPromotion",
    "FactFinancials",
    "FactInventory",
    "FactLabor",
    "FactPurchaseOrders",
    "FactSales",
    "FactSalesLine",
    "FactStorePerformance"
]

results = []

for table_name in tables:
    source = (
        f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{source_lakehouse_id}/Tables/dbo/{table_name}"
    )

    destination = (
        f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{target_lakehouse_id}/Tables/dbo/{table_name}"
    )

    try:
        df = spark.read.format("delta").load(source)
        source_count = df.count()

        if DeltaTable.isDeltaTable(spark, destination):
            status = "SKIPPED - EXISTS"
            destination_count = spark.read.format("delta").load(destination).count()
        else:
            df.write.format("delta").mode("errorifexists").save(destination)
            destination_count = spark.read.format("delta").load(destination).count()
            status = "PASS" if source_count == destination_count else "MISMATCH"

        results.append((table_name, source_count, destination_count, status))
        print(f"{table_name}: {status} ({destination_count:,} rows)")

    except Exception as exc:
        results.append((table_name, source_count if 'source_count' in locals() else None, None, "FAILED"))
        print(f"{table_name}: FAILED - {str(exc)[:500]}")

print("\nGATE 4 SUMMARY")
for result in results:
    print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gate 5 — Reconcile all 17 Silver tables

tables = [
    "DimAccount", "DimCustomer", "DimDate",
    "DimEmployee", "DimFranchise", "DimProduct",
    "DimPromotion", "DimStore", "DimSupplier",
    "FactFinancials", "FactInventory", "FactLabor",
    "FactMarketing", "FactPurchaseOrders",
    "FactSales", "FactSalesLine", "FactStorePerformance"
]

total_source = 0
total_target = 0
failures = []

for name in tables:
    source = (
        f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{source_lakehouse_id}/Tables/dbo/{name}"
    )
    target = (
        f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{target_lakehouse_id}/Tables/dbo/{name}"
    )

    try:
        src_count = spark.read.format("delta").load(source).count()
        dst_count = spark.read.format("delta").load(target).count()

        total_source += src_count
        total_target += dst_count

        status = "PASS" if src_count == dst_count else "FAIL"
        if status == "FAIL":
            failures.append(name)

        print(f"{name}: Source={src_count:,}, Target={dst_count:,}, {status}")

    except Exception as exc:
        failures.append(name)
        print(f"{name}: ERROR - {str(exc)[:300]}")

print("\nGATE 5 SUMMARY")
print(f"Tables checked: {len(tables)}")
print(f"Source rows: {total_source:,}")
print(f"Target rows: {total_target:,}")
print(f"Failures: {len(failures)}")

if not failures and total_source == total_target == 2764410:
    print("GATE 5: PASS")
else:
    print("GATE 5: REVIEW REQUIRED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gate 5A — Repair duplicate destination tables only

repair_tables = ["DimAccount", "DimCustomer"]

for name in repair_tables:
    source = (
        f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{source_lakehouse_id}/Tables/dbo/{name}"
    )

    target = (
        f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{target_lakehouse_id}/Tables/dbo/{name}"
    )

    df = spark.read.format("delta").load(source)
    expected = df.count()

    # Overwrite only the two explicitly identified duplicate tables.
    df.write.format("delta").mode("overwrite").save(target)

    actual = spark.read.format("delta").load(target).count()

    print(
        f"{name}: Source={expected:,}, "
        f"Target={actual:,}, "
        f"{'PASS' if expected == actual else 'FAIL'}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
