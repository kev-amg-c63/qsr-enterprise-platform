# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Gate 6D — Publish dimension-only Gold Delta tables from Silver.
# Facts, measures, and warehouse loading are OUT OF SCOPE until fact CSVs
# have been ingested and their Silver transformations approved.
from pyspark.sql import functions as F
WORKSPACE = "qsr-dev-data-platform"
SILVER = "lh_qsr_silver"
GOLD = "lh_qsr_gold"
BASE = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com"
SILVER_TABLES = f"{BASE}/{SILVER}.Lakehouse/Tables"
GOLD_TABLES = f"{BASE}/{GOLD}.Lakehouse/Tables"
DIMENSIONS = ["DimAccount","DimCustomer","DimDate","DimEmployee",
              "DimFranchise","DimProduct","DimPromotion","DimStore","DimSupplier"]
for name in DIMENSIONS:
    df = spark.read.format("delta").load(f"{SILVER_TABLES}/{name}")
    # Preserve all approved typed dimension columns without inventing attributes.
    df.write.format("delta").mode("errorifexists").save(f"{GOLD_TABLES}/{name}")
    print(f"Created Gold dimension: {name}")
print("Dimension-only Gold publication finished; no fact or Warehouse writes attempted.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
