# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Gate 6B — Register the already-copied Bronze CSV files as raw Delta tables.
# The existing pl_qsr_bronze_ingestion pipeline already copied the files.
# This notebook DOES NOT repeat the ADLS-to-OneLake copy.
from pyspark.sql import functions as F
WORKSPACE = "qsr-dev-data-platform"
BRONZE = "lh_qsr_bronze"
BASE = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{BRONZE}.Lakehouse"
SOURCE = f"{BASE}/Files/master-data"
DEST = f"{BASE}/Tables"
FILES = ["DimAccount","DimCustomer","DimDate","DimEmployee","DimFranchise",
         "DimProduct","DimPromotion","DimStore","DimSupplier"]
# Do not overwrite the previously copied source files or any existing Delta paths.
for name in FILES:
    df = (spark.read.option("header", "true").option("mode", "FAILFAST")
          .csv(f"{SOURCE}/{name}.csv"))
    if len(df.columns) != len(set(df.columns)):
        raise ValueError(f"Duplicate CSV column names: {name}")
    # Bronze retains source text values; typed enforcement belongs to Silver.
    target = f"{DEST}/bronze_{name.lower()}"
    df.write.format("delta").mode("errorifexists").save(target)
    print(f"Created raw Bronze Delta path: {target}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
