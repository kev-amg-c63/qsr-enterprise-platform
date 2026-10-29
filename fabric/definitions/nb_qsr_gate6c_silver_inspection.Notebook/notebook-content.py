# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************


# Gate 6C — Read-only Silver dimension inspection
# No writes, moves, deletes, or table registration.

from delta.tables import DeltaTable
import json

BASE = (
    "abfss://qsr-dev-data-platform@"
    "onelake.dfs.fabric.microsoft.com/"
    "lh_qsr_silver.Lakehouse"
)

TABLES = BASE + "/Tables"
QUARANTINE = BASE + "/Files/quarantine/master-data"

NAMES = [
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

def inspect_delta(name, path):
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        history = DeltaTable.forPath(spark, path).history(1).collect()

        return {
            "name": name,
            "path": path,
            "status": "READABLE",
            "rows": count,
            "columns": len(df.columns),
            "version": history[0]["version"] if history else None,
            "operation": history[0]["operation"] if history else None,
        }
    except Exception as exc:
        return {
            "name": name,
            "path": path,
            "status": "NOT_READABLE_OR_ABSENT",
            "error": str(exc)[:500],
        }

def list_directory(path):
    try:
        return {
            "path": path,
            "status": "ACCESSIBLE",
            "items": [x.name for x in notebookutils.fs.ls(path)],
        }
    except Exception as exc:
        return {
            "path": path,
            "status": "NOT_ACCESSIBLE_OR_ABSENT",
            "error": str(exc)[:500],
        }

results = {
    "gate": "6C",
    "mode": "READ_ONLY",
    "misplaced_paths": [],
    "dbo_paths": [],
    "quarantine_paths": [],
}

for name in NAMES:
    results["misplaced_paths"].append(
        inspect_delta(name, f"{TABLES}/{name}")
    )

    results["dbo_paths"].append(
        inspect_delta(name, f"{TABLES}/dbo/{name}")
    )

    results["quarantine_paths"].append(
        inspect_delta(name, f"{QUARANTINE}/{name}")
    )

results["dbo_directory"] = list_directory(f"{TABLES}/dbo")
results["quarantine_directory"] = list_directory(QUARANTINE)

print(json.dumps(results, indent=2, default=str))

print("\nGATE 6C INSPECTION COMPLETE — READ ONLY")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
