# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Gate 7A | Microsoft Fabric Spark notebook: Silver contract snapshot
# READ ONLY. Does not modify Silver or Warehouse.
import json

WORKSPACE = 'qsr-dev-data-platform'
SILVER = 'lh_qsr_silver'
ROOT = f'abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SILVER}.Lakehouse/Tables/dbo'
EXPECTED_ROWS = {
    'DimAccount':100, 'DimCustomer':50000, 'DimDate':365,
    'DimEmployee':2000, 'DimFranchise':20, 'DimProduct':150,
    'DimPromotion':100, 'DimStore':100, 'DimSupplier':75,
    'FactFinancials':200000, 'FactInventory':250000, 'FactLabor':150000,
    'FactMarketing':25000, 'FactPurchaseOrders':50000, 'FactSales':500000,
    'FactSalesLine':1500000, 'FactStorePerformance':36500,
}

records = []
for name, expected in EXPECTED_ROWS.items():
    df = spark.read.format('delta').load(f'{ROOT}/{name}')
    actual = df.count()
    fields = [
        {'ordinal': i+1, 'name': f.name, 'spark_type': f.dataType.simpleString(),
         'nullable': f.nullable}
        for i, f in enumerate(df.schema.fields)
    ]
    record = {'table': name, 'expected_rows': expected, 'silver_rows': actual,
              'row_count_status': 'MATCH' if actual == expected else 'MISMATCH',
              'columns': fields}
    records.append(record)
    print(json.dumps(record))

summary = {'gate':'7A','mode':'READ_ONLY','tables_checked':len(records),
           'silver_rows':sum(r['silver_rows'] for r in records),
           'row_count_mismatches':sum(r['row_count_status']!='MATCH' for r in records),
           'status':'PASSED' if len(records)==17 and all(r['row_count_status']=='MATCH' for r in records) else 'FAILED'}
print('GATE 7A SILVER SNAPSHOT SUMMARY')
print(json.dumps(summary,indent=2))
if summary['status'] != 'PASSED':
    raise RuntimeError('Gate 7A Silver row-count snapshot FAILED')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Gate 7A — Silver Schema Comparison Export
# READ ONLY — no data modifications

import json

for record in records:
    print(json.dumps({
        "table": record["table"],
        "columns": [
            {
                "ordinal": c["ordinal"],
                "name": c["name"],
                "spark_type": c["spark_type"],
                "nullable": c["nullable"]
            }
            for c in record["columns"]
        ]
    }, separators=(",", ":")))

print("GATE 7A SILVER SCHEMA EXPORT COMPLETE")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
