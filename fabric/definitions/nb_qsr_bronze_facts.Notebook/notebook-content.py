# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# QSR Fabric Fact Processing — Bronze
# Exact fact schemas from Gate 2B-2D Warehouse DDL. No Warehouse writes.
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
import json
WORKSPACE="qsr-dev-data-platform"
BASE=f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com"
FACTS={'FactSales': [('SalesKey', 'BIGINT', True), ('TransactionID', 'VARCHAR(50)', True), ('DateKey', 'INT', True), ('StoreKey', 'BIGINT', True), ('CustomerKey', 'BIGINT', False), ('PromotionKey', 'BIGINT', False), ('TransactionTimestamp', 'DATETIME2(6)', False), ('GrossSales', 'DECIMAL(18,2)', False), ('DiscountAmount', 'DECIMAL(18,2)', False), ('NetSales', 'DECIMAL(18,2)', False), ('TaxAmount', 'DECIMAL(18,2)', False), ('TotalAmount', 'DECIMAL(18,2)', False), ('ItemCount', 'INT', False), ('PaymentMethod', 'VARCHAR(30)', False), ('SalesChannel', 'VARCHAR(30)', False), ('LoadTimestamp', 'DATETIME2(6)', False)], 'FactSalesLine': [('SalesLineKey', 'BIGINT', True), ('SalesKey', 'BIGINT', True), ('TransactionID', 'VARCHAR(50)', True), ('LineNumber', 'INT', True), ('DateKey', 'INT', True), ('StoreKey', 'BIGINT', True), ('ProductKey', 'BIGINT', True), ('PromotionKey', 'BIGINT', False), ('Quantity', 'DECIMAL(18,3)', True), ('UnitPrice', 'DECIMAL(18,2)', False), ('GrossLineAmount', 'DECIMAL(18,2)', False), ('DiscountAmount', 'DECIMAL(18,2)', False), ('NetLineAmount', 'DECIMAL(18,2)', False), ('TaxAmount', 'DECIMAL(18,2)', False), ('LoadTimestamp', 'DATETIME2(6)', False)], 'FactInventory': [('InventoryKey', 'BIGINT', True), ('DateKey', 'INT', True), ('StoreKey', 'BIGINT', True), ('ProductKey', 'BIGINT', True), ('SnapshotTimestamp', 'DATETIME2(6)', False), ('QuantityOnHand', 'DECIMAL(18,3)', False), ('QuantityReserved', 'DECIMAL(18,3)', False), ('UnitCost', 'DECIMAL(18,4)', False), ('InventoryValue', 'DECIMAL(18,2)', False), ('ReorderPoint', 'DECIMAL(18,3)', False), ('LoadTimestamp', 'DATETIME2(6)', False)], 'FactLabor': [('LaborKey', 'BIGINT', True), ('ShiftID', 'VARCHAR(50)', True), ('DateKey', 'INT', True), ('StoreKey', 'BIGINT', True), ('EmployeeKey', 'BIGINT', True), ('ShiftStart', 'DATETIME2(6)', False), ('ShiftEnd', 'DATETIME2(6)', False), ('RegularHours', 'DECIMAL(9,2)', False), ('OvertimeHours', 'DECIMAL(9,2)', False), ('HourlyRate', 'DECIMAL(18,4)', False), ('LaborCost', 'DECIMAL(18,2)', False), ('LoadTimestamp', 'DATETIME2(6)', False)], 'FactPurchaseOrders': [('PurchaseOrderLineKey', 'BIGINT', True), ('PurchaseOrderID', 'VARCHAR(50)', True), ('LineNumber', 'INT', True), ('OrderDateKey', 'INT', True), ('ExpectedDateKey', 'INT', False), ('ReceivedDateKey', 'INT', False), ('StoreKey', 'BIGINT', True), ('SupplierKey', 'BIGINT', True), ('ProductKey', 'BIGINT', True), ('OrderedQuantity', 'DECIMAL(18,3)', False), ('ReceivedQuantity', 'DECIMAL(18,3)', False), ('UnitCost', 'DECIMAL(18,4)', False), ('ExtendedCost', 'DECIMAL(18,2)', False), ('OrderStatus', 'VARCHAR(30)', False), ('LoadTimestamp', 'DATETIME2(6)', False)], 'FactFinancials': [('FinancialEntryKey', 'BIGINT', True), ('JournalEntryID', 'VARCHAR(50)', True), ('JournalLineNumber', 'INT', True), ('DateKey', 'INT', True), ('StoreKey', 'BIGINT', False), ('FranchiseKey', 'BIGINT', False), ('AccountKey', 'BIGINT', True), ('DebitAmount', 'DECIMAL(18,2)', False), ('CreditAmount', 'DECIMAL(18,2)', False), ('CurrencyCode', 'VARCHAR(3)', False), ('PostingTimestamp', 'DATETIME2(6)', False), ('LoadTimestamp', 'DATETIME2(6)', False)], 'FactMarketing': [('MarketingKey', 'BIGINT', True), ('CampaignID', 'VARCHAR(50)', True), ('DateKey', 'INT', True), ('PromotionKey', 'BIGINT', False), ('StoreKey', 'BIGINT', False), ('Channel', 'VARCHAR(50)', True), ('Impressions', 'BIGINT', False), ('Clicks', 'BIGINT', False), ('Conversions', 'BIGINT', False), ('SpendAmount', 'DECIMAL(18,2)', False), ('AttributedRevenue', 'DECIMAL(18,2)', False), ('LoadTimestamp', 'DATETIME2(6)', False)], 'FactStorePerformance': [('StorePerformanceKey', 'BIGINT', True), ('DateKey', 'INT', True), ('StoreKey', 'BIGINT', True), ('TransactionCount', 'BIGINT', False), ('UnitsSold', 'DECIMAL(18,3)', False), ('NetSales', 'DECIMAL(18,2)', False), ('LaborHours', 'DECIMAL(18,2)', False), ('LaborCost', 'DECIMAL(18,2)', False), ('FoodCost', 'DECIMAL(18,2)', False), ('WasteCost', 'DECIMAL(18,2)', False), ('LoadTimestamp', 'DATETIME2(6)', False)]}
ORDER=list(FACTS)

def dtype(t):
 t=t.upper()
 if t=='BIGINT':return T.LongType()
 if t=='INT':return T.IntegerType()
 if t=='BIT':return T.BooleanType()
 if t=='DATE':return T.DateType()
 if t.startswith('DATETIME2'):return T.TimestampType()
 if t.startswith('VARCHAR'):return T.StringType()
 if t.startswith('DECIMAL'):
  p,s=map(int,t[t.index('(')+1:t.index(')')].split(','));return T.DecimalType(p,s)
 raise ValueError(t)

def present(path):
 try:
  spark.read.format('binaryFile').load(path).limit(1).collect();return True
 except Exception:return False

# Expected files: lh_qsr_bronze/Files/fact-data/<FactName>.csv
# Source file copies must be ingested separately before this notebook runs.
SRC=f"{BASE}/lh_qsr_bronze.Lakehouse/Files/fact-data"
DEST=f"{BASE}/lh_qsr_bronze.Lakehouse/Tables"
missing=[n for n in ORDER if not present(f"{SRC}/{n}.csv")]
if missing:raise FileNotFoundError(f"STOP: fact CSV files not present: {missing}. No writes attempted.")
for name in ORDER:
 path=f"{SRC}/{name}.csv"
 raw=spark.read.option('header','true').option('mode','FAILFAST').csv(path)
 expected=[c[0] for c in FACTS[name]]
 if raw.columns!=expected:raise ValueError(f"{name} header differs from approved DDL: {raw.columns} != {expected}")
 # Preserve raw CSV values; do not overwrite an existing Bronze Delta path.
 raw.write.format('delta').mode('errorifexists').save(f"{DEST}/bronze_{name}")
 print(f"Bronze fact written: {name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
