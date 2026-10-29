# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Gate 6E — Silver Data Quality and Referential Integrity
# READ ONLY. No writes, table creation, overwrite, or deletes.
# Microsoft Fabric PySpark notebook: nb_qsr_gate6e_silver_data_quality
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
import json

WORKSPACE = 'qsr-dev-data-platform'
SILVER = 'lh_qsr_silver'
ROOT = f'abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SILVER}.Lakehouse/Tables/dbo'

DIMENSIONS = ['DimAccount','DimCustomer','DimDate','DimEmployee','DimFranchise',
              'DimProduct','DimPromotion','DimStore','DimSupplier']
FACTS = ['FactFinancials','FactInventory','FactLabor','FactMarketing',
         'FactPurchaseOrders','FactSales','FactSalesLine','FactStorePerformance']
PRIMARY_KEYS = {
    'DimAccount':'AccountKey', 'DimCustomer':'CustomerKey', 'DimDate':'DateKey',
    'DimEmployee':'EmployeeKey', 'DimFranchise':'FranchiseKey',
    'DimProduct':'ProductKey', 'DimPromotion':'PromotionKey',
    'DimStore':'StoreKey', 'DimSupplier':'SupplierKey',
    'FactFinancials':'FinancialEntryKey', 'FactInventory':'InventoryKey',
    'FactLabor':'LaborKey', 'FactMarketing':'MarketingKey',
    'FactPurchaseOrders':'PurchaseOrderLineKey', 'FactSales':'SalesKey',
    'FactSalesLine':'SalesLineKey', 'FactStorePerformance':'StorePerformanceKey'
}
# Every listed relationship is checked, including nullable references (nulls are allowed).
FOREIGN_KEYS = {
    'FactFinancials': [('DateKey','DimDate','DateKey'),('StoreKey','DimStore','StoreKey'),
                       ('FranchiseKey','DimFranchise','FranchiseKey'),('AccountKey','DimAccount','AccountKey')],
    'FactInventory': [('DateKey','DimDate','DateKey'),('StoreKey','DimStore','StoreKey'),
                      ('ProductKey','DimProduct','ProductKey')],
    'FactLabor': [('DateKey','DimDate','DateKey'),('StoreKey','DimStore','StoreKey'),
                  ('EmployeeKey','DimEmployee','EmployeeKey')],
    'FactMarketing': [('DateKey','DimDate','DateKey'),('PromotionKey','DimPromotion','PromotionKey'),
                      ('StoreKey','DimStore','StoreKey')],
    'FactPurchaseOrders': [('OrderDateKey','DimDate','DateKey'),
                           ('ExpectedDateKey','DimDate','DateKey'),
                           ('ReceivedDateKey','DimDate','DateKey'),
                           ('StoreKey','DimStore','StoreKey'),('SupplierKey','DimSupplier','SupplierKey'),
                           ('ProductKey','DimProduct','ProductKey')],
    'FactSales': [('DateKey','DimDate','DateKey'),('StoreKey','DimStore','StoreKey'),
                  ('CustomerKey','DimCustomer','CustomerKey'),('PromotionKey','DimPromotion','PromotionKey')],
    'FactSalesLine': [('SalesKey','FactSales','SalesKey'),('DateKey','DimDate','DateKey'),
                      ('StoreKey','DimStore','StoreKey'),('ProductKey','DimProduct','ProductKey'),
                      ('PromotionKey','DimPromotion','PromotionKey')],
    'FactStorePerformance': [('DateKey','DimDate','DateKey'),('StoreKey','DimStore','StoreKey')]
}

# 1. Fail-closed preflight: all tables and referenced columns must be readable.
tables = {}
errors = []
for name in DIMENSIONS + FACTS:
    try:
        tables[name] = spark.read.format('delta').load(f'{ROOT}/{name}')
        if PRIMARY_KEYS[name] not in tables[name].columns:
            errors.append(f'{name}: missing primary key {PRIMARY_KEYS[name]}')
    except Exception as exc:
        errors.append(f'{name}: unreadable/missing Delta table: {str(exc)[:300]}')
for fact, refs in FOREIGN_KEYS.items():
    for source_col, parent, parent_col in refs:
        if fact in tables and source_col not in tables[fact].columns:
            errors.append(f'{fact}: missing FK column {source_col}')
        if parent in tables and parent_col not in tables[parent].columns:
            errors.append(f'{parent}: missing referenced column {parent_col}')
if errors:
    print(json.dumps({'gate':'6E','stage':'PREFLIGHT','status':'FAILED','errors':errors}, indent=2))
    raise RuntimeError('Gate 6E preflight failed; no validation conclusions can be drawn.')
print(json.dumps({'gate':'6E','stage':'PREFLIGHT','mode':'READ_ONLY',
                  'tables_readable':len(tables),'relationships_defined':sum(map(len,FOREIGN_KEYS.values())),
                  'status':'PASSED'}))

# 2. PK checks. Count every row in a duplicate group as affected; also count extra rows.
key_checks = []
for name in DIMENSIONS + FACTS:
    df, key = tables[name], PRIMARY_KEYS[name]
    total = df.count()
    nulls = df.filter(F.col(key).isNull()).count()
    groups = df.filter(F.col(key).isNotNull()).groupBy(key).count().filter(F.col('count') > 1)
    stats = groups.agg(F.count('*').alias('groups'),
                       F.coalesce(F.sum('count'),F.lit(0)).alias('affected'),
                       F.coalesce(F.sum(F.col('count')-1),F.lit(0)).alias('extras')).first()
    item = {'table':name,'key':key,'rows':total,'null_keys':nulls,
            'duplicate_groups':stats['groups'],'duplicate_affected_rows':stats['affected'],
            'duplicate_extra_rows':stats['extras']}
    key_checks.append(item)
    print(json.dumps({'check':'PRIMARY_KEY',**item}))

# 3. Anti-joins detect non-null orphan FKs; no relationship is skipped silently.
fk_checks = []
for fact, refs in FOREIGN_KEYS.items():
    for source_col, parent, parent_col in refs:
        child = tables[fact].select(F.col(source_col).alias('fk'))
        nonnull = child.filter(F.col('fk').isNotNull())
        reference = tables[parent].select(F.col(parent_col).alias('pk')).filter(
            F.col('pk').isNotNull()).distinct()
        orphan = nonnull.join(reference, nonnull['fk'] == reference['pk'], 'left_anti')
        item = {'table':fact,'column':source_col,'references':f'{parent}.{parent_col}',
                'non_null_references':nonnull.count(),'orphan_rows':orphan.count(),
                'orphan_distinct_values':orphan.select('fk').distinct().count()}
        fk_checks.append(item)
        print(json.dumps({'check':'FOREIGN_KEY',**item}))

# 4. Advisory reconciliation metrics; not automatic failures without approved business rules.
advisory = []
for table, columns in {
    'FactSales':['GrossSales','DiscountAmount','NetSales','TaxAmount','TotalAmount'],
    'FactSalesLine':['Quantity','GrossLineAmount','DiscountAmount','NetLineAmount','TaxAmount'],
    'FactPurchaseOrders':['OrderedQuantity','ReceivedQuantity','ExtendedCost'],
    'FactStorePerformance':['TransactionCount','UnitsSold','NetSales','LaborCost','FoodCost','WasteCost']
}.items():
    df = tables[table]
    for col in columns:
        if col not in df.columns:
            errors.append(f'{table}: missing advisory column {col}')
            continue
        if not isinstance(df.schema[col].dataType, NumericType):
            errors.append(f'{table}.{col}: expected numeric type for advisory sum')
            continue
        val = df.agg(F.sum(F.col(col)).cast('string').alias('sum')).first()['sum']
        item = {'table':table,'metric':f'SUM({col})','value':val}
        advisory.append(item)
        print(json.dumps({'check':'ADVISORY_TOTAL',**item}))

# SalesLine and Sales share SalesKey, but validate consistency of transaction/date/store fields
# separately: a valid SalesKey alone does not guarantee a consistent transaction linkage.
parent = tables['FactSales'].select('SalesKey','TransactionID','DateKey','StoreKey').alias('p')
child = tables['FactSalesLine'].select('SalesKey','TransactionID','DateKey','StoreKey').alias('c')
joined = child.join(parent, F.col('c.SalesKey') == F.col('p.SalesKey'), 'inner')
mislinked = joined.filter(
    (~F.col('c.TransactionID').eqNullSafe(F.col('p.TransactionID'))) |
    (~F.col('c.DateKey').eqNullSafe(F.col('p.DateKey'))) |
    (~F.col('c.StoreKey').eqNullSafe(F.col('p.StoreKey')))
).count()
print(json.dumps({'check':'SALES_LINE_PARENT_ATTRIBUTES','mismatched_rows':mislinked}))

pk_failures = sum(x['null_keys'] + x['duplicate_extra_rows'] for x in key_checks)
fk_failures = sum(x['orphan_rows'] for x in fk_checks)
status = 'PASSED' if not errors and pk_failures == 0 and fk_failures == 0 and mislinked == 0 else 'FAILED'
summary = {'gate':'6E','mode':'READ_ONLY','status':status,
           'tables_checked':len(key_checks),'fact_rows':sum(x['rows'] for x in key_checks if x['table'] in FACTS),
           'dimension_rows':sum(x['rows'] for x in key_checks if x['table'] in DIMENSIONS),
           'relationships_checked':len(fk_checks),'primary_key_failure_rows':pk_failures,
           'orphan_foreign_key_rows':fk_failures,'sales_line_parent_mismatches':mislinked,
           'advisory_metrics':len(advisory),'errors':errors}
print('GATE 6E FINAL SUMMARY')
print(json.dumps(summary,indent=2))
if status != 'PASSED':
    raise RuntimeError('Gate 6E integrity validation FAILED. Review results before Gold loading.')
print('GATE 6E INTEGRITY VALIDATION PASSED — NO DATA MODIFIED')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
