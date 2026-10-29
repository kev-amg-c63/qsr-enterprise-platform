# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Gate 6D-C | QSR Bronze -> Silver Facts | manual-first, fail-closed
# Notebook: nb_qsr_bronze_to_silver_facts
# No default Lakehouse attachment required. DRY RUN by default.
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

WORKSPACE = 'qsr-dev-data-platform'
BRONZE = 'lh_qsr_bronze'
SILVER = 'lh_qsr_silver'
DRY_RUN = False
BASE = f'abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com'
SOURCE_ROOT = f'{BASE}/{BRONZE}.Lakehouse/Files'
DEST = f'{BASE}/{SILVER}.Lakehouse/Tables/dbo'
QUARANTINE = f'{BASE}/{SILVER}.Lakehouse/Files/quarantine/facts'

# Contract tuples: (column, Warehouse SQL type, required/non-null)
CONTRACTS = {
    'FactFinancials': [
        ('FinancialEntryKey','BIGINT',True),('JournalEntryID','VARCHAR(50)',True),
        ('JournalLineNumber','INT',True),('DateKey','INT',True),
        ('StoreKey','BIGINT',False),('FranchiseKey','BIGINT',False),
        ('AccountKey','BIGINT',True),('DebitAmount','DECIMAL(18,2)',False),
        ('CreditAmount','DECIMAL(18,2)',False),('CurrencyCode','VARCHAR(3)',False),
        ('PostingTimestamp','DATETIME2',False),('LoadTimestamp','DATETIME2',False)
    ],
    'FactInventory': [
        ('InventoryKey','BIGINT',True),('DateKey','INT',True),('StoreKey','BIGINT',True),
        ('ProductKey','BIGINT',True),('SnapshotTimestamp','DATETIME2',False),
        ('QuantityOnHand','DECIMAL(18,3)',False),('QuantityReserved','DECIMAL(18,3)',False),
        ('UnitCost','DECIMAL(18,4)',False),('InventoryValue','DECIMAL(18,2)',False),
        ('ReorderPoint','DECIMAL(18,3)',False),('LoadTimestamp','DATETIME2',False)
    ],
    'FactLabor': [
        ('LaborKey','BIGINT',True),('ShiftID','VARCHAR(50)',True),('DateKey','INT',True),
        ('StoreKey','BIGINT',True),('EmployeeKey','BIGINT',True),
        ('ShiftStart','DATETIME2',False),('ShiftEnd','DATETIME2',False),
        ('RegularHours','DECIMAL(9,2)',False),('OvertimeHours','DECIMAL(9,2)',False),
        ('HourlyRate','DECIMAL(18,4)',False),('LaborCost','DECIMAL(18,2)',False),
        ('LoadTimestamp','DATETIME2',False)
    ],
    'FactMarketing': [
        ('MarketingKey','BIGINT',True),('CampaignID','VARCHAR(50)',True),
        ('DateKey','INT',True),('PromotionKey','BIGINT',False),('StoreKey','BIGINT',False),
        ('Channel','VARCHAR(50)',True),('Impressions','BIGINT',False),
        ('Clicks','BIGINT',False),('Conversions','BIGINT',False),
        ('SpendAmount','DECIMAL(18,2)',False),('AttributedRevenue','DECIMAL(18,2)',False),
        ('LoadTimestamp','DATETIME2',False)
    ],
    'FactPurchaseOrders': [
        ('PurchaseOrderLineKey','BIGINT',True),('PurchaseOrderID','VARCHAR(50)',True),
        ('LineNumber','INT',True),('OrderDateKey','INT',True),
        ('ExpectedDateKey','INT',False),('ReceivedDateKey','INT',False),
        ('StoreKey','BIGINT',True),('SupplierKey','BIGINT',True),
        ('ProductKey','BIGINT',True),('OrderedQuantity','DECIMAL(18,3)',False),
        ('ReceivedQuantity','DECIMAL(18,3)',False),('UnitCost','DECIMAL(18,4)',False),
        ('ExtendedCost','DECIMAL(18,2)',False),('OrderStatus','VARCHAR(30)',False),
        ('LoadTimestamp','DATETIME2',False)
    ],
    'FactSales': [
        ('SalesKey','BIGINT',True),('TransactionID','VARCHAR(50)',True),
        ('DateKey','INT',True),('StoreKey','BIGINT',True),
        ('CustomerKey','BIGINT',False),('PromotionKey','BIGINT',False),
        ('TransactionTimestamp','DATETIME2',False),('GrossSales','DECIMAL(18,2)',False),
        ('DiscountAmount','DECIMAL(18,2)',False),('NetSales','DECIMAL(18,2)',False),
        ('TaxAmount','DECIMAL(18,2)',False),('TotalAmount','DECIMAL(18,2)',False),
        ('ItemCount','INT',False),('PaymentMethod','VARCHAR(30)',False),
        ('SalesChannel','VARCHAR(30)',False),('LoadTimestamp','DATETIME2',False)
    ],
    'FactSalesLine': [
        ('SalesLineKey','BIGINT',True),('SalesKey','BIGINT',True),
        ('TransactionID','VARCHAR(50)',True),('LineNumber','INT',True),
        ('DateKey','INT',True),('StoreKey','BIGINT',True),
        ('ProductKey','BIGINT',True),('PromotionKey','BIGINT',False),
        ('Quantity','DECIMAL(18,3)',True),('UnitPrice','DECIMAL(18,2)',False),
        ('GrossLineAmount','DECIMAL(18,2)',False),('DiscountAmount','DECIMAL(18,2)',False),
        ('NetLineAmount','DECIMAL(18,2)',False),('TaxAmount','DECIMAL(18,2)',False),
        ('LoadTimestamp','DATETIME2',False)
    ],
    'FactStorePerformance': [
        ('StorePerformanceKey','BIGINT',True),('DateKey','INT',True),
        ('StoreKey','BIGINT',True),('TransactionCount','BIGINT',False),
        ('UnitsSold','DECIMAL(18,3)',False),('NetSales','DECIMAL(18,2)',False),
        ('LaborHours','DECIMAL(18,2)',False),('LaborCost','DECIMAL(18,2)',False),
        ('FoodCost','DECIMAL(18,2)',False),('WasteCost','DECIMAL(18,2)',False),
        ('LoadTimestamp','DATETIME2',False)
    ]
}
SOURCE_FOLDERS = {
    'FactFinancials':'finance', 'FactInventory':'inventory',
    'FactLabor':'labor', 'FactMarketing':'marketing',
    'FactPurchaseOrders':'purchasing', 'FactSales':'pos',
    'FactSalesLine':'pos', 'FactStorePerformance':'finance'
}
NAMES = list(SOURCE_FOLDERS)

def path_exists(path):
    try:
        notebookutils.fs.ls(path)
        return True
    except Exception as exc:
        msg = str(exc).lower()
        if any(s in msg for s in ('pathnotfound','path_not_found',
                                   'filenotfoundexception','does not exist')):
            return False
        raise RuntimeError(f'Unable to inspect {path}') from exc

def cast_type(sql_type):
    t = sql_type.upper()
    if t.startswith('DATETIME2'):
        return 'TIMESTAMP'
    if t.startswith(('VARCHAR(', 'CHAR(')):
        return 'STRING'
    return t

def safe_typed_column(col, sql_type):
    """Allow exact integral decimal notation for INT/BIGINT; reject fractions."""
    target = cast_type(sql_type)
    if target in ('INT', 'BIGINT'):
        raw_value = F.trim(F.col(f'`{col}`'))
        # Accepted: 39, -39, +39, 39.0000; rejected: 39.25, 39e0, N/A.
        integral = raw_value.rlike(r'^[+-]?[0-9]+(?:\.0+)?$')
        # try_cast still enforces INT/BIGINT range; malformed values become NULL
        # and are flagged by the existing original-value invalid_type check.
        return F.when(integral, F.expr(f"try_cast(regexp_replace(trim(`{col}`), '[.]0+$', '') AS {target})")).otherwise(
            F.lit(None).cast(target)
        ).alias(col)
    return F.expr(f'try_cast(`{col}` AS {target})').alias(col)

def preflight():
    if set(CONTRACTS) != set(NAMES):
        raise RuntimeError('Fact contract list does not match source mapping')
    if not path_exists(DEST):
        raise RuntimeError(f'Target dbo schema directory missing: {DEST}')
    existing = [n for n in NAMES if path_exists(f'{DEST}/{n}')]
    if existing:
        raise RuntimeError(f'BLOCKED: existing Silver fact paths: {existing}')
    existing_q = [n for n in NAMES if path_exists(f'{QUARANTINE}/{n}')]
    if existing_q:
        raise RuntimeError(f'BLOCKED: existing quarantine paths: {existing_q}')
    print(json.dumps({'gate':'6D','stage':'PREFLIGHT','mode':'DRY_RUN' if DRY_RUN else 'SILVER_WRITE',
                      'contracts':len(CONTRACTS),'destination':DEST,'status':'PASSED'}))

def process(name):
    contract = CONTRACTS[name]
    expected = [c for c, _, _ in contract]
    src = f'{SOURCE_ROOT}/{SOURCE_FOLDERS[name]}/{name}.csv'
    raw = (spark.read.option('header','true').option('multiLine','false')
           .option('mode','FAILFAST').csv(src))
    if raw.columns != expected:
        raise RuntimeError(f'{name}: header mismatch. Expected={expected}; Actual={raw.columns}')

    # Preserve the original CSV strings in __raw_ columns, including for optional values.
    typed = [safe_typed_column(col, sql_type)
             for col, sql_type, _ in contract]
    original = [F.col(f'`{col}`').alias(f'__raw_{col}') for col in expected]
    df = raw.select(*typed, *original)
    issues = []
    for col, sql_type, required in contract:
        source = F.col(f'__raw_{col}')
        trimmed = F.trim(source)
        missing = source.isNull() | (trimmed == '')
        converted = F.col(col)
        if required:
            issues.append(F.when(missing, F.lit(f'{col}:required')))
        issues.append(F.when(~missing & converted.isNull(), F.lit(f'{col}:invalid_type')))
        if sql_type.upper().startswith('VARCHAR('):
            limit = int(sql_type.split('(')[1].split(')')[0])
            issues.append(F.when(source.isNotNull() & (F.length(source) > limit),
                                 F.lit(f'{col}:too_long')))

    pk = expected[0]
    if not pk.endswith('Key') or not contract[0][2]:
        raise RuntimeError(f'{name}: unexpected surrogate key contract')
    # Window over raw key so malformed key strings are also grouped consistently.
    df = df.withColumn('__pk_duplicates', F.count(F.lit(1)).over(Window.partitionBy(F.col(f'__raw_{pk}'))))
    issues.append(F.when(F.col('__pk_duplicates') > 1, F.lit(f'{pk}:duplicate')))
    staged = df.withColumn('_issues', F.filter(F.array(*issues), lambda x: x.isNotNull()))
    invalid = staged.filter(F.size('_issues') > 0).drop('__pk_duplicates')
    valid = staged.filter(F.size('_issues') == 0).drop(
        '_issues','__pk_duplicates', *[f'__raw_{c}' for c in expected]
    )
    source_rows = raw.count()
    valid_rows = valid.count()
    invalid_rows = invalid.count()
    if valid_rows + invalid_rows != source_rows:
        raise RuntimeError(f'{name}: row reconciliation failed')
    report = {'table':name,'source_rows':source_rows,'valid_rows':valid_rows,
              'quarantined_rows':invalid_rows,'dry_run':DRY_RUN}
    print(json.dumps(report))
    return {'report':report,'valid':valid,'invalid':invalid}

preflight()
results = [process(name) for name in NAMES]

summary = {'gate':'6D','mode':'DRY_RUN' if DRY_RUN else 'SILVER_WRITE',
           'facts_processed':len(results),
           'source_rows':sum(r['report']['source_rows'] for r in results),
           'valid_rows':sum(r['report']['valid_rows'] for r in results),
           'quarantined_rows':sum(r['report']['quarantined_rows'] for r in results)}

# Gate 6D-D: all eight tables must validate before the first Silver write.
if not DRY_RUN and summary['quarantined_rows'] != 0:
    raise RuntimeError(
        f"Silver load BLOCKED: {summary['quarantined_rows']:,} quarantined records."
    )
print('ZERO-QUARANTINE CHECK PASSED')

if not DRY_RUN:
    # No overwrite. Writes are sequential, not atomic across the eight datasets.
    for name, result in zip(NAMES, results):
        if result['report']['quarantined_rows']:
            result['invalid'].write.format('delta').mode('errorifexists').save(f'{QUARANTINE}/{name}')
        result['valid'].write.format('delta').mode('errorifexists').save(f'{DEST}/{name}')
        read_back = spark.read.format('delta').load(f'{DEST}/{name}').count()
        if read_back != result['report']['valid_rows']:
            raise RuntimeError(f'{name}: post-write count mismatch: {read_back}')
        print(f'{name}: WRITE VERIFIED — {read_back} rows')

summary['status'] = 'DRY_RUN_COMPLETE' if DRY_RUN else 'WRITE_COMPLETE'
print(json.dumps(summary, indent=2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Gate 6D — Quarantine issue diagnosis
# READ ONLY — no writes, deletes, or modifications.

from pyspark.sql import functions as F

for r in results:
    report = r["report"]

    if report["quarantined_rows"] == 0:
        continue

    print(
        f"\n{report['table']} — "
        f"{report['quarantined_rows']:,} quarantined rows"
    )

    (
        r["invalid"]
        .select(F.explode("_issues").alias("issue"))
        .groupBy("issue")
        .count()
        .orderBy(F.desc("count"))
        .show(50, truncate=False)
    )

def sample(table, column, issue_type, n=10):
    result = next(
        r for r in results
        if r["report"]["table"] == table
    )

    (
        result["invalid"]
        .filter(
            F.array_contains(
                F.col("_issues"),
                f"{column}:{issue_type}"
            )
        )
        .groupBy(f"__raw_{column}")
        .count()
        .orderBy(F.desc("count"))
        .show(n, truncate=False)
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Gate 6D — Original value inspection
# READ ONLY — no writes or modifications

sample("FactPurchaseOrders", "ReceivedDateKey", "invalid_type")
sample("FactSales", "CustomerKey", "invalid_type")
sample("FactSales", "PromotionKey", "invalid_type")
sample("FactSalesLine", "PromotionKey", "invalid_type")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
