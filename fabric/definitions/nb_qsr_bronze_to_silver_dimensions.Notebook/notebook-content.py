# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************


# ============================================================
# Gate 6C | QSR Bronze -> Silver Dimensions
# Microsoft Fabric | Manual-first | Fail-closed
#
# Notebook: nb_qsr_bronze_to_silver_dimensions
#
# Corrections:
# 1. Write Delta tables to Tables/dbo/<Dimension>
# 2. Validate types against original Bronze CSV values
# 3. Preserve original CSV values in quarantine
# 4. Preflight all destination and quarantine paths
# 5. Transform and validate all dimensions before writes
# 6. Verify row counts after successful writes
#
# DEFAULT: DRY_RUN = True
#
# No default Lakehouse attachment required.
# ============================================================

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
import json

# ------------------------------------------------------------
# 1. Configuration
# ------------------------------------------------------------

WORKSPACE = "qsr-dev-data-platform"
BRONZE = "lh_qsr_bronze"
SILVER = "lh_qsr_silver"

DRY_RUN = False

BASE = (
    f"abfss://{WORKSPACE}@"
    "onelake.dfs.fabric.microsoft.com"
)

SRC = (
    f"{BASE}/{BRONZE}.Lakehouse/"
    "Files/master-data"
)

DEST = (
    f"{BASE}/{SILVER}.Lakehouse/"
    "Tables/dbo"
)

QUARANTINE = (
    f"{BASE}/{SILVER}.Lakehouse/"
    "Files/quarantine/master-data"
)

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

# ------------------------------------------------------------
# 2. Approved Warehouse DDL Contracts
# ------------------------------------------------------------

DIM_DATE_CONTRACT = [
    ["DateKey", "INT", True],
    ["FullDate", "DATE", True],
    ["CalendarYear", "INT", True],
    ["CalendarQuarter", "INT", True],
    ["CalendarMonth", "INT", True],
    ["MonthName", "VARCHAR(20)", False],
    ["DayOfMonth", "INT", True],
    ["DayOfWeek", "INT", True],
    ["DayName", "VARCHAR(20)", False],
    ["IsWeekend", "BIT", True],
]

CONTRACTS = {
    "DimStore": [
        ["StoreKey", "BIGINT", True],
        ["StoreID", "VARCHAR(30)", True],
        ["StoreName", "VARCHAR(150)", True],
        ["FranchiseID", "VARCHAR(30)", False],
        ["City", "VARCHAR(100)", False],
        ["StateCode", "VARCHAR(2)", False],
        ["Region", "VARCHAR(50)", False],
        ["OpenDate", "DATE", False],
        ["IsActive", "BIT", True],
        ["EffectiveStartDate", "DATETIME2(6)", False],
        ["EffectiveEndDate", "DATETIME2(6)", False],
        ["IsCurrent", "BIT", False],
    ],
    "DimProduct": [
        ["ProductKey", "BIGINT", True],
        ["ProductID", "VARCHAR(30)", True],
        ["ProductName", "VARCHAR(150)", True],
        ["Category", "VARCHAR(50)", False],
        ["SubCategory", "VARCHAR(50)", False],
        ["StandardPrice", "DECIMAL(18,2)", False],
        ["IsActive", "BIT", True],
        ["EffectiveStartDate", "DATETIME2(6)", False],
        ["EffectiveEndDate", "DATETIME2(6)", False],
        ["IsCurrent", "BIT", False],
    ],
    "DimCustomer": [
        ["CustomerKey", "BIGINT", True],
        ["CustomerID", "VARCHAR(50)", True],
        ["LoyaltyTier", "VARCHAR(30)", False],
        ["EnrollmentDate", "DATE", False],
        ["CustomerSegment", "VARCHAR(50)", False],
        ["IsActive", "BIT", True],
        ["EffectiveStartDate", "DATETIME2(6)", False],
        ["EffectiveEndDate", "DATETIME2(6)", False],
        ["IsCurrent", "BIT", False],
    ],
    "DimEmployee": [
        ["EmployeeKey", "BIGINT", True],
        ["EmployeeID", "VARCHAR(50)", True],
        ["StoreID", "VARCHAR(30)", False],
        ["JobTitle", "VARCHAR(100)", False],
        ["EmploymentType", "VARCHAR(30)", False],
        ["HireDate", "DATE", False],
        ["TerminationDate", "DATE", False],
        ["IsActive", "BIT", True],
        ["EffectiveStartDate", "DATETIME2(6)", False],
        ["EffectiveEndDate", "DATETIME2(6)", False],
        ["IsCurrent", "BIT", False],
    ],
    "DimSupplier": [
        ["SupplierKey", "BIGINT", True],
        ["SupplierID", "VARCHAR(30)", True],
        ["SupplierName", "VARCHAR(150)", True],
        ["SupplierCategory", "VARCHAR(80)", False],
        ["StateCode", "VARCHAR(2)", False],
        ["IsActive", "BIT", True],
        ["EffectiveStartDate", "DATETIME2(6)", False],
        ["EffectiveEndDate", "DATETIME2(6)", False],
        ["IsCurrent", "BIT", False],
    ],
    "DimPromotion": [
        ["PromotionKey", "BIGINT", True],
        ["PromotionID", "VARCHAR(40)", True],
        ["PromotionName", "VARCHAR(150)", False],
        ["PromotionType", "VARCHAR(50)", False],
        ["StartDate", "DATE", False],
        ["EndDate", "DATE", False],
        ["DiscountType", "VARCHAR(30)", False],
        ["DiscountValue", "DECIMAL(18,2)", False],
        ["IsActive", "BIT", True],
    ],
    "DimFranchise": [
        ["FranchiseKey", "BIGINT", True],
        ["FranchiseID", "VARCHAR(30)", True],
        ["FranchiseName", "VARCHAR(150)", True],
        ["Territory", "VARCHAR(100)", False],
        ["AgreementStartDate", "DATE", False],
        ["IsActive", "BIT", True],
        ["EffectiveStartDate", "DATETIME2(6)", False],
        ["EffectiveEndDate", "DATETIME2(6)", False],
        ["IsCurrent", "BIT", False],
    ],
    "DimAccount": [
        ["AccountKey", "BIGINT", True],
        ["AccountCode", "VARCHAR(30)", True],
        ["AccountName", "VARCHAR(150)", True],
        ["AccountType", "VARCHAR(50)", False],
        ["AccountCategory", "VARCHAR(80)", False],
        ["IsActive", "BIT", True],
    ],
}

CONTRACTS["DimDate"] = DIM_DATE_CONTRACT

# ------------------------------------------------------------
# 3. Type Mapping
# ------------------------------------------------------------

def spark_cast_type(sql_type):
    sql_type = sql_type.upper()

    if sql_type == "BIT":
        return "BOOLEAN"

    if sql_type.startswith("DATETIME2"):
        return "TIMESTAMP"

    return sql_type


# ------------------------------------------------------------
# 4. Path Inspection
# ------------------------------------------------------------

def path_exists(path):
    try:
        notebookutils.fs.ls(path)
        return True

    except Exception as exc:
        message = str(exc).lower()

        missing_indicators = [
            "pathnotfound",
            "path_not_found",
            "filenotfoundexception",
            "does not exist",
            "404",
        ]

        if any(
            indicator in message
            for indicator in missing_indicators
        ):
            return False

        raise RuntimeError(
            f"Unable to inspect path: {path}"
        ) from exc


# ------------------------------------------------------------
# 5. Preflight
# ------------------------------------------------------------

def preflight():
    missing_contracts = [
        name
        for name in NAMES
        if name not in CONTRACTS
        or not CONTRACTS[name]
    ]

    if missing_contracts:
        raise RuntimeError(
            "Missing approved contracts: "
            f"{missing_contracts}"
        )

    # Verify the target schema directory exists.
    if not path_exists(DEST):
        raise RuntimeError(
            "Target dbo schema directory not found: "
            f"{DEST}"
        )

    # Check every destination before processing.
    existing_destinations = [
        name
        for name in NAMES
        if path_exists(f"{DEST}/{name}")
    ]

    if existing_destinations:
        raise RuntimeError(
            "Gate 6C BLOCKED: destination paths exist: "
            f"{existing_destinations}"
        )

    existing_quarantine = [
        name
        for name in NAMES
        if path_exists(f"{QUARANTINE}/{name}")
    ]

    if existing_quarantine:
        raise RuntimeError(
            "Gate 6C BLOCKED: quarantine paths exist: "
            f"{existing_quarantine}"
        )

    print(json.dumps({
        "gate": "6C",
        "stage": "PREFLIGHT",
        "dry_run": DRY_RUN,
        "contracts": len(NAMES),
        "destination": DEST,
        "status": "PASSED",
    }))


# ------------------------------------------------------------
# 6. Transform and Validate
# ------------------------------------------------------------

def transform(name, columns):

    expected = [col[0] for col in columns]

    source_path = f"{SRC}/{name}.csv"

    raw = (
        spark.read
        .option("header", "true")
        .option("multiLine", "false")
        .option("mode", "FAILFAST")
        .csv(source_path)
    )

    if raw.columns != expected:
        raise ValueError(
            f"{name}: CSV header mismatch. "
            f"Expected={expected}; "
            f"Actual={raw.columns}"
        )

    # Retain original strings alongside typed columns.
    typed_columns = [
        F.expr(
            f"try_cast(`{col}` AS "
            f"{spark_cast_type(dtype)})"
        ).alias(col)
        for col, dtype, required in columns
    ]

    raw_columns = [
        F.col(f"`{col}`").alias(f"__raw_{col}")
        for col in expected
    ]

    converted = raw.select(
        *typed_columns,
        *raw_columns
    )

    violations = []

    for col, dtype, required in columns:

        original = F.col(f"__raw_{col}")
        trimmed = F.trim(original)
        typed = F.col(col)

        missing = (
            original.isNull()
            | (trimmed == "")
        )

        if required:
            violations.append(
                F.when(
                    missing,
                    F.lit(f"{col}:required")
                )
            )

        # Detect malformed nonblank values even for
        # optional columns.
        violations.append(
            F.when(
                (~missing) & typed.isNull(),
                F.lit(f"{col}:invalid_type")
            )
        )

        if dtype.upper().startswith("VARCHAR("):
            limit = int(
                dtype[
                    dtype.index("(") + 1:
                    dtype.index(")")
                ]
            )

            violations.append(
                F.when(
                    original.isNotNull()
                    & (F.length(original) > limit),
                    F.lit(f"{col}:too_long")
                )
            )

    # Surrogate-key duplicate validation.
    pk = next(
        (
            col
            for col in expected
            if col.endswith("Key")
        ),
        None
    )

    if not pk:
        raise ValueError(
            f"{name}: no surrogate key found"
        )

    window = Window.partitionBy(pk)

    converted = converted.withColumn(
        "__duplicate_count",
        F.count("*").over(window)
    )

    violations.append(
        F.when(
            F.col("__duplicate_count") > 1,
            F.lit(f"{pk}:duplicate")
        )
    )

    # Preserve original DimDate checks.
    if name == "DimDate":

        d = converted

        violations.extend([
            F.when(
                d.DateKey != F.date_format(
                    d.FullDate,
                    "yyyyMMdd"
                ).cast("int"),
                F.lit("DateKey:date_mismatch")
            ),
            F.when(
                d.CalendarYear != F.year(d.FullDate),
                F.lit("CalendarYear:mismatch")
            ),
            F.when(
                d.CalendarQuarter != F.quarter(d.FullDate),
                F.lit("CalendarQuarter:mismatch")
            ),
            F.when(
                d.CalendarMonth != F.month(d.FullDate),
                F.lit("CalendarMonth:mismatch")
            ),
            F.when(
                d.DayOfMonth != F.dayofmonth(d.FullDate),
                F.lit("DayOfMonth:mismatch")
            ),
            F.when(
                ~d.CalendarQuarter.between(1, 4),
                F.lit("CalendarQuarter:range")
            ),
            F.when(
                ~d.CalendarMonth.between(1, 12),
                F.lit("CalendarMonth:range")
            ),
            F.when(
                ~d.DayOfWeek.between(1, 7),
                F.lit("DayOfWeek:range")
            ),
        ])

    # Generate violation flags.
    staged = converted.withColumn(
        "_issues",
        F.filter(
            F.array(*violations),
            lambda issue: issue.isNotNull()
        )
    )

    bad = staged.filter(
        F.size("_issues") > 0
    ).drop("__duplicate_count")

    good = staged.filter(
        F.size("_issues") == 0
    ).drop(
        "_issues",
        "__duplicate_count",
        *[
            f"__raw_{col}"
            for col in expected
        ]
    )

    source_rows = raw.count()
    valid_rows = good.count()
    invalid_rows = bad.count()

    if valid_rows + invalid_rows != source_rows:
        raise RuntimeError(
            f"{name}: reconciliation failed. "
            f"Source={source_rows}, "
            f"Valid={valid_rows}, "
            f"Invalid={invalid_rows}"
        )

    print(json.dumps({
        "table": name,
        "source_rows": source_rows,
        "valid_rows": valid_rows,
        "quarantined_rows": invalid_rows,
        "dry_run": DRY_RUN,
    }))

    return {
        "name": name,
        "good": good,
        "bad": bad,
        "source_rows": source_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
    }


# ------------------------------------------------------------
# 7. Execute Preflight
# ------------------------------------------------------------

preflight()


# ------------------------------------------------------------
# 8. Transform All Nine Dimensions
# ------------------------------------------------------------

results = []

for name in NAMES:
    result = transform(
        name,
        CONTRACTS[name]
    )

    results.append(result)


# ------------------------------------------------------------
# 9. Write Only When Explicitly Authorized
# ------------------------------------------------------------

if not DRY_RUN:

    for result in results:

        name = result["name"]

        if result["invalid_rows"] > 0:

            result["bad"].write \
                .format("delta") \
                .mode("errorifexists") \
                .save(
                    f"{QUARANTINE}/{name}"
                )

        result["good"].write \
            .format("delta") \
            .mode("errorifexists") \
            .save(
                f"{DEST}/{name}"
            )

        # Verify written Delta row count.
        actual_rows = (
            spark.read
            .format("delta")
            .load(f"{DEST}/{name}")
            .count()
        )

        if actual_rows != result["valid_rows"]:
            raise RuntimeError(
                f"{name}: post-write mismatch. "
                f"Expected={result['valid_rows']}; "
                f"Actual={actual_rows}"
            )

        print(
            f"{name}: WRITE VERIFIED — "
            f"{actual_rows} rows"
        )


# ------------------------------------------------------------
# 10. Final Summary
# ------------------------------------------------------------

summary = {
    "gate": "6C",
    "mode": (
        "DRY_RUN"
        if DRY_RUN
        else "SILVER_WRITE"
    ),
    "dimensions_processed": len(results),
    "source_rows": sum(
        r["source_rows"]
        for r in results
    ),
    "valid_rows": sum(
        r["valid_rows"]
        for r in results
    ),
    "quarantined_rows": sum(
        r["invalid_rows"]
        for r in results
    ),
    "status": (
        "DRY_RUN_COMPLETE"
        if DRY_RUN
        else "WRITE_COMPLETE"
    ),
}

print(
    json.dumps(
        summary,
        indent=2
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
