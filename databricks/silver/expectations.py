"""
Silver Layer Data Quality Expectations (DLT-Style)

Defines validation rules for Silver layer data quality with severity levels:
- DROP: Critical failures that prevent writing to Silver
- WARN: Non-critical issues logged but data is still written

Expectations are applied before writing resolved states to silver_cows_history.
"""

from datetime import datetime
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, lit, when, concat_ws, array
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# EXPECTATION DEFINITIONS
# ============================================================================

class ExpectationSeverity:
    """Severity levels for expectation failures"""
    DROP = "DROP"    # Critical: Drop rows that fail
    WARN = "WARN"    # Non-critical: Log but allow


class Expectation:
    """Represents a single data quality expectation"""
    
    def __init__(self, name: str, condition: str, severity: str, description: str = None):
        """
        Args:
            name: Expectation identifier
            condition: SQL WHERE clause condition (rows passing this are valid)
            severity: ExpectationSeverity.DROP or ExpectationSeverity.WARN
            description: Human-readable description
        """
        self.name = name
        self.condition = condition
        self.severity = severity
        self.description = description or name
    
    def __repr__(self):
        return f"Expectation(name={self.name}, severity={self.severity})"


# Valid cattle breeds (comprehensive list)
VALID_BREEDS = [
    'Angus', 'Black Angus', 'Hereford', 'Charolais', 'Simmental', 'Limousin',
    'Gelbvieh', 'Red Angus', 'Brahman', 'Brangus', 'Beefmaster',
    'Holstein', 'Jersey', 'Guernsey', 'Brown Swiss', 'Ayrshire',
    'Shorthorn', 'Longhorn', 'Highland', 'Belted Galloway', 'Devon',
    'Piedmontese', 'Chianina', 'Blonde d\'Aquitaine', 'Salers', 'Tarentaise',
    'Maine-Anjou', 'Marchigiana', 'Romagnola', 'Santa Gertrudis', 'Senepol',
    'Wagyu', 'Murray Grey', 'Dexter', 'Galloway', 'Welsh Black',
    'Mixed', 'Crossbred', 'Other', 'Unknown'
]

# Build breed condition for SQL
BREED_CONDITION = f"breed IN ({', '.join([repr(b) for b in VALID_BREEDS])})"


# Silver Layer Expectations
SILVER_EXPECTATIONS = [
    # ========================================================================
    # CRITICAL EXPECTATIONS (DROP)
    # ========================================================================
    
    Expectation(
        name="valid_cow_id",
        condition="cow_id IS NOT NULL",
        severity=ExpectationSeverity.DROP,
        description="Cow ID must not be null"
    ),
    
    Expectation(
        name="valid_tenant_id",
        condition="tenant_id IS NOT NULL",
        severity=ExpectationSeverity.DROP,
        description="Tenant ID must not be null"
    ),
    
    Expectation(
        name="valid_tag_number",
        condition="tag_number IS NOT NULL AND LENGTH(TRIM(tag_number)) > 0",
        severity=ExpectationSeverity.DROP,
        description="Tag number must not be null or empty"
    ),
    
    Expectation(
        name="valid_status",
        condition="status IN ('active', 'inactive')",
        severity=ExpectationSeverity.DROP,
        description="Status must be 'active' or 'inactive'"
    ),
    
    Expectation(
        name="valid_start_at",
        condition="__START_AT IS NOT NULL",
        severity=ExpectationSeverity.DROP,
        description="Start timestamp must not be null"
    ),
    
    Expectation(
        name="valid_sequence",
        condition="__SEQUENCE_NUMBER IS NOT NULL AND __SEQUENCE_NUMBER >= 0",
        severity=ExpectationSeverity.DROP,
        description="Sequence number must be non-negative"
    ),
    
    # ========================================================================
    # NON-CRITICAL EXPECTATIONS (WARN)
    # ========================================================================
    
    Expectation(
        name="valid_breed",
        condition=BREED_CONDITION,
        severity=ExpectationSeverity.WARN,
        description=f"Breed should be one of {len(VALID_BREEDS)} recognized breeds"
    ),
    
    Expectation(
        name="valid_birth_date",
        condition="birth_date >= '2000-01-01' AND birth_date <= CURRENT_DATE",
        severity=ExpectationSeverity.WARN,
        description="Birth date should be between 2000-01-01 and today"
    ),
    
    Expectation(
        name="valid_weight",
        condition="weight_kg IS NULL OR (weight_kg > 0 AND weight_kg < 2000)",
        severity=ExpectationSeverity.WARN,
        description="Weight should be between 0 and 2000 kg (or null)"
    ),
    
    Expectation(
        name="valid_sex",
        condition="sex IS NULL OR sex IN ('male', 'female')",
        severity=ExpectationSeverity.WARN,
        description="Sex should be 'male', 'female', or null"
    ),
    
    Expectation(
        name="reasonable_birth_weight",
        condition="birth_weight_kg IS NULL OR (birth_weight_kg > 10 AND birth_weight_kg < 100)",
        severity=ExpectationSeverity.WARN,
        description="Birth weight should be between 10 and 100 kg (or null)"
    ),
    
    Expectation(
        name="reasonable_name_length",
        condition="name IS NULL OR LENGTH(name) <= 100",
        severity=ExpectationSeverity.WARN,
        description="Name should be 100 characters or less"
    ),
    
    Expectation(
        name="valid_color",
        condition="""color IS NULL OR color IN (
            'Black', 'Red', 'White', 'Brown', 'Grey', 'Yellow', 'Cream',
            'Brindle', 'Roan', 'Spotted', 'Mixed', 'Other'
        )""",
        severity=ExpectationSeverity.WARN,
        description="Color should be a recognized value"
    ),
    
    Expectation(
        name="valid_deactivation_reason",
        condition="""deactivation_reason IS NULL OR deactivation_reason IN (
            'sold', 'deceased', 'transferred', 'culled', 'exported', 'other'
        )""",
        severity=ExpectationSeverity.WARN,
        description="Deactivation reason should be a recognized value"
    ),
]


# ============================================================================
# EXPECTATION APPLICATION
# ============================================================================

def get_expectations_by_severity() -> Dict[str, List[Expectation]]:
    """Group expectations by severity level"""
    result = {
        ExpectationSeverity.DROP: [],
        ExpectationSeverity.WARN: []
    }
    
    for exp in SILVER_EXPECTATIONS:
        result[exp.severity].append(exp)
    
    return result


def apply_expectations(df: DataFrame) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
    """
    Apply all expectations to a DataFrame.
    
    Returns:
        - valid_df: Rows that passed all DROP expectations
        - failed_df: Rows that failed any DROP expectation (with failure reasons)
        - stats: Dictionary of failure counts per expectation
    
    Process:
    1. Evaluate all expectations
    2. Drop rows failing DROP expectations
    3. Warn on rows failing WARN expectations (but keep them)
    4. Return valid data and failure stats
    """
    
    if df.rdd.isEmpty():
        logger.info("Empty DataFrame, skipping expectations")
        return df, df.limit(0), {}
    
    logger.info(f"Applying {len(SILVER_EXPECTATIONS)} expectations to {df.count()} rows")
    
    # Add columns to track expectation failures
    result_df = df
    failed_expectations_col = []
    
    stats = {}
    
    expectations_by_severity = get_expectations_by_severity()
    
    # ========================================================================
    # Phase 1: Evaluate all expectations
    # ========================================================================
    
    for exp in SILVER_EXPECTATIONS:
        col_name = f"_passes_{exp.name}"
        
        # Use SQL expression to evaluate condition
        try:
            result_df.createOrReplaceTempView("_temp_expectation_check")
            result_df = result_df.sparkSession.sql(f"""
                SELECT *,
                    CASE WHEN {exp.condition} THEN TRUE ELSE FALSE END AS {col_name}
                FROM _temp_expectation_check
            """)
        except Exception as e:
            logger.error(f"Failed to evaluate expectation {exp.name}: {e}")
            # Default to passing if evaluation fails
            result_df = result_df.withColumn(col_name, lit(True))
        
        # Count failures
        failure_count = result_df.filter(col(col_name) == False).count()
        stats[exp.name] = failure_count
        
        if failure_count > 0:
            if exp.severity == ExpectationSeverity.DROP:
                logger.warning(f"❌ DROP: {failure_count} rows failed '{exp.name}': {exp.description}")
            else:
                logger.warning(f"⚠️  WARN: {failure_count} rows failed '{exp.name}': {exp.description}")
        
        # Build list of failed expectation names for each row
        failed_expectations_col.append(
            when(col(col_name) == False, lit(exp.name))
        )
    
    # ========================================================================
    # Phase 2: Create failure reason column
    # ========================================================================
    
    # Concatenate all failed expectation names
    result_df = result_df.withColumn(
        "_failed_expectations",
        concat_ws(", ", array(*failed_expectations_col))
    )
    
    # ========================================================================
    # Phase 3: Separate DROP failures from valid rows
    # ========================================================================
    
    # Build condition for DROP expectations
    drop_conditions = []
    for exp in expectations_by_severity[ExpectationSeverity.DROP]:
        col_name = f"_passes_{exp.name}"
        drop_conditions.append(col(col_name) == True)
    
    # Combine all DROP conditions with AND
    if drop_conditions:
        from functools import reduce
        from pyspark.sql.functions import expr
        passes_all_drop = reduce(lambda a, b: a & b, drop_conditions)
    else:
        passes_all_drop = lit(True)
    
    # Split into valid and failed
    valid_df = result_df.filter(passes_all_drop)
    failed_df = result_df.filter(~passes_all_drop)
    
    # ========================================================================
    # Phase 4: Log warnings for non-critical failures
    # ========================================================================
    
    for exp in expectations_by_severity[ExpectationSeverity.WARN]:
        col_name = f"_passes_{exp.name}"
        warn_failures = valid_df.filter(col(col_name) == False)
        warn_count = warn_failures.count()
        
        if warn_count > 0:
            logger.warning(f"⚠️  {warn_count} rows passed DROP but failed WARN expectation '{exp.name}'")
            # These rows are kept in valid_df but logged
    
    # ========================================================================
    # Clean up temporary columns from valid_df
    # ========================================================================
    
    # Remove _passes_* columns from valid output
    cols_to_keep = [c for c in valid_df.columns if not c.startswith("_passes_") and c != "_failed_expectations"]
    valid_df = valid_df.select(*cols_to_keep)
    
    # Keep failure tracking columns in failed_df for logging
    
    logger.info(f"Expectations applied: {valid_df.count()} valid, {failed_df.count()} failed DROP")
    
    return valid_df, failed_df, stats


# ============================================================================
# QUALITY LOG TABLE
# ============================================================================

def get_quality_log_schema():
    """Schema for silver_quality_log table"""
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
    
    return StructType([
        StructField("log_id", StringType(), False),
        StructField("run_timestamp", TimestampType(), False),
        StructField("expectation_name", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("failure_count", LongType(), False),
        StructField("total_rows", LongType(), False),
        StructField("failure_rate", StringType(), False),
        StructField("tenant_id", StringType(), True),
        StructField("cow_id", StringType(), True),
        StructField("sequence_number", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("failure_reason", StringType(), True),
        StructField("created_at", TimestampType(), False),
    ])


def write_quality_log(
    spark,
    failed_df: DataFrame,
    stats: Dict[str, int],
    total_rows: int,
    run_timestamp: datetime
):
    """
    Write expectation failures to silver_quality_log table.
    
    Args:
        spark: SparkSession
        failed_df: DataFrame of rows that failed DROP expectations
        stats: Dictionary of failure counts per expectation
        total_rows: Total number of rows evaluated
        run_timestamp: Timestamp of this quality check run
    """
    
    from pyspark.sql.functions import lit, current_timestamp
    import uuid
    
    log_records = []
    
    # ========================================================================
    # Write summary stats for each expectation
    # ========================================================================
    
    for exp in SILVER_EXPECTATIONS:
        failure_count = stats.get(exp.name, 0)
        
        if failure_count > 0:
            failure_rate = f"{(failure_count / total_rows * 100):.2f}%"
            
            summary_record = {
                "log_id": str(uuid.uuid4()),
                "run_timestamp": run_timestamp,
                "expectation_name": exp.name,
                "severity": exp.severity,
                "failure_count": failure_count,
                "total_rows": total_rows,
                "failure_rate": failure_rate,
                "tenant_id": None,
                "cow_id": None,
                "sequence_number": None,
                "event_type": None,
                "failure_reason": exp.description,
                "created_at": datetime.utcnow(),
            }
            
            log_records.append(summary_record)
    
    # ========================================================================
    # Write detailed failure records
    # ========================================================================
    
    if not failed_df.rdd.isEmpty():
        # Select relevant columns from failed rows
        failure_details = failed_df.select(
            "tenant_id",
            "cow_id",
            "__SEQUENCE_NUMBER",
            "__EVENT_TYPE",
            "_failed_expectations"
        ).collect()
        
        for row in failure_details:
            detail_record = {
                "log_id": str(uuid.uuid4()),
                "run_timestamp": run_timestamp,
                "expectation_name": "MULTIPLE" if ", " in row._failed_expectations else row._failed_expectations,
                "severity": ExpectationSeverity.DROP,
                "failure_count": 1,
                "total_rows": total_rows,
                "failure_rate": f"{(1 / total_rows * 100):.6f}%",
                "tenant_id": getattr(row, "tenant_id", None),
                "cow_id": getattr(row, "cow_id", None),
                "sequence_number": getattr(row, "__SEQUENCE_NUMBER", None),
                "event_type": getattr(row, "__EVENT_TYPE", None),
                "failure_reason": row._failed_expectations,
                "created_at": datetime.utcnow(),
            }
            
            log_records.append(detail_record)
    
    # ========================================================================
    # Write to Delta table
    # ========================================================================
    
    if log_records:
        log_df = spark.createDataFrame(log_records, schema=get_quality_log_schema())
        
        try:
            log_df.write \
                .format("delta") \
                .mode("append") \
                .save("s3a://silver/quality_log")
            
            logger.info(f"Wrote {len(log_records)} quality log records to silver_quality_log")
        except Exception as e:
            logger.error(f"Failed to write quality log: {e}")


def create_quality_log_table(spark):
    """Initialize silver_quality_log Delta table"""
    from delta import DeltaTable
    
    path = "s3a://silver/quality_log"
    
    if DeltaTable.isDeltaTable(spark, path):
        logger.info("silver_quality_log table already exists")
        return
    
    logger.info("Creating silver_quality_log table...")
    
    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], schema=get_quality_log_schema())
    
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    
    logger.info(f"✓ Created silver_quality_log table at {path}")


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def print_expectations():
    """Print all expectations for documentation"""
    
    expectations_by_severity = get_expectations_by_severity()
    
    print("=" * 80)
    print("SILVER LAYER DATA QUALITY EXPECTATIONS")
    print("=" * 80)
    print()
    
    for severity in [ExpectationSeverity.DROP, ExpectationSeverity.WARN]:
        expectations = expectations_by_severity[severity]
        
        print(f"\n{severity} Expectations ({len(expectations)}):")
        print("-" * 80)
        
        for i, exp in enumerate(expectations, 1):
            print(f"\n{i}. {exp.name}")
            print(f"   Condition: {exp.condition}")
            print(f"   Description: {exp.description}")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    # Print expectations for documentation
    print_expectations()
    
    print("\n\nExpectation Summary:")
    print(f"  Total: {len(SILVER_EXPECTATIONS)}")
    print(f"  DROP (Critical): {len([e for e in SILVER_EXPECTATIONS if e.severity == ExpectationSeverity.DROP])}")
    print(f"  WARN (Non-Critical): {len([e for e in SILVER_EXPECTATIONS if e.severity == ExpectationSeverity.WARN])}")
