"""
Gold Layer - Common Utilities and Configuration

Shared functions for Gold layer analytics:
- Spark session creation with MinIO
- Data lineage metadata
- Table creation helpers
- Reproducibility guarantees
"""

import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp
from delta import configure_spark_with_delta_pip
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

SILVER_HISTORY_PATH = "s3a://silver/cows_history"
SILVER_CURRENT_PATH = "s3a://silver/cows_current"
GOLD_BASE_PATH = "s3a://gold"

# SQL Server Configuration (for projections only)
SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
SQL_DATABASE = os.getenv("SQL_DATABASE", "cattlesaas")
SQL_USER = os.getenv("SQL_USER", "sa")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "YourStrong!Passw0rd")
SQL_JDBC_URL = f"jdbc:sqlserver://{SQL_SERVER}:1433;databaseName={SQL_DATABASE};encrypt=false"

# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session(app_name: str = "GoldLayer") -> SparkSession:
    """Create Spark session with MinIO configuration for Gold layer"""
    # Get the downloaded JAR paths
    import os
    ivy_jars = os.path.expanduser("~/.ivy2/jars")
    hadoop_aws_jar = f"{ivy_jars}/org.apache.hadoop_hadoop-aws-3.3.1.jar"
    aws_sdk_jar = f"{ivy_jars}/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar"
    delta_spark_jar = f"{ivy_jars}/io.delta_delta-spark_2.12-3.2.1.jar"
    delta_storage_jar = f"{ivy_jars}/io.delta_delta-storage-3.2.1.jar"
    
    # SQL Server JDBC driver for projections
    mssql_jdbc_jar = f"{ivy_jars}/mssql-jdbc-12.8.1.jre11.jar"
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Add JARs for S3A, Delta, and SQL Server support
        .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{delta_spark_jar},{delta_storage_jar},{mssql_jdbc_jar}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


# ============================================================================
# DATA LINEAGE METADATA
# ============================================================================

def add_lineage_metadata(
    df: DataFrame,
    source_tables: list,
    aggregation_type: str,
    gold_table_name: str,
    computation_logic: str = None
) -> DataFrame:
    """
    Add data lineage metadata to Gold table.
    
    This ensures full traceability from Gold → Silver → Bronze → SQL.
    
    Args:
        df: DataFrame to add metadata to
        source_tables: List of source table paths (e.g., ['silver_cows_history'])
        aggregation_type: Type of aggregation (e.g., 'daily_snapshot', 'herd_composition')
        gold_table_name: Name of the Gold table being created
        computation_logic: Optional description of computation
    
    Returns:
        DataFrame with lineage metadata columns
    """
    
    run_timestamp = datetime.now(timezone.utc)
    
    df_with_lineage = df \
        .withColumn("_gold_table", lit(gold_table_name)) \
        .withColumn("_source_tables", lit(",".join(source_tables))) \
        .withColumn("_aggregation_type", lit(aggregation_type)) \
        .withColumn("_computation_timestamp", lit(run_timestamp)) \
        .withColumn("_computation_logic", lit(computation_logic or "See documentation"))
    
    return df_with_lineage


# ============================================================================
# TABLE OPERATIONS
# ============================================================================

def write_gold_table(
    df: DataFrame,
    table_path: str,
    mode: str = "overwrite",
    partition_by: list = None
) -> None:
    """
    Write DataFrame to Gold Delta table with lineage.
    
    Args:
        df: DataFrame to write
        table_path: Full S3 path (e.g., 's3a://gold/herd_composition')
        mode: Write mode ('overwrite' or 'append')
        partition_by: Optional list of columns to partition by
    """
    
    logger.info(f"Writing to Gold table: {table_path}")
    logger.info(f"  Rows: {df.count()}")
    logger.info(f"  Mode: {mode}")
    
    writer = df.write.format("delta").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
        logger.info(f"  Partitioned by: {partition_by}")
    
    writer.save(table_path)
    
    logger.info(f"✓ Gold table written: {table_path}")


def truncate_gold_table(spark: SparkSession, table_path: str) -> None:
    """
    Truncate (delete all data from) a Gold table.
    
    Used by rebuild_all.py to ensure clean slate.
    """
    
    from delta import DeltaTable
    
    if DeltaTable.isDeltaTable(spark, table_path):
        logger.info(f"Truncating Gold table: {table_path}")
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.delete()
        logger.info(f"✓ Truncated: {table_path}")
    else:
        logger.info(f"Table does not exist (will be created): {table_path}")


def verify_reproducibility(
    spark: SparkSession,
    table_path: str,
    expected_row_count: int = None
) -> Dict[str, Any]:
    """
    Verify Gold table was created correctly.
    
    Returns metrics for reproducibility validation.
    """
    
    df = spark.read.format("delta").load(table_path)
    
    metrics = {
        "table_path": table_path,
        "row_count": df.count(),
        "partition_count": df.rdd.getNumPartitions(),
        "columns": df.columns,
        "schema": str(df.schema),
    }
    
    if expected_row_count is not None:
        metrics["expected_count"] = expected_row_count
        metrics["count_matches"] = (metrics["row_count"] == expected_row_count)
    
    return metrics


# ============================================================================
# SILVER TABLE ACCESS
# ============================================================================

def read_silver_cows_history(spark: SparkSession) -> DataFrame:
    """Read Silver cows history table (SCD Type 2)"""
    return spark.read.format("delta").load(SILVER_HISTORY_PATH)


def read_silver_cows_current(spark: SparkSession) -> DataFrame:
    """Read Silver current cows (view of __CURRENT = true)"""
    # Register history table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver_cows_history
        USING DELTA
        LOCATION '{SILVER_HISTORY_PATH}'
    """)
    
    # Create view
    spark.sql("""
        CREATE OR REPLACE VIEW silver_cows_current AS
        SELECT * FROM silver_cows_history WHERE __CURRENT = true
    """)
    
    return spark.sql("SELECT * FROM silver_cows_current")


# ============================================================================
# REPRODUCIBILITY HELPERS
# ============================================================================

def get_silver_source_info(spark: SparkSession) -> Dict[str, Any]:
    """
    Get information about Silver source data.
    
    Used to prove Gold is reproducible from same Silver state.
    """
    
    history_df = read_silver_cows_history(spark)
    
    info = {
        "silver_history_rows": history_df.count(),
        "silver_history_path": SILVER_HISTORY_PATH,
        "tenants": history_df.select("tenant_id").distinct().count(),
        "cows": history_df.select("cow_id").distinct().count(),
        "earliest_event": history_df.agg({"__START_AT": "min"}).collect()[0][0],
        "latest_event": history_df.agg({"__START_AT": "max"}).collect()[0][0],
    }
    
    return info


def validate_gold_against_silver(
    spark: SparkSession,
    gold_summary: Dict[str, Any],
    silver_summary: Dict[str, Any]
) -> bool:
    """
    Validate that Gold aggregations are consistent with Silver source.
    
    This is a key reproducibility check.
    """
    
    # Example checks (customize per Gold table)
    checks = []
    
    # Check 1: Total cows in Gold should match unique cows in Silver
    if "total_cows" in gold_summary and "cows" in silver_summary:
        match = gold_summary["total_cows"] == silver_summary["cows"]
        checks.append(("total_cows_match", match))
    
    # Check 2: Tenant count should match
    if "total_tenants" in gold_summary and "tenants" in silver_summary:
        match = gold_summary["total_tenants"] == silver_summary["tenants"]
        checks.append(("tenant_count_match", match))
    
    all_passed = all(check[1] for check in checks)
    
    logger.info("Gold vs Silver validation:")
    for check_name, passed in checks:
        status = "✓" if passed else "✗"
        logger.info(f"  {status} {check_name}")
    
    return all_passed


# ============================================================================
# GOLD TABLE REGISTRY
# ============================================================================

GOLD_TABLES = {
    "herd_composition": {
        "path": f"{GOLD_BASE_PATH}/herd_composition",
        "description": "Daily herd composition by breed, status, and tenant",
        "source": ["silver_cows_history"],
        "partitions": ["snapshot_date"],
        "script": "gold_herd_composition.py"
    },
    "daily_snapshots": {
        "path": f"{GOLD_BASE_PATH}/daily_snapshots",
        "description": "Daily cow count snapshots per tenant",
        "source": ["silver_cows_history"],
        "partitions": ["snapshot_date"],
        "script": "gold_daily_snapshots.py"
    },
    "cow_lifecycle": {
        "path": f"{GOLD_BASE_PATH}/cow_lifecycle",
        "description": "Cow lifecycle analytics (creation to deactivation)",
        "source": ["silver_cows_history"],
        "partitions": ["tenant_id"],
        "script": "gold_cow_lifecycle.py"
    },
    # Future Gold tables (when Silver is extended)
    "daily_sales": {
        "path": f"{GOLD_BASE_PATH}/daily_sales",
        "description": "Daily sales aggregations (requires silver_sales)",
        "source": ["silver_sales"],
        "partitions": ["sale_date"],
        "script": "gold_daily_sales.py",
        "status": "planned"
    },
    "weight_trends": {
        "path": f"{GOLD_BASE_PATH}/weight_trends",
        "description": "30-day weight trends (requires silver_weights)",
        "source": ["silver_weights"],
        "partitions": ["tenant_id"],
        "script": "gold_cow_weight_trends.py",
        "status": "planned"
    },
}


def list_gold_tables(include_planned: bool = False) -> Dict[str, Dict]:
    """List all Gold tables (active and optionally planned)"""
    
    if include_planned:
        return GOLD_TABLES
    else:
        return {
            name: info
            for name, info in GOLD_TABLES.items()
            if info.get("status") != "planned"
        }


def get_gold_table_info(table_name: str) -> Optional[Dict]:
    """Get information about a specific Gold table"""
    return GOLD_TABLES.get(table_name)


# ============================================================================
# SQL SERVER PROJECTION (Disposable Cache)
# ============================================================================

def project_gold_to_sql(
    df: DataFrame,
    sql_table_name: str,
    mode: str = "overwrite"
) -> None:
    """
    Project Gold DataFrame to SQL Server analytics schema.
    
    IMPORTANT: This is a PROJECTION step, not part of Gold computation.
    - SQL tables are disposable
    - Can be rebuilt from Gold Delta tables anytime
    - SQL is NOT the source of truth
    - Gold Delta Lake remains canonical analytical truth
    
    Args:
        df: Gold DataFrame to project
        sql_table_name: Target table in analytics schema (e.g., 'herd_composition')
        mode: Write mode ('overwrite' or 'append')
    
    Example:
        gold_df = compute_herd_composition(spark, date)
        gold_df.write.format('delta').save('s3a://gold/herd_composition')  # Canonical
        project_gold_to_sql(gold_df, 'herd_composition')  # Projection
    """
    
    logger.info(f"Projecting Gold data to SQL: analytics.{sql_table_name}")
    logger.info(f"  Rows: {df.count()}")
    logger.info(f"  Mode: {mode}")
    logger.info("  NOTE: This is a disposable projection, Gold Delta is canonical")
    
    try:
        df.write \
            .format("jdbc") \
            .option("url", SQL_JDBC_URL) \
            .option("dbtable", f"analytics.{sql_table_name}") \
            .option("user", SQL_USER) \
            .option("password", SQL_PASSWORD) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode(mode) \
            .save()
        
        logger.info(f"✓ SQL projection complete: analytics.{sql_table_name}")
        
    except Exception as e:
        logger.error(f"⚠️  SQL projection failed: {str(e)}")
        logger.error("   Gold data is still valid in Delta Lake")
        raise
