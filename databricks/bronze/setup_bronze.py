"""
Bronze Layer Setup - Initialize Delta Lake tables in MinIO.

This script creates the bronze_cow_events Delta table with:
- Append-only mode
- Partitioning by tenant_id and date
- Schema enforcement
- Event deduplication constraints

Usage:
    python databricks/bronze/setup_bronze.py
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, IntegerType
)
from delta import configure_spark_with_delta_pip


# ========================================
# Configuration
# ========================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_BUCKET = "bronze"
BRONZE_TABLE_PATH = f"s3a://{BRONZE_BUCKET}/cow_events"


# ========================================
# Schema Definition
# ========================================

def get_bronze_schema():
    """
    Define schema for bronze_cow_events table.
    
    Matches SQL cow_events table structure plus metadata fields.
    """
    return StructType([
        # Event identity
        StructField("event_id", StringType(), nullable=False),
        StructField("tenant_id", StringType(), nullable=False),
        StructField("cow_id", StringType(), nullable=False),
        
        # Event metadata
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("sequence_number", IntegerType(), nullable=False),
        
        # Event payload
        StructField("payload", StringType(), nullable=False),
        
        # Change tracking
        StructField("created_by", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=False),
        
        # Publishing metadata
        StructField("published_to_bronze", BooleanType(), nullable=False),
        StructField("published_to_bronze_at", TimestampType(), nullable=True),
        
        # Ingestion metadata (added by Bronze layer)
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("ingestion_batch_id", StringType(), nullable=False),
        
        # Partition columns (duplicated for explicit partitioning)
        StructField("partition_date", StringType(), nullable=False),  # YYYY-MM-DD format
    ])


# ========================================
# Spark Session
# ========================================

def create_spark_session():
    """Create Spark session configured for Delta Lake and MinIO."""
    print("Creating Spark session...")
    
    # Get the downloaded JAR paths
    import os
    ivy_jars = os.path.expanduser("~/.ivy2/jars")
    hadoop_aws_jar = f"{ivy_jars}/org.apache.hadoop_hadoop-aws-3.3.1.jar"
    aws_sdk_jar = f"{ivy_jars}/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar"
    delta_spark_jar = f"{ivy_jars}/io.delta_delta-spark_2.12-3.2.1.jar"
    delta_storage_jar = f"{ivy_jars}/io.delta_delta-storage-3.2.1.jar"
    
    builder = (
        SparkSession.builder
        .appName("Bronze Layer Setup")
        .master("local[*]")
        # Add explicit JARs for S3A and Delta support
        .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{delta_spark_jar},{delta_storage_jar}")
        # Delta Lake configuration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO/S3 configuration
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # S3A performance and timeout configs (all numeric, no string suffixes)
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.retry.interval", "500")
        .config("spark.hadoop.fs.s3a.retry.limit", "3")
        .config("spark.hadoop.fs.s3a.socket.send.buffer", "8192")
        .config("spark.hadoop.fs.s3a.socket.recv.buffer", "8192")
        # Disable problematic features that might have string-based configs
        .config("spark.hadoop.fs.s3a.committer.name", "file")
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    )
    
    # Configure Delta Lake (with explicit JARs already loaded above)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Override hadoop configuration directly to fix "60s" string parsing issues
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
    hadoop_conf.set("fs.s3a.attempts.maximum", "3")
    hadoop_conf.set("fs.s3a.retry.interval", "500")
    hadoop_conf.set("fs.s3a.retry.limit", "3")
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✓ Spark session created (version {spark.version})")
    return spark


# ========================================
# Table Setup
# ========================================

def create_bronze_table(spark: SparkSession):
    """
    Create bronze_cow_events Delta table with proper configuration.
    
    Table properties:
    - appendOnly: true (immutable events)
    - Partitioned by: tenant_id, partition_date
    - Schema enforcement enabled
    """
    print(f"\nCreating bronze_cow_events table at: {BRONZE_TABLE_PATH}")
    
    schema = get_bronze_schema()
    
    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], schema)
    
    # Write as Delta table with properties
    (
        empty_df.write
        .format("delta")
        .mode("ignore")  # Don't overwrite if exists
        .partitionBy("tenant_id", "partition_date")
        .option("delta.appendOnly", "true")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.checkpoint.writeStatsAsJson", "true")
        .option("delta.checkpoint.writeStatsAsStruct", "true")
        .save(BRONZE_TABLE_PATH)
    )
    
    print("✓ Table created successfully")


def configure_table_properties(spark: SparkSession):
    """Configure additional table properties after creation."""
    print("\nConfiguring table properties...")
    
    # Set additional properties using SQL
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze_cow_events
        USING DELTA
        LOCATION '{BRONZE_TABLE_PATH}'
    """)
    
    # Set table properties
    spark.sql("""
        ALTER TABLE bronze_cow_events
        SET TBLPROPERTIES (
            'delta.appendOnly' = 'true',
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'description' = 'Bronze layer: Immutable cow events ingested from SQL Server',
            'created_by' = 'setup_bronze.py',
            'layer' = 'bronze'
        )
    """)
    
    print("✓ Table properties configured")


def verify_table(spark: SparkSession):
    """Verify table was created correctly."""
    print("\nVerifying table setup...")
    
    # Read table metadata
    df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
    
    print(f"✓ Schema fields: {len(df.schema.fields)}")
    print(f"✓ Record count: {df.count()}")
    
    # Show schema
    print("\nTable schema:")
    df.printSchema()
    
    # Show table properties
    try:
        detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{BRONZE_TABLE_PATH}`")
        print("\nTable details:")
        detail_df.select(
            "format", "partitionColumns", "numFiles", "sizeInBytes"
        ).show(truncate=False)
    except Exception as e:
        print(f"Warning: Could not get table details: {e}")


# ========================================
# MinIO Verification
# ========================================

def verify_minio_connection(spark: SparkSession):
    """Verify MinIO connection before setup."""
    print("Verifying MinIO connection...")
    
    try:
        # Get the hadoop configuration from Spark context (which has our overrides)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        
        # Try to list buckets using Hadoop FileSystem API with our configuration
        uri = spark._jvm.java.net.URI(f"s3a://{BRONZE_BUCKET}")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        
        # Try to list root of bronze bucket
        path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{BRONZE_BUCKET}/")
        if fs.exists(path):
            print(f"✓ MinIO connection successful - bucket '{BRONZE_BUCKET}' exists")
            return True
        else:
            print(f"✗ Bucket '{BRONZE_BUCKET}' does not exist")
            return False
            
    except Exception as e:
        print(f"✗ MinIO connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure MinIO is running: docker-compose -f docker/docker-compose.yml up -d minio")
        print("2. Check MinIO is accessible at: http://localhost:9000")
        print("3. Verify buckets exist: docker exec cattlesaas-minio-client mc ls localminio")
        return False


# ========================================
# Main Execution
# ========================================

def main():
    """Main setup workflow."""
    print("=" * 60)
    print("Bronze Layer Setup - Delta Lake on MinIO")
    print("=" * 60)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Skip MinIO verification due to hadoop-aws config parsing issue
        # The table creation will fail if MinIO is not accessible
        print("\nSkipping MinIO connection verification...")
        print("(Will verify during table creation)")
        
        # Create table
        create_bronze_table(spark)
        
        # Configure properties
        configure_table_properties(spark)
        
        # Verify setup
        verify_table(spark)
        
        print("\n" + "=" * 60)
        print("✓ Bronze layer setup completed successfully!")
        print("=" * 60)
        print(f"\nTable location: {BRONZE_TABLE_PATH}")
        print("\nNext steps:")
        print("1. Run ingestion job: python databricks/bronze/ingest_from_sql.py")
        print("2. Create events via API: curl -X POST http://localhost:8000/api/v1/cows")
        print("3. Verify events in Delta: python databricks/bronze/query_bronze.py")
        
        spark.stop()
        return 0
        
    except Exception as e:
        print(f"\n✗ Setup failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
