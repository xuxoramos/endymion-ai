"""
Silver Layer Setup - Initialize SCD Type 2 Tables

Creates the silver_cows_history Delta Lake table with proper schema and partitioning.
This prevents ProtocolChangedException race conditions by pre-creating the table
before any concurrent writes from analytics sync or manual scripts.

Usage:
    python databricks/silver/setup_silver.py
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from databricks.silver.resolve_cow_state import (
    get_silver_history_schema,
    create_spark_session
)

# MinIO configuration
SILVER_PATH = "s3a://silver/cows_history"


def setup_silver_layer():
    """Initialize Silver layer Delta tables."""
    
    print("=" * 60)
    print("Silver Layer Setup - SCD Type 2 Tables")
    print("=" * 60)
    print("\nInitializing silver_cows_history table...")
    
    try:
        # Create Spark session
        print("\nCreating Spark session...")
        spark = create_spark_session()
        print(f"✓ Spark session created (version {spark.version})")
        
        # Get schema
        schema = get_silver_history_schema()
        
        # Create empty DataFrame with schema
        empty_df = spark.createDataFrame([], schema)
        
        # Write table structure (mode='ignore' prevents errors if exists)
        print(f"\nCreating Silver table at: {SILVER_PATH}")
        empty_df.write.format("delta") \
            .mode("ignore") \
            .partitionBy("partition_tenant_id") \
            .option("delta.enableChangeDataFeed", "true") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .save(SILVER_PATH)
        
        print("✓ Table structure created")
        
        # Verify table exists
        print("\nVerifying table setup...")
        delta_table = spark.read.format("delta").load(SILVER_PATH)
        print(f"✓ Schema fields: {len(delta_table.schema.fields)}")
        print(f"✓ Record count: {delta_table.count()}")
        
        print("\nTable schema:")
        delta_table.printSchema()
        
        print("\nTable details:")
        detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{SILVER_PATH}`")
        detail_df.select("format", "partitionColumns", "numFiles", "sizeInBytes").show(truncate=False)
        
        print("\n" + "=" * 60)
        print("✓ Silver layer setup completed successfully!")
        print("=" * 60)
        print(f"\nTable location: {SILVER_PATH}")
        print("\nNext steps:")
        print("1. Run ingestion: python databricks/bronze/ingest_from_sql.py")
        print("2. Run resolution: python databricks/silver/resolve_cow_state.py")
        print("3. Verify data: Query the Silver table")
        
        spark.stop()
        return 0
        
    except Exception as e:
        print(f"\n✗ Setup failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(setup_silver_layer())
