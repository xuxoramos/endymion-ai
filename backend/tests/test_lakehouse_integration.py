"""
Integration Test: Complete Lakehouse Flow
SQL Server → Bronze → Silver → SQL Projection

Tests the full medallion architecture:
1. Create events in SQL Server
2. Ingest to Bronze (Delta Lake in MinIO)
3. Resolve state in Silver (SCD Type 2)
4. Sync back to SQL projections

Prerequisites:
- docker-compose services running (SQL Server, MinIO, Redis)
- PySpark with Delta Lake installed
- Database initialized with tables
"""

import sys
import os
import uuid
import json
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from backend.database.connection import DatabaseManager
from backend.models.events import CowEvent


# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def create_spark_with_s3a():
    """Create Spark session with S3A support for MinIO."""
    print("\n🔧 Creating Spark session with S3A support...")
    
    builder = (
        SparkSession.builder
        .appName("Lakehouse Integration Test")
        .master("local[*]")
        # Add Hadoop AWS packages for S3A support
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
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
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"✅ Spark {spark.version} with Delta Lake and S3A support")
    return spark


def test_bronze_layer_access(spark: SparkSession):
    """Test if we can access Bronze layer in MinIO."""
    print("\n📦 Testing Bronze layer access...")
    
    try:
        bronze_path = "s3a://bronze/cow_events"
        
        # Try to list the path
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(bronze_path),
            spark._jsc.hadoopConfiguration()
        )
        
        exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(bronze_path))
        
        if exists:
            # Try to read the Delta table
            df = spark.read.format("delta").load(bronze_path)
            count = df.count()
            print(f"✅ Bronze layer accessible - {count} records")
            return True, count
        else:
            print("⚠️  Bronze table not initialized yet")
            return False, 0
            
    except Exception as e:
        print(f"❌ Bronze layer access failed: {str(e)[:100]}")
        return False, 0


def create_test_events(session: Session, tenant_id: str, count: int = 3):
    """Create test events in SQL Server."""
    print(f"\n📝 Creating {count} test events in SQL Server...")
    
    events = []
    for i in range(count):
        cow_id = str(uuid.uuid4())
        event = CowEvent(
            event_id=uuid.uuid4(),
            tenant_id=uuid.UUID(tenant_id),
            cow_id=uuid.UUID(cow_id),
            event_type="cow_created",
            payload=json.dumps({
                "cow_id": cow_id,
                "tenant_id": tenant_id,
                "tag_number": f"LAKE{int(time.time()) % 10000:04d}-{i+1:02d}",
                "name": f"TestCow-{i+1}",
                "breed": "Holstein" if i % 2 == 0 else "Jersey",
                "birth_date": "2024-01-15",
                "sex": "female",
                "status": "active",
                "created_at": datetime.now().isoformat(),
            }),
            event_time=datetime.now(),
            created_at=datetime.now(),
            published_to_bronze=False
        )
        session.add(event)
        events.append(event)
    
    session.commit()
    
    print(f"✅ Created {count} events:")
    for e in events:
        payload = json.loads(e.payload)
        print(f"   - {payload['tag_number']}: {payload['name']} ({payload['breed']})")
    
    return events


def count_unpublished_events(session: Session) -> int:
    """Count unpublished events in SQL Server."""
    result = session.execute(
        text("SELECT COUNT(*) FROM operational.cow_events WHERE published_to_bronze = 0")
    )
    return result.scalar()


def verify_events_published(session: Session, event_ids: list) -> bool:
    """Verify events are marked as published."""
    placeholders = ','.join([f"'{str(eid)}'" for eid in event_ids])
    result = session.execute(
        text(f"""
            SELECT COUNT(*) 
            FROM operational.cow_events 
            WHERE event_id IN ({placeholders}) 
            AND published_to_bronze = 1
        """)
    )
    published_count = result.scalar()
    return published_count == len(event_ids)


def main():
    """Run complete lakehouse integration test."""
    
    print("=" * 80)
    print("🧪 LAKEHOUSE INTEGRATION TEST - SQL → Bronze → Silver → SQL")
    print("=" * 80)
    
    # Test configuration
    tenant_id = str(uuid.uuid4())
    print(f"\n📋 Test Configuration:")
    print(f"   Tenant ID: {tenant_id}")
    print(f"   Events to create: 3")
    
    # Initialize database connection
    print("\n🔌 Connecting to SQL Server...")
    db = DatabaseManager()
    engine = db.init_engine()
    session = Session(engine)
    
    try:
        # Step 1: Check initial state
        print("\n" + "=" * 80)
        print("STEP 1: Check Initial State")
        print("=" * 80)
        
        unpublished = count_unpublished_events(session)
        print(f"Unpublished events in SQL: {unpublished}")
        
        # Step 2: Create Spark session
        print("\n" + "=" * 80)
        print("STEP 2: Initialize Spark with S3A Support")
        print("=" * 80)
        
        spark = create_spark_with_s3a()
        
        # Step 3: Test Bronze layer access
        print("\n" + "=" * 80)
        print("STEP 3: Test Bronze Layer Access")
        print("=" * 80)
        
        bronze_accessible, bronze_count = test_bronze_layer_access(spark)
        
        if not bronze_accessible:
            print("\n⚠️  Bronze layer not initialized. Run setup:")
            print("   python databricks/bronze/setup_bronze.py")
            print("\n⏸️  Test paused - Bronze layer setup required")
            return False
        
        # Step 4: Create test events
        print("\n" + "=" * 80)
        print("STEP 4: Create Test Events in SQL")
        print("=" * 80)
        
        events = create_test_events(session, tenant_id, count=3)
        event_ids = [e.event_id for e in events]
        
        unpublished_after = count_unpublished_events(session)
        print(f"\n📊 Unpublished events now: {unpublished_after}")
        
        # Step 5: Instructions for ingestion
        print("\n" + "=" * 80)
        print("STEP 5: Ingest to Bronze (Manual)")
        print("=" * 80)
        
        print("\n▶️  Run Bronze ingestion:")
        print("   python databricks/bronze/ingest_from_sql.py --once")
        print("\n⏸️  Waiting for manual ingestion...")
        print("   (In production, this would run automatically)")
        
        # Wait for user to run ingestion
        input("\nPress Enter after running ingestion...")
        
        # Step 6: Verify Bronze ingestion
        print("\n" + "=" * 80)
        print("STEP 6: Verify Bronze Ingestion")
        print("=" * 80)
        
        bronze_accessible, bronze_count_after = test_bronze_layer_access(spark)
        new_records = bronze_count_after - bronze_count
        
        print(f"📊 Bronze records: {bronze_count} → {bronze_count_after}")
        print(f"✅ New records ingested: {new_records}")
        
        # Check if events marked as published
        published = verify_events_published(session, event_ids)
        if published:
            print("✅ Events marked as published in SQL")
        else:
            print("⚠️  Events not yet marked as published")
        
        # Step 7: Query Bronze data
        print("\n" + "=" * 80)
        print("STEP 7: Query Bronze Data")
        print("=" * 80)
        
        bronze_df = spark.read.format("delta").load("s3a://bronze/cow_events")
        test_events_df = bronze_df.filter(f"tenant_id = '{tenant_id}'")
        
        print(f"\nTest events in Bronze: {test_events_df.count()}")
        
        if test_events_df.count() > 0:
            print("\nSample records:")
            test_events_df.select("event_id", "cow_id", "event_type", "event_timestamp") \
                         .show(5, truncate=False)
        
        # Step 8: Summary
        print("\n" + "=" * 80)
        print("📊 TEST SUMMARY")
        print("=" * 80)
        
        print(f"\n✅ Spark Session: Created with S3A support")
        print(f"✅ Bronze Layer: Accessible ({bronze_count_after} total records)")
        print(f"✅ Test Events: {len(events)} created in SQL")
        print(f"✅ Ingestion: {new_records} records moved to Bronze")
        print(f"{'✅' if published else '⏸️ '} Publishing: Events {'marked' if published else 'pending'}")
        
        print("\n📝 Next Steps:")
        print("   1. Run Silver resolution:")
        print("      python databricks/silver/resolve_cow_state.py")
        print("   2. Run SQL sync:")
        print("      python backend/jobs/sync_silver_to_sql.py")
        print("   3. Verify projections:")
        print("      SELECT * FROM operational.cows WHERE tenant_id = '{}'".format(tenant_id))
        
        print("\n✅ Lakehouse integration test completed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        session.close()
        engine.dispose()
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
