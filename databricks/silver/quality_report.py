"""
Silver Layer Quality Report

Generates data quality reports from silver_quality_log table:
- Expectation failure rates
- Identifies problematic events in Bronze
- Shows which tenants/cows have most quality issues
- Provides recommendations for data cleanup

Usage:
    # Show overall quality summary
    python databricks/silver/quality_report.py
    
    # Show detailed failures for specific expectation
    python databricks/silver/quality_report.py --expectation valid_breed
    
    # Show quality issues for specific tenant
    python databricks/silver/quality_report.py --tenant <tenant_id>
    
    # Export report to CSV
    python databricks/silver/quality_report.py --export quality_report.csv
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max,
    desc, round as spark_round, concat_ws, collect_list
)
from delta import configure_spark_with_delta_pip


# ============================================================================
# SPARK CONFIGURATION
# ============================================================================

def create_spark_session():
    """Create Spark session with MinIO configuration"""
    
    builder = (
        SparkSession.builder
        .appName("SilverQualityReport")
        .master("local[*]")
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
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark


# ============================================================================
# REPORT FUNCTIONS
# ============================================================================

def show_overall_summary(spark: SparkSession):
    """Show overall data quality summary"""
    
    print("\n" + "=" * 80)
    print("SILVER LAYER DATA QUALITY SUMMARY")
    print("=" * 80)
    
    try:
        log_df = spark.read.format("delta").load("s3a://silver/quality_log")
    except Exception as e:
        print(f"\n⚠️  Quality log table not found or empty")
        print(f"   Error: {e}")
        return
    
    if log_df.rdd.isEmpty():
        print("\n✓ No quality issues recorded - all data passed validation!")
        return
    
    # Get summary records (where tenant_id IS NULL)
    summary_df = log_df.filter(col("tenant_id").isNull())
    
    if summary_df.rdd.isEmpty():
        print("\n⚠️  No summary records found in quality log")
        return
    
    # Get most recent run
    latest_run = summary_df.agg(spark_max("run_timestamp")).collect()[0][0]
    print(f"\nLatest Quality Check: {latest_run}")
    
    latest_df = summary_df.filter(col("run_timestamp") == latest_run)
    
    # Calculate overall pass rate
    total_rows = latest_df.select("total_rows").first()[0]
    total_failures = latest_df.agg(spark_sum("failure_count")).collect()[0][0]
    
    if total_rows > 0:
        pass_rate = ((total_rows - total_failures) / total_rows) * 100
        print(f"\n📊 Overall Quality Metrics:")
        print(f"   Total Records Evaluated: {total_rows:,}")
        print(f"   Total Expectation Failures: {total_failures:,}")
        print(f"   Pass Rate: {pass_rate:.2f}%")
    
    # Show failures by expectation
    print(f"\n📋 Failures by Expectation:")
    print("-" * 80)
    
    failures_df = latest_df.filter(col("failure_count") > 0) \
        .orderBy(desc("failure_count")) \
        .select(
            "expectation_name",
            "severity",
            "failure_count",
            "failure_rate",
            "failure_reason"
        )
    
    if failures_df.rdd.isEmpty():
        print("   ✓ No failures - all expectations passed!")
    else:
        failures_df.show(truncate=False)
    
    # Show failures by severity
    print(f"\n🔍 Failures by Severity:")
    print("-" * 80)
    
    severity_df = latest_df.groupBy("severity") \
        .agg(
            spark_sum("failure_count").alias("total_failures"),
            count("expectation_name").alias("failed_expectations")
        ) \
        .orderBy("severity")
    
    severity_df.show(truncate=False)
    
    # Show detailed failure records (where tenant_id IS NOT NULL)
    detail_df = log_df.filter(
        (col("tenant_id").isNotNull()) &
        (col("run_timestamp") == latest_run)
    )
    
    detail_count = detail_df.count()
    
    if detail_count > 0:
        print(f"\n⚠️  Detailed Failure Records: {detail_count}")
        print("-" * 80)
        
        # Show most problematic tenants
        print("\nMost Problematic Tenants:")
        tenant_issues = detail_df.groupBy("tenant_id") \
            .agg(count("*").alias("failure_count")) \
            .orderBy(desc("failure_count")) \
            .limit(10)
        
        tenant_issues.show(truncate=False)
        
        # Show most problematic cows
        print("\nMost Problematic Cows:")
        cow_issues = detail_df.groupBy("tenant_id", "cow_id") \
            .agg(
                count("*").alias("failure_count"),
                collect_list("failure_reason").alias("reasons")
            ) \
            .orderBy(desc("failure_count")) \
            .limit(10)
        
        cow_issues.select("tenant_id", "cow_id", "failure_count").show(truncate=False)


def show_expectation_details(spark: SparkSession, expectation_name: str):
    """Show detailed failures for a specific expectation"""
    
    print("\n" + "=" * 80)
    print(f"EXPECTATION DETAILS: {expectation_name}")
    print("=" * 80)
    
    try:
        log_df = spark.read.format("delta").load("s3a://silver/quality_log")
    except Exception as e:
        print(f"\n⚠️  Quality log table not found")
        return
    
    # Filter to this expectation
    exp_df = log_df.filter(col("expectation_name") == expectation_name)
    
    if exp_df.rdd.isEmpty():
        print(f"\n✓ No failures recorded for expectation '{expectation_name}'")
        return
    
    # Show summary over time
    print("\n📈 Failure Trend (Last 10 Runs):")
    print("-" * 80)
    
    trend_df = exp_df.filter(col("tenant_id").isNull()) \
        .orderBy(desc("run_timestamp")) \
        .limit(10) \
        .select(
            "run_timestamp",
            "failure_count",
            "total_rows",
            "failure_rate"
        )
    
    trend_df.show(truncate=False)
    
    # Show recent failures
    latest_run = exp_df.agg(spark_max("run_timestamp")).collect()[0][0]
    
    detail_df = exp_df.filter(
        (col("run_timestamp") == latest_run) &
        (col("tenant_id").isNotNull())
    )
    
    detail_count = detail_df.count()
    
    if detail_count > 0:
        print(f"\n🔍 Recent Failures ({detail_count} records):")
        print("-" * 80)
        
        detail_df.select(
            "tenant_id",
            "cow_id",
            "sequence_number",
            "event_type",
            "failure_reason"
        ).show(20, truncate=False)


def show_tenant_quality(spark: SparkSession, tenant_id: str):
    """Show quality issues for a specific tenant"""
    
    print("\n" + "=" * 80)
    print(f"QUALITY ISSUES FOR TENANT: {tenant_id}")
    print("=" * 80)
    
    try:
        log_df = spark.read.format("delta").load("s3a://silver/quality_log")
    except Exception as e:
        print(f"\n⚠️  Quality log table not found")
        return
    
    tenant_df = log_df.filter(col("tenant_id") == tenant_id)
    
    if tenant_df.rdd.isEmpty():
        print(f"\n✓ No quality issues for this tenant!")
        return
    
    # Show issues by expectation
    print("\n📋 Issues by Expectation:")
    print("-" * 80)
    
    by_expectation = tenant_df.groupBy("expectation_name", "severity") \
        .agg(count("*").alias("failure_count")) \
        .orderBy(desc("failure_count"))
    
    by_expectation.show(truncate=False)
    
    # Show affected cows
    print("\n🐮 Affected Cows:")
    print("-" * 80)
    
    by_cow = tenant_df.groupBy("cow_id") \
        .agg(
            count("*").alias("failure_count"),
            collect_list("expectation_name").alias("failed_expectations")
        ) \
        .orderBy(desc("failure_count"))
    
    by_cow.select("cow_id", "failure_count").show(truncate=False)


def show_problematic_bronze_events(spark: SparkSession):
    """Identify problematic events in Bronze that are causing Silver failures"""
    
    print("\n" + "=" * 80)
    print("PROBLEMATIC BRONZE EVENTS")
    print("=" * 80)
    
    try:
        log_df = spark.read.format("delta").load("s3a://silver/quality_log")
        bronze_df = spark.read.format("delta").load("s3a://bronze/cow_events")
    except Exception as e:
        print(f"\n⚠️  Could not load tables: {e}")
        return
    
    # Get failed records from quality log
    latest_run = log_df.agg(spark_max("run_timestamp")).collect()[0][0]
    
    failed_df = log_df.filter(
        (col("run_timestamp") == latest_run) &
        (col("tenant_id").isNotNull())
    )
    
    if failed_df.rdd.isEmpty():
        print("\n✓ No problematic events found!")
        return
    
    # Join with Bronze to get event details
    print("\n🔍 Bronze Events That Failed Silver Validation:")
    print("-" * 80)
    
    joined_df = failed_df.join(
        bronze_df,
        (failed_df.tenant_id == bronze_df.tenant_id) &
        (failed_df.cow_id == bronze_df.cow_id) &
        (failed_df.sequence_number == bronze_df.sequence_number),
        "inner"
    )
    
    joined_df.select(
        bronze_df.event_id,
        bronze_df.tenant_id,
        bronze_df.cow_id,
        bronze_df.event_type,
        bronze_df.sequence_number,
        bronze_df.event_timestamp,
        failed_df.expectation_name,
        failed_df.failure_reason
    ).show(20, truncate=False)
    
    print(f"\nTotal problematic events: {joined_df.count()}")
    print("\nRecommendations:")
    print("  1. Review source data quality in SQL Server")
    print("  2. Add validation rules to Bronze ingestion")
    print("  3. Update cow records with correct values")
    print("  4. Re-run Silver resolution after fixing source data")


def export_report(spark: SparkSession, output_file: str):
    """Export quality report to CSV"""
    
    print(f"\n📄 Exporting quality report to: {output_file}")
    
    try:
        log_df = spark.read.format("delta").load("s3a://silver/quality_log")
    except Exception as e:
        print(f"\n⚠️  Quality log table not found")
        return
    
    if log_df.rdd.isEmpty():
        print("\n⚠️  No data to export")
        return
    
    # Get latest run summary
    latest_run = log_df.agg(spark_max("run_timestamp")).collect()[0][0]
    
    summary_df = log_df.filter(
        (col("run_timestamp") == latest_run) &
        (col("tenant_id").isNull())
    ).orderBy(desc("failure_count"))
    
    # Export to CSV
    summary_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_file + "_temp")
    
    # Move CSV file to final location
    import shutil
    import glob
    
    csv_files = glob.glob(f"{output_file}_temp/*.csv")
    if csv_files:
        shutil.move(csv_files[0], output_file)
        shutil.rmtree(f"{output_file}_temp")
        print(f"✓ Report exported to {output_file}")
    else:
        print("⚠️  Export failed")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Silver Layer Quality Report")
    parser.add_argument("--expectation", help="Show details for specific expectation")
    parser.add_argument("--tenant", help="Show quality issues for specific tenant")
    parser.add_argument("--problematic", action="store_true", help="Show problematic Bronze events")
    parser.add_argument("--export", help="Export report to CSV file")
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    try:
        if args.expectation:
            show_expectation_details(spark, args.expectation)
        elif args.tenant:
            show_tenant_quality(spark, args.tenant)
        elif args.problematic:
            show_problematic_bronze_events(spark)
        elif args.export:
            export_report(spark, args.export)
        else:
            show_overall_summary(spark)
            
            # Also show problematic events if any failures
            log_df = spark.read.format("delta").load("s3a://silver/quality_log")
            if not log_df.rdd.isEmpty():
                latest_run = log_df.agg(spark_max("run_timestamp")).collect()[0][0]
                failed_count = log_df.filter(
                    (col("run_timestamp") == latest_run) &
                    (col("tenant_id").isNotNull())
                ).count()
                
                if failed_count > 0:
                    show_problematic_bronze_events(spark)
    
    finally:
        spark.stop()
    
    print("\n" + "=" * 80)
    print("Report complete")
    print("=" * 80)


if __name__ == "__main__":
    main()
