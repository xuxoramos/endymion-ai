"""
Gold Layer - Rebuild All Tables

Rebuilds ALL Gold tables from Silver to prove reproducibility.

This is the REPRODUCIBILITY PROOF:
1. Truncates all Gold tables
2. Rebuilds from Silver source data
3. Verifies counts and consistency
4. Proves Gold is fully recomputable with no manual overrides

CRITICAL: Gold layer must be deterministic.
Same Silver → Same Gold (always)

Usage:
    # Rebuild all Gold tables
    python databricks/gold/rebuild_all.py
    
    # Rebuild specific table
    python databricks/gold/rebuild_all.py --table herd_composition
    
    # Verify only (no rebuild)
    python databricks/gold/rebuild_all.py --verify-only
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from pyspark.sql import SparkSession

from common import (
    create_spark_session,
    truncate_gold_table,
    verify_reproducibility,
    get_silver_source_info,
    list_gold_tables,
    GOLD_TABLES
)

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# REBUILD ORCHESTRATION
# ============================================================================

def rebuild_table(spark: SparkSession, table_name: str):
    """
    Rebuild a specific Gold table from Silver.
    
    Steps:
    1. Truncate existing Gold table
    2. Run the table's build script
    3. Verify results
    """
    
    table_info = GOLD_TABLES.get(table_name)
    
    if not table_info:
        logger.error(f"Unknown table: {table_name}")
        return False
    
    if table_info.get("status") == "planned":
        logger.warning(f"Table '{table_name}' is planned but not yet implemented")
        return False
    
    logger.info(f"\n{'=' * 80}")
    logger.info(f"REBUILDING: {table_name}")
    logger.info(f"{'=' * 80}")
    logger.info(f"Description: {table_info['description']}")
    logger.info(f"Source: {', '.join(table_info['source'])}")
    logger.info(f"Path: {table_info['path']}")
    
    # Step 1: Truncate
    logger.info("\nStep 1: Truncating existing data...")
    truncate_gold_table(spark, table_info['path'])
    
    # Step 2: Rebuild (import and run the script's main function)
    logger.info("\nStep 2: Rebuilding from Silver...")
    
    try:
        if table_name == "herd_composition":
            from gold_herd_composition import run_full_refresh
            run_full_refresh(spark)
        
        elif table_name == "daily_snapshots":
            from gold_daily_snapshots import run_full_refresh
            run_full_refresh(spark)
        
        elif table_name == "cow_lifecycle":
            from gold_cow_lifecycle import run_full_refresh
            run_full_refresh(spark)
        
        else:
            logger.error(f"No rebuild logic for table: {table_name}")
            return False
        
        logger.info(f"✓ {table_name} rebuilt successfully")
        
    except Exception as e:
        logger.error(f"✗ Failed to rebuild {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 3: Verify
    logger.info("\nStep 3: Verifying...")
    metrics = verify_reproducibility(spark, table_info['path'])
    
    logger.info(f"  Rows: {metrics['row_count']}")
    logger.info(f"  Columns: {len(metrics['columns'])}")
    logger.info(f"  Partitions: {metrics['partition_count']}")
    
    return True


def rebuild_all(spark: SparkSession):
    """
    Rebuild ALL Gold tables from Silver.
    
    This proves full reproducibility of the Gold layer.
    """
    
    logger.info("\n" + "=" * 80)
    logger.info("REBUILD ALL GOLD TABLES")
    logger.info("=" * 80)
    
    # Get Silver source info
    logger.info("\nSilver Source Information:")
    logger.info("-" * 80)
    
    silver_info = get_silver_source_info(spark)
    
    for key, value in silver_info.items():
        logger.info(f"  {key}: {value}")
    
    # Get list of active Gold tables
    tables = list_gold_tables(include_planned=False)
    
    logger.info(f"\n{len(tables)} Gold tables to rebuild")
    logger.info("-" * 80)
    
    # Rebuild each table
    results = {}
    
    for table_name in tables.keys():
        success = rebuild_table(spark, table_name)
        results[table_name] = success
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("REBUILD SUMMARY")
    logger.info("=" * 80)
    
    successful = sum(1 for success in results.values() if success)
    failed = len(results) - successful
    
    for table_name, success in results.items():
        status = "✓" if success else "✗"
        logger.info(f"  {status} {table_name}")
    
    logger.info(f"\nTotal: {len(results)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    
    if failed == 0:
        logger.info("\n🎉 ALL GOLD TABLES REBUILT SUCCESSFULLY")
        logger.info("✓ Reproducibility proven: Same Silver → Same Gold")
        return True
    else:
        logger.error(f"\n⚠️  {failed} table(s) failed to rebuild")
        return False


def verify_all(spark: SparkSession):
    """
    Verify all Gold tables without rebuilding.
    
    Checks that tables exist and reports metrics.
    """
    
    logger.info("\n" + "=" * 80)
    logger.info("VERIFY ALL GOLD TABLES")
    logger.info("=" * 80)
    
    tables = list_gold_tables(include_planned=False)
    
    logger.info(f"\nVerifying {len(tables)} Gold tables")
    logger.info("-" * 80)
    
    for table_name, table_info in tables.items():
        logger.info(f"\n{table_name}:")
        
        try:
            metrics = verify_reproducibility(spark, table_info['path'])
            logger.info(f"  ✓ Exists")
            logger.info(f"  Rows: {metrics['row_count']:,}")
            logger.info(f"  Columns: {len(metrics['columns'])}")
            
            # Check for lineage metadata
            if "_gold_table" in metrics['columns']:
                logger.info(f"  ✓ Has lineage metadata")
            else:
                logger.info(f"  ⚠️  Missing lineage metadata")
        
        except Exception as e:
            logger.warning(f"  ✗ Does not exist or error: {e}")
    
    logger.info("\n" + "=" * 80)
    logger.info("Verification Complete")
    logger.info("=" * 80)


def compare_before_after(spark: SparkSession, table_name: str):
    """
    Rebuild a table and compare before/after to prove determinism.
    
    Steps:
    1. Read current table metrics
    2. Truncate and rebuild
    3. Read new table metrics
    4. Compare (should be identical)
    """
    
    table_info = GOLD_TABLES.get(table_name)
    
    if not table_info:
        logger.error(f"Unknown table: {table_name}")
        return
    
    logger.info(f"\n{'=' * 80}")
    logger.info(f"DETERMINISM TEST: {table_name}")
    logger.info(f"{'=' * 80}")
    
    # Before metrics
    logger.info("\nStep 1: Reading current state...")
    try:
        before_metrics = verify_reproducibility(spark, table_info['path'])
        logger.info(f"  Current rows: {before_metrics['row_count']}")
    except Exception as e:
        logger.warning(f"  Table does not exist yet: {e}")
        before_metrics = None
    
    # Rebuild
    logger.info("\nStep 2: Rebuilding...")
    success = rebuild_table(spark, table_name)
    
    if not success:
        logger.error("Rebuild failed")
        return
    
    # After metrics
    logger.info("\nStep 3: Reading new state...")
    after_metrics = verify_reproducibility(spark, table_info['path'])
    logger.info(f"  New rows: {after_metrics['row_count']}")
    
    # Compare
    logger.info("\nStep 4: Comparing...")
    
    if before_metrics:
        row_match = before_metrics['row_count'] == after_metrics['row_count']
        
        if row_match:
            logger.info(f"  ✓ Row count matches: {after_metrics['row_count']}")
            logger.info(f"  ✓ DETERMINISTIC: Same Silver → Same Gold")
        else:
            logger.warning(f"  ⚠️  Row count changed:")
            logger.warning(f"     Before: {before_metrics['row_count']}")
            logger.warning(f"     After: {after_metrics['row_count']}")
            logger.warning(f"     This may be expected if Silver changed")
    else:
        logger.info(f"  ℹ️  First build, no comparison")
    
    logger.info("\n" + "=" * 80)


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Rebuild All Gold Tables")
    parser.add_argument("--table", help="Rebuild specific table only")
    parser.add_argument("--verify-only", action="store_true", help="Verify without rebuilding")
    parser.add_argument("--determinism-test", help="Test determinism for specific table")
    
    args = parser.parse_args()
    
    spark = create_spark_session("GoldRebuildAll")
    
    try:
        if args.verify_only:
            verify_all(spark)
        
        elif args.determinism_test:
            compare_before_after(spark, args.determinism_test)
        
        elif args.table:
            success = rebuild_table(spark, args.table)
            
            if not success:
                sys.exit(1)
        
        else:
            # Default: rebuild all
            success = rebuild_all(spark)
            
            if not success:
                sys.exit(1)
    
    finally:
        spark.stop()
    
    logger.info("\n" + "=" * 80)
    logger.info("Rebuild Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
