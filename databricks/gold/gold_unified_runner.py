"""
Gold Layer - Unified Runner (Optimized)

Runs all Gold layer analytics in a SINGLE Spark session for maximum efficiency.
This eliminates Spark startup overhead (20-40 seconds per script).

Features:
- Single Spark session initialization
- Sequential execution of all Gold computations
- Shared Spark context across all analytics
- Error handling preserves partial results
- Progress logging

Expected time savings: 4-6 minutes per sync cycle

Usage:
    # Run all Gold analytics with default settings
    python databricks/gold/gold_unified_runner.py
    
    # Full refresh mode (rebuild all historical data)
    python databricks/gold/gold_unified_runner.py --full-refresh
    
    # Process specific date range
    python databricks/gold/gold_unified_runner.py --days 7
"""

import sys
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from common import create_spark_session, project_gold_to_sql
import logging

# Import computation functions from each Gold script
from gold_herd_composition import compute_herd_composition
from gold_cow_lifecycle import compute_cow_lifecycle
from gold_daily_snapshots import compute_daily_snapshot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# UNIFIED GOLD RUNNER
# ============================================================================

def run_all_gold_analytics(
    full_refresh: bool = False,
    days: int = 1,
    snapshot_date: date = None
):
    """
    Run all Gold layer analytics in a single Spark session.
    
    Args:
        full_refresh: If True, rebuild all historical data
        days: Number of days to process (for daily snapshots/composition)
        snapshot_date: Specific date to process (overrides days parameter)
    """
    
    logger.info("=" * 80)
    logger.info("Gold Layer - Unified Runner")
    logger.info("=" * 80)
    
    # Determine target date
    # Use UTC date since Silver data is written with UTC timestamps
    if snapshot_date is None:
        from datetime import timezone
        snapshot_date = datetime.now(timezone.utc).date()
    
    logger.info(f"Target date: {snapshot_date} (UTC)")
    logger.info(f"Full refresh: {full_refresh}")
    logger.info(f"Days to process: {days}")
    
    # Track execution status
    results = {
        "herd_composition": False,
        "cow_lifecycle": False,
        "daily_snapshots": False
    }
    
    # Track Gold DataFrames for SQL projection
    gold_dataframes = {}
    
    # Create single Spark session (MAJOR OPTIMIZATION)
    logger.info("")
    logger.info("⚡ Creating shared Spark session...")
    spark = create_spark_session("GoldLayerUnified")
    
    try:
        # ====================================================================
        # 1. Herd Composition Analytics
        # ====================================================================
        logger.info("")
        logger.info("📊 Running Herd Composition Analytics...")
        try:
            from gold_herd_composition import run_for_date as run_herd_composition_date
            
            if full_refresh:
                # Process last 30 days for full refresh
                start_date = snapshot_date - timedelta(days=30)
                end_date = snapshot_date
                current = start_date
                
                while current <= end_date:
                    logger.info(f"  Processing herd composition for {current}")
                    run_herd_composition_date(spark, current)
                    current += timedelta(days=1)
            else:
                # Just process target date
                run_herd_composition_date(spark, snapshot_date)
            
            # Read Gold data for SQL projection
            gold_df = spark.read.format('delta').load('s3a://gold/herd_composition')
            gold_dataframes['herd_composition'] = gold_df
            
            results["herd_composition"] = True
            logger.info("✅ Herd composition completed")
            
        except Exception as e:
            logger.error(f"❌ Herd composition failed: {str(e)}", exc_info=True)
        
        # ====================================================================
        # 2. Cow Lifecycle Analytics
        # ====================================================================
        logger.info("")
        logger.info("📊 Running Cow Lifecycle Analytics...")
        try:
            from gold_cow_lifecycle import run_full_refresh as run_lifecycle_full
            
            # Lifecycle always processes all cows (no incremental mode)
            # Add small delay to avoid Delta Lake write conflicts
            import time
            time.sleep(1)
            
            run_lifecycle_full(spark)
            
            # Read Gold data for SQL projection
            gold_df = spark.read.format('delta').load('s3a://gold/cow_lifecycle')
            gold_dataframes['cow_lifecycle'] = gold_df
            
            results["cow_lifecycle"] = True
            logger.info("✅ Cow lifecycle completed")
            
        except Exception as e:
            logger.error(f"❌ Cow lifecycle failed: {str(e)}", exc_info=True)
        
        # ====================================================================
        # 3. Daily Snapshots Analytics
        # ====================================================================
        logger.info("")
        logger.info("📊 Running Daily Snapshots Analytics...")
        try:
            from gold_daily_snapshots import run_for_date as run_snapshots_date
            
            if full_refresh:
                # Process last N days
                start_date = snapshot_date - timedelta(days=days)
                end_date = snapshot_date
                current = start_date
                
                while current <= end_date:
                    logger.info(f"  Processing daily snapshot for {current}")
                    run_snapshots_date(spark, current)
                    current += timedelta(days=1)
            else:
                # Just process target date
                run_snapshots_date(spark, snapshot_date)
            
            # Read Gold data for SQL projection
            gold_df = spark.read.format('delta').load('s3a://gold/daily_snapshots')
            gold_dataframes['daily_snapshots'] = gold_df
            
            results["daily_snapshots"] = True
            logger.info("✅ Daily snapshots completed")
            
        except Exception as e:
            logger.error(f"❌ Daily snapshots failed: {str(e)}", exc_info=True)
        
        # ====================================================================
        # SQL Projection (DISPOSABLE CACHE)
        # ====================================================================
        # IMPORTANT: This is a PROJECTION step, not part of Gold computation
        # - Gold Delta Lake is the canonical analytical truth
        # - SQL Server analytics schema is a disposable cache for API reads
        # - SQL tables can be rebuilt from Gold at any time
        # ====================================================================
        logger.info("")
        logger.info("📤 Projecting Gold analytics to SQL Server...")
        logger.info("   NOTE: SQL is a disposable projection, Gold Delta is canonical")
        
        projection_results = {}
        for table_name, df in gold_dataframes.items():
            try:
                logger.info(f"  → Projecting {table_name}...")
                project_gold_to_sql(df, table_name, mode="overwrite")
                projection_results[table_name] = True
                logger.info(f"  ✅ {table_name} projected to SQL")
            except Exception as e:
                logger.error(f"  ⚠️  Failed to project {table_name}: {str(e)}")
                logger.error(f"     Gold data is still valid in Delta Lake")
                projection_results[table_name] = False
                # Continue with other projections even if one fails
        
        logger.info("")
        logger.info(f"📊 SQL Projection Summary:")
        for table_name, success in projection_results.items():
            status = "✅" if success else "❌"
            logger.info(f"  {status} {table_name}")
        
    finally:
        # Always stop Spark session
        logger.info("")
        logger.info("🛑 Stopping Spark session...")
        spark.stop()
    
    # ========================================================================
    # Summary
    # ========================================================================
    logger.info("")
    logger.info("=" * 80)
    logger.info("Execution Summary")
    logger.info("=" * 80)
    
    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)
    
    for script, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"  {script:<25} {status}")
    
    logger.info("")
    logger.info(f"Completed: {success_count}/{total_count} analytics")
    
    if success_count == total_count:
        logger.info("🎉 All Gold analytics completed successfully!")
        return 0
    elif success_count > 0:
        logger.warning("⚠️  Some Gold analytics failed (partial success)")
        return 1
    else:
        logger.error("❌ All Gold analytics failed")
        return 2


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Unified Gold Layer Analytics Runner"
    )
    
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Rebuild all historical data (processes last 30 days)"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to process for daily analytics (default: 1)"
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="Specific date to process (YYYY-MM-DD format)"
    )
    
    args = parser.parse_args()
    
    # Parse target date
    if args.date:
        try:
            snapshot_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date} (expected YYYY-MM-DD)")
            return 1
    else:
        snapshot_date = date.today()
    
    # Run unified analytics
    exit_code = run_all_gold_analytics(
        full_refresh=args.full_refresh,
        days=args.days,
        snapshot_date=snapshot_date
    )
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
