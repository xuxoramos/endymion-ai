"""
FastAPI router for Gold Layer Analytics.

Provides pre-computed analytics from Gold Delta tables via DuckDB:
- Herd composition (breed, status, sex breakdown)
- Cow lifecycle (individual cow tracking)
- Daily snapshots (herd totals over time)

Architecture:
    Gold Delta (canonical truth) → DuckDB queries → FastAPI

This mirrors Databricks SQL pattern where analytics query Delta tables directly.

Advantages over SQL Server projection:
- No sync delay (queries canonical Gold directly)
- Eliminates Gold→SQL projection step
- Faster analytics pipeline
- Direct Delta table access

Features:
- Fast DuckDB queries (10-50ms)
- 5-second in-memory cache
- Tenant isolation
- "as_of" timestamps for data freshness
"""

import logging
from datetime import datetime, timedelta, date as date_type
from typing import Dict, List, Optional, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database.connection import get_db_manager
from backend.database.duckdb_analytics import DuckDBAnalyticsRepository, get_duckdb_analytics
from backend.database.analytics_repository import AnalyticsRepository  # Keep for fallback


# ========================================
# Logging Configuration
# ========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ========================================
# Response Models
# ========================================

class AnalyticsResponse(BaseModel):
    """Base response model for analytics endpoints."""
    as_of: datetime = Field(..., description="Timestamp when data was retrieved")
    cached: bool = Field(..., description="Whether data was served from cache")
    tenant_id: UUID = Field(..., description="Tenant ID for the data")


class HerdCompositionBreakdown(BaseModel):
    """Single breakdown dimension (breed, status, or sex)."""
    dimension: str = Field(..., description="Dimension name (e.g., 'Holstein')")
    count: int = Field(..., description="Number of cows")
    percentage: float = Field(..., description="Percentage of total herd")


class HerdCompositionData(BaseModel):
    """Herd composition analytics data."""
    snapshot_date: str = Field(..., description="Date of snapshot (YYYY-MM-DD)")
    total_cows: int = Field(..., description="Total cows in herd")
    by_breed: List[HerdCompositionBreakdown] = Field(..., description="Breakdown by breed")
    by_status: List[HerdCompositionBreakdown] = Field(..., description="Breakdown by status")
    by_sex: List[HerdCompositionBreakdown] = Field(..., description="Breakdown by sex")


class HerdCompositionResponse(AnalyticsResponse):
    """Response for herd composition endpoint."""
    data: HerdCompositionData


class WeightTrendPoint(BaseModel):
    """Single point in weight trend."""
    date: str = Field(..., description="Date of weight measurement")
    weight_kg: float = Field(..., description="Weight in kilograms")
    trend: Optional[str] = Field(None, description="Trend indicator (up/down/stable)")


class WeightTrendsData(BaseModel):
    """Weight trends data."""
    cow_id: UUID = Field(..., description="Cow identifier")
    cow_name: Optional[str] = Field(None, description="Cow name")
    measurements: List[WeightTrendPoint] = Field(..., description="Weight measurements over time")
    average_weight_kg: Optional[float] = Field(None, description="Average weight")
    weight_change_kg: Optional[float] = Field(None, description="Total weight change")


class WeightTrendsResponse(AnalyticsResponse):
    """Response for weight trends endpoint."""
    data: WeightTrendsData


class SalesSummaryItem(BaseModel):
    """Single day sales summary."""
    date: str = Field(..., description="Sale date (YYYY-MM-DD)")
    total_sales: int = Field(..., description="Number of cows sold")
    total_revenue: Optional[float] = Field(None, description="Total revenue")
    average_price: Optional[float] = Field(None, description="Average sale price")


class SalesSummaryData(BaseModel):
    """Sales summary data."""
    start_date: str = Field(..., description="Start of date range")
    end_date: str = Field(..., description="End of date range")
    total_sales: int = Field(..., description="Total cows sold in period")
    total_revenue: Optional[float] = Field(None, description="Total revenue in period")
    daily_breakdown: List[SalesSummaryItem] = Field(..., description="Daily sales breakdown")


class SalesSummaryResponse(AnalyticsResponse):
    """Response for sales summary endpoint."""
    data: SalesSummaryData


# ========================================
# Cache Implementation
# ========================================

class SimpleCache:
    """
    Simple in-memory cache with TTL.
    
    For production, replace with Redis or similar.
    """
    
    def __init__(self, ttl_seconds: int = 60):
        """Initialize cache with TTL in seconds."""
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.ttl_seconds = ttl_seconds
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        if key not in self.cache:
            return None
        
        entry = self.cache[key]
        expires_at = entry["expires_at"]
        
        if datetime.utcnow() > expires_at:
            # Expired - remove from cache
            del self.cache[key]
            return None
        
        return entry["value"]
    
    def set(self, key: str, value: Any) -> None:
        """Set value in cache with TTL."""
        expires_at = datetime.utcnow() + timedelta(seconds=self.ttl_seconds)
        self.cache[key] = {
            "value": value,
            "expires_at": expires_at
        }
    
    def clear(self) -> None:
        """Clear all cache entries."""
        self.cache.clear()


# Global cache instance (5-second TTL for faster weight updates)
analytics_cache = SimpleCache(ttl_seconds=5)


# ========================================
# Database Session Dependency
# ========================================

def get_db() -> Session:
    """
    Get database session for analytics queries.
    
    Yields:
        SQLAlchemy Session
    """
    db_manager = get_db_manager()
    session = db_manager.get_session()
    try:
        yield session
    finally:
        session.close()


def get_analytics_repo(session: Session = Depends(get_db)) -> AnalyticsRepository:
    """
    Get analytics repository.
    
    Args:
        session: Database session
        
    Returns:
        AnalyticsRepository instance
    """
    return AnalyticsRepository(session)


# ========================================
# Dependency: Tenant ID
# ========================================

async def get_tenant_id(
    tenant_id: UUID = Query(..., description="Tenant ID for multi-tenant isolation")
) -> UUID:
    """
    Extract tenant ID from query parameter.
    
    Args:
        tenant_id: Tenant UUID from query string
    
    Returns:
        UUID of tenant
    
    Raises:
        HTTPException: If tenant ID is missing or invalid
    """
    if not tenant_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing tenant_id query parameter"
        )
    
    return tenant_id


# ========================================
# Router Definition
# ========================================

router = APIRouter()


# ========================================
# Endpoint: Herd Composition
# ========================================

@router.get(
    "/herd-composition",
    response_model=HerdCompositionResponse,
    summary="Get herd composition analytics",
    description="""
    Get pre-computed herd composition breakdown by breed, status, and sex.
    
    Data is sourced from Gold Delta tables via DuckDB (direct query, no SQL projection).
    
    **Features:**
    - Returns latest snapshot date or specific date
    - Breakdown by breed, status, and sex
    - Percentage calculations included
    - 5-second cache for performance
    - Direct Gold Delta query (10-50ms)
    - No sync delay (queries canonical Gold)
    
    **Response includes:**
    - `as_of`: Timestamp when data was retrieved
    - `cached`: Whether data was served from cache
    - `data`: Herd composition breakdown
    
    **Example:**
    ```bash
    curl "http://localhost:8000/api/v1/analytics/herd-composition?tenant_id=550e8400-e29b-41d4-a716-446655440000"
    ```
    """
)
async def get_herd_composition(
    tenant_id: UUID = Depends(get_tenant_id),
    date: Optional[str] = Query(None, description="Snapshot date (YYYY-MM-DD). Defaults to latest."),
    repo: DuckDBAnalyticsRepository = Depends(get_duckdb_analytics)
) -> HerdCompositionResponse:
    """
    Get herd composition analytics for a tenant.
    
    Args:
        tenant_id: Tenant identifier
        date: Optional snapshot date (defaults to latest)
        repo: DuckDB analytics repository
    
    Returns:
        HerdCompositionResponse with breed/status/sex breakdown
    
    Raises:
        HTTPException: If data not found or query fails
    """
    # Generate cache key
    cache_key = f"herd_composition:{tenant_id}:{date or 'latest'}"
    
    # Check cache
    cached_data = analytics_cache.get(cache_key)
    if cached_data:
        logger.info(f"Cache HIT for {cache_key}")
        return HerdCompositionResponse(
            as_of=datetime.utcnow(),
            cached=True,
            tenant_id=tenant_id,
            data=cached_data
        )
    
    logger.info(f"Cache MISS for {cache_key} - querying Gold Delta via DuckDB")
    
    try:
        # Parse date if provided
        snapshot_date = None
        if date:
            try:
                snapshot_date = datetime.strptime(date, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid date format: {date}. Use YYYY-MM-DD."
                )
        
        # Query DuckDB analytics repository (queries Gold Delta directly)
        df = repo.get_herd_composition(tenant_id, snapshot_date)
        
        if df is None or len(df) == 0:
            # Check if it's because no data exists or wrong date
            latest_date = repo.get_latest_snapshot_date(tenant_id)
            if latest_date is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No herd composition data found for tenant {tenant_id}"
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No herd composition data found for tenant {tenant_id} on {date or 'latest'}. Latest available: {latest_date}"
                )
        
        # Process Pandas DataFrame - group by dimension_type
        by_breed = []
        by_status = []
        by_sex = []
        total_cows = 0
        actual_date = None
        
        for _, row in df.iterrows():
            if actual_date is None:
                actual_date = row['snapshot_date']
                total_cows = int(row['total_cows'])
            
            breakdown = HerdCompositionBreakdown(
                dimension=row['dimension_value'],
                count=int(row['count']),
                percentage=float(row['percentage']) if row['percentage'] else 0.0
            )
            
            if row['dimension_type'] == "breed":
                by_breed.append(breakdown)
            elif row['dimension_type'] == "status":
                by_status.append(breakdown)
            elif row['dimension_type'] == "sex":
                by_sex.append(breakdown)
        
        # Build response data
        data = HerdCompositionData(
            snapshot_date=str(actual_date),
            total_cows=total_cows,
            by_breed=by_breed,
            by_status=by_status,
            by_sex=by_sex
        )
        
        # Cache result
        analytics_cache.set(cache_key, data)
        
        logger.info(f"✅ Herd composition query succeeded for {tenant_id} on {actual_date} (DuckDB/Gold Delta)")
        
        return HerdCompositionResponse(
            as_of=datetime.utcnow(),
            cached=False,
            tenant_id=tenant_id,
            data=data
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error querying herd composition: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query herd composition: {str(e)}"
        )


# ========================================
# Endpoint: Weight Trends (Placeholder)
# ========================================

@router.get(
    "/weight-trends/{cow_id}",
    response_model=WeightTrendsResponse,
    summary="Get cow weight trends",
    description="""
    Get weight trend analytics for a specific cow.
    
    **Status:** Not yet implemented (placeholder)
    
    Requires `gold_cow_weight_trends` table to be created first.
    
    **Example:**
    ```bash
    curl "http://localhost:8000/api/v1/analytics/weight-trends/550e8400-e29b-41d4-a716-446655440000?tenant_id=..."
    ```
    """
)
async def get_weight_trends(
    cow_id: UUID,
    tenant_id: UUID = Depends(get_tenant_id)
) -> WeightTrendsResponse:
    """
    Get weight trends for a cow.
    
    Args:
        cow_id: Cow identifier
        tenant_id: Tenant identifier
    
    Returns:
        WeightTrendsResponse with weight history
    
    Raises:
        HTTPException: Not yet implemented
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Weight trends endpoint not yet implemented. Requires gold_cow_weight_trends table."
    )


# ========================================
# Endpoint: Sales Summary (Placeholder)
# ========================================

@router.get(
    "/sales-summary",
    response_model=SalesSummaryResponse,
    summary="Get sales summary analytics",
    description="""
    Get sales summary analytics for a date range.
    
    **Status:** Not yet implemented (placeholder)
    
    Requires `gold_daily_sales` table to be created first.
    
    **Example:**
    ```bash
    curl "http://localhost:8000/api/v1/analytics/sales-summary?tenant_id=...&start_date=2024-01-01&end_date=2024-01-31"
    ```
    """
)
async def get_sales_summary(
    tenant_id: UUID = Depends(get_tenant_id),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)")
) -> SalesSummaryResponse:
    """
    Get sales summary for date range.
    
    Args:
        tenant_id: Tenant identifier
        start_date: Start of date range
        end_date: End of date range
    
    Returns:
        SalesSummaryResponse with sales breakdown
    
    Raises:
        HTTPException: Not yet implemented
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Sales summary endpoint not yet implemented. Requires gold_daily_sales table."
    )


# ========================================
# Endpoint: Cow Weight Trends
# ========================================

@router.get(
    "/cow/{cow_id}",
    response_model=WeightTrendsResponse,
    summary="Get cow lifecycle trends",
    description="""
    Get lifecycle and trend analytics for a specific cow.
    
    Data is sourced from operational.cow_events table (event history).
    This endpoint queries events directly for real-time weight data.
    
    **Note:** This endpoint does NOT use Gold Delta/DuckDB because:
    - Weight events are in operational.cow_events (not yet in Gold)
    - Provides real-time data from event log
    - No sync delay needed
    
    **Features:**
    - Weight measurement history
    - Real-time event data
    - 5-second cache for performance
    
    **Response includes:**
    - `as_of`: Timestamp when data was retrieved
    - `cached`: Whether data was served from cache
    - `data`: Weight trend data from events
    
    **Example:**
    ```bash
    curl "http://localhost:8000/api/v1/analytics/cow/{cow_id}?tenant_id=..."
    ```
    """
)
async def get_cow_lifecycle_trends(
    cow_id: UUID,
    tenant_id: UUID = Depends(get_tenant_id),
    repo: AnalyticsRepository = Depends(get_analytics_repo)
) -> WeightTrendsResponse:
    """
    Get lifecycle trends for a specific cow.
    
    Args:
        cow_id: Cow identifier
        tenant_id: Tenant identifier
        repo: Analytics repository
    
    Returns:
        WeightTrendsResponse with lifecycle data
    
    Raises:
        HTTPException: If cow not found or query fails
    """
    # Generate cache key
    cache_key = f"cow_lifecycle:{cow_id}:{tenant_id}"
    
    # Check cache
    cached_data = analytics_cache.get(cache_key)
    if cached_data:
        logger.info(f"Cache HIT for cow lifecycle {cow_id}")
        return WeightTrendsResponse(
            as_of=datetime.utcnow(),
            cached=True,
            tenant_id=tenant_id,
            data=cached_data
        )
    
    try:
        # FIX: Check cow existence from event history, not lifecycle analytics
        # Lifecycle data may not be available in newly created environments
        # or before analytics sync has run
        from sqlalchemy import text
        
        # Verify cow exists by checking for cow_created event
        cow_exists_query = text("""
            SELECT COUNT(*) as cnt
            FROM operational.cow_events
            WHERE cow_id = :cow_id
                AND tenant_id = :tenant_id
                AND event_type = 'cow_created'
        """)
        
        result = repo.session.execute(
            cow_exists_query,
            {
                "cow_id": str(cow_id).lower(),
                "tenant_id": str(tenant_id).lower()
            }
        )
        
        cow_exists = result.scalar() > 0
        
        if not cow_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Cow {cow_id} not found (no cow_created event)"
            )
        
        # Build trend points from weight events
        measurements = []
        
        # Query weight events directly from operational.cow_events table
        # This is a temporary solution until gold_cow_weight_trends is implemented
        try:
            weight_query = text("""
                SELECT 
                    event_time,
                    JSON_VALUE(payload, '$.weight_kg') as weight_kg,
                    JSON_VALUE(payload, '$.measurement_date') as measurement_date
                FROM operational.cow_events
                WHERE cow_id = :cow_id
                    AND tenant_id = :tenant_id
                    AND event_type = 'cow_weight_recorded'
                ORDER BY event_time ASC
            """)
            
            result = repo.session.execute(
                weight_query,
                {
                    "cow_id": str(cow_id).lower(),  # UUID normalized to lowercase per convention
                    "tenant_id": str(tenant_id).lower()  # UUID normalized to lowercase per convention
                }
            )
            
            for row in result:
                if row.weight_kg:
                    # Use measurement_date if available, otherwise event_time
                    date_str = row.measurement_date if row.measurement_date else str(row.event_time.date())
                    measurements.append(WeightTrendPoint(
                        date=date_str,
                        weight_kg=float(row.weight_kg),
                        trend="stable"
                    ))
            
            logger.info(f"Found {len(measurements)} weight measurements for cow {cow_id}")
            
        except Exception as e:
            logger.error(f"Failed to query weight events: {str(e)}")
            # Continue with empty measurements
        
        # Build response data
        lifecycle_data = WeightTrendsData(
            cow_id=cow_id,
            cow_name=None,  # Not available in lifecycle table
            measurements=measurements,
            average_weight_kg=None,  # Not tracked in lifecycle
            weight_change_kg=None  # Not tracked in lifecycle
        )
        
        # Cache the result
        analytics_cache.set(cache_key, lifecycle_data)
        
        logger.info(f"✅ Cow lifecycle query succeeded for {cow_id}")
        
        return WeightTrendsResponse(
            as_of=datetime.utcnow(),
            cached=False,
            tenant_id=tenant_id,
            data=lifecycle_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get cow lifecycle: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve cow lifecycle: {str(e)}"
        )


# ========================================
# Endpoint: Cache Management
# ========================================

@router.delete(
    "/cache",
    summary="Clear analytics cache",
    description="""
    Clear the in-memory analytics cache.
    
    Useful for development/testing or forcing fresh data retrieval.
    
    **Example:**
    ```bash
    curl -X DELETE "http://localhost:8000/api/v1/analytics/cache"
    ```
    """
)
async def clear_cache() -> Dict[str, str]:
    """
    Clear analytics cache.
    
    Returns:
        Status message
    """
    analytics_cache.clear()
    logger.info("Analytics cache cleared")
    return {
        "status": "success",
        "message": "Analytics cache cleared",
        "timestamp": datetime.utcnow().isoformat()
    }
