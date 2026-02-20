"""
FastAPI application - Cattle Management SaaS Platform.

Architecture: Pure Projection Pattern A (Event Sourcing + CQRS)

Write Path (Commands):
    API → cow_events (append-only) → Bronze → Silver → Gold
    
Read Path (Queries):
    API ← cows (projection) ← Silver

Key Principles:
1. Writes create events in cow_events table (source of truth)
2. Reads query cows projection table (materialized view)
3. Projection sync is asynchronous (eventual consistency)
4. Events are immutable (append-only, never updated/deleted)
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError

from backend.database.connection import get_db_manager
from backend.models.responses import ErrorResponse, HealthResponse
from backend.api.routers import cows, analytics, events
from backend.api.middleware import (
    TenantContextMiddleware,
    RequestLoggingMiddleware,
    PerformanceMonitoringMiddleware
)


# ========================================
# Logging Configuration
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ========================================
# Application Lifecycle
# ========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    
    Handles startup and shutdown events.
    """
    # Startup
    logger.info("Starting Endymion-AI API...")
    
    # Initialize database connection
    db_manager = get_db_manager()
    if db_manager.test_connection():
        logger.info("✅ Database connection established")
    else:
        logger.error("❌ Database connection failed")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Endymion-AI API...")
    db_manager.dispose()
    logger.info("✅ Database connections closed")


# ========================================
# FastAPI Application
# ========================================

app = FastAPI(
    title="Endymion-AI - Cattle Management Platform",
    description="""
    ## Event-Driven Cattle Management API
    
    This API follows **Pure Projection Pattern A** (Event Sourcing + CQRS):
    
    ### Architecture
    
    **Write Operations** (POST, PUT, DELETE):
    - Create immutable events in `cow_events` table
    - Return event acknowledgment immediately
    - Changes sync to projection asynchronously (60-120 seconds)
    
    **Read Operations** (GET):
    - Query from `cows` projection table
    - Data may lag behind recent events
    - Check `is_stale` flag for freshness indicator
    
    ### Data Flow
    
    ```
    Write: API → cow_events → Bronze → Silver → cows
    Read:  API ← cows (projection) ← Silver
    ```
    
    ### Event Types
    
    - `cow_created` - New cow registration
    - `cow_updated` - Cow attribute changes
    - `cow_deactivated` - Logical delete (status change)
    - `cow_weight_recorded` - Weight measurements
    - `cow_health_event` - Health-related events
    
    ### Important Notes
    
    ⚠️ **Eventual Consistency**: After a write operation, the entity may not 
    immediately appear in GET requests. Wait 60-120 seconds or poll with retry logic.
    
    ⚠️ **Projection Lag**: The `last_synced_at` timestamp indicates when data 
    was last synced from Silver layer. Use `is_stale` flag to detect outdated data.
    
    ⚠️ **Tenant Isolation**: All requests require `X-Tenant-ID` header for 
    multi-tenant isolation.
    
    ### Authentication
    
    Include tenant ID in header:
    ```
    X-Tenant-ID: 550e8400-e29b-41d4-a716-446655440000
    ```
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "health",
            "description": "Health check and system status"
        },
        {
            "name": "cows",
            "description": """
            Cow management endpoints (Event Sourcing + CQRS).
            
            **Write Operations** create events and return immediately.
            **Read Operations** query projection table (may lag behind events).
            """
        },
        {
            "name": "analytics",
            "description": """
            Pre-computed analytics from Gold layer Delta tables.
            
            **Features:**
            - Herd composition (breed/status/sex breakdown)
            - Weight trends (cow weight history)
            - Sales summary (daily sales aggregations)
            - 60-second in-memory cache
            - Direct PySpark queries to Delta Lake
            """
        },
    ]
)


# ========================================
# CORS Configuration
# ========================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========================================
# Custom Middleware
# ========================================

# Tenant context middleware (validates tenant ID, adds context)
app.add_middleware(TenantContextMiddleware)

# Performance monitoring middleware
app.add_middleware(PerformanceMonitoringMiddleware)

# Request logging middleware (detailed logging)
app.add_middleware(RequestLoggingMiddleware, log_bodies=False)


# ========================================
# Exception Handlers
# ========================================

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle Pydantic validation errors."""
    logger.warning(f"Validation error: {exc.errors()}")
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "ValidationError",
            "message": "Request validation failed",
            "details": exc.errors(),
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.exception_handler(SQLAlchemyError)
async def database_exception_handler(request: Request, exc: SQLAlchemyError):
    """Handle database errors."""
    logger.error(f"Database error: {str(exc)}")
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "DatabaseError",
            "message": "Database operation failed",
            "details": {"error": str(exc)},
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors."""
    logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "InternalServerError",
            "message": "An unexpected error occurred",
            "details": {"error": str(exc)},
            "timestamp": datetime.utcnow().isoformat()
        }
    )


# ========================================
# Root Endpoints
# ========================================

@app.get(
    "/",
    summary="API Root",
    description="Get API information and available endpoints",
    tags=["health"]
)
async def root():
    """API root endpoint."""
    return {
        "name": "Endymion-AI API",
        "version": "1.0.0",
        "description": "Event-driven cattle management platform",
        "architecture": "Pure Projection Pattern A (Event Sourcing + CQRS)",
        "docs": "/docs",
        "health": "/health"
    }


@app.get(
    "/health",
    summary="Health Check",
    description="""
    Comprehensive health check including sync job monitoring.
    
    Returns:
    - 200 OK if all systems healthy (sync lag < 5 minutes)
    - 503 Service Unavailable if critical issues detected
    
    Checks:
    - Database connectivity
    - Sync lag (time since last successful sync)
    - Event backlog (unpublished events)
    - Silver layer freshness
    - Data consistency (SQL vs Silver)
    - Recent sync failure rate
    """,
    tags=["health"]
)
async def health_check():
    """
    Health check endpoint with sync monitoring.
    
    Returns 200 if healthy, 503 if unhealthy.
    """
    from backend.monitoring.health_check import run_health_checks
    
    # Run basic DB check
    db_manager = get_db_manager()
    db_connected = db_manager.test_connection()
    
    if not db_connected:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "database": "disconnected",
                "message": "Database connection failed"
            }
        )
    
    # Run sync health checks
    try:
        health_report = run_health_checks()
        
        # Prepare response
        response_data = {
            "status": health_report.overall_status.value.lower(),
            "is_healthy": health_report.is_healthy,
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected",
            "version": "1.0.0",
            "checks": [
                {
                    "name": check.name,
                    "status": check.status.value,
                    "message": check.message
                }
                for check in health_report.checks
            ]
        }
        
        # Return 503 if unhealthy, 200 otherwise
        if health_report.is_healthy:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=response_data
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=response_data
            )
    
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "error",
                "timestamp": datetime.utcnow().isoformat(),
                "database": "connected",
                "message": f"Health check failed: {str(e)}"
            }
        )


@app.get(
    "/metrics",
    summary="Prometheus Metrics",
    description="Export Prometheus-style metrics for monitoring",
    tags=["health"]
)
async def metrics():
    """
    Prometheus metrics endpoint.
    
    Returns metrics in Prometheus text format.
    """
    from backend.monitoring.metrics import collect_all_metrics
    
    try:
        collector = collect_all_metrics()
        prometheus_text = collector.to_prometheus()
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"metrics": prometheus_text},
            media_type="text/plain"
        )
    
    except Exception as e:
        logger.error(f"Metrics collection failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "MetricsError",
                "message": "Failed to collect metrics",
                "details": {"error": str(e)}
            }
        )


@app.get(
    "/dashboard",
    summary="Monitoring Dashboard",
    description="HTML dashboard for sync monitoring",
    tags=["health"]
)
async def dashboard():
    """
    Web dashboard endpoint.
    
    Returns HTML dashboard.
    """
    from fastapi.responses import HTMLResponse
    from backend.monitoring.dashboard import generate_dashboard_html
    
    try:
        html = generate_dashboard_html()
        return HTMLResponse(content=html, status_code=200)
    
    except Exception as e:
        logger.error(f"Dashboard generation failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "DashboardError",
                "message": "Failed to generate dashboard",
                "details": {"error": str(e)}
            }
        )


# ========================================
# Router Registration
# ========================================

app.include_router(
    cows.router,
    prefix="/api/v1/cows",
    tags=["cows"]
)

app.include_router(
    analytics.router,
    prefix="/api/v1/analytics",
    tags=["analytics"]
)

app.include_router(
    events.router,
    prefix="/api/v1",
    tags=["events"]
)


# ========================================
# Development Server
# ========================================

if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting development server...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
