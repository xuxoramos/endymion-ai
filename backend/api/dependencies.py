"""
FastAPI dependency injection utilities.

Provides:
- Database session management
- Tenant identification from JWT/headers
- Current user context
"""

import os
from typing import Generator, Optional
from uuid import UUID

from fastapi import Depends, HTTPException, Header, status
from sqlalchemy.orm import Session

from backend.database.connection import get_db_manager


# ========================================
# Database Session Dependencies
# ========================================

def get_db() -> Generator[Session, None, None]:
    """
    Dependency for database session injection.
    
    Provides a SQLAlchemy session with automatic commit/rollback.
    
    Usage:
        @app.get("/cows")
        def list_cows(db: Session = Depends(get_db)):
            return db.query(Cow).all()
    
    Yields:
        SQLAlchemy Session instance
    """
    db_manager = get_db_manager()
    session = db_manager.get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ========================================
# Tenant Identification
# ========================================

async def get_tenant_id(
    x_tenant_id: Optional[str] = Header(None, description="Tenant ID from header")
) -> UUID:
    """
    Extract tenant ID from request headers.
    
    In production, this should extract tenant_id from:
    1. JWT token (preferred) - validates user belongs to tenant
    2. X-Tenant-ID header (for testing/dev)
    
    Current implementation uses header for simplicity.
    
    Args:
        x_tenant_id: Tenant ID from X-Tenant-ID header
    
    Returns:
        UUID of tenant
    
    Raises:
        HTTPException: If tenant ID is missing or invalid
    
    Usage:
        @app.get("/cows")
        def list_cows(tenant_id: UUID = Depends(get_tenant_id)):
            return Cow.get_active_cows(session, tenant_id).all()
    """
    if not x_tenant_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing tenant ID. Provide X-Tenant-ID header.",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    try:
        tenant_uuid = UUID(x_tenant_id)
        return tenant_uuid
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid tenant ID format: {x_tenant_id}. Must be a valid UUID."
        )


async def get_optional_tenant_id(
    x_tenant_id: Optional[str] = Header(None, description="Optional tenant ID")
) -> Optional[UUID]:
    """
    Extract optional tenant ID from request headers.
    
    Used for endpoints that can work without tenant context (e.g., health checks).
    
    Args:
        x_tenant_id: Optional tenant ID from X-Tenant-ID header
    
    Returns:
        UUID of tenant or None
    """
    if not x_tenant_id:
        return None
    
    try:
        return UUID(x_tenant_id)
    except ValueError:
        return None


# ========================================
# User Context (Future Enhancement)
# ========================================

async def get_current_user(
    authorization: Optional[str] = Header(None, description="Bearer token")
) -> Optional[str]:
    """
    Extract current user from JWT token.
    
    TODO: Implement proper JWT validation:
    1. Extract token from Authorization header
    2. Verify signature with secret key
    3. Decode claims (user_id, tenant_id, roles)
    4. Return user context
    
    Current implementation is a placeholder for development.
    
    Args:
        authorization: Authorization header with Bearer token
    
    Returns:
        User identifier or None
    
    Usage:
        @app.post("/cows")
        def create_cow(user: str = Depends(get_current_user)):
            # Use user for created_by field
            pass
    """
    # TODO: Implement JWT validation
    # For now, return a placeholder or extract from header
    if authorization and authorization.startswith("Bearer "):
        # In production, decode and validate JWT here
        # token = authorization[7:]
        # payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        # return payload.get("user_id")
        return "api-user"
    
    return "anonymous"


async def get_current_user_email(
    authorization: Optional[str] = Header(None, description="Bearer token")
) -> Optional[str]:
    """
    Extract current user email from JWT token.
    
    TODO: Implement proper JWT validation to extract email claim.
    
    Args:
        authorization: Authorization header with Bearer token
    
    Returns:
        User email or None
    """
    # TODO: Implement JWT validation
    # For now, return placeholder
    return "user@example.com"


# ========================================
# Pagination Dependencies
# ========================================

def get_pagination(
    page: int = 1,
    page_size: int = 20
) -> tuple[int, int]:
    """
    Dependency for pagination parameters.
    
    Validates and normalizes pagination parameters.
    
    Args:
        page: Page number (1-indexed)
        page_size: Number of items per page
    
    Returns:
        Tuple of (offset, limit)
    
    Raises:
        HTTPException: If parameters are invalid
    
    Usage:
        @app.get("/cows")
        def list_cows(pagination: tuple[int, int] = Depends(get_pagination)):
            offset, limit = pagination
            return query.offset(offset).limit(limit).all()
    """
    # Validate page number
    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Page number must be >= 1"
        )
    
    # Validate and limit page size
    min_page_size = 1
    max_page_size = 100
    
    if page_size < min_page_size:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Page size must be >= {min_page_size}"
        )
    
    if page_size > max_page_size:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Page size must be <= {max_page_size}"
        )
    
    # Calculate offset and limit
    offset = (page - 1) * page_size
    limit = page_size
    
    return offset, limit


# ========================================
# Filtering Dependencies
# ========================================

def get_cow_filters(
    status: Optional[str] = None,
    breed: Optional[str] = None,
    min_age_days: Optional[int] = None,
    max_age_days: Optional[int] = None
) -> dict:
    """
    Dependency for cow filtering parameters.
    
    Collects and validates filter parameters for cow queries.
    
    Args:
        status: Filter by status (active, sold, deceased, transferred)
        breed: Filter by breed
        min_age_days: Minimum age in days
        max_age_days: Maximum age in days
    
    Returns:
        Dictionary of filters
    
    Usage:
        @app.get("/cows")
        def list_cows(filters: dict = Depends(get_cow_filters)):
            if filters.get("status"):
                query = query.filter(Cow.status == filters["status"])
    """
    filters = {}
    
    if status:
        valid_statuses = ["active", "sold", "deceased", "transferred"]
        if status not in valid_statuses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
            )
        filters["status"] = status
    
    if breed:
        filters["breed"] = breed
    
    if min_age_days is not None:
        if min_age_days < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="min_age_days must be >= 0"
            )
        filters["min_age_days"] = min_age_days
    
    if max_age_days is not None:
        if max_age_days < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="max_age_days must be >= 0"
            )
        if min_age_days is not None and max_age_days < min_age_days:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="max_age_days must be >= min_age_days"
            )
        filters["max_age_days"] = max_age_days
    
    return filters


# ========================================
# Environment Configuration
# ========================================

def get_settings():
    """
    Get application settings from environment variables.
    
    Returns:
        Dictionary of settings
    """
    return {
        "environment": os.getenv("ENVIRONMENT", "development"),
        "debug": os.getenv("DEBUG", "true").lower() == "true",
        "api_version": os.getenv("API_VERSION", "1.0.0"),
        "sync_interval_seconds": int(os.getenv("SYNC_INTERVAL_SECONDS", "60")),
    }
