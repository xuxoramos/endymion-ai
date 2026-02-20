"""
Authentication and authorization utilities.

Provides tenant identification and validation for multi-tenant isolation.
For prototype: Uses X-Tenant-ID header. In production: Use JWT tokens.
"""

import logging
from typing import Optional
from uuid import UUID

from fastapi import Header, HTTPException, status
from jose import JWTError, jwt


logger = logging.getLogger(__name__)


# ========================================
# JWT Configuration (Future Implementation)
# ========================================

# TODO: Move to environment variables
SECRET_KEY = "your-secret-key-here-replace-in-production"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


# ========================================
# Tenant Identification
# ========================================

async def get_current_tenant_id(
    x_tenant_id: Optional[str] = Header(
        None,
        description="Tenant UUID for multi-tenant isolation",
        example="550e8400-e29b-41d4-a716-446655440000"
    )
) -> UUID:
    """
    Extract and validate tenant ID from request.
    
    **Prototype Implementation:**
    - Reads from X-Tenant-ID header
    - Validates UUID format
    - No authentication yet
    
    **Production Implementation (TODO):**
    - Extract from JWT token (Authorization: Bearer <token>)
    - Verify token signature
    - Validate user belongs to tenant
    - Check user permissions
    
    Args:
        x_tenant_id: Tenant ID from X-Tenant-ID header
    
    Returns:
        Validated tenant UUID
    
    Raises:
        HTTPException 401: If tenant ID is missing
        HTTPException 400: If tenant ID format is invalid
        HTTPException 403: If user doesn't have access to tenant (future)
    
    Usage:
        @app.get("/cows")
        async def list_cows(
            tenant_id: UUID = Depends(get_current_tenant_id)
        ):
            return query_cows(tenant_id)
    """
    if not x_tenant_id:
        logger.warning("Request missing X-Tenant-ID header")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "MissingTenantID",
                "message": "Tenant ID is required for this operation",
                "help": "Provide X-Tenant-ID header with valid UUID"
            }
        )
    
    # Validate UUID format
    try:
        tenant_uuid = UUID(x_tenant_id)
        logger.debug(f"Tenant ID validated: {tenant_uuid}")
        return tenant_uuid
    except ValueError as e:
        logger.warning(f"Invalid tenant ID format: {x_tenant_id}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "InvalidTenantIDFormat",
                "message": f"Tenant ID must be a valid UUID format",
                "provided": x_tenant_id,
                "example": "550e8400-e29b-41d4-a716-446655440000"
            }
        )


async def get_optional_tenant_id(
    x_tenant_id: Optional[str] = Header(None)
) -> Optional[UUID]:
    """
    Extract optional tenant ID from request.
    
    Used for endpoints that can work without tenant context
    (e.g., health checks, public endpoints).
    
    Args:
        x_tenant_id: Optional tenant ID from header
    
    Returns:
        Tenant UUID or None if not provided
    """
    if not x_tenant_id:
        return None
    
    try:
        return UUID(x_tenant_id)
    except ValueError:
        logger.debug(f"Invalid optional tenant ID: {x_tenant_id}")
        return None


# ========================================
# JWT Token Handling (Future Implementation)
# ========================================

def create_access_token(data: dict, expires_delta: Optional[int] = None) -> str:
    """
    Create JWT access token.
    
    TODO: Implement for production authentication.
    
    Args:
        data: Token payload (user_id, tenant_id, roles)
        expires_delta: Expiration time in minutes
    
    Returns:
        Encoded JWT token
    
    Example:
        token = create_access_token({
            "user_id": "123",
            "tenant_id": "550e8400-...",
            "email": "user@example.com",
            "roles": ["admin"]
        })
    """
    from datetime import datetime, timedelta
    
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + timedelta(minutes=expires_delta)
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> dict:
    """
    Decode and validate JWT token.
    
    TODO: Implement for production authentication.
    
    Args:
        token: JWT token string
    
    Returns:
        Decoded token payload
    
    Raises:
        HTTPException: If token is invalid or expired
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError as e:
        logger.warning(f"JWT validation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )


async def get_current_user_from_token(
    authorization: Optional[str] = Header(None)
) -> dict:
    """
    Extract current user from JWT token.
    
    TODO: Implement for production authentication.
    
    Args:
        authorization: Authorization header (Bearer <token>)
    
    Returns:
        User information from token
    
    Raises:
        HTTPException: If token is missing or invalid
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format. Use: Bearer <token>",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    token = authorization[7:]  # Remove "Bearer " prefix
    payload = decode_access_token(token)
    
    return {
        "user_id": payload.get("user_id"),
        "tenant_id": payload.get("tenant_id"),
        "email": payload.get("email"),
        "roles": payload.get("roles", [])
    }


# ========================================
# Tenant Validation
# ========================================

def validate_tenant_access(user_tenant_id: UUID, resource_tenant_id: UUID) -> bool:
    """
    Validate user has access to resource tenant.
    
    In multi-tenant system, users can only access resources
    within their own tenant.
    
    Args:
        user_tenant_id: Tenant ID from user's token/header
        resource_tenant_id: Tenant ID of the resource being accessed
    
    Returns:
        True if access is allowed
    
    Raises:
        HTTPException 403: If tenant IDs don't match
    """
    if user_tenant_id != resource_tenant_id:
        logger.warning(
            f"Tenant access violation: user tenant {user_tenant_id} "
            f"attempted to access resource tenant {resource_tenant_id}"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "TenantAccessDenied",
                "message": "You do not have access to this resource",
                "reason": "Resource belongs to a different tenant"
            }
        )
    
    return True


def validate_tenant_in_request_body(tenant_id: UUID, body_tenant_id: Optional[UUID]) -> bool:
    """
    Validate tenant ID in request body matches authenticated tenant.
    
    Prevents tenant ID spoofing in request payloads.
    
    Args:
        tenant_id: Authenticated tenant ID
        body_tenant_id: Tenant ID from request body
    
    Returns:
        True if valid
    
    Raises:
        HTTPException 400: If tenant IDs don't match
    """
    if body_tenant_id and body_tenant_id != tenant_id:
        logger.warning(
            f"Tenant ID mismatch: authenticated={tenant_id}, "
            f"body={body_tenant_id}"
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "TenantIDMismatch",
                "message": "Tenant ID in request body does not match authenticated tenant",
                "authenticated_tenant": str(tenant_id),
                "provided_tenant": str(body_tenant_id)
            }
        )
    
    return True


# ========================================
# Mock User Context (Development)
# ========================================

async def get_current_user_email(
    authorization: Optional[str] = Header(None)
) -> str:
    """
    Get current user email for audit trail.
    
    Development implementation returns mock value.
    Production should extract from JWT token.
    
    Args:
        authorization: Optional Bearer token
    
    Returns:
        User email address
    """
    # TODO: Extract from JWT in production
    if authorization and authorization.startswith("Bearer "):
        # In production, decode token and get email
        return "user@example.com"
    
    # Development default
    return "api-user@localhost"


async def get_current_user_id(
    authorization: Optional[str] = Header(None)
) -> str:
    """
    Get current user ID for audit trail.
    
    Development implementation returns mock value.
    Production should extract from JWT token.
    
    Args:
        authorization: Optional Bearer token
    
    Returns:
        User ID
    """
    # TODO: Extract from JWT in production
    if authorization and authorization.startswith("Bearer "):
        # In production, decode token and get user_id
        return "user-123"
    
    # Development default
    return "api-user"
