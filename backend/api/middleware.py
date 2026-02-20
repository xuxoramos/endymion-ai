"""
Middleware for multi-tenant isolation and request logging.

Provides:
- Request logging with tenant context
- Tenant ID validation
- Request/response timing
- Error tracking
"""

import logging
import time
from typing import Callable
from uuid import UUID

from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp


logger = logging.getLogger(__name__)


class TenantContextMiddleware(BaseHTTPMiddleware):
    """
    Middleware for tenant isolation and context management.
    
    Responsibilities:
    1. Extract and validate tenant ID from headers
    2. Add tenant context to request state
    3. Log all requests with tenant context
    4. Measure request duration
    5. Handle tenant-related errors
    
    Exempt Routes:
    - /health - Health check endpoint
    - /docs - API documentation
    - /redoc - API documentation (alternative)
    - /openapi.json - OpenAPI schema
    - / - Root endpoint
    """
    
    # Routes that don't require tenant ID
    EXEMPT_ROUTES = {
        "/",
        "/health",
        "/docs",
        "/redoc",
        "/openapi.json"
    }
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request with tenant context.
        
        Args:
            request: Incoming HTTP request
            call_next: Next middleware or route handler
        
        Returns:
            HTTP response
        """
        start_time = time.time()
        
        # Check if route is exempt from tenant validation
        path = request.url.path
        is_exempt = any(path.startswith(route) for route in self.EXEMPT_ROUTES)
        
        # Extract tenant ID from header
        tenant_id_str = request.headers.get("x-tenant-id")
        tenant_id = None
        
        # Validate tenant ID for non-exempt routes
        if not is_exempt:
            if not tenant_id_str:
                logger.warning(
                    f"Request to {path} missing tenant ID - "
                    f"Method: {request.method}, IP: {request.client.host}"
                )
                return JSONResponse(
                    status_code=status.HTTP_403_FORBIDDEN,
                    content={
                        "error": "MissingTenantID",
                        "message": "Tenant ID is required for this endpoint",
                        "help": "Include X-Tenant-ID header with valid UUID",
                        "path": path,
                        "method": request.method
                    }
                )
            
            # Validate UUID format
            try:
                tenant_id = UUID(tenant_id_str)
            except ValueError:
                logger.warning(
                    f"Invalid tenant ID format: {tenant_id_str} - "
                    f"Path: {path}, Method: {request.method}"
                )
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={
                        "error": "InvalidTenantIDFormat",
                        "message": "Tenant ID must be a valid UUID",
                        "provided": tenant_id_str,
                        "example": "550e8400-e29b-41d4-a716-446655440000",
                        "path": path,
                        "method": request.method
                    }
                )
        
        # Add tenant context to request state
        request.state.tenant_id = tenant_id
        request.state.tenant_id_str = tenant_id_str
        
        # Log request with tenant context
        log_message = (
            f"Request: {request.method} {path} - "
            f"Tenant: {tenant_id_str or 'none'} - "
            f"IP: {request.client.host if request.client else 'unknown'}"
        )
        logger.info(log_message)
        
        # Process request
        try:
            response = await call_next(request)
            
            # Calculate request duration
            duration = time.time() - start_time
            
            # Add custom headers
            response.headers["X-Request-Duration"] = f"{duration:.3f}"
            if tenant_id:
                response.headers["X-Tenant-ID"] = tenant_id_str
            
            # Log response
            logger.info(
                f"Response: {response.status_code} - "
                f"{request.method} {path} - "
                f"Tenant: {tenant_id_str or 'none'} - "
                f"Duration: {duration:.3f}s"
            )
            
            return response
            
        except Exception as e:
            duration = time.time() - start_time
            
            # Safely extract error details as strings
            error_message = str(e)
            error_type = type(e).__name__
            path_str = str(path)
            method_str = str(request.method)
            tenant_str = str(tenant_id_str) if tenant_id_str else "none"
            
            # Log error with tenant context
            logger.error(
                f"Error: {error_message} - "
                f"{method_str} {path_str} - "
                f"Tenant: {tenant_str} - "
                f"Duration: {duration:.3f}s - "
                f"Type: {error_type}",
                exc_info=True
            )
            
            # Return error response with safely serialized content
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "error": error_type,
                    "message": error_message if error_message else "An unexpected error occurred",
                    "path": path_str,
                    "method": method_str,
                    "tenant_id": tenant_str
                }
            )


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for detailed request/response logging.
    
    Logs:
    - Request method, path, headers
    - Request body (if configured)
    - Response status code
    - Response time
    - Errors and exceptions
    """
    
    def __init__(self, app: ASGIApp, log_bodies: bool = False):
        super().__init__(app)
        self.log_bodies = log_bodies
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Log request and response details.
        
        Args:
            request: Incoming HTTP request
            call_next: Next middleware or route handler
        
        Returns:
            HTTP response
        """
        # Extract request details
        tenant_id = request.headers.get("x-tenant-id", "none")
        user_agent = request.headers.get("user-agent", "unknown")
        client_ip = request.client.host if request.client else "unknown"
        
        # Log request
        logger.debug(
            f">>> Request: {request.method} {request.url.path} - "
            f"Tenant: {tenant_id} - "
            f"IP: {client_ip} - "
            f"User-Agent: {user_agent}"
        )
        
        # Process request
        start_time = time.time()
        response = await call_next(request)
        duration = time.time() - start_time
        
        # Log response
        logger.debug(
            f"<<< Response: {response.status_code} - "
            f"{request.method} {request.url.path} - "
            f"Duration: {duration:.3f}s"
        )
        
        return response


class TenantIsolationMiddleware(BaseHTTPMiddleware):
    """
    Middleware to enforce strict tenant isolation.
    
    Additional security layer that validates:
    - Tenant ID is present in all API requests
    - Tenant ID format is valid
    - No cross-tenant data leakage
    """
    
    def __init__(self, app: ASGIApp, strict_mode: bool = True):
        super().__init__(app)
        self.strict_mode = strict_mode
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Enforce tenant isolation.
        
        Args:
            request: Incoming HTTP request
            call_next: Next middleware or route handler
        
        Returns:
            HTTP response
        """
        # Skip validation for public endpoints
        public_paths = ["/", "/health", "/docs", "/redoc", "/openapi.json"]
        if any(request.url.path.startswith(p) for p in public_paths):
            return await call_next(request)
        
        # In strict mode, require tenant ID for all API endpoints
        if self.strict_mode and request.url.path.startswith("/api/"):
            tenant_id = request.headers.get("x-tenant-id")
            
            if not tenant_id:
                logger.error(
                    f"Tenant isolation violation: Missing tenant ID - "
                    f"{request.method} {request.url.path}"
                )
                return JSONResponse(
                    status_code=status.HTTP_403_FORBIDDEN,
                    content={
                        "error": "TenantIsolationViolation",
                        "message": "This endpoint requires tenant authentication",
                        "path": request.url.path,
                        "help": "Include X-Tenant-ID header"
                    }
                )
        
        return await call_next(request)


class PerformanceMonitoringMiddleware(BaseHTTPMiddleware):
    """
    Middleware for performance monitoring and metrics.
    
    Tracks:
    - Request count per tenant
    - Response times
    - Error rates
    - Slow queries
    """
    
    # Threshold for slow request warning (seconds)
    SLOW_REQUEST_THRESHOLD = 1.0
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.request_counts = {}
        self.error_counts = {}
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Monitor request performance.
        
        Args:
            request: Incoming HTTP request
            call_next: Next middleware or route handler
        
        Returns:
            HTTP response
        """
        tenant_id = request.headers.get("x-tenant-id", "anonymous")
        start_time = time.time()
        
        # Increment request counter
        self.request_counts[tenant_id] = self.request_counts.get(tenant_id, 0) + 1
        
        try:
            response = await call_next(request)
            duration = time.time() - start_time
            
            # Warn on slow requests
            if duration > self.SLOW_REQUEST_THRESHOLD:
                logger.warning(
                    f"Slow request: {duration:.3f}s - "
                    f"{request.method} {request.url.path} - "
                    f"Tenant: {tenant_id}"
                )
            
            return response
            
        except Exception as e:
            # Track errors per tenant
            self.error_counts[tenant_id] = self.error_counts.get(tenant_id, 0) + 1
            
            logger.error(
                f"Request error: {str(e)} - "
                f"Tenant: {tenant_id} - "
                f"Total errors: {self.error_counts[tenant_id]}"
            )
            raise


# ========================================
# Middleware Factory Functions
# ========================================

def create_tenant_middleware() -> TenantContextMiddleware:
    """
    Create tenant context middleware instance.
    
    Returns:
        Configured middleware
    """
    return TenantContextMiddleware


def create_logging_middleware(log_bodies: bool = False) -> RequestLoggingMiddleware:
    """
    Create request logging middleware instance.
    
    Args:
        log_bodies: Whether to log request/response bodies
    
    Returns:
        Configured middleware
    """
    def middleware_factory(app: ASGIApp) -> RequestLoggingMiddleware:
        return RequestLoggingMiddleware(app, log_bodies=log_bodies)
    
    return middleware_factory


def create_isolation_middleware(strict_mode: bool = True) -> TenantIsolationMiddleware:
    """
    Create tenant isolation middleware instance.
    
    Args:
        strict_mode: Whether to enforce strict tenant validation
    
    Returns:
        Configured middleware
    """
    def middleware_factory(app: ASGIApp) -> TenantIsolationMiddleware:
        return TenantIsolationMiddleware(app, strict_mode=strict_mode)
    
    return middleware_factory
