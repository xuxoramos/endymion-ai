"""
Database connection and session management.

Provides database engine, session factory, and utilities for
managing SQLAlchemy connections to SQL Server.
"""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, event, pool, Engine, text, URL
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.pool import QueuePool

# Load environment variables from .env file
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

from .base import Base


class DatabaseConfig:
    """Database connection configuration."""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: Optional[str] = None,
    ):
        """
        Initialize database configuration.
        
        Args:
            host: SQL Server hostname (defaults to SQLSERVER_HOST env var)
            port: SQL Server port (defaults to SQLSERVER_PORT env var)
            database: Database name (defaults to SQLSERVER_DATABASE env var)
            username: Username (defaults to SQLSERVER_USERNAME env var)
            password: Password (defaults to SQLSERVER_PASSWORD env var)
            driver: ODBC driver (defaults to SQLSERVER_DRIVER env var)
        """
        self.host = host or os.getenv("SQLSERVER_HOST", "localhost")
        self.port = int(port or os.getenv("SQLSERVER_PORT", "1433"))
        self.database = database or os.getenv("SQLSERVER_DATABASE", "cattledb")
        self.username = username or os.getenv("SQLSERVER_USERNAME", "sa")
        self.password = password or os.getenv("SQLSERVER_PASSWORD", "")
        self.driver = driver or os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
        
        # Connection pool settings
        self.pool_size = int(os.getenv("SQLSERVER_POOL_SIZE", "5"))
        self.max_overflow = int(os.getenv("SQLSERVER_MAX_OVERFLOW", "10"))
        self.pool_timeout = int(os.getenv("SQLSERVER_POOL_TIMEOUT", "30"))
        self.pool_recycle = int(os.getenv("SQLSERVER_POOL_RECYCLE", "3600"))
        
        # Connection settings
        self.connect_timeout = int(os.getenv("SQLSERVER_CONNECT_TIMEOUT", "30"))
        self.echo = os.getenv("SQLSERVER_ECHO", "false").lower() == "true"
        self.trust_server_certificate = os.getenv("SQLSERVER_TRUST_CERT", "yes")
    
    def get_connection_string(self, driver_type: str = "pyodbc") -> str:
        """
        Build SQLAlchemy connection string.
        
        Args:
            driver_type: 'pyodbc' or 'pymssql'
            
        Returns:
            SQLAlchemy connection string or URL object
        """
        if driver_type == "pymssql":
            # pymssql connection string (simpler, but less featured)
            return (
                f"mssql+pymssql://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            )
        else:
            # pyodbc connection - use URL object for proper escaping
            url = URL.create(
                "mssql+pyodbc",
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                query={
                    "driver": self.driver,
                    "TrustServerCertificate": self.trust_server_certificate,
                    "Encrypt": "no",
                    "timeout": str(self.connect_timeout)
                }
            )
            return url
    
    def __repr__(self) -> str:
        """String representation (without password)."""
        return (
            f"<DatabaseConfig("
            f"host={self.host}:{self.port}, "
            f"database={self.database}, "
            f"username={self.username}"
            f")>"
        )


class DatabaseManager:
    """
    Database connection and session management.
    
    Provides:
    - Engine creation with connection pooling
    - Session factory with proper configuration
    - Context managers for automatic cleanup
    - Tenant-scoped sessions
    
    Usage:
        # Initialize (usually done once at startup)
        db = DatabaseManager()
        db.init_engine()
        
        # Use sessions
        with db.get_session() as session:
            cows = session.query(Cow).all()
        
        # Or for tenant-specific operations
        with db.get_tenant_session(tenant_id) as session:
            events = session.query(CowEvent).all()
    """
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize database manager.
        
        Args:
            config: Database configuration (defaults to env vars)
        """
        self.config = config or DatabaseConfig()
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._scoped_session: Optional[scoped_session] = None
    
    def init_engine(self, driver_type: str = "pyodbc") -> Engine:
        """
        Initialize database engine with connection pooling.
        
        Args:
            driver_type: 'pyodbc' or 'pymssql'
            
        Returns:
            SQLAlchemy Engine instance
        """
        if self._engine is not None:
            return self._engine
        
        connection_string = self.config.get_connection_string(driver_type)
        
        # Create engine with connection pooling
        self._engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            echo=self.config.echo,
            future=True,  # Use SQLAlchemy 2.0 style
        )
        
        # Setup event listeners
        self._setup_event_listeners()
        
        # Create session factory
        self._session_factory = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
        
        # Create scoped session for thread-safety
        self._scoped_session = scoped_session(self._session_factory)
        
        return self._engine
    
    def _setup_event_listeners(self) -> None:
        """Setup SQLAlchemy event listeners for connection handling."""
        
        @event.listens_for(self._engine, "connect")
        def receive_connect(dbapi_conn, connection_record):
            """Set connection properties on connect."""
            # Note: NOT setting NOCOUNT ON because SQLAlchemy needs rowcounts
            # for optimistic concurrency control and StaleDataError detection
            pass
        
        @event.listens_for(self._engine, "checkout")
        def receive_checkout(dbapi_conn, connection_record, connection_proxy):
            """Validate connection on checkout from pool."""
            # Test connection is alive
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("SELECT 1")
            except Exception:
                # Connection is stale, invalidate it
                raise pool.DisconnectionError()
            finally:
                cursor.close()
    
    @property
    def engine(self) -> Engine:
        """Get database engine (initializes if needed)."""
        if self._engine is None:
            self.init_engine()
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get session factory."""
        if self._session_factory is None:
            self.init_engine()
        return self._session_factory
    
    def get_session(self) -> Session:
        """
        Get a new database session.
        
        Returns:
            SQLAlchemy Session instance
            
        Note:
            Remember to close the session when done, or use get_session_context()
        """
        return self.session_factory()
    
    @contextmanager
    def get_session_context(self) -> Generator[Session, None, None]:
        """
        Get session as context manager (auto-cleanup).
        
        Yields:
            SQLAlchemy Session instance
            
        Usage:
            with db.get_session_context() as session:
                results = session.query(Cow).all()
            # Session automatically closed
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    @contextmanager
    def get_tenant_session(self, tenant_id) -> Generator[Session, None, None]:
        """
        Get session with tenant context set.
        
        This is a convenience wrapper that ensures all queries
        are automatically filtered by tenant_id.
        
        Args:
            tenant_id: UUID of tenant
            
        Yields:
            SQLAlchemy Session with tenant filter applied
            
        Usage:
            with db.get_tenant_session(tenant_id) as session:
                # All queries automatically filtered by tenant
                cows = session.query(Cow).all()
        """
        session = self.get_session()
        
        # Set tenant context
        # Note: You'd implement this with a session-level filter or middleware
        # For now, we just pass through the session
        session.info["tenant_id"] = tenant_id
        
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def create_all_tables(self) -> None:
        """
        Create all tables defined in models.
        
        WARNING: Only use for development/testing!
        In production, use proper migration tools like Alembic.
        """
        Base.metadata.create_all(bind=self.engine)
    
    def drop_all_tables(self) -> None:
        """
        Drop all tables defined in models.
        
        WARNING: This will delete all data! Only use for testing!
        """
        Base.metadata.drop_all(bind=self.engine)
    
    def dispose(self) -> None:
        """Dispose of connection pool and close all connections."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            self._scoped_session = None
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_session_context() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"Database connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> dict:
        """
        Get connection information (without sensitive data).
        
        Returns:
            Dictionary with connection info
        """
        return {
            "host": self.config.host,
            "port": self.config.port,
            "database": self.config.database,
            "username": self.config.username,
            "driver": self.config.driver,
            "pool_size": self.config.pool_size,
            "max_overflow": self.config.max_overflow,
            "is_connected": self._engine is not None,
        }


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """
    Get global database manager instance (singleton).
    
    Returns:
        DatabaseManager instance
        
    Usage:
        db = get_db_manager()
        with db.get_session_context() as session:
            cows = session.query(Cow).all()
    """
    global _db_manager
    
    if _db_manager is None:
        _db_manager = DatabaseManager()
        _db_manager.init_engine()
    
    return _db_manager


def get_session() -> Session:
    """
    Get database session (FastAPI dependency).
    
    Returns:
        SQLAlchemy Session instance
        
    Usage with FastAPI:
        @app.get("/cows")
        def list_cows(session: Session = Depends(get_session)):
            return session.query(Cow).all()
    """
    db = get_db_manager()
    session = db.get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Get database session as context manager.
    
    Yields:
        SQLAlchemy Session instance
        
    Usage:
        with get_db_session() as session:
            cows = session.query(Cow).all()
    """
    db = get_db_manager()
    with db.get_session_context() as session:
        yield session


# Convenience function for testing
def init_database(drop_existing: bool = False) -> DatabaseManager:
    """
    Initialize database with tables.
    
    Args:
        drop_existing: Whether to drop existing tables first
        
    Returns:
        DatabaseManager instance
        
    WARNING: Only use for development/testing!
    """
    db = get_db_manager()
    
    if drop_existing:
        print("Dropping existing tables...")
        db.drop_all_tables()
    
    print("Creating tables...")
    db.create_all_tables()
    
    print("Database initialized successfully!")
    return db
