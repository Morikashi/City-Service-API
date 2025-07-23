# City Service API - Database Management
# Async SQLAlchemy with connection pooling and health monitoring

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

from app.config import settings
from app.models import Base

# Configure logging
logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Async database manager with connection pooling, health checks, and error handling.
    Handles the entire lifecycle of database connections for the application.
    """
    
    def __init__(self):
        """Initialize database manager with empty engine and session factory."""
        self.engine: Optional[AsyncEngine] = None
        self.session_factory: Optional[async_sessionmaker] = None
        self._is_connected = False
    
    async def initialize(self) -> None:
        """
        Initialize database connection with retry logic and connection pooling.
        Creates tables if they don't exist and tests the connection.
        """
        max_retries = 5
        retry_delay = 5  # seconds
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"🔌 Database connection attempt {attempt}/{max_retries}")
                
                # Create async engine with optimized connection pool
                self.engine = create_async_engine(
                    settings.database_url,
                    echo=settings.debug,  # Log SQL in debug mode
                    
                    # Connection pool settings for production
                    poolclass=QueuePool,
                    pool_size=10,           # Number of connections to maintain
                    max_overflow=20,        # Additional connections when pool is full
                    pool_timeout=30,        # Timeout waiting for connection
                    pool_recycle=3600,      # Recycle connections after 1 hour
                    pool_pre_ping=True,     # Validate connections before use
                    
                    # Async engine specific settings
                    future=True,            # Use SQLAlchemy 2.0 style
                )
                
                # Create session factory
                self.session_factory = async_sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False,  # Keep objects accessible after commit
                    autoflush=True,         # Automatically flush before queries
                    autocommit=False        # Explicit transaction management
                )
                
                # Test connection and create tables
                await self._create_tables()
                await self._test_connection()
                
                self._is_connected = True
                logger.info("✅ Database initialized successfully")
                return
                
            except Exception as e:
                logger.error(f"❌ Database connection attempt {attempt} failed: {e}")
                
                # Clean up failed engine
                if self.engine:
                    await self.engine.dispose()
                    self.engine = None
                
                # If this is the last attempt, raise the error
                if attempt == max_retries:
                    logger.error("💥 All database connection attempts failed")
                    raise
                
                # Wait before retry
                logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
    
    async def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        if not self.engine:
            raise RuntimeError("Database engine not initialized")
        
        try:
            async with self.engine.begin() as conn:
                # Create all tables defined in models
                await conn.run_sync(Base.metadata.create_all)
                logger.info("📋 Database tables created/verified")
        
        except Exception as e:
            logger.error(f"❌ Failed to create tables: {e}")
            raise
    
    async def _test_connection(self) -> None:
        """Test database connection with a simple query."""
        if not self.engine or not self.session_factory:
            raise RuntimeError("Database not initialized")
        
        try:
            async with self.session_factory() as session:
                result = await session.execute(text("SELECT 1 as health_check"))
                value = result.scalar_one()
                
                if value != 1:
                    raise RuntimeError("Health check query returned unexpected value")
                
                logger.info("💚 Database connection test successful")
        
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {e}")
            raise
    
    async def close(self) -> None:
        """Gracefully close all database connections."""
        if self.engine:
            try:
                await self.engine.dispose()
                self._is_connected = False
                logger.info("🔐 Database connections closed")
            
            except Exception as e:
                logger.error(f"❌ Error closing database connections: {e}")
            
            finally:
                self.engine = None
                self.session_factory = None
    
    async def health_check(self) -> bool:
        """
        Perform comprehensive database health check.
        Tests connection, query execution, and transaction capabilities.
        """
        if not self._is_connected or not self.session_factory:
            return False
        
        try:
            async with self.session_factory() as session:
                # Test basic query
                result = await session.execute(text("SELECT 1"))
                if result.scalar_one() != 1:
                    return False
                
                # Test transaction capability
                async with session.begin():
                    await session.execute(text("SELECT NOW()"))
                
                return True
        
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            return False
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get database session with automatic transaction management.
        
        Usage:
            async with db_manager.get_session() as session:
                # Use session for queries
                result = await session.execute(query)
                # Transaction is automatically committed
        """
        if not self.session_factory:
            raise RuntimeError("Database not initialized - call initialize() first")
        
        async with self.session_factory() as session:
            try:
                # Start a new transaction
                async with session.begin():
                    yield session
                    # Transaction is automatically committed here
                
            except Exception as e:
                # Transaction is automatically rolled back on exception
                logger.error(f"❌ Database session error: {e}", exc_info=True)
                raise
            
            finally:
                # Session is automatically closed
                pass
    
    @property
    def is_connected(self) -> bool:
        """Check if database manager is connected and ready."""
        return self._is_connected and self.engine is not None
    
    async def execute_raw_sql(self, query: str, params: dict = None) -> any:
        """
        Execute raw SQL query with optional parameters.
        Use with caution - prefer ORM queries when possible.
        """
        if not self.session_factory:
            raise RuntimeError("Database not initialized")
        
        async with self.get_session() as session:
            result = await session.execute(text(query), params or {})
            return result
    
    async def get_connection_info(self) -> dict:
        """Get information about current database connections."""
        if not self.engine:
            return {"status": "disconnected"}
        
        pool = self.engine.pool
        return {
            "status": "connected" if self._is_connected else "disconnected",
            "pool_size": pool.size(),
            "checked_in_connections": pool.checkedin(),
            "checked_out_connections": pool.checkedout(),
            "overflow_connections": pool.overflow(),
            "invalid_connections": pool.invalidated(),
        }


# =============================================================================
# GLOBAL DATABASE MANAGER INSTANCE
# =============================================================================

# Create global database manager instance
db_manager = DatabaseManager()


# =============================================================================
# DEPENDENCY INJECTION FUNCTIONS
# =============================================================================

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency to get database session.
    
    Usage in FastAPI endpoints:
        @app.get("/items/")
        async def get_items(db: AsyncSession = Depends(get_db)):
            # Use db session here
    """
    async with db_manager.get_session() as session:
        yield session


def get_db_manager() -> DatabaseManager:
    """
    FastAPI dependency to get database manager instance.
    Useful for health checks and connection info.
    """
    return db_manager


# =============================================================================
# INITIALIZATION FUNCTIONS
# =============================================================================

async def init_db() -> None:
    """
    Initialize database connection.
    Called during application startup.
    """
    await db_manager.initialize()


async def close_db() -> None:
    """
    Close database connections.
    Called during application shutdown.
    """
    await db_manager.close()


# =============================================================================
# HEALTH CHECK UTILITIES
# =============================================================================

async def check_database_health() -> dict:
    """
    Comprehensive database health check with detailed information.
    Returns status information for monitoring and debugging.
    """
    try:
        # Basic connection test
        is_healthy = await db_manager.health_check()
        
        # Get connection pool information
        connection_info = await db_manager.get_connection_info()
        
        # Test query performance
        start_time = asyncio.get_event_loop().time()
        
        if is_healthy:
            async with db_manager.get_session() as session:
                await session.execute(text("SELECT COUNT(*) FROM information_schema.tables"))
        
        query_time = (asyncio.get_event_loop().time() - start_time) * 1000  # ms
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "connection_info": connection_info,
            "query_response_time_ms": round(query_time, 2) if is_healthy else None,
            "database_url_host": settings.postgres_host,
            "database_name": settings.postgres_db,
        }
    
    except Exception as e:
        logger.error(f"❌ Database health check error: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "connection_info": await db_manager.get_connection_info(),
        }


# =============================================================================
# DATABASE UTILITIES
# =============================================================================

async def reset_database() -> None:
    """
    Reset database by dropping and recreating all tables.
    WARNING: This will delete all data! Use only in development/testing.
    """
    if settings.is_production:
        raise RuntimeError("Cannot reset database in production environment")
    
    logger.warning("⚠️  Resetting database - all data will be lost!")
    
    if not db_manager.engine:
        raise RuntimeError("Database not initialized")
    
    async with db_manager.engine.begin() as conn:
        # Drop all tables
        await conn.run_sync(Base.metadata.drop_all)
        logger.info("🗑️  All tables dropped")
        
        # Recreate all tables
        await conn.run_sync(Base.metadata.create_all)
        logger.info("🏗️  All tables recreated")


async def get_database_stats() -> dict:
    """Get comprehensive database statistics for monitoring."""
    try:
        stats = {}
        
        async with db_manager.get_session() as session:
            # Get table information
            result = await session.execute(text("""
                SELECT 
                    schemaname,
                    tablename,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes
                FROM pg_stat_user_tables
                WHERE tablename = 'cities'
            """))
            
            table_stats = result.mappings().all()
            stats["table_stats"] = [dict(row) for row in table_stats]
            
            # Get database size
            result = await session.execute(text("""
                SELECT pg_size_pretty(pg_database_size(current_database())) as db_size
            """))
            
            stats["database_size"] = result.scalar_one()
            
            return stats
    
    except Exception as e:
        logger.error(f"❌ Error getting database stats: {e}")
        return {"error": str(e)}
