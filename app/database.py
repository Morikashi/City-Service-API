# app/database.py

import asyncio
import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from app.config import settings
from app.models import Base

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.engine: AsyncEngine = None
        self.session_factory: sessionmaker = None

    async def initialize(self):
        """Initialize database connection with retries for robustness."""
        max_retries = 5
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                logger.info(f"🔗 Database connection attempt {attempt + 1}/{max_retries}...")
                
                self.engine = create_async_engine(
                    settings.database_url,
                    echo=settings.debug,
                    pool_pre_ping=True,
                    pool_size=10,
                    max_overflow=20,
                    pool_recycle=3600,  # Recycle connections after 1 hour
                    pool_timeout=30
                )

                # Create session factory
                self.session_factory = sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False
                )

                # Test connection and create tables
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                
                logger.info("✅ Database initialized successfully")
                return  # Exit the function on success

            except Exception as e:
                logger.warning(f"❌ Database connection attempt {attempt + 1} failed: {e}")
                if self.engine:
                    await self.engine.dispose()  # Dispose of the failed engine
                if attempt + 1 == max_retries:
                    logger.error("💥 All database connection attempts failed. Application cannot start.")
                    raise
                logger.info(f"⏳ Retrying database connection in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)

    async def close(self):
        """Close database connections."""
        if self.engine:
            await self.engine.dispose()
            logger.info("✅ Database connections closed")

    async def health_check(self) -> bool:
        """
        Check database health with proper session management.
        FIX: Addresses SQLAlchemy session and connection pool issues.
        """
        if not self.engine:
            logger.error("❌ Database engine not initialized")
            return False

        try:
            # Use a simple connection test instead of session-based approach
            # This avoids transaction conflicts and session management issues
            async with self.engine.connect() as conn:
                # Simple health check query
                result = await conn.execute(text("SELECT 1 as health_check"))
                health_value = result.scalar_one_or_none()
                
                if health_value == 1:
                    logger.debug("✅ Database health check passed")
                    return True
                else:
                    logger.error("❌ Database health check returned unexpected value")
                    return False

        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            return False

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get database session with proper cleanup and error handling."""
        if not self.session_factory:
            logger.error("❌ Database session factory not initialized")
            raise RuntimeError("Database not initialized")

        async with self.session_factory() as session:
            try:
                yield session
                # Only commit if no exception occurred
                if session.in_transaction():
                    await session.commit()
            except Exception as e:
                # Rollback on any exception
                if session.in_transaction():
                    await session.rollback()
                logger.error(f"❌ Database session error: {e}", exc_info=True)
                raise
            finally:
                # Ensure session is properly closed
                await session.close()

    async def get_connection_info(self) -> dict:
        """Get database connection information for monitoring."""
        if not self.engine:
            return {"status": "not_initialized"}

        try:
            pool = self.engine.pool
            return {
                "status": "connected",
                "pool_size": getattr(pool, 'size', lambda: 0)(),
                "checked_in": getattr(pool, 'checkedin', lambda: 0)(),
                "checked_out": getattr(pool, 'checkedout', lambda: 0)(),
                "overflow": getattr(pool, 'overflow', lambda: 0)(),
                "total_connections": getattr(pool, 'size', lambda: 0)() + getattr(pool, 'overflow', lambda: 0)()
            }
        except Exception as e:
            logger.error(f"❌ Error getting connection info: {e}")
            return {"status": "error", "error": str(e)}


# Global database manager instance
db_manager = DatabaseManager()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for getting database session."""
    async with db_manager.get_session() as session:
        yield session


async def init_db():
    """Initialize database."""
    await db_manager.initialize()


async def close_db():
    """Close database."""
    await db_manager.close()


# Additional utility functions
async def check_database_connection() -> bool:
    """Standalone function to check database connectivity."""
    return await db_manager.health_check()


async def get_database_stats() -> dict:
    """Get database statistics for monitoring."""
    health_status = await db_manager.health_check()
    connection_info = await db_manager.get_connection_info()
    
    return {
        "healthy": health_status,
        "connection_info": connection_info,
        "engine_url": str(db_manager.engine.url) if db_manager.engine else "not_initialized"
    }
