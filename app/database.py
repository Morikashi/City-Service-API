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
        """Initialize database connection with retries and ensure tables are created."""
        max_retries = 5
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                logger.info(f"🔌 Database connection attempt {attempt + 1}/{max_retries}...")
                
                # Create engine with proper settings
                self.engine = create_async_engine(
                    settings.database_url,
                    echo=settings.debug,
                    pool_pre_ping=True,
                    pool_size=10,
                    max_overflow=20,
                    pool_recycle=3600
                )

                # Create session factory
                self.session_factory = sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False
                )

                # Test connection and create tables
                async with self.engine.begin() as conn:
                    logger.info("🏗️  Creating database tables...")
                    await conn.run_sync(Base.metadata.create_all)
                    logger.info("✅ Database tables created successfully")
                
                # Verify connection with a simple query
                async with self.session_factory() as session:
                    result = await session.execute(text("SELECT 1"))
                    if result.scalar() == 1:
                        logger.info("✅ Database connection verified")
                
                logger.info("🎉 Database initialized successfully")
                return  # Exit the function on success

            except Exception as e:
                logger.warning(f"❌ Database connection attempt {attempt + 1} failed: {e}")
                if self.engine:
                    await self.engine.dispose()
                    self.engine = None
                if attempt + 1 == max_retries:
                    logger.error("💥 All database connection attempts failed. Application cannot start.")
                    raise
                logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)

    async def close(self):
        """Close database connections."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            logger.info("🔌 Database connections closed")

    async def health_check(self) -> bool:
        """Check database health."""
        if not self.engine:
            return False
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                return result.scalar_one_or_none() == 1
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get database session with proper cleanup."""
        if not self.session_factory:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Database session error: {e}", exc_info=True)
                raise
            finally:
                await session.close()

    async def recreate_tables(self):
        """Drop and recreate all tables - use with caution!"""
        if not self.engine:
            raise RuntimeError("Database not initialized")
            
        logger.warning("🗑️  Dropping all tables...")
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            logger.info("✅ All tables dropped")
            
        logger.info("🏗️  Creating all tables...")
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            logger.info("✅ All tables created")


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


async def check_db_connection() -> bool:
    """Check if database connection is healthy."""
    return await db_manager.health_check()


async def ensure_tables_exist():
    """
    Ensure all database tables exist. This is a safety check
    that can be called before running operations that require tables.
    """
    if not db_manager.engine:
        logger.error("Database engine not initialized")
        return False
        
    try:
        async with db_manager.engine.begin() as conn:
            # Check if cities table exists
            result = await conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'cities'
                );
            """))
            
            table_exists = result.scalar()
            
            if not table_exists:
                logger.warning("⚠️  Cities table does not exist, creating it...")
                await conn.run_sync(Base.metadata.create_all)
                logger.info("✅ Database tables created")
            else:
                logger.info("✅ Cities table exists")
                
            return True
            
    except Exception as e:
        logger.error(f"Error checking/creating tables: {e}")
        return False
