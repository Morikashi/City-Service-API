from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy import text
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import logging
from app.config import settings
from app.models import Base

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Database manager for PostgreSQL operations"""
    
    def __init__(self):
        self.engine: AsyncEngine = None
        self.session_factory: sessionmaker = None
        
    async def initialize(self):
        """Initialize database connection and create tables"""
        try:
            # Create async engine with connection pooling
            self.engine = create_async_engine(
                settings.database_url,
                echo=settings.debug,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                poolclass=StaticPool if "sqlite" in settings.database_url else None
            )
            
            # Create session factory
            self.session_factory = sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Create tables
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def close(self):
        """Close database connections"""
        if self.engine:
            await self.engine.dispose()
            logger.info("Database connections closed")
    
    async def health_check(self) -> bool:
        """Check database health"""
        try:
            async with self.session_factory() as session:
                result = await session.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get database session with proper cleanup"""
        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Database session error: {e}")
                raise
            finally:
                await session.close()


# Global database manager instance
db_manager = DatabaseManager()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for getting database session"""
    async with db_manager.get_session() as session:
        yield session


async def init_db():
    """Initialize database"""
    await db_manager.initialize()


async def close_db():
    """Close database"""
    await db_manager.close()
