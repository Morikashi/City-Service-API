# app/main.py

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import settings
from app.database import init_db, close_db, db_manager, ensure_tables_exist
from app.cache import init_cache, close_cache, cache_manager
from app.kafka_logger import kafka_logger, init_kafka, close_kafka
from app.api.cities import router as cities_router
from app.schemas import HealthResponse, ErrorResponse

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Application startup time
app_start_time = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with proper error handling."""
    # Startup
    logger.info("🚀 Starting City Service API...")
    
    try:
        # Initialize database with retries
        logger.info("📊 Initializing database...")
        await init_db()
        logger.info("✅ Database initialized")
        
        # Ensure all tables exist (safety check)
        await ensure_tables_exist()
        logger.info("✅ Database tables verified")
        
        # Initialize cache
        logger.info("🗄️  Initializing cache...")
        await init_cache()
        logger.info("✅ Cache initialized")
        
        # Initialize Kafka
        logger.info("📨 Initializing Kafka...")
        await init_kafka()
        logger.info("✅ Kafka initialized")
        
        logger.info("🎉 City Service API started successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to start application: {e}")
        logger.error("💥 Application startup failed")
        raise
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down City Service API...")
    
    try:
        # Close Kafka
        await close_kafka()
        logger.info("✅ Kafka closed")
        
        # Close cache
        await close_cache()
        logger.info("✅ Cache closed")
        
        # Close database
        await close_db()
        logger.info("✅ Database closed")
        
        logger.info("🎯 City Service API shutdown complete")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")


# Create FastAPI application
app = FastAPI(
    title=settings.project_name,
    version="1.0.0",
    description="A FastAPI application for managing cities and their country codes with Redis caching and Kafka logging",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all HTTP requests"""
    start_time = time.time()
    
    # Process request
    response = await call_next(request)
    
    # Calculate response time
    response_time = time.time() - start_time
    
    # Log request details
    logger.info(
        f"{request.method} {request.url.path} - "
        f"Status: {response.status_code} - "
        f"Time: {response_time:.4f}s"
    )
    
    return response


# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions"""
    error_data = {"detail": exc.detail, "error_type": "http_exception"}
    return JSONResponse(status_code=exc.status_code, content=error_data)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors"""
    error_data = {
        "detail": "Validation error",
        "error_type": "validation_error",
        "field_errors": exc.errors(),
    }
    return JSONResponse(status_code=422, content=error_data)


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    error_data = {"detail": "Internal server error", "error_type": "general_exception"}
    return JSONResponse(status_code=500, content=error_data)


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": f"Welcome to {settings.project_name}",
        "version": "1.0.0",
        "documentation": "/docs",
        "health": "/health",
        "api_info": "/api/v1/cities/info"
    }


# Health check endpoint
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint that verifies all service dependencies"""
    try:
        start_time = time.time()
        
        # Check database health
        db_health = await db_manager.health_check()
        
        # Check cache health
        cache_health = await cache_manager.health_check()
        
        # Check Kafka health
        kafka_health = kafka_logger.is_healthy()
        
        # Calculate response time
        response_time_ms = (time.time() - start_time) * 1000
        
        # Determine overall status
        if db_health and cache_health and kafka_health:
            status = "healthy"
        elif db_health:  # Database is most critical
            status = "degraded"
        else:
            status = "unhealthy"
        
        return HealthResponse(
            status=status,
            response_time_ms=response_time_ms,
            dependencies={
                "database": "healthy" if db_health else "unhealthy",
                "redis": "healthy" if cache_health else "unhealthy",
                "kafka": "healthy" if kafka_health else "unhealthy",
            }
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")


# Application info endpoint
@app.get("/info")
async def app_info():
    """Get application information"""
    uptime_seconds = time.time() - app_start_time
    return {
        "name": settings.project_name,
        "version": "1.0.0",
        "description": "FastAPI service for managing cities and country codes",
        "uptime_seconds": round(uptime_seconds, 2),
        "environment": "development" if settings.debug else "production",
        "features": {
            "caching": "Redis LRU cache (10 items, 10-minute TTL)",
            "logging": "Apache Kafka message streaming",
            "database": "PostgreSQL with async SQLAlchemy",
            "api_framework": "FastAPI with async/await support"
        },
        "endpoints": {
            "health": "/health",
            "docs": "/docs",
            "cities": "/api/v1/cities/",
            "metrics": "/api/v1/cities/metrics"
        }
    }


# Include API routes
app.include_router(
    cities_router,
    prefix=settings.api_v1_str,
    tags=["cities"]
)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )
