import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import settings
from app.database import init_db, close_db, db_manager
from app.cache import init_cache, close_cache, cache_manager
from app.kafka_logger import init_kafka, close_kafka, kafka_logger
from app.api.cities import router as cities_router
from app.schemas import HealthResponse

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
    """Application lifespan manager for startup and shutdown events"""
    # Startup
    logger.info("🚀 Starting City Service API...")
    
    try:
        # Initialize database
        await init_db()
        logger.info("✅ Database initialized")
        
        # Initialize cache
        await init_cache()
        logger.info("✅ Cache initialized")
        
        # Initialize Kafka
        await init_kafka()
        logger.info("✅ Kafka initialized")
        
        # Log system startup to Kafka
        kafka_logger.log_system_event(
            event_type="system_startup",
            message="City Service API started successfully",
            data={"version": settings.app_version}
        )
        
        logger.info("🎉 City Service API started successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to start application: {e}")
        raise
    
    yield  # Application is running
    
    # Shutdown
    logger.info("🔄 Shutting down City Service API...")
    
    try:
        # Log shutdown event
        kafka_logger.log_system_event(
            event_type="system_shutdown",
            message="City Service API shutting down gracefully"
        )
        
        # Close Kafka
        await close_kafka()
        logger.info("✅ Kafka closed")
        
        # Close cache
        await close_cache()
        logger.info("✅ Cache closed")
        
        # Close database
        await close_db()
        logger.info("✅ Database closed")
        
        logger.info("👋 City Service API shutdown complete")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")


# Create FastAPI application
app = FastAPI(
    title=settings.project_name,
    version=settings.app_version,
    description="A production-ready FastAPI application for managing cities and their country codes with advanced caching, logging, and CSV data integration.",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all HTTP requests with timing"""
    start_time = time.time()
    
    # Process request
    response = await call_next(request)
    
    # Calculate response time
    response_time = time.time() - start_time
    
    # Log request details
    logger.info(
        f"{request.method} {request.url.path} - "
        f"Status: {response.status_code} - "
        f"Time: {response_time:.4f}s - "
        f"Client: {request.client.host if request.client else 'unknown'}"
    )
    
    return response


# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions"""
    error_data = {
        "detail": exc.detail,
        "error_type": "http_exception",
        "status_code": exc.status_code,
        "path": str(request.url.path),
        "method": request.method,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.warning(f"HTTP {exc.status_code}: {exc.detail} - {request.method} {request.url.path}")
    return JSONResponse(status_code=exc.status_code, content=error_data)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors"""
    error_data = {
        "detail": "Validation error",
        "error_type": "validation_error",
        "field_errors": exc.errors(),
        "path": str(request.url.path),
        "method": request.method,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.warning(f"Validation error: {exc.errors()} - {request.method} {request.url.path}")
    return JSONResponse(status_code=422, content=error_data)


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions"""
    error_data = {
        "detail": "Internal server error",
        "error_type": "general_exception",
        "path": str(request.url.path),
        "method": request.method,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.error(f"Unhandled exception: {exc} - {request.method} {request.url.path}", exc_info=True)
    return JSONResponse(status_code=500, content=error_data)


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    uptime = time.time() - app_start_time
    return {
        "message": f"Welcome to {settings.project_name}",
        "version": settings.app_version,
        "uptime_seconds": round(uptime, 2),
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "cities": f"{settings.api_v1_str}/cities",
            "metrics": f"{settings.api_v1_str}/cities/metrics"
        }
    }


# Health check endpoint
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Comprehensive health check endpoint.
    
    Checks the health of all critical dependencies:
    - PostgreSQL database
    - Redis cache  
    - Kafka producer
    """
    start_time = time.time()
    
    try:
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
        elif db_health:  # Database is critical, others can be degraded
            status = "degraded"
        else:
            status = "unhealthy"
        
        health_data = {
            "status": status,
            "response_time_ms": round(response_time_ms, 2),
            "dependencies": {
                "database": "healthy" if db_health else "unhealthy",
                "redis": "healthy" if cache_health else "unhealthy", 
                "kafka": "healthy" if kafka_health else "unhealthy",
            },
            "uptime_seconds": round(time.time() - app_start_time, 2),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Return appropriate HTTP status code
        if status == "healthy":
            return JSONResponse(status_code=200, content=health_data)
        elif status == "degraded":
            return JSONResponse(status_code=200, content=health_data)  # Still operational
        else:
            return JSONResponse(status_code=503, content=health_data)  # Service unavailable
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        error_data = {
            "status": "unhealthy",
            "response_time_ms": round((time.time() - start_time) * 1000, 2),
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
        return JSONResponse(status_code=503, content=error_data)


# Include API routes
app.include_router(
    cities_router,
    prefix=settings.api_v1_str + "/cities",
    tags=["Cities"]
)


# Additional info endpoint for debugging
@app.get("/info")
async def app_info():
    """Get application information and configuration"""
    return {
        "app_name": settings.project_name,
        "version": settings.app_version,
        "debug_mode": settings.debug,
        "database_url": settings.database_url.split("@")[0] + "@***",  # Hide sensitive parts
        "redis_url": settings.redis_url,
        "kafka_servers": settings.kafka_bootstrap_servers,
        "cache_config": {
            "max_size": settings.cache_max_size,
            "ttl_seconds": settings.cache_ttl
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )
