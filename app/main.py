# City Service API - FastAPI Application
# Production-ready FastAPI app with comprehensive lifecycle management

import logging
import time
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import settings, validate_configuration
from app.database import init_db, close_db, db_manager, check_database_health
from app.cache import init_cache, close_cache, cache_manager
from app.kafka_logger import init_kafka, close_kafka, kafka_logger, log_application_startup, log_application_shutdown
from app.api.cities import router as cities_router
from app.schemas import HealthResponse, ErrorResponse, create_error_response

# Configure logging with structured format
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Application startup time for uptime calculation
app_start_time = time.time()


# =============================================================================
# APPLICATION LIFECYCLE MANAGEMENT
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage FastAPI application lifecycle with comprehensive startup/shutdown.
    
    Startup sequence:
    1. Validate configuration
    2. Initialize database with retry logic
    3. Initialize cache system (Redis + local LRU)
    4. Initialize Kafka producer
    5. Log startup completion
    
    Shutdown sequence:
    1. Log shutdown initiation
    2. Close Kafka connections
    3. Close cache connections  
    4. Close database connections
    5. Log shutdown completion
    """
    # =============================================================================
    # STARTUP SEQUENCE
    # =============================================================================
    
    logger.info("🚀 Starting City Service API...")
    startup_start = time.time()
    
    try:
        # Step 1: Configuration validation
        logger.info("⚙️  Validating configuration...")
        validate_configuration()
        
        # Step 2: Database initialization
        logger.info("🗄️  Initializing database...")
        await init_db()
        logger.info("✅ Database initialized successfully")
        
        # Step 3: Cache system initialization
        logger.info("🚀 Initializing cache system...")
        await init_cache()
        logger.info("✅ Cache system initialized successfully")
        
        # Step 4: Kafka producer initialization
        logger.info("📨 Initializing Kafka producer...")
        await init_kafka()
        logger.info("✅ Kafka producer initialized successfully")
        
        # Step 5: Log successful startup
        startup_time = time.time() - startup_start
        logger.info(f"🎉 City Service API started successfully in {startup_time:.2f}s")
        
        # Send startup event to Kafka
        await log_application_startup()
        
        # Yield control to FastAPI application
        yield
        
    except Exception as e:
        logger.error(f"💥 Failed to start City Service API: {e}", exc_info=True)
        raise
    
    # =============================================================================
    # SHUTDOWN SEQUENCE
    # =============================================================================
    
    logger.info("🔄 Shutting down City Service API...")
    shutdown_start = time.time()
    
    try:
        # Step 1: Log shutdown initiation
        await log_application_shutdown()
        
        # Step 2: Close Kafka connections
        logger.info("📨 Closing Kafka connections...")
        await close_kafka()
        logger.info("✅ Kafka connections closed")
        
        # Step 3: Close cache connections
        logger.info("🚀 Closing cache system...")
        await close_cache()
        logger.info("✅ Cache system closed")
        
        # Step 4: Close database connections
        logger.info("🗄️  Closing database connections...")
        await close_db()
        logger.info("✅ Database connections closed")
        
        # Step 5: Final shutdown log
        shutdown_time = time.time() - shutdown_start
        total_uptime = time.time() - app_start_time
        logger.info(f"✨ City Service API shutdown complete in {shutdown_time:.2f}s (uptime: {total_uptime:.2f}s)")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}", exc_info=True)


# =============================================================================
# FASTAPI APPLICATION CREATION
# =============================================================================

app = FastAPI(
    title=settings.project_name,
    version=settings.app_version,
    description="""
    **City Service API** - A production-ready FastAPI microservice for managing cities and their country codes.
    
    ## Features
    
    * **PostgreSQL Database**: Async SQLAlchemy with connection pooling
    * **Redis Caching**: LRU cache with exactly 10 items and 10-minute TTL  
    * **Kafka Logging**: Comprehensive request logging with performance metrics
    * **CSV Data Import**: Bulk import from CSV files with validation
    * **Health Monitoring**: Detailed health checks for all services
    * **Performance Metrics**: Real-time cache hit rates and response times
    
    ## API Endpoints
    
    * **POST /api/v1/cities/**: Create or update cities
    * **GET /api/v1/cities/{city_name}/country-code**: Get country code (with caching)
    * **GET /api/v1/cities/**: List cities with pagination and search
    * **GET /api/v1/cities/metrics**: Performance and cache metrics
    * **DELETE /api/v1/cities/{city_name}**: Delete cities
    * **GET /health**: Comprehensive health check
    """,
    docs_url=settings.docs_url if not settings.is_production else None,
    redoc_url=settings.redoc_url if not settings.is_production else None,
    openapi_url="/openapi.json" if not settings.is_production else None,
    lifespan=lifespan,
    # API metadata
    contact={
        "name": "City Service API Support",
        "email": "support@cityservice.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    tags_metadata=[
        {
            "name": "cities",
            "description": "City management operations with country code associations",
        },
        {
            "name": "health",
            "description": "Health checks and system monitoring",
        },
        {
            "name": "metrics",
            "description": "Performance metrics and cache statistics",
        },
    ]
)


# =============================================================================
# MIDDLEWARE CONFIGURATION
# =============================================================================

# CORS middleware for cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID", "X-Response-Time"],
)


# Request/Response logging and timing middleware
@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """
    Log all HTTP requests with timing and performance metrics.
    Adds request ID and response time headers for debugging.
    """
    start_time = time.time()
    request_id = f"{int(start_time * 1000)}-{hash(str(request.url))}"
    
    # Add request ID to request state
    request.state.request_id = request_id
    
    try:
        # Process the request
        response = await call_next(request)
        
        # Calculate response time
        response_time = time.time() - start_time
        
        # Add headers for debugging
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time"] = f"{response_time * 1000:.2f}ms"
        
        # Log request details
        logger.info(
            f"🌐 {request.method} {request.url.path} - "
            f"Status: {response.status_code} - "
            f"Time: {response_time * 1000:.2f}ms - "
            f"ID: {request_id}"
        )
        
        return response
        
    except Exception as e:
        response_time = time.time() - start_time
        logger.error(
            f"❌ {request.method} {request.url.path} - "
            f"Error: {str(e)} - "
            f"Time: {response_time * 1000:.2f}ms - "
            f"ID: {request_id}",
            exc_info=True
        )
        raise


# =============================================================================
# EXCEPTION HANDLERS
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with structured error responses."""
    request_id = getattr(request.state, 'request_id', None)
    
    # Log to Kafka for monitoring
    if kafka_logger.is_healthy():
        asyncio.create_task(
            kafka_logger.log_error(
                error_type="http_exception",
                error_message=exc.detail,
                context={
                    "status_code": exc.status_code,
                    "path": str(request.url.path),
                    "method": request.method,
                    "request_id": request_id
                }
            )
        )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=create_error_response(
            message=exc.detail,
            error_type="http_exception",
            request_id=request_id
        ).dict(),
        headers={"X-Request-ID": request_id} if request_id else {}
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle Pydantic validation errors with detailed field information."""
    request_id = getattr(request.state, 'request_id', None)
    
    # Extract field errors for detailed response
    field_errors = []
    for error in exc.errors():
        field_errors.append({
            "field": ".".join(str(x) for x in error["loc"]),
            "message": error["msg"],
            "type": error["type"]
        })
    
    # Log validation error to Kafka
    if kafka_logger.is_healthy():
        asyncio.create_task(
            kafka_logger.log_error(
                error_type="validation_error",
                error_message=f"Validation failed for {len(field_errors)} field(s)",
                context={
                    "path": str(request.url.path),
                    "method": request.method,
                    "field_errors": field_errors,
                    "request_id": request_id
                }
            )
        )
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "detail": "Validation failed",
            "error_type": "validation_error",
            "field_errors": field_errors,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "request_id": request_id
        },
        headers={"X-Request-ID": request_id} if request_id else {}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions with error logging and monitoring."""
    request_id = getattr(request.state, 'request_id', None)
    
    # Log the unexpected exception
    logger.error(
        f"💥 Unhandled exception in {request.method} {request.url.path}: {exc}",
        exc_info=True
    )
    
    # Log to Kafka for monitoring
    if kafka_logger.is_healthy():
        asyncio.create_task(
            kafka_logger.log_error(
                error_type="internal_server_error",
                error_message=str(exc),
                stack_trace=str(exc.__traceback__) if exc.__traceback__ else None,
                context={
                    "path": str(request.url.path),
                    "method": request.method,
                    "exception_type": type(exc).__name__,
                    "request_id": request_id
                }
            )
        )
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=create_error_response(
            message="Internal server error occurred",
            error_type="internal_server_error",
            request_id=request_id
        ).dict(),
        headers={"X-Request-ID": request_id} if request_id else {}
    )


# =============================================================================
# ROOT AND UTILITY ENDPOINTS
# =============================================================================

@app.get("/", tags=["root"])
async def root():
    """
    Root endpoint with API information and navigation links.
    Provides quick access to documentation and health status.
    """
    uptime = time.time() - app_start_time
    
    return {
        "service": settings.project_name,
        "version": settings.app_version,
        "status": "operational",
        "uptime_seconds": round(uptime, 2),
        "environment": settings.environment,
        "links": {
            "documentation": f"{settings.docs_url}",
            "health_check": "/health",
            "api_v1": f"{settings.api_v1_str}",
            "metrics": f"{settings.api_v1_str}/cities/metrics"
        },
        "features": [
            "PostgreSQL async database",
            "Redis LRU caching (10 items, 10min TTL)",
            "Kafka request logging",
            "CSV data import",
            "Comprehensive health monitoring"
        ]
    }


@app.get("/info", tags=["root"])
async def info():
    """
    Detailed application information for monitoring and debugging.
    """
    return {
        "application": {
            "name": settings.project_name,
            "version": settings.app_version,
            "environment": settings.environment,
            "debug": settings.debug,
            "started_at": datetime.fromtimestamp(app_start_time).isoformat() + "Z",
            "uptime_seconds": round(time.time() - app_start_time, 2)
        },
        "configuration": {
            "api_v1_prefix": settings.api_v1_str,
            "cache_max_size": settings.cache_max_size,
            "cache_ttl_seconds": settings.cache_ttl,
            "cors_enabled": len(settings.cors_origins) > 0,
            "docs_enabled": settings.docs_url is not None
        },
        "dependencies": {
            "database": "PostgreSQL with async SQLAlchemy",
            "cache": "Redis + Local LRU",
            "messaging": "Apache Kafka",
            "validation": "Pydantic v2",
            "web_framework": "FastAPI"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check():
    """
    Comprehensive health check endpoint for monitoring and load balancers.
    
    Checks the health of:
    - Database connection and query performance
    - Redis cache availability
    - Kafka producer connectivity
    - Overall system status
    
    Returns detailed status information with response times.
    """
    start_time = time.time()
    
    try:
        # Check database health
        db_health_info = await check_database_health()
        db_healthy = db_health_info["status"] == "healthy"
        
        # Check cache health
        cache_healthy = await cache_manager.health_check()
        cache_status = await cache_manager.get_detailed_status()
        
        # Check Kafka health
        kafka_healthy = kafka_logger.is_healthy()
        kafka_status = await kafka_logger.get_health_status()
        
        # Calculate overall health status
        if db_healthy and cache_healthy and kafka_healthy:
            overall_status = "healthy"
        elif db_healthy:
            overall_status = "degraded"  # Can function without cache/kafka
        else:
            overall_status = "unhealthy"  # Cannot function without database
        
        # Calculate response time
        response_time = (time.time() - start_time) * 1000
        
        # Calculate uptime
        uptime = time.time() - app_start_time
        
        health_response = {
            "status": overall_status,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "response_time_ms": round(response_time, 2),
            "uptime_seconds": round(uptime, 2),
            "services": {
                "database": {
                    "status": "healthy" if db_healthy else "unhealthy",
                    "response_time_ms": db_health_info.get("query_response_time_ms"),
                    "details": {
                        "host": settings.postgres_host,
                        "database": settings.postgres_db,
                        "connection_info": db_health_info.get("connection_info", {})
                    }
                },
                "cache": {
                    "status": "healthy" if cache_healthy else "unhealthy",
                    "details": cache_status
                },
                "kafka": {
                    "status": "healthy" if kafka_healthy else "unhealthy",
                    "details": kafka_status.get("connection", {})
                }
            }
        }
        
        # Return appropriate HTTP status code
        status_code = status.HTTP_200_OK if overall_status == "healthy" else status.HTTP_503_SERVICE_UNAVAILABLE
        
        return JSONResponse(
            status_code=status_code,
            content=health_response
        )
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}", exc_info=True)
        
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "response_time_ms": (time.time() - start_time) * 1000,
                "error": str(e),
                "services": {
                    "database": {"status": "unknown"},
                    "cache": {"status": "unknown"},
                    "kafka": {"status": "unknown"}
                }
            }
        )


# =============================================================================
# API ROUTES REGISTRATION
# =============================================================================

# Include cities API router with prefix and tags
app.include_router(
    cities_router,
    prefix=settings.api_v1_str,
    tags=["cities"]
)


# =============================================================================
# MAIN APPLICATION ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Development server configuration
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
        access_log=True,
        reload_dirs=["app"] if settings.debug else None,
    )
