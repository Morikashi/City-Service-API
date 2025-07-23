# City Service API - Kafka Logging System
# Async Kafka producer for request logging and performance metrics

import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaTimeoutError

from app.config import settings

# Configure logging
logger = logging.getLogger(__name__)


# =============================================================================
# LOG MESSAGE STRUCTURES
# =============================================================================

@dataclass
class RequestLogMessage:
    """Structure for API request log messages."""
    
    # Request identification
    event_type: str = "api_request"
    timestamp: str = None
    request_id: Optional[str] = None
    
    # Request details
    city_name: str = None
    endpoint: str = None
    method: str = "GET"
    
    # Performance metrics
    response_time_ms: float = None
    status_code: int = None
    
    # Cache metrics
    cache_hit: bool = None
    cache_hit_percentage: float = None
    total_requests: int = None
    
    # Application metrics
    service: str = "city_service"
    version: str = "1.0.0"
    
    def __post_init__(self):
        """Set default timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat() + "Z"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class SystemEventMessage:
    """Structure for system event log messages."""
    
    event_type: str = "system_event"
    timestamp: str = None
    event_name: str = None
    message: str = None
    level: str = "INFO"
    service: str = "city_service"
    version: str = "1.0.0"
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Set default timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat() + "Z"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass 
class ErrorLogMessage:
    """Structure for error log messages."""
    
    event_type: str = "error"
    timestamp: str = None
    error_type: str = None
    error_message: str = None
    stack_trace: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    service: str = "city_service"
    version: str = "1.0.0"
    
    def __post_init__(self):
        """Set default timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat() + "Z"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {k: v for k, v in asdict(self).items() if v is not None}


# =============================================================================
# KAFKA PRODUCER MANAGER
# =============================================================================

class KafkaLogger:
    """
    Async Kafka logger for request tracking and performance monitoring.
    
    Features:
    - Async message publishing for non-blocking performance
    - Connection pooling and automatic reconnection
    - Comprehensive request metrics tracking
    - Error handling and graceful degradation
    - Message serialization and validation
    """
    
    def __init__(self):
        """Initialize Kafka logger with default settings."""
        self.producer: Optional[AIOKafkaProducer] = None
        self.connected = False
        
        # Performance metrics
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.start_time = time.time()
        
        # Message queues for reliability
        self._message_queue = asyncio.Queue(maxsize=10000)
        self._queue_task: Optional[asyncio.Task] = None
        
        # Connection retry settings
        self.max_retries = 5
        self.retry_delay = 5  # seconds
        self.current_retries = 0
    
    async def initialize(self) -> None:
        """
        Initialize Kafka producer with connection pooling and error handling.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"🔌 Kafka connection attempt {attempt}/{self.max_retries}")
                
                # Create Kafka producer with optimized settings
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    
                    # Message serialization
                    value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
                    
                    # Reliability settings
                    acks='all',  # Wait for all replicas to acknowledge
                    enable_idempotence=True,  # Prevent duplicate messages
                    
                    # Performance settings
                    batch_size=16384,  # Batch size in bytes
                    linger_ms=10,      # Wait time for batching
                    buffer_memory=33554432,  # 32MB buffer
                    
                    # Timeout settings
                    request_timeout_ms=30000,
                    delivery_timeout_ms=120000,
                    
                    # Retry settings
                    retries=3,
                    retry_backoff_ms=1000,
                    
                    # Compression for better throughput
                    compression_type='gzip'
                )
                
                # Start the producer
                await self.producer.start()
                
                # Test connection with a ping message
                await self._send_system_event("kafka_initialized", "Kafka producer started successfully")
                
                self.connected = True
                logger.info("✅ Kafka producer initialized successfully")
                
                # Start message queue processor
                self._queue_task = asyncio.create_task(self._process_message_queue())
                
                return
                
            except Exception as e:
                logger.error(f"❌ Kafka connection attempt {attempt} failed: {e}")
                
                if self.producer:
                    try:
                        await self.producer.stop()
                    except:
                        pass
                    self.producer = None
                
                if attempt == self.max_retries:
                    logger.error("💥 All Kafka connection attempts failed - operating in degraded mode")
                    self.connected = False
                    return
                
                logger.info(f"⏳ Retrying Kafka connection in {self.retry_delay} seconds...")
                await asyncio.sleep(self.retry_delay)
    
    async def close(self) -> None:
        """Close Kafka producer and cleanup resources."""
        logger.info("🔐 Closing Kafka producer...")
        
        # Stop queue processor
        if self._queue_task and not self._queue_task.done():
            self._queue_task.cancel()
            try:
                await self._queue_task
            except asyncio.CancelledError:
                pass
        
        # Flush remaining messages
        if self.producer and self.connected:
            try:
                await self.producer.stop()
                logger.info("✅ Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"❌ Error closing Kafka producer: {e}")
        
        self.connected = False
        self.producer = None
    
    def is_healthy(self) -> bool:
        """Check if Kafka producer is healthy and connected."""
        return self.connected and self.producer is not None
    
    async def _send_message(self, topic: str, message: Dict[str, Any]) -> bool:
        """
        Send message to Kafka topic with error handling.
        
        Args:
            topic: Kafka topic name
            message: Message dictionary to send
            
        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.connected or not self.producer:
            logger.warning("⚠️  Kafka not connected - message queued")
            try:
                await self._message_queue.put_nowait((topic, message))
                return True
            except asyncio.QueueFull:
                logger.error("❌ Message queue full - dropping message")
                return False
        
        try:
            # Send message with timeout
            await asyncio.wait_for(
                self.producer.send_and_wait(topic, message),
                timeout=5.0
            )
            return True
            
        except (KafkaTimeoutError, asyncio.TimeoutError):
            logger.error(f"⏰ Kafka send timeout for topic {topic}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Kafka send error for topic {topic}: {e}")
            self.connected = False
            return False
    
    async def _process_message_queue(self) -> None:
        """Process queued messages when Kafka becomes available."""
        while True:
            try:
                # Wait for messages in queue
                topic, message = await self._message_queue.get()
                
                # Try to send message
                if self.connected and self.producer:
                    await self._send_message(topic, message)
                else:
                    # Requeue if still not connected
                    await self._message_queue.put((topic, message))
                    await asyncio.sleep(1)  # Wait before retrying
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error processing message queue: {e}")
                await asyncio.sleep(1)
    
    # =============================================================================
    # REQUEST LOGGING METHODS
    # =============================================================================
    
    def log_request(
        self,
        city_name: str,
        response_time: float,
        cache_hit: bool,
        status_code: int,
        endpoint: str = None,
        method: str = "GET",
        request_id: str = None
    ) -> None:
        """
        Log API request with performance metrics.
        
        Args:
            city_name: Name of city requested
            response_time: Response time in seconds
            cache_hit: Whether request was a cache hit
            status_code: HTTP status code
            endpoint: API endpoint (optional)
            method: HTTP method (default: GET)
            request_id: Unique request ID (optional)
        """
        # Update metrics
        self.total_requests += 1
        if cache_hit:
            self.cache_hits += 1
        else:
            self.cache_misses += 1
        
        # Calculate cache hit percentage
        cache_hit_percentage = (self.cache_hits / self.total_requests) * 100 if self.total_requests > 0 else 0
        
        # Create log message
        log_message = RequestLogMessage(
            city_name=city_name,
            endpoint=endpoint or f"/api/v1/cities/{city_name}/country-code",
            method=method,
            response_time_ms=round(response_time * 1000, 2),
            status_code=status_code,
            cache_hit=cache_hit,
            cache_hit_percentage=round(cache_hit_percentage, 2),
            total_requests=self.total_requests,
            request_id=request_id
        )
        
        # Send to Kafka asynchronously (fire-and-forget)
        asyncio.create_task(
            self._send_message(settings.kafka_topic, log_message.to_dict())
        )
        
        logger.debug(f"📊 Request logged: {city_name}, cache_hit={cache_hit}, time={response_time*1000:.2f}ms")
    
    async def _send_system_event(
        self,
        event_name: str,
        message: str,
        level: str = "INFO",
        metadata: Dict[str, Any] = None
    ) -> None:
        """Send system event message to Kafka."""
        event_message = SystemEventMessage(
            event_name=event_name,
            message=message,
            level=level,
            metadata=metadata
        )
        
        await self._send_message(settings.kafka_topic, event_message.to_dict())
    
    async def log_error(
        self,
        error_type: str,
        error_message: str,
        stack_trace: str = None,
        context: Dict[str, Any] = None
    ) -> None:
        """Log error message to Kafka."""
        error_log = ErrorLogMessage(
            error_type=error_type,
            error_message=error_message,
            stack_trace=stack_trace,
            context=context
        )
        
        await self._send_message(settings.kafka_topic, error_log.to_dict())
        logger.error(f"🚨 Error logged to Kafka: {error_type} - {error_message}")
    
    # =============================================================================
    # METRICS AND MONITORING
    # =============================================================================
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive Kafka logger metrics.
        
        Returns:
            Dictionary with performance and connection metrics
        """
        uptime = time.time() - self.start_time
        cache_hit_percentage = (self.cache_hits / self.total_requests) * 100 if self.total_requests > 0 else 0
        
        return {
            "connection": {
                "status": "connected" if self.connected else "disconnected",
                "healthy": self.is_healthy(),
                "bootstrap_servers": settings.kafka_bootstrap_servers,
                "topic": settings.kafka_topic
            },
            "performance": {
                "total_requests": self.total_requests,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "cache_hit_percentage": round(cache_hit_percentage, 2),
                "uptime_seconds": round(uptime, 2)
            },
            "queue": {
                "queue_size": self._message_queue.qsize(),
                "max_queue_size": self._message_queue.maxsize,
                "queue_task_running": self._queue_task and not self._queue_task.done()
            }
        }
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get detailed health status for monitoring."""
        metrics = self.get_metrics()
        
        # Test connection with a ping
        ping_success = False
        if self.connected and self.producer:
            try:
                await self._send_system_event("health_check", "Health check ping")
                ping_success = True
            except Exception as e:
                logger.debug(f"Health check ping failed: {e}")
        
        return {
            **metrics,
            "health": {
                "overall_status": "healthy" if self.is_healthy() else "unhealthy",
                "connection_test": "passed" if ping_success else "failed",
                "last_check": datetime.utcnow().isoformat() + "Z"
            }
        }
    
    def reset_metrics(self) -> None:
        """Reset performance metrics (for testing purposes)."""
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.start_time = time.time()
        logger.info("📊 Kafka logger metrics reset")


# =============================================================================
# GLOBAL KAFKA LOGGER INSTANCE
# =============================================================================

# Create global Kafka logger instance
kafka_logger = KafkaLogger()


# =============================================================================
# DEPENDENCY INJECTION FUNCTIONS
# =============================================================================

def get_kafka_logger() -> KafkaLogger:
    """
    FastAPI dependency to get Kafka logger instance.
    
    Usage in FastAPI endpoints:
        @app.get("/items/")
        async def get_items(logger: KafkaLogger = Depends(get_kafka_logger)):
            # Use logger here
    """
    return kafka_logger


# =============================================================================
# INITIALIZATION FUNCTIONS
# =============================================================================

async def init_kafka() -> None:
    """
    Initialize Kafka logger.
    Called during application startup.
    """
    await kafka_logger.initialize()


async def close_kafka() -> None:
    """
    Close Kafka connections.
    Called during application shutdown.
    """
    await kafka_logger.close()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

async def log_application_startup() -> None:
    """Log application startup event."""
    await kafka_logger._send_system_event(
        "application_startup",
        "City Service API started successfully",
        metadata={
            "version": settings.app_version,
            "environment": settings.environment,
            "debug_mode": settings.debug
        }
    )


async def log_application_shutdown() -> None:
    """Log application shutdown event."""
    metrics = kafka_logger.get_metrics()
    await kafka_logger._send_system_event(
        "application_shutdown",
        "City Service API shutting down",
        metadata={
            "uptime_seconds": metrics["performance"]["uptime_seconds"],
            "total_requests_processed": metrics["performance"]["total_requests"]
        }
    )


def create_request_logger(endpoint: str, method: str = "GET"):
    """
    Decorator factory for automatic request logging.
    
    Usage:
        @create_request_logger("/api/v1/cities/{city_name}/country-code")
        async def get_city_country_code(city_name: str):
            # Function implementation
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            request_id = f"{int(time.time())}-{hash(func.__name__)}"
            
            try:
                result = await func(*args, **kwargs)
                response_time = time.time() - start_time
                
                # Log successful request
                kafka_logger.log_request(
                    city_name=kwargs.get('city_name', 'unknown'),
                    response_time=response_time,
                    cache_hit=False,  # Will be overridden in actual implementation
                    status_code=200,
                    endpoint=endpoint,
                    method=method,
                    request_id=request_id
                )
                
                return result
                
            except Exception as e:
                response_time = time.time() - start_time
                
                # Log failed request
                await kafka_logger.log_error(
                    error_type=type(e).__name__,
                    error_message=str(e),
                    context={
                        "endpoint": endpoint,
                        "method": method,
                        "request_id": request_id,
                        "response_time_ms": response_time * 1000
                    }
                )
                
                raise
                
        return wrapper
    return decorator
