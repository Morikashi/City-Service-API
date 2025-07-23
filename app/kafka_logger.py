# app/kafka_logger.py

import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from datetime import datetime

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class RequestLogMessage:
    """Schema for API request log messages"""
    event_type: str
    timestamp: str
    city_name: str
    response_time_ms: float
    cache_hit: bool
    status_code: int
    cache_hit_percentage: float
    total_requests: int
    service_name: str = "city_service"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SystemEventMessage:
    """Schema for system event log messages"""
    event_type: str
    timestamp: str
    service_name: str
    message: str
    data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> dict:
        return asdict(self)


class KafkaLogger:
    """Kafka logger with improved error handling and metrics tracking"""

    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_healthy_flag: bool = False
        self.total_requests: int = 0
        self.cache_hits: int = 0
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._background_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    async def initialize_producer(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 5
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                logger.info(f"🔌 Kafka connection attempt {attempt + 1}/{max_retries}")
                
                # FIX: Removed invalid 'buffer_memory' parameter
                # Only use valid parameters for current aiokafka version
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    max_batch_size=16384,  # This is the correct parameter name
                    linger_ms=50,          # Batch for better throughput
                    request_timeout_ms=30000,
                    retry_backoff_ms=100
                )
                
                await self.producer.start()
                self.is_healthy_flag = True
                
                # Start background message processor
                self._background_task = asyncio.create_task(self._process_message_queue())
                
                logger.info("✅ Kafka producer started successfully")
                return

            except Exception as e:
                logger.error(f"❌ Kafka connection attempt {attempt + 1} failed: {e}")
                if self.producer:
                    try:
                        await self.producer.stop()
                    except:
                        pass
                    self.producer = None
                
                if attempt + 1 == max_retries:
                    logger.error("💥 All Kafka connection attempts failed - operating in degraded mode")
                    self.is_healthy_flag = False
                    return
                
                logger.info(f"⏳ Retrying Kafka connection in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)

    async def close_producer(self):
        """Gracefully close Kafka producer"""
        logger.info("🔄 Shutting down Kafka logger...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Cancel background task
        if self._background_task and not self._background_task.done():
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass

        # Close producer
        if self.producer:
            try:
                await self.producer.stop()
                logger.info("✅ Kafka producer stopped successfully")
            except Exception as e:
                logger.error(f"❌ Error stopping Kafka producer: {e}")
            finally:
                self.producer = None

        self.is_healthy_flag = False
        logger.info("✅ Kafka logger shutdown complete")

    def is_healthy(self) -> bool:
        """Check if Kafka producer is healthy"""
        return self.is_healthy_flag and self.producer is not None

    async def _process_message_queue(self):
        """Background task to process queued messages"""
        logger.info("🚀 Kafka message processor started")
        
        while not self._shutdown_event.is_set():
            try:
                # Wait for messages with timeout
                try:
                    topic, message = await asyncio.wait_for(
                        self._message_queue.get(), 
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                if self.is_healthy():
                    try:
                        await self.producer.send_and_wait(topic, message)
                    except Exception as e:
                        logger.error(f"❌ Failed to send message to Kafka: {e}")
                        self.is_healthy_flag = False
                else:
                    logger.warning("⚠️  Kafka not connected - discarding message")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error in message processor: {e}")

        logger.info("🛑 Kafka message processor stopped")

    async def _send_message(self, topic: str, message: Dict[str, Any]):
        """Queue message for sending"""
        if not self._shutdown_event.is_set():
            try:
                # Use put_nowait (synchronous) - don't await it
                self._message_queue.put_nowait((topic, message))
            except asyncio.QueueFull:
                logger.warning("⚠️  Kafka message queue full - dropping message")
        else:
            logger.warning("⚠️  Kafka not connected - message queued")

    def log_request(self, city_name: str, response_time: float, cache_hit: bool, status_code: int):
        """Log API request with performance metrics"""
        self.total_requests += 1
        if cache_hit:
            self.cache_hits += 1

        cache_hit_percentage = (self.cache_hits / self.total_requests) * 100 if self.total_requests > 0 else 0

        log_message = RequestLogMessage(
            event_type="api_request",
            timestamp=datetime.utcnow().isoformat(),
            city_name=city_name,
            response_time_ms=response_time * 1000,  # Convert to milliseconds
            cache_hit=cache_hit,
            status_code=status_code,
            cache_hit_percentage=round(cache_hit_percentage, 2),
            total_requests=self.total_requests
        )

        # Queue message for background sending
        asyncio.create_task(self._send_message(settings.kafka_topic, log_message.to_dict()))

    async def _send_system_event(self, event_message: SystemEventMessage):
        """Send system event message"""
        await self._send_message(settings.kafka_topic, event_message.to_dict())

    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        cache_hit_percentage = (self.cache_hits / self.total_requests) * 100 if self.total_requests > 0 else 0
        
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.total_requests - self.cache_hits,
            "cache_hit_percentage": round(cache_hit_percentage, 2),
            "kafka_healthy": self.is_healthy(),
            "queue_size": self._message_queue.qsize()
        }


# Global instances
kafka_logger = KafkaLogger()


async def init_kafka():
    """Initialize Kafka logger"""
    await kafka_logger.initialize_producer()


async def close_kafka():
    """Close Kafka logger"""
    await kafka_logger.close_producer()


def get_kafka_logger() -> KafkaLogger:
    """Get Kafka logger instance for dependency injection"""
    return kafka_logger


async def log_application_startup():
    """Log application startup event"""
    startup_message = SystemEventMessage(
        event_type="system_startup",
        timestamp=datetime.utcnow().isoformat(),
        service_name="city_service",
        message="City Service API started successfully",
        data={
            "version": getattr(settings, 'app_version', '1.0.0'),
            "environment": getattr(settings, 'debug', False)
        }
    )
    
    await kafka_logger._send_system_event(startup_message)


async def log_application_shutdown():
    """Log application shutdown event"""
    shutdown_message = SystemEventMessage(
        event_type="system_shutdown",
        timestamp=datetime.utcnow().isoformat(),
        service_name="city_service",
        message="City Service API shutting down",
        data={
            "total_requests_processed": kafka_logger.total_requests,
            "final_cache_hit_percentage": kafka_logger.get_metrics()["cache_hit_percentage"]
        }
    )
    
    await kafka_logger._send_system_event(shutdown_message)
