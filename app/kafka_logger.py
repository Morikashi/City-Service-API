import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any
from datetime import datetime

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config import settings

logger = logging.getLogger(__name__)


class KafkaLogger:
    """A logger that sends messages to a Kafka topic with performance tracking"""

    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_healthy_flag = False
        self.total_requests = 0
        self.cache_hits = 0
        self.start_time = time.time()

    async def initialize_producer(self):
        """Initialize the Kafka producer connection"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            await self.producer.start()
            self.is_healthy_flag = True
            logger.info("Kafka producer started successfully.")
        except KafkaConnectionError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.is_healthy_flag = False
            self.producer = None
        except Exception as e:
            logger.error(f"An unexpected error occurred during Kafka initialization: {e}")
            self.is_healthy_flag = False
            self.producer = None
            raise

    async def close_producer(self):
        """Gracefully close the Kafka producer"""
        if self.producer:
            try:
                await self.producer.stop()
                logger.info("Kafka producer stopped successfully.")
            except Exception as e:
                logger.error(f"Error stopping Kafka producer: {e}")
            finally:
                self.producer = None
        self.is_healthy_flag = False

    def is_healthy(self) -> bool:
        """Check if the Kafka producer is connected and healthy"""
        return self.is_healthy_flag and self.producer is not None

    async def send_log(self, topic: str, message: Dict[str, Any]):
        """Send a log message to a specific Kafka topic"""
        if not self.is_healthy():
            logger.warning("Kafka producer is not available. Log message was not sent.")
            return

        try:
            await self.producer.send_and_wait(topic, message)
        except Exception as e:
            logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
            self.is_healthy_flag = False

    def log_request(self, city_name: str, response_time: float, cache_hit: bool, status_code: int):
        """
        Log an API request for a city's country code with required metrics:
        - Request response time
        - Request cache hit/miss 
        - Cache hit percentage from the beginning of execution
        """
        self.total_requests += 1
        if cache_hit:
            self.cache_hits += 1

        cache_hit_percentage = (self.cache_hits / self.total_requests) * 100 if self.total_requests > 0 else 0
        uptime = time.time() - self.start_time

        log_message = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "api_request",
            "service": "city_service",
            "city_name": city_name,
            "response_time_ms": round(response_time * 1000, 2),
            "cache_hit": cache_hit,
            "status_code": status_code,
            "cache_hit_percentage": round(cache_hit_percentage, 2),
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.total_requests - self.cache_hits,
            "uptime_seconds": round(uptime, 2)
        }

        # Send log message asynchronously without blocking the request
        asyncio.create_task(self.send_log(settings.kafka_topic, log_message))

    def log_system_event(self, event_type: str, message: str, data: Dict[str, Any] = None):
        """Log system events like startup, shutdown, errors"""
        log_message = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "service": "city_service",
            "message": message,
            "data": data or {}
        }
        
        asyncio.create_task(self.send_log(settings.kafka_topic, log_message))

    def get_stats(self) -> Dict[str, Any]:
        """Get current logging and performance statistics"""
        cache_hit_percentage = (self.cache_hits / self.total_requests) * 100 if self.total_requests > 0 else 0
        uptime = time.time() - self.start_time
        
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.total_requests - self.cache_hits,
            "cache_hit_percentage": round(cache_hit_percentage, 2),
            "uptime_seconds": round(uptime, 2),
            "kafka_healthy": self.is_healthy()
        }


# Global Kafka logger instance
kafka_logger = KafkaLogger()


async def init_kafka():
    """Initialize the global Kafka logger"""
    await kafka_logger.initialize_producer()


async def close_kafka():
    """Close the global Kafka logger"""
    await kafka_logger.close_producer()


def get_kafka_logger() -> KafkaLogger:
    """Dependency for getting the Kafka logger instance"""
    return kafka_logger
