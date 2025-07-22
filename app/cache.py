import asyncio
import json
import time
from typing import Optional, Dict, Any, List
from collections import OrderedDict
from dataclasses import dataclass
import redis.asyncio as redis
import logging
from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class CacheItem:
    """Cache item with metadata"""
    value: str
    timestamp: float
    access_count: int = 0


class LRUCache:
    """In-memory LRU cache implementation with TTL support"""
    
    def __init__(self, max_size: int = 10, ttl: int = 600):
        self.max_size = max_size
        self.ttl = ttl
        self.cache: OrderedDict[str, CacheItem] = OrderedDict()
        self.hits = 0
        self.misses = 0
        
    def get(self, key: str) -> Optional[str]:
        """Get value from cache"""
        if key in self.cache:
            item = self.cache[key]
            # Check if item has expired
            if time.time() - item.timestamp > self.ttl:
                del self.cache[key]
                self.misses += 1
                return None
            
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            item.access_count += 1
            self.hits += 1
            return item.value
        
        self.misses += 1
        return None
    
    def set(self, key: str, value: str):
        """Set value in cache"""
        current_time = time.time()
        
        if key in self.cache:
            # Update existing item
            self.cache[key].value = value
            self.cache[key].timestamp = current_time
            self.cache.move_to_end(key)
        else:
            # Add new item
            if len(self.cache) >= self.max_size:
                # Remove least recently used item
                self.cache.popitem(last=False)
            
            self.cache[key] = CacheItem(value, current_time)
    
    def delete(self, key: str):
        """Delete key from cache"""
        if key in self.cache:
            del self.cache[key]
    
    def clear(self):
        """Clear all cache entries"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
    
    def size(self) -> int:
        """Get current cache size"""
        return len(self.cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests) * 100 if total_requests > 0 else 0
        
        return {
            "current_size": len(self.cache),
            "max_size": self.max_size,
            "hit_rate": hit_rate,
            "total_hits": self.hits,
            "total_misses": self.misses,
            "total_requests": total_requests
        }


class RedisCache:
    """Redis-based distributed cache"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.connected = False
        
    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            await self.redis_client.ping()
            self.connected = True
            logger.info("Redis connection established")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            self.connected = False
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis_client:
            await self.redis_client.close()
            self.connected = False
            logger.info("Redis connection closed")
    
    async def get(self, key: str) -> Optional[str]:
        """Get value from Redis"""
        if not self.connected:
            return None
        
        try:
            return await self.redis_client.get(key)
        except Exception as e:
            logger.error(f"Redis get error: {e}")
            return None
    
    async def set(self, key: str, value: str, ttl: int = None):
        """Set value in Redis"""
        if not self.connected:
            return False
        
        try:
            await self.redis_client.set(key, value, ex=ttl or settings.cache_ttl)
            return True
        except Exception as e:
            logger.error(f"Redis set error: {e}")
            return False
    
    async def delete(self, key: str):
        """Delete key from Redis"""
        if not self.connected:
            return False
        
        try:
            await self.redis_client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Redis delete error: {e}")
            return False
    
    async def clear(self):
        """Clear all Redis keys"""
        if not self.connected:
            return False
        
        try:
            await self.redis_client.flushdb()
            return True
        except Exception as e:
            logger.error(f"Redis clear error: {e}")
            return False
    
    async def health_check(self) -> bool:
        """Check Redis health"""
        if not self.connected:
            return False
        
        try:
            await self.redis_client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False


class CacheManager:
    """Combined cache manager with local LRU + Redis"""
    
    def __init__(self):
        self.lru_cache = LRUCache(
            max_size=settings.cache_max_size,
            ttl=settings.cache_ttl
        )
        self.redis_cache = RedisCache()
    
    async def initialize(self):
        """Initialize cache manager"""
        await self.redis_cache.connect()
        logger.info("Cache manager initialized")
    
    async def close(self):
        """Close cache manager"""
        await self.redis_cache.disconnect()
        logger.info("Cache manager closed")
    
    async def get(self, key: str) -> Optional[str]:
        """Get value from cache (LRU first, then Redis)"""
        # Try local LRU cache first
        value = self.lru_cache.get(key)
        if value is not None:
            return value
        
        # Try Redis cache
        value = await self.redis_cache.get(key)
        if value is not None:
            # Update local cache
            self.lru_cache.set(key, value)
            return value
        
        return None
    
    async def set(self, key: str, value: str):
        """Set value in both caches"""
        # Set in local LRU cache
        self.lru_cache.set(key, value)
        
        # Set in Redis cache
        await self.redis_cache.set(key, value)
    
    async def delete(self, key: str):
        """Delete key from both caches"""
        self.lru_cache.delete(key)
        await self.redis_cache.delete(key)
    
    async def clear(self):
        """Clear both caches"""
        self.lru_cache.clear()
        await self.redis_cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return self.lru_cache.get_stats()
    
    async def health_check(self) -> bool:
        """Check cache health"""
        return await self.redis_cache.health_check()


# Global cache manager instance
cache_manager = CacheManager()


async def get_cache() -> CacheManager:
    """Dependency for getting cache manager"""
    return cache_manager


async def init_cache():
    """Initialize cache"""
    await cache_manager.initialize()


async def close_cache():
    """Close cache"""
    await cache_manager.close()
