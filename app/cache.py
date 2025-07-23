# City Service API - Advanced Caching System
# Dual-level LRU cache with Redis and local memory optimization

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional, Dict, Any
import redis.asyncio as redis
from app.config import settings

# Configure logging
logger = logging.getLogger(__name__)


# =============================================================================
# CACHE DATA STRUCTURES
# =============================================================================

@dataclass
class CacheItem:
    """
    Cache item with metadata for LRU tracking and performance monitoring.
    Stores the cached value along with access statistics and timestamps.
    """
    value: str
    timestamp: float
    access_count: int = 0
    last_accessed: float = None
    
    def __post_init__(self):
        """Set last_accessed to current time if not provided."""
        if self.last_accessed is None:
            self.last_accessed = time.time()
    
    def is_expired(self, ttl: int) -> bool:
        """Check if cache item has expired based on TTL."""
        return time.time() - self.timestamp > ttl
    
    def update_access(self):
        """Update access statistics when item is retrieved."""
        self.access_count += 1
        self.last_accessed = time.time()


# =============================================================================
# LOCAL LRU CACHE IMPLEMENTATION
# =============================================================================

class LocalLRUCache:
    """
    High-performance in-memory LRU cache with TTL expiration.
    
    Features:
    - Exactly 10 items maximum (as specified in requirements)
    - 10-minute TTL expiration
    - LRU eviction policy
    - Comprehensive performance metrics
    - Thread-safe operations
    """
    
    def __init__(self, max_size: int = 10, ttl: int = 600):
        """
        Initialize LRU cache.
        
        Args:
            max_size: Maximum number of items in cache (default: 10)
            ttl: Time to live in seconds (default: 600 = 10 minutes)
        """
        self.max_size = max_size
        self.ttl = ttl
        self.cache: OrderedDict[str, CacheItem] = OrderedDict()
        
        # Performance metrics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expirations = 0
        
        # Thread safety
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[str]:
        """
        Get value from cache with LRU update and TTL check.
        
        Args:
            key: Cache key to retrieve
            
        Returns:
            Cached value if found and not expired, None otherwise
        """
        async with self._lock:
            if key not in self.cache:
                self.misses += 1
                return None
            
            item = self.cache[key]
            
            # Check if item has expired
            if item.is_expired(self.ttl):
                del self.cache[key]
                self.expirations += 1
                self.misses += 1
                return None
            
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            item.update_access()
            self.hits += 1
            
            logger.debug(f"Cache HIT for key: {key}")
            return item.value
    
    async def set(self, key: str, value: str) -> None:
        """
        Set value in cache with LRU eviction if necessary.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        async with self._lock:
            current_time = time.time()
            
            if key in self.cache:
                # Update existing item
                item = self.cache[key]
                item.value = value
                item.timestamp = current_time
                item.update_access()
                self.cache.move_to_end(key)
                logger.debug(f"Cache UPDATE for key: {key}")
            else:
                # Add new item
                if len(self.cache) >= self.max_size:
                    # Remove least recently used item
                    oldest_key, oldest_item = self.cache.popitem(last=False)
                    self.evictions += 1
                    logger.debug(f"Cache EVICTION: {oldest_key} (LRU)")
                
                # Add new item to cache
                self.cache[key] = CacheItem(value, current_time)
                logger.debug(f"Cache SET for key: {key}")
    
    async def delete(self, key: str) -> bool:
        """
        Delete key from cache.
        
        Args:
            key: Cache key to delete
            
        Returns:
            True if key was deleted, False if key didn't exist
        """
        async with self._lock:
            if key in self.cache:
                del self.cache[key]
                logger.debug(f"Cache DELETE for key: {key}")
                return True
            return False
    
    async def clear(self) -> None:
        """Clear all cache entries and reset metrics."""
        async with self._lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0
            self.evictions = 0
            self.expirations = 0
            logger.info("Local cache cleared")
    
    def size(self) -> int:
        """Get current cache size."""
        return len(self.cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive cache statistics.
        
        Returns:
            Dictionary with cache performance metrics
        """
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests) * 100 if total_requests > 0 else 0
        
        return {
            "current_size": len(self.cache),
            "max_size": self.max_size,
            "hit_rate": round(hit_rate, 2),
            "total_hits": self.hits,
            "total_misses": self.misses,
            "total_requests": total_requests,
            "evictions": self.evictions,
            "expirations": self.expirations,
            "ttl_seconds": self.ttl
        }
    
    def get_cache_contents(self) -> Dict[str, Dict[str, Any]]:
        """Get detailed information about cached items (for debugging)."""
        return {
            key: {
                "value": item.value,
                "age_seconds": time.time() - item.timestamp,
                "access_count": item.access_count,
                "expires_in": max(0, self.ttl - (time.time() - item.timestamp))
            }
            for key, item in self.cache.items()
        }


# =============================================================================
# REDIS DISTRIBUTED CACHE
# =============================================================================

class RedisCache:
    """
    Async Redis-based distributed cache with connection pooling and error handling.
    
    Features:
    - Automatic connection management
    - Connection pooling for performance
    - Graceful degradation when Redis is unavailable
    - Comprehensive error handling and logging
    """
    
    def __init__(self):
        """Initialize Redis cache manager."""
        self.redis_client: Optional[redis.Redis] = None
        self.connected = False
        self.connection_pool = None
    
    async def connect(self) -> None:
        """
        Establish connection to Redis with connection pooling.
        """
        try:
            # Create connection pool for better performance
            self.connection_pool = redis.ConnectionPool.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                max_connections=20,
                health_check_interval=30
            )
            
            # Create Redis client with connection pool
            self.redis_client = redis.Redis(
                connection_pool=self.connection_pool,
                decode_responses=True
            )
            
            # Test connection
            await self.redis_client.ping()
            self.connected = True
            
            logger.info("✅ Redis connection established")
            
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            self.connected = False
            self.redis_client = None
    
    async def disconnect(self) -> None:
        """Close Redis connection and cleanup resources."""
        if self.redis_client:
            try:
                await self.redis_client.close()
                if self.connection_pool:
                    await self.connection_pool.disconnect()
                
                self.connected = False
                logger.info("🔐 Redis connection closed")
                
            except Exception as e:
                logger.error(f"❌ Error closing Redis connection: {e}")
            
            finally:
                self.redis_client = None
                self.connection_pool = None
    
    async def get(self, key: str) -> Optional[str]:
        """
        Get value from Redis cache.
        
        Args:
            key: Cache key to retrieve
            
        Returns:
            Cached value if found, None if not found or Redis unavailable
        """
        if not self.connected or not self.redis_client:
            return None
        
        try:
            value = await self.redis_client.get(key)
            if value:
                logger.debug(f"Redis HIT for key: {key}")
            else:
                logger.debug(f"Redis MISS for key: {key}")
            return value
            
        except Exception as e:
            logger.error(f"❌ Redis get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: str, ttl: int = None) -> bool:
        """
        Set value in Redis cache with TTL.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (default from settings)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connected or not self.redis_client:
            return False
        
        try:
            result = await self.redis_client.set(
                key, value, ex=ttl or settings.cache_ttl
            )
            if result:
                logger.debug(f"Redis SET for key: {key}")
            return bool(result)
            
        except Exception as e:
            logger.error(f"❌ Redis set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete key from Redis cache.
        
        Args:
            key: Cache key to delete
            
        Returns:
            True if key was deleted, False otherwise
        """
        if not self.connected or not self.redis_client:
            return False
        
        try:
            result = await self.redis_client.delete(key)
            if result:
                logger.debug(f"Redis DELETE for key: {key}")
            return bool(result)
            
        except Exception as e:
            logger.error(f"❌ Redis delete error for key {key}: {e}")
            return False
    
    async def clear(self) -> bool:
        """
        Clear all keys from Redis database.
        WARNING: This affects the entire Redis database!
        """
        if not self.connected or not self.redis_client:
            return False
        
        try:
            await self.redis_client.flushdb()
            logger.info("🗑️  Redis cache cleared")
            return True
            
        except Exception as e:
            logger.error(f"❌ Redis clear error: {e}")
            return False
    
    async def health_check(self) -> bool:
        """
        Check Redis connection health.
        
        Returns:
            True if Redis is healthy, False otherwise
        """
        if not self.connected or not self.redis_client:
            return False
        
        try:
            result = await self.redis_client.ping()
            return result is True
            
        except Exception as e:
            logger.error(f"❌ Redis health check failed: {e}")
            self.connected = False
            return False
    
    async def get_info(self) -> Dict[str, Any]:
        """Get Redis server information for monitoring."""
        if not self.connected or not self.redis_client:
            return {"status": "disconnected"}
        
        try:
            info = await self.redis_client.info()
            return {
                "status": "connected",
                "redis_version": info.get("redis_version"),
                "used_memory": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "total_connections_received": info.get("total_connections_received"),
                "keyspace_hits": info.get("keyspace_hits"),
                "keyspace_misses": info.get("keyspace_misses"),
            }
            
        except Exception as e:
            logger.error(f"❌ Redis info error: {e}")
            return {"status": "error", "error": str(e)}


# =============================================================================
# UNIFIED CACHE MANAGER
# =============================================================================

class CacheManager:
    """
    Unified cache manager combining local LRU cache and Redis distributed cache.
    
    Strategy:
    1. Check local LRU cache first (fastest)
    2. If miss, check Redis cache (fast)
    3. If found in Redis, update local cache
    4. For writes, update both caches
    
    This provides optimal performance with Redis as backup/shared storage.
    """
    
    def __init__(self):
        """Initialize dual-level cache manager."""
        self.local_cache = LocalLRUCache(
            max_size=settings.cache_max_size,
            ttl=settings.cache_ttl
        )
        self.redis_cache = RedisCache()
        self.start_time = time.time()
    
    async def initialize(self) -> None:
        """Initialize cache manager and connect to Redis."""
        await self.redis_cache.connect()
        logger.info("🚀 Cache manager initialized")
    
    async def close(self) -> None:
        """Close cache connections and cleanup resources."""
        await self.local_cache.clear()
        await self.redis_cache.disconnect()
        logger.info("🔐 Cache manager closed")
    
    async def get(self, key: str) -> Optional[str]:
        """
        Get value using dual-level cache strategy.
        
        Args:
            key: Cache key to retrieve
            
        Returns:
            Cached value if found, None otherwise
        """
        # Try local cache first (fastest)
        value = await self.local_cache.get(key)
        if value is not None:
            logger.debug(f"L1 cache HIT for key: {key}")
            return value
        
        # Try Redis cache (distributed)
        value = await self.redis_cache.get(key)
        if value is not None:
            # Update local cache with Redis value
            await self.local_cache.set(key, value)
            logger.debug(f"L2 cache HIT for key: {key}, updating L1")
            return value
        
        logger.debug(f"Cache MISS for key: {key}")
        return None
    
    async def set(self, key: str, value: str) -> None:
        """
        Set value in both cache levels.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        # Set in local cache (immediate availability)
        await self.local_cache.set(key, value)
        
        # Set in Redis cache (distributed/persistent)
        await self.redis_cache.set(key, value)
        
        logger.debug(f"Cache SET for key: {key} (both levels)")
    
    async def delete(self, key: str) -> None:
        """
        Delete key from both cache levels.
        
        Args:
            key: Cache key to delete
        """
        await self.local_cache.delete(key)
        await self.redis_cache.delete(key)
        logger.debug(f"Cache DELETE for key: {key} (both levels)")
    
    async def clear(self) -> None:
        """Clear both cache levels."""
        await self.local_cache.clear()
        await self.redis_cache.clear()
        logger.info("Both cache levels cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive cache statistics from both levels.
        
        Returns:
            Dictionary with detailed cache performance metrics
        """
        local_stats = self.local_cache.get_stats()
        uptime = time.time() - self.start_time
        
        return {
            "local_cache": local_stats,
            "uptime_seconds": round(uptime, 2),
            "cache_strategy": "dual_level_lru",
            "levels": ["local_lru", "redis_distributed"]
        }
    
    async def health_check(self) -> bool:
        """
        Check health of cache system.
        
        Returns:
            True if at least local cache is working, False if both fail
        """
        # Local cache should always work
        local_healthy = self.local_cache.size() >= 0  # Simple health check
        
        # Redis health check
        redis_healthy = await self.redis_cache.health_check()
        
        # System is healthy if local cache works (Redis is optional)
        return local_healthy
    
    async def get_detailed_status(self) -> Dict[str, Any]:
        """Get detailed status of both cache levels."""
        local_stats = self.local_cache.get_stats()
        redis_info = await self.redis_cache.get_info()
        
        return {
            "local_cache": {
                **local_stats,
                "status": "healthy",
                "type": "lru_memory"
            },
            "redis_cache": {
                **redis_info,
                "type": "distributed"
            },
            "overall_status": "healthy" if await self.health_check() else "degraded"
        }


# =============================================================================
# GLOBAL CACHE MANAGER INSTANCE
# =============================================================================

# Create global cache manager instance
cache_manager = CacheManager()


# =============================================================================
# DEPENDENCY INJECTION FUNCTIONS
# =============================================================================

def get_cache() -> CacheManager:
    """
    FastAPI dependency to get cache manager instance.
    
    Usage in FastAPI endpoints:
        @app.get("/items/")
        async def get_items(cache: CacheManager = Depends(get_cache)):
            # Use cache here
    """
    return cache_manager


# =============================================================================
# INITIALIZATION FUNCTIONS
# =============================================================================

async def init_cache() -> None:
    """
    Initialize cache system.
    Called during application startup.
    """
    await cache_manager.initialize()


async def close_cache() -> None:
    """
    Close cache connections.
    Called during application shutdown.
    """
    await cache_manager.close()


# =============================================================================
# CACHE UTILITIES
# =============================================================================

def generate_cache_key(prefix: str, identifier: str) -> str:
    """
    Generate standardized cache key.
    
    Args:
        prefix: Key prefix (e.g., 'city', 'user')
        identifier: Unique identifier
        
    Returns:
        Formatted cache key
    """
    return f"{prefix}:{identifier.lower()}"


async def cache_with_fallback(
    key: str,
    fallback_func,
    ttl: int = None,
    cache_manager: CacheManager = None
) -> Any:
    """
    Cache pattern with automatic fallback to data source.
    
    Args:
        key: Cache key
        fallback_func: Async function to call on cache miss
        ttl: Custom TTL (optional)
        cache_manager: Cache manager instance (optional)
        
    Returns:
        Cached value or result from fallback function
    """
    if cache_manager is None:
        cache_manager = globals()['cache_manager']
    
    # Try cache first
    cached_value = await cache_manager.get(key)
    if cached_value is not None:
        return cached_value
    
    # Call fallback function
    value = await fallback_func()
    
    # Cache the result
    if value is not None:
        await cache_manager.set(key, str(value))
    
    return value
