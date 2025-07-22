import time
import logging
from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func

from app.database import get_db
from app.models import City
from app.schemas import (
    CityCreate, CityUpdate, CityResponse, CityCountryCodeResponse, 
    CityListResponse, MetricsResponse, CacheMetrics, PerformanceMetrics
)
from app.cache import get_cache, CacheManager
from app.kafka_logger import get_kafka_logger, KafkaLogger

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/", response_model=CityResponse, status_code=201)
async def create_or_update_city(
    city_data: CityCreate,
    db: AsyncSession = Depends(get_db),
    cache: CacheManager = Depends(get_cache)
):
    """
    Create a new city or update an existing one.
    If the city exists, its country code is updated.
    """
    try:
        # Check if city already exists (case-insensitive)
        stmt = select(City).where(func.lower(City.name) == city_data.name.lower())
        result = await db.execute(stmt)
        existing_city = result.scalar_one_or_none()

        if existing_city:
            # Update existing city
            existing_city.country_code = city_data.country_code
            db.add(existing_city)
            await db.commit()
            await db.refresh(existing_city)
            
            # Invalidate cache for updated city
            cache_key = f"city:{existing_city.name.lower()}"
            await cache.delete(cache_key)
            
            logger.info(f"Updated city: {existing_city.name} -> {existing_city.country_code}")
            return existing_city
        else:
            # Create new city
            new_city = City(name=city_data.name, country_code=city_data.country_code)
            db.add(new_city)
            await db.commit()
            await db.refresh(new_city)
            
            logger.info(f"Created new city: {new_city.name} -> {new_city.country_code}")
            return new_city
            
    except Exception as e:
        logger.error(f"Error creating/updating city {city_data.name}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{city_name}/country-code", response_model=CityCountryCodeResponse)
async def get_city_country_code(
    city_name: str,
    db: AsyncSession = Depends(get_db),
    cache: CacheManager = Depends(get_cache),
    kafka: KafkaLogger = Depends(get_kafka_logger)
):
    """
    Retrieve the country code for a given city.
    
    Implementation follows the specified caching strategy:
    1. Search in Redis cache first
    2. If not found, query PostgreSQL 
    3. Update cache with result
    4. Log all requests to Kafka with performance metrics
    """
    start_time = time.time()
    cache_key = f"city:{city_name.lower()}"
    
    try:
        # Step 1: Search in Redis cache
        cached_value = await cache.get(cache_key)
        if cached_value:
            response_time = time.time() - start_time
            kafka.log_request(city_name, response_time, cache_hit=True, status_code=200)
            return CityCountryCodeResponse(country_code=cached_value)

        # Step 2: If not in cache, search in PostgreSQL
        stmt = select(City).where(func.lower(City.name) == city_name.lower())
        result = await db.execute(stmt)
        db_city = result.scalar_one_or_none()

        if not db_city:
            response_time = time.time() - start_time
            kafka.log_request(city_name, response_time, cache_hit=False, status_code=404)
            raise HTTPException(status_code=404, detail=f"City '{city_name}' not found")

        # Step 3: Update cache with result
        await cache.set(cache_key, db_city.country_code)
        
        response_time = time.time() - start_time
        kafka.log_request(city_name, response_time, cache_hit=False, status_code=200)

        return CityCountryCodeResponse(country_code=db_city.country_code)
        
    except HTTPException:
        raise
    except Exception as e:
        response_time = time.time() - start_time
        kafka.log_request(city_name, response_time, cache_hit=False, status_code=500)
        logger.error(f"Error retrieving country code for {city_name}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/", response_model=CityListResponse)
async def list_cities(
    db: AsyncSession = Depends(get_db),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(10, ge=1, le=100, description="Items per page"),
    search: str = Query(None, description="Search cities by name")
):
    """
    Retrieve a paginated list of all cities with optional search.
    """
    try:
        # Calculate offset
        offset = (page - 1) * per_page
        
        # Build query with optional search
        base_query = select(City)
        count_query = select(func.count()).select_from(City)
        
        if search:
            search_pattern = f"%{search.lower()}%"
            base_query = base_query.where(func.lower(City.name).like(search_pattern))
            count_query = count_query.where(func.lower(City.name).like(search_pattern))
        
        # Get total count
        total_result = await db.execute(count_query)
        total = total_result.scalar_one()
        
        # Get paginated cities
        cities_query = base_query.offset(offset).limit(per_page).order_by(City.name)
        cities_result = await db.execute(cities_query)
        cities = cities_result.scalars().all()
        
        total_pages = (total + per_page - 1) // per_page
        
        return CityListResponse(
            cities=cities,
            total=total,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )
        
    except Exception as e:
        logger.error(f"Error listing cities: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics(
    cache: CacheManager = Depends(get_cache),
    kafka: KafkaLogger = Depends(get_kafka_logger)
):
    """
    Get performance metrics including cache statistics and Kafka logging stats.
    """
    try:
        # Get cache statistics
        cache_stats = cache.get_stats()
        cache_metrics = CacheMetrics(
            current_size=cache_stats["current_size"],
            max_size=cache_stats["max_size"],
            hit_rate=cache_stats["hit_rate"],
            total_hits=cache_stats["total_hits"],
            total_misses=cache_stats["total_misses"],
            total_requests=cache_stats["total_requests"]
        )
        
        # Get Kafka performance statistics
        kafka_stats = kafka.get_stats()
        performance_metrics = PerformanceMetrics(
            total_requests=kafka_stats["total_requests"],
            cache_hits=kafka_stats["cache_hits"],
            cache_misses=kafka_stats["cache_misses"],
            cache_hit_percentage=kafka_stats["cache_hit_percentage"],
            uptime_seconds=kafka_stats["uptime_seconds"],
            kafka_healthy=kafka_stats["kafka_healthy"]
        )
        
        return MetricsResponse(
            cache_metrics=cache_metrics,
            performance_metrics=performance_metrics
        )
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/{city_name}")
async def delete_city(
    city_name: str,
    db: AsyncSession = Depends(get_db),
    cache: CacheManager = Depends(get_cache)
):
    """
    Delete a city from the database and cache.
    """
    try:
        # Find city in database
        stmt = select(City).where(func.lower(City.name) == city_name.lower())
        result = await db.execute(stmt)
        city = result.scalar_one_or_none()
        
        if not city:
            raise HTTPException(status_code=404, detail=f"City '{city_name}' not found")
        
        # Delete from database
        await db.delete(city)
        await db.commit()
        
        # Delete from cache
        cache_key = f"city:{city_name.lower()}"
        await cache.delete(cache_key)
        
        logger.info(f"Deleted city: {city.name}")
        return {"message": f"City '{city.name}' deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting city {city_name}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
