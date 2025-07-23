# app/api/cities.py

import time
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, delete

from app.database import get_db
from app.models import City
from app.schemas import (
    CityCreate, CityUpdate, CityResponse, CityCountryCodeResponse, 
    CityListResponse, MetricsResponse, SearchRequest
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
            
            # Invalidate cache
            await cache.delete(f"city:{city_data.name.lower()}")
            
            logger.info(f"Updated city: {existing_city.name}")
            return existing_city
        else:
            # Create new city
            new_city = City(name=city_data.name, country_code=city_data.country_code)
            db.add(new_city)
            await db.commit()
            await db.refresh(new_city)
            
            logger.info(f"Created new city: {new_city.name}")
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
    Retrieve the country code for a given city, using caching and logging to Kafka.
    """
    start_time = time.time()
    cache_key = f"city:{city_name.lower()}"

    try:
        # 1. Search in cache first
        cached_value = await cache.get(cache_key)
        if cached_value:
            response_time = time.time() - start_time
            kafka.log_request(city_name, response_time, cache_hit=True, status_code=200)
            return CityCountryCodeResponse(country_code=cached_value)

        # 2. If not in cache, search in database
        stmt = select(City).where(func.lower(City.name) == city_name.lower())
        result = await db.execute(stmt)
        db_city = result.scalar_one_or_none()

        if not db_city:
            response_time = time.time() - start_time
            kafka.log_request(city_name, response_time, cache_hit=False, status_code=404)
            raise HTTPException(status_code=404, detail="City not found")

        # 3. Update cache and return
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
    search: Optional[str] = Query(None, description="Search term for city names"),
    country_code: Optional[str] = Query(None, description="Filter by country code")
):
    """
    Retrieve a paginated list of all cities with optional search and filtering.
    """
    try:
        # Calculate offset
        offset = (page - 1) * per_page
        
        # Build base query
        query = select(City)
        count_query = select(func.count()).select_from(City)
        
        # Apply search filter
        if search:
            search_filter = func.lower(City.name).like(f"%{search.lower()}%")
            query = query.where(search_filter)
            count_query = count_query.where(search_filter)
        
        # Apply country code filter
        if country_code:
            country_filter = func.upper(City.country_code) == country_code.upper()
            query = query.where(country_filter)
            count_query = count_query.where(country_filter)
        
        # Get total count
        total_result = await db.execute(count_query)
        total = total_result.scalar_one()
        
        # Get paginated results
        query = query.offset(offset).limit(per_page).order_by(City.name)
        result = await db.execute(query)
        cities = result.scalars().all()
        
        # Calculate total pages
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


@router.delete("/{city_name}", status_code=204)
async def delete_city(
    city_name: str,
    db: AsyncSession = Depends(get_db),
    cache: CacheManager = Depends(get_cache)
):
    """Delete a city by name."""
    try:
        # Delete the city (case-insensitive)
        stmt = delete(City).where(func.lower(City.name) == city_name.lower())
        result = await db.execute(stmt)
        await db.commit()

        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="City not found")

        # Invalidate cache
        await cache.delete(f"city:{city_name.lower()}")
        
        logger.info(f"Deleted city: {city_name}")
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting city {city_name}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics(
    cache: CacheManager = Depends(get_cache),
    kafka: KafkaLogger = Depends(get_kafka_logger)
):
    """Get performance metrics for the application."""
    try:
        # Get cache statistics
        cache_stats = cache.get_stats()
        
        # Calculate cache miss count
        cache_misses = kafka.total_requests - kafka.cache_hits if kafka.total_requests > 0 else 0
        
        # Calculate cache hit percentage
        cache_hit_percentage = (kafka.cache_hits / kafka.total_requests) * 100 if kafka.total_requests > 0 else 0
        
        return MetricsResponse(
            cache_metrics=cache_stats,
            performance_metrics={
                "total_requests": kafka.total_requests,
                "cache_hits": kafka.cache_hits,
                "cache_misses": cache_misses,
                "cache_hit_percentage": round(cache_hit_percentage, 2),
                "uptime_seconds": None  # Can be calculated from app start time if needed
            }
        )
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/info")
async def get_api_info():
    """Get general API information."""
    return {
        "name": "City Service API",
        "version": "1.0.0",
        "description": "FastAPI service for managing cities and country codes with Redis caching and Kafka logging",
        "endpoints": {
            "create_city": "POST /api/v1/cities/",
            "get_country_code": "GET /api/v1/cities/{city_name}/country-code",
            "list_cities": "GET /api/v1/cities/",
            "delete_city": "DELETE /api/v1/cities/{city_name}",
            "metrics": "GET /api/v1/cities/metrics",
            "health": "GET /health"
        }
    }
