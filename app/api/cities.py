# City Service API - Cities API Endpoints
# CRUD operations with caching and performance logging

import logging
import time
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, delete

from app.database import get_db
from app.models import City
from app.schemas import (
    CityCreate,
    CityResponse,
    CityCountryCodeResponse,
    CityListResponse,
    BulkCityCreate,
    BulkOperationResponse,
    SearchParams,
)
from app.cache import get_cache, CacheManager, generate_cache_key
from app.kafka_logger import get_kafka_logger, KafkaLogger

# Configure logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def _get_city_from_db(session: AsyncSession, city_name: str) -> City | None:
    stmt = select(City).where(func.lower(City.name) == city_name.lower())
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


# =============================================================================
# API ENDPOINTS
# =============================================================================

@router.post("/", response_model=CityResponse, status_code=status.HTTP_201_CREATED)
async def create_or_update_city(
    city_data: CityCreate,
    db: AsyncSession = Depends(get_db),
    cache: CacheManager = Depends(get_cache)
):
    """Create a new city or update an existing one."""
    existing_city = await _get_city_from_db(db, city_data.name)

    if existing_city:
        existing_city.country_code = city_data.country_code
        db.add(existing_city)
        await db.commit()
        await db.refresh(existing_city)

        # Invalidate cache
        await cache.delete(generate_cache_key("city", existing_city.name))
        return existing_city

    new_city = City(name=city_data.name, country_code=city_data.country_code)
    db.add(new_city)
    await db.commit()
    await db.refresh(new_city)
    return new_city


@router.get("/{city_name}/country-code", response_model=CityCountryCodeResponse)
async def get_country_code(
    city_name: str,
    db: AsyncSession = Depends(get_db),
    cache: CacheManager = Depends(get_cache),
    kafka: KafkaLogger = Depends(get_kafka_logger)
):
    """Retrieve the country code for a given city, using cache with fallback."""
    start_time = time.time()
    cache_key = generate_cache_key("city", city_name)

    # Try cache first
    cached_value = await cache.get(cache_key)
    if cached_value:
        kafka.log_request(city_name, time.time() - start_time, cache_hit=True, status_code=200)
        return CityCountryCodeResponse(country_code=cached_value)

    # Fallback to database
    city = await _get_city_from_db(db, city_name)
    if city is None:
        kafka.log_request(city_name, time.time() - start_time, cache_hit=False, status_code=404)
        raise HTTPException(status_code=404, detail="City not found")

    # Update cache
    await cache.set(cache_key, city.country_code)
    kafka.log_request(city_name, time.time() - start_time, cache_hit=False, status_code=200)
    return CityCountryCodeResponse(country_code=city.country_code)


@router.get("/", response_model=CityListResponse)
async def list_cities(
    params: SearchParams = Depends(),
    db: AsyncSession = Depends(get_db)
):
    """List cities with pagination and optional search/filter."""
    offset = (params.page - 1) * params.per_page

    stmt = select(City)
    count_stmt = select(func.count()).select_from(City)

    # Apply search filter
    if params.search:
        search_filter = City.get_search_filter(params.search)
        stmt = stmt.where(search_filter)
        count_stmt = count_stmt.where(search_filter)

    # Apply country filter
    if params.country_code:
        country_filter = City.get_country_filter(params.country_code)
        stmt = stmt.where(country_filter)
        count_stmt = count_stmt.where(country_filter)

    # Execute count query
    total = (await db.execute(count_stmt)).scalar_one()

    # Execute paginated query
    stmt = stmt.offset(offset).limit(params.per_page)
    cities = (await db.execute(stmt)).scalars().all()

    total_pages = (total + params.per_page - 1) // params.per_page

    return {
        "cities": cities,
        "total": total,
        "page": params.page,
        "per_page": params.per_page,
        "total_pages": total_pages,
        "has_next": params.page < total_pages,
        "has_prev": params.page > 1
    }


@router.delete("/{city_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_city(
    city_name: str,
    db: AsyncSession = Depends(get_db),
    cache: CacheManager = Depends(get_cache)
):
    """Delete a city by name."""
    stmt = delete(City).where(func.lower(City.name) == city_name.lower())
    result = await db.execute(stmt)
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="City not found")

    await cache.delete(generate_cache_key("city", city_name))
    return None


@router.post("/bulk", response_model=BulkOperationResponse)
async def bulk_create_cities(
    payload: BulkCityCreate,
    db: AsyncSession = Depends(get_db),
    cache: CacheManager = Depends(get_cache)
):
    """Bulk create or update cities from payload."""
    start_time = time.time()
    created, updated, errors = 0, 0, []

    for city in payload.cities:
        try:
            existing = await _get_city_from_db(db, city.name)
            if existing:
                if payload.update_existing:
                    existing.country_code = city.country_code
                    db.add(existing)
                    updated += 1
                    await cache.delete(generate_cache_key("city", existing.name))
            else:
                db.add(City(name=city.name, country_code=city.country_code))
                created += 1
        except Exception as e:
            errors.append({"city": city.name, "error": str(e)})
            logger.error(f"Error processing city {city.name}: {e}")

    await db.commit()

    return {
        "created": created,
        "updated": updated,
        "errors": errors,
        "total_processed": created + updated + len(errors),
        "processing_time_seconds": round(time.time() - start_time, 2)
    }


@router.get("/metrics", tags=["metrics"])
async def get_metrics(
    cache: CacheManager = Depends(get_cache),
    kafka: KafkaLogger = Depends(get_kafka_logger)
):
    """Return cache and Kafka performance metrics."""
    return {
        "cache_metrics": cache.get_stats(),
        "performance_metrics": kafka.get_metrics(),
        "timestamp": datetime.utcnow()
    }
