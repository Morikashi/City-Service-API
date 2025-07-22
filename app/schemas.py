from typing import Optional, List
from pydantic import BaseModel, field_validator
from datetime import datetime


class CityCreateRequest(BaseModel):
    """Schema for creating or updating a city"""
    name: str
    country_code: str

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate city name - remove extra spaces and ensure proper format"""
        if not v or not v.strip():
            raise ValueError('City name cannot be empty')

        # Clean up the name: strip whitespace, title case
        cleaned_name = v.strip().title()

        # Ensure name contains only valid characters
        if not all(c.isalpha() or c.isspace() or c in '-_' for c in cleaned_name):
            raise ValueError('City name can only contain letters, spaces, hyphens, and underscores')

        return cleaned_name

    @field_validator('country_code')
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        """Validate country code format"""
        if not v or not v.strip():
            raise ValueError('Country code cannot be empty')

        # Clean and normalize country code
        cleaned_code = v.strip().upper()

        # Basic validation - country codes are typically 2-3 characters
        if len(cleaned_code) < 2 or len(cleaned_code) > 3:
            raise ValueError('Country code must be 2-3 characters long')

        # Ensure only alphabetic characters
        if not cleaned_code.isalpha():
            raise ValueError('Country code can only contain letters')

        return cleaned_code


# Aliases for backward compatibility with API endpoints
CityCreate = CityCreateRequest
CityUpdate = CityCreateRequest


class CityResponse(BaseModel):
    """Schema for city response"""
    id: int
    name: str
    country_code: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CityCountryCodeResponse(BaseModel):
    """Schema for country code response"""
    country_code: str


class CityListResponse(BaseModel):
    """Schema for paginated city list response"""
    cities: List[CityResponse]
    total: int
    page: int
    per_page: int
    total_pages: int


class CacheMetrics(BaseModel):
    """Schema for cache metrics"""
    current_size: int
    max_size: int
    hit_rate: float
    total_hits: int
    total_misses: int
    total_requests: int


class PerformanceMetrics(BaseModel):
    """Schema for performance metrics"""
    total_requests: int
    cache_hits: int
    cache_misses: int
    cache_hit_percentage: float
    uptime_seconds: float
    kafka_healthy: bool


class MetricsResponse(BaseModel):
    """Schema for metrics response"""
    cache_metrics: CacheMetrics
    performance_metrics: PerformanceMetrics


class HealthResponse(BaseModel):
    """Schema for health check response"""
    status: str
    response_time_ms: float
    dependencies: dict


class ErrorResponse(BaseModel):
    """Schema for error responses"""
    detail: str
    error_type: Optional[str] = None
    field_errors: Optional[dict] = None


class BulkCreateRequest(BaseModel):
    """Schema for bulk city creation"""
    cities: List[CityCreateRequest]

    @field_validator('cities')
    @classmethod
    def validate_cities_list(cls, v: List[CityCreateRequest]) -> List[CityCreateRequest]:
        """Validate the list of cities"""
        if not v:
            raise ValueError('Cities list cannot be empty')

        if len(v) > 1000:
            raise ValueError('Cannot process more than 1000 cities at once')

        # Check for duplicate city names in the request
        city_names = [city.name.lower() for city in v]
        if len(city_names) != len(set(city_names)):
            raise ValueError('Duplicate city names found in request')

        return v


class BulkCreateResponse(BaseModel):
    """Schema for bulk creation response"""
    created: int
    updated: int
    errors: List[dict]
    total_processed: int


class SearchRequest(BaseModel):
    """Schema for search request"""
    search: Optional[str] = None
    country_code: Optional[str] = None
    page: int = 1
    per_page: int = 10

    @field_validator('page')
    @classmethod
    def validate_page(cls, v: int) -> int:
        """Validate page number"""
        if v < 1:
            raise ValueError('Page number must be greater than 0')
        return v

    @field_validator('per_page')
    @classmethod
    def validate_per_page(cls, v: int) -> int:
        """Validate per_page parameter"""
        if v < 1:
            raise ValueError('Per page value must be greater than 0')
        if v > 100:
            raise ValueError('Per page value cannot exceed 100')
        return v

    @field_validator('search')
    @classmethod
    def validate_search(cls, v: Optional[str]) -> Optional[str]:
        """Validate search term"""
        if v is not None:
            v = v.strip()
            if len(v) < 2:
                raise ValueError('Search term must be at least 2 characters long')
            if len(v) > 50:
                raise ValueError('Search term cannot exceed 50 characters')
        return v


class CSVImportResponse(BaseModel):
    """Schema for CSV import response"""
    total_rows: int
    processed_rows: int
    created_cities: int
    updated_cities: int
    skipped_rows: int
    errors: List[dict]
    processing_time_seconds: float
