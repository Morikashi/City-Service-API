# app/schemas.py

from typing import Optional, List
from pydantic import BaseModel, field_validator
from datetime import datetime


class CityCreateRequest(BaseModel):
    """Schema for creating a city-country code pair"""
    name: str
    country_code: str

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate city name - basic cleaning only"""
        if not v or not v.strip():
            raise ValueError('City name cannot be empty')
        return v.strip()

    @field_validator('country_code')
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        """Validate country code format"""
        if not v or not v.strip():
            raise ValueError('Country code cannot be empty')
        cleaned_code = v.strip().upper()
        if len(cleaned_code) > 20:
            raise ValueError('Country code cannot exceed 20 characters')
        return cleaned_code


# Aliases for backward compatibility
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


class CityCountryCodesResponse(BaseModel):
    """Schema for multiple country codes response"""
    city_name: str
    country_codes: List[str]
    total_codes: int


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
