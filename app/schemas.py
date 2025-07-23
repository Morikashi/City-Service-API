# City Service API - Pydantic Schemas
# Request/Response validation models with comprehensive error handling

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, ConfigDict


# =============================================================================
# CORE CITY SCHEMAS
# =============================================================================

class CityBase(BaseModel):
    """Base city schema with common fields and validation."""
    
    name: str = Field(
        ...,
        min_length=2,
        max_length=255,
        description="City name (2-255 characters)",
        examples=["New York", "London", "SanDiego"]
    )
    
    country_code: str = Field(
        ...,
        min_length=2,
        max_length=3,
        description="Country code (2-3 characters, e.g., 'US', 'GBR')",
        examples=["US", "GB", "CV"]
    )
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate and normalize city name."""
        if not v or not v.strip():
            raise ValueError('City name cannot be empty')
        
        # Clean up the name: strip whitespace, title case
        cleaned_name = v.strip().title()
        
        # Ensure name contains only valid characters
        valid_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ -'.")
        if not set(cleaned_name).issubset(valid_chars):
            invalid_chars = set(cleaned_name) - valid_chars
            raise ValueError(f'City name contains invalid characters: {invalid_chars}')
        
        return cleaned_name
    
    @field_validator('country_code')
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        """Validate and normalize country code."""
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


class CityCreate(CityBase):
    """Schema for creating a new city."""
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "SanDiego",
                "country_code": "CV"
            }
        }
    )


class CityUpdate(CityBase):
    """Schema for updating an existing city."""
    
    # Make all fields optional for partial updates
    name: Optional[str] = Field(
        None,
        min_length=2,
        max_length=255,
        description="City name (optional for updates)"
    )
    
    country_code: Optional[str] = Field(
        None,
        min_length=2,
        max_length=3,
        description="Country code (optional for updates)"
    )
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "country_code": "USA"
            }
        }
    )


class CityResponse(CityBase):
    """Schema for city response with metadata."""
    
    id: int = Field(
        ...,
        description="Unique city identifier",
        examples=[1, 42, 1337]
    )
    
    created_at: datetime = Field(
        ...,
        description="Timestamp when city was created"
    )
    
    updated_at: datetime = Field(
        ...,
        description="Timestamp when city was last updated"
    )
    
    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "name": "SanDiego",
                "country_code": "CV",
                "created_at": "2025-07-22T15:46:00Z",
                "updated_at": "2025-07-22T15:46:00Z"
            }
        }
    )


class CityCountryCodeResponse(BaseModel):
    """Schema for country code-only responses."""
    
    country_code: str = Field(
        ...,
        description="The country code for the requested city",
        examples=["US", "GB", "CV"]
    )
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "country_code": "CV"
            }
        }
    )


# =============================================================================
# PAGINATION AND LISTING SCHEMAS
# =============================================================================

class PaginationParams(BaseModel):
    """Schema for pagination parameters."""
    
    page: int = Field(
        default=1,
        ge=1,
        description="Page number (starts from 1)"
    )
    
    per_page: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Items per page (1-100)"
    )


class SearchParams(PaginationParams):
    """Schema for search and filtering parameters."""
    
    search: Optional[str] = Field(
        default=None,
        min_length=2,
        max_length=100,
        description="Search term for city names"
    )
    
    country_code: Optional[str] = Field(
        default=None,
        min_length=2,
        max_length=3,
        description="Filter by country code"
    )
    
    @field_validator('search')
    @classmethod
    def validate_search(cls, v: Optional[str]) -> Optional[str]:
        """Validate search term."""
        if v is not None:
            v = v.strip()
            if len(v) < 2:
                raise ValueError('Search term must be at least 2 characters long')
        return v


class CityListResponse(BaseModel):
    """Schema for paginated city list responses."""
    
    cities: List[CityResponse] = Field(
        ...,
        description="List of cities for the current page"
    )
    
    total: int = Field(
        ...,
        description="Total number of cities matching the query"
    )
    
    page: int = Field(
        ...,
        description="Current page number"
    )
    
    per_page: int = Field(
        ...,
        description="Number of items per page"
    )
    
    total_pages: int = Field(
        ...,
        description="Total number of pages available"
    )
    
    has_next: bool = Field(
        ...,
        description="Whether there are more pages available"
    )
    
    has_prev: bool = Field(
        ...,
        description="Whether there are previous pages available"
    )
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "cities": [
                    {
                        "id": 1,
                        "name": "SanDiego",
                        "country_code": "CV",
                        "created_at": "2025-07-22T15:46:00Z",
                        "updated_at": "2025-07-22T15:46:00Z"
                    }
                ],
                "total": 50,
                "page": 1,
                "per_page": 10,
                "total_pages": 5,
                "has_next": True,
                "has_prev": False
            }
        }
    )


# =============================================================================
# BULK OPERATIONS SCHEMAS
# =============================================================================

class BulkCityCreate(BaseModel):
    """Schema for bulk city creation."""
    
    cities: List[CityCreate] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="List of cities to create (max 1000)"
    )
    
    update_existing: bool = Field(
        default=False,
        description="Whether to update existing cities with same name"
    )
    
    @field_validator('cities')
    @classmethod
    def validate_cities(cls, v: List[CityCreate]) -> List[CityCreate]:
        """Validate cities list for duplicates."""
        if not v:
            raise ValueError('Cities list cannot be empty')
        
        # Check for duplicates within the request
        city_names = [city.name.lower() for city in v]
        if len(city_names) != len(set(city_names)):
            raise ValueError('Duplicate city names found in request')
        
        return v


class BulkOperationResponse(BaseModel):
    """Schema for bulk operation results."""
    
    created: int = Field(
        ...,
        description="Number of cities created"
    )
    
    updated: int = Field(
        ...,
        description="Number of cities updated"
    )
    
    errors: List[Dict[str, Any]] = Field(
        default=[],
        description="List of errors encountered"
    )
    
    total_processed: int = Field(
        ...,
        description="Total number of records processed"
    )
    
    processing_time_seconds: float = Field(
        ...,
        description="Time taken to process the request"
    )


# =============================================================================
# METRICS AND MONITORING SCHEMAS
# =============================================================================

class CacheMetrics(BaseModel):
    """Schema for cache performance metrics."""
    
    current_size: int = Field(
        ...,
        description="Current number of items in cache"
    )
    
    max_size: int = Field(
        ...,
        description="Maximum cache size"
    )
    
    hit_rate: float = Field(
        ...,
        ge=0,
        le=100,
        description="Cache hit rate percentage"
    )
    
    total_hits: int = Field(
        ...,
        description="Total cache hits"
    )
    
    total_misses: int = Field(
        ...,
        description="Total cache misses"
    )
    
    total_requests: int = Field(
        ...,
        description="Total cache requests"
    )


class PerformanceMetrics(BaseModel):
    """Schema for application performance metrics."""
    
    total_requests: int = Field(
        ...,
        description="Total API requests processed"
    )
    
    cache_hits: int = Field(
        ...,
        description="Number of cache hits"
    )
    
    cache_misses: int = Field(
        ...,
        description="Number of cache misses"
    )
    
    cache_hit_percentage: float = Field(
        ...,
        ge=0,
        le=100,
        description="Cache hit percentage since startup"
    )
    
    uptime_seconds: float = Field(
        ...,
        description="Application uptime in seconds"
    )
    
    average_response_time_ms: float = Field(
        ...,
        description="Average response time in milliseconds"
    )


class MetricsResponse(BaseModel):
    """Schema for comprehensive metrics response."""
    
    cache_metrics: CacheMetrics = Field(
        ...,
        description="Cache performance metrics"
    )
    
    performance_metrics: PerformanceMetrics = Field(
        ...,
        description="Application performance metrics"
    )
    
    timestamp: datetime = Field(
        ...,
        description="When these metrics were collected"
    )


# =============================================================================
# HEALTH CHECK SCHEMAS
# =============================================================================

class ServiceStatus(BaseModel):
    """Schema for individual service status."""
    
    status: str = Field(
        ...,
        description="Service status",
        pattern="^(healthy|unhealthy|degraded)$"
    )
    
    response_time_ms: Optional[float] = Field(
        None,
        description="Service response time in milliseconds"
    )
    
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional service details"
    )


class HealthResponse(BaseModel):
    """Schema for health check responses."""
    
    status: str = Field(
        ...,
        description="Overall system status",
        pattern="^(healthy|unhealthy|degraded)$"
    )
    
    timestamp: datetime = Field(
        ...,
        description="Health check timestamp"
    )
    
    response_time_ms: float = Field(
        ...,
        description="Total health check response time"
    )
    
    services: Dict[str, ServiceStatus] = Field(
        ...,
        description="Status of individual services"
    )
    
    uptime_seconds: float = Field(
        ...,
        description="Application uptime in seconds"
    )


# =============================================================================
# ERROR RESPONSE SCHEMAS
# =============================================================================

class ErrorDetail(BaseModel):
    """Schema for error details."""
    
    field: Optional[str] = Field(
        None,
        description="Field name where error occurred"
    )
    
    message: str = Field(
        ...,
        description="Error message"
    )
    
    code: Optional[str] = Field(
        None,
        description="Error code"
    )


class ErrorResponse(BaseModel):
    """Schema for error responses."""
    
    detail: str = Field(
        ...,
        description="Main error message"
    )
    
    error_type: Optional[str] = Field(
        None,
        description="Type of error"
    )
    
    field_errors: Optional[List[ErrorDetail]] = Field(
        None,
        description="Field-specific validation errors"
    )
    
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the error occurred"
    )
    
    request_id: Optional[str] = Field(
        None,
        description="Request ID for tracking"
    )


# =============================================================================
# CSV IMPORT SCHEMAS
# =============================================================================

class CSVImportRequest(BaseModel):
    """Schema for CSV import requests."""
    
    file_path: str = Field(
        ...,
        description="Path to CSV file to import"
    )
    
    update_existing: bool = Field(
        default=True,
        description="Whether to update existing cities"
    )
    
    batch_size: int = Field(
        default=1000,
        ge=100,
        le=10000,
        description="Number of records to process per batch"
    )
    
    @field_validator('file_path')
    @classmethod
    def validate_file_path(cls, v: str) -> str:
        """Validate CSV file path."""
        if not v.strip():
            raise ValueError('File path cannot be empty')
        
        if not v.lower().endswith('.csv'):
            raise ValueError('File must be a CSV file')
        
        return v.strip()


class CSVImportResponse(BaseModel):
    """Schema for CSV import results."""
    
    total_rows: int = Field(
        ...,
        description="Total rows in CSV file"
    )
    
    processed_rows: int = Field(
        ...,
        description="Number of rows successfully processed"
    )
    
    created_cities: int = Field(
        ...,
        description="Number of new cities created"
    )
    
    updated_cities: int = Field(
        ...,
        description="Number of existing cities updated"
    )
    
    skipped_rows: int = Field(
        ...,
        description="Number of rows skipped due to errors"
    )
    
    errors: List[Dict[str, Any]] = Field(
        default=[],
        description="Detailed error information"
    )
    
    processing_time_seconds: float = Field(
        ...,
        description="Total processing time"
    )
    
    file_path: str = Field(
        ...,
        description="Path to processed CSV file"
    )


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def create_error_response(
    message: str,
    error_type: str = None,
    field_errors: List[ErrorDetail] = None,
    request_id: str = None
) -> ErrorResponse:
    """Helper function to create standardized error responses."""
    return ErrorResponse(
        detail=message,
        error_type=error_type,
        field_errors=field_errors,
        request_id=request_id
    )


def create_validation_error_response(
    errors: List[Dict[str, Any]],
    request_id: str = None
) -> ErrorResponse:
    """Helper function to create validation error responses."""
    field_errors = [
        ErrorDetail(
            field=error.get('loc', ['unknown'])[-1],
            message=error.get('msg', 'Validation error'),
            code=error.get('type', 'validation_error')
        )
        for error in errors
    ]
    
    return ErrorResponse(
        detail="Validation failed",
        error_type="validation_error",
        field_errors=field_errors,
        request_id=request_id
    )
