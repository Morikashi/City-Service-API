# City Service API - Database Models
# SQLAlchemy ORM models with optimized indexing and validation

from datetime import datetime
from typing import Dict, Any

from sqlalchemy import Column, Integer, String, DateTime, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.orm import validates

# Create declarative base for all models
Base = declarative_base()


class City(Base):
    """
    City model representing cities and their country codes.
    
    This model handles:
    - City name storage with proper validation
    - Country code associations
    - Automatic timestamp tracking
    - Optimized database indexing for fast queries
    """
    
    __tablename__ = "cities"
    
    # =============================================================================
    # TABLE COLUMNS
    # =============================================================================
    
    # Primary key - auto-incrementing integer
    id = Column(
        Integer,
        primary_key=True,
        index=True,
        comment="Unique identifier for each city"
    )
    
    # City name - unique, indexed, required
    name = Column(
        String(255),
        unique=True,
        index=True,
        nullable=False,
        comment="City name (unique across all records)"
    )
    
    # Country code - indexed, required
    country_code = Column(
        String(10),
        nullable=False,
        index=True,
        comment="Country code (2-3 characters, e.g., 'US', 'GBR')"
    )
    
    # Automatic timestamp tracking
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="Timestamp when record was created"
    )
    
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="Timestamp when record was last updated"
    )
    
    # =============================================================================
    # TABLE CONSTRAINTS AND INDEXES
    # =============================================================================
    
    __table_args__ = (
        # Composite index for queries filtering by both city and country
        Index('idx_city_country_composite', 'name', 'country_code'),
        
        # Dedicated index for country code lookups
        Index('idx_country_code_lookup', 'country_code'),
        
        # Case-insensitive index for city name searches
        Index('idx_city_name_lower', func.lower('name')),
        
        # Composite index for timestamp-based queries
        Index('idx_created_updated', 'created_at', 'updated_at'),
        
        # Unique constraint to ensure data integrity
        UniqueConstraint('name', name='uq_city_name'),
        
        # Table comment for documentation
        {'comment': 'Cities and their associated country codes with timestamp tracking'}
    )
    
    # =============================================================================
    # VALIDATION METHODS
    # =============================================================================
    
    @validates('name')
    def validate_name(self, key: str, name: str) -> str:
        """
        Validate and normalize city name.
        
        Rules:
        - Cannot be empty or None
        - Strips leading/trailing whitespace
        - Converts to title case for consistency
        - Validates character set (letters, spaces, hyphens, apostrophes)
        - Maximum length of 255 characters
        """
        if not name:
            raise ValueError("City name cannot be empty")
        
        # Normalize the name
        normalized = name.strip().title()
        
        if not normalized:
            raise ValueError("City name cannot be empty after normalization")
        
        if len(normalized) > 255:
            raise ValueError("City name cannot exceed 255 characters")
        
        # Validate character set (allow letters, spaces, hyphens, apostrophes, periods)
        allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ -'.")
        if not set(normalized).issubset(allowed_chars):
            invalid_chars = set(normalized) - allowed_chars
            raise ValueError(f"City name contains invalid characters: {invalid_chars}")
        
        return normalized
    
    @validates('country_code')
    def validate_country_code(self, key: str, country_code: str) -> str:
        """
        Validate and normalize country code.
        
        Rules:
        - Cannot be empty or None
        - Strips leading/trailing whitespace
        - Converts to uppercase for consistency
        - Must be 2-3 characters long
        - Must contain only alphabetic characters
        """
        if not country_code:
            raise ValueError("Country code cannot be empty")
        
        # Normalize the country code
        normalized = country_code.strip().upper()
        
        if not normalized:
            raise ValueError("Country code cannot be empty after normalization")
        
        if len(normalized) < 2 or len(normalized) > 3:
            raise ValueError("Country code must be 2-3 characters long")
        
        if not normalized.isalpha():
            raise ValueError("Country code must contain only alphabetic characters")
        
        return normalized
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert model instance to dictionary.
        Useful for JSON serialization and API responses.
        """
        return {
            "id": self.id,
            "name": self.name,
            "country_code": self.country_code,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    def to_json_dict(self) -> Dict[str, Any]:
        """
        Convert model instance to JSON-safe dictionary.
        Handles datetime serialization for API responses.
        """
        return {
            "id": self.id,
            "name": self.name,
            "country_code": self.country_code,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    def update_from_dict(self, data: Dict[str, Any]) -> None:
        """
        Update model instance from dictionary.
        Useful for handling API updates while maintaining validation.
        """
        for key, value in data.items():
            if hasattr(self, key) and key not in ['id', 'created_at']:
                setattr(self, key, value)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'City':
        """
        Create model instance from dictionary.
        Useful for bulk creation from CSV or API data.
        """
        # Filter out non-model fields
        model_data = {
            key: value for key, value in data.items()
            if key in ['name', 'country_code']
        }
        return cls(**model_data)
    
    # =============================================================================
    # QUERY HELPER METHODS
    # =============================================================================
    
    @classmethod
    def get_search_filter(cls, search_term: str):
        """
        Get SQLAlchemy filter for searching cities by name.
        Performs case-insensitive partial matching.
        """
        return func.lower(cls.name).contains(func.lower(search_term))
    
    @classmethod
    def get_country_filter(cls, country_code: str):
        """
        Get SQLAlchemy filter for filtering cities by country code.
        Performs case-insensitive exact matching.
        """
        return func.upper(cls.country_code) == func.upper(country_code)
    
    # =============================================================================
    # STRING REPRESENTATION
    # =============================================================================
    
    def __repr__(self) -> str:
        """
        Developer-friendly string representation.
        Shows key information for debugging and logging.
        """
        return (
            f"<City(id={self.id}, name='{self.name}', "
            f"country_code='{self.country_code}')>"
        )
    
    def __str__(self) -> str:
        """
        User-friendly string representation.
        Shows human-readable information.
        """
        return f"{self.name} ({self.country_code})"
    
    # =============================================================================
    # COMPARISON METHODS
    # =============================================================================
    
    def __eq__(self, other) -> bool:
        """
        Compare cities for equality based on name (case-insensitive).
        """
        if not isinstance(other, City):
            return False
        return self.name.lower() == other.name.lower()
    
    def __hash__(self) -> int:
        """
        Make City objects hashable based on normalized name.
        Allows use in sets and as dictionary keys.
        """
        return hash(self.name.lower())
    
    # =============================================================================
    # BUSINESS LOGIC METHODS
    # =============================================================================
    
    def is_same_country(self, other: 'City') -> bool:
        """Check if this city is in the same country as another city."""
        return self.country_code.upper() == other.country_code.upper()
    
    def get_display_name(self) -> str:
        """Get formatted display name for UI purposes."""
        return f"{self.name}, {self.country_code}"
    
    def get_cache_key(self) -> str:
        """Get cache key for Redis caching."""
        return f"city:{self.name.lower()}"
    
    def is_recently_created(self, hours: int = 24) -> bool:
        """Check if city was created within the specified number of hours."""
        if not self.created_at:
            return False
        
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return self.created_at.replace(tzinfo=None) > cutoff
    
    def is_recently_updated(self, hours: int = 24) -> bool:
        """Check if city was updated within the specified number of hours."""
        if not self.updated_at:
            return False
        
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return self.updated_at.replace(tzinfo=None) > cutoff


# =============================================================================
# MODEL UTILITIES
# =============================================================================

def get_all_models():
    """Get all model classes for database operations."""
    return [City]


def get_model_by_name(name: str):
    """Get model class by name."""
    models_map = {
        'city': City,
        'cities': City,
    }
    return models_map.get(name.lower())


# =============================================================================
# DATABASE SCHEMA INFORMATION
# =============================================================================

def get_schema_info() -> Dict[str, Any]:
    """
    Get comprehensive information about the database schema.
    Useful for API documentation and debugging.
    """
    return {
        "models": {
            "City": {
                "table_name": City.__tablename__,
                "columns": [
                    {
                        "name": col.name,
                        "type": str(col.type),
                        "nullable": col.nullable,
                        "primary_key": col.primary_key,
                        "unique": col.unique,
                        "comment": col.comment,
                    }
                    for col in City.__table__.columns
                ],
                "indexes": [
                    {
                        "name": idx.name,
                        "columns": [col.name for col in idx.columns],
                        "unique": idx.unique,
                    }
                    for idx in City.__table__.indexes
                ],
                "constraints": [
                    {
                        "name": const.name,
                        "type": type(const).__name__,
                    }
                    for const in City.__table__.constraints
                ],
            }
        },
        "total_models": len(get_all_models()),
    }
