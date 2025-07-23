# City Service API - Configuration Management
# Pydantic Settings for environment-aware configuration

import os
from typing import List, Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings with environment variable support.
    Uses Pydantic Settings for type validation and environment loading.
    """
    
    # =============================================================================
    # APPLICATION METADATA
    # =============================================================================
    project_name: str = Field(default="City Service API", description="Application name")
    app_version: str = Field(default="1.0.0", description="Application version")
    debug: bool = Field(default=True, description="Debug mode toggle")
    log_level: str = Field(default="INFO", description="Logging level")
    environment: str = Field(default="development", description="Environment name")
    
    # =============================================================================
    # API CONFIGURATION
    # =============================================================================
    api_v1_str: str = Field(default="/api/v1", description="API v1 prefix")
    api_host: str = Field(default="0.0.0.0", description="API host binding")
    api_port: int = Field(default=8000, description="API port")
    docs_url: str = Field(default="/docs", description="Swagger docs URL")
    redoc_url: str = Field(default="/redoc", description="ReDoc URL")
    
    # =============================================================================
    # DATABASE CONFIGURATION (PostgreSQL)
    # =============================================================================
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:password@postgres:5432/citydb",
        description="Complete database connection URL"
    )
    postgres_user: str = Field(default="postgres", description="PostgreSQL username")
    postgres_password: str = Field(default="password", description="PostgreSQL password")
    postgres_db: str = Field(default="citydb", description="PostgreSQL database name")
    postgres_host: str = Field(default="postgres", description="PostgreSQL host")
    postgres_port: int = Field(default=5432, description="PostgreSQL port")
    
    # =============================================================================
    # REDIS CONFIGURATION (Caching)
    # =============================================================================
    redis_url: str = Field(
        default="redis://redis:6379/0",
        description="Complete Redis connection URL"
    )
    redis_host: str = Field(default="redis", description="Redis host")
    redis_port: int = Field(default=6379, description="Redis port")
    redis_db: int = Field(default=0, description="Redis database number")
    
    # =============================================================================
    # CACHE CONFIGURATION (LRU)
    # =============================================================================
    cache_ttl: int = Field(
        default=600,
        description="Cache TTL in seconds (10 minutes)",
        ge=60,  # Minimum 1 minute
        le=3600  # Maximum 1 hour
    )
    cache_max_size: int = Field(
        default=10,
        description="Maximum cache size (LRU)",
        ge=1,
        le=1000
    )
    
    # =============================================================================
    # KAFKA CONFIGURATION (Message Streaming)
    # =============================================================================
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers"
    )
    kafka_topic: str = Field(
        default="city_service_logs",
        description="Kafka topic for application logs"
    )
    kafka_group_id: str = Field(
        default="city_service_group",
        description="Kafka consumer group ID"
    )
    
    # =============================================================================
    # CSV DATA CONFIGURATION
    # =============================================================================
    csv_file_path: str = Field(
        default="country-code.csv",
        description="Path to CSV data file"
    )
    
    # =============================================================================
    # SECURITY CONFIGURATION
    # =============================================================================
    secret_key: str = Field(
        default="your-super-secret-key-change-in-production",
        description="Application secret key"
    )
    algorithm: str = Field(default="HS256", description="JWT algorithm")
    access_token_expire_minutes: int = Field(
        default=30,
        description="JWT token expiration time"
    )
    
    # =============================================================================
    # CORS CONFIGURATION
    # =============================================================================
    cors_origins: List[str] = Field(
        default=["*"],
        description="CORS allowed origins"
    )
    
    # =============================================================================
    # MONITORING CONFIGURATION
    # =============================================================================
    health_check_interval: int = Field(
        default=30,
        description="Health check interval in seconds"
    )
    metrics_enabled: bool = Field(
        default=True,
        description="Enable metrics collection"
    )
    
    # =============================================================================
    # VALIDATORS
    # =============================================================================
    
    @validator('log_level')
    def validate_log_level(cls, v):
        """Validate log level is one of the standard levels."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'Log level must be one of {valid_levels}')
        return v.upper()
    
    @validator('environment')
    def validate_environment(cls, v):
        """Validate environment is one of the expected values."""
        valid_envs = ['development', 'staging', 'production', 'testing']
        if v.lower() not in valid_envs:
            raise ValueError(f'Environment must be one of {valid_envs}')
        return v.lower()
    
    @validator('cors_origins')
    def validate_cors_origins(cls, v):
        """Validate CORS origins format."""
        if isinstance(v, str):
            # Handle string representation of list from env vars
            import ast
            try:
                return ast.literal_eval(v)
            except (ValueError, SyntaxError):
                return [v]
        return v
    
    # =============================================================================
    # COMPUTED PROPERTIES
    # =============================================================================
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "development"
    
    @property
    def database_url_sync(self) -> str:
        """Get synchronous database URL (for migrations)."""
        return self.database_url.replace("+asyncpg", "")
    
    # =============================================================================
    # MODEL CONFIGURATION
    # =============================================================================
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore extra environment variables
        validate_default=True,
        # Allow field aliases for backward compatibility
        populate_by_name=True
    )


# =============================================================================
# GLOBAL SETTINGS INSTANCE
# =============================================================================

def get_settings() -> Settings:
    """
    Get application settings instance.
    This function can be overridden in tests for dependency injection.
    """
    return Settings()


# Create global settings instance
settings = get_settings()


# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================

def validate_configuration():
    """
    Validate the current configuration and log warnings for potential issues.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Validate production settings
    if settings.is_production:
        if settings.secret_key == "your-super-secret-key-change-in-production":
            logger.warning("⚠️  Using default secret key in production!")
        
        if settings.debug:
            logger.warning("⚠️  Debug mode is enabled in production!")
        
        if "*" in settings.cors_origins:
            logger.warning("⚠️  CORS allows all origins in production!")
    
    # Validate cache settings
    if settings.cache_max_size > 100:
        logger.warning(f"⚠️  Large cache size: {settings.cache_max_size}")
    
    # Validate database URL format
    if not settings.database_url.startswith(("postgresql://", "postgresql+asyncpg://")):
        logger.error("❌ Invalid database URL format")
    
    logger.info(f"✅ Configuration validated for environment: {settings.environment}")


# =============================================================================
# ENVIRONMENT-SPECIFIC SETTINGS
# =============================================================================

class DevelopmentSettings(Settings):
    """Development-specific settings."""
    debug: bool = True
    log_level: str = "DEBUG"
    cors_origins: List[str] = ["*"]


class ProductionSettings(Settings):
    """Production-specific settings."""
    debug: bool = False
    log_level: str = "INFO"
    docs_url: Optional[str] = None  # Disable docs in production
    redoc_url: Optional[str] = None


class TestingSettings(Settings):
    """Testing-specific settings."""
    debug: bool = True
    log_level: str = "DEBUG"
    cache_ttl: int = 60  # Shorter TTL for faster tests
    database_url: str = "postgresql+asyncpg://postgres:password@postgres:5432/test_citydb"


def get_settings_by_environment(env: str = None) -> Settings:
    """Get settings based on environment."""
    env = env or os.getenv("ENVIRONMENT", "development")
    
    settings_map = {
        "development": DevelopmentSettings,
        "production": ProductionSettings,
        "testing": TestingSettings
    }
    
    settings_class = settings_map.get(env, Settings)
    return settings_class()
