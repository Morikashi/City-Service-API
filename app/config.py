from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Database Configuration
    database_url: str = Field(default="postgresql+asyncpg://postgres:password@localhost:5432/citydb")
    db_host: str = Field(default="localhost")
    db_port: int = Field(default=5432)
    db_name: str = Field(default="citydb")
    db_user: str = Field(default="postgres")
    db_password: str = Field(default="password")
    
    # Redis Configuration
    redis_url: str = Field(default="redis://localhost:6379")
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    redis_db: int = Field(default=0)
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic: str = Field(default="city_service_logs")
    kafka_group_id: str = Field(default="city_service_group")
    
    # Application Configuration
    project_name: str = Field(default="City Service API")
    app_version: str = Field(default="1.0.0")
    debug: bool = Field(default=True)
    log_level: str = Field(default="INFO")
    
    # Cache Configuration
    cache_ttl: int = Field(default=600)  # 10 minutes
    cache_max_size: int = Field(default=10)
    
    # CSV Data Configuration
    csv_file_path: str = Field(default="country-code.csv")
    
    # API Configuration
    api_v1_str: str = Field(default="/api/v1")
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings"""
    return settings
