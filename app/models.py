# app/models.py

from sqlalchemy import Column, Integer, String, DateTime, Index, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime
from typing import Dict, Any

Base = declarative_base()


class City(Base):
    __tablename__ = "cities"
    
    id = Column(Integer, primary_key=True, index=True)
    # Updated to use Text type to handle any Unicode characters without restrictions
    name = Column(Text, unique=True, index=True, nullable=False)
    # Updated country_code to handle longer codes and special characters
    country_code = Column(String(20), nullable=False)  # Increased from 10 to 20
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Additional indexes for better query performance
    __table_args__ = (
        Index('idx_city_name_country', 'name', 'country_code'),
        Index('idx_country_code', 'country_code'),
        # Add a case-insensitive index for better search performance
        Index('idx_city_name_lower', func.lower(name)),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model instance to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "country_code": self.country_code,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    def __repr__(self) -> str:
        return f"<City(name='{self.name}', country_code='{self.country_code}')>"
