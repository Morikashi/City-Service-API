"""
CSV Data Loader for City Service API

This script loads city and country code data from a CSV file into the PostgreSQL database.
It handles the specific format: "Country Code" and "City" columns.

Usage:
    python load_csv_data.py [--file-path path/to/csv] [--batch-size 1000]
"""

import asyncio
import pandas as pd
import logging
import time
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func

from app.config import settings
from app.database import init_db, get_db, db_manager
from app.models import City

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVDataLoader:
    """Handles loading city data from CSV files into the database"""
    
    def __init__(self, file_path: str = None, batch_size: int = 1000):
        self.file_path = file_path or settings.csv_file_path
        self.batch_size = batch_size
        self.stats = {
            "total_rows": 0,
            "processed_rows": 0,
            "created_cities": 0,
            "updated_cities": 0,
            "skipped_rows": 0,
            "errors": []
        }
    
    def validate_csv_file(self) -> bool:
        """Validate that the CSV file exists and has the required columns"""
        try:
            # Read just the header to check columns
            df_sample = pd.read_csv(self.file_path, nrows=0)
            required_columns = ['Country Code', 'City']
            
            missing_columns = [col for col in required_columns if col not in df_sample.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                logger.error(f"Found columns: {list(df_sample.columns)}")
                return False
            
            logger.info(f"✅ CSV file validation passed: {self.file_path}")
            logger.info(f"📊 Found columns: {list(df_sample.columns)}")
            return True
            
        except FileNotFoundError:
            logger.error(f"❌ CSV file not found: {self.file_path}")
            return False
        except Exception as e:
            logger.error(f"❌ Error validating CSV file: {e}")
            return False
    
    def clean_and_validate_row(self, row: pd.Series) -> Dict[str, str]:
        """Clean and validate a single row of data"""
        try:
            # Extract and clean city name
            city_name = str(row['City']).strip()
            if pd.isna(row['City']) or not city_name:
                raise ValueError("City name is empty")
            
            # Clean city name: title case, handle special characters
            city_name = city_name.title()
            
            # Extract and clean country code
            country_code = str(row['Country Code']).strip().upper()
            if pd.isna(row['Country Code']) or not country_code:
                raise ValueError("Country code is empty")
            
            # Basic country code validation
            if len(country_code) < 2 or len(country_code) > 3:
                raise ValueError(f"Invalid country code length: {country_code}")
            
            if not country_code.isalpha():
                raise ValueError(f"Country code contains non-alphabetic characters: {country_code}")
            
            return {
                "name": city_name,
                "country_code": country_code
            }
            
        except Exception as e:
            raise ValueError(f"Row validation error: {e}")
    
    async def process_batch(self, batch_data: List[Dict[str, str]], db: AsyncSession):
        """Process a batch of city data"""
        batch_created = 0
        batch_updated = 0
        batch_errors = []
        
        try:
            for city_data in batch_data:
                try:
                    # Check if city already exists (case-insensitive)
                    stmt = select(City).where(func.lower(City.name) == city_data['name'].lower())
                    result = await db.execute(stmt)
                    existing_city = result.scalar_one_or_none()
                    
                    if existing_city:
                        # Update existing city if country code is different
                        if existing_city.country_code != city_data['country_code']:
                            existing_city.country_code = city_data['country_code']
                            db.add(existing_city)
                            batch_updated += 1
                            logger.debug(f"Updated: {city_data['name']} -> {city_data['country_code']}")
                    else:
                        # Create new city
                        new_city = City(
                            name=city_data['name'],
                            country_code=city_data['country_code']
                        )
                        db.add(new_city)
                        batch_created += 1
                        logger.debug(f"Created: {city_data['name']} -> {city_data['country_code']}")
                
                except Exception as e:
                    error_msg = f"Error processing city {city_data.get('name', 'unknown')}: {e}"
                    batch_errors.append(error_msg)
                    logger.warning(error_msg)
            
            # Commit the batch
            await db.commit()
            
            return batch_created, batch_updated, batch_errors
            
        except Exception as e:
            # Rollback on batch error
            await db.rollback()
            error_msg = f"Batch processing error: {e}"
            logger.error(error_msg)
            return 0, 0, [error_msg]
    
    async def load_data(self) -> Dict[str, Any]:
        """Main method to load CSV data into the database"""
        start_time = time.time()
        logger.info(f"🔄 Starting CSV data load from: {self.file_path}")
        
        # Validate file first
        if not self.validate_csv_file():
            return self.stats
        
        try:
            # Read CSV file
            logger.info(f"📖 Reading CSV file...")
            df = pd.read_csv(self.file_path)
            self.stats["total_rows"] = len(df)
            logger.info(f"📊 Found {self.stats['total_rows']} total rows")
            
            # Process data in batches
            batch_data = []
            
            async with db_manager.get_session() as db:
                for index, row in df.iterrows():
                    try:
                        # Clean and validate row
                        clean_data = self.clean_and_validate_row(row)
                        batch_data.append(clean_data)
                        
                        # Process batch when it reaches the batch size
                        if len(batch_data) >= self.batch_size:
                            created, updated, errors = await self.process_batch(batch_data, db)
                            self.stats["created_cities"] += created
                            self.stats["updated_cities"] += updated
                            self.stats["errors"].extend(errors)
                            self.stats["processed_rows"] += len(batch_data)
                            
                            logger.info(
                                f"📦 Batch completed: {self.stats['processed_rows']}/{self.stats['total_rows']} "
                                f"(Created: {created}, Updated: {updated}, Errors: {len(errors)})"
                            )
                            
                            batch_data = []  # Reset batch
                    
                    except ValueError as e:
                        self.stats["skipped_rows"] += 1
                        error_msg = f"Row {index + 2}: {e}"  # +2 because index is 0-based and we have header
                        self.stats["errors"].append(error_msg)
                        logger.warning(error_msg)
                
                # Process remaining batch
                if batch_data:
                    created, updated, errors = await self.process_batch(batch_data, db)
                    self.stats["created_cities"] += created
                    self.stats["updated_cities"] += updated
                    self.stats["errors"].extend(errors)
                    self.stats["processed_rows"] += len(batch_data)
            
            # Calculate final statistics
            processing_time = time.time() - start_time
            self.stats["processing_time_seconds"] = round(processing_time, 2)
            
            # Log final results
            logger.info("🎉 CSV data loading completed!")
            logger.info(f"📊 Final Statistics:")
            logger.info(f"   - Total rows in CSV: {self.stats['total_rows']}")
            logger.info(f"   - Successfully processed: {self.stats['processed_rows']}")
            logger.info(f"   - Cities created: {self.stats['created_cities']}")
            logger.info(f"   - Cities updated: {self.stats['updated_cities']}")
            logger.info(f"   - Rows skipped: {self.stats['skipped_rows']}")
            logger.info(f"   - Processing time: {self.stats['processing_time_seconds']}s")
            
            if self.stats["errors"]:
                logger.warning(f"⚠️  {len(self.stats['errors'])} errors occurred during processing")
                if len(self.stats["errors"]) <= 10:
                    for error in self.stats["errors"]:
                        logger.warning(f"   - {error}")
                else:
                    logger.warning(f"   - Showing first 5 errors:")
                    for error in self.stats["errors"][:5]:
                        logger.warning(f"   - {error}")
                    logger.warning(f"   - ... and {len(self.stats['errors']) - 5} more")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"❌ Critical error during CSV loading: {e}")
            self.stats["errors"].append(f"Critical error: {e}")
            return self.stats


async def main():
    """Main function to run the CSV loader"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Load city data from CSV into database")
    parser.add_argument("--file-path", default=settings.csv_file_path, 
                       help="Path to the CSV file")
    parser.add_argument("--batch-size", type=int, default=1000,
                       help="Number of records to process in each batch")
    
    args = parser.parse_args()
    
    # Initialize database
    await init_db()
    
    # Create loader and run
    loader = CSVDataLoader(file_path=args.file_path, batch_size=args.batch_size)
    results = await loader.load_data()
    
    # Exit with appropriate code
    if results["errors"]:
        logger.warning("⚠️  Completed with errors")
        exit(1)
    else:
        logger.info("✅ Completed successfully")
        exit(0)


if __name__ == "__main__":
    asyncio.run(main())
