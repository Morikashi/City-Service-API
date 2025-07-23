# scripts/load_csv_data.py

import asyncio
import csv
import logging
from pathlib import Path
from typing import Dict, Any
import time

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.models import City

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def process_single_city(session: AsyncSession, city_name: str, country_code: str) -> Dict[str, Any]:
    """
    Process a single city with proper error handling and session management.
    Returns a result dictionary indicating success/failure and action taken.
    """
    try:
        # Check if city already exists (case-insensitive)
        stmt = select(City).where(func.lower(City.name) == city_name.lower())
        result = await session.execute(stmt)
        existing_city = result.scalar_one_or_none()

        if existing_city:
            # Update existing city if country code is different
            if existing_city.country_code != country_code:
                existing_city.country_code = country_code
                await session.commit()
                return {
                    'status': 'updated',
                    'city': city_name,
                    'old_code': existing_city.country_code,
                    'new_code': country_code
                }
            else:
                # City exists with same country code - skip
                return {
                    'status': 'skipped',
                    'city': city_name,
                    'reason': 'already_exists_same_code'
                }
        else:
            # Create new city
            new_city = City(name=city_name, country_code=country_code)
            session.add(new_city)
            await session.commit()
            return {
                'status': 'created',
                'city': city_name,
                'country_code': country_code
            }

    except IntegrityError as e:
        # Handle duplicate key errors gracefully
        await session.rollback()
        logger.warning(f"Integrity error for city '{city_name}': {str(e)}")
        return {
            'status': 'error',
            'city': city_name,
            'error': f'Integrity error: {str(e)}'
        }
    except Exception as e:
        # Handle any other database errors
        await session.rollback()
        logger.error(f"Unexpected error processing city '{city_name}': {str(e)}")
        return {
            'status': 'error',
            'city': city_name,
            'error': f'Unexpected error: {str(e)}'
        }


async def load_data():
    """
    Connects to the database and loads data from the country-code.csv file.
    Uses individual transactions for better error handling.
    """
    logger.info("🚀 Starting CSV data loader...")
    start_time = time.time()

    # Create database engine and session factory
    engine = create_async_engine(settings.database_url)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    # Find CSV file (should be in project root directory)
    csv_file_path = Path(__file__).parent.parent / "country-code.csv"
    
    if not csv_file_path.exists():
        logger.error(f"❌ CSV file not found at: {csv_file_path}")
        logger.error("Please ensure 'country-code.csv' is in the project root directory")
        return

    # Load CSV data
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            cities_data = []
            
            for row_num, row in enumerate(reader, start=1):
                city_name = row.get('City', '').strip()
                country_code = row.get('Country Code', '').strip()
                
                # Validate row data
                if not city_name:
                    logger.warning(f"Row {row_num}: City name is empty, skipping")
                    continue
                if not country_code:
                    logger.warning(f"Row {row_num}: Country code is empty for city '{city_name}', skipping")
                    continue
                
                cities_data.append({
                    'row_num': row_num,
                    'city': city_name,
                    'country_code': country_code
                })

        logger.info(f"📂 Loaded {len(cities_data)} valid records from CSV file")

    except FileNotFoundError:
        logger.error(f"❌ Error: The file '{csv_file_path}' was not found.")
        return
    except Exception as e:
        logger.error(f"❌ Error reading CSV file: {e}")
        return

    # Process cities one by one with individual sessions for better error isolation
    stats = {
        'total_rows': len(cities_data),
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0,
        'error_details': []
    }

    logger.info(f"🔄 Processing {len(cities_data)} cities...")
    
    for i, city_data in enumerate(cities_data):
        if i > 0 and i % 100 == 0:
            logger.info(f"📊 Progress: {i}/{len(cities_data)} cities processed...")

        # Use a fresh session for each city to avoid session contamination
        async with async_session() as session:
            result = await process_single_city(
                session, 
                city_data['city'], 
                city_data['country_code']
            )
            
            # Update statistics based on result
            if result['status'] == 'created':
                stats['created'] += 1
            elif result['status'] == 'updated':
                stats['updated'] += 1
            elif result['status'] == 'skipped':
                stats['skipped'] += 1
            elif result['status'] == 'error':
                stats['errors'] += 1
                if len(stats['error_details']) < 10:  # Keep only first 10 errors for logging
                    stats['error_details'].append({
                        'row': city_data['row_num'],
                        'city': city_data['city'],
                        'error': result['error']
                    })

    # Close the engine
    await engine.dispose()

    # Calculate processing time
    processing_time = time.time() - start_time

    # Log final statistics
    logger.info("🎉 CSV data loading completed!")
    logger.info("📊 Final Statistics:")
    logger.info(f"   - Total rows processed: {stats['total_rows']}")
    logger.info(f"   - Cities created: {stats['created']}")
    logger.info(f"   - Cities updated: {stats['updated']}")
    logger.info(f"   - Cities skipped: {stats['skipped']}")
    logger.info(f"   - Errors encountered: {stats['errors']}")
    logger.info(f"   - Processing time: {processing_time:.2f}s")

    # Log error details if any
    if stats['errors'] > 0:
        logger.warning(f"⚠️  {stats['errors']} errors occurred during processing:")
        for error_detail in stats['error_details']:
            logger.warning(f"   - Row {error_detail['row']}: {error_detail['city']} - {error_detail['error']}")
        if stats['errors'] > len(stats['error_details']):
            logger.warning(f"   - ... and {stats['errors'] - len(stats['error_details'])} more errors")

    # Determine overall success
    if stats['errors'] == 0:
        logger.info("✅ Data loading completed successfully!")
    else:
        logger.warning("⚠️  Data loading completed with errors")


if __name__ == "__main__":
    asyncio.run(load_data())
