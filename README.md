# City Service API

A production-ready FastAPI application for managing cities and their country codes with advanced caching, real-time performance logging, and CSV data integration.

## 🎯 Project Overview

The City Service API is a comprehensive microservice that demonstrates modern Python development practices with:

- **FastAPI** web framework with async/await support
- **PostgreSQL** database with async SQLAlchemy ORM  
- **Redis** LRU caching (10 items max, 10-minute TTL)
- **Apache Kafka** for comprehensive request logging and metrics
- **Docker** containerization for easy deployment
- **CSV data integration** for bulk city imports

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FastAPI App   │────│   PostgreSQL    │    │      Redis      │
│   (Port 8000)   │    │   (Port 5432)   │    │   (Port 6379)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                                              │
         │              ┌─────────────────┐            │
         └──────────────│  Apache Kafka   │────────────┘
                        │   (Port 9092)   │
                        └─────────────────┘
                                │
                        ┌─────────────────┐
                        │    Kafka UI     │
                        │   (Port 8080)   │
                        └─────────────────┘
```

## ✨ Features

### Core API Features
- **Create/Update Cities**: Add new cities or update existing country codes
- **Retrieve Country Codes**: Get country codes with intelligent caching
- **List Cities**: Paginated city listings with search functionality
- **Performance Metrics**: Real-time cache and request statistics

### Caching Strategy
- **Two-Level Caching**: Local LRU cache + Redis distributed cache
- **Intelligent Eviction**: LRU eviction with exactly 10 items maximum
- **TTL Expiration**: Automatic 10-minute cache expiration
- **Cache Invalidation**: Smart invalidation on data updates

### Logging & Monitoring
- **Request Tracking**: All API requests logged to Kafka
- **Performance Metrics**: Response times, cache hit/miss ratios
- **Real-time Analytics**: Running cache hit percentage from startup
- **Health Monitoring**: Comprehensive health checks for all services

### Data Management  
- **CSV Integration**: Bulk import from CSV files with validation
- **Batch Processing**: Efficient handling of large datasets (10,000+ records)
- **Data Validation**: Robust input validation and sanitization
- **Error Handling**: Comprehensive error reporting and recovery

## 📋 Requirements

- **Docker Desktop** (latest version recommended)
- **docker-compose** (included with Docker Desktop)
- **CSV File**: `country-code.csv` with columns: "Country Code", "City"

### CSV File Format
```csv
Country Code,City
CV,SanDiego
USA,New York
GBR,London
JPN,Tokyo
```

## 🚀 Quick Start

### 1. Clone and Setup
```bash
# Clone the project
git clone <repository-url>
cd city-service-api

# Ensure your CSV file is in the project root
# File should be named: country-code.csv
ls -la country-code.csv
```

### 2. Start the Application
```bash
# Make the startup script executable
chmod +x start.sh

# Start everything with one command
./start.sh
```

The startup script will:
- ✅ Validate your environment
- ✅ Clean up any previous containers
- ✅ Build and start all services
- ✅ Wait for services to be healthy
- ✅ Load your CSV data automatically  
- ✅ Run comprehensive tests
- ✅ Display access information

### 3. Access the Application

Once started, you can access:
- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health  
- **Kafka UI**: http://localhost:8080
- **App Info**: http://localhost:8000/info

## 📊 API Endpoints

### Core Endpoints

#### Create or Update City
```bash
POST /api/v1/cities/
Content-Type: application/json

{
  "name": "Paris",
  "country_code": "FRA"
}
```

#### Get Country Code (with Caching)
```bash
GET /api/v1/cities/{city_name}/country-code

# Response
{
  "country_code": "FRA"
}
```

#### List Cities (with Pagination)
```bash
GET /api/v1/cities/?page=1&per_page=10&search=paris

# Response
{
  "cities": [...],
  "total": 1000,
  "page": 1,
  "per_page": 10,
  "total_pages": 100
}
```

#### Performance Metrics
```bash
GET /api/v1/cities/metrics

# Response
{
  "cache_metrics": {
    "current_size": 8,
    "max_size": 10,
    "hit_rate": 85.5,
    "total_hits": 120,
    "total_misses": 30,
    "total_requests": 150
  },
  "performance_metrics": {
    "total_requests": 150,
    "cache_hits": 120,
    "cache_misses": 30,
    "cache_hit_percentage": 80.0,
    "uptime_seconds": 3600,
    "kafka_healthy": true
  }
}
```

#### Delete City
```bash
DELETE /api/v1/cities/{city_name}
```

### Example Usage

```bash
# Create a city
curl -X POST http://localhost:8000/api/v1/cities/ \
  -H "Content-Type: application/json" \
  -d '{"name": "Barcelona", "country_code": "ESP"}'

# Get country code (cache miss - first request)
curl http://localhost:8000/api/v1/cities/Barcelona/country-code

# Get country code again (cache hit - second request)  
curl http://localhost:8000/api/v1/cities/Barcelona/country-code

# Search for cities
curl "http://localhost:8000/api/v1/cities/?search=Bar&page=1&per_page=5"

# Get performance metrics
curl http://localhost:8000/api/v1/cities/metrics
```

## 🎛️ Management Commands

### Service Management
```bash
# View all service logs
docker-compose logs -f

# View only application logs  
docker-compose logs -f app

# Restart all services
docker-compose restart

# Stop all services
docker-compose down

# Stop and remove all data
docker-compose down -v
```

### Data Management
```bash
# Load CSV data manually
docker-compose exec app python load_csv_data.py

# Load custom CSV file
docker-compose exec app python load_csv_data.py --file-path /path/to/data.csv

# Run tests manually
docker-compose exec app python test_api.py
```

### Database Operations
```bash
# Connect to PostgreSQL
docker exec -it city_service_postgres psql -U postgres -d citydb

# Connect to Redis
docker exec -it city_service_redis redis-cli

# View Kafka topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## 🔧 Configuration

### Environment Variables (.env)
```bash
# Database
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=citydb
DB_USER=postgres
DB_PASSWORD=password

# Redis  
REDIS_HOST=host.docker.internal
REDIS_PORT=6379

# Kafka
KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092
KAFKA_TOPIC=city_service_logs

# Application
PROJECT_NAME="City Service API"
DEBUG=false
LOG_LEVEL=INFO

# Cache Settings
CACHE_TTL=600  # 10 minutes
CACHE_MAX_SIZE=10  # Maximum 10 items
```

## 📈 Performance Optimization

### Caching Strategy
- **L1 Cache (Local)**: In-memory OrderedDict with LRU eviction
- **L2 Cache (Redis)**: Distributed cache for multi-instance deployments
- **Write-Through**: Updates both cache levels simultaneously
- **Smart Eviction**: Removes least recently used items when full

### Database Optimization  
- **Connection Pooling**: 10 connections with 20 overflow
- **Async Operations**: Non-blocking database I/O
- **Indexed Queries**: Optimized indexes on city names and country codes
- **Batch Processing**: Efficient bulk operations for CSV imports

### Typical Performance
- **Cache Hit Response**: ~5ms average
- **Cache Miss Response**: ~45ms average  
- **Performance Improvement**: ~88% faster with cache hits
- **Throughput**: 100+ requests/second under normal load

## 📊 Monitoring

### Kafka Logging
Every request generates a structured log message:
```json
{
  "timestamp": "2025-07-21T12:00:00Z",
  "event_type": "api_request",
  "service": "city_service", 
  "city_name": "New York",
  "response_time_ms": 45.2,
  "cache_hit": true,
  "status_code": 200,
  "cache_hit_percentage": 85.5,
  "total_requests": 1000,
  "uptime_seconds": 3600
}
```

### Health Monitoring
```json
{
  "status": "healthy",
  "response_time_ms": 12.34,
  "dependencies": {
    "database": "healthy",
    "redis": "healthy", 
    "kafka": "healthy"
  },
  "uptime_seconds": 3600
}
```

## 🧪 Testing

The project includes a comprehensive test suite that validates:
- ✅ Health endpoint functionality
- ✅ CRUD operations  
- ✅ Cache performance (hit vs miss)
- ✅ Error handling (404, 422, 500)
- ✅ Concurrent request handling
- ✅ Metrics collection accuracy

Run tests manually:
```bash
docker-compose exec app python test_api.py
```

## 🐛 Troubleshooting

### Common Issues

**Application won't start**
```bash
# Check all service logs
docker-compose logs

# Check specific service
docker-compose logs app
docker-compose logs postgres
docker-compose logs redis
docker-compose logs kafka
```

**Database connection issues**
```bash
# Test database connectivity
docker exec city_service_postgres pg_isready -U postgres

# Check database logs
docker-compose logs postgres
```

**CSV file not loading**
```bash
# Ensure CSV file is mounted correctly
docker-compose exec app ls -la /app/country-code.csv

# Check CSV format
docker-compose exec app head -5 /app/country-code.csv
```

**Cache not working**  
```bash
# Test Redis connectivity
docker exec city_service_redis redis-cli ping

# Check Redis logs
docker-compose logs redis
```

### Performance Issues

**Slow response times**
- Check `docker stats` for resource usage
- Verify cache hit rates at `/api/v1/cities/metrics`
- Monitor Kafka UI at http://localhost:8080

**Memory usage**
- Adjust cache size in `.env`: `CACHE_MAX_SIZE=5`
- Monitor container memory: `docker stats`

## 🔐 Security

### Production Deployment
- Change default passwords in `.env`
- Restrict CORS origins in `app/main.py`
- Use environment variables for secrets
- Enable SSL/TLS certificates
- Set up proper firewall rules

### Container Security
- Application runs as non-root user
- Read-only CSV file mounting
- Network isolation between services
- Health check validation

## 📚 Development

### Project Structure
```
city-service-api/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application
│   ├── config.py            # Configuration management
│   ├── database.py          # Database connection & ORM
│   ├── models.py            # SQLAlchemy models
│   ├── schemas.py           # Pydantic validation schemas
│   ├── cache.py             # Redis & LRU cache implementation  
│   ├── kafka_logger.py      # Kafka logging & metrics
│   └── api/
│       ├── __init__.py
│       └── cities.py        # API endpoints
├── docker-compose.yml       # Service orchestration
├── Dockerfile              # Python app container
├── requirements.txt        # Python dependencies
├── .env                    # Environment configuration
├── load_csv_data.py        # CSV data loader utility
├── test_api.py             # Comprehensive test suite
├── start.sh                # Startup orchestration script
└── country-code.csv        # Your city data file
```

### Adding New Features
1. **New API Endpoints**: Add to `app/api/cities.py`  
2. **Database Models**: Extend `app/models.py`
3. **Validation**: Update `app/schemas.py`
4. **Caching Logic**: Modify `app/cache.py`
5. **Logging**: Enhance `app/kafka_logger.py`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add comprehensive tests
4. Update documentation
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙋‍♂️ Support

For issues or questions:
1. Check the troubleshooting section above
2. Review container logs: `docker-compose logs`
3. Verify service health: http://localhost:8000/health
4. Open an issue with detailed error information

---

**Built with ❤️ using FastAPI, Docker, and modern Python development practices.**
