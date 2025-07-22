#!/bin/bash

# =============================================================================
# City Service API Startup Script
# =============================================================================
# This script starts the complete City Service API environment including:
# - PostgreSQL database
# - Redis cache
# - Apache Kafka + Zookeeper
# - FastAPI application
# - CSV data loading
# - Comprehensive testing
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Functions for colored output
info() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }
step() { echo -e "${BLUE}[STEP]${NC} $1"; }
success() { echo -e "${CYAN}[SUCCESS]${NC} $1"; }

# Configuration
PROJECT_NAME="City Service API"
TIMEOUT_SECONDS=120
CSV_FILE="country-code.csv"

# =============================================================================
# Step 1: Environment Validation
# =============================================================================
step "Step 1: Validating environment prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    error "Docker is not installed or not in PATH"
    error "Please install Docker Desktop: https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    error "docker-compose is not installed or not in PATH"
    error "Please install docker-compose or update Docker Desktop"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    error "Docker daemon is not running"
    error "Please start Docker Desktop"
    exit 1
fi

# Check CSV file
if [[ ! -f "$CSV_FILE" ]]; then
    warn "CSV file '$CSV_FILE' not found in current directory"
    warn "The application will start without initial data"
    warn "You can load data later using the load_csv_data.py script"
else
    info "Found CSV data file: $CSV_FILE"
fi

success "Environment validation complete ✓"

# =============================================================================
# Step 2: Cleanup Previous Environment
# =============================================================================
step "Step 2: Cleaning up previous environment..."

# Stop and remove existing containers
if [[ $(docker-compose ps -q) ]]; then
    info "Stopping existing containers..."
    docker-compose down -v --remove-orphans
    success "Previous environment cleaned ✓"
else
    info "No existing containers found"
fi

# Optional: Clean up unused Docker resources
read -p "Do you want to clean unused Docker resources? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    info "Cleaning unused Docker resources..."
    docker system prune -f
    success "Docker cleanup complete ✓"
fi

# =============================================================================
# Step 3: Build and Start Services
# =============================================================================
step "Step 3: Building and starting all services..."

info "Starting PostgreSQL, Redis, Kafka, and API services..."
docker-compose up --build -d

if [[ $? -eq 0 ]]; then
    success "All services started successfully ✓"
else
    error "Failed to start services"
    error "Check logs with: docker-compose logs"
    exit 1
fi

# =============================================================================
# Step 4: Wait for Services to be Ready
# =============================================================================
step "Step 4: Waiting for services to be healthy..."

# Wait for PostgreSQL
info "⏳ Waiting for PostgreSQL..."
counter=0
max_attempts=30
while ! docker exec city_service_postgres pg_isready -U postgres -q > /dev/null 2>&1; do
    if [[ $counter -ge $max_attempts ]]; then
        error "PostgreSQL failed to start within $((max_attempts * 2)) seconds"
        error "Check logs with: docker-compose logs postgres"
        exit 1
    fi
    counter=$((counter + 1))
    printf "${YELLOW}.${NC}"
    sleep 2
done
echo
success "PostgreSQL is ready ✓"

# Wait for Redis
info "⏳ Waiting for Redis..."
counter=0
while ! docker exec city_service_redis redis-cli ping > /dev/null 2>&1; do
    if [[ $counter -ge $max_attempts ]]; then
        error "Redis failed to start within $((max_attempts * 2)) seconds"
        error "Check logs with: docker-compose logs redis"
        exit 1
    fi
    counter=$((counter + 1))
    printf "${YELLOW}.${NC}"
    sleep 2
done
echo
success "Redis is ready ✓"

# Wait for Kafka
info "⏳ Waiting for Kafka..."
counter=0
max_kafka_attempts=45  # Kafka takes longer to start
while ! docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    if [[ $counter -ge $max_kafka_attempts ]]; then
        error "Kafka failed to start within $((max_kafka_attempts * 2)) seconds"
        error "Check logs with: docker-compose logs kafka"
        exit 1
    fi
    counter=$((counter + 1))
    printf "${YELLOW}.${NC}"
    sleep 2
done
echo
success "Kafka is ready ✓"

# Wait for FastAPI application
info "⏳ Waiting for FastAPI application..."
counter=0
while ! curl -sf http://localhost:8000/health > /dev/null 2>&1; do
    if [[ $counter -ge $max_attempts ]]; then
        error "FastAPI application failed to start within $((max_attempts * 2)) seconds"
        error "Check logs with: docker-compose logs app"
        exit 1
    fi
    counter=$((counter + 1))
    printf "${YELLOW}.${NC}"
    sleep 2
done
echo
success "FastAPI application is ready ✓"

# =============================================================================
# Step 5: Load CSV Data (Optional)
# =============================================================================
step "Step 5: Loading CSV data..."

if [[ -f "$CSV_FILE" ]]; then
    info "Loading data from $CSV_FILE..."
    
    # Run CSV loader inside the container
    if docker-compose exec -T app python load_csv_data.py; then
        success "CSV data loaded successfully ✓"
    else
        warn "CSV data loading failed, but the application can still run without initial data"
        warn "You can load data later using: docker-compose exec app python load_csv_data.py"
    fi
else
    info "No CSV file found, skipping data loading"
fi

# =============================================================================
# Step 6: Run Comprehensive Tests
# =============================================================================
step "Step 6: Running comprehensive API tests..."

info "Executing test suite inside container..."
if docker-compose exec -T app python test_api.py; then
    success "All tests passed successfully ✓"
else
    error "Some tests failed"
    warn "The application is still running, but there may be issues"
    warn "Check the test output above for details"
fi

# =============================================================================
# Step 7: Display Connection Information
# =============================================================================
step "Step 7: Application ready!"

echo
echo "════════════════════════════════════════════════════════════════"
echo -e "                ${GREEN}🚀 $PROJECT_NAME IS READY! 🚀${NC}"
echo "════════════════════════════════════════════════════════════════"
echo
echo -e "${CYAN}📋 Access Information:${NC}"
echo "   • API Documentation (Swagger): http://localhost:8000/docs"
echo "   • Alternative Docs (ReDoc):     http://localhost:8000/redoc" 
echo "   • Health Check:                http://localhost:8000/health"
echo "   • Application Info:            http://localhost:8000/info"
echo "   • Kafka Management UI:         http://localhost:8080"
echo
echo -e "${CYAN}🔧 API Endpoints:${NC}"
echo "   • Create/Update City:   POST   http://localhost:8000/api/v1/cities/"
echo "   • Get Country Code:     GET    http://localhost:8000/api/v1/cities/{city}/country-code"
echo "   • List Cities:          GET    http://localhost:8000/api/v1/cities/"
echo "   • Performance Metrics:  GET    http://localhost:8000/api/v1/cities/metrics"
echo "   • Delete City:          DELETE http://localhost:8000/api/v1/cities/{city}"
echo
echo -e "${CYAN}🛠️  Management Commands:${NC}"
echo "   • View logs:           docker-compose logs -f"
echo "   • View app logs only:  docker-compose logs -f app"
echo "   • Stop services:       docker-compose down"
echo "   • Restart services:    docker-compose restart"
echo "   • Load CSV data:       docker-compose exec app python load_csv_data.py"
echo
echo -e "${CYAN}📊 Service Status:${NC}"
echo "   • PostgreSQL:    Running on port 5432"
echo "   • Redis:         Running on port 6379"
echo "   • Kafka:         Running on port 9092"
echo "   • FastAPI App:   Running on port 8000"
echo "   • Kafka UI:      Running on port 8080"
echo

# Test a few sample API calls
echo -e "${CYAN}🧪 Quick API Test:${NC}"
echo "   Creating test city..."
if curl -s -X POST http://localhost:8000/api/v1/cities/ \
   -H "Content-Type: application/json" \
   -d '{"name": "Sample City", "country_code": "TEST"}' > /dev/null; then
    echo "   ✅ City creation: Success"
    
    # Test retrieval
    if response=$(curl -s http://localhost:8000/api/v1/cities/Sample%20City/country-code 2>/dev/null); then
        country_code=$(echo "$response" | python3 -c "import sys, json; print(json.load(sys.stdin)['country_code'])" 2>/dev/null || echo "ERROR")
        if [[ "$country_code" == "TEST" ]]; then
            echo "   ✅ Country code retrieval: Success ($country_code)"
        else
            echo "   ⚠️  Country code retrieval: Unexpected result"
        fi
    else
        echo "   ⚠️  Country code retrieval: Failed"
    fi
else
    echo "   ⚠️  City creation: Failed"
fi

echo
echo "════════════════════════════════════════════════════════════════"
echo -e "                    ${GREEN}🎉 READY TO USE! 🎉${NC}"
echo "════════════════════════════════════════════════════════════════"
echo

# Keep the script running to show final status
echo -e "${PURPLE}Press Ctrl+C to stop monitoring, or close this terminal.${NC}"
echo -e "${PURPLE}The services will continue running in the background.${NC}"
echo

# Optional: Monitor logs
read -p "Do you want to monitor application logs? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    info "Monitoring application logs (Press Ctrl+C to stop)..."
    docker-compose logs -f app
fi
