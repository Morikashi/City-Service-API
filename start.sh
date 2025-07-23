#!/bin/bash

# City Service API - Production-Ready Startup Script
# Comprehensive orchestration with health checks and error handling

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# =============================================================================
# CONFIGURATION
# =============================================================================

# Colors for output formatting
readonly COLOR_GREEN='\033[0;32m'
readonly COLOR_YELLOW='\033[1;33m'
readonly COLOR_RED='\033[0;31m'
readonly COLOR_BLUE='\033[0;34m'
readonly COLOR_RESET='\033[0m'

# Timing constants
readonly MAX_RETRIES=30
readonly RETRY_DELAY=2
readonly KAFKA_MAX_RETRIES=45
readonly API_MAX_RETRIES=30

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

log() {
    echo -e "${COLOR_BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${COLOR_RESET} $1"
}

success() {
    echo -e "${COLOR_GREEN}[SUCCESS]${COLOR_RESET} $1"
}

warning() {
    echo -e "${COLOR_YELLOW}[WARNING]${COLOR_RESET} $1"
}

error() {
    echo -e "${COLOR_RED}[ERROR]${COLOR_RESET} $1" >&2
}

# Wait for a service with health check
wait_for_service() {
    local service_name="$1"
    local check_command="$2"
    local max_retries="$3"
    local delay="$4"
    
    log "⏳ Waiting for ${service_name} to be ready..."
    
    local counter=0
    while ! eval "$check_command" >/dev/null 2>&1; do
        if [ $counter -ge $max_retries ]; then
            error "${service_name} failed to become ready within $((max_retries * delay)) seconds"
            return 1
        fi
        counter=$((counter + 1))
        printf "."
        sleep "$delay"
    done
    
    success "${service_name} is ready! ✓"
    return 0
}

# Validate prerequisites
check_prerequisites() {
    log "🔍 Checking prerequisites..."
    
    local missing_tools=()
    
    if ! command -v docker >/dev/null 2>&1; then
        missing_tools+=("docker")
    fi
    
    if ! command -v docker-compose >/dev/null 2>&1; then
        missing_tools+=("docker-compose")
    fi
    
    if [ ${#missing_tools[@]} -gt 0 ]; then
        error "Missing required tools: ${missing_tools[*]}"
        error "Please install the missing tools and try again"
        exit 1
    fi
    
    success "All prerequisites satisfied ✓"
}

# Clean up previous deployments
cleanup_previous() {
    log "🧹 Cleaning up previous deployment..."
    
    if [ "$(docker-compose ps -q)" ]; then
        docker-compose down -v --remove-orphans
        success "Previous environment cleaned up ✓"
    else
        log "No previous deployment found, skipping cleanup"
    fi
}

# Start all services
start_services() {
    log "🚀 Starting all services..."
    
    if ! docker-compose up --build -d; then
        error "Failed to start services"
        return 1
    fi
    
    success "Services started successfully ✓"
}

# Check service health
check_services() {
    log "🏥 Performing health checks..."
    
    # PostgreSQL health check
    if ! wait_for_service "PostgreSQL" "docker exec city_service_postgres pg_isready -U postgres -d citydb" $MAX_RETRIES $RETRY_DELAY; then
        error "PostgreSQL health check failed"
        return 1
    fi
    
    # Redis health check
    if ! wait_for_service "Redis" "docker exec city_service_redis redis-cli ping | grep -q PONG" $MAX_RETRIES $RETRY_DELAY; then
        error "Redis health check failed"
        return 1
    fi
    
    # Kafka health check (longer timeout as Kafka takes more time)
    if ! wait_for_service "Kafka" "docker exec city_service_kafka kafka-topics --bootstrap-server localhost:9092 --list" $KAFKA_MAX_RETRIES $RETRY_DELAY; then
        error "Kafka health check failed"
        return 1
    fi
    
    # FastAPI application health check
    if ! wait_for_service "FastAPI Application" "curl -f http://localhost:8000/health" $API_MAX_RETRIES $RETRY_DELAY; then
        error "FastAPI application health check failed"
        return 1
    fi
    
    success "All services are healthy! 🎉"
}

# Load CSV data
load_csv_data() {
    log "📊 Loading CSV data..."
    
    if [ ! -f "country-code.csv" ]; then
        warning "CSV file 'country-code.csv' not found. Skipping data loading."
        return 0
    fi
    
    if docker-compose exec -T app python scripts/load_csv_data.py; then
        success "CSV data loaded successfully ✓"
    else
        warning "CSV data loading failed, but continuing..."
    fi
}

# Run comprehensive tests
run_tests() {
    log "🧪 Running API tests..."
    
    if docker-compose exec -T app python scripts/test_api.py; then
        success "All tests passed! ✓"
    else
        error "Some tests failed"
        return 1
    fi
}

# Display access information
show_access_info() {
    log "📋 Application Access Information:"
    echo
    echo "🌐 Web Interfaces:"
    echo "   API Documentation (Swagger): http://localhost:8000/docs"
    echo "   API Documentation (ReDoc):   http://localhost:8000/redoc" 
    echo "   Health Check:                http://localhost:8000/health"
    echo "   Kafka UI:                    http://localhost:8080"
    echo
    echo "🔌 Direct Connections:"
    echo "   PostgreSQL: postgresql://postgres:password@localhost:5432/citydb"
    echo "   Redis:      redis://localhost:6379"
    echo "   Kafka:      localhost:9092"
    echo
    echo "🛠️  Management Commands:"
    echo "   View logs:      docker-compose logs -f"
    echo "   Stop services:  docker-compose down"
    echo "   Restart:        docker-compose restart"
    echo
    echo "🚀 API Endpoints:"
    echo "   POST   /api/v1/cities/              - Create/update city"
    echo "   GET    /api/v1/cities/{city}/country-code - Get country code" 
    echo "   GET    /api/v1/cities/              - List cities"
    echo "   GET    /api/v1/cities/metrics       - Performance metrics"
    echo "   DELETE /api/v1/cities/{city}        - Delete city"
    echo
}

# Main execution flow
main() {
    log "🎯 Starting City Service API deployment..."
    echo
    
    # Step 1: Prerequisites
    check_prerequisites
    
    # Step 2: Cleanup
    cleanup_previous
    
    # Step 3: Start services
    if ! start_services; then
        error "Failed to start services"
        exit 1
    fi
    
    # Step 4: Health checks
    if ! check_services; then
        error "Health checks failed"
        log "💡 Debug commands:"
        log "   docker-compose logs app"
        log "   docker-compose ps"
        exit 1
    fi
    
    # Step 5: Load data
    load_csv_data
    
    # Step 6: Run tests
    if ! run_tests; then
        error "Tests failed"
        exit 1
    fi
    
    # Step 7: Success message
    echo
    success "🎉 City Service API is fully operational!"
    success "🎉 All services healthy, data loaded, tests passed!"
    echo
    
    # Step 8: Show access information
    show_access_info
    
    log "✨ Deployment completed successfully!"
}

# Trap for cleanup on script interruption
trap 'error "Script interrupted"; exit 130' INT TERM

# Run main function
main "$@"
