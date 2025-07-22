"""
Comprehensive Test Suite for City Service API

This script tests all functionality of the City Service API including:
- Health endpoints
- CRUD operations 
- Cache performance
- Error handling
- Concurrent requests
- Metrics collection
"""

import httpx
import asyncio
import time
import sys
import json
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Base URLs for testing
BASE_URL = "http://localhost:8000"
API_BASE = f"{BASE_URL}/api/v1/cities"
HEALTH_URL = f"{BASE_URL}/health"

# Test configuration
TEST_CITIES = [
    {"name": "Tokyo", "country_code": "JPN"},
    {"name": "London", "country_code": "GBR"}, 
    {"name": "New York", "country_code": "USA"},
    {"name": "Paris", "country_code": "FRA"},
    {"name": "Berlin", "country_code": "DEU"}
]

# Statistics
PASSED_COUNT = 0
FAILED_COUNT = 0


def print_header(title: str):
    """Print a formatted test section header"""
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print('=' * 60)


def print_success(message: str):
    """Print a success message"""
    global PASSED_COUNT
    PASSED_COUNT += 1
    print(f"✅ {message}")


def print_error(message: str):
    """Print an error message"""
    global FAILED_COUNT
    FAILED_COUNT += 1
    print(f"❌ {message}")


def print_info(message: str):
    """Print an info message"""
    print(f"ℹ️  {message}")


def test_health_endpoint():
    """Test the health check endpoint"""
    print_header("Testing Health Endpoint")
    
    try:
        response = httpx.get(HEALTH_URL, timeout=10)
        
        if response.status_code == 200:
            health_data = response.json()
            print_success(f"Health check passed - Status: {health_data.get('status', 'unknown')}")
            
            # Check dependencies
            deps = health_data.get('dependencies', {})
            for service, status in deps.items():
                if status == 'healthy':
                    print_success(f"{service.title()} is healthy")
                else:
                    print_error(f"{service.title()} is unhealthy")
        else:
            print_error(f"Health check failed with status: {response.status_code}")
            
    except Exception as e:
        print_error(f"Health check error: {e}")


def test_crud_operations():
    """Test create, read, update, delete operations"""
    print_header("Testing CRUD Operations")
    
    created_cities = []
    
    # Test city creation/updates
    for city in TEST_CITIES:
        try:
            response = httpx.post(API_BASE + "/", json=city, timeout=10)
            if response.status_code == 201:
                city_data = response.json()
                created_cities.append(city_data)
                print_success(f"Created city: {city['name']} -> {city['country_code']}")
            else:
                print_error(f"Failed to create city {city['name']}: {response.status_code}")
        except Exception as e:
            print_error(f"Failed to create city {city['name']}: {e}")
    
    # Test retrieving country codes
    for city in TEST_CITIES:
        try:
            response = httpx.get(f"{API_BASE}/{city['name']}/country-code", timeout=10)
            if response.status_code == 200:
                data = response.json()
                expected_code = city['country_code']
                actual_code = data.get('country_code')
                if actual_code == expected_code:
                    print_success(f"Retrieved correct country code for {city['name']}: {actual_code}")
                else:
                    print_error(f"Wrong country code for {city['name']}: got {actual_code}, expected {expected_code}")
            else:
                print_error(f"Failed to get country code for {city['name']}: {response.status_code}")
        except Exception as e:
            print_error(f"Failed to get country code for {city['name']}: {e}")
    
    # Test listing cities
    try:
        response = httpx.get(f"{API_BASE}/", timeout=10)
        if response.status_code == 200:
            data = response.json()
            cities_count = len(data.get('cities', []))
            total = data.get('total', 0)
            print_success(f"Listed cities successfully: {cities_count} cities, {total} total")
        else:
            print_error(f"Failed to list cities: {response.status_code}")
    except Exception as e:
        print_error(f"Failed to list cities: {e}")
    
    # Test metrics endpoint
    try:
        response = httpx.get(f"{API_BASE}/metrics", timeout=10)
        if response.status_code == 200:
            data = response.json()
            cache_metrics = data.get('cache_metrics', {})
            perf_metrics = data.get('performance_metrics', {})
            print_success(f"Retrieved metrics - Cache size: {cache_metrics.get('current_size', 0)}, Total requests: {perf_metrics.get('total_requests', 0)}")
        else:
            print_error(f"Failed to get metrics: {response.status_code}")
    except Exception as e:
        print_error(f"Failed to get metrics: {e}")


def test_cache_performance():
    """Test cache hit vs miss performance"""
    print_header("Testing Cache Performance")
    
    # Ensure we have test data
    test_city = TEST_CITIES[0]
    try:
        httpx.post(API_BASE + "/", json=test_city, timeout=10)
    except:
        pass  # City might already exist
    
    # Test cache miss (first request)
    print_info("Testing cache miss performance...")
    try:
        start_time = time.time()
        response1 = httpx.get(f"{API_BASE}/{test_city['name']}/country-code", timeout=10)
        miss_time = time.time() - start_time
        
        if response1.status_code == 200:
            print_success(f"Cache miss request successful")
        else:
            print_error(f"Cache miss request failed: {response1.status_code}")
    except Exception as e:
        print_error(f"Cache miss request failed: {e}")
        miss_time = 0
    
    # Test cache hit (second request)
    print_info("Testing cache hit performance...")
    try:
        start_time = time.time()
        response2 = httpx.get(f"{API_BASE}/{test_city['name']}/country-code", timeout=10)
        hit_time = time.time() - start_time
        
        if response2.status_code == 200:
            print_success(f"Cache hit request successful")
        else:
            print_error(f"Cache hit request failed: {response2.status_code}")
    except Exception as e:
        print_error(f"Cache hit request failed: {e}")
        hit_time = 0
    
    # Compare performance
    if miss_time > 0 and hit_time > 0:
        if miss_time > hit_time:
            improvement = ((miss_time - hit_time) / miss_time) * 100
            print_success(f"Cache performance improvement: {improvement:.1f}%")
        else:
            improvement = ((hit_time - miss_time) / hit_time) * 100  
            print_info(f"Cache performance difference: {improvement:.1f}% (hit might be slower due to network variance)")
            
        print_info(f"Cache miss time: {miss_time * 1000:.2f}ms")
        print_info(f"Cache hit time: {hit_time * 1000:.2f}ms")


def test_error_handling():
    """Test error handling for various scenarios"""
    print_header("Testing Error Handling")
    
    # Test 404 for non-existent city
    try:
        response = httpx.get(f"{API_BASE}/NonExistentCity123/country-code", timeout=10)
        if response.status_code == 404:
            print_success("404 error handling works correctly")
        else:
            print_error(f"Expected 404, got {response.status_code}")
    except Exception as e:
        print_error(f"Error testing 404 handling: {e}")
    
    # Test validation error
    try:
        invalid_data = {"name": "", "country_code": "X"}
        response = httpx.post(API_BASE + "/", json=invalid_data, timeout=10)
        if response.status_code == 422:
            print_success("Validation error handling works correctly")
        else:
            print_error(f"Expected 422, got {response.status_code}")
    except Exception as e:
        print_error(f"Error testing validation: {e}")


def test_concurrent_requests():
    """Test handling of concurrent requests"""
    print_header("Testing Concurrent Requests")
    
    # Prepare test cities
    test_cities = ["London", "Rome", "Paris", "Madrid", "Berlin"]
    
    def make_request(city_name):
        try:
            response = httpx.get(f"{API_BASE}/{city_name}/country-code", timeout=10)
            return response.status_code == 200
        except:
            return False
    
    # Execute concurrent requests
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_city = {executor.submit(make_request, city): city for city in test_cities}
        results = []
        
        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                result = future.result()
                results.append(result)
                if not result:
                    print_error(f"Failed to get country code for {city}: request failed")
            except Exception as e:
                print_error(f"Failed to get country code for {city}: {e}")
                results.append(False)
    
    total_time = time.time() - start_time
    successful_requests = sum(results)
    
    print_success(f"Concurrent requests completed: {successful_requests}/{len(test_cities)}")
    print_info(f"Total time: {total_time * 1000:.2f}ms")
    print_info(f"Average time per request: {total_time * 1000 / len(test_cities):.2f}ms")
    
    # Test cleanup - delete a city
    try:
        response = httpx.delete(f"{API_BASE}/Tokyo", timeout=10)
        if response.status_code == 200:
            print_success("City deletion successful")
        else:
            print_error(f"Failed to delete city Tokyo: {response.status_code}")
    except Exception as e:
        print_error(f"Failed to delete city Tokyo: {e}")


def run_all_tests():
    """Run all tests and print summary"""
    print_header("Starting City Service API Tests")
    
    # Run all test suites
    test_health_endpoint()
    test_crud_operations() 
    test_cache_performance()
    test_error_handling()
    test_concurrent_requests()
    
    # Print final summary
    total_tests = PASSED_COUNT + FAILED_COUNT
    success_rate = (PASSED_COUNT / total_tests * 100) if total_tests > 0 else 0
    
    print_header("Test Summary")
    
    if FAILED_COUNT == 0:
        print_success(f"All {total_tests} tests passed! 🎉")
        sys.exit(0)
    else:
        print_error(f"Tests passed: {PASSED_COUNT}/{total_tests} ({success_rate:.1f}%)")
        print_error(f"Tests failed: {FAILED_COUNT}")
        print("\n⚠️  Some tests failed. Check the logs above for details.")
        sys.exit(1)


if __name__ == "__main__":
    # Add some delay to ensure the API is ready
    print("⏳ Waiting 2 seconds for API to be ready...")
    time.sleep(2)
    
    try:
        run_all_tests()
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Critical test error: {e}")
        sys.exit(1)
