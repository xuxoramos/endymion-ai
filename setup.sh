#!/bin/bash

# CattleSaaS Development Environment Setup Script
# This script sets up the complete development environment for WSL/Linux

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to wait for service to be ready
wait_for_service() {
    local service=$1
    local max_attempts=$2
    local attempt=0

    print_info "Waiting for $service to be ready..."
    
    while [ $attempt -lt $max_attempts ]; do
        if docker compose -f docker/docker-compose.yml ps | grep -q "$service.*healthy"; then
            print_success "$service is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done
    
    print_error "$service failed to start within expected time"
    return 1
}

print_info "=========================================="
print_info "  CattleSaaS Development Setup"
print_info "=========================================="
echo

# Step 1: Check prerequisites
print_info "Step 1: Checking prerequisites..."

if ! command_exists python3; then
    print_error "Python 3 is not installed. Please install Python 3.9 or higher."
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
print_success "Python $PYTHON_VERSION found"

if ! command_exists docker; then
    print_error "Docker is not installed. Please install Docker Desktop for WSL."
    exit 1
fi
print_success "Docker found"

if ! command_exists docker-compose && ! docker compose version >/dev/null 2>&1; then
    print_error "Docker Compose is not installed."
    exit 1
fi
print_success "Docker Compose found"

# Step 2: Create Python virtual environment
print_info "Step 2: Setting up Python virtual environment..."

if [ -d "venv" ]; then
    print_warning "Virtual environment already exists. Skipping creation."
else
    python3 -m venv venv
    print_success "Virtual environment created"
fi

# Activate virtual environment
source .venv/bin/activate
print_success "Virtual environment activated"

# Step 3: Upgrade pip
print_info "Step 3: Upgrading pip..."
python -m pip install --upgrade pip setuptools wheel
print_success "pip upgraded"

# Step 4: Install Python dependencies
print_info "Step 4: Installing Python dependencies..."
pip install -r requirements.txt
print_success "Dependencies installed"

# Step 5: Create .env file if it doesn't exist
print_info "Step 5: Setting up environment variables..."

if [ ! -f ".env" ]; then
    cp .env.example .env
    print_success ".env file created from .env.example"
    print_warning "Please edit .env file with your actual configuration"
else
    print_warning ".env file already exists. Skipping."
fi

# Step 6: Stop any existing containers
print_info "Step 6: Cleaning up existing containers..."
docker compose -f docker/docker-compose.yml down -v 2>/dev/null || true
print_success "Cleanup complete"

# Step 7: Start Docker containers
print_info "Step 7: Starting Docker containers..."
docker compose -f docker/docker-compose.yml up -d

# Step 8: Wait for services to be ready
print_info "Step 8: Waiting for services to start..."

# Wait for SQL Server
print_info "Waiting for SQL Server..."
sleep 10
max_wait=60
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    if docker exec cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1" -C >/dev/null 2>&1; then
        print_success "SQL Server is ready!"
        break
    fi
    echo -n "."
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $max_wait ]; then
    print_error "SQL Server failed to start"
    exit 1
fi

# Wait for MinIO
print_info "Waiting for MinIO..."
sleep 5
max_wait=30
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        print_success "MinIO is ready!"
        break
    fi
    echo -n "."
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $max_wait ]; then
    print_error "MinIO failed to start"
    exit 1
fi

# Wait for Redis
print_info "Waiting for Redis..."
sleep 3
if docker exec cattlesaas-redis redis-cli ping >/dev/null 2>&1; then
    print_success "Redis is ready!"
else
    print_warning "Redis may not be ready, but continuing..."
fi

# Step 9: Run SQL initialization
print_info "Step 9: Initializing database..."

# Check if init script exists and run it
if [ -f "docker/sql/01-init-database.sql" ]; then
    docker exec -i cattlesaas-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -C < docker/sql/01-init-database.sql
    print_success "Database initialized"
else
    print_warning "Database initialization script not found"
fi

# Step 10: Verify MinIO buckets
print_info "Step 10: Verifying MinIO buckets..."
sleep 2
BUCKETS=$(docker exec cattlesaas-minio-client mc ls localminio 2>/dev/null | wc -l) || BUCKETS=0
if [ "$BUCKETS" -gt 0 ]; then
    print_success "MinIO buckets verified"
    docker exec cattlesaas-minio-client mc ls localminio 2>/dev/null || true
else
    print_warning "MinIO buckets may not be created yet"
fi

# Step 11: Display status
print_info "Step 11: Checking service status..."
echo
docker compose -f docker/docker-compose.yml ps
echo

# Final summary
print_info "=========================================="
print_success "Development Environment Setup Complete!"
print_info "=========================================="
echo
print_info "Services running:"
echo "  • SQL Server:    localhost:1433"
echo "  • MinIO API:     http://localhost:9000"
echo "  • MinIO Console: http://localhost:9001"
echo "  • Redis:         localhost:6379"
echo
print_info "Credentials:"
echo "  • SQL Server:    sa / YourStrong!Passw0rd"
echo "  • MinIO:         minioadmin / minioadmin"
echo
print_info "Next steps:"
echo "  1. Edit .env file with your configuration"
echo "  2. Activate virtual environment: source .venv/bin/activate"
echo "  3. Run the backend: cd backend && uvicorn api.main:app --reload"
echo
print_info "To stop services: docker compose -f docker/docker-compose.yml down"
print_info "To view logs: docker compose -f docker/docker-compose.yml logs -f"
echo
