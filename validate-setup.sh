#!/bin/bash

# Validation script to check if setup files are correct
# This doesn't require Docker to be installed

echo "====================================="
echo "  Setup Validation Checklist"
echo "====================================="
echo

# Check if files exist
check_file() {
    if [ -f "$1" ]; then
        echo "✅ $1 exists"
        return 0
    else
        echo "❌ $1 missing"
        return 1
    fi
}

check_dir() {
    if [ -d "$1" ]; then
        echo "✅ $1/ exists"
        return 0
    else
        echo "❌ $1/ missing"
        return 1
    fi
}

echo "Checking project structure..."
check_file "requirements.txt"
check_file "setup.sh"
check_file ".env.example"
check_file ".gitignore"
check_file "README.md"
check_file "SETUP.md"
check_file "docker/docker-compose.yml"
check_file "docker/sql/01-init-database.sql"

echo
check_dir "databricks/bronze"
check_dir "databricks/silver"
check_dir "databricks/gold"
check_dir "databricks/workflows"
check_dir "backend/api"
check_dir "backend/models"
check_dir "backend/database"
check_dir "backend/tests"
check_dir "frontend"
check_dir "docs"

echo
echo "Checking file permissions..."
if [ -x "setup.sh" ]; then
    echo "✅ setup.sh is executable"
else
    echo "⚠️  setup.sh is not executable (run: chmod +x setup.sh)"
fi

echo
echo "Checking requirements.txt..."
REQUIRED_PACKAGES=(
    "fastapi"
    "uvicorn"
    "pydantic"
    "pydantic-settings"
    "sqlalchemy"
    "pymssql"
    "pyodbc"
    "databricks-sql-connector"
    "delta-spark"
    "azure-storage-blob"
    "python-dotenv"
    "pytest"
    "httpx"
)

for pkg in "${REQUIRED_PACKAGES[@]}"; do
    if grep -q "$pkg" requirements.txt; then
        echo "✅ $pkg in requirements.txt"
    else
        echo "❌ $pkg missing from requirements.txt"
    fi
done

echo
echo "Checking .env.example variables..."
REQUIRED_VARS=(
    "DATABASE_URL"
    "DATABRICKS_HOST"
    "DATABRICKS_TOKEN"
    "DATABRICKS_HTTP_PATH"
    "AZURE_STORAGE_CONNECTION_STRING"
    "TENANT_ID"
)

for var in "${REQUIRED_VARS[@]}"; do
    if grep -q "$var" .env.example; then
        echo "✅ $var in .env.example"
    else
        echo "❌ $var missing from .env.example"
    fi
done

echo
echo "Checking docker-compose.yml..."
REQUIRED_SERVICES=(
    "sqlserver"
    "minio"
    "redis"
)

for service in "${REQUIRED_SERVICES[@]}"; do
    if grep -q "$service:" docker/docker-compose.yml; then
        echo "✅ $service service defined"
    else
        echo "❌ $service service missing"
    fi
done

echo
echo "====================================="
echo "  Validation Complete"
echo "====================================="
echo
echo "Next steps:"
echo "1. Install Docker Desktop with WSL 2 backend"
echo "2. Run: ./setup.sh"
echo "3. Follow instructions in SETUP.md"
