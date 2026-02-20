# Endymion-AI Production Deployment Guide

Complete guide for deploying Endymion-AI to Azure production environment.

## Table of Contents

1. [Azure Databricks Setup](#1-azure-databricks-setup)
2. [Azure SQL Database](#2-azure-sql-database)
3. [ADLS Gen2 Storage](#3-adls-gen2-storage)
4. [Migration Checklist](#4-migration-checklist)
5. [Security Hardening](#5-security-hardening)
6. [Cost Optimization](#6-cost-optimization)
7. [Post-Deployment Validation](#7-post-deployment-validation)

---

## Prerequisites

- **Azure Subscription** with appropriate permissions
- **Azure CLI** installed (`az --version`)
- **Terraform** (optional, for IaC)
- **Access to source repository**
- **Service Principal** for automation

---

## 1. Azure Databricks Setup

### 1.1 Create Databricks Workspace

#### Using Azure Portal

1. Navigate to Azure Portal → Create a resource → Azure Databricks
2. Configure workspace:
   - **Subscription**: Select subscription
   - **Resource Group**: `rg-endymion-ai-prod`
   - **Workspace Name**: `dbw-endymion-ai-prod`
   - **Region**: `East US 2` (or preferred)
   - **Pricing Tier**: `Premium` (required for Unity Catalog)

#### Using Azure CLI

```bash
# Create resource group
az group create \
  --name rg-endymion-ai-prod \
  --location eastus2

# Create Databricks workspace
az databricks workspace create \
  --resource-group rg-endymion-ai-prod \
  --name dbw-endymion-ai-prod \
  --location eastus2 \
  --sku premium
```

#### Using Terraform

```hcl
# terraform/databricks.tf

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "endymion-ai" {
  name     = "rg-endymion-ai-prod"
  location = "East US 2"
}

resource "azurerm_databricks_workspace" "endymion-ai" {
  name                = "dbw-endymion-ai-prod"
  resource_group_name = azurerm_resource_group.endymion-ai.name
  location            = azurerm_resource_group.endymion-ai.location
  sku                 = "premium"

  tags = {
    Environment = "Production"
    Project     = "Endymion-AI"
  }
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.endymion-ai.workspace_url
}
```

### 1.2 Configure Unity Catalog

**Unity Catalog** provides centralized governance for data and AI assets.

#### Enable Unity Catalog

```bash
# Get Databricks workspace URL
DATABRICKS_HOST=$(az databricks workspace show \
  --resource-group rg-endymion-ai-prod \
  --name dbw-endymion-ai-prod \
  --query workspaceUrl -o tsv)

# Create metastore (one per region)
# Use Databricks CLI or Terraform
```

#### Terraform Configuration

```hcl
# terraform/unity-catalog.tf

resource "databricks_metastore" "endymion-ai" {
  name          = "endymion-ai-metastore"
  region        = "eastus2"
  storage_root  = "abfss://unity-catalog@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
  force_destroy = false
}

resource "databricks_metastore_assignment" "endymion-ai" {
  metastore_id = databricks_metastore.endymion-ai.id
  workspace_id = azurerm_databricks_workspace.endymion-ai.workspace_id
}

# Create catalog
resource "databricks_catalog" "endymion-ai" {
  name    = "endymion_ai_prod"
  comment = "Endymion-AI production catalog"
  
  properties = {
    environment = "production"
  }
}

# Create schemas
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.endymion-ai.name
  name         = "bronze"
  comment      = "Bronze layer - raw events"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.endymion-ai.name
  name         = "silver"
  comment      = "Silver layer - canonical state with SCD Type 2"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.endymion-ai.name
  name         = "gold"
  comment      = "Gold layer - pre-aggregated analytics"
}
```

### 1.3 Set Up Row-Level Security

**Row-Level Security (RLS)** for multi-tenant isolation.

#### Create Security Model

```sql
-- Databricks SQL notebook: setup-rls.sql

-- Enable row-level security on Silver tables
CREATE OR REPLACE TABLE endymion_ai_prod.silver.cows (
  cow_id STRING,
  tenant_id STRING,  -- Partition key for RLS
  breed STRING,
  birth_date DATE,
  sex STRING,
  is_current BOOLEAN,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  CONSTRAINT pk_cow PRIMARY KEY (cow_id, valid_from)
) USING DELTA
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
);

-- Create row filter function
CREATE OR REPLACE FUNCTION endymion_ai_prod.silver.tenant_filter(tenant_id STRING)
RETURN 
  CASE
    WHEN IS_MEMBER('admin') THEN TRUE
    WHEN tenant_id = current_user() THEN TRUE
    ELSE FALSE
  END;

-- Apply row filter
ALTER TABLE endymion_ai_prod.silver.cows 
SET ROW FILTER endymion_ai_prod.silver.tenant_filter ON (tenant_id);

-- Grant permissions
GRANT SELECT, MODIFY ON TABLE endymion_ai_prod.silver.cows TO `tenant-users`;
```

#### Configure Access Control

```python
# databricks/setup_permissions.py

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Grant catalog permissions
w.grants.update(
    securable_type="catalog",
    full_name="endymion_ai_prod",
    changes=[
        {
            "principal": "tenant-admin-group",
            "add": ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
        }
    ]
)

# Grant schema permissions
for schema in ["bronze", "silver", "gold"]:
    w.grants.update(
        securable_type="schema",
        full_name=f"endymion_ai_prod.{schema}",
        changes=[
            {
                "principal": "data-engineers",
                "add": ["USE_SCHEMA", "SELECT", "MODIFY"]
            }
        ]
    )
```

### 1.4 Create Delta Live Tables (DLT) Pipelines

**Migrate local PySpark code to DLT pipelines.**

#### Bronze Layer Pipeline

```python
# databricks/dlt/bronze_pipeline.py

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_cow_events",
    comment="Raw cow events from SQL outbox",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "event_timestamp,aggregate_id"
    }
)
def bronze_cow_events():
    """
    Read events from SQL outbox and write to Bronze layer.
    """
    return (
        spark.readStream
        .format("jdbc")
        .option("url", "jdbc:sqlserver://endymion-ai-sql.database.windows.net:1433;database=endymion_ai")
        .option("dbtable", "events.cow_events")
        .option("user", spark.conf.get("sql.username"))
        .option("password", spark.conf.get("sql.password"))
        .load()
        .withColumn("ingestion_timestamp", current_timestamp())
    )
```

#### Silver Layer Pipeline

```python
# databricks/dlt/silver_pipeline.py

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dlt.table(
    name="silver_cows",
    comment="Canonical cow state with SCD Type 2 history",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_cow_id", "cow_id IS NOT NULL")
@dlt.expect_or_drop("valid_breed", "breed IS NOT NULL")
def silver_cows():
    """
    Process Bronze events into Silver state with SCD Type 2.
    """
    bronze_df = dlt.read_stream("bronze_cow_events")
    
    # Group events by cow_id and process in order
    window_spec = Window.partitionBy("aggregate_id").orderBy("event_timestamp")
    
    # Build state transitions
    state_df = (
        bronze_df
        .withColumn("row_num", row_number().over(window_spec))
        .withColumn("is_current", 
            col("row_num") == max("row_num").over(Window.partitionBy("aggregate_id")))
        .withColumn("valid_from", col("event_timestamp"))
        .withColumn("valid_to", 
            lead("event_timestamp").over(window_spec)
            .otherwise(lit("9999-12-31").cast("timestamp")))
        .select(
            col("aggregate_id").alias("cow_id"),
            col("data.breed").alias("breed"),
            col("data.birth_date").cast("date").alias("birth_date"),
            col("data.sex").alias("sex"),
            col("is_current"),
            col("valid_from"),
            col("valid_to")
        )
    )
    
    return state_df
```

#### Gold Layer Pipeline

```python
# databricks/dlt/gold_pipeline.py

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_herd_analytics",
    comment="Pre-aggregated herd analytics",
    table_properties={
        "quality": "gold"
    }
)
def gold_herd_analytics():
    """
    Aggregate Silver data for analytics.
    """
    silver_df = dlt.read("silver_cows").filter(col("is_current") == True)
    
    analytics_df = (
        silver_df
        .groupBy("breed")
        .agg(
            count("*").alias("cow_count"),
            avg(datediff(current_date(), col("birth_date")) / 365.25).alias("avg_age_years"),
            min("birth_date").alias("oldest_birth_date"),
            max("birth_date").alias("youngest_birth_date")
        )
        .withColumn("last_updated", current_timestamp())
    )
    
    return analytics_df
```

#### Deploy DLT Pipeline

```bash
# Using Databricks CLI

# Create pipeline configuration
cat > dlt-pipeline.json <<EOF
{
  "name": "endymion-ai-production-pipeline",
  "storage": "abfss://delta-lake@endymion-aiadls.dfs.core.windows.net/",
  "target": "endymion_ai_prod",
  "continuous": true,
  "libraries": [
    {
      "notebook": {
        "path": "/Workspace/endymion-ai/dlt/bronze_pipeline"
      }
    },
    {
      "notebook": {
        "path": "/Workspace/endymion-ai/dlt/silver_pipeline"
      }
    },
    {
      "notebook": {
        "path": "/Workspace/endymion-ai/dlt/gold_pipeline"
      }
    }
  ],
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": 1,
        "max_workers": 5
      }
    }
  ]
}
EOF

# Create pipeline
databricks pipelines create --settings dlt-pipeline.json

# Start pipeline
databricks pipelines start --pipeline-id <pipeline-id>
```

### 1.5 Configure Databricks Workflows

**Automate sync jobs and maintenance tasks.**

```json
// databricks/workflows/sync-workflow.json
{
  "name": "Endymion-AI Sync Job",
  "schedule": {
    "quartz_cron_expression": "0 */30 * * * ?",
    "timezone_id": "America/New_York"
  },
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "sync_silver_to_sql",
      "notebook_task": {
        "notebook_path": "/Workspace/endymion-ai/sync/sync_silver_to_sql",
        "base_parameters": {
          "table_name": "cows"
        }
      },
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_D4s_v3",
        "num_workers": 2
      }
    },
    {
      "task_key": "update_gold_analytics",
      "depends_on": [
        {"task_key": "sync_silver_to_sql"}
      ],
      "notebook_task": {
        "notebook_path": "/Workspace/endymion-ai/gold/update_analytics"
      },
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_D4s_v3",
        "num_workers": 1
      }
    }
  ]
}
```

---

## 2. Azure SQL Database

### 2.1 Provisioning

#### Using Azure Portal

1. Navigate to Azure Portal → Create a resource → SQL Database
2. Configure database:
   - **Server**: Create new → `endymion-ai-sql.database.windows.net`
   - **Database Name**: `endymion_ai`
   - **Compute + Storage**: General Purpose, 2 vCores
   - **Backup Storage**: Geo-redundant

#### Using Azure CLI

```bash
# Create SQL Server
az sql server create \
  --resource-group rg-endymion-ai-prod \
  --name endymion-ai-sql \
  --location eastus2 \
  --admin-user sqladmin \
  --admin-password '<strong-password>'

# Create SQL Database
az sql db create \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name endymion-ai \
  --edition GeneralPurpose \
  --family Gen5 \
  --capacity 2 \
  --compute-model Provisioned \
  --zone-redundant false

# Configure geo-replication (optional)
az sql db replica create \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name endymion-ai \
  --partner-server endymion-ai-sql-secondary \
  --partner-resource-group rg-endymion-ai-dr
```

#### Using Terraform

```hcl
# terraform/sql-database.tf

resource "azurerm_mssql_server" "endymion-ai" {
  name                         = "endymion-ai-sql"
  resource_group_name          = azurerm_resource_group.endymion-ai.name
  location                     = azurerm_resource_group.endymion-ai.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = var.sql_admin_password

  azuread_administrator {
    login_username = "AzureAD Admin"
    object_id      = var.azuread_admin_object_id
  }

  tags = {
    Environment = "Production"
  }
}

resource "azurerm_mssql_database" "endymion-ai" {
  name           = "endymion-ai"
  server_id      = azurerm_mssql_server.endymion-ai.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  max_size_gb    = 250
  sku_name       = "GP_Gen5_2"
  zone_redundant = false

  short_term_retention_policy {
    retention_days = 7
  }

  long_term_retention_policy {
    weekly_retention  = "P4W"
    monthly_retention = "P12M"
    yearly_retention  = "P5Y"
    week_of_year      = 1
  }

  tags = {
    Environment = "Production"
  }
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.endymion-ai.fully_qualified_domain_name
}
```

### 2.2 Connection Strings

**Store in Azure Key Vault:**

```bash
# Create Key Vault
az keyvault create \
  --resource-group rg-endymion-ai-prod \
  --name kv-endymion-ai-prod \
  --location eastus2

# Store connection string
az keyvault secret set \
  --vault-name kv-endymion-ai-prod \
  --name sql-connection-string \
  --value "Server=tcp:endymion-ai-sql.database.windows.net,1433;Database=endymion_ai;User ID=sqladmin;Password=<password>;Encrypt=yes;Connection Timeout=30;"
```

**Connection String Formats:**

```bash
# ADO.NET
Server=tcp:endymion-ai-sql.database.windows.net,1433;Initial Catalog=endymion_ai;Persist Security Info=False;User ID=sqladmin;Password=<password>;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;

# JDBC
jdbc:sqlserver://endymion-ai-sql.database.windows.net:1433;database=endymion_ai;user=sqladmin;password=<password>;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;

# Python (pyodbc)
DRIVER={ODBC Driver 18 for SQL Server};SERVER=endymion-ai-sql.database.windows.net;DATABASE=endymion_ai;UID=sqladmin;PWD=<password>
```

### 2.3 Firewall Rules

```bash
# Allow Azure services
az sql server firewall-rule create \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name AllowAzureServices \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0

# Allow specific IP (office, CI/CD)
az sql server firewall-rule create \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name AllowOfficeIP \
  --start-ip-address 203.0.113.0 \
  --end-ip-address 203.0.113.255

# Allow Databricks (use Private Link for production)
az sql server firewall-rule create \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name AllowDatabricks \
  --start-ip-address <databricks-ip-range-start> \
  --end-ip-address <databricks-ip-range-end>
```

### 2.4 Backup Configuration

**Automated Backups:**

```bash
# Configure backup retention
az sql db str-policy set \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --database endymion-ai \
  --retention-days 35

# Configure long-term retention
az sql db ltr-policy set \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --database endymion-ai \
  --weekly-retention P4W \
  --monthly-retention P12M \
  --yearly-retention P5Y \
  --week-of-year 1
```

**Manual Backup:**

```bash
# Export to BACPAC
az sql db export \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name endymion-ai \
  --admin-user sqladmin \
  --admin-password <password> \
  --storage-key-type StorageAccessKey \
  --storage-key <storage-key> \
  --storage-uri https://endymion-aibackup.blob.core.windows.net/backups/endymion-ai-$(date +%Y%m%d).bacpac
```

---

## 3. ADLS Gen2 Storage

### 3.1 Create Storage Account

#### Using Azure CLI

```bash
# Create storage account
az storage account create \
  --resource-group rg-endymion-ai-prod \
  --name endymion-aiadls \
  --location eastus2 \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true \
  --access-tier Hot

# Enable versioning
az storage account blob-service-properties update \
  --resource-group rg-endymion-ai-prod \
  --account-name endymion-aiadls \
  --enable-versioning true
```

#### Using Terraform

```hcl
# terraform/storage.tf

resource "azurerm_storage_account" "adls" {
  name                     = "endymion-aiadls"
  resource_group_name      = azurerm_resource_group.endymion-ai.name
  location                 = azurerm_resource_group.endymion-ai.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  
  blob_properties {
    versioning_enabled = true
    
    delete_retention_policy {
      days = 30
    }
  }

  network_rules {
    default_action             = "Deny"
    bypass                     = ["AzureServices"]
    virtual_network_subnet_ids = [azurerm_subnet.databricks.id]
  }

  tags = {
    Environment = "Production"
  }
}
```

### 3.2 Set Up Containers

```bash
# Create containers for each layer
az storage container create \
  --name bronze \
  --account-name endymion-aiadls \
  --auth-mode login

az storage container create \
  --name silver \
  --account-name endymion-aiadls \
  --auth-mode login

az storage container create \
  --name gold \
  --account-name endymion-aiadls \
  --auth-mode login

# Create directory structure
az storage fs directory create \
  --name events \
  --file-system bronze \
  --account-name endymion-aiadls

az storage fs directory create \
  --name cows \
  --file-system silver \
  --account-name endymion-aiadls

az storage fs directory create \
  --name analytics \
  --file-system gold \
  --account-name endymion-aiadls
```

### 3.3 Configure Access Policies

**Using Service Principal:**

```bash
# Create service principal
az ad sp create-for-rbac \
  --name sp-endymion-ai-databricks \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/<subscription-id>/resourceGroups/rg-endymion-ai-prod/providers/Microsoft.Storage/storageAccounts/endymion-aiadls

# Assign RBAC roles
az role assignment create \
  --assignee <service-principal-object-id> \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/<subscription-id>/resourceGroups/rg-endymion-ai-prod/providers/Microsoft.Storage/storageAccounts/endymion-aiadls
```

**Configure Databricks Access:**

```python
# Databricks notebook: configure_adls_access.py

# Mount ADLS Gen2 to Databricks
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<service-principal-client-id>",
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvault", key="sp-secret"),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

# Mount bronze
dbutils.fs.mount(
  source = "abfss://bronze@endymion-aiadls.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs
)

# Mount silver
dbutils.fs.mount(
  source = "abfss://silver@endymion-aiadls.dfs.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs
)

# Mount gold
dbutils.fs.mount(
  source = "abfss://gold@endymion-aiadls.dfs.core.windows.net/",
  mount_point = "/mnt/gold",
  extra_configs = configs
)
```

### 3.4 Data Lifecycle Management

```bash
# Create lifecycle policy
cat > lifecycle-policy.json <<EOF
{
  "rules": [
    {
      "enabled": true,
      "name": "MoveBronzeToArchive",
      "type": "Lifecycle",
      "definition": {
        "actions": {
          "baseBlob": {
            "tierToArchive": {
              "daysAfterModificationGreaterThan": 90
            },
            "delete": {
              "daysAfterModificationGreaterThan": 365
            }
          }
        },
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["bronze/"]
        }
      }
    },
    {
      "enabled": true,
      "name": "DeleteOldSilverVersions",
      "type": "Lifecycle",
      "definition": {
        "actions": {
          "version": {
            "delete": {
              "daysAfterCreationGreaterThan": 180
            }
          }
        },
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["silver/"]
        }
      }
    }
  ]
}
EOF

# Apply lifecycle policy
az storage account management-policy create \
  --account-name endymion-aiadls \
  --resource-group rg-endymion-ai-prod \
  --policy @lifecycle-policy.json
```

---

## 4. Migration Checklist

### Pre-Migration Tasks

- [ ] **Backup local data**
  ```bash
  pg_dump endymion_ai > backup_$(date +%Y%m%d).sql
  tar -czf delta_lake_backup_$(date +%Y%m%d).tar.gz delta_lake/
  ```

- [ ] **Document current configuration**
  ```bash
  cat .env > env_backup.txt
  psql -d endymion_ai -c "\dt" > tables_backup.txt
  ```

- [ ] **Test Azure connectivity**
  ```bash
  az account show
  az sql server show --resource-group rg-endymion-ai-prod --name endymion-ai-sql
  ```

### Migration Steps

#### Step 1: Replace MinIO with ADLS Gen2

```bash
# Old configuration (local)
DELTA_LAKE_PATH=/data/delta_lake

# New configuration (Azure)
DELTA_LAKE_PATH=abfss://bronze@endymion-aiadls.dfs.core.windows.net/
SILVER_LAKE_PATH=abfss://silver@endymion-aiadls.dfs.core.windows.net/
GOLD_LAKE_PATH=abfss://gold@endymion-aiadls.dfs.core.windows.net/
```

**Update application code:**

```python
# backend/config.py

import os

# Old
# DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "/data/delta_lake")

# New
if os.getenv("ENVIRONMENT") == "production":
    DELTA_LAKE_PATH = "abfss://bronze@endymion-aiadls.dfs.core.windows.net/"
    SILVER_LAKE_PATH = "abfss://silver@endymion-aiadls.dfs.core.windows.net/"
    GOLD_LAKE_PATH = "abfss://gold@endymion-aiadls.dfs.core.windows.net/"
else:
    DELTA_LAKE_PATH = "/data/delta_lake"
```

**Migrate data:**

```bash
# Upload existing Delta Lake data to ADLS
az storage blob upload-batch \
  --source ./delta_lake/bronze \
  --destination bronze \
  --account-name endymion-aiadls \
  --auth-mode login

az storage blob upload-batch \
  --source ./delta_lake/silver \
  --destination silver \
  --account-name endymion-aiadls \
  --auth-mode login

az storage blob upload-batch \
  --source ./delta_lake/gold \
  --destination gold \
  --account-name endymion-aiadls \
  --auth-mode login
```

#### Step 2: Replace Local PySpark with Databricks

**Upload notebooks:**

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Upload notebooks
databricks workspace import_dir \
  ./databricks/dlt/ \
  /Workspace/endymion-ai/dlt/ \
  --overwrite

databricks workspace import_dir \
  ./databricks/sync/ \
  /Workspace/endymion-ai/sync/ \
  --overwrite
```

**Update job configuration:**

```python
# backend/spark/config.py

# Old (local)
# spark = SparkSession.builder.master("local[*]").getOrCreate()

# New (Databricks)
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import *

spark = DatabricksSession.builder.getOrCreate()
```

#### Step 3: Replace Docker SQL with Azure SQL

**Migrate schema:**

```bash
# Export schema from local SQL Server as DACPAC
sqlpackage \
  /Action:Extract \
  /SourceServerName:localhost \
  /SourceDatabaseName:endymion_ai \
  /SourceUser:sa \
  /SourcePassword:StrongP@ssw0rd \
  /TargetFile:endymion_ai.dacpac

# Publish schema to Azure SQL
sqlpackage \
  /Action:Publish \
  /SourceFile:endymion_ai.dacpac \
  /TargetServerName:endymion-ai-sql.database.windows.net \
  /TargetDatabaseName:endymion-ai \
  /TargetUser:sqladmin \
  /TargetPassword:<password> \
  /p:BlockOnPossibleDataLoss=false
```

**Migrate data:**

```bash
# Export tables to native format
bcp operational.cow_events out cow_events.dat \
  -S localhost -d endymion_ai -U sa -P StrongP@ssw0rd -n
bcp operational.cows out cows.dat \
  -S localhost -d endymion_ai -U sa -P StrongP@ssw0rd -n

# Import into Azure SQL (consider Azure Data Factory for large volumes)
bcp operational.cow_events in cow_events.dat \
  -S endymion-ai-sql.database.windows.net \
  -d endymion-ai -U sqladmin -P <password> -n
bcp operational.cows in cows.dat \
  -S endymion-ai-sql.database.windows.net \
  -d endymion-ai -U sqladmin -P <password> -n
```

**Update connection strings:**

```python
# backend/database/connection.py

# Old (local Docker SQL Server)
# DATABASE_URL = "mssql+pyodbc://sa:StrongP@ssw0rd@localhost:1433/endymion_ai?driver=ODBC+Driver+18+for+SQL+Server"

# New (Azure SQL)
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

if os.getenv("ENVIRONMENT") == "production":
    credential = DefaultAzureCredential()
    client = SecretClient(
        vault_url="https://kv-endymion-ai-prod.vault.azure.net/",
        credential=credential
    )
    DATABASE_URL = client.get_secret("sql-connection-string").value
else:
    DATABASE_URL = os.getenv("DATABASE_URL")
```

#### Step 4: Configure Databricks Workflows

```bash
# Deploy workflows
databricks jobs create --json-file databricks/workflows/sync-workflow.json

# Schedule maintenance tasks
databricks jobs create --json-file databricks/workflows/maintenance-workflow.json
```

#### Step 5: Set Up Monitoring

```bash
# Enable Azure Monitor for SQL Database
az monitor diagnostic-settings create \
  --resource /subscriptions/<sub-id>/resourceGroups/rg-endymion-ai-prod/providers/Microsoft.Sql/servers/endymion-ai-sql/databases/endymion-ai \
  --name sql-diagnostics \
  --workspace <log-analytics-workspace-id> \
  --logs '[{"category":"QueryStoreRuntimeStatistics","enabled":true}]' \
  --metrics '[{"category":"Basic","enabled":true}]'

# Enable monitoring for Storage Account
az monitor diagnostic-settings create \
  --resource /subscriptions/<sub-id>/resourceGroups/rg-endymion-ai-prod/providers/Microsoft.Storage/storageAccounts/endymion-aiadls \
  --name storage-diagnostics \
  --workspace <log-analytics-workspace-id> \
  --logs '[{"category":"StorageRead","enabled":true},{"category":"StorageWrite","enabled":true}]'
```

#### Step 6: Enable SSL/TLS

**Azure SQL:**
- SSL enabled by default
- Force encryption in connection strings

**Application:**

```python
# backend/api/main.py

from fastapi import FastAPI
import ssl

app = FastAPI()

if os.getenv("ENVIRONMENT") == "production":
    # HTTPS only
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain('cert.pem', 'key.pem')
    
    # Run with SSL
    # uvicorn.run(app, host="0.0.0.0", port=443, ssl_context=ssl_context)
```

#### Step 7: Configure Auto-Scaling

**Databricks Auto-Scaling:**

```json
{
  "autoscale": {
    "min_workers": 2,
    "max_workers": 10
  },
  "spark_conf": {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
  }
}
```

**Azure SQL Auto-Scaling:**

```bash
# Enable serverless compute
az sql db update \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name endymion-ai \
  --edition GeneralPurpose \
  --compute-model Serverless \
  --auto-pause-delay 60
```

#### Step 8: Test Disaster Recovery

```bash
# Test database restore
az sql db restore \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name endymion-ai-restored \
  --dest-name endymion-ai \
  --time "2024-01-25T12:00:00Z"

# Test geo-failover
az sql server failover \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql
```

### Post-Migration Validation

- [ ] **Verify data integrity**
  ```sql
  -- Compare row counts
  SELECT COUNT(*) FROM events.cow_events;
  SELECT COUNT(*) FROM operational.cows;
  ```

- [ ] **Test API endpoints**
  ```bash
  curl https://api.endymion-ai.com/api/health
  curl https://api.endymion-ai.com/api/cows | jq '.[] | .cow_id'
  ```

- [ ] **Verify sync job**
  ```bash
  databricks jobs list
  databricks jobs get --job-id <job-id>
  ```

- [ ] **Check monitoring**
  - Azure Monitor dashboards
  - Application Insights
  - Log Analytics queries

---

## 5. Security Hardening

### 5.1 Service Principals

**Create Service Principals for each service:**

```bash
# Databricks service principal
az ad sp create-for-rbac \
  --name sp-endymion-ai-databricks \
  --role Contributor \
  --scopes /subscriptions/<subscription-id>/resourceGroups/rg-endymion-ai-prod

# API service principal
az ad sp create-for-rbac \
  --name sp-endymion-ai-api \
  --role Reader \
  --scopes /subscriptions/<subscription-id>/resourceGroups/rg-endymion-ai-prod

# Grant specific permissions
az role assignment create \
  --assignee <sp-object-id> \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/<sub-id>/resourceGroups/rg-endymion-ai-prod/providers/Microsoft.Storage/storageAccounts/endymion-aiadls
```

### 5.2 Key Vault Integration

**Store all secrets in Key Vault:**

```bash
# Create Key Vault
az keyvault create \
  --resource-group rg-endymion-ai-prod \
  --name kv-endymion-ai-prod \
  --location eastus2 \
  --enable-soft-delete true \
  --enable-purge-protection true

# Store secrets
az keyvault secret set --vault-name kv-endymion-ai-prod --name sql-connection-string --value "<connection-string>"
az keyvault secret set --vault-name kv-endymion-ai-prod --name databricks-token --value "<token>"
az keyvault secret set --vault-name kv-endymion-ai-prod --name storage-account-key --value "<key>"

# Grant access to service principals
az keyvault set-policy \
  --name kv-endymion-ai-prod \
  --spn <sp-client-id> \
  --secret-permissions get list
```

**Access from application:**

```python
# backend/config.py

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
client = SecretClient(
    vault_url="https://kv-endymion-ai-prod.vault.azure.net/",
    credential=credential
)

DATABASE_URL = client.get_secret("sql-connection-string").value
DATABRICKS_TOKEN = client.get_secret("databricks-token").value
```

### 5.3 Network Isolation

**Create Virtual Network:**

```bash
# Create VNet
az network vnet create \
  --resource-group rg-endymion-ai-prod \
  --name vnet-endymion-ai \
  --address-prefix 10.0.0.0/16

# Create subnets
az network vnet subnet create \
  --resource-group rg-endymion-ai-prod \
  --vnet-name vnet-endymion-ai \
  --name subnet-databricks-public \
  --address-prefixes 10.0.1.0/24

az network vnet subnet create \
  --resource-group rg-endymion-ai-prod \
  --vnet-name vnet-endymion-ai \
  --name subnet-databricks-private \
  --address-prefixes 10.0.2.0/24

# Create private endpoints
az network private-endpoint create \
  --resource-group rg-endymion-ai-prod \
  --name pe-sql \
  --vnet-name vnet-endymion-ai \
  --subnet subnet-databricks-private \
  --private-connection-resource-id /subscriptions/<sub-id>/resourceGroups/rg-endymion-ai-prod/providers/Microsoft.Sql/servers/endymion-ai-sql \
  --group-id sqlServer \
  --connection-name sql-privatelink
```

### 5.4 Audit Logging

```bash
# Enable SQL audit logging
az sql server audit-policy update \
  --resource-group rg-endymion-ai-prod \
  --name endymion-ai-sql \
  --state Enabled \
  --storage-account endymion-aiaudit \
  --storage-key <storage-key>

# Enable Storage account logging
az monitor diagnostic-settings create \
  --resource /subscriptions/<sub-id>/resourceGroups/rg-endymion-ai-prod/providers/Microsoft.Storage/storageAccounts/endymion-aiadls \
  --name audit-logs \
  --workspace <log-analytics-workspace-id> \
  --logs '[{"category":"StorageRead","enabled":true},{"category":"StorageWrite","enabled":true},{"category":"StorageDelete","enabled":true}]'
```

---

## 6. Cost Optimization

### 6.1 Right-Size Clusters

**Databricks Cluster Sizing:**

| Workload | Node Type | Workers | Monthly Cost |
|----------|-----------|---------|--------------|
| Dev/Test | Standard_D4s_v3 | 1-2 | ~$500 |
| Production (Small) | Standard_D8s_v3 | 2-5 | ~$2,000 |
| Production (Large) | Standard_D16s_v3 | 5-10 | ~$8,000 |

**Recommendations:**
- Use **Spot instances** for non-critical workloads (60-90% discount)
- Enable **auto-termination** (idle timeout: 30 minutes)
- Use **serverless SQL** for ad-hoc queries

```json
{
  "cluster_name": "endymion-ai-prod",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_D8s_v3",
  "autoscale": {
    "min_workers": 2,
    "max_workers": 5
  },
  "azure_attributes": {
    "first_on_demand": 1,
    "availability": "SPOT_WITH_FALLBACK_AZURE",
    "spot_bid_max_price": -1
  },
  "autotermination_minutes": 30
}
```

### 6.2 Use Serverless SQL

**Serverless vs Provisioned:**

| Aspect | Serverless | Provisioned |
|--------|-----------|-------------|
| **Billing** | Per vCore-second | Per vCore-hour |
| **Idle Cost** | $0 (auto-pauses) | Full cost |
| **Startup Time** | 30-60 seconds | Instant |
| **Use Case** | Intermittent | Always-on |

**Enable Serverless:**

```bash
az sql db update \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql \
  --name endymion-ai \
  --edition GeneralPurpose \
  --compute-model Serverless \
  --min-capacity 1 \
  --max-capacity 4 \
  --auto-pause-delay 60
```

### 6.3 Partition Pruning Strategies

**Partition by Tenant and Date:**

```python
# Optimize Delta tables with partitioning
(spark.read.format("delta")
  .load("abfss://bronze@endymion-aiadls.dfs.core.windows.net/events")
  .write
  .format("delta")
  .partitionBy("tenant_id", "event_date")
  .option("overwriteSchema", "true")
  .mode("overwrite")
  .save("abfss://bronze@endymion-aiadls.dfs.core.windows.net/events_partitioned"))

# Queries will prune partitions
spark.read.format("delta") \
  .load("abfss://bronze@endymion-aiadls.dfs.core.windows.net/events_partitioned") \
  .where("tenant_id = 'tenant-001' AND event_date >= '2024-01-01'")
```

**Z-Ordering for better compression:**

```sql
OPTIMIZE endymion_ai_prod.silver.cows
ZORDER BY (cow_id, tenant_id);
```

### 6.4 Retention Policies

**Data Retention Strategy:**

| Layer | Retention | Reasoning |
|-------|-----------|-----------|
| **Bronze** | 1 year | Compliance, replay |
| **Silver** | Indefinite | Source of truth |
| **Gold** | 2 years | Analytics history |
| **SQL Outbox** | 90 days | After published to Bronze |

```sql
-- Clean up old published events
DELETE FROM events.cow_events
WHERE published = TRUE
AND event_timestamp < DATEADD(day, -90, GETDATE());

-- Archive to cool/archive storage
```

### 6.5 Cost Estimates

#### Development Environment

**Monthly Costs:**

| Resource | Configuration | Cost |
|----------|---------------|------|
| **Azure SQL** | Basic, 5 DTU | $5 |
| **ADLS Gen2** | 100 GB Hot | $2 |
| **Databricks** | 1 Standard_D4s_v3 node, 40 hrs/month | $100 |
| **Key Vault** | 10K operations | $1 |
| **Total** | | **~$110/month** |

#### Production: Small (1,000 cows, 10 tenants)

**Monthly Costs:**

| Resource | Configuration | Cost |
|----------|---------------|------|
| **Azure SQL** | GP Gen5 2 vCores, 100 GB | $400 |
| **ADLS Gen2** | 1 TB Hot, 5 TB Archive | $150 |
| **Databricks Compute** | Standard_D8s_v3, 2-5 workers, 720 hrs | $2,500 |
| **Databricks DBU** | 300 DBUs @ $0.30 | $90 |
| **App Service** | P1v2 (FastAPI) | $75 |
| **Application Insights** | 10 GB ingestion | $25 |
| **Key Vault** | 100K operations | $5 |
| **Bandwidth** | 500 GB egress | $40 |
| **Total** | | **~$3,285/month** |

#### Production: Large (100,000 cows, 100 tenants)

**Monthly Costs:**

| Resource | Configuration | Cost |
|----------|---------------|------|
| **Azure SQL** | GP Gen5 8 vCores, 1 TB | $2,000 |
| **ADLS Gen2** | 10 TB Hot, 50 TB Archive | $1,200 |
| **Databricks Compute** | Standard_D16s_v3, 5-10 workers, 720 hrs | $12,000 |
| **Databricks DBU** | 2,000 DBUs @ $0.30 | $600 |
| **App Service** | P3v2 (FastAPI, 3 instances) | $600 |
| **Application Gateway** | Standard v2 | $250 |
| **Application Insights** | 100 GB ingestion | $230 |
| **Log Analytics** | 50 GB ingestion | $115 |
| **Key Vault** | 1M operations | $10 |
| **Bandwidth** | 5 TB egress | $400 |
| **Backup** | 2 TB backup storage | $50 |
| **Total** | | **~$17,455/month** |

**Cost Optimization Tips:**
- Use **Reserved Instances** (40% savings on compute)
- Use **Spot VMs** for Databricks (60-90% savings)
- Enable **auto-pause** on Serverless SQL
- Archive old data to **Cool/Archive tiers**
- Use **Azure Hybrid Benefit** if you have licenses

---

## 7. Post-Deployment Validation

### 7.1 Functional Testing

```bash
# Test API endpoints
curl -X POST https://api.endymion-ai.com/api/cows \
  -H "Content-Type: application/json" \
  -d '{"breed":"Holstein","birth_date":"2024-01-15","sex":"Female"}'

# Wait for sync (30s)
sleep 35

# Verify cow created
curl https://api.endymion-ai.com/api/cows | jq '.[] | select(.breed=="Holstein")'
```

### 7.2 Performance Testing

```bash
# Load test with Apache Bench
ab -n 1000 -c 10 https://api.endymion-ai.com/api/cows

# Monitor Databricks jobs
databricks jobs list --output JSON | jq '.jobs[] | select(.settings.name | contains("endymion-ai"))'
```

### 7.3 Security Validation

```bash
# Check SSL/TLS
openssl s_client -connect endymion-ai-sql.database.windows.net:1433

# Verify firewall rules
az sql server firewall-rule list \
  --resource-group rg-endymion-ai-prod \
  --server endymion-ai-sql

# Check Key Vault access
az keyvault secret show \
  --vault-name kv-endymion-ai-prod \
  --name sql-connection-string
```

### 7.4 Monitoring Validation

```bash
# Check metrics
az monitor metrics list \
  --resource /subscriptions/<sub-id>/resourceGroups/rg-endymion-ai-prod/providers/Microsoft.Sql/servers/endymion-ai-sql/databases/endymion-ai \
  --metric-names cpu_percent

# Query logs
az monitor log-analytics query \
  --workspace <workspace-id> \
  --analytics-query "AzureDiagnostics | where ResourceType == 'SERVERS/DATABASES' | take 10"
```

---

## Rollback Plan

If migration fails:

1. **Stop all Azure services**
2. **Restore local database** from backup
3. **Restore Delta Lake** from backup
4. **Reconfigure connection strings** to local
5. **Restart local services**

```bash
# Rollback script
./scripts/rollback-to-local.sh
```

---

## Additional Resources

- [Azure Databricks Documentation](https://docs.microsoft.com/en-us/azure/databricks/)
- [Azure SQL Database Best Practices](https://docs.microsoft.com/en-us/azure/azure-sql/database/)
- [ADLS Gen2 Documentation](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Delta Lake on Azure](https://docs.delta.io/latest/azure.html)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/)

---

**Last Updated:** January 2026  
**Version:** 1.0.0  
**Maintainer:** Endymion-AI DevOps Team
