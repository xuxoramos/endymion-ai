# Databricks Migration Guide (Cloud-Agnostic)

## Overview

This guide walks through migrating the CattleSaaS demo from local PySpark/MinIO to managed Databricks with cloud object storage, maintaining cloud-provider agnostic design principles.

**Current Architecture:**
```
SQL Server (local) → PySpark (local) → MinIO (s3a://) → FastAPI → React
                    ↓
            Bronze/Silver/Gold (Delta Lake)
```

**Target Architecture (Cloud-Agnostic):**
```
SQL Database (cloud/on-prem) → Databricks → Object Storage (S3/ADLS/GCS) → API Layer → Frontend
                               ↓
                       Bronze/Silver/Gold (Delta Lake)
```

**Supported Cloud Providers:**
- **AWS**: Databricks on AWS + S3
- **Azure**: Databricks on Azure + ADLS Gen2
- **GCP**: Databricks on GCP + Cloud Storage

---

## 1. Prerequisites

### Choose Your Cloud Provider

This guide supports all three major cloud providers. Select one based on your requirements:

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| **Object Storage** | S3 | ADLS Gen2 | Cloud Storage |
| **Storage Protocol** | `s3://` | `abfss://` | `gs://` |
| **Databricks Tier** | Standard/Premium | Standard/Premium | Standard/Premium |
| **Unity Catalog** | ✅ Available | ✅ Available | ✅ Available |
| **SQL Database** | RDS/Aurora | SQL Database | Cloud SQL |

**Recommendation:** Choose the cloud provider where your existing workloads run, or where you have existing credits/agreements.

### Databricks Requirements (All Clouds)

1. **Databricks Workspace**
   - Standard tier minimum
   - **Premium tier recommended** for:
     - Unity Catalog (centralized governance)
     - Role-based access control (RBAC)
     - Audit logging
     - Serverless compute

2. **Object Storage Account**
   - **AWS**: S3 bucket with versioning enabled
   - **Azure**: Storage account with hierarchical namespace (ADLS Gen2)
   - **GCP**: Cloud Storage bucket with uniform access
   - Container/bucket name: `lakehouse`

3. **SQL Database** (optional - can keep on-premises)
  - **AWS**: RDS for SQL Server or MySQL
  - **Azure**: Azure SQL Database or flexible server for SQL Server
  - **GCP**: Cloud SQL for SQL Server or MySQL
   - **On-premises**: Keep existing with VPN/ExpressRoute/Cloud VPN

4. **Secrets Management**
   - **Databricks Secrets** (built-in, works on all clouds) ✅ Recommended
   - **AWS**: AWS Secrets Manager (optional integration)
   - **Azure**: Azure Key Vault (optional integration)
   - **GCP**: Secret Manager (optional integration)

### Local Requirements

- **Databricks CLI**: `pip install databricks-cli`
- Cloud provider CLI (optional):
  - AWS: `aws configure`
  - Azure: `az login`
  - GCP: `gcloud init`

---

## 2. Object Storage Setup (Cloud-Agnostic)

### 2.1 Create Storage Bucket/Container

Choose your cloud provider and create object storage:

<details>
<summary><b>AWS S3</b></summary>

```bash
# Variables
BUCKET_NAME="cattlesaas-lakehouse-dev"  # Must be globally unique
REGION="us-east-1"

# Create S3 bucket
aws s3api create-bucket \
  --bucket $BUCKET_NAME \
  --region $REGION

# Enable versioning (recommended for data protection)
aws s3api put-bucket-versioning \
  --bucket $BUCKET_NAME \
  --versioning-configuration Status=Enabled

# Set lifecycle policy (optional - archive old data)
aws s3api put-bucket-lifecycle-configuration \
  --bucket $BUCKET_NAME \
  --lifecycle-configuration file://s3-lifecycle.json
```
</details>

<details>
<summary><b>Azure ADLS Gen2</b></summary>

```bash
# Variables
RESOURCE_GROUP="rg-cattlesaas-dev"
LOCATION="eastus"
STORAGE_ACCOUNT="sacattlesaasdev"  # Must be globally unique, lowercase
CONTAINER="lakehouse"

# Create resource group
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create storage account with hierarchical namespace
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Get storage key
STORAGE_KEY=$(az storage account keys list \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query '[0].value' -o tsv)

# Create container
az storage container create \
  --name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --account-key $STORAGE_KEY
```
</details>

<details>
<summary><b>GCP Cloud Storage</b></summary>

```bash
# Variables
BUCKET_NAME="cattlesaas-lakehouse-dev"  # Must be globally unique
PROJECT_ID="your-gcp-project-id"
REGION="us-central1"

# Create bucket
gsutil mb \
  -p $PROJECT_ID \
  -c STANDARD \
  -l $REGION \
  gs://$BUCKET_NAME/

# Enable versioning
gsutil versioning set on gs://$BUCKET_NAME/

# Set lifecycle policy (optional)
gsutil lifecycle set gs-lifecycle.json gs://$BUCKET_NAME/
```
</details>

### 2.2 Directory Structure (Universal Across All Clouds)

Create the same medallion structure regardless of cloud provider:

**AWS S3:**
```
s3://cattlesaas-lakehouse-dev/
├── bronze/
│   └── cow_events/          # Delta table
├── silver/
│   ├── cows/                # Delta table (current state)
│   └── cows_history/        # Delta table (SCD Type 2)
└── gold/
    ├── herd_composition/    # Delta table
    ├── cow_lifecycle/       # Delta table
    └── daily_snapshots/     # Delta table
```

**Azure ADLS Gen2:**
```
abfss://lakehouse@sacattlesaasdev.dfs.core.windows.net/
├── bronze/
│   └── cow_events/
├── silver/
│   ├── cows/
│   └── cows_history/
└── gold/
    ├── herd_composition/
    ├── cow_lifecycle/
    └── daily_snapshots/
```

**GCP Cloud Storage:**
```
gs://cattlesaas-lakehouse-dev/
├── bronze/
│   └── cow_events/
├── silver/
│   ├── cows/
│   └── cows_history/
└── gold/
    ├── herd_composition/
    ├── cow_lifecycle/
    └── daily_snapshots/
```

### 2.3 Migrate Existing Data (Optional)

If you have existing data in MinIO to migrate:

```bash
# Export from MinIO to local
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mirror local/lakehouse /tmp/lakehouse-export

# Upload to cloud (choose your provider)

# AWS S3
aws s3 sync /tmp/lakehouse-export/ s3://$BUCKET_NAME/ --recursive

# Azure ADLS Gen2 (using AzCopy)
azcopy copy "/tmp/lakehouse-export/*" \
  "https://${STORAGE_ACCOUNT}.dfs.core.windows.net/${CONTAINER}?${SAS_TOKEN}" \
  --recursive

# GCP Cloud Storage
gsutil -m rsync -r /tmp/lakehouse-export/ gs://$BUCKET_NAME/
```

**Note:** For this demo, it's cleaner to start fresh and re-run ingestion jobs on Databricks.

---

## 3. Databricks Workspace Setup (Cloud-Agnostic)

### 3.1 Create Databricks Workspace

<details>
<summary><b>AWS Databricks</b></summary>

**Via Console:**
1. Go to https://accounts.cloud.databricks.com/
2. Create Account → AWS
3. Select region (e.g., `us-east-1`)
4. Choose deployment: **Customer-managed VPC** (recommended) or **Databricks-managed**
5. Create workspace

**Via Terraform (Infrastructure as Code):**
```hcl
provider "databricks" {
  host = "https://accounts.cloud.databricks.com"
}

resource "databricks_mws_workspaces" "this" {
  account_id     = var.databricks_account_id
  workspace_name = "cattlesaas-workspace"
  aws_region     = "us-east-1"
  
  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
  network_id               = databricks_mws_networks.this.network_id
}
```
</details>

<details>
<summary><b>Azure Databricks</b></summary>

```bash
# Create Databricks workspace
az databricks workspace create \
  --name dbw-cattlesaas-dev \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku premium

# Get workspace URL
DATABRICKS_HOST=$(az databricks workspace show \
  --name dbw-cattlesaas-dev \
  --resource-group $RESOURCE_GROUP \
  --query workspaceUrl -o tsv)

echo "Databricks Workspace: https://$DATABRICKS_HOST"
```

**Via ARM Template:**
```json
{
  "type": "Microsoft.Databricks/workspaces",
  "apiVersion": "2023-02-01",
  "name": "dbw-cattlesaas-dev",
  "location": "[parameters('location')]",
  "sku": {
    "name": "premium"
  },
  "properties": {
    "managedResourceGroupId": "[subscriptionResourceId('Microsoft.Resources/resourceGroups', variables('managedResourceGroupName'))]"
  }
}
```
</details>

<details>
<summary><b>GCP Databricks</b></summary>

**Via Console:**
1. Go to https://accounts.gcp.databricks.com/
2. Create Account → GCP
3. Select region (e.g., `us-central1`)
4. Choose deployment: **Customer-managed VPC** or **Databricks-managed**
5. Create workspace

**Via gcloud + Terraform:**
```hcl
provider "databricks" {
  host = "https://accounts.gcp.databricks.com"
}

resource "databricks_mws_workspaces" "this" {
  account_id     = var.databricks_account_id
  workspace_name = "cattlesaas-workspace"
  gcp_region     = "us-central1"
  
  gke_config {
    connectivity_type = "PRIVATE_NODE_PUBLIC_MASTER"
    master_ip_range   = "10.3.0.0/28"
  }
}
```
</details>

### 3.2 Configure Databricks CLI (Universal)

```bash
# Install Databricks CLI (works for all clouds)
pip install databricks-cli

# Configure authentication
databricks configure --token

# When prompted, enter:
# - Databricks Host: 
#   AWS: https://<workspace-id>.<region>.databricks.com
#   Azure: https://<workspace-name>.azuredatabricks.net
#   GCP: https://<workspace-id>.gcp.databricks.com
# - Token: Generate from User Settings → Developer → Access Tokens in UI

# Verify connection
databricks workspace ls /
```

### 3.3 Create Databricks Cluster (Cloud-Agnostic Configuration)

**Cluster Configuration (works on all clouds):**
```json
{
  "cluster_name": "cattlesaas-processing",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "COMPUTE_OPTIMIZED",
  "num_workers": 2,
  "autoscale": {
    "min_workers": 1,
    "max_workers": 4
  },
  "spark_conf": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  },
  "custom_tags": {
    "project": "cattlesaas",
    "environment": "dev"
  },
  "enable_elastic_disk": true,
  "enable_local_disk_encryption": false
}
```

**Node Type Mapping (choose appropriate size):**
- **AWS**: `m5.xlarge` (4 cores, 16GB) or `i3.xlarge` (with local SSD)
- **Azure**: `Standard_DS3_v2` (4 cores, 14GB) or `Standard_E4s_v3`
- **GCP**: `n1-standard-4` (4 cores, 15GB) or `n1-highmem-4`

Create via CLI:
```bash
databricks clusters create --json-file cluster-config.json
```

Or via UI (works identically on all clouds):
1. Navigate to **Compute** → **Create Cluster**
2. Name: `cattlesaas-processing`
3. Runtime: **14.3 LTS (includes Apache Spark 3.5.0, Scala 2.12)** ✅ Delta Lake included
4. Worker type: Select compute-optimized instance
5. Workers: 1-4 (autoscaling)
6. Enable **Photon Acceleration** (Premium tier only, all clouds)

### 3.4 Configure Object Storage Access (Cloud-Specific)

### 3.4 Configure Object Storage Access (Cloud-Specific)

**Recommended Approach: Unity Catalog (Premium Tier, All Clouds)**

Unity Catalog provides centralized governance and eliminates the need for manual mounting:

```python
# No configuration needed! Unity Catalog manages credentials centrally
# Access data directly:
bronze_df = spark.read.format("delta").load("s3://bucket/bronze/cow_events")  # AWS
bronze_df = spark.read.format("delta").load("abfss://container@account.dfs.core.windows.net/bronze/cow_events")  # Azure
bronze_df = spark.read.format("delta").load("gs://bucket/bronze/cow_events")  # GCP
```

**Alternative: Instance Profiles / Service Principals / Service Accounts**

<details>
<summary><b>AWS: IAM Instance Profile (Recommended)</b></summary>

```bash
# Create IAM role for Databricks
aws iam create-role \
  --role-name databricks-s3-access-role \
  --assume-role-policy-document file://trust-policy.json

# Attach S3 access policy
aws iam attach-role-policy \
  --role-name databricks-s3-access-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Create instance profile
aws iam create-instance-profile \
  --instance-profile-name databricks-s3-access

# Add role to instance profile
aws iam add-role-to-instance-profile \
  --instance-profile-name databricks-s3-access \
  --role-name databricks-s3-access-role

# In Databricks: Configure cluster with instance profile ARN
# Compute → Edit Cluster → Advanced → Instance Profile
# Enter: arn:aws:iam::<account-id>:instance-profile/databricks-s3-access
```

**No mounting needed - access S3 directly:**
```python
bronze_df = spark.read.format("delta").load("s3://cattlesaas-lakehouse-dev/bronze/cow_events")
```
</details>

<details>
<summary><b>Azure: Service Principal (Recommended)</b></summary>

```bash
# Create service principal
SP_NAME="sp-cattlesaas-databricks"
az ad sp create-for-rbac --name $SP_NAME \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/<SUBSCRIPTION_ID>/resourceGroups/$RESOURCE_GROUP

# Note the output:
# - appId (client_id)
# - password (client_secret)
# - tenant (tenant_id)

# Store in Databricks secrets
databricks secrets create-scope --scope cattlesaas-secrets
databricks secrets put --scope cattlesaas-secrets --key adls-client-id
databricks secrets put --scope cattlesaas-secrets --key adls-client-secret
databricks secrets put --scope cattlesaas-secrets --key adls-tenant-id

# Configure in cluster Spark config (Compute → Edit → Advanced → Spark):
spark.hadoop.fs.azure.account.auth.type OAuth
spark.hadoop.fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
spark.hadoop.fs.azure.account.oauth2.client.id {{secrets/cattlesaas-secrets/adls-client-id}}
spark.hadoop.fs.azure.account.oauth2.client.secret {{secrets/cattlesaas-secrets/adls-client-secret}}
spark.hadoop.fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/{{secrets/cattlesaas-secrets/adls-tenant-id}}/oauth2/token
```

**No mounting needed - access ADLS Gen2 directly:**
```python
bronze_df = spark.read.format("delta").load("abfss://lakehouse@sacattlesaasdev.dfs.core.windows.net/bronze/cow_events")
```
</details>

<details>
<summary><b>GCP: Service Account (Recommended)</b></summary>

```bash
# Create service account
gcloud iam service-accounts create databricks-gcs-access \
  --display-name="Databricks GCS Access"

# Grant storage permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:databricks-gcs-access@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Create and download key
gcloud iam service-accounts keys create key.json \
  --iam-account=databricks-gcs-access@$PROJECT_ID.iam.gserviceaccount.com

# Store key in Databricks secrets
databricks secrets create-scope --scope cattlesaas-secrets
databricks secrets put --scope cattlesaas-secrets --key gcs-service-account-key

# Configure in cluster Spark config:
spark.hadoop.google.cloud.auth.service.account.enable true
spark.hadoop.fs.gs.auth.service.account.email databricks-gcs-access@$PROJECT_ID.iam.gserviceaccount.com
spark.hadoop.fs.gs.auth.service.account.private.key.id {{secrets/cattlesaas-secrets/gcs-service-account-key}}
```

**No mounting needed - access GCS directly:**
```python
bronze_df = spark.read.format("delta").load("gs://cattlesaas-lakehouse-dev/bronze/cow_events")
```
</details>

**Option: Databricks Secrets (Cloud-Agnostic, Works Everywhere)**

```bash
# Create secret scope (works on all clouds)
databricks secrets create-scope --scope cattlesaas-secrets

# Store credentials (choose based on cloud provider)
databricks secrets put --scope cattlesaas-secrets --key storage-access-key

# In cluster Spark config:
# AWS S3:
spark.hadoop.fs.s3a.access.key {{secrets/cattlesaas-secrets/aws-access-key}}
spark.hadoop.fs.s3a.secret.key {{secrets/cattlesaas-secrets/aws-secret-key}}

# Azure ADLS Gen2:
spark.hadoop.fs.azure.account.key.sacattlesaasdev.dfs.core.windows.net {{secrets/cattlesaas-secrets/adls-access-key}}

# GCP Cloud Storage:
spark.hadoop.fs.gs.auth.service.account.private.key {{secrets/cattlesaas-secrets/gcs-key}}
```

---

## 4. Cloud-Agnostic Code Migration

### 4.1 Create Cloud-Agnostic Configuration

Create `databricks/config/cloud_config.py`:

```python
"""
Cloud-agnostic Databricks configuration
Supports AWS (S3), Azure (ADLS Gen2), GCP (Cloud Storage), and local development
"""
import os

# Environment detection
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
IS_LOCAL = not IS_DATABRICKS

# Cloud provider detection (if running in Databricks)
CLOUD_PROVIDER = None
if IS_DATABRICKS:
    # Detect cloud provider from Spark configuration or environment
    # AWS: Check for EC2 metadata endpoint
    # Azure: Check for Azure-specific environment variables
    # GCP: Check for GCE metadata
    import subprocess
    try:
        result = subprocess.run(
            ['curl', '-s', '-f', 'http://169.254.169.254/latest/meta-data/'],
            timeout=1,
            capture_output=True
        )
        if result.returncode == 0:
            CLOUD_PROVIDER = "AWS"
    except:
        pass
    
    if not CLOUD_PROVIDER and os.getenv("AZURE_DATABRICKS"):
        CLOUD_PROVIDER = "AZURE"
    
    if not CLOUD_PROVIDER:
        try:
            result = subprocess.run(
                ['curl', '-s', '-f', '-H', 'Metadata-Flavor: Google', 
                 'http://metadata.google.internal/'],
                timeout=1,
                capture_output=True
            )
            if result.returncode == 0:
                CLOUD_PROVIDER = "GCP"
        except:
            pass

# Storage configuration (set via environment variables or defaults)
CLOUD_STORAGE_BUCKET = os.getenv("CLOUD_STORAGE_BUCKET")
CLOUD_STORAGE_ACCOUNT = os.getenv("CLOUD_STORAGE_ACCOUNT")  # Azure only
CLOUD_STORAGE_CONTAINER = os.getenv("CLOUD_STORAGE_CONTAINER")  # Azure only

# Construct base path based on cloud provider
if IS_LOCAL:
    # Local development (MinIO with S3 API)
    BASE_PATH = "s3a://lakehouse"
elif CLOUD_PROVIDER == "AWS":
    # AWS S3
    BASE_PATH = f"s3://{CLOUD_STORAGE_BUCKET}"
elif CLOUD_PROVIDER == "AZURE":
    # Azure ADLS Gen2
    container = CLOUD_STORAGE_CONTAINER or "lakehouse"
    BASE_PATH = f"abfss://{container}@{CLOUD_STORAGE_ACCOUNT}.dfs.core.windows.net"
elif CLOUD_PROVIDER == "GCP":
    # GCP Cloud Storage
    BASE_PATH = f"gs://{CLOUD_STORAGE_BUCKET}"
else:
    # Fallback or manual override
    BASE_PATH = os.getenv("STORAGE_BASE_PATH", "s3a://lakehouse")

print(f"🌍 Environment: {'Databricks' if IS_DATABRICKS else 'Local'}")
print(f"☁️  Cloud Provider: {CLOUD_PROVIDER or 'Local/Unknown'}")
print(f"🗄️  Base Path: {BASE_PATH}")

# Delta Lake paths (cloud-agnostic)
BRONZE_TABLE_PATH = f"{BASE_PATH}/bronze/cow_events"
SILVER_COWS_PATH = f"{BASE_PATH}/silver/cows"
SILVER_HISTORY_PATH = f"{BASE_PATH}/silver/cows_history"
GOLD_HERD_COMPOSITION_PATH = f"{BASE_PATH}/gold/herd_composition"
GOLD_COW_LIFECYCLE_PATH = f"{BASE_PATH}/gold/cow_lifecycle"
GOLD_DAILY_SNAPSHOTS_PATH = f"{BASE_PATH}/gold/daily_snapshots"

# SQL Server configuration (cloud-agnostic)
# Use Databricks secrets if available, otherwise fall back to environment variables
if IS_DATABRICKS:
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        
        SQL_SERVER_HOST = dbutils.secrets.get(scope="cattlesaas-secrets", key="sql-server-host")
        SQL_SERVER_USER = dbutils.secrets.get(scope="cattlesaas-secrets", key="sql-server-user")
        SQL_SERVER_PASSWORD = dbutils.secrets.get(scope="cattlesaas-secrets", key="sql-server-password")
        SQL_SERVER_DATABASE = dbutils.secrets.get(scope="cattlesaas-secrets", key="sql-server-database")
    except:
        # Fallback to environment variables if secrets not configured
        SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST", "localhost")
        SQL_SERVER_USER = os.getenv("SQL_SERVER_USER", "sa")
        SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD")
        SQL_SERVER_DATABASE = os.getenv("SQL_SERVER_DATABASE", "cattlesaas")
else:
    # Local development
    SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST", "localhost")
    SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT", "1433")
    SQL_SERVER_USER = os.getenv("SQL_SERVER_USER", "sa")
    SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD", "YourStrong!Passw0rd")
    SQL_SERVER_DATABASE = os.getenv("SQL_SERVER_DATABASE", "cattlesaas")

# JDBC connection string
SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT", "1433")
JDBC_URL = f"jdbc:sqlserver://{SQL_SERVER_HOST}:{SQL_SERVER_PORT};databaseName={SQL_SERVER_DATABASE};encrypt=true;trustServerCertificate=true"

print(f"💾 SQL Server: {SQL_SERVER_HOST}:{SQL_SERVER_PORT}/{SQL_SERVER_DATABASE}")
```

**Environment Variables to Set:**

```bash
# For AWS
export CLOUD_STORAGE_BUCKET="cattlesaas-lakehouse-dev"

# For Azure
export CLOUD_STORAGE_ACCOUNT="sacattlesaasdev"
export CLOUD_STORAGE_CONTAINER="lakehouse"

# For GCP
export CLOUD_STORAGE_BUCKET="cattlesaas-lakehouse-dev"

# SQL Server (all clouds)
export SQL_SERVER_HOST="your-database-host"
export SQL_SERVER_USER="admin"
export SQL_SERVER_PASSWORD="YourStrong!Passw0rd"
export SQL_SERVER_DATABASE="cattlesaas"
```

### 4.2 Update Spark Session Initialization (Cloud-Agnostic)

Modify all Bronze/Silver/Gold scripts to use cloud-agnostic config:

**Before (local MinIO):**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .master("local[*]") \
    .config("spark.jars", "<hadoop-aws jars>") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .getOrCreate()
```

**After (cloud-agnostic):**
```python
from pyspark.sql import SparkSession
from databricks.config.cloud_config import IS_DATABRICKS, IS_LOCAL

if IS_DATABRICKS:
    # Use existing Databricks session (works on AWS, Azure, GCP)
    spark = SparkSession.builder.getOrCreate()
else:
    # Local development (MinIO with S3 API)
    import os
    ivy_jars = os.path.expanduser("~/.ivy2/jars")
    hadoop_aws_jar = f"{ivy_jars}/org.apache.hadoop_hadoop-aws-3.3.1.jar"
    aws_sdk_jar = f"{ivy_jars}/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar"
    
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .master("local[*]") \
        .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
```

### 4.3 Update Bronze Ingestion Script (Cloud-Agnostic)

`databricks/bronze/ingest_from_sql.py`:

```python
from pyspark.sql import SparkSession
from databricks.config.cloud_config import (
    IS_DATABRICKS, IS_LOCAL, CLOUD_PROVIDER, BASE_PATH,
    BRONZE_TABLE_PATH, JDBC_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Initialize Spark (cloud-agnostic)
    if IS_DATABRICKS:
        spark = SparkSession.builder.getOrCreate()
        logger.info(f"Running on Databricks ({CLOUD_PROVIDER})")
    else:
        # Local setup (MinIO S3)
        import os
        ivy_jars = os.path.expanduser("~/.ivy2/jars")
        hadoop_aws_jar = f"{ivy_jars}/org.apache.hadoop_hadoop-aws-3.3.1.jar"
        aws_sdk_jar = f"{ivy_jars}/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar"
        
        spark = SparkSession.builder \
            .appName("BronzeIngestion") \
            .master("local[*]") \
            .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar}") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        logger.info("Running locally (MinIO)")
    
    logger.info(f"Storage base path: {BASE_PATH}")
    logger.info(f"Reading from SQL Server: {JDBC_URL}")
    
    # Read from operational database (works with any SQL database)
    bronze_df = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "operational.cow_events") \
        .option("user", SQL_SERVER_USER) \
        .option("password", SQL_SERVER_PASSWORD) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    
    event_count = bronze_df.count()
    logger.info(f"Read {event_count} events from SQL Server")
    logger.info(f"Writing to Bronze: {BRONZE_TABLE_PATH}")
    
    # Write to Bronze (append-only) - works on S3, ADLS Gen2, GCS
    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .save(BRONZE_TABLE_PATH)
    
    logger.info(f"✅ Bronze ingestion complete: {event_count} events written")
    
    spark.stop()

if __name__ == "__main__":
    main()
```

### 4.4 Update Silver and Gold Scripts (Cloud-Agnostic)

Apply same pattern to:
- `databricks/silver/resolve_cow_state.py`
- `databricks/gold/unified_runner.py`

Key changes:
1. Import from `cloud_config` instead of hardcoded paths
2. Use `IS_DATABRICKS` to conditionally initialize Spark
3. Use path constants (`BRONZE_TABLE_PATH`, etc.) which work across all clouds
4. Remove cloud-specific configurations when running on Databricks

**Example for Silver:**
```python
from databricks.config.cloud_config import (
    IS_DATABRICKS, BRONZE_TABLE_PATH, SILVER_COWS_PATH, SILVER_HISTORY_PATH
)

# Initialize Spark
if IS_DATABRICKS:
    spark = SparkSession.builder.getOrCreate()
else:
    # Local setup...
    spark = SparkSession.builder...

# Read from Bronze (works on all clouds)
bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)

# Write to Silver (works on all clouds)
silver_df.write.format("delta").mode("overwrite").save(SILVER_COWS_PATH)
```

---

## 5. SQL Database Migration (Optional - Cloud-Agnostic)

You can keep your on-premises SQL Server or migrate to a cloud-managed database.

### 5.1 Cloud SQL Database Options

<details>
<summary><b>AWS: RDS for SQL Server</b></summary>

```bash
# Create RDS SQL Server instance
aws rds create-db-instance \
  --db-instance-identifier cattlesaas-sqlserver \
  --db-instance-class db.t3.small \
  --engine sqlserver-ex \
  --master-username admin \
  --master-user-password 'YourStrong!Passw0rd' \
  --allocated-storage 20 \
  --vpc-security-group-ids sg-xxxxx \
  --db-subnet-group-name my-subnet-group \
  --publicly-accessible

# Get endpoint
aws rds describe-db-instances \
  --db-instance-identifier cattlesaas-sqlserver \
  --query 'DBInstances[0].Endpoint.Address' --output text
```

**Connection String:**
```
jdbc:sqlserver://<endpoint>:1433;databaseName=cattlesaas;encrypt=true
```
</details>

<details>
<summary><b>Azure: SQL Database</b></summary>

```bash
# Create Azure SQL Server
az sql server create \
  --name cattlesaas-sqlserver \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --admin-user sqladmin \
  --admin-password 'YourStrong!Passw0rd'

# Configure firewall
az sql server firewall-rule create \
  --server cattlesaas-sqlserver \
  --resource-group $RESOURCE_GROUP \
  --name AllowAzureServices \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0

# Create database
az sql db create \
  --server cattlesaas-sqlserver \
  --resource-group $RESOURCE_GROUP \
  --name cattlesaas \
  --edition Basic
```

**Connection String:**
```
jdbc:sqlserver://cattlesaas-sqlserver.database.windows.net:1433;databaseName=cattlesaas;encrypt=true
```
</details>

<details>
<summary><b>GCP: Cloud SQL for SQL Server</b></summary>

```bash
# Create Cloud SQL instance
gcloud sql instances create cattlesaas-sqlserver \
  --database-version=SQLSERVER_2019_STANDARD \
  --tier=db-custom-2-7680 \
  --region=us-central1 \
  --root-password='YourStrong!Passw0rd'

# Create database
gcloud sql databases create cattlesaas \
  --instance=cattlesaas-sqlserver

# Get connection name
gcloud sql instances describe cattlesaas-sqlserver \
  --format='value(connectionName)'
```

**Connection String:**
```
jdbc:sqlserver://<public-ip>:1433;databaseName=cattlesaas;encrypt=true
```
</details>

### 5.2 Store Database Credentials (Cloud-Agnostic)

```bash
# Store in Databricks secrets (works on all clouds)
databricks secrets put --scope cattlesaas-secrets --key sql-server-host
databricks secrets put --scope cattlesaas-secrets --key sql-server-user
databricks secrets put --scope cattlesaas-secrets --key sql-server-password
databricks secrets put --scope cattlesaas-secrets --key sql-server-database
```

### 5.3 Keep On-Premises SQL Server (VPN/Private Connectivity)

If keeping on-premises SQL Server:
- **AWS**: Use AWS Direct Connect or Site-to-Site VPN
- **Azure**: Use ExpressRoute or VPN Gateway
- **GCP**: Use Cloud VPN or Dedicated Interconnect

---

## 6. Databricks Jobs Configuration (Cloud-Agnostic)

All Databricks job configurations work identically across AWS, Azure, and GCP.

### 6.1 Create Bronze Ingestion Job

**Job Configuration (works on all clouds):**
```json
{
  "name": "cattlesaas-bronze-ingestion",
  "tasks": [
    {
      "task_key": "ingest_from_sql",
      "notebook_task": {
        "notebook_path": "/Workspace/Repos/cattlesaas/databricks/bronze/ingest_from_sql",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "bronze_cluster",
      "libraries": []
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "bronze_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "COMPUTE_OPTIMIZED_MEDIUM",
        "num_workers": 2,
        "spark_conf": {
          "spark.databricks.delta.optimizeWrite.enabled": "true"
        }
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 */15 * * * ?",
    "timezone_id": "America/Chicago",
    "pause_status": "UNPAUSED"
  },
  "email_notifications": {
    "on_failure": ["admin@cattlesaas.com"]
  },
  "max_concurrent_runs": 1,
  "timeout_seconds": 3600
}
```

Create via CLI (works on all clouds):
```bash
databricks jobs create --json-file bronze-job.json
```

Or via UI (identical on AWS, Azure, GCP):
1. **Workflows** → **Create Job**
2. Task name: `ingest_from_sql`
3. Type: **Notebook**
4. Path: `/Workspace/Repos/cattlesaas/databricks/bronze/ingest_from_sql`
5. Cluster: **Job cluster** (auto-terminates after run)
6. Schedule: Every 15 minutes (`0 */15 * * * ?`)
7. Notifications: Email on failure

### 6.2 Create Silver Processing Job

```json
{
  "name": "cattlesaas-silver-processing",
  "tasks": [
    {
      "task_key": "resolve_cow_state",
      "notebook_task": {
        "notebook_path": "/Workspace/Repos/cattlesaas/databricks/silver/resolve_cow_state",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "silver_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "silver_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "COMPUTE_OPTIMIZED_MEDIUM",
        "num_workers": 2,
        "spark_conf": {
          "spark.databricks.delta.optimizeWrite.enabled": "true",
          "spark.databricks.delta.autoCompact.enabled": "true"
        }
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 */30 * * * ?",
    "timezone_id": "America/Chicago"
  },
  "max_concurrent_runs": 1
}
```

### 6.3 Create Gold Processing Job

```json
{
  "name": "cattlesaas-gold-processing",
  "tasks": [
    {
      "task_key": "unified_runner",
      "notebook_task": {
        "notebook_path": "/Workspace/Repos/cattlesaas/databricks/gold/unified_runner",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "gold_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "gold_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "COMPUTE_OPTIMIZED_MEDIUM",
        "num_workers": 2
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "America/Chicago"
  }
}
```

### 6.4 Multi-Task Workflow with Dependencies (Recommended)

Create a single workflow with task dependencies:

```json
{
  "name": "cattlesaas-medallion-pipeline",
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "notebook_task": {
        "notebook_path": "/Workspace/Repos/cattlesaas/databricks/bronze/ingest_from_sql"
      },
      "job_cluster_key": "shared_cluster"
    },
    {
      "task_key": "silver_processing",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "notebook_task": {
        "notebook_path": "/Workspace/Repos/cattlesaas/databricks/silver/resolve_cow_state"
      },
      "job_cluster_key": "shared_cluster"
    },
    {
      "task_key": "gold_processing",
      "depends_on": [{"task_key": "silver_processing"}],
      "notebook_task": {
        "notebook_path": "/Workspace/Repos/cattlesaas/databricks/gold/unified_runner"
      },
      "job_cluster_key": "shared_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "shared_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "COMPUTE_OPTIMIZED_MEDIUM",
        "autoscale": {
          "min_workers": 1,
          "max_workers": 4
        },
        "spark_conf": {
          "spark.databricks.delta.optimizeWrite.enabled": "true",
          "spark.databricks.delta.autoCompact.enabled": "true"
        }
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 */6 * * ?",
    "timezone_id": "America/Chicago"
  },
  "email_notifications": {
    "on_failure": ["admin@cattlesaas.com"],
    "on_success": ["admin@cattlesaas.com"]
  },
  "webhook_notifications": {
    "on_failure": [{"id": "slack-webhook-id"}]
  },
  "max_concurrent_runs": 1,
  "timeout_seconds": 7200
}
```

**Benefits of Multi-Task Workflow:**
- Single cluster reused across tasks (cost savings)
- Automatic task ordering based on dependencies
- Unified monitoring and alerting
- Easier to manage than 3 separate jobs

---

## 7. Upload Code to Databricks (Cloud-Agnostic)

### 7.1 Option A: Databricks Repos (Recommended - Works on All Clouds)

Databricks Repos provides Git integration for version control:

```bash
# Initialize git in your project
cd /home/xuxoramos/endymion-ai
git init
git add .
git commit -m "Initial commit for Databricks migration"

# Push to your Git provider (GitHub, GitLab, Bitbucket, Azure DevOps)
git remote add origin https://github.com/your-org/cattlesaas.git
git branch -M main
git push -u origin main
```

**In Databricks UI (identical on AWS, Azure, GCP):**
1. Navigate to **Workspace** → **Repos**
2. Click **Add Repo**
3. Select Git provider: GitHub / GitLab / Bitbucket / Azure DevOps
4. Enter repository URL: `https://github.com/your-org/cattlesaas.git`
5. Click **Create Repo**

**Benefits:**
- ✅ Automatic sync on git push
- ✅ Branch switching and pull requests
- ✅ Version control for all notebooks
- ✅ CI/CD integration
- ✅ Team collaboration

**Run notebooks from Repos:**
```
/Workspace/Repos/<username>/cattlesaas/databricks/bronze/ingest_from_sql
```

### 7.2 Option B: Databricks CLI Sync

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure (use your cloud-specific workspace URL)
databricks configure --token

# Create workspace directory
databricks workspace mkdirs /Workspace/cattlesaas

# Upload entire directory
databricks workspace import_dir \
  ./databricks \
  /Workspace/cattlesaas \
  --overwrite --exclude-hidden-files

# Verify upload
databricks workspace ls /Workspace/cattlesaas
```

### 7.3 Option C: Manual Upload via UI

1. Navigate to **Workspace** → **Create** → **Notebook**
2. Name: `ingest_from_sql`
3. Language: Python
4. Copy code from `databricks/bronze/ingest_from_sql.py`
5. Remove `if __name__ == "__main__"` block
6. Repeat for all scripts

### 7.4 Set Environment Variables (Cloud-Agnostic)

**In cluster configuration (Compute → Edit → Advanced → Environment Variables):**

```bash
# Storage configuration (choose based on cloud provider)
CLOUD_STORAGE_BUCKET=cattlesaas-lakehouse-dev  # AWS or GCP
CLOUD_STORAGE_ACCOUNT=sacattlesaasdev          # Azure only
CLOUD_STORAGE_CONTAINER=lakehouse              # Azure only

# SQL Server configuration
SQL_SERVER_HOST=your-database-host
SQL_SERVER_PORT=1433
SQL_SERVER_DATABASE=cattlesaas
```

**Or use Databricks secrets (recommended):**
```bash
databricks secrets create-scope --scope cattlesaas-secrets

databricks secrets put --scope cattlesaas-secrets --key cloud-storage-bucket
databricks secrets put --scope cattlesaas-secrets --key sql-server-host
databricks secrets put --scope cattlesaas-secrets --key sql-server-user
databricks secrets put --scope cattlesaas-secrets --key sql-server-password
databricks secrets put --scope cattlesaas-secrets --key sql-server-database
```

### 7.5 Install Python Dependencies (Cloud-Agnostic)

Create `requirements.txt`:
```txt
delta-spark==3.2.1
pyodbc==5.0.1
python-dotenv==1.0.0
```

**Install on cluster:**
1. **Compute** → Select cluster → **Libraries** → **Install New**
2. Library Source: **PyPI**
3. Package: `delta-spark==3.2.1`
4. Click **Install**
5. Repeat for other packages

**Or use init script (all clusters automatically install):**
```bash
# Create init script
cat > /tmp/install-packages.sh << 'EOF'
#!/bin/bash
/databricks/python/bin/pip install delta-spark==3.2.1 pyodbc==5.0.1
EOF

# Upload to DBFS (works on all clouds)
databricks fs cp /tmp/install-packages.sh dbfs:/databricks/init-scripts/install-packages.sh

# Configure in cluster settings:
# Advanced → Init Scripts → dbfs:/databricks/init-scripts/install-packages.sh
```

---

## 8. API and Frontend Deployment (Optional - Cloud-Agnostic)

### 8.1 Deploy FastAPI Backend

<details>
<summary><b>AWS: Elastic Beanstalk or App Runner</b></summary>

**Using AWS App Runner:**
```bash
# Create ECR repository
aws ecr create-repository --repository-name cattlesaas-api

# Build and push Docker image
docker build -t cattlesaas-api ./backend
docker tag cattlesaas-api:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/cattlesaas-api:latest
aws ecr get-login-password | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/cattlesaas-api:latest

# Create App Runner service
aws apprunner create-service \
  --service-name cattlesaas-api \
  --source-configuration '{
    "ImageRepository": {
      "ImageIdentifier": "<account-id>.dkr.ecr.us-east-1.amazonaws.com/cattlesaas-api:latest",
      "ImageRepositoryType": "ECR"
    },
    "AutoDeploymentsEnabled": true
  }'
```
</details>

<details>
<summary><b>Azure: App Service</b></summary>

```bash
# Create App Service Plan
az appservice plan create \
  --name plan-cattlesaas-dev \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --is-linux \
  --sku B1

# Create Web App
az webapp create \
  --name app-cattlesaas-api \
  --resource-group $RESOURCE_GROUP \
  --plan plan-cattlesaas-dev \
  --runtime "PYTHON|3.12"

# Configure startup command
az webapp config set \
  --name app-cattlesaas-api \
  --resource-group $RESOURCE_GROUP \
  --startup-file "gunicorn -w 4 -k uvicorn.workers.UvicornWorker backend.api.main:app"

# Deploy code
cd /home/xuxoramos/endymion-ai
zip -r deploy.zip . -x "*.git*" -x "*node_modules*" -x "*.venv*"
az webapp deploy \
  --name app-cattlesaas-api \
  --resource-group $RESOURCE_GROUP \
  --src-path deploy.zip \
  --type zip
```
</details>

<details>
<summary><b>GCP: Cloud Run</b></summary>

```bash
# Create Dockerfile if not exists
cat > Dockerfile << 'EOF'
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "backend.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
EOF

# Build and push to Artifact Registry
gcloud builds submit --tag gcr.io/$PROJECT_ID/cattlesaas-api

# Deploy to Cloud Run
gcloud run deploy cattlesaas-api \
  --image gcr.io/$PROJECT_ID/cattlesaas-api \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```
</details>

### 8.2 Deploy React Frontend

<details>
<summary><b>AWS: S3 + CloudFront</b></summary>

```bash
# Build frontend
cd frontend
npm run build

# Create S3 bucket
aws s3 mb s3://cattlesaas-frontend

# Upload build
aws s3 sync dist/ s3://cattlesaas-frontend --delete

# Create CloudFront distribution
aws cloudfront create-distribution \
  --origin-domain-name cattlesaas-frontend.s3.amazonaws.com \
  --default-root-object index.html
```
</details>

<details>
<summary><b>Azure: Static Web Apps</b></summary>

```bash
# Build frontend
cd frontend
npm run build

# Create Static Web App
az staticwebapp create \
  --name swa-cattlesaas-frontend \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --source https://github.com/your-org/cattlesaas \
  --branch main \
  --app-location "/frontend" \
  --output-location "dist"

# Configure API proxy in frontend/public/staticwebapp.config.json
```
</details>

<details>
<summary><b>GCP: Firebase Hosting</b></summary>

```bash
# Install Firebase CLI
npm install -g firebase-tools

# Build frontend
cd frontend
npm run build

# Initialize Firebase
firebase init hosting

# Deploy
firebase deploy --only hosting
```
</details>

---

## 9. Testing Migration

### 9.1 Verify Storage Access

Create test notebook in Databricks:

```python
# Test notebook: /Workspace/cattlesaas/tests/test_storage
from databricks.config.azure_config import BASE_PATH

# List directories
display(dbutils.fs.ls(BASE_PATH))

# Test write
test_df = spark.createDataFrame([(1, "test")], ["id", "value"])
test_df.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/test/data")

# Test read
read_df = spark.read.format("delta").load(f"{BASE_PATH}/test/data")
display(read_df)

print("✅ Storage access working")
```

### 9.2 Run Bronze Ingestion

```bash
# Trigger job manually via CLI
databricks jobs run-now --job-id <BRONZE_JOB_ID>

# Or in UI: Workflows → cattlesaas-bronze-ingestion → Run Now
```

Verify:
```python
# In notebook
bronze_df = spark.read.format("delta").load("/mnt/lakehouse/bronze/cow_events")
print(f"Bronze records: {bronze_df.count()}")
display(bronze_df)
```

### 9.3 Run Full Pipeline

```bash
# Trigger workflow
databricks jobs run-now --job-id <PIPELINE_JOB_ID>

# Monitor in UI: Workflows → cattlesaas-medallion-pipeline → Latest Run
```

Verify each layer:
```python
# Bronze
bronze_count = spark.read.format("delta").load("/mnt/lakehouse/bronze/cow_events").count()

# Silver
silver_count = spark.read.format("delta").load("/mnt/lakehouse/silver/cows").count()

# Gold
gold_count = spark.read.format("delta").load("/mnt/lakehouse/gold/herd_composition").count()

print(f"Bronze: {bronze_count}, Silver: {silver_count}, Gold: {gold_count}")
```

### 9.4 Test End-to-End

1. **Record weight in UI** (or via API)
2. **Wait for Bronze job** (15 min schedule)
3. **Wait for Silver job** (30 min schedule)
4. **Wait for Gold job** (1 hour schedule)
5. **Query analytics schema** via FastAPI
6. **Verify dashboard updates**

Or trigger manually:
```bash
databricks jobs run-now --job-id <BRONZE_JOB_ID>
# Wait for completion
databricks jobs run-now --job-id <SILVER_JOB_ID>
# Wait for completion
databricks jobs run-now --job-id <GOLD_JOB_ID>
```

---

## 10. Cost Optimization

### 10.1 Databricks Cluster Strategies

**Development:**
- Use **job clusters** (ephemeral, terminate after job)
- Enable **autoscaling** (1-4 workers)
- Use **spot instances** (Azure Spot VMs, 70-90% savings)
- Schedule jobs during off-hours

**Production:**
- Use **all-purpose cluster** only for interactive work
- Configure **auto-termination** (30 min idle)
- Use **Photon engine** (2-3x faster, cost-effective)
- Enable **Delta caching** for frequently accessed data

**Cost estimate (job clusters):**
- Standard_DS3_v2: ~$0.15/hour
- 2 workers: ~$0.30/hour
- Bronze job (15 min): ~$0.08 per run
- Silver job (30 min): ~$0.15 per run
- Gold job (1 hour): ~$0.30 per run
- **Daily cost:** ~$2-3 for all jobs

### 10.2 Storage Optimization

- Use **Hot tier** for active Delta tables (Bronze/Silver/Gold)
- Move old Bronze data to **Cool tier** after 30 days
- Archive Bronze data >90 days to **Archive tier**
- Enable **lifecycle management** policies

```bash
# Create lifecycle policy
az storage account management-policy create \
  --account-name $STORAGE_ACCOUNT \
  --policy @lifecycle-policy.json

# lifecycle-policy.json
{
  "rules": [
    {
      "name": "move-bronze-to-cool",
      "type": "Lifecycle",
      "definition": {
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["lakehouse/bronze"]
        },
        "actions": {
          "baseBlob": {
            "tierToCool": {"daysAfterModificationGreaterThan": 30},
            "tierToArchive": {"daysAfterModificationGreaterThan": 90}
          }
        }
      }
    }
  ]
}
```

### 10.3 SQL Database Optimization

- Use **Basic tier** for dev/test (5 DTUs, $5/month)
- Scale up to **Standard** for production (10-100 DTUs)
- Enable **auto-pause** (serverless) if using Azure SQL Serverless
- Consider **elastic pools** for multiple databases

---

## 11. Monitoring and Observability

### 11.1 Databricks Monitoring

**Built-in:**
- **Job Runs**: Workflows → Job Name → Runs
- **Cluster Metrics**: Compute → Cluster → Metrics
- **Delta Table History**: `DESCRIBE HISTORY delta.`<path>`

**Azure Monitor Integration:**
```bash
# Enable diagnostic settings
az monitor diagnostic-settings create \
  --name databricks-diagnostics \
  --resource <DATABRICKS_RESOURCE_ID> \
  --logs '[{"category": "jobs", "enabled": true}]' \
  --metrics '[{"category": "AllMetrics", "enabled": true}]' \
  --workspace <LOG_ANALYTICS_WORKSPACE_ID>
```

### 11.2 Create Alerts

```bash
# Alert on job failure
az monitor metrics alert create \
  --name "databricks-job-failure" \
  --resource-group $RESOURCE_GROUP \
  --scopes <DATABRICKS_RESOURCE_ID> \
  --condition "count jobs.failed > 0" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --action <ACTION_GROUP_ID>
```

### 11.3 Delta Lake Monitoring

Add to each job:
```python
# At end of job
from delta.tables import DeltaTable

# Get table metrics
delta_table = DeltaTable.forPath(spark, BRONZE_TABLE_PATH)
history = delta_table.history(limit=1).collect()[0]

print(f"Operation: {history['operation']}")
print(f"Records added: {history['operationMetrics']['numOutputRows']}")
print(f"Files added: {history['operationMetrics']['numAddedFiles']}")

# Log to Application Insights or Log Analytics
```

---

## 12. Security Best Practices

### 12.1 Secret Management

**Never hardcode secrets.** Use Azure Key Vault:

```bash
# Create Key Vault
az keyvault create \
  --name kv-cattlesaas-dev \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION

# Add secrets
az keyvault secret set \
  --vault-name kv-cattlesaas-dev \
  --name "sql-server-password" \
  --value "YourStrong!Passw0rd"

# Grant Databricks access
az keyvault set-policy \
  --name kv-cattlesaas-dev \
  --object-id <DATABRICKS_SP_OBJECT_ID> \
  --secret-permissions get list
```

**Create Databricks secret scope backed by Key Vault:**
```python
# In Databricks notebook
databricks secrets create-scope \
  --scope cattlesaas-secrets \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/<SUB_ID>/resourceGroups/<RG>/providers/Microsoft.KeyVault/vaults/kv-cattlesaas-dev \
  --dns-name https://kv-cattlesaas-dev.vault.azure.net/

# Use in code
password = dbutils.secrets.get(scope="cattlesaas-secrets", key="sql-server-password")
```

### 12.2 Network Security

- **Private Endpoints**: Connect ADLS Gen2 and SQL Database via private endpoints
- **VNet Injection**: Deploy Databricks in your VNet for network isolation
- **NSG Rules**: Restrict inbound/outbound traffic
- **Firewall**: Use Azure Firewall for egress filtering

### 12.3 Access Control

- **RBAC**: Assign roles (Owner, Contributor, Reader) at resource level
- **Databricks RBAC**: Assign workspace permissions (Admin, User, Read-Only)
- **Unity Catalog**: Centralized governance for data access (Premium tier)
- **Delta Sharing**: Securely share Delta Lake data across organizations

---

## 13. Migration Checklist

### Pre-Migration
- [ ] Create Azure resource group
- [ ] Create ADLS Gen2 storage account with hierarchical namespace
- [ ] Create Databricks workspace (Premium recommended)
- [ ] Create Azure SQL Database (or keep on-premises with VPN)
- [ ] Create Azure Key Vault for secrets
- [ ] Set up service principal with Storage Blob Data Contributor role
- [ ] Configure Databricks CLI locally

### Code Migration
- [ ] Create `databricks/config/azure_config.py` with environment detection
- [ ] Update all scripts to import from `azure_config`
- [ ] Test locally with `IS_LOCAL=True`
- [ ] Convert Python scripts to Databricks notebooks
- [ ] Upload code to Databricks Workspace or Repos
- [ ] Install Python dependencies on cluster

### Storage Migration
- [ ] Mount ADLS Gen2 to Databricks (`/mnt/lakehouse`)
- [ ] Create Bronze/Silver/Gold directory structure
- [ ] (Optional) Migrate existing Delta Lake data from MinIO using AzCopy
- [ ] Verify storage access with test notebook

### Job Configuration
- [ ] Create Bronze ingestion job (15 min schedule)
- [ ] Create Silver processing job (30 min schedule)
- [ ] Create Gold processing job (1 hour schedule)
- [ ] Create workflow with task dependencies
- [ ] Configure email notifications for failures
- [ ] Set up retry policies

### Testing
- [ ] Run Bronze ingestion manually and verify data
- [ ] Run Silver processing and verify SCD Type 2 history
- [ ] Run Gold processing and verify analytics tables
- [ ] Test end-to-end: UI → SQL → Bronze → Silver → Gold → API → Dashboard
- [ ] Verify job schedules trigger correctly
- [ ] Test failure scenarios and alerts

### Monitoring
- [ ] Enable Databricks diagnostic logs to Azure Monitor
- [ ] Create alerts for job failures
- [ ] Set up Delta Lake table monitoring
- [ ] Configure Application Insights for FastAPI (if migrated)
- [ ] Set up dashboards in Azure Monitor or Grafana

### Security
- [ ] Store all secrets in Azure Key Vault
- [ ] Create Databricks secret scope backed by Key Vault
- [ ] Configure RBAC for all Azure resources
- [ ] Enable private endpoints for ADLS Gen2 and SQL Database
- [ ] Review Databricks workspace access permissions
- [ ] Enable audit logging

### Documentation
- [ ] Update README.md with Azure-specific instructions
- [ ] Document connection strings and service principal setup
- [ ] Create runbook for common operations (job restart, manual ingestion)
- [ ] Document cost monitoring and optimization strategies

---

## 14. Rollback Plan

If migration issues occur:

1. **Revert to local setup**:
   ```bash
   cd /home/xuxoramos/endymion-ai
   ./start_all.sh
   ```

2. **Keep both environments running in parallel** during transition period

3. **Sync data back from Azure to local**:
   ```bash
   # Export from ADLS Gen2
   azcopy copy \
     "https://${STORAGE_ACCOUNT}.dfs.core.windows.net/${CONTAINER}?${SAS_TOKEN}" \
     "/tmp/azure-export" \
     --recursive
   
   # Import to MinIO
   mc cp --recursive /tmp/azure-export/* local/lakehouse/
   ```

---

## 15. Post-Migration Tasks

### 15.1 Decommission Local Resources

Once Azure migration is stable:
```bash
# Stop local services
docker-compose down

# Backup local data
mc mirror local/lakehouse /mnt/backup/lakehouse-$(date +%Y%m%d)

# Remove containers (after confirming Azure working)
docker-compose down -v
```

### 15.2 Update CI/CD

Update GitHub Actions or Azure DevOps pipelines to deploy to Azure:

```yaml
# .github/workflows/deploy-databricks.yml
name: Deploy to Databricks

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Databricks CLI
        run: pip install databricks-cli
      
      - name: Configure Databricks
        run: |
          echo "${{ secrets.DATABRICKS_TOKEN }}" | databricks configure --token
      
      - name: Sync code to workspace
        run: |
          databricks workspace import_dir \
            ./databricks \
            /Workspace/cattlesaas \
            --overwrite
```

### 15.3 Performance Tuning

After migration, optimize:
- **Z-Ordering**: Cluster data by frequently queried columns
  ```python
  spark.sql("OPTIMIZE delta.`/mnt/lakehouse/bronze/cow_events` ZORDER BY (cow_id)")
  ```
- **Data Skipping**: Collect stats for partition pruning
  ```python
  spark.sql("ANALYZE TABLE delta.`/mnt/lakehouse/silver/cows` COMPUTE STATISTICS")
  ```
- **Compaction**: Merge small files
  ```python
  spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
  ```

---

## 16. Resources (Cloud-Agnostic)

**Databricks (All Clouds):**
- **Databricks Documentation**: https://docs.databricks.com/
- **Delta Lake Guide**: https://docs.delta.io/
- **Unity Catalog**: https://docs.databricks.com/data-governance/unity-catalog/
- **Databricks CLI**: https://docs.databricks.com/dev-tools/cli/
- **Databricks API**: https://docs.databricks.com/dev-tools/api/

**Cloud-Specific Resources:**

| AWS | Azure | GCP |
|-----|-------|-----|
| [Databricks on AWS](https://docs.databricks.com/aws/) | [Databricks on Azure](https://docs.databricks.com/azure/) | [Databricks on GCP](https://docs.databricks.com/gcp/) |
| [S3 Documentation](https://docs.aws.amazon.com/s3/) | [ADLS Gen2 Docs](https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-introduction) | [Cloud Storage Docs](https://cloud.google.com/storage/docs) |
| [RDS Documentation](https://docs.aws.amazon.com/rds/) | [Azure SQL Database](https://learn.microsoft.com/azure/azure-sql/database/) | [Cloud SQL Docs](https://cloud.google.com/sql/docs) |
| [AWS Pricing Calculator](https://calculator.aws/) | [Azure Pricing Calculator](https://azure.microsoft.com/pricing/calculator/) | [GCP Pricing Calculator](https://cloud.google.com/products/calculator) |

---

## Summary

### Migration Timeline
- **Initial Setup**: 1-2 days (cloud resources + Databricks workspace)
- **Code Migration**: 1 day (cloud-agnostic config + testing)
- **Testing & Validation**: 2-3 days (end-to-end verification)
- **Production Deployment**: 1-2 days (CI/CD + monitoring)
- **Total**: 1-2 weeks

### Cost Comparison (Dev/Test Environment)

| Component | AWS | Azure | GCP |
|-----------|-----|-------|-----|
| **Databricks** (job clusters, 6h/day, spot) | ~$50-60/month | ~$50-60/month | ~$45-55/month |
| **Object Storage** (100 GB) | S3: ~$2-3/month | ADLS Gen2: ~$2/month | GCS: ~$2-3/month |
| **SQL Database** (Basic) | RDS: ~$15/month | SQL DB: ~$5/month | Cloud SQL: ~$7/month |
| **Networking** (egress) | ~$5-10/month | ~$5-10/month | ~$5-10/month |
| **Total** | **~$72-88/month** | **~$62-77/month** | **~$59-75/month** |

**Production Cost**: ~$500-1000/month (scaled clusters, HA databases, more storage)

### Cost Optimization Summary
✅ Use job clusters with spot/preemptible instances (**70% savings**)
✅ Enable autoscaling (pay only for what you use)
✅ Implement storage lifecycle policies (**50-80% savings on cold data**)
✅ Use serverless SQL databases (auto-pause when idle)
✅ Schedule jobs during off-peak hours
✅ Enable Delta Lake optimizations (OPTIMIZE, Z-ORDER, Auto Compact)

### Key Benefits (Cloud-Agnostic)
✅ **Scalable compute**: Autoscaling clusters from 1-100+ workers
✅ **Managed platform**: No infrastructure maintenance
✅ **Multi-cloud**: Run identical code on AWS, Azure, or GCP
✅ **Enterprise security**: Unity Catalog, RBAC, encryption at rest
✅ **Monitoring**: Built-in metrics + cloud-native observability
✅ **CI/CD ready**: Git integration with Repos
✅ **High availability**: Cloud-provider SLAs (99.9%+)
✅ **Disaster recovery**: Cross-region replication available

### Why Cloud-Agnostic Design?
🌍 **Flexibility**: Switch clouds without rewriting code
🔒 **Avoid lock-in**: No vendor-specific dependencies
💰 **Cost optimization**: Choose cheapest cloud per region
🛡️ **Risk mitigation**: Multi-cloud backup strategy
🚀 **Innovation**: Adopt best features from each cloud

**Next Steps:**
1. Choose your cloud provider (AWS, Azure, or GCP)
2. Set environment variables (`CLOUD_STORAGE_BUCKET`, etc.)
3. Deploy Databricks workspace
4. Run cloud-agnostic code unchanged
5. Scale globally with same architecture
