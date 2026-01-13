# MTA Azure Transit Brain

## Overview
A cloud-native transit analytics platform built on **Microsoft Azure**.
This project uses **Terraform** to provision infrastructure and **Python** for Data & AI services.

## Prerequisites
1.  **Azure Subscription** (Active)
2.  **Azure CLI** (`az login` must be run first)
3.  **Terraform** installed
4.  **Python 3.10+**

## Deployment Instructions

### 1. Provision Infrastructure
Deploy the Azure Data Lake Storage Gen2 resources using Terraform.
```bash
cd .azure
terraform init
terraform apply -auto-approve
```
*Take note of the `storage_account_name` output.*

### 2. Configure Environment
Set the storage account name so the scripts can find it.
**PowerShell:**
```powershell
$env:AZURE_STORAGE_ACCOUNT_NAME = "mtadls<unique_id>"
```
**Bash:**
```bash
export AZURE_STORAGE_ACCOUNT_NAME="mtadls<unique_id>"
```

### 3. Run the Data Pipeline
Execute the Medallion Architecture flow (Locally -> Cloud).
```bash
# Ingest: Download from MTA -> Upload to Azure Bronze
python src/ingest/fetch_data.py

# Process: Read Bronze -> Clean -> Write Silver -> Aggregate -> Write Gold
python src/process/etl_pipeline.py

# Train: Read Gold -> Train Model -> Save Artifacts
python src/models/forecaster.py
```

### 4. Launch Dashboard
Start the user interface.
```bash
streamlit run src/app/dashboard.py
```

## Architecture
- **Infrastructure**: Terraform (`.azure/main.tf`)
- **Storage**: Azure Data Lake Storage Gen2 (Bronze/Silver/Gold Containers)
- **Compute**: Python (Simulating Databricks Clusters)
- **Identity**: `azure-identity` (DefaultAzureCredential)
