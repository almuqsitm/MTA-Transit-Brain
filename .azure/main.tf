terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "mta_rg" {
  name     = "mta-transit-brain-rg"
  location = "East US"
}

resource "random_id" "storage_account_suffix" {
  byte_length = 4
}

# Azure Data Lake Storage Gen2 (HNS Enabled)
resource "azurerm_storage_account" "datalake" {
  name                     = "mtadls${random_id.storage_account_suffix.hex}"
  resource_group_name      = azurerm_resource_group.mta_rg.name
  location                 = azurerm_resource_group.mta_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Critical for ADLS Gen2
}

# File Systems (Containers) for Medallion Architecture
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.datalake.id
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}
