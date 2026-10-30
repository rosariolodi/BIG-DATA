terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id                 = var.subscription_id
  resource_provider_registrations = "none"
}

# Sufijo aleatorio: el nombre de la cuenta de almacenamiento debe ser único en todo Azure
resource "random_string" "suffix" {
  length  = 6
  upper   = false
  special = false
}

locals {
  nombre_base = "${var.project}-${var.environment}"
  # Solo minúsculas y números, 3 a 24 caracteres
  nombre_storage = "st${var.project}${random_string.suffix.result}"
  capas          = toset(["bronze", "silver", "gold"])
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.nombre_base}"
  location = var.location
  tags     = var.tags
}

# ADLS Gen2 = Storage Account con namespace jerárquico activado
resource "azurerm_storage_account" "datalake" {
  name                     = local.nombre_storage
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
}

# Arquitectura Medallion: un contenedor por capa
resource "azurerm_storage_container" "capas" {
  for_each              = local.capas
  name                  = each.key
  storage_account_id    = azurerm_storage_account.datalake.id
  container_access_type = "private"
}

resource "azurerm_databricks_workspace" "dbx" {
  name                = "dbw-${local.nombre_base}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
  tags                = var.tags
}
