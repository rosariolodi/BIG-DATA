variable "subscription_id" {
  description = "ID de la suscripción de Azure (az account show --query id -o tsv)"
  type        = string
}

variable "location" {
  description = "Región de Azure. Si da error RequestDisallowedByAzure, prueba otra (centralus, westus3, canadacentral, brazilsouth)"
  type        = string
  default     = "eastus2"
}

variable "project" {
  description = "Nombre corto del proyecto (solo minúsculas y números, máx. 12)"
  type        = string
  default     = "datamarca"
}

variable "environment" {
  description = "Entorno"
  type        = string
  default     = "dev"
}

variable "tags" {
  description = "Etiquetas para los recursos"
  type        = map(string)
  default = {
    proyecto = "BIG-DATA"
    entorno  = "dev"
  }
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "ruta_bronze" {
  value = "abfss://bronze@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
}

output "ruta_silver" {
  value = "abfss://silver@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
}

output "ruta_gold" {
  value = "abfss://gold@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
}

output "databricks_url" {
  value = "https://${azurerm_databricks_workspace.dbx.workspace_url}"
}

