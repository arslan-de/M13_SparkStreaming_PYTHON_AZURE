terraform {
  backend "azurerm" {
    subscription_id = "a3e75a16-1480-4c80-9ddc-e1e3ffa483eb"
    resource_group_name = "Homework"
    storage_account_name = "m13storage"
    container_name = "m13"
    key = "JDAHqqhl9/VIGaL66xXkw95g6z+91P6sNvHuo97gbVxRHXLwDxFIERs99Usfu26JnmWUAYCBWc911TGdYc8Tlw=="
  }
}

provider "azurerm" {
  version = "~> 2.62.0"
  features {
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "bdcc" {
  name = "rg-${var.ENV}-${var.LOCATION}"
  location = var.LOCATION

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_account" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc]

  name = "st${var.ENV}${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  account_tier = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled = "true"

  network_rules {
    default_action = "Allow"
    ip_rules = values(var.IP_RULES)
  }

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
  depends_on = [
    azurerm_storage_account.bdcc]

  name = "data"
  storage_account_id = azurerm_storage_account.bdcc.id

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_databricks_workspace" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc
  ]

  name = "dbw-${var.ENV}-${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  sku = "standard"

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}
