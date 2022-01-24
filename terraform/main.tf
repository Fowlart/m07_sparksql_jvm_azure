# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
    resource_group_name  = "my_resources_north_europe"
    storage_account_name = "fowlartstor"
    container_name       = "4terraform"
    sas_token            = "sp=racwdli&st=2022-01-24T11:46:46Z&se=2023-01-24T19:46:46Z&sv=2020-08-04&sr=c&sig=fIMoL1DhJfTVouRlIHOcmWuEcjEd3wLH0jEgPYIxGic%3D"
    key                  = "key_1"
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

resource "azurerm_databricks_workspace" "bdcc" {

  name                = var.DATABRIKS_NAME
  resource_group_name = "my_resources_north_europe"
  location            = "northeurope"
  sku                 = "trial"
}
