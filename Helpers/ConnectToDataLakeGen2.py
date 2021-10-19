# Databricks notebook source
key_vault_scope="AzureKeyVaultScope"
storage_account="datalakedlsparkdemo"

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = ""+key_vault_scope+"", key = "databricksAppClientId"),
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = ""+key_vault_scope+"", key = "databricksAppClientSecret"),
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+dbutils.secrets.get(scope = ""+key_vault_scope+"", key = "tenantId")+"/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

print("This notebook is now connected to the '"+storage_account+"' Azure Data Lake Gen2 via Service Principal and OAuth.")
