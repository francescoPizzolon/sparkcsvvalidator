# Databricks notebook source
dbutils.widgets.text("container_name","sftp","container_name")
container_name=dbutils.widgets.get("container_name")

dbutils.widgets.text("storage_account_name_no_env","spotpricelanding","storage_account_name_no_env")
storage_account_name_no_env=dbutils.widgets.get("storage_account_name_no_env")

dbutils.widgets.text("key_vault_key","landingZoneKey","key_vault_key")
key_vault_key=dbutils.widgets.get("key_vault_key")

dbutils.widgets.text("mount_path","mnt","mount_path")
mount_path=dbutils.widgets.get("mount_path")

key_vault_scope="AzureKeyVaultScope"
environment_name = dbutils.secrets.get(scope = ""+key_vault_scope+"", key = "environmentName")
storage_account= storage_account_name_no_env + environment_name

path_to_mount = "/"+mount_path+"/"+storage_account_name_no_env+"/"+container_name
configs = {"fs.azure.account.key."+storage_account+".blob.core.windows.net":dbutils.secrets.get(scope = ""+key_vault_scope+"", key = key_vault_key)}

# mount file system
try:
  # this will fail the first time but not all the subsequent times
  dbutils.fs.ls(path_to_mount)
  print("Already mounted: '"+str(path_to_mount)+"'")
except:
  # this will execute just the first time
  dbutils.fs.mount(
  source = "wasbs://"+container_name+"@"+storage_account+".blob.core.windows.net/",
  mount_point = path_to_mount,
  extra_configs = configs)
  print("Mounted: '"+str(path_to_mount)+"'")
  
# this will trigger error if path is not mounted correctly
dbutils.fs.ls(path_to_mount)
print("")
