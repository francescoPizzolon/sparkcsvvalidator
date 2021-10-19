# Databricks notebook source
dbutils.widgets.text("file_system","raw","file_system")
file_system=dbutils.widgets.get("file_system")

dbutils.widgets.text("mount_path","mnt","mount_path")
mount_path=dbutils.widgets.get("mount_path")

key_vault_scope="AzureKeyVaultScope"
storage_account="datalakedlsparkdemo"

path_to_mount = "/"+mount_path+"/"+file_system

# mount file system
try:
  # this will fail the first time but not all the subsequent times
  dbutils.fs.ls(path_to_mount)
except:
  # this will execute just the first time
  dbutils.fs.mount(
  source = "abfss://"+file_system+"@"+storage_account+".dfs.core.windows.net/",
  mount_point = path_to_mount,
  extra_configs = configs)
  print("Mounted: '"+str(path_to_mount)+"'")
  
# this will trigger error if path is not mounted correctly
dbutils.fs.ls(path_to_mount)
print("Already mounted: '"+str(path_to_mount)+"'")
