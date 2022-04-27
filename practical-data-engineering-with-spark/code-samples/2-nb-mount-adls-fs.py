# Databricks notebook source
# 
# Name:         nb-mount-adls-fs
# 
# Design Phase:
#     Author:   John Miner
#     Date:     09-15-2020
#     Purpose:  Mount the adls gen 2 storage.
# 
# Parameters:
#     mount_point_path - path to mount storage on.
#         
# Algorithm:
#     1 - Get secrets from key vault
#     2 - Mount adls as local storage
#     


# COMMAND ----------

#
# Input parameters for notebook
#

# define parameter 1
dbutils.widgets.text("mount_point_path", "/mnt/datalake")

# define parameter 2
dbutils.widgets.text("file_system", "sc4rissug2020")

# define parameter 3
dbutils.widgets.text("account_name", "sa4rissug2020")



# COMMAND ----------

#
#  Read information from attached key vault
#

# Azure active directory (tennant id)
tenant_id = dbutils.secrets.get("ss4kvs", "sec-tenant-id")

# Service principle (client id)
client_id = dbutils.secrets.get("ss4kvs", "sec-client-id")

# Service principle (client secret)
client_secret = dbutils.secrets.get("ss4kvs", "sec-client-pwd")


# COMMAND ----------

#
#  Read input parameters
#

# grab mount point
mount_point_path = dbutils.widgets.get("mount_point_path")

# grab file system
file_system = dbutils.widgets.get("file_system")

# grab account name
account_name = dbutils.widgets.get("account_name")



# COMMAND ----------

#
#  Define utility function
#

def show_secret_text(secret_txt):
  for c in secret_txt:
    print(c)

# COMMAND ----------

#
#  Decode the secrets
#

# show_secret_text(tenant_id)

# COMMAND ----------

#
#  Unmount storage (prevents errors)
#

# unmount storage 
try:
  dbutils.fs.unmount(mount_point_path)
except:
  print("The volume {} is not mounted".format(mount_point_path))

# COMMAND ----------

#
#  Mount storage 
#

# Make tenant str
tenant_str = "https://login.microsoftonline.com/"  + tenant_id + "/oauth2/token"

# Config dictionary
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": client_id,
       "fs.azure.account.oauth2.client.secret": client_secret,
       "fs.azure.account.oauth2.client.endpoint": tenant_str,
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}


# Try mounting storage
try : 
    # exec command
    dbutils.fs.mount(
    source = "abfss://{}@{}.dfs.core.windows.net/".format(file_system, account_name),
    mount_point = mount_point_path,
    extra_configs = configs)
    
    # debug message
    print("Successfully mounted volume {}".format(mount_point_path))
    
except Exception:
  
    # debug message
    print("Failure when mounted volume {}".format(mount_point_path))
    
    pass 

