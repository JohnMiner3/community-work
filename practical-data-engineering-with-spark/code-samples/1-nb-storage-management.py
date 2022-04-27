# Databricks notebook source
# MAGIC %md
# MAGIC <img src= "/files/tables/avatar.jpg" width="100" height="100" />
# MAGIC  
# MAGIC ```
# MAGIC 
# MAGIC Name:         1-nb-storage-management
# MAGIC 
# MAGIC Design Phase:
# MAGIC     Author:   John Miner
# MAGIC     Date:     12-01-2020
# MAGIC     Purpose:  How to manage the databricks file system (dbfs)
# MAGIC 
# MAGIC Learning Guide:
# MAGIC     1 - Library vs magic commands
# MAGIC     2 - Working with directories (create, delete, copy, rename)
# MAGIC     3 - Working with files (create, delete, copy, rename)
# MAGIC     4 - Local vs remote storage
# MAGIC     
# MAGIC ```

# COMMAND ----------

#
#  1.0 - List available commands
#

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------



# COMMAND ----------

#
#  2.0 - Magic vs library commands
#


# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------



# COMMAND ----------

#
# 3.0 - Manage local storage
#

# COMMAND ----------

#
# 3.1 - Create sample data lake
#

# remove existing
try:
  dbutils.fs.rm("/lake", recurse=True)  
except:
  pass

# root
dbutils.fs.mkdirs("/lake")

# raw quality
dbutils.fs.mkdirs("/lake/bronze")

# refine quality
dbutils.fs.mkdirs("/lake/silver")

# gold quality
dbutils.fs.mkdirs("/lake/gold")

# show folder structure
dbutils.fs.ls("/lake")


# COMMAND ----------

#
# 3.2 - Show sample files
#


# COMMAND ----------

# sample readme
dbutils.fs.ls("/databricks-datasets/power-plant/")

# COMMAND ----------

# show top 500 bytes
dbutils.fs.head("/databricks-datasets/power-plant/README.md", 1000)

# COMMAND ----------

# sample files
dbutils.fs.ls("/databricks-datasets/power-plant/data")


# COMMAND ----------

# show top 500 bytes
dbutils.fs.head("/databricks-datasets/power-plant/data/Sheet1.tsv", 500)

# COMMAND ----------



# COMMAND ----------

# Can write to local storage
dbutils.fs.put("/Insight/hello-world.txt", "This is my first program")

# COMMAND ----------

# Show new file
dbutils.fs.ls("/Insight/hello-world.txt")

# COMMAND ----------

# Remove new file
dbutils.fs.rm("/Insight/hello-world.txt")

# COMMAND ----------



# COMMAND ----------

# Can write to external storage
path = "/dbfs/mnt/datalake/hello-world.txt"
f = open(path, "w")
f.write("This is my first program")
f.close()


# COMMAND ----------

# look in top directory
import glob  
for name in glob.glob('/dbfs/mnt/datalake/*.txt'):
  print(name)
  

# COMMAND ----------

# traverse directories
import os

print("~ files ~\n")
for root, dirs, files in os.walk("/dbfs/mnt/datalake/bronze", topdown=False):
  for name in files:
    print(os.path.join(root, name))

print("\n~ directories ~\n")
for root, dirs, files in os.walk("/dbfs/mnt/datalake/bronze", topdown=False):
  for name in dirs:
    print(os.path.join(root, name))
    

# COMMAND ----------

# Remove new file
import os
os.remove("/dbfs/mnt/datalake/hello-world.txt")


# COMMAND ----------



# COMMAND ----------

#
# 3.3 - Copy files and folders
#

# remove existing
try:
  dbutils.fs.rm("/lake/data", recurse=True)  
except:
  pass

# copy dir
dbutils.fs.cp("/databricks-datasets/power-plant/data", "/lake/data", recurse=True)

# copy file
dbutils.fs.cp("/databricks-datasets/power-plant/README.md", "/lake/data/README.md")

# show folder structure
dbutils.fs.ls("/lake/data")


# COMMAND ----------



# COMMAND ----------

#
# 3.4 - Move files and folders
#

# bronze -> junk
dbutils.fs.mv("/lake/bronze", "/lake/junk", recurse=True)

# data -> bronze
dbutils.fs.mv("/lake/data", "/lake/bronze", recurse=True)

# readme.md -> readme.txt
dbutils.fs.mv("/lake/bronze/README.md", "/lake/bronze/readme.txt")

# show directory contents
dbutils.fs.ls("/lake/bronze")


# COMMAND ----------



# COMMAND ----------

#
# 3.4 - Remove files and folders
#



# COMMAND ----------

# remove 1 file
dbutils.fs.rm("dbfs:/lake/bronze/readme.txt")


# COMMAND ----------

# show dir
dbutils.fs.ls("/lake/bronze")

# COMMAND ----------

# no wild cards
dbutils.fs.rm("dbfs:/lake/bronze/*.tsv")


# COMMAND ----------

# remove dir + sub dirs
dbutils.fs.rm("/lake", recurse=True)


# COMMAND ----------

# show dirs
# dbutils.fs.ls("/lake/")
dbutils.fs.ls("/")


# COMMAND ----------



# COMMAND ----------

#
# 4.0 - Manage remote storage
#

# COMMAND ----------

#
# 4.1 - show mounts + files
#

# COMMAND ----------

# show existing mounts
dbutils.fs.mounts()

# COMMAND ----------

# adventure works files?
dbutils.fs.ls("/mnt/datalake/test")

# COMMAND ----------

# Unmount storage 
dbutils.fs.unmount("/mnt/datalake/")


# COMMAND ----------

#
# 4.2 - Re-mount Storage
#

# Set parameters for notebook
parms = {
"account_name": "sa4rissug2020",
"file_system": "sc4rissug2020",
"mount_point_path": "/mnt/datalake",
}

# Run notebook with selections
ret = dbutils.notebook.run("./2-nb-mount-adls-fs", 60, parms)

# Show return value if any
print(ret)



# COMMAND ----------

# adventure works files?
dbutils.fs.ls("/mnt/datalake/test")

# COMMAND ----------


