# Databricks notebook source
# MAGIC %pip install delta-sharing

# COMMAND ----------

import delta_sharing

# COMMAND ----------

# MAGIC %md
# MAGIC # On the provider side
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate a table to share

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS max_demos_delta_sharing;
# MAGIC USE CATALOG max_demos_delta_sharing;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS sharing_patterns;
# MAGIC USE SCHEMA sharing_patterns;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE material_number_demand (
# MAGIC     material_number STRING,
# MAGIC     plant STRING,
# MAGIC     demand DOUBLE
# MAGIC )
# MAGIC PARTITIONED BY (plant)
# MAGIC TBLPROPERTIES (delta.enableDeletionVectors = false);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO material_number_demand (material_number, plant, demand)
# MAGIC VALUES 
# MAGIC     ('M123451', 'Plant1', 100.0),
# MAGIC     ('M678901', 'Plant2', 200.0),
# MAGIC     ('M123452', 'Plant1', 101.0),
# MAGIC     ('M678902', 'Plant2', 201.0),
# MAGIC     ('M123453', 'Plant1', 103.0),
# MAGIC     ('M678903', 'Plant2', 203.0),
# MAGIC     ('M123454', 'Plant1', 104.0),
# MAGIC     ('M678904', 'Plant2', 204.0);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from material_number_demand;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create shares

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the share if it exists
# MAGIC DROP SHARE IF EXISTS plant_1_share;
# MAGIC
# MAGIC -- Drop the second share if it exists
# MAGIC DROP SHARE IF EXISTS plant_2_share;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE IF NOT EXISTS `plant_1_share`;
# MAGIC CREATE SHARE IF NOT EXISTS `plant_2_share`;
# MAGIC
# MAGIC DESCRIBE SHARE plant_1_share;
# MAGIC
# MAGIC -- If this wasn't a demo you should set the ower to a group insted of a user
# MAGIC -- ALTER SHARE plant_1_share OWNER TO `some admin group`;
# MAGIC -- ALTER SHARE plant_2_share OWNER TO `some admin group`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add tables to the shares

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER SHARE plant_1_share ADD TABLE material_number_demand PARTITION (plant = "Plant1");
# MAGIC ALTER SHARE plant_2_share ADD TABLE material_number_demand PARTITION (plant = "Plant2");

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW ALL IN SHARE plant_1_share;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a recipient

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP RECIPIENT IF EXISTS plant1_recipient;
# MAGIC DROP RECIPIENT IF EXISTS plant2_recipient;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE RECIPIENT IF NOT EXISTS plant1_recipient;
# MAGIC CREATE RECIPIENT IF NOT EXISTS plant2_recipient;
# MAGIC
# MAGIC -- For the demo we'll grant ownership to all users. Typical deployments wouls have admin groups or similar.
# MAGIC --ALTER RECIPIENT plant1_recipient OWNER TO `some_group`;
# MAGIC --ALTER RECIPIENT plant2_recipient OWNER TO `some_group`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create "access" for the recipient

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE RECIPIENT plant1_recipient;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SHARE plant_1_share TO RECIPIENT plant1_recipient;
# MAGIC GRANT SELECT ON SHARE plant_2_share TO RECIPIENT plant2_recipient;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANT ON SHARE plant_1_share;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This is to revoke the grant of a share
# MAGIC --REVOKE SELECT ON SHARE ... FROM RECIPIENT ...;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW ALL IN SHARE plant_1_share;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # On the recipient side
# MAGIC

# COMMAND ----------

dbfs_path = "dbfs:/Users/max.kohler@databricks.com/shared"

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/Users/max.kohler@databricks.com/shared")

# COMMAND ----------

#dbutils.fs.rm(dbfs_path, recurse=True)

# COMMAND ----------


###################################################################
# This is a one time effort
###################################################################



############################
# Import necessary libraries
############################

import requests
import json
from pyspark.sql import SparkSession
import os

############################
# Get the activation links
############################

links = \
[
  (spark.sql("DESCRIBE RECIPIENT plant1_recipient")
                          .filter("info_name = 'activation_link'")
                          .select("info_value").collect()[0][0]),
  (spark.sql("DESCRIBE RECIPIENT plant2_recipient")
                          .filter("info_name = 'activation_link'")
                          .select("info_value").collect()[0][0])
]

plants = ["plant1", "plant2"]

activation_links = dict(zip(plants, links))

############################
# Create the dbfs path
############################
dbutils.fs.mkdirs(dbfs_path)


############################
# Make a request to the activation link to download the credential file
############################
response_plant1 = requests.get(activation_links.get("plant1"))
if response_plant1.status_code == 200:
    credential_file_contents_plant1 = response_plant1.text
else:
    raise Exception(f"Failed to download the credential file. Status code: {response_plant1.status_code}")

response_plant2 = requests.get(activation_links.get("plant2"))
if response_plant2.status_code == 200:
    credential_file_contents_plant2 = response_plant2.text
else:
    raise Exception(f"Failed to download the credential file. Status code: {response_plant2.status_code}")

############################
# Save the credential file contents to DBFS
############################
dbutils.fs.put(os.path.join(dbfs_path, "plant1.share"), credential_file_contents_plant1, overwrite=True)
dbutils.fs.put(os.path.join(dbfs_path, "plant2.share"), credential_file_contents_plant2, overwrite=True)

# COMMAND ----------

print("List the files")
display(dbutils.fs.ls(dbfs_path))
print("/n/n/n ------------------------------------ /n/n/n")
print("Print the file for plant 1")
display(dbutils.fs.head(os.path.join(dbfs_path, "plant1.share"), 1000))
print("/n/n/n ------------------------------------ /n/n/n")
print("Print the file for plant 2")
display(dbutils.fs.head(os.path.join(dbfs_path, "plant2.share"), 1000))

# COMMAND ----------

files = dbutils.fs.ls("/Users/max.kohler@databricks.com/shared/")
display(files)

# COMMAND ----------

profile_path = '/dbfs/Users/max.kohler@databricks.com/shared/plant1.share'

# COMMAND ----------

# Create a SharingClient
client = delta_sharing.SharingClient(profile_path)

# List all shared tables.
client.list_all_tables()
