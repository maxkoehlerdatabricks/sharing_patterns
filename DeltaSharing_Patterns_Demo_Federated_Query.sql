-- Databricks notebook source
-- MAGIC %md
-- MAGIC I need to create a table in a 3 level namespace on the provider side
-- MAGIC I need to create a serverless dbsql cluster on the provider side
-- MAGIC I need to provide it in a separate catalog in 3 level namesspace on the recipient side. This should be done via federated queries. Shoild include the compute and the table ---- ausformulieren!!!
-- MAGIC I need to query the data using a separate compute on the recipient side
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC https://learn.microsoft.com/en-us/azure/databricks/query-federation/databricks
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # On the provider side

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Generate a table to share

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS max_demos_delta_sharing;
USE CATALOG max_demos_delta_sharing;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS sharing_patterns;
USE SCHEMA sharing_patterns;

-- COMMAND ----------

CREATE OR REPLACE TABLE material_number_demand (
    material_number STRING,
    plant STRING,
    demand DOUBLE
);

-- COMMAND ----------

INSERT INTO material_number_demand (material_number, plant, demand)
VALUES 
    ('M123451', 'Plant1', 100.0),
    ('M678901', 'Plant2', 200.0),
    ('M123452', 'Plant1', 101.0),
    ('M678902', 'Plant2', 201.0),
    ('M123453', 'Plant1', 103.0),
    ('M678903', 'Plant2', 203.0),
    ('M123454', 'Plant1', 104.0),
    ('M678904', 'Plant2', 204.0);

-- COMMAND ----------

select * from material_number_demand;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Create a DSBQL warehouse called 'max_test_provider'
-- MAGIC - Create a PAT Token, xxx
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # On the Recipient Side

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Create DBSQL warehouse called 'max_test_recipient'

-- COMMAND ----------

CREATE CONNECTION shared_data TYPE databricks
OPTIONS (
  host 'https://adb-984752964297111.11.azuredatabricks.net/',
  httpPath '/sql/1.0/warehouses/f81dd263fe7cd4bd',
  personalAccessToken '...'
);

-- COMMAND ----------

--DROP CATALOG IF EXISTS max_shared_data CASCADE;
