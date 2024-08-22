-- Databricks notebook source
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
)
CLUSTER BY (plant);

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
-- MAGIC ## Create table for row level access

-- COMMAND ----------

SELECT current_user(), is_account_group_member('account users');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION plant_filter(plant STRING) 
RETURN 
  --is_account_group_member('account_users');
  plant = 'Plant1'

--ALTER FUNCTION region_filter OWNER TO `some_group`; 

-- COMMAND ----------

ALTER TABLE material_number_demand SET ROW FILTER plant_filter ON (plant);

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Create a dynamic view

-- COMMAND ----------

CREATE OR REPLACE VIEW plant1_material_number_demand_view AS
SELECT *
FROM material_number_demand
WHERE plant = 'Plant1'; -- where is_account_group_member('account_users');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # On the recipient side

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The table with the udf

-- COMMAND ----------

-- Can also be executed on a DBSQL cluster
select * from material_number_demand;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The dynamic view

-- COMMAND ----------

-- Just to make sure ...
ALTER TABLE material_number_demand DROP ROW FILTER;
select * from material_number_demand;

-- COMMAND ----------

select * from plant1_material_number_demand_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Clean Up

-- COMMAND ----------

--DROP CATALOG IF EXISTS max_demos_delta_sharing CASCADE;
