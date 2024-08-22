-- Databricks notebook source
-- MAGIC %md
-- MAGIC # On the provider side
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

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
PARTITIONED BY (plant)
TBLPROPERTIES (delta.enableDeletionVectors = false);

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
-- MAGIC ## Create view

-- COMMAND ----------

CREATE OR REPLACE VIEW plant1_material_number_demand_view AS
SELECT *
FROM material_number_demand
WHERE plant = 'Plant1';

-- COMMAND ----------

-- This cannot be shared
--CREATE OR REPLACE VIEW material_number_demand_dynamic_view AS
--SELECT *
--FROM material_number_demand
--WHERE is_account_group_member('account users');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create shares

-- COMMAND ----------

-- Drop the share if it exists
DROP SHARE IF EXISTS plant_1_share;

-- Drop the second share if it exists
DROP SHARE IF EXISTS plant_2_share;

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS `plant_1_share`;
CREATE SHARE IF NOT EXISTS `plant_2_share`;

DESCRIBE SHARE plant_1_share;

-- If this wasn't a demo you should set the ower to a group insted of a user
-- ALTER SHARE plant_1_share OWNER TO `some admin group`;
-- ALTER SHARE plant_2_share OWNER TO `some admin group`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Add tables to the shares

-- COMMAND ----------

ALTER SHARE plant_1_share ADD TABLE material_number_demand PARTITION (plant = "Plant1");
ALTER SHARE plant_2_share ADD TABLE material_number_demand PARTITION (plant = "Plant2");
ALTER SHARE plant_1_share ADD VIEW plant1_material_number_demand_view;

-- COMMAND ----------

-- This cannot be shared
--ALTER SHARE plant_1_share ADD VIEW material_number_demand_dynamic_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a recipient

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Reciever needs to tell the provider their metastore
-- MAGIC - Using this id the recipient can be created
-- MAGIC - The metastore id can only be used once when creating the recipient. It was already created using this recipient 'field_eng_self'. For the course of the demo, use this recipient

-- COMMAND ----------

SELECT current_metastore();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create "access" for the recipient

-- COMMAND ----------

GRANT SELECT ON SHARE plant_1_share TO RECIPIENT field_eng_self;
GRANT SELECT ON SHARE plant_2_share TO RECIPIENT field_eng_self;

-- COMMAND ----------

-- This cannot be shared
--GRANT SELECT ON SHARE plant_1_share TO RECIPIENT aws_e2demofieldeng;

-- COMMAND ----------

SHOW GRANT ON SHARE plant_1_share;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # On the recipient side

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Switching roles to the recipoent, we need to find out the provider's name. See this link
-- MAGIC https://docs.databricks.com/en/delta-sharing/manage-provider.html#do-recipients-need-to-create-provider-objects. 
-- MAGIC - As the same workspace (an therefore metsatore) is used on the providers and the receivers side, we can easily get the provider clicking in the UI --> Ctalog --> Delta Sharing --> Shared by me --> somewhere top right. In this example it is 'azure:eastus2:b86c6879-8c55-4e70-a585-18d16a4fa6e9'

-- COMMAND ----------

SHOW SHARES IN PROVIDER `azure:eastus2:databricks:field-eng-east`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The following needs priviledges:
-- MAGIC - USE PROVIDER: In Delta Sharing, this privilege gives a recipient user read-only access to all providers in a recipient metastore and their shares. Combined with the CREATE CATALOG privilege, this privilege allows a recipient user who is not a metastore admin to mount a share as a catalog. This helps limit the number of users with the powerful metastore admin role
-- MAGIC - USE SHARE: In Delta Sharing, this privilege gives a provider user read-only access to all shares defined in a provider metastore. This allows a provider user who is not a metastore admin to list shares and list the assets (tables and notebooks) in a share, along with the share’s recipients.
-- MAGIC

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS max_shared_data USING SHARE `azure:eastus2:databricks:field-eng-east`.plant_1_share;

-- COMMAND ----------

select * from max_shared_data.sharing_patterns.material_number_demand;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Access to this catalog can be managed
-- MAGIC

-- COMMAND ----------

SHOW GRANTS ON CATALOG max_shared_data;

-- COMMAND ----------

-- Grant USE CATALOG privilege to the group
GRANT USE CATALOG ON CATALOG max_shared_data TO `account users`;

-- Grant USE SCHEMA privilege to the group for each schema in the catalog
GRANT USE SCHEMA ON SCHEMA max_shared_data.sharing_patterns TO `account users`;

-- Grant SELECT privilege to the group for each table in the schema
GRANT SELECT ON TABLE max_shared_data.sharing_patterns.material_number_demand TO `account users`;

-- COMMAND ----------

SHOW GRANTS ON CATALOG max_shared_data;

-- COMMAND ----------

select * from max_shared_data.sharing_patterns.plant1_material_number_demand_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A note on sharing data using properties

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You share views using https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/current_recipient
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC -- This does not work for me since I do not have permission to change the recipient for the given metastore id
-- MAGIC
-- MAGIC --ALTER RECIPIENT field_eng_self SET PROPERTIES ('plant' = 'plant1');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC --Did not test, just to show the concept 
-- MAGIC
-- MAGIC --CREATE VIEW test_just_to_demonstarte_sharing_patterns AS
-- MAGIC
-- MAGIC --    SELECT * FROM material_number_demand
-- MAGIC
-- MAGIC --    WHERE plant = current_recipient('plant');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Clean Up

-- COMMAND ----------

--DROP CATALOG IF EXISTS max_shared_data CASCADE;
--DROP CATALOG IF EXISTS max_demos_delta_sharing CASCADE;
--DROP SHARE IF EXISTS plant_1_share;
--DROP SHARE IF EXISTS plant_2_share;

-- COMMAND ----------


