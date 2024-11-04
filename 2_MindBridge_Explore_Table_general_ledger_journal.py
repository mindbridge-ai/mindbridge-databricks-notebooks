# Databricks notebook source
# MAGIC %md
# MAGIC ![](https://www.mindbridge.ai/wp-content/uploads/2021/07/MindBridge_Logo_Primary_RGB.png)
# MAGIC # Databricks -> MindBridge Example : Exploring the General Ledger within MindBridge

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Query Data from the general_ledger_journal Table. Note: An example query is included below but will not work for your specific Databricks instance.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `dbwork1`.`91m_complex`.`year_2021_general_ledger_journal_1` where transaction_id < 50000;

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Save Queried Data as a Compressed File
# MAGIC Note:  In the future, this will be made more direct like a [Table Share](https://adb-3836261087006569.9.azuredatabricks.net/sql/editor/6c9e5da0-f9f8-4a6f-a67a-7326f403caac?o=3836261087006569)  Note 2: Example file paths are are included below but will not work for your specific Databricks instance.
# MAGIC
# MAGIC

# COMMAND ----------


# Define paths for saving compressed CSV files
vol_path_dev = "/Volumes/dbwork1/91m_complex/temporary/sqldf/"
vol_path_copy = "/Volumes/dbwork1/91m_complex/temporary/upload.csv.gz"

# Write the SQL query results to a compressed CSV file
_sqldf.coalesce(1).write.mode("overwrite").csv(vol_path_dev, header = True, compression="gzip")

# Check if the CSV file was just created and copy it to a new location with a .gz extension
file_list = dbutils.fs.ls(vol_path_dev)

# Loop through files to find the CSV and rename with .gz extension
for filename in file_list:
  if filename.name.endswith('.csv.gz'):
    name = filename.name
    dbutils.fs.cp(vol_path_dev + name, vol_path_copy)

