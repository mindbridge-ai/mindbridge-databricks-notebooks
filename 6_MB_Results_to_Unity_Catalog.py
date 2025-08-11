# Databricks notebook source
# MAGIC %md
# MAGIC ![](https://www.mindbridge.ai/wp-content/uploads/2021/07/MindBridge_Logo_Primary_RGB.png)
# MAGIC # Databricks -> MindBridge Example: Subledger_Results_to_Unity_Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧾 Step 0: Setup Environment and Define Parameters
# MAGIC
# MAGIC 📦 This step imports the necessary libraries and sets global parameters used throughout the notebook.
# MAGIC
# MAGIC - `token`: Your MindBridge API key
# MAGIC - `catalog_table`: Unity Catalog table containing source data
# MAGIC - `organization_id`, `engagement_id`, `analysis_type_id`: MindBridge context
# MAGIC - `risk_threshold`: Minimum risk to export from results (use 0 for all entries)
# MAGIC - `output_table_path`: Unity Catalog destination for result export
# MAGIC

# COMMAND ----------

# MAGIC %pip install --upgrade mindbridge-api-python-client
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 MindBridge API Authentication
# MAGIC Connect to the MindBridge platform using a securely stored API token.

# COMMAND ----------

# ── MindBridge API Authentication ─────────────────────────────────────────────
# Retrieve your API token securely from Databricks secrets and set the token equal to that API key
#
# Before running this cell:
# 1. Ensure your Databricks workspace has access to a secret scope linked to an Azure Key Vault.
# 2. Store your MindBridge API token in the Key Vault under a recognizable key name.
# 3. Replace the values below with your actual scope and key name.
#
# 📍 You can create or retrieve your API token by visiting:
#     https://[your_tenant].mindbridge.ai/app/admin/api-create-token

secret_scope_name = "integration-secret-scope"         # Replace with your secret scope
secret_key_name = "mindbridge-demo-api-key"        # Replace with your key name in Azure Key Vault

token = dbutils.secrets.get(scope=secret_scope_name, key=secret_key_name)

print("✅ MindBridge API token successfully retrieved.")


# COMMAND ----------

# ── MindBridge Configuration ──────────────────────────────────────────────
url = "demo.mindbridge.ai"
organization_id = "67a4e11e9bf85b335ff95535"
engagement_id = "6807f4517f8b8f332f5921bc"
analysis_type_id = "6807f2ca1484440ffc362062" # In your analysis configuration file, may require MB support.

# ── Analysis Results Export ───────────────────────────────────────────────
risk_threshold = 0
# 📌 Note: Risk score is scaled by 100 (e.g., 3000 = 30.00 UI). Use 0 to include all entries.

# ── Unity Catalog Output Table ─────────────────────────────────────────────
output_catalog = "dbwork1"
output_schema = "iceberg_storage"
output_table_name = "revenue_MB_results"
output_table_path = f"{output_catalog}.{output_schema}.{output_table_name}"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📡 Connect to MindBridge API
# MAGIC
# MAGIC Initialize the Python client and fetch your organization and engagement context from MindBridge.
# MAGIC

# COMMAND ----------

# Import libraries
import mindbridgeapi as mbapi
from pathlib import Path
import pandas as pd

# Connect to the MindBridge server using the configured API key and tenant URL
server = mbapi.Server(url=url, token=token)

# Fetch organization and engagement context objects
organization = server.organizations.get_by_id(organization_id)
engagement = server.engagements.get_by_id(engagement_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Select Active Analysis
# MAGIC
# MAGIC Fetch the analysis object associated with the current engagement.  
# MAGIC This analysis will be used when linking the uploaded file as a new source.
# MAGIC

# COMMAND ----------

# 🔄 Refresh the engagement to get a fresh generator
engagement = server.engagements.get_by_id(engagement_id)

# 📎 Retrieve analysis from engagement
analyses = list(engagement.analyses)  # Convert generator to list

if not analyses:
    raise ValueError("❌ No analyses found in this engagement. Please create one in the MindBridge UI.")
elif len(analyses) > 1:
    raise ValueError(f"❌ Multiple analyses found ({len(analyses)}). Please ensure only one is present or filter/select explicitly.")

# ✅ Exactly one analysis found
analysis = analyses[0]
print(f"✅ Using analysis (id: {analysis.id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 View Analysis Results in MindBridge
# MAGIC
# MAGIC Once the analysis completes, we retrieve the latest result ID and generate a direct link to open the results within the MindBridge UI.

# COMMAND ----------

# Get the result ID for the most recent run of the analysis
analysis_result_id = analysis.latest_analysis_result_id
print(analysis_result_id)

# COMMAND ----------

# Create a direct link to open the analysis results in MindBridge
url_analysis = (
    'https://' + url + '/app/organization/' + str(organization.id) +
    '/engagement/' + engagement.id + "/analysis/" +
    analysis_result_id + "/analyze/risk-overview?"
)
html_code = f'<a href="{url_analysis}" target="_blank">Open Analysis Results in MindBridge</a>'
displayHTML(html_code)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Prepare to Export Entries from MindBridge
# MAGIC
# MAGIC We'll now extract entries from the analysis based on the current `risk_threshold`, save them to a local file, and prepare for writing to Unity Catalog.

# COMMAND ----------

# Define the output file path where results will be downloaded
from pathlib import Path
output_path = Path("/tmp/mindbridge_export_output_entries.csv")


# COMMAND ----------

# Ensure the analysis results are refreshed before exporting
server.analyses.restart_data_tables(analysis)

# Inspect available data tables in the analysis and locate the main output
for data_table in analysis.data_tables:
    print(data_table.id, data_table.logical_name, data_table.type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Export Filtered Entries from MindBridge
# MAGIC
# MAGIC This step pulls entries that meet your risk threshold and downloads the results as a local CSV file.
# MAGIC

# COMMAND ----------

# Define a query to extract entries with risk >= threshold (e.g. 3000 = 30.00 in UI)
query = { "risk": { "$gte": risk_threshold } }

# Start export and wait for results to be ready
export_async_result = server.data_tables.export(data_table, query=query)
server.data_tables.wait_for_export(export_async_result)

# COMMAND ----------

# Download the filtered entries to a local CSV file
path_output = server.data_tables.download(
    export_async_result,
    output_file_path=output_path,
)
print(f"✅ Success! Saved to: {path_output}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Save Results to Unity Catalog for Downstream Use
# MAGIC We now load the exported file, convert to Spark, and write it to Unity Catalog so it can be leveraged in downstream dashboards, reports, and workflow tools.

# COMMAND ----------

import pandas as pd

# Read the local CSV output from MindBridge
df = pd.read_csv(output_path)

print(df.columns)  # Check the columns in the DataFrame

# Find columns that contain '_date'
date_columns = [col for col in df.columns if '_date' in col]

if date_columns:
    # Ensure all '_date' columns are in datetime format
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
else:
    raise KeyError("No columns containing '_date' found in the CSV file")

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(df)

display(spark_df)

# COMMAND ----------

# Save results as a Delta table in Unity Catalog
spark_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(output_table_path)

print(f"📊 MindBridge results saved to Unity Catalog as: {output_table_path}")
print("📢 You can now publish this table to Power BI via the Databricks UI.")
