# Databricks notebook source
# MAGIC %md
# MAGIC ![](https://www.mindbridge.ai/wp-content/uploads/2021/07/MindBridge_Logo_Primary_RGB.png)
# MAGIC # Databricks -> MindBridge Example: Ingest and Run a MindBridge Subledger Analysis

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

# MAGIC %pip install typing_extensions==4.5.0
# MAGIC %pip install --upgrade mindbridge-api-python-client
# MAGIC %restart_python

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
secret_key_name = "mindbridge-uat-perf-api-key"        # Replace with your key name in Azure Key Vault

token = dbutils.secrets.get(scope=secret_scope_name, key=secret_key_name)

print("✅ MindBridge API token successfully retrieved.")


# COMMAND ----------

# ── Unity Catalog Input Table ─────────────────────────────────────────────
catalog = "dbwork1"
schema = "default"
table_name = "vendor_demo"
catalog_table = f"{catalog}.{schema}.{table_name}"  # e.g. dbwork1.iceberg_storage.vendor_demo

# ── MindBridge Configuration ──────────────────────────────────────────────
url = "uat-perf.mindbridge.ai"
organization_id = "67eea6518242093533b1b9f7"
engagement_id = "67eea8838242093533b1ba95"
analysis_type_id = "67cde2c5f336d647f74307f0" # In your analysis configuration file, may require MB support.

# ── Analysis Results Export ───────────────────────────────────────────────
risk_threshold = 0
# 📌 Note: Risk score is scaled by 100 (e.g., 3000 = 30.00 UI). Use 0 to include all entries.

# ── Unity Catalog Output Table ─────────────────────────────────────────────
output_catalog = "dbwork1"
output_schema = "iceberg_storage"
output_table_name = "vendor_MB_results"
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
# MAGIC ## 📤 Upload File to MindBridge
# MAGIC
# MAGIC This step uploads the prepared source file to MindBridge and registers it as the analysis source for an existing analysis.  
# MAGIC It automatically applies column mappings based on file headers.
# MAGIC

# COMMAND ----------

import os

# Read from the Iceberg table
df = spark.read.table(catalog_table)

# Convert to pandas
pdf = df.toPandas()

# Save to local path
local_path = "/tmp/mindbridge_source.csv"
pdf.to_csv(local_path, index=False)

# Upload to MindBridge
file_manager_file = server.file_manager.upload(
    input_item=mbapi.FileManagerItem(engagement_id=engagement.id),
    input_file=local_path,
)

print("✅ File uploaded to MindBridge successfully.")

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
# MAGIC ## 🧱 Create Analysis Source Object
# MAGIC
# MAGIC Use the uploaded file to build a new `AnalysisSourceItem` and attach it to the target analysis.  
# MAGIC We use the first available analysis period and set the workflow to `COMPLETED` to trigger scoring.
# MAGIC
# MAGIC Note: The `analysis_source_type` is pulled from the analysis type definition and should have only one option.
# MAGIC

# COMMAND ----------

# Get analysis type definition by ID
analysis_type = server.analysis_types.get_by_id(analysis_type_id)

# There should be only one analysis_source_type associated with this analysis type
analysis_source_type = next(analysis_type.analysis_source_types)
try:
    _ = next(analysis_type.analysis_source_types)  # Try fetching another, should raise StopIteration
    raise ValueError("Should only be 1 analysis type")  # Error if more than 1 found
except StopIteration:
    pass  # As expected — only one source type

# Create the analysis source item using the uploaded file
analysis_source = mbapi.AnalysisSourceItem(
    engagement_id=engagement_id,
    analysis_id=analysis.id,
    analysis_source_type_id=analysis_source_type.id,
    file_manager_file_id=file_manager_file.id,
    analysis_period_id=analysis.analysis_periods[0].id,  # Use current analysis period
    target_workflow_state=mbapi.TargetWorkflowState.COMPLETED,  # Set status to 'completed' to trigger scoring
)

# Submit the analysis source to MindBridge
analysis_source_b = server.analysis_sources.create(analysis_source)
print(f"✅ Analysis source created (id: {analysis_source_b.id})")
print("🧠 Columns will be mapped automatically based on headers.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Validate Analysis Sources and Run Analysis
# MAGIC
# MAGIC Before the analysis can begin, MindBridge must complete validation of uploaded sources.  
# MAGIC If validation fails (e.g. due to unverified account mappings), a link to correct the issue in the UI will be shown.

# COMMAND ----------

try:
    # ⏳ Wait for MindBridge to validate uploaded analysis sources
    analysis = server.analyses.wait_for_analysis_sources(analysis)

except Exception as err:
    # 🧩 Most common issue: unverified account mappings
    print("❌ Validation error:", str(err))

    # 📎 Link user to engagement's analysis list in UI
    url_error = f"https://{url}/app/organization/{organization.id}/engagement/{engagement.id}/analysis-list"
    html_code = f'<a href="{url_error}" target="_blank">🔗 Open Analysis in MindBridge to resolve the issue</a>'
    displayHTML(html_code)

else:
    print("✅ All analysis sources validated — ready to run analysis.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Run the Analysis and Wait for Completion
# MAGIC
# MAGIC Once validation has passed, this step triggers your Subledger analysis in MindBridge.  
# MAGIC The notebook will then wait until the analysis is finished before continuing.
# MAGIC
# MAGIC ⏳ **Note:** Large datasets or complex configurations may take time to process.  
# MAGIC You can schedule this notebook or revisit it later after execution.
# MAGIC

# COMMAND ----------

# 🚀 Trigger the analysis run
analysis = server.analyses.run(analysis)
print("⏳ Analysis is running. Please wait...")

# 🔁 Poll until analysis is complete
analysis = server.analyses.wait_for_analysis(analysis)
print("✅ Analysis completed successfully.")
print(server.http.headers["user-agent"])  # Shows API client version

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

# Ensure 'date' column is in datetime format
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(df)


# COMMAND ----------

# Save results as a Delta table in Unity Catalog
spark_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(output_table_path)

print(f"📊 MindBridge results saved to Unity Catalog as: {output_table_path}")
print("📢 You can now publish this table to Power BI via the Databricks UI.")
