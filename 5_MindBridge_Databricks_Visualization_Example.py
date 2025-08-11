# Databricks notebook source
# MAGIC %md
# MAGIC ![](https://www.mindbridge.ai/wp-content/uploads/2021/07/MindBridge_Logo_Primary_RGB.png)
# MAGIC # MindBridge -> Databricks Example : Leverage MindBridge analysis results for additional visualization/analysis Example

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Import necessary libraries and set up the MindBridge API connection

# COMMAND ----------

# MAGIC %pip install --upgrade mindbridge-api-python-client
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mindbridgeapi as mbapi

# Provide the MindBridge API URL and the API token (replace with your actual token)
url = "[insert tenant].mindbridge.ai"
token = dbutils.secrets.get(scope="mindbridge-api-tutorials", key="MINDBRIDGE_API_TOKEN")

# Initialize a connection to the MindBridge server using the API token
server = mbapi.Server(url=url, token=token)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Define engagement and analysis details
# MAGIC

# COMMAND ----------

# Replace with the actual engagement and analysis IDs
engagement_id = "[insert your engagement id here]"

# Fetch the engagement and related analysis information
engagement = server.engagements.get_by_id(engagement_id)
analysis = next(engagement.analyses)

# Print the organization and analysis details for confirmation
organization = server.organizations.get_by_id(engagement.organization_id)
print(f"{organization.name} / {engagement.name} / {analysis.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Request data for specific risk areas from the general ledger table
# MAGIC

# COMMAND ----------

# Define the output file path where the results will be saved
output_file = "/Volumes/[insert your target file location path here]"
print("Requesting Elevated Risk General Ledger Transactions")

# Restart data tables to ensure the latest results are available for the analysis
server.analyses.restart_data_tables(analysis)

# Select the general ledger journal table from the analysis data tables
data_table = next(x for x in analysis.data_tables if x.logical_name == "gl_journal_tx")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4: Query data with specific risk criteria
# MAGIC

# COMMAND ----------

# Define a query to extract transactions with a risk score greater than or equal to 3000
query = {"risk": {"$gte": 3000}}

# Export the filtered data asynchronously
export_async_result = server.data_tables.export(data_table, query=query)
server.data_tables.wait_for_export(export_async_result)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5: Download and save the exported data locally
# MAGIC

# COMMAND ----------

# Define the output path for saving the CSV file
path_output = server.data_tables.download(
    export_async_result,
    output_file_path=output_file,
)
print(f"Success! Saved to: {path_output}")


# COMMAND ----------

# MAGIC %md
# MAGIC Step 6: Load the exported CSV file into a Spark DataFrame

# COMMAND ----------

# Read the CSV file saved in the previous step into a Spark DataFrame for further processing
df = spark.read.csv(output_file,
  header=True,
  inferSchema=True,
  sep=",")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7: Display the DataFrame
# MAGIC

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 8: Store the DataFrame in a Unity Catalog table
# MAGIC

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("dbwork1.91m_complex.highrisk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 1. In Databricks, use notebooks to create visualizations using the table you created above.
# MAGIC 2. Use SQL → Dashboards to create dashboards, or SQL → Genie to work with your data using natural language queries.
# MAGIC 3. To view the analysis in the full MindBridge user interface, use the analysis URL, below.

# COMMAND ----------

analysis_url = f"https://{url}/app/organization/{organization.id}/engagement/{engagement.id}/analysis/{analysis.latest_analysis_result_id}"

print(f"Analysis URL: {analysis_url}")
