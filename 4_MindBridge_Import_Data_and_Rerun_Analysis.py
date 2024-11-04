# Databricks notebook source
# MAGIC %md
# MAGIC ![](https://www.mindbridge.ai/wp-content/uploads/2021/07/MindBridge_Logo_Primary_RGB.png)
# MAGIC # Databricks -> MindBridge Example : Run MindBridge Periodic Analysis on Updated Data

# COMMAND ----------

# MAGIC %md
# MAGIC MindBridge API Installation in Databricks is as simple as...

# COMMAND ----------

# Install the MindBridge API
%pip install --upgrade mindbridge-api-python-client
%restart_python
%pip show mindbridge-api-python-client

import mindbridgeapi as mbapi

# COMMAND ----------

# MAGIC %md
# MAGIC We start by connecting to a MindBridge tenant. If you want to find out how to create an API key for your MindBridge tenant you can check out the guide [here](https://www.mindbridge.ai/support/api/) .

# COMMAND ----------

# Provide the MindBridge API URL and the API token (replace with your actual token)
url = "[insert tenant].mindbridge.ai"
token = dbutils.secrets.get(scope="mindbridge-api-tutorials", key="MINDBRIDGE_API_TOKEN")

# Initialize a connection to the MindBridge server using the API token
server = mbapi.Server(url=url, token=token)

# COMMAND ----------

# MAGIC %md
# MAGIC To simplify later steps, we will create and define variables for all of the needed IDs which relate to our existing analysis.

# COMMAND ----------

# These can be found in the URL from any page in your engagement
organization_id = "[insert your organization_id here]"
engagement_id = "[insert your engagement_id here]"

# Pull back the information related to your organization
organization = server.organizations.get_by_id(organization_id)
engagement = server.engagements.get_by_id(engagement_id)

# COMMAND ----------

# MAGIC %md
# MAGIC Now load a demo dataset from Databricks into your MindBridge Analysis. This can be done in many different ways with additional details in our knowledge base here: https://support.mindbridge.ai/hc/en-us/sections/4408298361879-Import-files
# MAGIC
# MAGIC Note: This requires that you have an existing Analysis set up within your tenant that you want to upload new data to.  If you have not yet set up your analysis, you can use the "MindBridge | Databricks Analysis Data In Flow" template to perform these steps.

# COMMAND ----------

# Setup the engagement data
gl_path = '[insert the path to your source dataset here]'
file_manager_file = mbapi.FileManagerItem(engagement_id=engagement.id)
file_manager_file = server.file_manager.upload(
    input_item=file_manager_file,
    input_file=gl_path,
)


# COMMAND ----------

analysis = next(engagement.analyses)

# COMMAND ----------

# MAGIC %md
# MAGIC Next we create an Analysis source using the dataset that we just uploaded and we assume the input file is in good shape.

# COMMAND ----------

analysis_source = mbapi.AnalysisSourceItem(
    engagement_id=engagement_id,
    analysis_id=analysis.id,
    file_manager_file_id=file_manager_file.id,
    analysis_period_id=analysis.analysis_periods[0].id, #this is specifying the current analysis period
    target_workflow_state=mbapi.TargetWorkflowState.COMPLETED,
)
analysis_source_b = server.analysis_sources.create(analysis_source)
print(f"Created analysis_source (id: {analysis_source_b.id})")
print("Note that column mappings are auto mapped based on column names. So your column names should map to the expected model for the analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC Before we can run the analysis a set of validation processes must complete. If the following code shows an error you may need to correct some validation issues. An easy way to do this is to open the analysis in MindBridge (the UI) and follow the guidance.

# COMMAND ----------

# If following line of code throws an exception saying 'Unverified Account Mappings' then
# open MindBridge and validate the accounts manually to proceed. This is a data integrity step
# that must be completed as part of 'pre-flight checks'.

try: 
    analysis = server.analyses.wait_for_analysis_sources(analysis)
except Exception as err:
    print(str(err))
    url_error = 'https://'+url+'/app/organization/'+str(organization.id)+'/engagement/'+engagement.id+"/analysis-list"
    html_code = f'<a href="{url_error}" target="_blank">An error occurred. View your Analysis In MindBridge to correct</a>'
    displayHTML(html_code) 
else: 
    print("All analysis sources are completed you're ready to run an analysis.")


# COMMAND ----------


analysis = server.analyses.run(analysis)
print("Analysis is running. Please wait. ")

# COMMAND ----------

# MAGIC %md
# MAGIC The polling operation below will run periodically until your analysis has completed. Note: This can take several hours if you have a very large dataset, so feel free to schedule this flow or come back later to check the results.

# COMMAND ----------

print(server.http.headers["user-agent"])
analysis = server.analyses.wait_for_analysis(analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC When your analysis is complete, you can review your results within MindBridge

# COMMAND ----------

#here we are obtaining the id for the most recent run of your analysis
analysis_result_id = analysis.latest_analysis_result_id
print(analysis_result_id)

# COMMAND ----------

url_analysis = 'https://'+url+'/app/organization/'+str(organization.id)+'/engagement/'+engagement.id+"/analysis/"+analysis_result_id+"/analyze/risk-overview?"
html_code = f'<a href="{url_analysis}" target="_blank">Open Risk Overview In MindBridge</a>'
displayHTML(html_code)
