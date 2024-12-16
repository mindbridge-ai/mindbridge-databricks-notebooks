# Databricks notebook source
# MAGIC %md
# MAGIC ![](https://www.mindbridge.ai/wp-content/uploads/2021/07/MindBridge_Logo_Primary_RGB.png)
# MAGIC # Databricks -> MindBridge Example : Creating an organization, engagement, and E2E analysis execution

# COMMAND ----------

# MAGIC %md
# MAGIC MindBridge API Installation in Databricks is as simple as...

# COMMAND ----------

# Use pip to install the MindBridge API
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
# MAGIC Now we are going to create a Organization inside the tenant. We can open the Organization that we just created by following the link below. 

# COMMAND ----------

organization = mbapi.OrganizationItem(name="Test 1")
organization = server.organizations.create(organization)

# Generate an URL to access your org in a browser. 
urlOrg = 'https://' + url + '/app/organization/' + str(organization.id)
html_code = f'<a href="{urlOrg}" target="_blank">Open Organization In MindBridge</a>'
displayHTML(html_code)

# COMMAND ----------

# MAGIC %md
# MAGIC Here we create an engagement

# COMMAND ----------

# Now generate an Engagement 

engagement = mbapi.EngagementItem(
    name="Databricks Demo 3",
    organization_id=organization.id,
    engagement_lead_id=organization.created_user_info.id,
)
engagement = server.engagements.create(engagement)
print(f"Created engagement {engagement.name!r} (id: {engagement.id})")
urlEngagement = urlOrg + '/engagement/' + engagement.id
html_code = f'<a href="{urlEngagement}" target="_blank">Open Engagement In MindBridge</a>'
displayHTML(html_code)


# COMMAND ----------

# MAGIC %md
# MAGIC We setup our analysis specifying the financial period and base currency 

# COMMAND ----------

from datetime import date

# Create an Analysis! analysis_periods the default is the current calendar year
analysis_i = mbapi.AnalysisItem(
    engagement_id=engagement.id,
    currency_code="USD",
    analysis_periods=[
        mbapi.AnalysisPeriod(end_date=date(2021, 12, 31), start_date=date(2021, 1, 1))
    ],
)
analysis = server.analyses.create(analysis_i)
print(f"Created analysis {analysis.name!r} (id: {analysis.id})")



# COMMAND ----------

# MAGIC %md
# MAGIC Now load a demo dataset form Databricks into the Analysis. This can be done in many different ways including streaming JSON lines etc. 

# COMMAND ----------

# Setup the data files that you want to use in your Analysis by pointing to your current file path
gl_path = '[insert the path to your source dataset here]'
file_manager_file = mbapi.FileManagerItem(engagement_id=analysis.engagement_id)
file_manager_file = server.file_manager.upload(
    input_item=file_manager_file,
    input_file=gl_path,
)


# COMMAND ----------

# MAGIC %md
# MAGIC Next we create an analysis using the file and we assume the input file is in good shape.

# COMMAND ----------

analysis_source = mbapi.AnalysisSourceItem(
    engagement_id=engagement.id,
    analysis_id=analysis.id,
    file_manager_file_id=file_manager_file.id,
    analysis_period_id=analysis.analysis_periods[0].id,
    target_workflow_state=mbapi.TargetWorkflowState.COMPLETED,
)
analysis_source_b = server.analysis_sources.create(analysis_source)
print(f"Created analysis_source (id: {analysis_source_b.id})")
print("Note that column mappings are auto mapped based on column names. So your column names should map to the expected model for the analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC Before we can run the analysis a set of validation processes must complete. If the following code shows an error you may need to correct some validation issues. An easy way to do this is to open the analysis in MindBridge and followed the guidance. 

# COMMAND ----------

# If following line of code throws an exception saying 'Unverified Account Mappings' then
# open MindBridge and validate the accounts manually to proceed. This is a data integrity step
# that must be completed as part of 'pre-flight checks'.

try: 
    analysis = server.analyses.wait_for_analysis_sources(analysis)
except Exception as err:
    print(str(err))
    url_error = urlEngagement+"/analysis-list"
    html_code = f'<a href="{url_error}" target="_blank">An error occurred. View your Analysis In MindBridge to correct</a>'
    displayHTML(html_code) 
else: 
    print("All analysis sources are completed")


# COMMAND ----------

# MAGIC %md
# MAGIC Next, we are going to execute our MindBridge Analysis

# COMMAND ----------


analysis = server.analyses.run(analysis)
print("Analysis is running. Please wait. ")

# COMMAND ----------

# MAGIC %md
# MAGIC The polling operation below will run periodically until your analysis has completed. Note: This can take several hours if you have a very large dataset, so feel free to schedule this flow or come back later to check the results.

# COMMAND ----------

try:
    analysis = server.analyses.wait_for_analysis(analysis)
    print("Your MindBridge analysis is complete.")
except Exception as e:
    print("Your MindBridge analysis has not finished running yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC After our analysis has completed, we'll get the results of the most recent analysis run

# COMMAND ----------

#here we are obtaining the id for the most recent run of your analysis
analysis_result_id = analysis.latest_analysis_result_id
print(analysis_result_id)

# COMMAND ----------

# MAGIC %md
# MAGIC We can now look at the results of our analysis run within MindBridge via the generated link below.

# COMMAND ----------

url_analysis = urlEngagement+"/analysis/"+analysis_result_id+"/analyze/risk-overview?"
html_code = f'<a href="{url_analysis}" target="_blank">Open Risk Overview In MindBridge</a>'
displayHTML(html_code)
