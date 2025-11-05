# MindBridge Databricks Integration Examples

This repository provides examples of integrating MindBridge with Databricks for streamlined data workflows and enhanced analysis capabilities. These notebooks guide users through various stages of working with data in Databricks and performing analyses using the MindBridge platform.

## Notebooks

### 1. MindBridge_Databricks_E2E_Integration_Tutorial

- **Description**: This comprehensive tutorial walks through the integration of MindBridge with Databricks, covering steps like setting up an Organization and Engagement, performing a General Ledger analysis, and extracting key information from analysis results.
- **Use Case**: Ideal for users new to integrating MindBridge with Databricks, offering a structured setup and analysis workflow for leveraging MindBridge’s analytical capabilities directly from Databricks.

### 2. MindBridge_Databricks_Explore_GL_Table

- **Description**: Provides an example of querying from a Databricks database containing General Ledger data. It generates a file format compatible with MindBridge, which can then be uploaded to MindBridge for further analysis.
- **Use Case**: Ideal for users who want to explore and transform their data in Databricks before leveraging MindBridge's analysis capabilities.

### 3. MindBridge_Databricks_E2E_Analysis_Data_Flow

- **Description**: Demonstrates the complete initial setup of a MindBridge organization, engagement, and analysis. It includes steps for uploading data from Databricks to MindBridge, running an analysis, and generating a link to access results in MindBridge.
- **Use Case**: Suitable for first-time users or new engagements requiring a complete data flow setup from Databricks to MindBridge.

### 4. MindBridge_Databricks_Update_Data_Run_Existing_Analysis

- **Description**: Illustrates the process of updating data (e.g., appending new transactional records) from Databricks and running an existing analysis in MindBridge. This process avoids modifying the initial configuration, supporting use cases where periodic data refreshes are required.
- **Use Case**: Useful for teams that need regular updates to their analysis without reconfiguring the entire setup, enabling refreshed insights based on the latest available data.

### 5. MindBridge_Databricks_Visualization_Example

- **Description**: Showcases how to pull MindBridge results back into Databricks, enabling users to perform additional analyses and visualizations using Databricks' capabilities. This allows for more in-depth exploration of MindBridge insights and the creation of tailored visualizations.
- **Use Case**: Perfect for users looking to use Databricks for visualization and to perform advanced analysis on MindBridge results.

## Getting Started

To get started with these notebooks:

1. Clone this repository to your Databricks environment.
2. Ensure you have access to both MindBridge and Databricks, with appropriate permissions for database access and API usage.
3. Follow the instructions within each notebook to set up and run the analyses as demonstrated.

## Prerequisites

- **MindBridge API Access**: Ensure you have API credentials for MindBridge to enable seamless integration with Databricks.
- **Databricks Environment**: Access to a Databricks workspace where you can run and modify these notebooks as per your data and analysis needs.

## Contributing

Contributions to expand or improve these notebooks are welcome. Please open a pull request or raise an issue to discuss any proposed changes or improvements.

---

These examples aim to streamline the process of integrating Databricks and MindBridge, empowering users to leverage both platforms for efficient data analysis and visualization workflows. Enjoy exploring the notebooks, and feel free to reach out with any questions or feedback!
