## Description
This project explores the correlation between rocket launches by Virgin Galactic and Virgin Orbit and changes in Virgin Galactic's stock value (SPCE). Using an ETL pipeline in Databricks, the analysis combines data on rocket launches from The Space Devs API and stock market data from YFinance. The project delivers insights through data transformations, aggregations, and visualizations, highlighting potential market trends triggered by space-related events. 🚀📊

Additionally, this project demonstrates skills in building and configuring a complete data pipeline environment in Databricks, including:

- Cluster Pool configuration for efficient resource management.
- Integration with Azure Data Lake Storage Gen 2 for scalable data storage.
- Setup of Azure Active Directory and Service Principal for secure access control.
- Use of SAS Tokens and Azure Key Vault for secure credential management.
- Implementation of Databricks Secret Scopes for sensitive data handling.
- Utilization of DBFS (Database File System) and Cluster-Scoped Credentials for seamless data processing.

### Data Sources:
- **The Space Devs API** for rocket launch data.
- **YFinance** for stock market data.

## Repository Structure

- **`notebooks/`**: Contains Databricks notebooks for each step of the ETL pipeline.
- **`data/`**: Sample data files used for testing.
- **`visualizations/`**: Output charts and plots for insights.

## Result plotly Visualistations
