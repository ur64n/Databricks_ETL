## Description
This project explores the correlation between test rocket launches by Virgin Galactic and changes in Virgin Galactic's stock value (SPCE). Using an ETL pipeline in Databricks, the analysis combines data on rocket launches from The Space Devs API and stock market data from YFinance. The project delivers insights through data transformations, aggregations, and visualizations, highlighting potential market trends triggered by space-related events. 🚀📊

This project demonstrates skills in building and configuring a complete data pipeline environment in Databricks, including cloud environment:

- Cluster Pool configuration for efficient resource management.
- Integration with Azure Data Lake Storage Gen 2 for scalable data storage.
- Setup of Azure Active Directory and Service Principal for secure access control.
- Use of SAS Tokens and Azure Key Vault for secure credential management.
- Implementation of Databricks Secret Scopes for sensitive data handling.
- Utilization of DBFS (Database File System) and Cluster-Scoped Credentials for seamless data processing.
- Scheduled job with notifications.

### Data Sources:
- **The Space Devs API** for rocket launch data.
- **YFinance** for stock market data.

## Repository Structure

```
main
├── additional_databrick_pyspark_queries
│   ├── environment_test_configuratio.dbc
│   ├── queries_for_managing_data.py
├── main_workbook
│   ├── extract_api_data.py
│   ├── load_clean_and_prepare_virgin_galactic_data.py
│   ├── transform_analyze_virgin_galactic_data.py
│   ├── visualize_virgin_galactic_stock_data.py
├── README.md
```

## Result plotly Visualistations

