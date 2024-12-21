Dodac info jak uruchomić ten pipeline u siebie

## Description
This project explores the correlation between test rocket launches by Virgin Galactic and changes in Virgin Galactic's stock value (SPCE). Using an ETL pipeline in Databricks, the analysis combines data on rocket launches from The Space Devs API and stock market data from YFinance. The project delivers insights through data transformations, aggregations, and visualizations, highlighting potential market trends triggered by space-related events. 🚀📊

This project demonstrates skills in building and configuring a complete data pipeline environment in Databricks, including cloud environment:

- Cluster Pool configuration for efficient resource management.
- Integration with Azure Data Lake Storage Gen 2 for scalable data storage.
- Setup of Azure Entra ID and Service Principal for secure access control.
- Use of SAS Tokens and Azure Key Vault for secure credential management.
- Implementation of Databricks Secret Scopes for sensitive data handling.
- Utilization of DBFS (Database File System) and Cluster-Scoped Credentials for seamless data processing.
- Scheduled job with notifications.

## Instruction

To run queries, you must first configure azure databricks in a premium subscription and configure Entra ID, Service Principal, Azure Data Lake Storage gen2, create containers, generate SAS Token, create Key Vault, implement Secret Scope for notebooks and configure Databricks File System. Then use the codes from the “main_workbook” folder in the notebooks.

### Data Sources:
- **The Space Devs API** for rocket launch data.
- **YFinance** for stock market data.

## Repository Structure

```
main
├── additional_databrick_pyspark_queries                # Additional queries used in project
│   ├── environment_test_configuration.dbc
│   ├── queries_for_managing_data.py
├── main_workbook                                       # All files for ETL pipeline
│   ├── extract_api_data.py
│   ├── load_clean_and_prepare_virgin_galactic_data.py
│   ├── transform_analyze_virgin_galactic_data.py
│   ├── visualize_virgin_galactic_stock_data.py
├── README.md
```

## Result plotly Visualistations

## Charts Overview

Here are the visualizations created during the analysis, highlighting the stock performance of Virgin Galactic in relation to its launch activity:

### 1. Virgin Galactic Stock Movement with Launch Days
This line chart shows the Virgin Galactic stock price trend over time, with markers highlighting the days when launches occurred. The data distinguishes between launch days (in red) and non-launch days (in blue).

![Virgin Galactic Stock Movement with Launch Days](https://github.com/ur64n/Databricks_ETL/blob/main/charts/chart.png)

---

### 2. Monthly Average Percent Change and Launch Count
This bar chart visualizes the average percentage change in Virgin Galactic's stock price on a monthly basis, alongside the number of launches per month. It helps identify how stock performance correlates with launch activity over time.

![Monthly Average Percent Change and Launch Count](https://github.com/ur64n/Databricks_ETL/blob/main/charts/chart2.png)

---

### 3. Period Comparison: Intensive vs Normal
This bar chart compares the average percent change in stock price during "intensive" periods (2018 and 2020) of high launch frequency versus "normal" periods. It reveals the relative market impact of frequent launches.

![Period Comparison: Intensive vs Normal](https://github.com/ur64n/Databricks_ETL/blob/main/charts/chart3.png)

---

### 4. Launch Frequency and Stability
This bar chart illustrates the average number of days between launches, grouped by successful and unsuccessful launches. It highlights the stability or irregularity of launch scheduling and its relation to success.

![Launch Frequency and Stability](https://github.com/ur64n/Databricks_ETL/blob/main/charts/chart4.png)
