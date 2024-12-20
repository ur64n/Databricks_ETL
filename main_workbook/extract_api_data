from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
import time
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder.appName("SpaceDataExtraction").getOrCreate()

# Configurations
secret_scope = "portfolio-scope"
sas_token_key = "SASTokenRawContainer"
container_url = "abfss://raw@portfoliodb1.dfs.core.windows.net/"

sas_token = dbutils.secrets.get(scope=secret_scope, key=sas_token_key)

# Parameters
start_date = datetime(2018, 1, 1)
end_date = datetime(2022, 11, 1)
interval = timedelta(days=30)  # Approximate 1-month interval

def get_the_space_devs_data(start_date, end_date, interval):
    """Fetch data from TheSpaceDevs API and save it to a DataFrame."""
    url = "https://ll.thespacedevs.com/2.3.0/launches/"
    params = {
        "format": "json",
        "limit": 100,
    }
    
    all_data = []

    current_date = start_date
    request_count = 0  # Counter for API requests
    max_requests = 15  # Limit of requests per hour
    total_records = 0  # Counter for total records fetched

    while current_date < end_date:
        next_date = current_date + interval
        params.update({
            "window_start__gte": current_date.strftime("%Y-%m-%d"),
            "window_start__lt": next_date.strftime("%Y-%m-%d")
        })

        # Handle rate limiting
        if request_count >= max_requests:
            print(f"Hold for 1 hour due to API limit after downloading {total_records} records...")
            time.sleep(3600)  # Wait for 1 hour
            request_count = 0  # Reset request counter
        
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json().get('results', [])
            record_count = len(data)
            total_records += record_count
            print(f"Downloaded {record_count} records from date: {current_date.strftime('%Y-%m')}. Total downloaded: {total_records} rekordów.")
            for launch in data:
                all_data.append({
                    "id": launch.get("id"),
                    "name": launch.get("name"),
                    "net": launch.get("net"),
                    "provider": launch.get("launch_service_provider", {}).get("name"),
                    "manufacturer": launch.get("rocket", {}).get("configuration", {}).get("manufacturer", {}).get("name") or "Unknown"
                })
            request_count += 1  # Increment API request count
        else:
            print(f"Error: {response.status_code} - {response.text}")

        current_date = next_date
        time.sleep(1)  # Short pause to avoid rapid requests

    print(f"Total records downloaded: {total_records}")
    return pd.DataFrame(all_data)

# Fetch SpaceDevs data
space_devs_data = get_the_space_devs_data(start_date, end_date, interval)

# Convert to Spark DataFrame
space_devs_df = spark.createDataFrame(space_devs_data)

# Save SpaceDevs data to Data Lake
output_path_spacedevs = f"{container_url}space_devs_data/"
space_devs_df.write.mode("overwrite").parquet(f"{output_path_spacedevs}?{sas_token}")

# Fetch YFinance data
def fetch_yfinance_data(tickers, start_date, end_date, interval):
    import yfinance as yf

    all_data = []
    for ticker in tickers:
        data = yf.download(ticker, start=start_date, end=end_date, interval=interval)
        data.reset_index(inplace=True)
        data["ticker"] = ticker
        all_data.append(data)

    return pd.concat(all_data, axis=0)

# Parameters for YFinance
yf_tickers = ["SPCE", "BA", "LMT"]

# Fetch YFinance data
yf_data = fetch_yfinance_data(yf_tickers, "2018-01-01", "2022-11-01", "1mo")

# Convert to Spark DataFrame
yf_spark_df = spark.createDataFrame(yf_data)

# Save YFinance data to Data Lake
output_path_yf = f"{container_url}yfinance_data/"
yf_spark_df.write.mode("overwrite").parquet(f"{output_path_yf}?{sas_token}")

print("Data extraction and saving completed.")
