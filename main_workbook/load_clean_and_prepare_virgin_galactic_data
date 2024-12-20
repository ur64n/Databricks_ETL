from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date

# Initialize Spark session
spark = SparkSession.builder.appName("Virgin Galactic Analysis").getOrCreate()

# -----------------------
# Load & Clean Space_Devs Data
# -----------------------

# File paths for Space_Devs data
space_devs_files = [
    "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_1",
    "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_2",
    "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_3",
    "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_4"
]

# Load Space_Devs data
space_devs_df = spark.read.parquet(*space_devs_files)

# Data cleaning and transformation
space_devs_cleaned = (
    space_devs_df
    .drop("manufacturer")
    .withColumn("net", expr("split(net, 'T')[0]"))  # Keep only the date part of the 'net' column
    .filter(col("provider") == "Virgin Galactic")
    .orderBy(col("net"))
)

# Save cleaned Space_Devs data to Delta format
space_devs_cleaned.write.format("delta").mode("overwrite").save("dbfs:/processed/virgin_launch_data")

# -----------------------
# Load & Clean YFinance Data
# -----------------------

# File paths for YFinance data
yfinance_files = [
    "abfss://raw@portfoliodb1.dfs.core.windows.net/yfinance_data/yf_data/yf_data_1",
    "abfss://raw@portfoliodb1.dfs.core.windows.net/yfinance_data/yf_data/yf_data_2",
    "abfss://raw@portfoliodb1.dfs.core.windows.net/yfinance_data/yf_data/yf_data_3",
    "abfss://raw@portfoliodb1.dfs.core.windows.net/yfinance_data/yf_data/yf_data_4"
]

# Load YFinance data
yfinance_df = spark.read.parquet(*yfinance_files)

# Data cleaning and transformation
yfinance_cleaned = (
    yfinance_df
    .drop("High", "Low", "Adj Close")
    .filter(col("ticker") == "SPCE")
    .withColumn("ticker", expr("'Virgin Galactic'"))
    .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))  # Convert to date format
    .orderBy(col("Date"))
)

# Save cleaned YFinance data to Delta format
yfinance_cleaned.write.format("delta").mode("overwrite").save("dbfs:/processed/virgin_stock_data")
