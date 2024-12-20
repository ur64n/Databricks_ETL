### dispaly TSD Data ###

from pyspark.sql import functions as F

file_path = "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_1"
file_path2 = "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_2"
file_path3 = "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_3"
file_path4 = "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_4"
df = spark.read.parquet(file_path, file_path2, file_path3, file_path4)
df.display()

### Display yf_data ###

file_path = "abfss://raw@portfoliodb1.dfs.core.windows.net/yfinance_data/yf_data/yf_data_1"
file_path2 = "abfss://raw@portfoliodb1.dfs.core.windows.net/yfinance_data/yf_data/yf_data_2"
file_path3 = "abfss://raw@portfoliodb1.dfs.core.windows.net/yfinance_data/yf_data/yf_data_3"
file_path4 = "abfss://raw@portfoliodb1.dfs.core.windows.net/yfinance_data/yf_data/yf_data_4"
df = spark.read.parquet(file_path, file_path2, file_path3, file_path4)
df.display()

### Merge filter and count data ###

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# File paths
file_path1 = "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_1"
file_path2 = "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_2"
file_path3 = "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_3"
file_path4 = "abfss://raw@portfoliodb1.dfs.core.windows.net/space_devs_data/tsd_data/tsd_data_4"

# Load file to DF
df1 = spark.read.format("parquet").load(file_path1)
df2 = spark.read.format("parquet").load(file_path2)
df3 = spark.read.format("parquet").load(file_path3)
df4 = spark.read.format("parquet").load(file_path4)

# Merge DF
merged_df = df1.union(df2).union(df3).union(df4)

# Filter&Count
result = merged_df.filter(col("provider") == "Virgin Galactic").count()

print(f"Count 'Virgin Galactic' in column 'provider': {result}")

### Display cleaned data ###

df = spark.read.format("delta").load("dbfs:/processed/virgin_stock_data")
df2 = spark.read.format("delta").load("dbfs:/processed/virgin_launch_data")
display(df)
display(df2)

### Display transformed data ###

vsm = spark.read.format("delta").load("dbfs:/processed/virgin_stock_movement")
mia = spark.read.format("delta").load("dbfs:/processed/monthly_impact_analysis")
pc = spark.read.format("delta").load("dbfs:/processed/period_comparison")
ppv = spark.read.format("delta").load("dbfs:/processed/pre_post_volume")
lfa = spark.read.format("delta").load("dbfs:/processed/launch_frequency_analysis")

display (vsm)
display (mia)
display (pc)
display (ppv)
display (lfa)

### Remove from DBFS command ###

dbutils.fs.rm("dbfs:/processed/virgin_stock_data", recurse=True
