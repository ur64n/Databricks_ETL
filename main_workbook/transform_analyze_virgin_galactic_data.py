%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date, lit, lag, avg, count, abs
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Virgin Galactic Transformation").getOrCreate()

# Load Processed Data

# Load Virgin Galactic stock data
stock_data = spark.read.format("delta").load("dbfs:/processed/virgin_stock_data")

# Load Virgin Galactic launch data
launch_data = spark.read.format("delta").load("dbfs:/processed/virgin_launch_data")

# Data Transformation

#  Prepare stock data
stock_data = (
    stock_data
    .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))  # Ensure Date column is in date format
    .orderBy(col("Date"))
)

# Prepare launch data
launch_data = (
    launch_data
    .withColumnRenamed("net", "LaunchDate")
    .withColumn("LaunchDate", to_date(col("LaunchDate"), "yyyy-MM-dd"))  # Ensure LaunchDate is in date format
    .withColumn("success", expr("CASE WHEN LaunchDate = '2020-12-12' THEN 0 ELSE 1 END"))  # Mark unsuccessful launch
    .orderBy(col("LaunchDate"))
)

# Add launch flag to stock data
stock_with_launch_flag = (
    stock_data
    .join(launch_data.select(col("LaunchDate"), col("name").alias("RocketName"), col("success")), 
          stock_data["Date"] == launch_data["LaunchDate"], "left")
    .withColumn("is_launch_day", expr("CASE WHEN RocketName IS NOT NULL THEN 1 ELSE 0 END"))
)

# Calculate stock movement
window_spec = Window.orderBy(col("Date"))

stock_with_movement = (
    stock_with_launch_flag
    .withColumn("prev_close", lag("Close", 1).over(window_spec))
    .withColumn("price_change", col("Close") - col("prev_close"))
    .withColumn("percent_change", expr("(Close - prev_close) / prev_close * 100"))
    .withColumn("price_volatility", abs(col("Close") - col("Open")))
)

# Monthly impact analysis
monthly_impact = (
    stock_with_movement
    .groupBy(expr("DATE_FORMAT(Date, 'yyyy-MM')").alias("month"))
    .agg(
        avg("percent_change").alias("avg_percent_change"),
        avg("Volume").alias("avg_volume"),
        count("is_launch_day").alias("launch_count")
    )
    .orderBy("month")
)

# Period comparison (2018 and 2020)
period_comparison = (
    stock_with_movement
    .withColumn("period", expr("CASE WHEN year(Date) IN (2018, 2020) THEN 'intensive' ELSE 'normal' END"))
    .groupBy("period")
    .agg(
        avg("percent_change").alias("avg_percent_change"),
        avg("price_volatility").alias("avg_volatility")
    )
)

# Pre/post launch volume analysis
pre_post_volume = (
    stock_with_movement
    .withColumn("volume_period", expr(
        "CASE WHEN is_launch_day = 1 THEN 'on_launch' "
        "WHEN lag(is_launch_day, 1) OVER (PARTITION BY RocketName ORDER BY Date) = 1 THEN 'post_launch' "
        "WHEN lead(is_launch_day, 1) OVER (PARTITION BY RocketName ORDER BY Date) = 1 THEN 'pre_launch' ELSE 'normal' END"
    ))
    .groupBy("volume_period")
    .agg(avg("Volume").alias("avg_volume"))
)

# Launch frequency and stability
launch_frequency = (
    launch_data
    .withColumn("next_launch_gap", lag("LaunchDate", -1).over(Window.orderBy("LaunchDate")))
    .withColumn("days_between_launches", expr("DATEDIFF(next_launch_gap, LaunchDate)"))
    .groupBy("success")
    .agg(avg("days_between_launches").alias("avg_days_between_launches"))
)

# Save stock with movement data
stock_with_movement.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/processed/virgin_stock_movement")

# Save monthly impact analysis
monthly_impact.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/processed/monthly_impact_analysis")

# Save period comparison
period_comparison.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/processed/period_comparison")

# Save pre/post volume analysis
pre_post_volume.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/processed/pre_post_volume")

# Save launch frequency analysis
launch_frequency.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/processed/launch_frequency_analysis")
