import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Virgin Galactic Visualization").getOrCreate()

# Load Processed Data

# Load datasets
stock_with_movement = spark.read.format("delta").load("dbfs:/processed/virgin_stock_movement").toPandas()
monthly_impact = spark.read.format("delta").load("dbfs:/processed/monthly_impact_analysis").toPandas()
period_comparison = spark.read.format("delta").load("dbfs:/processed/period_comparison").toPandas()
pre_post_volume = spark.read.format("delta").load("dbfs:/processed/pre_post_volume").toPandas()
launch_frequency = spark.read.format("delta").load("dbfs:/processed/launch_frequency_analysis").toPandas()


# Visualization

# Stock Movement Timeline
fig1 = px.line(
    stock_with_movement,
    x="Date",
    y="Close",
    color="is_launch_day",
    title="Virgin Galactic Stock Movement with Launch Days",
    labels={"Close": "Stock Price", "Date": "Date", "is_launch_day": "Launch Day"}
)
fig1.update_traces(mode="lines+markers")
fig1.show()

# 2. Monthly Impact Analysis
fig2 = px.bar(
    monthly_impact,
    x="month",
    y="avg_percent_change",
    color="launch_count",
    title="Monthly Average Percent Change and Launch Count",
    labels={"month": "Month", "avg_percent_change": "Avg % Change", "launch_count": "Launch Count"}
)
fig2.show()

# 3. Period Comparison
fig3 = px.bar(
    period_comparison,
    x="period",
    y="avg_percent_change",
    color="period",
    title="Period Comparison: Intensive vs Normal",
    labels={"avg_percent_change": "Avg % Change", "period": "Period"}
)
fig3.show()

# 4. Pre/Post Launch Volume Analysis
fig4 = px.box(
    pre_post_volume,
    x="volume_period",
    y="avg_volume",
    title="Pre/Post Launch Volume Analysis",
    labels={"volume_period": "Volume Period", "avg_volume": "Average Volume"}
)
fig4.show()

# 5. Launch Frequency and Stability
fig5 = px.bar(
    launch_frequency,
    x="success",
    y="avg_days_between_launches",
    title="Launch Frequency and Stability",
    labels={"success": "Launch Success (1=Success, 0=Failure)", "avg_days_between_launches": "Avg Days Between Launches"}
)
fig5.show()
