from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("AI Ghibli Dataset Analysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Step 2: Load Dataset
file_path = "/content/ai_ghibli_trend_dataset_v2.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Step 3: Data Analysis

# Total number of rows
row_count = data.count()
print(f"Total Rows: {row_count}")

# Summary statistics for numeric columns
print("Summary Statistics for Numerical Columns:")
data.select(
    avg(col("likes")).alias("Avg Likes"),
    avg(col("shares")).alias("Avg Shares"),
    avg(col("comments")).alias("Avg Comments"),
    avg(col("gpu_usage")).alias("Avg GPU Usage"),
    avg(col("file_size_kb")).alias("Avg File Size (KB)")
).show()

# Example: Maximum and minimum likes
data.select(
    max(col("likes")).alias("Max Likes"),
    min(col("likes")).alias("Min Likes")
).show()

# Group by platform and calculate total likes
print("Total Likes by Platform:")
data.groupBy("platform").sum("likes").alias("Total Likes").show()

# Count the number of hand-edited images
print("Count of Hand-Edited Images:")
data.groupBy("is_hand_edited").count().show()

# Step 4: Stop Spark Session
spark.stop()
