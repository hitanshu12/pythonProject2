"""
Pre system setting for pyspark
"""

from pyspark.sql import SparkSession

import sys
import os

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
# os.environ["SPARK_LOCAL_DIRS"] = r"C:\spark\tmp"

tmp_dir = r"C:\spark\tmp"
os.makedirs(tmp_dir, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = tmp_dir

JAVA_HOME = "C:\Program Files\Java\jdk-17"
HADOOP_HOME = "C:\hadoop"
# PYSPARK_PYTHON=<YOUR PROJECT PATH>\.venv\Scripts\python.exe
# PYSPARK_DRIVER_PYTHON=<YOUR PROJECT PATH>\.venv\Scripts\python.exe
# SPARK_HOME=C:\spark\spark-3.5.7-bin-hadoop3

# PYTHONPATH=<PROJECT ROOT>

# 1. Stop any old session (PyCharm keeps them alive)
# try:
#     spark.stop()
# except:
#     pass

# 2. Create SparkSession with spark-xml support (Spark 3.5.x)
spark = (
    SparkSession.builder
    .appName("CompleteSparkProject")
    .master("local[*]")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .config("spark.python.worker.mode", "process")
    .getOrCreate()
)

# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

sc = spark.sparkContext

print("PySpark Version:", __import__("pyspark").__version__)
print("Spark Version:", spark.version)
print("SparkContext:", sc)

# ================================Code Start Below=======================================



"""
Scenario:

You are working for an e-commerce analytics company.
You have access to the Superstore dataset that contains orders,
customers, products, and sales data. Your goal is to process and
transform this data for business intelligence, such as sales analysis,
year-over-year growth, and customer segmentation.
"""
# import important methods

from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ETL Pipeline Tasks:

"""
Extract (Loading Data):
Load the raw Superstore CSV data into a Spark DataFrame.
"""
superStoreDF = (
    spark.read.format("csv")
    .option("header","true")
    .option("inferschema","true")
    .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\superstore_dataset.csv")
)

superStoreDF.show(5)
superStoreDF.printSchema()

"""
Transform (Data Cleaning, Aggregation, and Calculation):
Data cleaning (null handling, type casting, invalid data).
Date parsing and extraction (e.g., extracting year, month, etc.).
Calculate year-over-year (YoY) sales growth.
Filter orders by specific regions or sales thresholds.
Aggregating sales and profit by customer segment or region.
"""

print("Data cleaning (null handling, type casting, invalid data).")

# change order_id to int

superStoreDF = superStoreDF.withColumn(
    "order_id", expr("split(order_id, '-')[2]")
).withColumn("order_id", expr("cast(order_id as int)"))

superStoreDF.show(5)
superStoreDF.printSchema()

# order_date and ship date to date
superStoreDF = superStoreDF.withColumn(
    "order_date", expr("to_date(order_date, 'M/d/yyyy')")
).withColumn("ship_date", expr("to_date(ship_date, 'M/d/yyyy')"))

superStoreDF.show(5)
superStoreDF.printSchema()

# discount, profit and quantity to float/double

superStoreDF = superStoreDF.fillna({"profit": 0, "discount": 0, "sales": 0}).withColumn(
    "discount", expr("case when discount rlike '^[0-9.-]+$' then discount else 0 end")
).withColumn(
    "profit", expr("case when profit rlike '^[0-9.-]+$' then discount else 0 end")
).withColumn(
    "quantity", expr("case when quantity rlike '^[0-9.-]+$' then discount else 0 end")
).withColumn(
    "discount", expr("cast(discount as double)")
).withColumn(
    "profit", expr("cast(profit as double)")
).withColumn(
    "quantity", expr("cast(quantity as double)")
)

superStoreDF.show(5)
superStoreDF.printSchema()

# Date parsing and extraction (e.g., extracting year, month, etc.).

superStoreDF = superStoreDF.withColumn(
    "order_year", F.year(F.col("order_date"))
).withColumn(
    "order_month", F.month(F.col("order_date"))
).withColumn(
    "order_day", F.day(F.col("order_date"))
)

superStoreDF.show(5)
superStoreDF.printSchema()


# Calculate year-over-year (YoY) sales growth.

sales_per_year = (
    superStoreDF
    .groupby("order_year")
    .agg(
        F.sum("sales").alias("total_sales")
    )
)


w = Window.orderBy("order_year")

sales_per_year = (
    sales_per_year
    .withColumn(
        "sales_prev_year",
        F.lag("total_sales").over(w)
    )
)

# YOY growth

sales_per_year = (
    sales_per_year
    .withColumn(
        "yoy_growth",
        F.round((F.col("total_sales") - F.col("sales_prev_year"))/F.col("sales_prev_year") * 100, 2)
    )
)

sales_per_year.show()

# Aggregating sales and profit by customer segment or region.
# round it by 2 decimal

segment_sales = (
    superStoreDF
    .groupby("segment")
    .agg(
        F.round(F.sum("sales"), 0).alias("total_sales"),
        F.round(F.sum("profit"), 0).alias("total_profit")
    )
)

segment_sales.show()





"""
Load (Saving Data to a Data Warehouse or File System):
Save the transformed data in Parquet or CSV format for further reporting or analysis.
"""