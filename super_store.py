
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
#PYSPARK_PYTHON=<YOUR PROJECT PATH>\.venv\Scripts\python.exe
#PYSPARK_DRIVER_PYTHON=<YOUR PROJECT PATH>\.venv\Scripts\python.exe
#SPARK_HOME=C:\spark\spark-3.5.7-bin-hadoop3

#PYTHONPATH=<PROJECT ROOT>

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

#spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

sc = spark.sparkContext

print("PySpark Version:", __import__("pyspark").__version__)
print("Spark Version:", spark.version)
print("SparkContext:", sc)

#================================Code Start Below=======================================

csvDF = (
    spark.read.format('csv')
    .option('header', 'true')
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\superstore_dataset.csv")
)
csvDF.show(5)

# Count total records

recDF = csvDF.count()
print(recDF)

# Column Operations
"""
Select only Customer Name, Sales, Region
Rename column Sales → total_sales
Add a new column tax = Sales * 0.18
"""

from pyspark.sql.functions import *

newDF = (
    csvDF.select(
        "customer",
        "sales",
        "region"
    )
)

newDF = (
    newDF.withColumnRenamed("sales", "total_sales")
)


newDF = (
    newDF.withColumn(
        "tax", round((col('total_sales') * 0.18),2)
    )
)
newDF.show(5)

"""
Filtering
Get all rows where Sales > 500
Find all orders where Profit < 0
Filter orders from “West” region
"""

filDF = (
    csvDF
    .filter(
        (col('sales') > 500) &
        (col('profit') > 0) &
        (col('region') == 'West')
    )
)

filDF.show(5)

"""
Aggregations
Total sales per region
Average profit per category
Count number of orders per segment
"""

aggDF = (
    csvDF.groupby(col('region'), col('segment'))
    .agg(
        round(sum('sales'),0).alias('total_sales'),
        round(avg('profit'),2).alias('avg_profit'),
        count('order_id').alias('order_count')
    )
    .filter(col('region').isin("East", "South", "West", "North"))
)
aggDF.show()


"""
Sorting
Top 10 highest sales orders
Lowest 5 profit orders
"""
from pyspark.sql.window import Window

sortDF = (
    csvDF
    .groupby(col("order_id"))
    .agg(
        round(sum("sales"), 0).alias("sales")
    )
)

w = Window.orderBy(col('sales').desc())
lw = Window.orderBy(col('sales').asc())

sortDF = (
    sortDF
    .withColumn("rnk", dense_rank().over(w))
    .filter(col("rnk") <= 10)
    .drop(col("rnk"))
)
sortDF.show()

lowest5DF = (
    sortDF
    .withColumn("rnk", row_number().over(lw))
    .filter(col("rnk") <= 5)
    .drop(col("rnk"))
)
lowest5DF.show()

"""
Grouping Logic
Which category generates the highest sales?
Which sub-category has the lowest profit?
"""

catDF = (
    csvDF
    .groupby(col('category'))
    .agg(
        round(sum(col('sales')), 0).alias('total_sales')
    )
)
catDF.show()

cw = Window.orderBy(col('total_sales').desc())

catDF = (
    catDF
    .withColumn('rnk', row_number().over(cw))
    .filter(col('rnk') == 1)
    .drop(col('rnk'))
)
catDF.show()

scatDF = (
    csvDF
    .groupby(col('subcategory'))
    .agg(
        round(sum(col('profit')), 0).alias('profit')
    )
    .filter(col('profit').isNotNull())
)
sw = Window.orderBy(col('profit').asc())

scatDF = (
    scatDF
    .withColumn('rnk', row_number().over(sw))
    .filter(col('rnk') == 1)
    .drop(col('rnk'))
)
scatDF.show()

# Rank orders by sales within each region

rw = Window.partitionBy(col('region')).orderBy(col('sales').desc())
csvDF.show(5)
rnkDF = (
    csvDF.withColumn(
        "region_rnk", rank().over(rw)
    )
)
rnkDF.show(truncate=False)

# Calculate cumulative sales per region

smw = Window.partitionBy(col('region')).orderBy(col('order_date').desc())

runTotDF = (
    csvDF.withColumn(
        "running_total", sum(col('sales')).over(rw)
    )
)
runTotDF.show(truncate=False)

# Get top 3 customers per region by sales

top3DF = (
    csvDF
    .withColumn(
        "region_rnk", rank().over(rw)
    )
    .withColumn(
        "running_total", sum(col('sales')).over(rw)
    )
    .filter(col('region_rnk') <= 3)
)
top3DF.show(truncate=False)

