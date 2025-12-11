
from pyspark.sql import SparkSession

import sys
import os

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

tmp_dir = r"C:\spark\tmp"
os.makedirs(tmp_dir, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = tmp_dir

JAVA_HOME = "C:\Program Files\Java\jdk-17"
HADOOP_HOME = "C:\hadoop"

spark = (
    SparkSession.builder
    .appName("CompleteSparkProject")
    .master("local[*]")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .config("spark.python.worker.mode", "process")
    # .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.0_0.20.4")   # library to read excel file
    .getOrCreate()
)


spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

sc = spark.sparkContext

print("PySpark Version:", __import__("pyspark").__version__)
print("Spark Version:", spark.version)
print("SparkContext:", sc)

# ================================Code Start Below=======================================

"""
Problem: From a transactions table, find the first purchase date for every customer.
"""
from pyspark.sql.functions import *

# read the data from csv file

custDF = (
    spark.read.format("csv")
    .option("header", "true").option("inferSchema", "true")
    .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\customer_purchase_behavior_datasets.csv")
)

custDF.show(10)

# first purchase date of each customer

# step 1:- Convert date into date column

custDF = (
    custDF
    .withColumn(
        "Date",
        to_date("Date", "MM/dd/yyyy HH:mm:ss")
    )
)
custDF.show(10)
custDF.printSchema()


# custDF.filter(to_date("Date").isNull()).show(truncate=False)


finalCustDF = (
    custDF
    .groupby("Customer_ID")
    .agg(
        min("Date").alias("first_purchase_date")
    )
    .filter("first_purchase_date is not null")
)

finalCustDF.show()

# first transaction row of each customer

from pyspark.sql.window import Window

w = Window.partitionBy("Customer_ID").orderBy("Date")

rowCustDF = (
    custDF
    .withColumn(
        "rowNum",
        row_number().over(w)
    )
    .filter("rowNum = 1")
    .drop("rowNum")
)
rowCustDF.show(10)


# Calculate 7-day Rolling Average of Sales

print("Calculate 7-day Rolling Average of spent")

spnt = Window.orderBy("Date").rowsBetween(-6, 0)

avgCustSpent = (
    custDF
    .withColumn(
        "Average_spent",
        avg("Total_Spent").over(spnt)
    )
    .select("Customer_ID", "Average_spent")
)
avgCustSpent.show()


























