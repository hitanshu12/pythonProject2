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

"""
Scenario:
You’re working as a Data Engineer for a power distribution company in India. 
A power company bills customers daily.Due to system glitches, some billing dates 
are missing in billing_logs.Management wants to detect all missing date gaps per 
customer between their first and last billing date.

-- management wants to identify for each customer
1) Total amount spent
2) First order date - Done
3) Latest Order date - Done
4) Average gap in days between consecutive orders



"""

from pyspark.sql.functions import *

data = [
 ("C001", "2024-01-01", 500),
 ("C001", "2024-01-10", 1000),
 ("C001", "2024-01-04", 700),
 ("C002", "2024-01-06", 300),
 ("C002", "2024-01-15", 1200),
 ("C002", "2024-01-20", 500),
 ("C003", "2024-01-05", 900)
]

df = spark.createDataFrame(data=data, schema= ["cuatomer_id", "order_date", "amount"])
# df.show()
# df.printSchema()


# convert order_date in date format

df = df.withColumn(
    "order_date", to_date("order_date", "yyyy-MM-dd")
)
df.printSchema()

# create a First order date column

from pyspark.sql.window import Window

w = Window.partitionBy("cuatomer_id")

df = df.withColumn(
    "first_order_date", min("order_date").over(w)
)
# df.show()


# create a latest order date

df = df.withColumn(
    "latest_order_date", max("order_date").over(w)
)

# df.show()

# gap in days between consecutive orders

df = df.withColumn(
    "gap", date_diff("latest_order_date", "first_order_date")
)

df.show()


# Total amount spent for each customer

result_df = (
    df
    .groupby("cuatomer_id")
    .agg(
        sum("amount").alias("total_amount"),
        min("first_order_date").alias("first_order"),
        max("latest_order_date").alias("last_order"),
        avg("gap").alias("average_gap")
    )
)

result_df.show()

# write the data in parquet format

df.write.mode("overwrite").parquet("/Volumes/devlopment/data/files/orders/linkedInScenario1.parquet")

































