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
    JOIN and remove ids which has invalid 
    email id - if not salary make it constant 1000 after join
"""

from pyspark.sql.functions import *

data1 = [(1,), (2,), (3,)]
print(data1)
columns = ["col"]


df1 = spark.createDataFrame(data1, columns)
df1.show()

data2 = [(1,), (2,), (3,), (4,), (5,)]
df2 = spark.createDataFrame(data2, columns)
df2.show()


# Get the max value of DF1

maxdf = (
    df1
    .selectExpr("max(col) as col")
)
maxdf.show()


# Perform Left anti Join

finalDF = (
    df2.join(maxdf, ["col"], "leftanti")
)
finalDF.show()


# Alternate Method
print("===========================Alternate Method=======================")
from pyspark.sql.window import Window

maxDF2 = (
    df1
    .withColumn("max_col", max("col").over(Window.partitionBy()))
    .filter(col("col") == col("max_col"))
    .drop("max_col")
)

maxDF2.show()

finalDF2 = (
    df2.join(maxDF2, ["col"], "leftanti")
)
finalDF2.show()





















