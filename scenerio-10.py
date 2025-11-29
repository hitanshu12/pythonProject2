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
    .config("spark.hadoop.io.nativeio.disable", "true")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()
)

# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

sc = spark.sparkContext

print("PySpark Version:", __import__("pyspark").__version__)
print("Spark Version:", spark.version)
print("SparkContext:", sc)

# ================================Code Start Below=======================================

from pyspark.sql.window import Window
from pyspark.sql.functions import *

print("Scenario 10")

data = [
    (1, 300, "31-Jan-2022"),
    (1, 400, "31-Apr-2022"),
    (1, 500, "31-Feb-2022"),
    (1, 600, "31-Mar-2022"),
    (2, 1000, "31-Oct-2022"),
    (2, 900, "31-Dec-2022")
]

schema = ["emp_id", "commissionAmt", "lastMonthDate"]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

print("create a df for employees on there max date")

maxDateDF = (
    df
    .groupby(col("emp_id").alias("emp_id1"))
    .agg(
        max("lastMonthDate").alias("maxDate")
    )
)
maxDateDF.show(truncate=True)

print("Perform join between maxDateDF and df")

joinDF = (
    df
    .join(
        maxDateDF,
        (df.emp_id == maxDateDF.emp_id1) & (df.lastMonthDate == maxDateDF.maxDate),
        "inner"
    )
    .drop("emp_id1", "maxDate")
)

joinDF.show()
















