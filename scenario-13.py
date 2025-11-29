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
    .config("spark.hadoop.io.nativeio.disable", "true")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()
)

#spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

sc = spark.sparkContext

print("PySpark Version:", __import__("pyspark").__version__)
print("Spark Version:", spark.version)
print("SparkContext:", sc)

#================================Code Start Below=======================================

from pyspark.sql.window import Window
from pyspark.sql.functions import *

employeeDF = (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\employees.csv"))
employeeDF.show()

deptDF = (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\departments.csv"))
deptDF.show()

# we have employee id, employee name, department. Need count of every department employees

df = (
    employeeDF.alias("e")
    .join(
        deptDF.alias("d"), employeeDF.dept_id == deptDF.dept_id, "left"
    )
    .select(
        "e.emp_id",
        "d.dept_name"
    )
    .groupby("dept_name")
    .agg(
        count("emp_id").alias("total_emp")
    )
)
df.show(truncate=False)




















