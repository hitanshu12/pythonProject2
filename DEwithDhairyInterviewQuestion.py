
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




from pyspark.sql.types import StructType,StructField,StringType,IntegerType

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("emp_gender", StringType(), True),
    StructField("emp_age", IntegerType(), True),
    StructField("emp_salary", IntegerType(), True),
    StructField("emp_manager", StringType(), True)
])

# creating the dataframe !
data = [
    (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
    (2, "Aarav Sharma", "Male", 28, 55000, "Zara Singh"),
    (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel"),
    (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
    (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
    (6, "Naina Verma", "Female", 31, 72000, "Arjun Patel"),
    (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
    (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
    (5, "Aditya Kapoor", "Male", 28, 58000, "Zara Singh"),
    (10, "Anaya Joshi", "Female", 27, 59000, "Aarav Sharma"),
    (11, "Rohan Malhotra", "Male", 36, 73000, "Zara Singh"),
    (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel")
]


df = spark.createDataFrame(data, schema=schema)
df.show()


"""
    how to find duplicates in dataframe
"""


# method 1 : Group by expression

print("============Solution 1=================")

rmDupGrp = (
    df
    .groupby(df.columns).count()
    .filter("count > 1")
    .drop("count")
)

rmDupGrp.show()



# method 2 : Window Function

print("============Solution 2=================")
from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.partitionBy(df.columns)    # df.columns is used to get all the columns

winDupDF = (
    df
    .withColumn("count", count("emp_id").over(w))  # creating a count column
    .where(col("count") > 1)   # Filtering the dataframe
    .dropDuplicates()          # drop the duplicate rows from the dataframe
    .drop("count")             # drop the count column
)


winDupDF.show()


















