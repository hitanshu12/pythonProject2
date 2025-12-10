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

data1 = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com")
]
myschema1 = ["id", "name", "age", "email"]
df1 = spark.createDataFrame(data1, schema=myschema1)
df1.show()

data2 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]
myschema2 = ["id", "name", "age", "email", "salary"]
df2 = spark.createDataFrame(data2, schema=myschema2)
# df2.show()


email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'








from pyspark.sql.functions import *

df2 = (
    df2.withColumnRenamed("email", "email2")
    .withColumnRenamed("name", "name2")
    .withColumnRenamed("age", "age2")
)

df2.show()

finaldf = (
    df1.join(df2, ["id"], "full")
    .withColumn("email", coalesce(col("email"), col("email2")))
    .withColumn("name", coalesce(col("name"), col("name2")))
    .withColumn("age", coalesce(col("age"), col("age2")))
    .drop("email2", "name2", "age2")
    .filter(col("email").isNotNull())
    .filter(instr(col("email"), "@") > 0)
    .filter(instr(col("email"), ".") > instr(col("email"), "@"))
    .filter(~col("email").startswith("@"))
    .filter(~col("email").endswith("@"))
    .filter(~col("email").contains(" "))
    .withColumn("salary", when(col("salary").isNull(), 1000).otherwise(col("salary")))
)

finaldf.show()
