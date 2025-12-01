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


"""
Join two dataset
"""


data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]

df = spark.createDataFrame(data=data, schema=["child", "parent"])

df.show()

df1 = df
df2 = df

# renamed the schema name from df2
df2 = df2.withColumnRenamed("child", "child1").withColumnRenamed("parent","parent1")


df1.show()
df2.show()


# inner join

joinDF = df1.join(df2, df1.child == df2.parent1, "inner")
joinDF.show()


# final df

finalDF = (
    joinDF
    .drop("parent1")
    .withColumnRenamed("parent", "grandParent")
    .withColumnRenamed("child", "parent")
    .withColumnRenamed("child1", "child")
    # reorder the column
    .select("child", "parent", "grandParent")
)

finalDF.show()


# =============================================Self Join=====================================================

from pyspark.sql.functions import *

print("Self Join Start")
selfjoinDF = (
    df.alias("df3")
    .join(
        df.alias("df4"),
        col("df3.child") == col("df4.parent"),
        "inner"
    )
    .drop(col("df4.parent"))
)

selfjoinDF.show()

#==============================================aggregation======================================

data2 = [("sai", 40), ("zeyo", 30), ("sai", 50), ("zeyo", 40)]

newdf = spark.createDataFrame(data=data2, schema=["name", "amount"])
newdf.show()

print("aggregation of sai and zeyo")

aggdf = (
    newdf
    .groupby("name")
    .agg(
        sum("amount").alias("total_amount"),
        count("name").alias("cnt")
    )
)

aggdf.show()














