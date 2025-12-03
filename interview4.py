

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
The roundtrip distance should be calculated using spark or SQL.

self join example

+----+---+----+
|from| to|dist|
+----+---+----+
| SEA| SF| 300|
| CHI|SEA|2000|
|  SF|SEA| 300|
| SEA|CHI|2000|
| SEA|LND| 500|
| LND|SEA| 500|
| LND|CHI|1000|
| CHI|NDL| 180|
+----+---+----+

"""


from pyspark.sql.functions import *


data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)
]
df = spark.createDataFrame(data, ["from", "to", "dist"])
df.show()

# spark DSL
print("Self Join")
dslDF = (
    df.alias("df1")
    .join(
        df.alias("df2"),
        (col("df1.from") == col("df2.to")) & (col("df1.to") == col("df2.from")),
        "inner"
    )
    .select(
        col("df1.from"),
        col("df1.to"),
        (col("df1.dist") + col("df2.dist")).alias("dist")
    )
)

dslDF.show()


# through sql

df.createOrReplaceTempView("dfSql")

sqlDF = spark.sql(
    """
        select 
            d1.from, 
            d1.to, 
            (d1.dist + d2.dist) as dist 
        from dfSql d1 join dfSql d2 
        on d1.from = d2.to and d1.to = d2.from
    """
)

sqlDF.show()











