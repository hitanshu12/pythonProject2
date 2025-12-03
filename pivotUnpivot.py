
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
    .getOrCreate()
)

# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

sc = spark.sparkContext

print("PySpark Version:", __import__("pyspark").__version__)
print("Spark Version:", spark.version)
print("SparkContext:", sc)

# ================================Code Start Below=======================================

print("Pivot Example")

from pyspark.sql.functions import *

data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", "Indore", "cash"),
    (6, "02-14-2011", 200.0, "Winter", "IceHockey", "cash"),
    (7, "02-14-2011", 200.0, "Winter", "IceHockey", "cash")
]

df1 = spark.createDataFrame(data2, ["id", "date", "amount", "category", "product", "spend"])
df1.show()


# pivot amount, date, product, spend based on category

from pyspark.sql.functions import expr

pivotDF = (
    df1
    .groupby("category")
    .pivot("product")
    .sum("amount")
)

# pivotDF = pivotDF.select(
#     "category",
#     col("Field").cast("double"),
#     col("Ice Hockey").cast("double"),
#     col("Indore").cast("double")
# )
pivotDF.show()

print("Unpivot Example")

unpivotExpr = "stack(3, 'Field', Field, 'IceHockey', IceHockey, 'Indore', Indore) as (product, amount)"

unpivotDF = pivotDF.select("category", expr(unpivotExpr))
unpivotDF.show()




