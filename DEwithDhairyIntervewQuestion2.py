
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
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# creating the dataframe !
data = [
    (1, "Mumbai"),
    (2, "Bangalore"),
    (3, "Delhi")
]


df = spark.createDataFrame(data, schema=schema)
df.show()


schema2 = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])

data1 = [
    (1, "Mumbai"),
    (2, "Bangalore"),
    (4, "Prayagraj")
]

df2 = spark.createDataFrame(data=data1, schema=schema2)
df2.show()

"""
    how to find df1 data which is not present in df2.
"""


# method 1 : Group by expression

print("============Solution 1: using left anti=================")

lefAnti = (
    df.join(df2, ["id"], "left_anti")
)

lefAnti.show()


print("============Solution 2: Using Subtract=================")

subtarctDF = (
    df.subtract(df2)
)
subtarctDF.show()

print("============Solution 3: ExceptAll=================")

exceptAllDF = (
    df.exceptAll(df2)
)
exceptAllDF.show()

print("============Solution 4: left Join & Filter=================")

from pyspark.sql.functions import *

leftJoin = (
    df.join(df2, ["id"], "left")
    .filter(df2.name.isNull())
    .drop(df2.name)
)
leftJoin.show()






data2 = [
    (1, "I love to play cricket"),
    (2, "I am into moterbiking"),
    (4, "what do you like")
]

schema3 = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("message", StringType(), nullable=True)
])

df4 = spark.createDataFrame(data=data2, schema=schema3)
df4.show(truncate=False)

vowels = "aeiou"

contantsDF = (
    df4
    .withColumn(
        "consonants",
        lower(col("message"))
    )
)
contantsDF.show(truncate=False)

contantsDF = (
    contantsDF
    .withColumn(
        "without_vowels_and_space",
        regexp_replace(col("message"), "a|e|i|o|u|\s", "-")
    )
    .withColumn(
        "without_vowels_and_space",
        regexp_replace(col("without_vowels_and_space"), "-", "")
    )
    .drop("message", "consonants")
    .withColumnRenamed("without_vowels_and_space", "consonants")
    .withColumn("count", length(col("consonants")))
)
contantsDF.show(truncate=False)
























