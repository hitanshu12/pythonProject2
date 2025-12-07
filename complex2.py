
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
Types of Data complexity
"""

# First Type:- Multiline Issue

data = """
    {
        "id" : 2,
        "trainer": "Sai"
    }
"""




rdd = sc.parallelize([data])

multilineDF = spark.read.option("multiline", "true").json(rdd)

multilineDF.show()
multilineDF.printSchema()

# Second Type:- Struct
data1 = """
{
    "id": 2,
    "trainer": "Sai",
    "zeyoAddress": {
        "permanentAdd": "hyderabad",
        "temporaryAdd": "chennai"
    }
}
"""

rdd1 = sc.parallelize([data1])

structDF = spark.read.option("multiline", "true").json(rdd1)

structDF.show()
structDF.printSchema()


# Only way to deal with struct Datatype
falttenDF = (
    structDF.select(
        "id",
        "trainer",
        "zeyoAddress.permanentAdd",
        "zeyoAddress.temporaryAdd"
    )
)
falttenDF.show()
falttenDF.printSchema()



# Nested Struct


data2 = """
{
    "id": 2,
    "trainer": "Sai",
    "zeyoAddress": {
        "user" : {
            "permanentAdd": "hyderabad",
            "temporaryAdd": "chennai"
        }
    }
}
"""

rdd2 = sc.parallelize([data2])

structDF1 = spark.read.option("multiline", "true").json(rdd2)

structDF1.show()
structDF1.printSchema()


# Only way to deal with struct Datatype
falttenDF1 = (
    structDF1.select(
        "id",
        "trainer",
        "zeyoAddress.user.permanentAdd",
        "zeyoAddress.user.temporaryAdd"
    )
)
falttenDF1.show()
falttenDF1.printSchema()




# nested struct

data3 = """
    {
        "place": "Hydrabad",
        "user": {
            "name": "zeyo",
            "address": {
                "number": "40",
                "street": "ashok nagar",
                "pin": "400209"
            }
        }
    }
"""

# create RDD

rdd3 = sc.parallelize([data3])    # RDD Conversion

# read the data
complexArr = spark.read.option("multiline","true").json(rdd3)    # DataFrame Reads
complexArr.show()                      # Second Priority
complexArr.printSchema()               # First Priority


# Flatten the Data

flatDF = (
    complexArr
    .select(
        "place",
        "user.name",
        "user.address.number",
        "user.address.street",
        "user.address.pin"
    )
)

flatDF.show()
flatDF.printSchema()


# Flatten the data with withcolumn expression
from pyspark.sql.functions import *

flatWithcolumn = (
    complexArr
    .withColumn(
        "name",
        expr("user.name")
    )
    .withColumn(
        "number",
        expr("user.address.number")
    )
    .withColumn(
        "street",
        expr("user.address.street")
    )
    .withColumn(
        "pin",
        expr("user.address.pin")
    )
    .drop("user")
)

flatWithcolumn.show()
flatWithcolumn.printSchema()


# Third Type :- Array

data4 = """
    {
        "id": 2,
        "trainer": "Sai",
        "zeyostudents": ["Aarthi", "Arun"]
    }
"""

rdd4 = sc.parallelize([data4])

arrtype = (
    spark.read.format("json")
    .option("multiline", "true")
    .json(rdd4)
)
arrtype.show()
arrtype.printSchema()


# Flatten the array type using selectExpr
arrFlatDF = (
    arrtype
    .selectExpr(
        "id",
        "trainer",
        "explode(zeyostudents) as zeyostudents"
    )
)
arrFlatDF.show()
arrFlatDF.printSchema()

# Flatten the array type using withColumn
print("using Explode")
arrFlatDF = (
    arrtype
    .withColumn(
        "zeyostudents",
        # expr("explode(zeyostudents)")
        explode(col("zeyostudents"))
    )
)
arrFlatDF.show()
arrFlatDF.printSchema()

