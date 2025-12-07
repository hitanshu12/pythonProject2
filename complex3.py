
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



#   Read API data through python


import urllib.request
import ssl

urlData = (
    urllib.request
    .urlopen(
        "https://randomuser.me/api/0.8/?results=10",
        context=ssl._create_unverified_context()
    )
    .read()
    .decode('utf-8')
)

print("urlData is loaded")


df = (
    spark.read.json(sc.parallelize([urlData]))
)

df.show()
df.printSchema()

# Solving complex Data
from pyspark.sql.functions import *
# solving array type to struct

flatDF = (
    df
    .withColumn(
        "results",
        explode("results")
    )
)

flatDF.show()
flatDF.printSchema()


# convert struct to normal column

flatDF = (
    flatDF
    .select(
        "nationality",
        "results.user.cell",
        "results.user.dob",
        "results.user.email",
        "results.user.gender",
        "results.user.location.city",
        "results.user.location.state",
        "results.user.location.street",
        "results.user.location.zip",
        "results.user.md5",
        "results.user.name.first",
        "results.user.name.last",
        "results.user.name.title",
        "results.user.password",
        "results.user.phone",
        "results.user.picture.large",
        "results.user.picture.medium",
        "results.user.picture.thumbnail",
        "results.user.registered",
        "results.user.salt",
        "results.user.sha1",
        "results.user.sha256",
        "results.user.username",
        "seed",
        "version"
    )
)

flatDF.show()
flatDF.printSchema()




























