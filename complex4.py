
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


from pyspark.sql.functions import *


#   Read data through python
file_path = r"C:\Users\homiv\PySaprkProject\pythonProject2\data\file7.json"


# pass filepath in [] bracket because parallelize can process only list data
# rdd = sc.parallelize([file_path])


# convert into dataframe

df = spark.read.option("multiline", "true").json(file_path)
# df.show()
df.printSchema()


# Process to flatten/ Simplify the data

flattenDf = (
    df
    .withColumn(
        "departments",
        explode(col("departments"))
    )
)

# flattenDf.show()
# flattenDf.printSchema()

# Flatten the inner array type  - departments - > Teams

flattenDf = (
    flattenDf
    .withColumn(
        "teams",
        explode(col("departments.teams"))
    )
)

print()
# flattenDf.show()
# flattenDf.printSchema()

# Flatten the inner array type  - teams -> members


flattenDf = (
    flattenDf
    .withColumn("members", explode(col("teams.members")))
)
flattenDf.show()
flattenDf.printSchema()
print()

# Flatten the inner array type  - members -> projects

flattenDf = (
    flattenDf
    .withColumn("projects", explode(col("members.projects")))
)

flattenDf.show()
flattenDf.printSchema()
print()
# Flatten the inner array type  - members -> skills

flattenDf = (
    flattenDf
    .withColumn("skills", explode(col("members.skills")))
)

flattenDf.show()
flattenDf.printSchema()


finalFlattenDF = (
    flattenDf
    .selectExpr(
        "company",
        "departments.name as departments_name",
        "teams.teamName as teamName",
        "members.id as members_id",
        "members.name as members_name",
        "projects.projectId as projectId",
        "projects.status as project_status",
        "projects.title as project_title",
        "skills"
    )
)

finalFlattenDF.show()
finalFlattenDF.printSchema()

























