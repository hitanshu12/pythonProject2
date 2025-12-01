
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

data = [
    (1, "Alice", 23),
    (2, "Bob", 34),
    (3, "Charlie", 29),
    (1, "Alice", 23),
    (2, "Bob", 34),
    (6, "Alice", 30)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data=data, schema=columns)
df.show()


"""
distinct() -> remove duplicate rows (entire row match).
dropDuplicates([subset]) -> removes duplicates based on one or mare specific columns.
"""
print("=============================Distinct========================")
distinctDF = df.distinct()
distinctDF.show()

print("=============================Drop Duplicates function========================")
dropDuplicatesDF = df.dropDuplicates(["id"])
dropDuplicatesDF.show()

print("=============================Sort and Order by Pyspark========================")

from pyspark.sql.functions import *

data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "date", "amount", "category", "product", "spend"])
df1.show()


# sort by date
print("=================Using sort() method===================")
sortDF = df1.sort("date")
sortDF.show()

sortDFAsc = df1.sort("date", ascending=True)
sortDF.show()

sortDFDesc = df1.sort("date", ascending=False)
sortDFDesc.show()


# grouping
print("=================Using sort() method===================")

# count the rows based on category

groupDF = df1.groupby("category").count()
groupDF.show()

# sum of amount the rows based on category

amountDF = df1.groupby("category").agg(
    sum(col("amount")).alias("total_amount"),
    min(col("amount")).alias("min_amount"),
    max(col("amount")).alias("max_amount")
)

amountDF.show()

data3 = [
    (1, "Manish", 20000, "Delhi"),
    (2, "Ashish", 25000, "Mumbai"),
    (3, "Rohit", 30000, "Chennai"),
    (4, "Harish", 40000, "Bangalore"),
    (5, "Satish", 35000, "Hyderabad")
]

data4 = [
    (6, "Rahul", 20000, "Dehradun"),
    (7, "Sonu", 20000, "Meerut"),
    (8, "Ambuj", 20000, "Guru gram"),
    (9, "Kishan", 20000, "Noida"),
    (10, "Nitin", 20000, "Pune"),
    (5, "Satish", 35000, "Hyderabad"),
    (11, "", 35000, "Pune"),
    (12, "", 35000, "Hyderabad")
]

columns1 = ["id", "name", "salary", "location"]

emp1 = spark.createDataFrame(data=data3, schema=columns1)
emp1.show()
emp2 = spark.createDataFrame(data=data4, schema=columns1)
emp2.show()

# perform union and Union All

print("===================Union=====================")

unionDF = emp1.union(emp2)
unionDF.show()

print("===================Union ALL=====================")

unionDF = emp1.unionAll(emp2)
unionDF.show()


print("===================Handle Null Value (na.fill)=====================")

handleNullDF = emp2.na.fill("NA")  # null values replace with NA
handleNullDF.show()

fillNaDF = emp2.fillna("unknown")  # null values replace with unknown
fillNaDF.show()

NaFillDF = emp2.na.fill("NA", ["name"])  # replace null in specific column
NaFillDF.show()

NaFillDF = emp2.na.fill("NA", ["name"]).na.fill(0, ["id"])   # replace null in  multiple column
NaFillDF.show()





