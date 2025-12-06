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
# Read Employee data

empDF = spark.read.csv(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\employees.csv", header= True, inferSchema=True)
empDF.show()

# Read Department data

depDF = spark.read.csv(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\departments.csv", header= True, inferSchema=True)
depDF.show()

print("=====Inner Join====")

innerjoinDF = empDF.join(depDF, empDF.dept_id == depDF.dept_id, "inner").show(truncate= False)


print("=====Left Join====")

leftjoinDF = empDF.join(depDF, empDF.dept_id  ==  depDF.dept_id, "Left")
leftjoinDF.show(truncate= False)

print()

print("=====Right Join====")

rightJoinDF = empDF.join(depDF, empDF.dept_id == depDF.dept_id, "right")
rightJoinDF.show(truncate=False)

print()

print("=====Full Outer Join====")

fullJoinDF = empDF.join(depDF, empDF.dept_id == depDF.dept_id, "full")
fullJoinDF.show(truncate=False)

print()

print("=====Left Semi Join====")

leftSemiJoinDF = empDF.join(depDF, empDF.dept_id == depDF.dept_id, "leftsemi")
leftSemiJoinDF.show(truncate=False)

print()

print("=====Left Anti Join====")

leftAntiJoinDF = empDF.join(depDF, empDF.dept_id == depDF.dept_id, "leftanti")
leftAntiJoinDF.show(truncate=False)

print("Join Finish")

print()

print("Show employee name, department name, and salary")

slctClmnDF = fullJoinDF.select("emp_name", "dept_name", "salary")
slctClmnDF.show(truncate=False)

print()

print("Find the department with the highest total salary")

from pyspark.sql.functions import sum, max, col

deptSalDF = slctClmnDF.groupby("dept_name")\
    .agg(
        sum("salary").alias("Total_Salary"),
        max("salary").alias("Highest_Salary")
    )

deptSalDF.show(truncate=False)

print()

print("==== Highest Salary Deprtment =====")

resultDF = slctClmnDF.filter(col("dept_name").isNotNull()) .groupby("dept_name")\
    .agg(
        sum("salary").alias("Total_Salary")
    ).orderBy("Total_Salary", ascending =False)\
    .limit(1)

# resultDF = (
#     deptSalDF
#         .groupBy("dept_name")
#         .orderBy(col("Total_Salary").desc())
#         .limit(1)
# )
resultDF.show()
print()



data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]
cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()
data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]
prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()

print("===============Inner Join==============")
innerJoin = cust.join(prod, ["id"], "inner").orderBy("id")
innerJoin.show()

print()
print("===============Left Join==============")
leftJoin = cust.join(prod, ["id"], "left").orderBy("id")
leftJoin.show()

print()
print("===============Right Join==============")
rightJoin = cust.join(prod, ["id"], "right").orderBy("id")
rightJoin.show()

print()
print("===============Full Join==============")
fullJoin = cust.join(prod, ["id"], "full").orderBy("id")
fullJoin.show()

print()
print("===============Left Anti Join==============")
leftAntiJoin = cust.join(prod, ["id"], "leftanti").orderBy("id")
leftAntiJoin.show()

print()
print("===============Left semi Join==============")
leftSemiJoin = cust.join(prod, ["id"], "leftsemi").orderBy("id")
leftSemiJoin.show()

# take the left table and assigned all the id to the right table

crossJoinDF = cust.crossJoin(prod)
crossJoinDF.show()


data5 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]
cust1 = spark.createDataFrame(data5, ["id", "name"])
cust1.show()
data6 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]
prod1 = spark.createDataFrame(data6, ["cid", "product"])
prod1.show()


print("===============Inner Join==============")
innerJoin = cust1.join(prod1, cust1["id"] == prod1["cid"], "inner").orderBy("id")
innerJoin.show()

print()
print("===============Left Join==============")
leftJoin = cust1.join(prod1, cust1["id"] == prod1["cid"], "left").orderBy("id")
leftJoin.show()

print()
print("===============Right Join==============")
rightJoin = cust1.join(prod1, cust1["id"] == prod1["cid"], "right").orderBy("id")
rightJoin.show()

print()
print("===============Full Join==============")
fullJoin = cust1.join(prod1, cust1["id"] == prod1["cid"], "full").orderBy("id")
fullJoin.show()

# remove cid column from full outer join
from pyspark.sql.functions import *
finalFullDF = (
    fullJoin
    .withColumn("id", expr("coalesce(id, cid)"))
    .withColumn("id_case", expr("case when id is null then cid else id end"))
    .drop("cid")
)

finalFullDF.show()







