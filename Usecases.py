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

print("Usecase 1 start")
print()
"""
**Goal: Analyze a CSV file 
(e.g., employees.csv) with columns: id, name, age, department, salary.**

**Step 1:-** Read CSV into a DataFrame.
**Step 2:-** Show schema and first 5 rows.
**Step 3:-** Find the average salary by department.
**Step 4:-** Find employees with salary > 50,000.
**Step 5:-** Count how many employees are in each department.
"""

from pyspark.sql.functions import *

empdf = (
         spark
         .read
         .format("csv")
         .option("header","true")
         .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\employees.csv")
         )

empdf.show(5)

deptdf = (
         spark
         .read
         .format("csv")
         .option("header","true")
         .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\departments.csv")
         )

deptdf.show(5)

# Join emp and Dept
joindf = empdf.join(deptdf, empdf.dept_id == deptdf.dept_id, "left") \
.select(empdf.emp_id,
        empdf.emp_name,
        empdf.dept_id,
        empdf.salary,
        deptdf.dept_name,
        deptdf.location
        )
joindf.show(truncate= False)

# Find the average salary by department

avgDf = joindf.groupby("dept_name").agg(
    avg("salary")
).orderBy("dept_name", ascending =False)

avgDf.show()

# Find employees with salary > 50,000.

filEmpdf = joindf.where("salary > 50000").orderBy("emp_name", ascending = False)

filEmpdf.show()

# Count how many employees are in each department

cntEmpDF = joindf.groupby("dept_name").agg(
    count("emp_name").alias("number_of_employees")
).orderBy("dept_name", ascending = False)

cntEmpDF.show()

print()
print("Usecase 1 End")

print("Usecase 2: Compare current row with the previous row")

joindf.show()

# creating new column previous row using withcolumn() method

from pyspark.sql.window import Window

w = Window.orderBy("emp_id")

previousAmtDF = joindf.withColumn("previous_amount", lag("salary",1).over(w))
previousAmtDF.show()

# creating new column difference of current  - previous row using withcolumn() method

previousAmtDF = previousAmtDF.withColumn("diff_amount", col("salary") - col("previous_amount") )
previousAmtDF.show()


# Spark Window Function

# row_number by salary

rowNumberDF = joindf.withColumn(
    "row_number",
    row_number().over(Window.partitionBy("dept_name").orderBy("salary"))
)
rowNumberDF.show()

# Rank by Salary
rankDF = rowNumberDF.withColumn(
    "rank",
    rank().over(Window.partitionBy("dept_name").orderBy("salary"))
)
rankDF.show()

# Dense Rank by Salary
denserankDF = rankDF.withColumn(
    "dense_rank",
    dense_rank().over(Window.partitionBy("dept_name").orderBy("salary"))
)
denserankDF.show()

# Running total
runningTotalDf = denserankDF.withColumn(
    "running_total",
    sum("salary").over(Window.partitionBy("dept_name").orderBy("salary"))
)
runningTotalDf.show()

# Avg value of each department
avgSalDF = runningTotalDf.withColumn(
    "avg_salary",
    avg("salary").over(Window.partitionBy("dept_name"))
)
avgSalDF.show()

# Max value of each department
maxSalDf = avgSalDF.withColumn(
    "max_salary",
    max("salary").over(Window.partitionBy("dept_name"))
)
maxSalDf.show()

# Min value of each department

minSalDf = maxSalDf.withColumn(
    "min_salary",
    min("salary").over(Window.partitionBy("dept_name"))
)
minSalDf.show()


print("Usecase 2 End")


