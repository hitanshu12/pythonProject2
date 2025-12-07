
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
You are given the following DataFrame marks:

| student_id | subject | marks | exam_date |
| ---------- | ------- | ----- | ---------- |
| 1     | Math  | 80  | 2024-01-01 |
| 1     | Science | 70  | 2024-01-02 |
| 1     | English | 90  | 2024-01-03 |
| 2     | Math  | 60  | 2024-01-01 |
| 2     | Science | 75  | 2024-01-02 |
| 2     | English | 65  | 2024-01-03 |
| 3     | Math  | 95  | 2024-01-01 |
| 3     | Science | 85  | 2024-01-02 |
| 3     | English | 80  | 2024-01-03 |


Write down PySpark code to :
1.  Calculate Total and Average Marks per Student
2.  Identify Best Subject per Student (Highest Marks)
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window

data = [
    (1, "Math", 80, "2024-01-01"),
    (1, "Science", 70, "2024-01-02"),
    (1, "English", 90, "2024-01-03"),
    (2, "Math", 60, "2024-01-01"),
    (2, "Science", 75, "2024-01-02"),
    (2, "English", 65, "2024-01-03"),
    (3, "Math", 95, "2024-01-01"),
    (3, "Science", 85, "2024-01-02"),
    (3, "English", 80, "2024-01-03"),
]

columns = ["student_id", "subject", "marks", "exam_date"]

df = spark.createDataFrame(data=data, schema= columns)
df.show()
df.printSchema()

# Calculate Total and Average Marks per Student
# Identify Best Subject per Student

w = Window.partitionBy("student_id")

df1 = (
    df
    .withColumn("avg_marks", round(avg("marks").over(w),2))
    .withColumn("highest_marks", max("marks").over(w))
)

df1.show()

# Second-highest marks of each student

shw = Window.partitionBy("student_id").orderBy("marks")

df2 = (
    df
    .withColumn(
        "SecondHighestMarks",
        rank().over(shw)
    )
    # .filter("SecondHighestMarks = 2")
    .filter(col("SecondHighestMarks") == 2)
)

df2.drop("SecondHighestMarks", "exam_date", "subject").show()






