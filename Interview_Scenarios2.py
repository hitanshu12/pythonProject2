
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

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


"""
Input :-
+-------+-------------------+
|food_id|          food_item|
+-------+-------------------+
|      1|        Veg Biryani|
|      2|     Veg Fried Rice|
|      3|    Kaju Fried Rice|
|      4|    Chicken Biryani|
|      5|Chicken Dum Biryani|
|      6|     Prawns Biryani|
|      7|      Fish Birayani|
+-------+-------------------+

+-------+------+
|food_id|rating|
+-------+------+
|      1|     5|
|      2|     3|
|      3|     4|
|      4|     4|
|      5|     5|
|      6|     4|
|      7|     4|
+-------+------+
Output :-
+-------+-------------------+------+---------------+
|food_id|          food_item|rating|stats(out of 5)|
+-------+-------------------+------+---------------+
|      1|        Veg Biryani|     5|          *****|
|      2|     Veg Fried Rice|     3|            ***|
|      3|    Kaju Fried Rice|     4|           ****|
|      4|    Chicken Biryani|     4|           ****|
|      5|Chicken Dum Biryani|     5|          *****|
|      6|     Prawns Biryani|     4|           ****|
|      7|      Fish Birayani|     4|           ****|
+-------+-------------------+------+---------------+
"""

food_data = [
    (1, "Veg Biryani"),
    (2, "Veg Fried Rice"),
    (3, "Kaju Fried Rice"),
    (4, "Chicken Biryani"),
    (5, "Chicken Dum Biryani"),
    (6, "Prawns Biryani"),
    (7, "Fish Birayani")
]

food_columns = ["food_id", "food_item"]

food_df = spark.createDataFrame(food_data, food_columns)

food_df.show()

rating_data = [
    (1, 5),
    (2, 3),
    (3, 4),
    (4, 4),
    (5, 5),
    (6, 4),
    (7, 4)
]

rating_columns = ["food_id", "rating"]

rating_df = spark.createDataFrame(rating_data, rating_columns)

rating_df.show()

# join

finalDF = (
    food_df.join(rating_df, food_df.food_id == rating_df.food_id, 'inner')
    .select(
        food_df["food_id"],
        food_df["food_item"],
        rating_df["rating"]
    )
)
finalDF.show()
finalDF.printSchema()

finalDF = finalDF.withColumn(
    "number_of_stars", expr("repeat('*', rating)")
)
finalDF.show()


"""
Input :-
+----+-----+--------+-----------+
|col1| col2|    col3|       col4|
+----+-----+--------+-----------+
|  m1|m1,m2|m1,m2,m3|m1,m2,m3,m4|
+----+-----+--------+-----------+
Output :-
+-----------+
|        col|
+-----------+
|         m1|
|      m1,m2|
|   m1,m2,m3|
|m1,m2,m3,m4|
|           |
+-----------+
"""
data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]

df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
df.show()

conDF = (
    # df.withColumn('col', concat(col('col1'), lit("-"), col('col2'), lit("-"), col('col3'), lit("-"), col('col4')))
    df.withColumn('col', concat_ws("-", col('col1'), col('col2'), col('col3'), col('col4')))
    .select(col('col'))
)

conDF = (
    conDF.withColumn(
        "new_col",
         explode(split(col('col'), '-'))
    )
    .select(col('new_col'))
)

conDF.show()

"""
Write a SQL Query to extract second most salary for each department
Input :-
+------+----+-------+-------+
|emp_id|name|dept_id| salary|
+------+----+-------+-------+
|     1|   A|      A|1000000|
|     2|   B|      A|2500000|
|     3|   C|      G| 500000|
|     4|   D|      G| 800000|
|     5|   E|      W|9000000|
|     6|   F|      W|2000000|
+------+----+-------+-------+

+--------+---------+
|dept_id1|dept_name|
+--------+---------+
|       A|    AZURE|
|       G|      GCP|
|       W|      AWS|
+--------+---------+
Output :-
+------+----+---------+-------+
|emp_id|name|dept_name| salary|
+------+----+---------+-------+
|     1|   A|    AZURE|1000000|
|     6|   F|      AWS|2000000|
|     3|   C|      GCP| 500000|
+------+----+---------+-------+
"""

emp_data = [
    (1, "A", "A", 1000000),
    (2, "B", "A", 2500000),
    (3, "C", "G", 500000),
    (4, "D", "G", 800000),
    (5, "E", "W", 9000000),
    (6, "F", "W", 2000000)
]

emp_columns = ["emp_id", "name", "dept_id", "salary"]

emp_df = spark.createDataFrame(emp_data, emp_columns)
emp_df.show()

dept_data = [
    ("A", "AZURE"),
    ("G", "GCP"),
    ("W", "AWS")
]

dept_columns = ["dept_id1", "dept_name"]

dept_df = spark.createDataFrame(dept_data, dept_columns)
dept_df.show()

joinDF = (
    emp_df.join(dept_df, emp_df["dept_id"] == dept_df["dept_id1"], 'inner')
    .select(
        emp_df["emp_id"],
        emp_df["name"].alias('emp_name'),
        dept_df["dept_name"],
        emp_df["salary"]
    )
)
joinDF.show()

w = Window.partitionBy(col('dept_name')).orderBy(col('salary').desc())

secDF = (
    joinDF.withColumn('rnk', rank().over(w))
    .filter(col('rnk') == 2)
    .drop(col('rnk'))
)
secDF.show()


secDF.groupby("emp_id").count().orderBy("count", ascending = False).show()

















