
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


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

foodDF = spark.sparkContext.parallelize([
    (1, "Veg Biryani"),
    (2, "Veg Fried Rice"),
    (3, "Kaju Fried Rice"),
    (4, "Chicken Biryani"),
    (5, "Chicken Dum Biryani"),
    (6, "Prawns Biryani"),
    (7, "Fish Birayani")
]).toDF(["food_id", "food_item"])

ratingDF = spark.sparkContext.parallelize([
    (1, 5),
    (2, 3),
    (3, 4),
    (4, 4),
    (5, 5),
    (6, 4),
    (7, 4)
]).toDF(["food_id", "rating"])

foodDF.show()
ratingDF.show()


# Join the DF

from pyspark.sql.functions import *


print("==================================Solution 1: Using case when========================")

joinDF = (
    foodDF.join(ratingDF, ["food_id"], "left")
    .withColumn(
        "stats( Out of 5 )",
        expr(
            """
                case 
                    when rating == 5 then "*****"
                    when rating == 4 then "****"
                    when rating == 3 then "***"
                    when rating == 2 then "**"
                    when rating == 1 then "*"
                    else "0" 
                End
            """
        )
    )
)

joinDF.show(truncate=False)

print("==================================Solution 2: Using repeat========================")

joinDF2 = (
    foodDF.join(ratingDF, ["food_id"], "left")
    .withColumn("stats( Out of 5 )", expr("repeat('*', rating)"))
)


joinDF2.show(truncate=False)

