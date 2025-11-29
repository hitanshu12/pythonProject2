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

from pyspark.sql.window import Window
from pyspark.sql.functions import *

superStoreDF = (spark
                .read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\superstore_dataset.csv"))
superStoreDF.show(5)

# Scenario 1: Find Duplicates customers
dupCust = (superStoreDF
           .groupby("category")
           .agg(
    count("customer").alias("customer")
)
           .filter("customer > 1")
           )

dupCust.show(5)

# Scenario 2: Remove Duplicates and keep Latest

w = Window.partitionBy("customer").orderBy(col("order_date").desc())
rmDupDF = (
    superStoreDF.withColumn(
        "rn",
        row_number().over(w)
    )
    .filter(col("rn") == 1)
    .drop("rn")
)
rmDupDF.show(5)

# Scenario 3: Pivot and Unpivot
print("# Pivot the DataFrame")
print()
pivotdf = rmDupDF.filter(col("country").isNotNull()).groupby("category").pivot("country").sum("sales")
pivotdf.show(5)
print()
print("# UnPivot the DataFrame")
print()

# Scenario 4: Join two dataframe with selected column

# Scenario 5: Conditional Column (Case When)

conditonalDF = (superStoreDF
.select("country", "sales", "profit")
.withColumn(
    "sales_range",
    when(col("sales") < 100, "low")
    .when((col("sales") > 100) & (col("sales") < 300), "medium")
    .otherwise("high")
)
)
conditonalDF.show()

# Scenario 6: Explode array and map

# Scenario 7: Find the customers who bought in both months

# month Column
df_month = superStoreDF.withColumn(
    "month",
    month(superStoreDF["order_date"])
).select("category","subcategory", "segment", "customer", "region", "country", "state", "city", "order_date", "month","quantity", "sales", "profit")
df_month.show()
# df_jan = superStoreDF.filter(col("order_date").month() = )

# Scenario 8: Top N records per group
print("Top N records per group")
topNDF = (superStoreDF
           .withColumn("rk", row_number().over(Window.partitionBy("category").orderBy("sales")))
           .filter("rk <= 3")
           .drop("rk")
           .select("category", "subcategory", "segment", "customer", "region", "country", "state", "city", "order_date", "quantity", "sales", "profit")
        )
topNDF.show()


# Calculate top customers for each category
supDF = superStoreDF.select("category", "subcategory", "segment", "customer", "region", "country", "state", "city", "order_date", "quantity", "sales", "profit")

supDF.show(5)

print("calculate top customer form each category")

topCust = (supDF
           .withColumn("rnk", dense_rank().over(Window.partitionBy("category").orderBy("sales")) )
           .filter("rnk == 1")
           .select("category", "customer", "sales", "profit")
        )

topCust.show()



# Rolling totals

# Ranking transactions per user

# End-of-week outcome:

# Scenario 9: Fill missing values

# Scenario 10: Convert wide to long format

# Scenario 11: Get max value per group
