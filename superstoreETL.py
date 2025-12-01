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
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# read Data set

orderDF = (
    spark.read.format("csv")
    .option("header","true")
    .option("inferschema","true")
    .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\superstore_dataset.csv")
)

orderDF.show(5)

orderDF.printSchema()

# Make order id as integer

orderDF1 = orderDF.withColumn("order_id", expr("split(order_id, '-')[2]"))\
    .withColumn("order_id", expr("cast(order_id as int)"))

orderDF1.show(5)

# Make order_date and ship_date as date
# Make quantity as int
# Make profit as int
# Make discount as int

print("column Data cleaning start")

orderDF1 = (orderDF\
    .withColumn("order_date", expr("to_date(order_date, 'M/d/yyyy')"))\
    .withColumn("ship_date", expr("to_date(ship_date, 'M/d/yyyy')"))\
    .withColumn(
        "quantity", expr("case when quantity = 'United States' then null else quantity end")
    ).withColumn("quantity",
                 expr("cast(quantity as int)")
    ).withColumn(
    "profit",
        expr("case when profit RLIKE '^[0-9.-]+$' then profit else 0 end")
    ).withColumn("profit", expr("cast(profit as int)"))\
    .withColumn("discount",
        expr("case when discount RLIKE '^[0-9.-]+$' then discount else 0 end")
    ).withColumn(
        "discount", expr("cast(discount as double)")
    ))

orderDF1.show(5)
orderDF1.printSchema()

print("column Data cleaning End")


# create a 2022 and 2021 sales column

print("year df")

yeardf = (
    orderDF1.select("order_id", "order_date", "ship_date")
    .withColumn("year", year("order_date"))
)
yeardf.show()

print("Sales Per year - Each year")
sales_per_year = (
    orderDF1
    .withColumn("year", F.year("order_date"))
    .groupBy("year")
    .agg(F.sum("sales").alias("total_sales"))
    .orderBy("year")
)

sales_per_year.show()

w = Window.orderBy("year")
print("Sales Per year - Previous year")

sales_per_year = sales_per_year.withColumn(
    "prev_year_sales",
    F.lag("total_sales").over(w)
)

sales_per_year.show()

# YoY Growth = (current - previous) / previous * 100
sales_per_year = sales_per_year.withColumn(
    "YoY_growth_percent",
    ((col("total_sales") - col("prev_year_sales")) / col("prev_year_sales") * 100)
)

sales_per_year.show()

# Round Sales Per year, prev_year_sales and YoY_growth_percent to 2 decimal places

sales_per_year = (
    sales_per_year
    .withColumn("total_sales", F.round("total_sales", 0) )
    .withColumn("prev_year_sales", F.round("prev_year_sales", 0) )
    .withColumn("YoY_growth_percent", F.round("YoY_growth_percent",2))
)

sales_per_year.show()

# orderDF2 = (
#     orderDF1
#     .withColumn(
#         "orders_2021",
#         expr("sum(case when year(order_date) = 2021 then sales else 0 end)")
#     )
#     .withColumn(
#         "orders_2022",
#         expr("sum(case when year(order_date) = 2022 then sales else 0 end)")
#     )
#     .withColumn(
#         "YOY_diff",
#         expr("orders_2021 - orders_2022")
#     )
#
# )

# orderDF2.show()
























