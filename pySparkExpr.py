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

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()




data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()

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

df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")

# df.persist() # DF id going to use again and again
# Perform Select Expression

# make category uppercase
print("make category uppercase")
print()

catUpDf = df.selectExpr(
    "id",
    "tdate",
    "amount",
    "upper(category) as category",
    "product",
    "spendby"
)

catUpDf.show(5)

print()
print("make category uppercase")
print()


#df.unpersist()
print("Add hundred to amount column")

addAmt = df.selectExpr(
    "id",
    "tdate",
    "amount + 100 as amount",
    "upper(category) as category",
    "product",
    "spendby"
)
addAmt.show()
print("Add ~ zeyo to product column")

prscesProduct = df.selectExpr(
    "id",
    "tdate",
    "amount + 100 as amount",    # Processing
    "upper(category) as category",   # Processing
    "concat(product, '~ Zeyo') as product",   # Processing
    "spendby"
)
prscesProduct.show()

print("year from tdate column")

prscesProduct = df.selectExpr(
    "id",
    "split(tdate,'-')[2] as tdate",  # Processing
    "amount + 100 as amount",    # Processing
    "upper(category) as category",   # Processing
    "concat(product, '~ Zeyo') as product",   # Processing
    "spendby"
)
prscesProduct.show()

statusDF = (df.selectExpr(
    "cast(id as int) as id",
    "split(tdate,'-')[2] as tdate",  # Processing
    "amount + 100 as amount",    # Processing
    "upper(category) as category",   # Processing
    "concat(product, '~ Zeyo') as product",   # Processing
    "spendby",
    "case when spendby = 'cash' then 0 else 1 end as status",
    """case 
        when spendby = 'cash' then 0
        when spendby = 'paytm' then 2
        else 1 end as multiCase"""
)
            .withColumn("status_withcolumn", F.when(F.col("spendby") == "cash", 0).otherwise(1))
            .withColumn("multiWithCol", F.when(F.col("spendby") == "credit", 2).otherwise(1))
            )

statusDF.show()

from pyspark.sql.functions import *

print("with column")

withcolDF = (
    df.withColumn("amount", expr("amount + 100"))
    .withColumn("category", expr("upper(category)"))
    .withColumn("product", expr("concat(product, '~ Zeyo')"))
    .withColumn("tdate", expr("split(tdate,'-')[2]"))
    .withColumn("multiStatus", expr("""
        case 
        when spendby = 'cash' then 0
        when spendby = 'paytm' then 2
        else 1 end as multiCase
    """))
)

withcolDF.show()

print("with column End")