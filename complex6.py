
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


spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

sc = spark.sparkContext

print("PySpark Version:", __import__("pyspark").__version__)
print("Spark Version:", spark.version)
print("SparkContext:", sc)

# ================================Code Start Below=======================================


# read complex struct data json file

data = (
    spark.read.format("json")
    .option("multiline","true")
    .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\sample.json")
)
data.show()
data.printSchema()

# Flatten the data with selectExpr
flatDF = data.selectExpr(
    "age",
    "billing_address.address as address",
    "billing_address.city as city",
    "billing_address.postal_code as postal_code",
    "billing_address.state as state",
    "date_of_birth",
    "email_address",
    "first_name",
    "height_cm",
    "is_alive",
    "last_name",
    "shipping_address.address as shipping_address",
    "shipping_address.city as shipping_city",
    "shipping_address.postal_code as shipping_postal_code",
    "shipping_address.state as shipping_state",
)
flatDF.show()
flatDF.printSchema()

# Flatten the data with withColumn Expr
print("=======================With Column Expression===================")
from pyspark.sql.functions import *
flattenDF = (
    data
    .withColumn("bill_address", expr("billing_address.address "))
    .withColumn("bill_city", expr("billing_address.city "))
    .withColumn("bill_postal_code", expr("billing_address.postal_code "))
    .withColumn("bill_state", expr("billing_address.state "))
    .withColumn("ship_address", expr("shipping_address.address "))
    .withColumn("ship_city", expr("shipping_address.city "))
    .withColumn("ship_postal_code", expr("shipping_address.postal_code "))
    .withColumn("ship_state", expr("shipping_address.state "))
    .drop("billing_address", "shipping_address")
)

flattenDF.show()
flattenDF.printSchema()

# flatten Json data through loop

print("=============FlattenDF Function======================")
from pyspark.sql.types import StructType
def flatten_df(json_obj):
    cols = []
    for field in json_obj.schema.fields:
        if isinstance(field.dataType, StructType):
            for nested in field.dataType.fields:
                cols.append(col(f"{field.name}.{nested.name}").alias(f"{field.name}_{nested.name}"))
        else:
            cols.append(col(field.name))
    # print(cols)
    return json_obj.select(cols)



flat_df = flatten_df(data)
flat_df.show()



# Array column

print("================Complex Data Processing: Array type=========================")

dataArr = {
    "id": 1,
    "trainer": "sai",
    "students": [
        "Archana",
        "Rishi"
    ]
}

df = spark.read.json(sc.parallelize([dataArr]))
df.show()
df.printSchema()

# select Expression
flattenDF = df.selectExpr(
    "id",
    "trainer",
    "explode(students) as students"
)
flattenDF.show()
flattenDF.printSchema()


# withcolumn

flattenDF = df.withColumn(
    "studentsr",
    expr("explode(students)")
)
flattenDF.show()
flattenDF.printSchema()


























