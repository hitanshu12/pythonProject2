
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
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


schema = StructType([
    StructField("customer_id", StringType(), nullable=True),
    StructField("start_location", StringType(), nullable=True),
    StructField("end_location", StringType(), nullable=True)
])

# creating the dataframe !
data = [
    ("c1", "New York", "Lima"),
    ("c1", "London", "New York"),
    ("c1", "Lima", "Sao Paulo"),
    ("c1", "Sao Paulo", "New Delhi"),
    ("c2", "Mumbai", "Hyderabad"),
    ("c2", "Surat", "Pune"),
    ("c2", "Hyderabad", "Surat"),
    ("c3", "Kochi", "Kurnool"),
    ("c3", "Lucknow", "Agra"),
    ("c3", "Agra", "Jaipur"),
    ("c3", "Jaipur", "Kochi")
]


df = spark.createDataFrame(data, schema=schema)
df.show()

"""
create a df for start and end location
"""








# Create a df from start_location

start_loc = (
    df.select("customer_id", "start_location")
)
start_loc.show()


# Create a df from end_location

end_loc = (
    df.select("customer_id", "end_location")
)
end_loc.show()

print("==============Solution 1=================")
df3 = (
    start_loc.alias('s')
    .join(
        end_loc.alias('e'),
        concat(col("s.customer_id"), col("s.start_location"))
        == concat(col("e.customer_id"), col("e.end_location")),
        "left_anti"
    )
)

df4 = end_loc.alias('e').join(
    start_loc.alias('s'),
    concat(col("s.customer_id"), col("s.start_location")) == concat(col("e.customer_id"), col("e.end_location")),
    "left_anti"
)

# final

df5 = df3.join(df4, ["customer_id"], "inner")
df5.show()



print("==============Solution 2=================")
# Create a union of start_loc and end_location
start_loc = (
    df.select(col("customer_id"), col("start_location").alias("loc"))
)

end_loc = (
    df.select(col("customer_id"), col("end_location").alias("loc"))
)

# create a union


df_union = (
    start_loc
    .union(end_loc)
)
df_union.show()

# now getting the unique records of each customer using group by

df_unique = (
    df_union
    .groupby(col("customer_id"), col("loc"))
    .agg(
        count(lit(1)).alias("count")
    )
    .filter(col("count") == 1)
    .drop("count")
    .orderBy(col("customer_id"))
)
df_unique.show()

# join the df_unique with the original df table to get the start and end location for the each
# customer with the help or when otherwise

df_answer = (
    df.join(
        df_unique, ((df["customer_id"] == df_unique["customer_id"]) & ( (df["start_location"] == df_unique["loc"]) | (df["end_location"] == df_unique["loc"]))), "inner"
    ).drop(df_unique.customer_id)
    .withColumn(
        "start_loc_cust",
        when(col("start_location") == col("loc"), col("start_location"))
    )
    .withColumn(
        "end_loc_cust",
        when(col("end_location") == col("loc"), col("end_location"))
    )
    .select("customer_id", "start_loc_cust", "end_loc_cust")
)

df_answer.show()

# getting the final output by grouping the df_answer dataframe

finalDF = (
    df_answer
    .groupby("customer_id")
    .agg(
        min("start_loc_cust").alias("start_location"),
        max("end_loc_cust").alias("end_location")
    )
)

finalDF.show()




