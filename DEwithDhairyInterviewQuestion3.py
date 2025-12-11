
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




from pyspark.sql.types import StructType, StructField, StringType, IntegerType


schema = StructType([
    StructField("k_no", IntegerType(), nullable=True),
    StructField("king", StringType(), nullable=True),
    StructField("house", StringType(), nullable=True)
])

# creating the dataframe !
data = [
    (1, "Robb Stark", "House Stark"),
    (2, "Joffery Baratheon", "House Lannister"),
    (3, "Stannis Baratheon", "House Baratheon"),
    (4, "Belon Greyjoy", "House Greyjoy"),
    (5, "Mace Tyrell", "House Tyrell"),
    (6, "Doren Martell", "House Martell")
]


df = spark.createDataFrame(data, schema=schema)
df.show()


schema2 = StructType([
    StructField("battle_number", IntegerType(), nullable=True),
    StructField("king", StringType(), nullable=True),
    StructField("attacker_king", IntegerType(), nullable=True),
    StructField("defender_king", IntegerType(), nullable=True),
    StructField("attacker_outcome", IntegerType(), nullable=True),
    StructField("region", StringType(), nullable=True)
])

data2 = [
    (1, "Battle of Oxcross", 1, 2, 1, "The North"),
    (2, "Battle of Blackwater", 3, 4, 0, "The North"),
    (3, "Battle of the Fords", 1, 5, 1, "The Reach"),
    (4, "Battle of Green Fork", 2, 6, 0, "The Reach"),
    (5, "Battle of Ruby Ford", 1, 3, 1, "The Reverrun"),
    (6, "Battle of the Golden Tooth", 2, 1, 0, "The North"),
    (7, "Battle of Reverrun", 3, 4, 1, "The Riverlands"),
    (8, "Battle of Reverrun", 1, 3, 0, "The Riverlands")

]

df2 = spark.createDataFrame(data=data2, schema=schema2)
df2.show()

"""
    output:- region, house and number of wins.
"""


# method 1 : Group by expression

print("============Solution=================")


from pyspark.sql.functions import *
from pyspark.sql.window import Window
# Create a king number column who defend

df2 = (
    df2
    .withColumn(
        "king_no",
        expr("case when attacker_outcome == 1 then attacker_outcome else defender_king end")
    )
)

df2.show()


# Join the two data frame


battleDF = (
    df.join(df2, df["k_no"] == df2["king_no"], "inner")
)

battleDF.show()

# w = Window.partitionBy("house")

battleDF = (
    battleDF
    # .select("region", "house")
    .groupby("region", "house")
    .agg(
        count('attacker_outcome').alias("no_of_wins")
    )
)

battleDF.show()


