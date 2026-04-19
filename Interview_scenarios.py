
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


data = [('2020-05-30','Headphone'),('2020-06-01','Pencil'),('2020-06-02','Mask'),('2020-05-30','Basketball'),('2020-06-01','Book'),('2020-06-02','Mask'),('2020-05-30','T-Shirt')]
# columns = ["sell_date",'product']
columns = StructType([
    StructField("sell_date", StringType(), True),
    StructField("product", StringType(), True)
])

df = spark.createDataFrame(data = data, schema=columns)
df.show()

# find the items that sold in per day

transformDf = (
    df.groupby(col('sell_date'))
    .agg(
        collect_set(col('product')).alias('product'),
        size(collect_set(col('product'))).alias('null_sell')
    )
)

transformDf.show()

"""
Question (IBM Question)

Create a new datafrane df1 with the given values
Count null entries in a datafarme
Remove null entries and the store the null entries in a new datafarme df2
Create a new dataframe df3 with the given values and join the two dataframes df1 & df2
Fill the null values with the mean age all of students
Filter the students who are 18 years above and older
"""

# creating the dataframe df1

data1 = [(1, 'Jhon', 17), (2, 'Maria', 20), (3, 'Raj' ,None), (4, 'Rachel', 18)]
columns = ["id", "name", "age"]
df1 = spark.createDataFrame(data1, columns)
df1.show()

# Count null entries in a datafarme

null_count = (
    df1.select(
        [sum(col(c).isNull().cast('int')).alias(c) for c in df1.columns]
    )
)
null_count.show()

# Remove null entries and the store the null entries in a new datafarme df2

df2 = df1.filter(col('age').isNull())
df2.show()

# Create a new dataframe df3 with the given values and join the two dataframes df1 & df2

data2 = [(1,'seatle',82),(2,'london',75),(3,'banglore',60),(4,'boston',90)]
columns2 = ["id","city","code"]

df3 = spark.createDataFrame(data2,columns2)
df3.show()

joinDF = (
    df1.join(df3, df1["id"] == df3["id"], 'full')
    .select(
        df1.id,
        "name",
        "age",
        "city",
        "code"
    )
)
joinDF.show()


# Fill the null values with the mean age all of students

meanAge = joinDF.select(round(mean("age"))).collect()[0][0]
print(meanAge)

filldf = joinDF.na.fill({"age": meanAge})
filldf.show()


# Filter the students who are 18 years above and older

filldf = filldf.filter(col("age") >= 18)
filldf.show()


"""
Input :-
+-----------+------+---+------+
|customer_id|  name|age|gender|
+-----------+------+---+------+
|          1| Alice| 25|     F|
|          2|   Bob| 40|     M|
|          3|   Raj| 46|     M|
|          4| Sekar| 66|     M|
|          5|  Jhon| 47|     M|
|          6|Timoty| 28|     M|
|          7|  Brad| 90|     M|
|          8|  Rita| 34|     F|
+-----------+------+---+------+
Output :-
+---------+-----+
|age_group|count|
+---------+-----+
|    19-35|    3|
|    36-50|    3|
|      51+|    2|
+---------+-----+
"""

data = [
    (1, "Alice", 25, "F"),
    (2, "Bob", 40, "M"),
    (3, "Raj", 46, "M"),
    (4, "Sekar", 66, "M"),
    (5, "Jhon", 47, "M"),
    (6, "Timoty", 28, "M"),
    (7, "Brad", 90, "M"),
    (8, "Rita", 34, "F")
]

columns = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("gender", StringType(), True),
])

df34 = spark.createDataFrame(data= data, schema=columns)
df34.show()

trans34 = (
    df34.withColumn(
        "age_group",
        when((col('age') >= 19) & (col('age') <= 35), "19-35")
        .when((col('age') >= 36) & (col('age') <= 50), "36-50")
        .otherwise("51+")
    )
)

trans34 = trans34.groupby('age_group')\
            .agg(
                size(collect_set(col('name'))).alias('count')
            )

trans34.show()


# scenario 33:- Write a query to print the maximum number of discount tours any 1 family can choose.
"""
+--------------------+--------------+-----------+
|                  id|          name|family_size|
+--------------------+--------------+-----------+
|c00dac11bde74750b...|   Alex Thomas|          9|
|eb6f2d3426694667a...|    Chris Gray|          2|
|3f7b5b8e835d4e1c8...| Emily Johnson|          4|
|9a345b079d9f4d3ca...| Michael Brown|          6|
|e0a5f57516024de2a...|Jessica Wilson|          3|
+--------------------+--------------+-----------+

+--------------------+------------+--------+--------+
|                  id|        name|min_size|max_size|
+--------------------+------------+--------+--------+
|023fd23615bd4ff4b...|     Bolivia|       2|       4|
|be247f73de0f4b2d8...|Cook Islands|       4|       8|
|3e85ab80a6f84ef3b...|      Brazil|       4|       7|
|e571e164152c4f7c8...|   Australia|       5|       9|
|f35a7bb7d44342f7a...|      Canada|       3|       5|
|a1b5a4b5fc5f46f89...|       Japan|      10|      12|
+--------------------+------------+--------+--------+

Output :-
+-------------+-------------------+
|         name|number_of_countries|
+-------------+-------------------+
|Emily Johnson|                  4|
+-------------+-------------------+
"""

data = [('c00dac11bde74750b4d207b9c182a85f', 'Alex Thomas', 9),('eb6f2d3426694667ae3e79d6274114a4', 'Chris Gray', 2),('3f7b5b8e835d4e1c8b3e12e964a741f3', 'Emily Johnson', 4),('9a345b079d9f4d3cafb2d4c11d20f8ce', 'Michael Brown', 6),('e0a5f57516024de2a231d09de2cbe9d1', 'Jessica Wilson', 3)]

familydf = spark.createDataFrame(data,["id","name","family_size"])
familydf.show()

countrydata = [('023fd23615bd4ff4b2ae0a13ed7efec9', 'Bolivia', 2 , 4),('be247f73de0f4b2d810367cb26941fb9', 'Cook Islands', 4,8),('3e85ab80a6f84ef3b9068b21dbcc54b3', 'Brazil', 4,7),('e571e164152c4f7c8413e2734f67b146', 'Australia', 5,9),('f35a7bb7d44342f7a8a42a53115294a8', 'Canada', 3,5),('a1b5a4b5fc5f46f891d9040566a78f27', 'Japan', 10,12)]

countrydf = spark.createDataFrame(countrydata,["id","name","min_size","max_size"])
countrydf.show()

disDF = (
    familydf.join(countrydf, ((familydf["family_size"] >= countrydf["min_size"]) & (familydf["family_size"] <= countrydf["max_size"])), 'inner')
    .select(familydf["id"],
                familydf["name"],
                familydf["family_size"],
                countrydf["name"].alias("country_name"),
                countrydf["min_size"],
                countrydf["max_size"]
    )
)
disDF.show()
disDF = (
    disDF.groupby(col('name'))
    .agg(
        count("*").alias("number_of_countries")
    )
)
disDF.show()

finalDF = (
    disDF.agg(max('number_of_countries').alias('number_of_countries'))
)
finalDF.show()































