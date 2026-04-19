
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

# Create a Python  List with 1,4,6,7 and Do an iteration and add 2 to it

lis = [1, 4, 6, 7]

# convert to rdd
rddLis = sc.parallelize(lis)

addLis = rddLis.map(lambda x: x + 2)
print(addLis.collect())


# Create a Python List with zeyobron,zeyo and analytics and filter elements contains zeyo-

lisStr = ['zeyobron', 'zeyo', 'analytics']

rddStr = sc.parallelize(lisStr)

filLis = rddStr.map(lambda x: 'zeyo' in x)
print(filLis.collect())


# Read file1 as an rdd and filter gymnastics rows

# read file

data = sc.textFile(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\file1.txt")

gymdata = data.filter(lambda x :  'Gymnastics' in x)
gymdata.foreach(print)

# Create a Named Tuple and Impose Named Tuple  on Gymdata to it for schema rdd
# And filter product contains Gymnastics?
#  Columns  -txnno,txndate,custno,amount,category,product,city,state,spendby

mapsplit = gymdata.map(lambda x: x.split(","))

from collections import namedtuple

columns = namedtuple('columns' , ['txnno','txndate','custno','amount','category','product','city','state','spendby'])

schemardd = mapsplit.map( lambda  x : columns(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))

prodfilter = schemardd.filter(lambda x : 'Gymnastics' in x.product)
print("======prodfilter=======")

schemadf = prodfilter.toDF()
print("====schema df=====")
schemadf.show(5)
print(schemadf.count())


# Read file 3 as csv with header true-

csvDF = spark.read.format("csv").option("header", "true")\
    .load(r"C:\Users\homiv\PySaprkProject\pythonProject2\data\file3.txt")

csvDF.show(5)

# Read file 4 as json and file 5 as parquet and show both the dataframe?---

jsondf = spark.read.format("json").load("file4.json").select('txnno','txndate','custno','amount','category','product','city','state','spendby')
print("====jsondf df=====")
jsondf.show(5)
print(jsondf.count())


parquetdf = spark.read.load("file5.parquet")
print("====parquetdf df=====")
parquetdf.show(5)
print(parquetdf.count())

# Union all the dataframes ensure columns are same order
uniondf = schemadf.union(csvDF).union(jsondf).union(parquetdf)
print("====uniondf df=====")
uniondf.show(5)
print(uniondf.count())

# From Union df Get year from txn date and rename it with year  and
# add one column at the end as status 1 for cash and 0 for credit in spendby
# and filter txnno>50000

from pyspark.sql.functions import *

procdf = (

            uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
                    .withColumnRenamed("txndate","year")
                    .withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
                    .filter("txnno > 50000")
)

print("====procdf df=====")
procdf.show(5)
print(procdf.count())

# From procdf Find the Cummulative sum of  amount and count the custno foreach CATEGORY-

aggdf = procdf.groupBy("category").agg(
    sum("amount").alias("total"),
    count("custno").alias("cnt")
)
print("====aggdf df=====")
aggdf.show(5)

# Write as an parquet in local with mode Append and partition the category column-

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

inner = cust.join(prod, ["id"] , "inner")
inner.show()

left = cust.join(prod, ["id"] , "left")
left.show()

right = cust.join(prod, ["id"] , "right")
right.show()

full = cust.join(prod, ["id"] , "full")
full.show()

lefanti = cust.join(prod, ["id"] , "leftanti")
lefanti.show()

cross = cust.crossJoin(prod)
cross.show()


from pyspark.sql.window import Window

data = [

    ("DEPT1", 1000),
    ("DEPT1", 500),
    ("DEPT1", 700),
    ("DEPT2", 400),
    ("DEPT2", 200),
    ("DEPT3", 200),
    ("DEPT3", 500)
]

columns = ["department", "salary"]
df = spark.createDataFrame(data, columns)

df.show()



# Find second highest salary
deptwindow = Window.partitionBy("department").orderBy(col("salary").desc())
denserankdf= df.withColumn("rnk", dense_rank().over(deptwindow))

denserankdf.show()

# difference between   Rank, Dense rank, Row Number

filrnk = denserankdf.filter(" rnk  = 2 ")
filrnk.show()

finaldf = filrnk.drop("rnk")
finaldf.show()













