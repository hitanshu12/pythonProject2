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

# 3. Read XML
xml_path = r"data/file6.xml"

xml_df = (
    spark.read.format("xml")
    .option("rowTag", "row")
    .load(xml_path)
)

print("\n=== XML DataFrame ===")
xml_df.show(10)

# 4. Read a text file via RDD
txt_path = r"data/file1.txt"

rdd = sc.textFile(txt_path)

print("\n=== First 5 Lines of Text File ===")
print(rdd.take(5))

# 5. Convert RDD to DataFrame
from pyspark.sql import Row

df_from_rdd = rdd.map(lambda line: Row(text=line)).toDF()

print("\n=== RDD Converted to DataFrame ===")
df_from_rdd.show()

# Find the no of row with more than 80 character
print("\n=== Filer Data ===")
filterDF = rdd.filter(lambda x: len(x) > 80)
#print(filterDF.collect())

# Split data with , delimiter
print("\n=== Split , Data ===")
splitDF = filterDF.flatMap(lambda x: x.split(','))
#print(splitDF.collect())


# Replace - if any
print("\n=== replace Data ===")
rplcDF = splitDF.map(lambda x: x.replace("-", ""))
#print(rplcDF.collect())

# add zeyo to each member
print("\n=== add Data ===")
addDF = rplcDF.map(lambda x: x + " Zeyo")
# addDF.foreach(print)

for x in addDF.take(20):
    print(x)
# print(addDF.take(20))


"""
Goal: Count how many times each word appears in a text file.

Read a text file using RDD.

Split each line into words.

Map each word to (word, 1) and reduce by key.

Show the top 10 most frequent words.
"""

print("Word Count Example")

data = ['Project', 'Gutenberg’s', 'Alice’s', 'Adventures', 'in', 'Wonderland', 'Project', 'Gutenberg’s', 'Adventures',
        'in', 'Wonderland', 'Project', 'Gutenberg’s']

rddDF = sc.parallelize(data)
for x in rddDF.take(5):
    print(x)
print()
print("Split each line into words.")
print()
flatRdd = rddDF.flatMap(lambda x: x.split(","))
print(flatRdd.take(5))
print()
print("Map each word to (word, 1) and reduce by key.")
print()
addRDD1 = flatRdd.map(lambda x: (x, 1))
print(addRDD1.take(5))
print()
print("Reduce by key to count occurrences")
print()
wordCnt = addRDD1.reduceByKey(lambda x, y: x+y)
print(wordCnt.collect())
# 6. Stop Spark
# spark.stop()


"""
Join two dataset
"""
# Read Employee data