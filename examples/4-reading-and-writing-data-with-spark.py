import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

spark.sparkContext.getConf().getAll()
spark

path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)


user_log.printSchema()

user_log.describe()

user_log.show(n=1)
user_log.take(5)
out_path = "data/sparkify_log_small.csv"
user_log.write.save(out_path, format="csv", header=True)
user_log_2 = spark.read.csv(out_path, header=True)
user_log_2.printSchema()
user_log_2.take(2)
user_log_2.select("userID").show()
user_log_2.take(1)
