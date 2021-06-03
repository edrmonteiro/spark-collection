"""
Data Wrangling with Spark
This is the code used in the previous screencast. Run each code cell to understand what the code does and how it works.

These first three cells import libraries, instantiate a SparkSession, and then read in the data set
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum


import datetime

import numpy as np
import pandas as pd
#matplotlib inline
import matplotlib.pyplot as plt

spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

path = "/home/eduardo/dev/spark_collection/examples/data/sparkify_log_small.json"
user_log = spark.read.json(path)

#Data Exploration
#The next cells explore the data set.

user_log.take(5)
user_log.printSchema()
user_log.describe().show()
user_log.describe("artist").show()
user_log.describe("sessionId").show()
user_log.count()
user_log.select("page").dropDuplicates().sort("page").show()
user_log.select(["userId", "firstname", "page", "song"]).where(user_log.userId == "1046").collect()


#Drop Rows with Missing Values
#As you'll see, it turns out there are no missing values in the userID or session columns. But there are userID values that are empty strings.

user_log_valid = user_log.dropna(how = "any", subset = ["userId", "sessionId"])
user_log_valid.count()
user_log.select("userId").dropDuplicates().sort("userId").show()
user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")
user_log_valid.count()


#Users Downgrade Their Accounts¶
#Find when users downgrade their accounts and then flag those log entries. Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
user_log_valid.filter("page = 'Submit Downgrade'").show()
user_log.select(["userId", "firstname", "page", "level", "song"]).where(user_log.userId == "1138").collect()
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())
user_log_valid = user_log_valid.withColumn("downgraded", flag_downgrade_event("page"))
user_log_valid.head()
from pyspark.sql import Window
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)
user_log_valid = user_log_valid.withColumn("phase", Fsum("downgraded").over(windowval))
user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log.userId == "1138").sort("ts").collect()


#Question 1
user_log = spark.read.json(path)
user_log.select(["page"]).where(user_log.userId == "").dropDuplicates().show()

df = spark.read.json(path)
# filter for users with blank user id
blank_pages = df.filter(df.userId == '') \
    .select(col('page') \
    .alias('blank_pages')) \
    .dropDuplicates()

# get a list of possible pages that could be visited
all_pages = df.select('page').dropDuplicates()

# find values in all_pages that are not in blank_pages
# these are the pages that the blank user did not go to
for row in set(all_pages.collect()) - set(blank_pages.collect()):
    print(row.page)
    



#question 2
user_log = spark.read.json(path)
user_log.select(["artist","auth","firstName","gender","itemInSession","lastName","length","level","location","method","page","registration","sessionId","song","status","ts","userAgent","userId"]).where(user_log.userId == "").show()





#question 3
user_log = spark.read.json(path)
user_log.count()
user_log.where(user_log.gender == "F").count()
user_log.where(user_log.gender == "M").count()

user_log.select(["artist","auth","firstName","gender","itemInSession","lastName","length","level","location","method","page","registration","sessionId","song","status","ts","userAgent","userId"]).show()


df.filter(df.gender == 'F') \
    .select('userId', 'gender') \
    .dropDuplicates() \
    .count()
    

#question 4
user_log = spark.read.json(path)
user_log.select(["artist","song"]).groupBy("artist").count().sort(col("count").desc()).show()

df.filter(df.page == 'NextSong') \
    .select('Artist') \
    .groupBy('Artist') \
    .agg({'Artist':'count'}) \
    .withColumnRenamed('count(Artist)', 'Artistcount') \
    .sort(desc('Artistcount')) \
    .show(1)

#question 5
user_log = spark.read.json(path)
user_log.select(["userId","artist", "song"]).where((col("page") =="Home") & (col("userId") != "")).count() 
user_log.select(["userId"]).where((col("page") =="Home") & (col("userId") != "")).dropDuplicates().count()
 

function = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', function(col('page'))) \
    .withColumn('period', Fsum('homevisit').over(user_window))

cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}).show()
    
    
    


