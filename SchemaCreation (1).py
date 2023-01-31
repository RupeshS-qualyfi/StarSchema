# Databricks notebook source
#dbutils.fs.cp('/tmp/landing/riders.zip', '/tmp/Rupesh/landing/riders.zip')

# COMMAND ----------

# MAGIC %sh 
# MAGIC unzip /dbfs/tmp/Rupesh/landing/trips.zip -d /dbfs/tmp/Rupesh/landing1
# MAGIC unzip /dbfs/tmp/Rupesh/landing/riders.zip -d /dbfs/tmp/Rupesh/landing1
# MAGIC unzip /dbfs/tmp/Rupesh/landing/payments.zip -d /dbfs/tmp/Rupesh/landing1
# MAGIC unzip /dbfs/tmp/Rupesh/landing/stations.zip -d /dbfs/tmp/Rupesh/landing1

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType, DecimalType


# define the schema for the trip table
trip_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("rider_id", StringType(), True)
])

# define the schema for the payment table
payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("rider_id", StringType(), True)
])

# define the schema for the station table
station_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True)
])

# define the schema for the rider table
rider_schema = StructType([
    StructField("rider_id", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", StringType(), True),
    StructField("account_start", StringType(), True),
    StructField("account_end", StringType(), True),
    StructField("is_member", StringType(), True)
])

# COMMAND ----------

# create a dataframe for the trip table
trip_df = spark.read.csv("/tmp/Rupesh/landing1/trips.csv", schema = trip_schema)

# create a dataframe for the payment table
payment_df = spark.read.csv("/tmp/Rupesh/landing1/payments.csv", schema = payment_schema)

# create a dataframe for the station table
station_df = spark.read.csv("/tmp/Rupesh/landing1/stations.csv", schema = station_schema)

# create a dataframe for the rider table
rider_df = spark.read.csv("/tmp/Rupesh/landing1/riders.csv", schema = rider_schema)


# COMMAND ----------

trip_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/trip")
payment_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/payment")
station_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/station")
rider_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/rider")

# COMMAND ----------



# COMMAND ----------

trips.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Schema

# COMMAND ----------

from pyspark.sql.types import *
# define the schema for the trip table
s_trip_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("rider_id", IntegerType(), True)
])

# define the schema for the payment table
s_payment_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("amount", FloatType(), True),
    StructField("rider_id", IntegerType(), True)
])

# define the schema for the station table
s_station_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True)
])

# define the schema for the rider table
s_rider_schema = StructType([
    StructField("rider_id", IntegerType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", DateType(), True),
    StructField("account_start", DateType(), True),
    StructField("account_end", DateType(), True),
    StructField("is_member", BooleanType(), True)
])

# COMMAND ----------

# read the delta tables in bronze and store as dataframes
from pyspark.sql.functions import unix_timestamp, col

# create a dataframe for the trip table
trip_df = spark.read.format("delta").load("/tmp/Rupesh/Bronze/trip")

# create a dataframe for the payment table
payment_df = spark.read.format("delta").load("/tmp/Rupesh/Bronze/payment")

# create a dataframe for the station table
station_df = spark.read.format("delta").load("/tmp/Rupesh/Bronze/station")

# create a dataframe for the rider table
rider_df = spark.read.format("delta").load("/tmp/Rupesh/Bronze/rider")


# COMMAND ----------

# need to cast the datetime as unix timestamp
trip_df = trip_df.withColumn('started_at', unix_timestamp(col('started_at'), 'dd/MM/yyyy HH:mm').cast('timestamp'))
trip_df = trip_df.withColumn('ended_at', unix_timestamp(col('ended_at'), 'dd/MM/yyyy HH:mm').cast('timestamp'))

# COMMAND ----------

# Convert the dataframes to the new schema

# trip
s_trip_df = trip_df.select(*(trip_df[c].cast(s_trip_schema[i].dataType).alias(s_trip_schema[i].name) for i, c in enumerate(trip_df.columns)))

# payment
s_payment_df = payment_df.select(*(payment_df[c].cast(s_payment_schema[i].dataType).alias(s_payment_schema[i].name) for i, c in enumerate(payment_df.columns)))

# station
s_station_df = station_df.select(*(station_df[c].cast(s_station_schema[i].dataType).alias(s_station_schema[i].name) for i, c in enumerate(station_df.columns)))

# rider
s_rider_df = rider_df.select(*(rider_df[c].cast(s_rider_schema[i].dataType).alias(s_rider_schema[i].name) for i, c in enumerate(rider_df.columns)))


# COMMAND ----------

print(s_trip_df.dtypes)
print(s_payment_df.dtypes)
print(s_station_df.dtypes)
print(s_rider_df.dtypes)

# COMMAND ----------

s_trip_df.display()

# COMMAND ----------

payment_df.display()

# COMMAND ----------

station_df.display()

# COMMAND ----------

rider_df.display()

# COMMAND ----------

s_trip_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/trip")
s_payment_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/payment")
s_station_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/station")
s_rider_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/rider")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Gold

# COMMAND ----------

# create a dataframe for the trip table
s_trip_df = spark.read.format("delta").load("/tmp/Rupesh/Silver/trip")

# create a dataframe for the payment table
s_payment_df = spark.read.format("delta").load("/tmp/Rupesh/Silver/payment")

# create a dataframe for the station table
s_station_df = spark.read.format("delta").load("/tmp/Rupesh/Silver/station")

# create a dataframe for the rider table
s_rider_df = spark.read.format("delta").load("/tmp/Rupesh/Silver/rider")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dimension table creation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bike dimension table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


bikes = s_trip_df.select('rideable_type').distinct()

# COMMAND ----------

w = Window.orderBy('rideable_type')
bike_dim = bikes.withColumn('bike_id', F.row_number().over(w)).select('bike_id', 'Rideable_type')

# COMMAND ----------

bike_dim.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Date and Time Dimension tables

# COMMAND ----------

dates1 = s_trip_df.select('started_at').distinct()
dates2 = s_trip_df.select('ended_at').distinct()
dates3 = s_payment_df.select('date').distinct()

# COMMAND ----------

dates3.display()

# COMMAND ----------

dates1.display()

# COMMAND ----------

dates2.display()

# COMMAND ----------

dates2 = dates2.withColumnRenamed('ended_at', 'started_at')

# COMMAND ----------

merged_dates = dates1.union(dates2).distinct()

# COMMAND ----------

merged_dates.display()

# COMMAND ----------

from pyspark.sql.functions import split, col

merged_dates = merged_dates.withColumn('started_at', col('started_at').cast('string'))

merged_dates1 = merged_dates.withColumn('date', split(col('started_at'), ' ')[0])
merged_dates1 = merged_dates1.withColumn('time', split(col('started_at'), ' ')[1].substr(0,5))

# COMMAND ----------

merged_dates1.display()

# COMMAND ----------

times = merged_dates1.select('time').distinct()
w = Window.orderBy('time')
time_dim = times.withColumn('time_id', F.row_number().over(w)).select('time_id', 'time')

# COMMAND ----------

time_dim.display()

# COMMAND ----------

dates4 = merged_dates1.select('date').distinct()
dates4.display()

# COMMAND ----------

dates3.display()

# COMMAND ----------

final_dates = dates4.union(dates3).distinct()
final_dates.display()

# COMMAND ----------

dates = final_dates.select('date').distinct()
w = Window.orderBy('date')
date_dim = dates.withColumn('date_id', F.row_number().over(w)).select('date_id', 'date')

# COMMAND ----------

date_dim.display()

# COMMAND ----------

date_dim = date_dim.withColumn('date', col('date').cast('date'))
date_dim.display()

# COMMAND ----------

date_dim.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payment fact table

# COMMAND ----------

s_payment_df.display()

# COMMAND ----------

payment_fact = s_payment_df.join(date_dim, on='date', how='left').select('payment_id', 'rider_id', 'date_id', 'amount')
payment_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trip fact table

# COMMAND ----------

s_trip_df.display()
s_trip_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip duration

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

trip_fact = s_trip_df.withColumn('trip_duration', (unix_timestamp(col('ended_at')) - unix_timestamp(col('started_at')))/60)
trip_fact.display()
trip_fact.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Bike ID

# COMMAND ----------

trip_fact = trip_fact.join(bike_dim, on='rideable_type', how='left').drop('rideable_type')
trip_fact.display()
trip_fact.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding date and time IDs

# COMMAND ----------

from pyspark.sql.functions import split, col

trip_fact1 = trip_fact.withColumn('started_at', col('started_at').cast('string'))
trip_fact1 = trip_fact1.withColumn('ended_at', col('ended_at').cast('string'))

trip_fact2 = trip_fact1.withColumn('started_at_date', split(col('started_at'), ' ')[0])
trip_fact2 = trip_fact2.withColumn('started_at_time', split(col('started_at'), ' ')[1].substr(0,5))

trip_fact2 = trip_fact2.withColumn('ended_at_date', split(col('ended_at'), ' ')[0])
trip_fact2 = trip_fact2.withColumn('ended_at_time', split(col('ended_at'), ' ')[1].substr(0,5))

trip_fact2 = trip_fact2.drop('started_at', 'ended_at')
trip_fact2.display()

# COMMAND ----------

#join
trip_fact3 = trip_fact2.join(date_dim, trip_fact2.started_at_date == date_dim.date, how='left').withColumnRenamed('date_id', 'started_at_date_id').drop('date')#.drop('started_at_date', 'date')
trip_fact3 = trip_fact3.join(date_dim, trip_fact3.ended_at_date == date_dim.date, how='left').withColumnRenamed('date_id', 'ended_at_date_id').drop('ended_at_date', 'date')

trip_fact3 = trip_fact3.join(time_dim, trip_fact2.started_at_time == time_dim.time, how='left').withColumnRenamed('time_id', 'started_at_time_id').drop('started_at_time', 'time')
trip_fact3 = trip_fact3.join(time_dim, trip_fact2.ended_at_time == time_dim.time, how='left').withColumnRenamed('time_id', 'ended_at_time_id').drop('ended_at_time', 'time')


trip_fact3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding rider age

# COMMAND ----------

s_rider_df.display()

# COMMAND ----------

riders = s_rider_df.select('rider_id', 'birthday')
riders.display()

# COMMAND ----------

trip_fact4 = trip_fact3.join(riders, on='rider_id', how='left')
trip_fact4.display()

# COMMAND ----------

trip_fact4.dtypes

# COMMAND ----------

from pyspark.sql.functions import datediff, col
import math

trip_fact5 = trip_fact4.withColumn('rider_age', (datediff(col('started_at_date'), col('birthday'))/365).cast('int'))
trip_fact5.display()

# COMMAND ----------

final_trip_fact = trip_fact5.select('trip_id', 'rider_id', 'bike_id', 'start_station_id', 'end_station_id', 'started_at_date_id', 'ended_at_date_id', 'started_at_time_id', 'ended_at_time_id', 'rider_age', 'trip_duration')
final_trip_fact.display()

# COMMAND ----------


