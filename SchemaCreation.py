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


