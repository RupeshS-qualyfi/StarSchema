# Databricks notebook source
#Creating the schemas for Bronze, Silver, Gold

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

## Bronze Schema

# COMMAND ----------

# define the schema for the trip table
b_trip_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("rider_id", StringType(), True)
])

# define the schema for the payment table
b_payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("rider_id", StringType(), True)
])

# define the schema for the station table
b_station_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True)
])

# define the schema for the rider table
b_rider_schema = StructType([
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

## Silver Schema

# COMMAND ----------

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

## Gold Schema

# COMMAND ----------

g_trip_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("rider_id", IntegerType(), True),
    StructField("bike_id", IntegerType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("started_at_date_id", IntegerType(), True),
    StructField("ended_at_date_id", IntegerType(), True),
    StructField("started_at_time_id", IntegerType(), True),
    StructField("ended_at_time_id", IntegerType(), True),
    StructField("rider_age", IntegerType(), True),
    StructField("trip_duration", IntegerType(), True)
])

g_payment_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("rider_id", IntegerType(), True),
    StructField("date_id", IntegerType(), True),
    StructField("amount", FloatType(), True)
])
g_rider_schema = StructType([
    StructField("rider_id", IntegerType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", DateType(), True),
    StructField("account_start", DateType(), True),
    StructField("account_end", DateType(), True),
    StructField("is_member", BooleanType(), True)
])

g_station_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True)
])

g_bike_schema = StructType([
    StructField("bike_id", IntegerType(), True),
    StructField("rideable_type", StringType(), True)  
])

g_date_schema = StructType([
    StructField("date_id", IntegerType(), True),
    StructField("date", DateType(), True)    
])

g_time_schema = StructType([
    StructField("time_id", IntegerType(), True),
    StructField("time", StringType(), True)    
])

# COMMAND ----------


