# Databricks notebook source
# MAGIC %md # Create empty dataframes with schemas

# COMMAND ----------

# MAGIC %run /Repos/rupesh.shrestha@qualyfi.co.uk/StarSchema/Schemas

# COMMAND ----------

# MAGIC %md ## Bronze

# COMMAND ----------

# create empty df
from pyspark.sql.types import *

df = sc.emptyRDD()

trip_df = spark.createDataFrame(df, b_trip_schema)

payment_df = spark.createDataFrame(df, b_payment_schema)

station_df = spark.createDataFrame(df, b_station_schema)

rider_df = spark.createDataFrame(df, b_rider_schema)

# COMMAND ----------

#create the empty files in a bronze folder
trip_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/trip")
payment_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/payment")
station_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/station")
rider_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/rider")

# COMMAND ----------

# MAGIC %md ## Silver

# COMMAND ----------

# create empty df
from pyspark.sql.types import *

df = sc.emptyRDD()

s_trip_df = spark.createDataFrame(df, s_trip_schema)

s_payment_df = spark.createDataFrame(df, s_payment_schema)

s_station_df = spark.createDataFrame(df, s_station_schema)

s_rider_df = spark.createDataFrame(df, s_rider_schema)

# COMMAND ----------

# write to silver
s_trip_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/trip")
s_payment_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/payment")
s_station_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/station")
s_rider_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/rider")

# COMMAND ----------

# MAGIC %md ## Gold

# COMMAND ----------

# create empty df
from pyspark.sql.types import *

df = sc.emptyRDD()

trip_fact = spark.createDataFrame(df, g_trip_schema)
payment_fact = spark.createDataFrame(df, g_payment_schema)

bike_dim = spark.createDataFrame(df, g_bike_schema)
date_dim = spark.createDataFrame(df, g_date_schema)
time_dim = spark.createDataFrame(df, g_time_schema)
rider_dim = spark.createDataFrame(df, g_rider_schema)
station_dim = spark.createDataFrame(df, g_station_schema)

# COMMAND ----------

trip_fact.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Gold/fact_trip")
payment_fact.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Gold/fact_payment")

bike_dim.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Gold/dim_bike")
date_dim.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Gold/dim_date")
time_dim.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Gold/dim_time")
rider_dim.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Gold/dim_rider")
station_dim.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Gold/dim_station")

# COMMAND ----------


