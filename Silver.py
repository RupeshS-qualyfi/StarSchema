# Databricks notebook source
# MAGIC %run /Repos/rupesh.shrestha@qualyfi.co.uk/StarSchema/SchemaCreation

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col

# COMMAND ----------

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

# trip
s_trip_df = trip_df.select(*(trip_df[c].cast(s_trip_schema[i].dataType).alias(s_trip_schema[i].name) for i, c in enumerate(trip_df.columns)))

# payment
s_payment_df = payment_df.select(*(payment_df[c].cast(s_payment_schema[i].dataType).alias(s_payment_schema[i].name) for i, c in enumerate(payment_df.columns)))

# station
s_station_df = station_df.select(*(station_df[c].cast(s_station_schema[i].dataType).alias(s_station_schema[i].name) for i, c in enumerate(station_df.columns)))

# rider
s_rider_df = rider_df.select(*(rider_df[c].cast(s_rider_schema[i].dataType).alias(s_rider_schema[i].name) for i, c in enumerate(rider_df.columns)))

# COMMAND ----------

# delete the Silver folder if it exists
dbutils.fs.rm('/tmp/Rupesh/Silver/', True)

# COMMAND ----------

# write to silver
s_trip_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/trip")
s_payment_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/payment")
s_station_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/station")
s_rider_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Silver/rider")
