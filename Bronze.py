# Databricks notebook source
# MAGIC %run /Repos/rupesh.shrestha@qualyfi.co.uk/StarSchema/SchemaCreation

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

# delete the bronze folder if it exists
dbutils.fs.rm('/tmp/Rupesh/Bronze/', True)

# COMMAND ----------

#create the files in a bronze folder
trip_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/trip")
payment_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/payment")
station_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/station")
rider_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/rider")
