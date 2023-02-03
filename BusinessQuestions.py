# Databricks notebook source
trip_fact = spark.read.format("delta").load("/tmp/Rupesh/Gold/fact_trip")
payment_fact = spark.read.format("delta").load("/tmp/Rupesh/Gold/fact_payment")

bike_dim = spark.read.format("delta").load("/tmp/Rupesh/Gold/dim_bike")
date_dim = spark.read.format("delta").load("/tmp/Rupesh/Gold/dim_date")
time_dim = spark.read.format("delta").load("/tmp/Rupesh/Gold/dim_time")
rider_dim = spark.read.format("delta").load("/tmp/Rupesh/Gold/dim_rider")
station_dim = spark.read.format("delta").load("/tmp/Rupesh/Gold/dim_station")

# COMMAND ----------

## Analyse how much time is spent per ride

# COMMAND ----------

##  Based on date and time factors such as day of week and time of day
def time_per_ride_day_of
