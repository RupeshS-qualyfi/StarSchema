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
from pyspark.sql.functions import date_format, datediff
from pyspark.sql.functions import col, avg, sum, round, quarter
from pyspark.sql.functions import hour
import math

# COMMAND ----------

##  Based on date and time factors such as day of week and time of day
## day of week
def time_per_ride_day_of_week():
    dow = trip_fact.select('trip_duration', 'started_at_date_id')
    dow = dow.join(date_dim, dow.started_at_date_id == date_dim.date_id, how='inner').drop('started_at_date_id')
    dow = dow.withColumn('date', date_format(col('date'), 'EEEE'))
    dow_grouped = dow.groupBy('date').agg(avg('trip_duration'))
    dow_grouped = dow_grouped.withColumnRenamed('date', 'day_of_week')
    return dow_grouped

# COMMAND ----------

## time of day by the hour
def time_per_ride_hour():
    hr = trip_fact.select('trip_duration', 'started_at_time_id')
    hr = hr.join(time_dim, hr.started_at_time_id == time_dim.time_id, how='inner').drop('started_at_time_id')
    hr = hr.withColumn('time', hour(col('time')))
    hr_grouped = hr.groupBy('time').agg(avg('trip_duration'))
    hr_grouped = hr_grouped.withColumnRenamed('time', 'hour_of_day').withColumnRenamed('avg(trip_duration)', 'average_trip_duration_minutes')
    return hr_grouped

# COMMAND ----------

## Based on which station is the starting and / or ending station
def time_per_start_station():
    stns = trip_fact.select('trip_duration', 'start_station_id', 'end_station_id')
    start_stns = stns.groupBy('start_station_id').agg(avg('trip_duration'))
    start_stns = start_stns.join(station_dim, start_stns.start_station_id == station_dim.station_id, how='left').select('name', 'avg(trip_duration)').withColumnRenamed('name', 'start_station')
    return start_stns

# COMMAND ----------

def time_per_end_station():
    stns = trip_fact.select('trip_duration', 'start_station_id', 'end_station_id')
    end_stns = stns.groupBy('end_station_id').agg(avg('trip_duration'))
    end_stns = end_stns.join(station_dim, end_stns.end_station_id == station_dim.station_id, how='left').select('name', 'avg(trip_duration)').withColumnRenamed('name', 'end_station')
    return end_stns

# COMMAND ----------

## Based on age of the rider at time of the ride
def time_per_age():
    age = trip_fact.groupBy('rider_age').agg(avg('trip_duration'))
    return age

# COMMAND ----------

## Based on whether the rider is a member or a casual rider
def time_per_membership():
    member = trip_fact.join(rider_dim, on='rider_id', how='left').select('trip_duration', 'is_member')
    member_grouped = member.groupBy('is_member').agg(avg('trip_duration'))
    return member_grouped

# COMMAND ----------

## Analyse how much money is spent

# COMMAND ----------

## per month
def money_per_month():
    per_month = payment_fact.join(date_dim, on='date_id', how='left').select('amount', 'date')
    per_month1 = per_month.withColumn('month', date_format(per_month['date'], 'MMMM'))
    per_month_grouped = per_month1.groupBy('month').agg(sum('amount'))
    per_month_grouped = per_month_grouped.withColumn('sum(amount)', round(col('sum(amount)'), 2))
    return per_month_grouped

# COMMAND ----------

## per quarter
def money_per_quarter():
    per_month = payment_fact.join(date_dim, on='date_id', how='left').select('amount', 'date')
    perQ = per_month.withColumn('quarter', quarter(col('date')))
    perQ = perQ.groupBy('quarter').agg(sum('amount'))
    perQ = perQ.withColumn('sum(amount)', round(col('sum(amount)'), 2))
    return perQ

# COMMAND ----------

## per year
def money_per_year():
    per_month = payment_fact.join(date_dim, on='date_id', how='left').select('amount', 'date')
    py = per_month.withColumn('year', date_format(per_month['date'], 'yyyy'))
    py_grouped = py.groupBy('year').agg(sum('amount'))
    py_grouped = py_grouped.withColumn('sum(amount)', round(col('sum(amount)'), 2))
    return py_grouped

# COMMAND ----------

## per member based on age of rider at account start
def money_per_member():
    ages = rider_dim.withColumn('age_at_start', (datediff(col('account_start'), col('birthday'))/365).cast('int')).select('rider_id', 'age_at_start')
    ar = payment_fact.join(ages, on='rider_id', how='left').select('rider_id', 'age_at_start', 'amount')
    ar_grouped = ar.groupBy('age_at_start').agg(sum('amount'))
    ar_grouped = ar_grouped.withColumn('sum(amount)', round(col('sum(amount)'), 2))
    return ar_grouped

# COMMAND ----------



# COMMAND ----------


