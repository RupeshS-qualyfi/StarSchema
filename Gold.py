# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import split, col, unix_timestamp, datediff

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

# Dimension table creation

# COMMAND ----------

## Bike dimension table
def make_bike_dimension():
    bikes = s_trip_df.select('rideable_type').distinct()
    w = Window.orderBy('rideable_type')
    bike_dim = bikes.withColumn('bike_id', F.row_number().over(w)).select('bike_id', 'Rideable_type')
    return bike_dim

# COMMAND ----------

## Date and Time Dimension tables
def make_date_dimension():
    # get all dates possible
    dates1 = s_trip_df.select('started_at').distinct()
    dates2 = s_trip_df.select('ended_at').distinct()
    dates3 = s_payment_df.select('date').distinct()
    
    dates2 = dates2.withColumnRenamed('ended_at', 'started_at')
    merged_dates = dates1.union(dates2).distinct()
    merged_dates = merged_dates.withColumn('started_at', col('started_at').cast('string'))

    merged_dates1 = merged_dates.withColumn('date', split(col('started_at'), ' ')[0])
    merged_dates1 = merged_dates1.withColumn('time', split(col('started_at'), ' ')[1].substr(0,5))
    
    dates4 = merged_dates1.select('date').distinct()
    final_dates = dates4.union(dates3).distinct()
    
    dates = final_dates.select('date').distinct()
    w = Window.orderBy('date')
    date_dim = dates.withColumn('date_id', F.row_number().over(w)).select('date_id', 'date')
    date_dim = date_dim.withColumn('date', col('date').cast('date'))
    
    return date_dim

# COMMAND ----------

def make_time_dimension():
    # get all dates possible
    dates1 = s_trip_df.select('started_at').distinct()
    dates2 = s_trip_df.select('ended_at').distinct()
    dates3 = s_payment_df.select('date').distinct()
    
    dates2 = dates2.withColumnRenamed('ended_at', 'started_at')
    merged_dates = dates1.union(dates2).distinct()
    merged_dates = merged_dates.withColumn('started_at', col('started_at').cast('string'))

    merged_dates1 = merged_dates.withColumn('date', split(col('started_at'), ' ')[0])
    merged_dates1 = merged_dates1.withColumn('time', split(col('started_at'), ' ')[1].substr(0,5))
    
    times = merged_dates1.select('time').distinct()
    w = Window.orderBy('time')
    time_dim = times.withColumn('time_id', F.row_number().over(w)).select('time_id', 'time')
    
    return time_dim

# COMMAND ----------

## Payment fact table
def make_payment_fact():
    payment_fact = s_payment_df.join(date_dim, on='date', how='left').select('payment_id', 'rider_id', 'date_id', 'amount')
    return payment_fact

# COMMAND ----------

def make_trip_fact(bike_dim, date_dim, time_dim):
    # trip duration
    trip_fact = s_trip_df.withColumn('trip_duration', (unix_timestamp(col('ended_at')) - unix_timestamp(col('started_at')))/60)
    
    # bike ID
    trip_fact = trip_fact.join(bike_dim, on='rideable_type', how='left').drop('rideable_type')
    
    # Adding date and time IDs
    trip_fact1 = trip_fact.withColumn('started_at', col('started_at').cast('string'))
    trip_fact1 = trip_fact1.withColumn('ended_at', col('ended_at').cast('string'))

    trip_fact2 = trip_fact1.withColumn('started_at_date', split(col('started_at'), ' ')[0])
    trip_fact2 = trip_fact2.withColumn('started_at_time', split(col('started_at'), ' ')[1].substr(0,5))

    trip_fact2 = trip_fact2.withColumn('ended_at_date', split(col('ended_at'), ' ')[0])
    trip_fact2 = trip_fact2.withColumn('ended_at_time', split(col('ended_at'), ' ')[1].substr(0,5))

    trip_fact2 = trip_fact2.drop('started_at', 'ended_at')


    #join
    trip_fact3 = trip_fact2.join(date_dim, trip_fact2.started_at_date == date_dim.date, how='left').withColumnRenamed('date_id', 'started_at_date_id').drop('date')#.drop('started_at_date', 'date')
    trip_fact3 = trip_fact3.join(date_dim, trip_fact3.ended_at_date == date_dim.date, how='left').withColumnRenamed('date_id', 'ended_at_date_id').drop('ended_at_date', 'date')

    trip_fact3 = trip_fact3.join(time_dim, trip_fact2.started_at_time == time_dim.time, how='left').withColumnRenamed('time_id', 'started_at_time_id').drop('started_at_time', 'time')
    trip_fact3 = trip_fact3.join(time_dim, trip_fact2.ended_at_time == time_dim.time, how='left').withColumnRenamed('time_id', 'ended_at_time_id').drop('ended_at_time', 'time')
    
    ## Adding rider age
    riders = s_rider_df.select('rider_id', 'birthday')
    trip_fact4 = trip_fact3.join(riders, on='rider_id', how='left')
    trip_fact5 = trip_fact4.withColumn('rider_age', (datediff(col('started_at_date'), col('birthday'))/365).cast('int'))
    
    trip_fact = trip_fact5.select('trip_id', 'rider_id', 'bike_id', 'start_station_id', 'end_station_id', 'started_at_date_id', 'ended_at_date_id', 'started_at_time_id', 'ended_at_time_id', 'rider_age', 'trip_duration')
    trip_fact = trip_fact.withColumn('trip_duration', col('trip_duration').cast('int'))
    
    return trip_fact

# COMMAND ----------

b = make_bike_dimension()
d = make_date_dimension()
t = make_time_dimension()
t = make_trip_fact(b, d, t)
t.display()

# COMMAND ----------


