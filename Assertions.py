# Databricks notebook source
# MAGIC %md # Assertions

# COMMAND ----------

# MAGIC %run /Repos/rupesh.shrestha@qualyfi.co.uk/StarSchema/BusinessQuestions

# COMMAND ----------

# MAGIC %md ## Assert Schemas

# COMMAND ----------

# MAGIC %run /Repos/rupesh.shrestha@qualyfi.co.uk/StarSchema/Schemas

# COMMAND ----------

def assert_gold_schemas():
    assert trip_fact.schema == g_trip_schema, 'trip fact schema mismatch'
    assert payment_fact.schema == g_payment_schema, 'payment fact schema mismatch'
    
    assert bike_dim.schema == g_bike_schema, 'bike dimension schema mismatch'
    assert date_dim.schema == g_date_schema, 'date dimension schema mismatch'
    assert time_dim.schema == g_time_schema, 'time dimension schema mismatch'
    assert rider_dim.schema == g_rider_schema, 'rider dimension schema mismatch'
    assert station_dim.schema == g_station_schema, 'station dimension schema mismatch'
    return 0
    
assert_gold_schemas()

# COMMAND ----------

# MAGIC %md ## Assert Business Questions

# COMMAND ----------

# MAGIC %md #### 1a. Analyse how much time is spent per ride based on date and time factors such as day of week and time of day

# COMMAND ----------

def assert_1a():
    one_a_1 = time_per_ride_day_of_week()
    one_a_2 = time_per_ride_hour()
    ## assert 7 rows, one for each day of the week
    assert one_a_1.count() <= 7, 'Have more than 7 days in day of the week df'
    assert one_a_2.count() <= 24, 'Have more than 24 hours in the time of day df'
    return 0
   
assert_1a()

# COMMAND ----------

# MAGIC %md #### 1b. Analyse how much time is spent per ride based on which station is the starting and / or ending station

# COMMAND ----------

def assert_1b():
    one_b_1 = time_per_start_station()
    one_b_2 = time_per_end_station()
    
    assert one_b_1.count() == 74, 'length of start df wrong'
    assert one_b_2.count() == 67, 'length of end df wrong'
    
    return 0
assert_1b()

# COMMAND ----------

# MAGIC %md #### 1c. Analyse how much time is spent per ride based on age of the rider at time of the ride

# COMMAND ----------

def assert_1c():
    one_c = time_per_age()
    max_age = one_c.agg({'rider_age': 'max'}).collect()[0][0]
    assert max_age <= 71, 'oldest rider is older than 71 which is false'
    return 0

assert_1c()

# COMMAND ----------

# MAGIC %md #### 1d. Analyse how much time is spent per ride based on whether the rider is a member or a casual rider

# COMMAND ----------

def assert_1d():
    df = time_per_membership()
    assert df.count() == 2, 'there are more than 2 rows which is wrong, (true, false) only'
    return 0
  
assert_1d()

# COMMAND ----------

# MAGIC %md #### 2a. Analyse how much money is spent per month, quarter, year

# COMMAND ----------

def assert_2a():
    df_m = money_per_month()
    df_q = money_per_quarter()
    df_y = money_per_year()
    
    assert df_m.count() <=12, 'there are more than 12 months'
    assert df_q.count() == 4, 'there should only be 4 quarters'
    assert int(df_y.agg({'year': 'max'}).collect()[0][0]) <= 2022, 'the year goes past 2022'
    
    return 0
    
assert_2a()

# COMMAND ----------

# MAGIC %md #### 2b. Analyse how much money is spent per member based on age of rider at account start

# COMMAND ----------

def assert_2b():
    df = money_per_member()
    assert df.agg({'age_at_start': 'min'}).collect()[0][0] == 7, 'the minimum age in the table is wrong'
    return 0

assert_2b()

# COMMAND ----------

# MAGIC %md #### EXTRA CREDIT 3a. Analyze how much money is spent per member based on how many rides the rider averages per month

# COMMAND ----------

def assert_3a():
    df = money_based_on_rides()
    assert df.agg({'number_of_rides': 'max'}).collect()[0][0] == 2, 'the number of rides maximum value is wrong'
    return 0

assert_3a()

# COMMAND ----------

# MAGIC %md #### EXTRA CREDIT 3b. Analyze how much money is spent per member based on how many minutes the rider spends on a bike per month

# COMMAND ----------

def assert_3b():
    df = money_based_on_minutes()
    assert df.agg({'sum_amount_till_now':'max'}).collect()[0][0] == 927, 'the maximum value for the sum of the amount spent is wrong'
    return 0

assert_3b()

# COMMAND ----------


