# Databricks notebook source
# MAGIC %run /Repos/rupesh.shrestha@qualyfi.co.uk/StarSchema/notebooks/Schemas

# COMMAND ----------

dbutils.fs.rm('/tmp/Rupesh/zipped_files', True)

# COMMAND ----------

!wget 'https://github.com/RupeshS-qualyfi/StarSchema/raw/main/csvs/payments.zip' -P '/dbfs/tmp/Rupesh/zipped_files'
!wget 'https://github.com/RupeshS-qualyfi/StarSchema/raw/main/csvs/riders.zip' -P '/dbfs/tmp/Rupesh/zipped_files'
!wget 'https://github.com/RupeshS-qualyfi/StarSchema/raw/main/csvs/stations.zip' -P '/dbfs/tmp/Rupesh/zipped_files'
!wget 'https://github.com/RupeshS-qualyfi/StarSchema/raw/main/csvs/trips.zip' -P '/dbfs/tmp/Rupesh/zipped_files'


# COMMAND ----------

dbutils.fs.rm('/tmp/Rupesh/landing', True)

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/Rupesh/landing")

# COMMAND ----------

!pip install unzip

# COMMAND ----------

# MAGIC %sh 
# MAGIC unzip /dbfs/tmp/Rupesh/zipped_files/trips.zip -d /dbfs/tmp/Rupesh/landing
# MAGIC unzip /dbfs/tmp/Rupesh/zipped_files/riders.zip -d /dbfs/tmp/Rupesh/landing
# MAGIC unzip /dbfs/tmp/Rupesh/zipped_files/payments.zip -d /dbfs/tmp/Rupesh/landing
# MAGIC unzip /dbfs/tmp/Rupesh/zipped_files/stations.zip -d /dbfs/tmp/Rupesh/landing

# COMMAND ----------

# create a dataframe for the trip table
trip_df = spark.read.csv("/tmp/Rupesh/landing/trips.csv", schema = b_trip_schema)

# create a dataframe for the payment table
payment_df = spark.read.csv("/tmp/Rupesh/landing/payments.csv", schema = b_payment_schema)

# create a dataframe for the station table
station_df = spark.read.csv("/tmp/Rupesh/landing/stations.csv", schema = b_station_schema)

# create a dataframe for the rider table
rider_df = spark.read.csv("/tmp/Rupesh/landing/riders.csv", schema = b_rider_schema)

# COMMAND ----------



# COMMAND ----------

#create the files in a bronze folder
trip_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/trip")
payment_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/payment")
station_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/station")
rider_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/rider")

# COMMAND ----------


