# StarSchema

![GitHub last commit](https://img.shields.io/github/last-commit/RupeshS-qualyfi/StarSchema)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/RupeshS-qualyfi/StarSchema)

[Intro](#introduction)
[Design](#designing-the-schema)
[Method](#methododology)
[Business Outcomes](#business-outcomes)
[Automated Tests](#automated-tests)



## Introduction

This project consists of using data from a bike sharing program and converting it into a Star Schema database format which allows anyone accessing the database to query the data efficiently to answer a variety of business questions. The bike sharing program allows costumers to rent a bike at any station owned by the business and ride the bike around the city for an amount of time before returning the bike at any station (the same or different).

The star schema was designed using 4 raw csv files which were stored as seperate .zip files in the folder `csvs` as shown on the below table:

.zip          | .csv
------------- | -------------
payments.zip  | payments.csv
riders.zip    | riders.csv
stations.zip  | stations.csv
trips.zip     | trips.csv


## Designing the Schema
The schema was designed by the following three steps: 

   <details>
   <summary>Conceptual Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/RupeshS-qualyfi/StarSchema/main/pictures/ConceptualDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540">
   ></p>
   >
   
   </details>

   <details>
   <summary>Logical Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/RupeshS-qualyfi/StarSchema/main/pictures/LogicalDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540"
   ></p>
   >
   
   </details>
  
   <details>
   <summary>Physical Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/RupeshS-qualyfi/StarSchema/main/pictures/PhysicalDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540"
   ></p>
   >
   
   </details>
   
   ## Methodology
   
   The whole workflow consisted of 5 seperate notebooks contained in the folder 'notebooks', each of which is described as follows:
   
   ### 1_DestroySchemas.py
   
   This notebook simply destroys any database already located in the DBFS path specified:
   ```python
   dbutils.fs.rm("/tmp/Rupesh/", True)
   ```
   
   ### 2_SchemaCreation.py
   
   Each of the schemas for the bronze, silver and gold medallion which are declared in the notebook 'Schemas.py' are read and an empty database was created for each and stored in their respective containers. 
   
   ### 3_Bronze.py
   
   This notebook first gets the zipped files from the github repo and stored them in the DBFS folder 'zipped_files' using '!wget' as follows
   
   ```python
   !wget 'https://github.com/RupeshS-qualyfi/StarSchema/raw/main/csvs/payments.zip' -P '/dbfs/tmp/Rupesh/zipped_files'
   ```
   
   Then it creates a 'landing' folder for the '.csv' files to be extracted into as follows
   
   ```python
   unzip /dbfs/tmp/Rupesh/zipped_files/trips.zip -d /dbfs/tmp/Rupesh/landing
   ```
   
   It then creates the files in the bronze folder after reading the '.csv' files and applying the schemas from the 'Schemas.py' file. 
   
   ```python
   trip_df = spark.read.csv("/tmp/Rupesh/landing/trips.csv", schema = b_trip_schema)
   trip_df.write.format("delta").mode("overwrite").save("/tmp/Rupesh/Bronze/trip")
   ```
   
   ### 4_Silver.py
   
   This notebook follows a similar structure to '3_bronze.py' and stores the new delta tables with the silver schema in the silver folder.
   
   ### 5_Gold.py
   
   This notebook creates all the fact and dimension tables shown in the physical database design shown above and stores them as delta tables in the Gold folder. 
   
   ## Business Outcomes
   
   After the whole cvhema has been applied, the business questions that the 'BusinessQuestions.py' notebook answers are as follows: 
   
   Q1) Analyse how much time is spent per ride:
* a) Based on date and time factors such as day of week and time of day
* b) Based on which station is the starting station and ending station
* c) Based on age of the rider at time of the ride
* d) Based on whether the rider is a member or a casual rider

Q2) Analyse how much money is spent:
* a) Per month, quarter, year
* b) Per member, based on the age of the rider at account start

Q3) EXTRA CREDIT - Analyse how much money is spent per member:
* a) Based on how many rides the rider averages per month
* b) Based on how many minutes the rider spends on a bike per month
   
   ## Automated Tests
   
   The notebook 'Assertions.py' asserts that everything was done correctly by checking the coucomes of the 'BusinessQuestions.py' notebook. It firstly checks that the gold schemas are applied correctly using the following function: 
   
   ```python
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
   ```
   
  Then it has different asserts for each of the business questions that were answered.

   
