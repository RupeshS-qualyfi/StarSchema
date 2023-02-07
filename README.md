# StarSchema

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
   ><img src="https://raw.githubusercontent.com/steviedas/StarSchemaProject/main/pictures/ConceptualDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540">
   ></p>
   >
   
   </details>

   <details>
   <summary>Logical Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/steviedas/StarSchemaProject/main/pictures/LogicalDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540"
   ></p>
   >
   
   </details>
  
   <details>
   <summary>Physical Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/steviedas/StarSchemaProject/main/pictures/PhysicalDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540"
   ></p>
   >
   
   </details>
