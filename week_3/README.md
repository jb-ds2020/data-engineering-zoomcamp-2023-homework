## Week 3 Homework
<b><u>Important Note:</b></u> <p>You can load the data however you would like, but keep the files in .GZ Format. 
If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You can use the CSV option for the GZ files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the fhv 2019 data. </br>

Rpository to be found here: https://github.com/jb-ds2020/dezoomcamp-week_3/tree/main/week_3_data_warehouse

Solution:
I created a .py file which loads the data into gcs using prefect

`python week_3_data_warehouse/ETL_web_gcs_fhv_taxi.py`

Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). </br>
Solution:

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ringed-enigma-376110.dezoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dezoomcamptaxibucket/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

```

Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

## Question 1:
What is the count for fhv vehicle records for year 2019?
Answer:
- 43,244,696

```sql
SELECT COUNT(*) FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata`;
```

43244696

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

Answer:
- 0 MB for the External Table and 317.94MB for the BQ Table 

```sql
-- Question 2
-- external table
SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata`
-- result 0MB

-- bq table
SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_non_partitioned`
-- result 317.94MB
```

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
Answer:
- 717,748

```sql
-- Question 3
SELECT COUNT(*)
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_non_partitioned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
-- 717748
```

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

Filter by pickup_datetime = Partitioning is good, since we have 365 days for year 2019 which is a suitable size
Order by or aggregate on affiliated_base number = Clustering is good for aggregations and for dimensions we dont know before (Affiliation codes)

- Partition by pickup_datetime Cluster on affiliated_base_number

Solution
```sql
-- Creating a partition (pickup) and cluster (Affiliate_base_number) table
CREATE OR REPLACE TABLE `ringed-enigma-376110.dezoomcamp.fhv_tripdata_partitioned_and_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata`;
```

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

Result: 725 distinct Affiliated_base_numbers

Solution:
```sql
-- a with bq table
SELECT COUNT(DISTINCT Affiliated_base_number) as number_of_distinct_affiliates
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_non_partitioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- result 725

-- with partitioned and clustered table
SELECT COUNT(DISTINCT Affiliated_base_number) as number_of_distinct_affiliates
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_partitioned_and_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- result 725
```

## Question 6: 
Where is the data stored in the External Table you created?

- GCP Bucket

The data remains in the GCP Bucket. I searched for the docu:

"External tables are similar to standard BigQuery tables, in that these tables store their metadata and schema in BigQuery storage. However, their data resides in an external source. External tables are contained inside a dataset, and you manage them in the same way that you manage a standard BigQuery table."

Link: https://cloud.google.com/bigquery/docs/external-data-sources?hl=de#:~:text=External%20tables%20are%20similar%20to,manage%20a%20standard%20BigQuery%20table.


## Question 7:
It is best practice in Big Query to always cluster your data:

- False

Answer: For small datasets (<1gb) clustering has no performance increase and for streaming clustering can increase cost.


## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 

SETUP:
Create an external table using the fhv 2019 data.

Rpository to be found here: https://github.com/jb-ds2020/dezoomcamp-week_3/tree/main/week_3_data_warehouse

Solution: I created a .py file which loads the data into gcs using prefect and before converting it to parquet. I used a date parsing specifier to enable correct time format for pickup and dropoff time.

`python week_3_data_warehouse/ETL_web_gcs_fhv_taxi_parquet.py`

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ringed-enigma-376110.dezoomcamp.fhv_tripdata_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamptaxibucket/data/fhv/fhv_tripdata_2019-*.parquet']
);
```

Query to count rows:
```sql
-- query count on 
SELECT COUNT(Affiliated_base_number)
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_parquet`;

-- result 40'767'583
```

Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 
 
## Submitting the solutions

* Form for submitting: https://forms.gle/rLdvQW2igsAT73HTA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 13 February (Monday), 22:00 CET


## Solution

We will publish the solution here
