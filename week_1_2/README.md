# 1_terraform_gcp

## Question 1. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

They way to reach the goal of applying terraform on the google cloud environtment:

- follow the videos for terraform:
https://youtu.be/Hajwnmj0xfQ & https://youtu.be/dNkEgO-CExg

- follow the video for gcp setup and terraform:
https://youtu.be/ae-CV2KfoN0

After finishing the whole setup on the virtual machine and cloning the course repo I used the following command line commands to reach the goal:

```
terraform init

terraform plan 

terraform apply

terraform destroy
```

After the `terraform apply` I copied the output to the homework form.


# 2_docker_sql

## Question 1. Knowing docker tags

Which tag has the following text? - *Write the image ID to the file* 


```docker build --help```

Solution: "--iidfile string"


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.

```docker run -it --entrypoint /bin/bash python:3.9```

Solution: 3 packages

# Prepare Postgres

Run Postgres and load data as shown in the videos. 

Using the combined docker image and build by using the included docker-compose.yaml file and then use:

```docker-compose up```

Insert the green taxi data by:
```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

python ingest_data_green.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=zones \
  --url=${URL}
```

Insert the zones by using:

```
URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

python ingest_data_zones.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=zones \
  --url=${URL}
```

Afterwards the column names were updated to lowercase and _ with the pgadmin tool.

## Question 3. Count records 

How many taxi trips were totally made on January 15?
```sql
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-01-15 00:00:00' 
AND lpep_pickup_datetime < '2019-01-16 00:00:00'
AND lpep_dropoff_datetime >= '2019-01-15 00:00:00'
AND lpep_dropoff_datetime < '2019-01-16 00:00:00';
```

-- 20530 rides Sharing and finishing on the 15th of January 2019

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

```sql
SELECT
lpep_pickup_datetime,
trip_distance
FROM green_taxi_trips
ORDER  BY trip_distance DESC
LIMIT 10;
```

-- 2019-01-15 with a distance of 117.99

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?

```sql
SELECT
passenger_count,
COUNT(*)
FROM green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-01'
GROUP BY passenger_count
ORDER BY passenger_count;
```

-- 2: 1282 ; 3: 254


## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

```sql
SELECT
green_taxi_trips.tip_amount,
green_taxi_trips.pu_location_id as pu_id,
green_taxi_trips.do_location_id as du_id,
zones.zone as pickup_zone,
zones2.zone as dropoff_zone
FROM green_taxi_trips
INNER JOIN zones
ON green_taxi_trips.pu_location_id = zones.location_id
INNER JOIN zones as zones2
ON green_taxi_trips.do_location_id = zones2.location_id
WHERE zones.zone = 'Astoria'
ORDER BY tip_amount DESC;
```

-- 88 tip from Astorie to Long Island City /  Queens Plaza
