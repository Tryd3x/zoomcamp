#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession, functions as F
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True) # Path to read green data
parser.add_argument('--input_yellow', required=True) # PAth to read yellow data
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = (
    SparkSession.builder
    .appName('test')
    .getOrCreate()
)

spark.conf.set('temporaryGcsBucket','dataproc-temp-us-central1-97567432058-efnmbde0')

# Spark reads subfolders so you can pass `/*/*` which is trying to read all years and months of the folders
# "../datasets/pq/green/*/*"
df_green = spark.read.parquet(input_green)

# Rename df_green columns
df_green = df_green \
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

# "../datasets/pq/yellow/*/*"
df_yellow = spark.read.parquet(input_yellow)

# Rename df_yellow columns
df_yellow = df_yellow \
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

# Find common attributes in both yellow and green (Unordered)
common_columns = set(df_green.columns).intersection(set(df_yellow.columns)) 
# common_col = set(df_green.columns) & set(df_yellow.columns)

# Find common attributes  in both yellow and green (Ordered)
common_columns = []
yellow_columns = set(df_yellow.columns)
for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)


# Since we are merging two df, we would like to know where each record originates from, so we create a column for it
df_green_sel = df_green \
    .select(common_columns) \
    .withColumn("service_type", F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn("service_type", F.lit('yellow'))

# Merging both dataframes
df_trips_data = df_green_sel.unionAll(df_yellow_sel)

# Performing count aggregation using groupby
df_trips_data.groupBy('service_type').count().show()

# In order to perform sql on dataframes, register the datafram as view
df_trips_data.createOrReplaceTempView('trips_data')

# # Using sql in spark
# spark.sql(
# """
# SELECT 
#     service_type
#     , count(*) as cnt
# FROM 
#     trips_data
# GROUP BY
#     service_type
# """).show()

df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

# Reducing the # of partitions to 1 using colaesce (prevents shuffling)
# '../datasets/report/revenue/'
# df_result.coalesce(1).write.parquet(output, mode='overwrite')

(df_result
    .write
    .format('bigquery')
    .option('table', output)
    .save()
)

