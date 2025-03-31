Download dataset into `datasets` folder:

```
wget -P ../datasets https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
wget -P ../datasets https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz
```

Use `download_data.sh <taxi_type> <year>` to download parquet data for taxi type.




types.StructType([
    types.StructField('VendorID', types.LongType(), True), 
    types.StructField('tpep_pickup_datetime', types.TimestampNTZType(), True), 
    types.StructField('tpep_dropoff_datetime', types.TimestampNTZType(), True), 
    types.StructField('passenger_count', types.DoubleType(), True), 
    types.StructField('trip_distance', types.DoubleType(), True), 
    types.StructField('RatecodeID', types.DoubleType(), True), 
    types.StructField('store_and_fwd_flag', types.StringType(), True), 
    types.StructField('PULocationID', types.LongType(), True), 
    types.StructField('DOLocationID', types.LongType(), True), 
    types.StructField('payment_type', types.LongType(), True), 
    types.StructField('fare_amount', types.DoubleType(), True), 
    types.StructField('extra', types.DoubleType(), True), 
    types.StructField('mta_tax', types.DoubleType(), True), 
    types.StructField('tip_amount', types.DoubleType(), True), 
    types.StructField('tolls_amount', types.DoubleType(), True), 
    types.StructField('improvement_surcharge', types.DoubleType(), True), 
    types.StructField('total_amount', types.DoubleType(), True), 
    types.StructField('congestion_surcharge', types.DoubleType(), True), 
    types.StructField('airport_fee', types.IntegerType(), True)
])