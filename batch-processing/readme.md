## Datasets

Download files into `datasets` folder:

```
wget -P ../datasets https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
wget -P ../datasets https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz
```

## Scripts

- `download_data.sh`: To download parquet data for taxi type  
  Run:
  ```
  ./download_data.sh <taxi_type> <year>
  ```

  Parameters:
  ```
  <taxi_type>:  ['green', 'yellow']  
  <year>:       [2020, 2021]
  ```

- `04_spark_sql_master.py`: To run spark script on master node for given year (2020) 
  Run:
  ```
  python 04_spark_sql_master.py \
    --input_green='../datasets/pq/green/2020/*' \
    --input_yellow='../datasets/pq/yellow/2020/*' \
    --output='../datasets/report/revenue/2020'
  ```

- `spark-submit`: To submit a python script as a spark job with additional configuration for the spark cluster
  ```
  URL=spark://Trydex.:7077
  spark-submit \
    --master="${URL}" \
    04_spark_sql.py \
      --input_green='../datasets/pq/green/2021/*' \
      --input_yellow='../datasets/pq/yellow/2021/*' \
      --output='../datasets/report/revenue/2021'
  ```

  **Note:** Obtain `URL` by navigating to `$SPARK_HOME/sbin` and executing `./start-master.sh`. Open browser to `localhost:8080` and copy the Master URL