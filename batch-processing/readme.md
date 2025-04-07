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


## Dataproc

  - To submit spark job on dataproc, modify the arguments for `04_spark_sql.py`  
  
    Arguments:  
    ```
      --input_green=gs://zoomcamp-454219-nytaxi/pq/green/2021/*/
      --input_yellow=gs://zoomcamp-454219-nytaxi/pq/yellow/2021/*/
      --output=gs://zoomcamp-454219-nytaxi/report-2021
    ```

  - To submit spark job on dataproc via gcloud  

    Run:
    ```
    gcloud dataproc jobs submit 
      pyspark \
      --cluster=cluster-f19a \
      --region=us-central1 \
      gs://zoomcamp-454219-nytaxi/code/dataproc_spark_sql.py \
      -- \
        --input_green=gs://zoomcamp-454219-nytaxi/pq/green/2021/*/ \
        --input_yellow=gs://zoomcamp-454219-nytaxi/pq/yellow/2021/*/ \
        --output=gs://zoomcamp-454219-nytaxi/data/report-2021
    ```

    Arguments:
    ```
    --cluster: name of the cluster created on gcp
    gs://zoomcamp-454219-nytaxi/code/dataproc_spark_sql.py: script to execute as spark job
    -- <script arguments>
    ```
    Note: Make sure to copy `dataproc_spark_sql.py` to `gs://zoomcamp-454219-nytaxi/code`
    ```
      gsutil -m cp -r dataproc_spark_sql.py gs://zoomcamp-454219-nytaxi/code/dataproc_spark_sql.py
    ```

  - To submit spark job on dataproc to store results in bigquery table `trips_data_all.reports-2020`  
    Run:  
    ```
    gcloud dataproc jobs submit \
      pyspark \
      --cluster=cluster-f19a \
      --region=us-central1 \
      gs://zoomcamp-454219-nytaxi/code/04_spark_sql_bigquery.py \
      -- \
        --input_green=gs://zoomcamp-454219-nytaxi/pq/green/2021/*/ \
        --input_yellow=gs://zoomcamp-454219-nytaxi/pq/yellow/2021/*/ \
        --output=de_zoomcamp.reports-2021
    ```
    Note:  
    - Inside `04_spark_sql_bigquery.py`, set `bucket` to one of the temp buckets created by dataproc cluster
    ```
    spark.conf.set('temporaryGcsBucket', bucket)
    ```
    - Make sure to copy `04_spark_sql_bigquery.py` to `gs://zoomcamp-454219-nytaxi/code`
    ```
      gsutil -m cp -r 04_spark_sql_bigquery.py gs://zoomcamp-454219-nytaxi/code/04_spark_sql_bigquery.py
    ```


