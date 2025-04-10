## Docker Container Setup

To spin up all containers below at once  
```
docker compose up
```

**I. Postgres Container**
- Setup to create a volume and network followed by running a postgres container 

  ```
  docker volume create pg_data
  docker network create pg-network
  docker run -it \
    -e POSTGRES_USER=root \
    -e POSTGRES_PASSWORD=root \
    -e POSTGRES_DB='ny_taxi' \
    -v pg_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name=postgres-1 \
  postgres:13
  ```

**II. Pgadmin Container**
- Setup to create and run a pgAdmin container

  ```
  docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name=pgadmin-1 \
    dpage/pgadmin4
  ```  

**III. Ingest Container**
- Setup to run ingest_data.py inside the docker container

  ```
  URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
  docker run -it --name=taxi_ingest-1 --network=pg-network taxi_ingest \
      --user=root \
      --password=root \
      --host=postgres-1 \
      --port=5432 \
      --db=ny_taxi \
      --table_name=yellow_taxi_data \
      --url=${URL}
  ```

## Kestra Container Setup

To spin up all containers below at once, navigate to kestra folder before running the command below
```
cd kestra
docker compose up
```

**I. Kestra container**
  This runs a kestra container with a directory based volume
  ```
  docker run -it \
    --pull=always \
    --rm \
    -p 8080:8080 \
    --user=root \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /tmp:/tmp \
  kestra/kestra:latest server local
  ```

## Miscellaneous
  1. Remote connect to postgres container via `pgcli`:
- Install pgcli (do it inside a conda environment):

  ```
  pip install pgcli
  ```

  _Note: If error thrown on pgcli, you need to install psychopg2_
  ```
  pip install psychopg2-binary
  ```

- Credentials:  
  
  `user = root`  
  `password = root`  
  `db = ny_taxi`

- Connect to postgres database (make sure postgres container is up and running)
  ```
  pgcli \
      -h localhost \
      -p 5432 \
      -u root \
      -d ny_taxi 
  ```
## Resources

yellow_taxi_data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page  
taxi_zones: https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv