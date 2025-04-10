## Setup

- You will require a key generated from GCP's service account with the following permissions, `Storage Admin` & `Bigquery Admin`
- Navigate to the service account and click 'Manage keys'. Click on 'Add key' and generate and JSON file.
- Copy the contents of the key under './keys/my-creds.json'

## How to use the flows folder

- Spin up the containers in docker-compose.yml
- Navigate to Kestra's UI [localhost:8080]
- Click on flows on the left navigation bar
- Top-right, click `Create` button
- Copy contents, say `02_postgres_taxi.yml` and paste it in Kestra's Flows UI and click `Save` button on top right.
- Top-right, click `Execute` to run the flow.

Note: In KestraUI, navigate to namespace -> zoomcamp -> KV Store. Add a new key-value pair and name it `GCP_CREDS`. Paste the contents of './keys-creds.json' and hit save.

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


