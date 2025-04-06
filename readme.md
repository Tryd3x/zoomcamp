## Preface

Tech stack: Big Query, Kestra, Terraform, Looker Studio, Google Cloud Storage, WSL - Ubuntu, Postgres, Conda, Docker, Python, Jupyter, CI/CD, DBT, DBT labs, DLT, bash, ssh, pandas, git, github, Spark


### **Things I've learnt**

- Docker shell commands to list and delete containers and images
- Difference between docker run and docker build
- Difference between using RUN and CMD, ENTRYPOINT and CMD
- Created a docker volume 'pg_data' instead of directory based mount. 
- Be mindful of host, port, role, password, database when connecting to postgres
- Learnt about handling authentications in pg_hba.conf (host and local authentication)
- Had to install vim in container since I had to edit pg_hba.conf [apt-get update && apt-get install vim]
- Miscellaneous shell commands: gunzip, less, more, head, tail, wc
- Learnt about pandas' `pd.io.sql.get_engine()` and sqlalchemy `create_engine()`
- Learnt about pandas iterators, python iterator and how to sequentially transform and insert pandas data into database
- Learnt about host-container communication and container-container communication
- Learnt about bridge network and host network
- Addressed shell injection vulnerabilities when utlizing: `os.system()` vs `subprocess.run()`
- Learnt WSL-Ubuntu as secondary dev environment via remote connect
- Learnt more about streaming pipeline via `subprocess.Popen()`
- Composed a docker-compose.yaml from multiple images
- Learnt how to pass arguments (using `commands` to pass it to `entrypoint`) to a docker container
- [Postgres] Difference between identifier `""` and String literal `''`
- Difference between `usermod` and `gpasswd`
- Generating ssh keys and defining config
- Learnt about `sftp` (put, get)
- Learnt data ingestion from github into postgres via Kestra
- Learnt about staging tables (temporary buffer table) and fact tables (quantified table)
- Difference between yml's `|` (literal block scalar; preserves newlines '\n') and `>` (folded block scalar; newlines '\n' replaced by whitespace). In simple words, `|` treats a multiline string as multiline and `>` treats multiline string as a single line.
- Integrated GCS and Bigquery with kestra workflow.
- Learnt about DLT (data load tools - focused on extraction/ingestion, normalization and loading)
- Understood the difference between dimension table and fact table (Star Schema)
- Brushed up on Data Normalization (3NF)
- DBT seeds are basically dimensional tables that dont change frequently and can be stored as .csv files.
- Usage of source() vs ref()
- DBT packages: dbt_utils, codegen. Instructions on how to install them.
- DBT docs: `dbt docs generate` to generate and host documentation
- Deployed dbt project (using dbt cloud) to production and configured a scheduler for periodic updates.
- Applied CI/CD for automation whenever development is merged to production.
- Learnt about Github Actions and Github Webhooks for CI/CD. Read article on how to manage CI in airflow.
- Utilized Looker Studio (formerly Google Data Studio) for visualizing data from data warehouse.
- Parquet supports schema evolution and comes with preserved schema
- Perform repartition() to distribute data across multiple workers for faster computation
- Perform coalesce() to gather all the distributed partitions 
- Spark pushes code to data instead of data being pushed to code (Data Locality)
- Shuffle causes data to move across partitions (expesive) before performing the final aggregation (reduce())
- Learnt about local aggregation vs global aggregation. [reduceByKey() vs groupByKey()]
- If data is small, it is broadcasted to each worker node instead having it to reshuffle the large data.
- RDD level operations: parallelize(), map(), reduceByKey(), take(), collect(), toDF(), mapPartition()
- Manually start Master node by navigating to spark's sbin folder and executing `start-master.sh`
- Manually add Worker node to Master by executing `start-worker.sh <master-URL>`
- Configure Master node to submit a python script as a spark job using `spark-submit`.

### **Challenges faced**

- Setting up `git remote` via http is deprecated. Resolved by using SSH key to setup remote `git remote add origin <ssh-url-of-repo>`
- Faced an issue with volumes. Windows is not fond of changing permissions involving linux so using docker managed volumes (volume mount) over host's directory as volume (bind mount) helped solve the issue.
- Update on the above fix: changed 'owner:group' permissions of the directory based volume by `sudo chown -R hyderreza:hyderreza <folder_as_volume>`
- Remote authentication to postgres. 
- Kestra doesnt give fine grain control over the network while executing a dbt docker container as a task. It only gives access to modify the attribute `networkMode: "bridge" | "host" | "none"`. However, airflow's DockerOperator() provides a network parameter to assign the following: `"bridge" | "host" | "none" | "<network-name>|<network-id>"`
- Kestra's flow yml are syntax sensitive -> `trigger.date | date('yyyy-MM')` is different from `(trigger.date | date('yyyy-MM'))`


## Environment Setup (Anaconda)

- For windows:

    ```
    conda env create -p env -f win-environment.yml
    ```
- For wsl-Ubuntu:
    
    ```
    conda env create -p env -f wsl-ubuntu-environment.yml
    ```
- Activate environment:

  ```
  conda activate env/
  ```

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