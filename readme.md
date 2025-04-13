## Preface

Tech stack: Big Query, Kestra, Terraform, Looker Studio, Google Cloud Storage, WSL - Ubuntu, Postgres, Conda, Docker, Python, Jupyter, CI/CD, DBT, DBT labs, DLT, bash, ssh, pandas, git, github, Spark, Kafka, Flink, Redpanda


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
- Partition splits table into smaller tables and parent table holds the metadata to these smaller tables. Partition can be performed on single attribute.
- Cluster logically sorts records based on some attribute. It does not split into smaller tables. Cluster can be performed on multiple attribute.  
- Difference:  
	- "Sort before insert" vs "Sort after insert"
	- If stream data inserts, periodic clustering is preferred.
	- If batch data inserts, partitioning is preferred
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
- Learnt about Kafka (Distributed platform for storing, managing and transmitting Stream data) - topics, partitions and group consumer/consumers.
- Learnt about flink (Distributed stream processing engine) - to process data stored in Kafka.
- Learnt about watermark and how it resolves out of order events (delayed events)
- Window types: Tumble (Non-overlap), Sliding (Overlap), Session
- Utilized conda environment for managing python version. Utilized `vevn` for managing project's subfolder level python dependencies.

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