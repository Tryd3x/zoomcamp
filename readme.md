## Preface

This project involves:
- conda
- docker
- python
- jupyter

## Notes

### **Things I've learnt**

- Docker shell commands to list and delete containers and images
- Difference between docker run and docker build
- Difference between using RUN and CMD, ENTRYPOINT and CMD
- Created a docker volume 'pg_data' instead of directory based mount
- Be mindful of host, port, role, password, database when connecting to postgres
- Learnt about handling authentications in pg_hba.conf (host and local authentication)
- Had to install vim in container since I had to edit pg_hba.conf [apt-get update && apt-get install vim]
- Miscellaneous shell commands: gunzip, less, more, head, tail, wc
- Learnt about pandas' `pd.io.sql.get_engine()` and sqlalchemy `create_enginer()`
- Learnt about pandas iterators, python iterator and how to sequentially transform and insert pandas data into database
- Learnt about host-container communication and container-container communication
- Learnt about bridge network and host network

### **Challenges faced**

- Setting up `git remote` since `git remote add origin` is deprecated. Resolved by using SSH token to setup remote `git remote set-url origin`
- Faced an issue with volumes. Windows is not fond of changing permissions involving linux so using docker managed volumes (volume mount) over host's directory as volume (bind mount) helped solve the issue
- Remote authentication to postgres. 


## Setup Instructions

- Create conda environment with python 3.9 in project folder `env`. Install pgcli interface
```
conda create -p env python=3.9
conda activate env/
```
Inside activated conda environment, install pgcli
```
pip install pgcli
```

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

- Remote connect to postgres container using pgcli installed in conda environment  
What we know so far:  
user = root  
password = root  
db = ny_taxi  

```
pgcli \
    -h localhost \
    -p 5432 \
    -u root \
    -d ny_taxi 
```

## Resources

dataset: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page


