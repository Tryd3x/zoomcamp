This project involves:
- conda
- docker
- python
- jupyter

## Notes

=> Things I've learnt

- Docker shell commands to list and delete containers and images
- Difference between docker run and docker build
- Difference between using RUN and CMD, ENTRYPOINT and CMD
- Created a docker volume 'pg_data' instead of directory based mount
- Be mindful of host, port, role, password, database when connecting to postgres
- Learnt about handling authentications in pg_hba.conf (host and local authentication)
- Had to install vim in container since I had to edit pg_hba.conf [apt-get update && apt-get install vim]

=> Challenges I faced

- Setting up `git remote` since `git remote add origin` is deprecated. Resolved by using SSH token to setup remote `git remote set-url origin`
- Faced an issue with volumes. Windows is not fond of changing permissions involving linux so using docker managed volumes (volume mount) over host's directory as volume (bind mount) helped solve the issue
- Remote authentication to postgres. 

=> Commands used

- Create conda environment with python 3.9 in project folder `env`. Install pgcli interface
```
conda create -p env python=3.9
conda activate env/
```
Inside activated conda environment, install pgcli
```
pip install pgcli
```

- Setup to create and run a postgres container and volume 
```
docker volume create pg_data
docker run -it \
  -e POSTGRES_USER=root \
  -e POSTGRES_PASSWORD=root \
  -e POSTGRES_DB='ny_taxi' \
  -v pg_data:/var/lib/postgresql/data \
  -p 5432:5432 \
postgres:13
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


