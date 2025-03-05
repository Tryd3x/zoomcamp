This project involves:
- conda
- docker
- python

## Notes

=> Things I've learnt

- Docker shell commands to list and delete containers and images
- Difference between docker run and docker build
- Difference between using RUN and CMD, ENTRYPOINT and CMD
- Created a docker volume 'pg_data' instead of directory based mount

=> Challenges I faced

- Setting up `git remote` since `git remote add origin` is deprecated. Resolved by using SSH token to setup remote `git remote set-url origin`
- Faced an issue with volumes. Windows is not fond of changing permissions involving linux so using docker managed volumes (volume mount) over host's directory as volume (bind mount) helped solve the issue.

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
  -u root \
  -e POSTGRES_USER='airflow' \
  -e POSTGRES_PASSWORD='airflow' \
  -e POSTGRES_DB='airflow' \
  -v pg_data:/var/lib/postgresql/data \
  -p 5432:5432 \
postgres:13
```


