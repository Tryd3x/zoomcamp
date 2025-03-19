FROM python:3.9

RUN apt-get update && apt-get install wget

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

RUN mkdir datasets && mkdir scripts

COPY ./scripts/ingest_data.py ./scripts/ingest_data.py

WORKDIR /app/scripts

ENTRYPOINT [ "python", "ingest_data.py" ]