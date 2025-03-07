from time import time
import subprocess
import argparse

import pandas as pd
from sqlalchemy import create_engine

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    # download the csv
    try:
        subprocess.run(
            ['wget', url, "-O", csv_name],
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        )
        print("Download successful!")
    except Exception as e:
        print(e)
        exit(99)

    # Establish connection to database and type of database using sqlalchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Load data in chunks due to enormous size and convert it into an iterator
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # Fetch each iterable from iterator
    df = next(df_iter)
    rows = df.shape[0] # obtain size of first chunk

    # Convert datetime string to datetime object
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Create table without inserting anything yet [THIS RESETS THE TABLE WITH 0 ROWS]
    df.head(0).to_sql(con=engine, name=table_name,if_exists='replace')

    # Insert data into table
    df.to_sql(con=engine, name=table_name,if_exists='append')

    # Iterate over chunks and insert into database
    try:
        while True:
            start = time()
            df = next(df_iter) # Fetch next chunk
            rows += df.shape[0] # Fetch rows inside current chunk

            # Convert datetime string to datetime object
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

            # Insert df rows into database
            df.to_sql(con=engine, name=table_name,if_exists='append')

            stop = time()

            print(f"Inserted another chunk in {round(stop-start,3)}s")
    except StopIteration:
        print(f"Successfully inserted {rows} rows!")

# If ingest_data.py executed directly in shell, the variable __name__ is set as follows:
# __name__ = "__name__" 
# If ingest_data.py is imported as module in another file, the variable __nane__ is set as follows:
# __name__ = "ingest_data" 

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user
    # password
    # host
    # port
    # dbname
    # table name
    # url of the csv
    parser.add_argument('user', help='Username for postgres')
    parser.add_argument('password', help='Password for postgres')
    parser.add_argument('host', help='Host for postgres')
    parser.add_argument('port', help='Port for postgres')
    parser.add_argument('db', help='Database name for postgres')
    parser.add_argument('table_name', help='name of the table to write the results to')
    parser.add_argument('url', help='Url of the csv file')

    args = parser.parse_args()

    main(args)

