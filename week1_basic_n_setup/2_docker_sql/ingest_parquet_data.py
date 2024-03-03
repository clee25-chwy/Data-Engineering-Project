
import argparse, os, sys
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time


# new data format is parquet
# df = pd.read_parquet('yellow_tripdata_2023-01.parquet')

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name= params.table_name
    url = params.url

    file_name = url.rsplit('/', 1)[-1].strip()
    print(f'Downloading {file_name}...')
    os.system(f"curl {url.strip()} -O {file_name}")
    print('\n')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()
    if '.csv' in file_name:
        df = pd.read_csv(file_name, nrows=10)
        df_iter = pd.read_csv(file_name, index_col=0, iterator=True, chunksize=100000)
    elif '.parquet' in file_name:
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size= 100000)
    else:
        print('Error. Only .csv or .parquet files allowed.')
        sys.exit()
    # print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert values
    t_start = time()
    count = 0
    for batch in df_iter:
        count += 1

        if '.parquet' in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch

        print(f'inserting batch {count}...')

        b_start = time()
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        b_end = time()

        print(f'inserted! time taken {b_end - b_start:10.3f} seconds.\n')

    t_end = time()
    print(f'Completed! Total time taken was {t_end - t_start:10.3f} seconds for {count} batches.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Loading data from .paraquet file link to a Postgres database.')

    # user, password, host, port, database name, table name, url of the csv
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the .parquet file')

    args=parser.parse_args()
    main(args)








# conn = psycopg2.connect(
#     host='localhost',
#     port=5432,
#     dbname='ny_taxi',
#     user='root',
#     password='root'
# )


