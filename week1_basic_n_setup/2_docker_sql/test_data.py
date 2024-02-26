import pandas as pd
from sqlalchemy import create_engine

user = "root"
password = "root"
host = "localhost"
port = 5432
db = "ny_taxi"

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

file = pd.read_csv("https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv")
file.to_sql(name='zones', con=engine, if_exists='replace')