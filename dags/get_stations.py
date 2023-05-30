import json
from pendulum import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable

from airflow.decorators import (
    dag,
    task,
) 


db_user = Variable.get("db_user")
db_password = Variable.get("db_password")
db_host = Variable.get("db_host")
db_port = Variable.get("db_port")
db_name = Variable.get("db_name")

conn_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

engine = create_engine(conn_string)

@dag(
   
    schedule="10 0 * * *",
   
    start_date=datetime(2023, 1, 1),
    
    catchup=False,
    default_args={
        "retries": 2,  
    },
    tags=["stations"],
) 
def get_stations_dag():
    

    @task()
    def extract():

        try:
            response = requests.get(Variable.get("api_url_stations"))
            response.raise_for_status()  
            data = response.json()  
            return data
        except requests.exceptions.RequestException as e:
            print("Error occurred:", e)
            return None
    @task()
    def transform(data):
        
        df = pd.json_normalize(data)

        df['longitude'] = df['geometry.coordinates'].apply(lambda x: x[1])
        df['latitude'] = df['geometry.coordinates'].apply(lambda x: x[0])

        df = df.drop('geometry.coordinates', axis=1)
        df = df.drop('geometry.type', axis=1)
        df = df.drop('type', axis=1)

        df.columns = df.columns.str.replace('geometry.', '').str.replace('properties.', '')
        
        return df

    @task()
    def load(df):
        table_name = 'raw.stations'

        # Write the DataFrame to PostgreSQL table
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        return True


    @task()
    def promote_data_to_next_layer(previous_status):
        
        query = """INSERT INTO "curated.stations" ("idEstacao","localEstacao",longitude,latitude)
SELECT "idEstacao","localEstacao",longitude,latitude
FROM (select 
		"idEstacao","localEstacao",longitude,latitude
	from (select 
	*,
	ROW_NUMBER() OVER (PARTITION BY o."idEstacao" ORDER BY o."idEstacao") AS row_number
	from "raw.stations" as o) as xx
	where xx.row_number = 1
	) as xx2
where  xx2."idEstacao" not in (select "idEstacao" from  "curated.stations")"""

        engine.execute(query)

    stations = extract()
    transformed = transform(stations)
    loaded = load(transformed)
    promote_data_to_next_layer(loaded)
    
get_stations_dag()

