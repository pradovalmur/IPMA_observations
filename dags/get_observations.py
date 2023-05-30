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
   
    schedule="15 * * * *",
   
    start_date=datetime(2023, 1, 1),
    
    catchup=False,
    default_args={
        "retries": 2,  
    },
    tags=["stations"],
) 
def get_observations_dag():
    

    @task()
    def extract():

        try:
            response = requests.get(Variable.get("api_url_observations"))
            response.raise_for_status()  
            data = json.dumps(response.json())
            return data
        except requests.exceptions.RequestException as e:
            print("Error occurred:", e)
            return None
    @task()
    def transform_load(data):
        
        df = pd.read_json(data)

        columns = df.columns

        for column in columns:

            df1 = df[column].apply(pd.Series)
            df1["datetimereading"] = column
            
            df1.reset_index(inplace=True)
            df1 = df1.rename(columns = {'index':'idEstacao'})
            
            table_name = 'raw.observations'

            df1.to_sql(table_name, engine, if_exists='append', index=False)

        return True
    
    @task()
    def promote_data_to_next_layer(previous_status):
        
        query = """INSERT INTO "curated.observations" ("idEstacao","intensidadeVentoKM",temperatura,radiacao,"idDireccVento","precAcumulada","intensidadeVento",humidade,pressao,datetimereading)
SELECT "idEstacao","intensidadeVentoKM",temperatura,radiacao,"idDireccVento","precAcumulada","intensidadeVento",humidade,pressao,datetimereading
FROM (select 
		"idEstacao",
		"intensidadeVentoKM",
		temperatura,
		radiacao,
		"idDireccVento",
		"precAcumulada",
		"intensidadeVento",
		humidade,
		pressao,
		datetimereading
	from (select 
	*,
	ROW_NUMBER() OVER (PARTITION BY o."idEstacao",o.datetimereading ORDER BY o.datetimereading) AS row_number
	from "raw.observations" as o) as xx
	where xx.row_number = 1
	) as xx2
where  xx2.datetimereading not in (select datetimereading from  "curated.observations")"""

        engine.execute(query)
        

    observations = extract()
    transformed = transform_load(observations)
    promote_data_to_next_layer(transformed)

  
    
get_observations_dag()


