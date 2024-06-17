# Imports
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import requests
import csv
import os
import json
from dotenv import load_dotenv
load_dotenv()

# def process_tokens(token_str, **kwargs):
def token_list(**kwargs):

    # print(f"Processing token: {token['name']}")
    baseUrl = "https://public-api.birdeye.so/defi/tokenlist?sort_by=v24hUSD&sort_type=asc";

    headers = {"X-API-KEY": os.environ.get("API_KEY")}

    with requests.Session() as s:
        download = s.get(baseUrl, headers=headers)
        decoded_content = download.content.decode('utf-8')
        
        data = json.loads(decoded_content)

        print(data)

        # return 1

        df = pd.DataFrame(data=data['data']['tokens'])
        table = pa.Table.from_pandas(df)

        now = datetime.now().strftime("%m%d%Y%H%M%S")

        #simple check for existence
        #TODO should be moved to utility module
        folder_path = f"/airflow_data/tokens_list"
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, mode=0o777)
            print(f"Folder '{folder_path}' created with chmod 777.")
        else:
            print(f"Folder '{folder_path}' already exists.")

        file_name = f"/airflow_data/tokens_list/tokens_list_"+now+".parquet"

        pq.write_table(table, file_name)


#just dummy default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 10, 4, 11, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('crypto_4',
         catchup = False,
         default_args = default_args,
        #  schedule_interval = '*/1 * * * *',
         schedule_interval=None,
         ) as dag:

    task_get_price = PythonOperator(
            task_id='token_list',
            python_callable=token_list,
            # op_kwargs={'token': 'tokens'},
            dag=dag,
        )



if __name__ == "__main__":
    dag.test()


