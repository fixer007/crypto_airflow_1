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
def networks_list(**kwargs):

    # print(f"Processing token: {token['name']}")
    baseUrl = "https://public-api.birdeye.so/defi/networks";

    headers = {"X-API-KEY": os.environ.get("API_KEY")}

    with requests.Session() as s:
        download = s.get(baseUrl, headers=headers)
        decoded_content = download.content.decode('utf-8')
        
        data = json.loads(decoded_content)

        #{'data': ['solana', 'ethereum', 'arbitrum', 'avalanche', 'bsc', 'optimism', 'polygon', 'base', 'zksync', 'sui'], 'success': True}
        print(data)

        return 1


#just dummy default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 10, 4, 11, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('crypto_5',
         catchup = False,
         default_args = default_args,
        #  schedule_interval = '*/1 * * * *',
         schedule_interval=None,
         ) as dag:

    task_get_price = PythonOperator(
            task_id='networks_list',
            python_callable=networks_list,
            # op_kwargs={'token': 'tokens'},
            dag=dag,
        )



if __name__ == "__main__":
    dag.test()


