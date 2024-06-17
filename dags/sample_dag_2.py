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
def process_tokens_get_price(**kwargs):

    #tokens should be fetched from external source and should not be hardcoded
    tokens = [
        {
            'name': 'boden',
            'address': '3psH1Mj1f7yUfaD5gh6Zj7epE8hhrMkMETgv5TshQA4o'
        },
        {
            'name': 'mother',
            'address': '3S8qX1MsMqRbiwKg2cQyx7nis1oHMgaCuc9c4VfvVdPN'
        },
        {
            'name': 'sol',
            'address': 'So11111111111111111111111111111111111111112'
        }
        ]

    for token in tokens:

        # print(f"Processing token: {token['name']}")
        baseUrl = "https://public-api.birdeye.so/defi/price?address="

        headers = {"X-API-KEY": os.environ.get("API_KEY")}

        with requests.Session() as s:
            download = s.get(baseUrl + token['address'], headers=headers)
            decoded_content = download.content.decode('utf-8')
            
            # print(decoded_content)
            data = json.loads(decoded_content)

            print(data['data']['value'])
            print(data['data']['updateHumanTime'])

            d = {
                "date":data['data']['updateHumanTime'],
                "price":data['data']['value']
            }

            df = pd.DataFrame(data=d, index=[0])
            table = pa.Table.from_pandas(df)

            now = datetime.now().strftime("%m%d%Y%H%M%S")

            #simple check for existence
            folder_path = f"/airflow_data/{token['name']}"
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, mode=0o777)
                print(f"Folder '{folder_path}' created with chmod 777.")
            else:
                print(f"Folder '{folder_path}' already exists.")

            file_name = f"/airflow_data/{token['name']}/price_data_"+now+".parquet"

            pq.write_table(table, file_name)


#just dummy default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 10, 4, 11, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('crypto_2',
         catchup = False,
         default_args = default_args,
        #  schedule_interval = '*/1 * * * *',
         schedule_interval=None,
         ) as dag:

    task_get_price = PythonOperator(
            task_id='token_get_price',
            python_callable=process_tokens_get_price,
            # op_kwargs={'token': 'tokens'},
            dag=dag,
        )



if __name__ == "__main__":
    dag.test()


