from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests
import os

from airflow.providers.email.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator

url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2023-01.csv"
csv_path = "/opt/airflow/data/taxi.csv"
cleaned_path = "/opt/airflow/data/taxi_cleaned.csv"

@dag(
    dag_id="real_world_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["project"]
)
def real_pipeline():
    start = EmptyOperator(task_id="start")

    wait_for_file = FileSensor(
        task_id="wait_for_taxi_file",
        filepath=csv_path,
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    @task
    def download():
        os.makedirs("/opt/airflow/data", exist_ok=True)
        r = requests.get(url)
        with open(csv_path, "wb") as f:
            f.write(r.content)

    @task
    def clean():
        df = pd.read_csv(csv_path)
        df_clean = df.dropna()
        df_clean.to_csv(cleaned_path, index=False)

    send_email = EmailOperator(
        task_id='send_email',
        to='mail@gmail.com', # à changer
        subject='Airflow: ETL Pipeline Complete ✅',
        html_content='<h3>NYC Taxi ETL pipeline has completed successfully!</h3>',
    )

    # DAG structure
    start >> download() >> wait_for_file >> clean() >> send_email

dag = real_pipeline()
