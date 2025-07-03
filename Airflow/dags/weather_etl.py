from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
import os

# Configuration
CITIES = {
    'Paris': {'lat': 48.85, 'lon': 2.35},
    'London': {'lat': 51.51, 'lon': -0.13},
    'Berlin': {'lat': 52.52, 'lon': 13.41}
}
DATA_PATH = "/opt/airflow/data"
CSV_FILE = f"{DATA_PATH}/weather_data.csv"

@dag(
    dag_id="weather_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="0 8 * * *",  # 8 AM UTC daily
    catchup=False,
    tags=["weather", "etl"]
)
def weather_etl():
    @task
    def extract():
        os.makedirs(DATA_PATH, exist_ok=True)
        weather_data = []
        for city, coords in CITIES.items():
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
            )
            r = requests.get(url)
            current = r.json()["current_weather"]
            current["city"] = city
            current["timestamp"] = datetime.utcnow().isoformat()
            weather_data.append(current)
        return weather_data

    @task
    def transform(data: list):
        df = pd.DataFrame(data)
        df = df[["city", "timestamp", "temperature", "windspeed", "weathercode"]]
        return df.to_dict()

    @task
    def load(data: dict):
        df = pd.DataFrame.from_dict(data)
        if os.path.exists(CSV_FILE):
            existing = pd.read_csv(CSV_FILE)
            df = pd.concat([existing, df])
            df.drop_duplicates(subset=["city", "timestamp"], inplace=True)
        df.to_csv(CSV_FILE, index=False)

    load(transform(extract()))

dag = weather_etl()
