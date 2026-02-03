from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from pathlib import Path

DATA_PATH = Path("/opt/airflow/data/IOT-temp.csv")
OUTPUT_PATH = Path("/opt/airflow/output")

def transform_weather():
    df = pd.read_csv(DATA_PATH)

    # 1. Отфильтровать out/in = In
    df = df[df["out/in"] == "In"]

    # 2. Перевести noted_date в формат 'yyyy-MM-dd' с типом данных date
    df["noted_date"] = pd.to_datetime(df["noted_date"], format='%d-%m-%Y %H:%M')
    df["noted_date"] = pd.to_datetime(df["noted_date"]).dt.strftime('%Y-%m-%d')

    # 3. Очистить температуру по 5-му и 95-му процентилю
    p05 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    df = df[(df["temp"] >= p05) & (df["temp"] <= p95)]

    # 4. Вычислить 5 самых жарких и самых холодных дней за год
    # Сначала группируем по дням и находим среднюю температуру за день
    daily_temp = df.groupby("noted_date")["temp"].mean().reset_index()
    
    hottest = daily_temp.nlargest(5, "temp").copy()
    hottest["type"] = "hot"
    
    coldest = daily_temp.nsmallest(5, "temp").copy()
    coldest["type"] = "cold"
    
    extremes = pd.concat([hottest, coldest])

    OUTPUT_PATH.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_PATH / "weather_cleaned.csv", index=False)
    extremes.to_csv(OUTPUT_PATH / "weather_extremes.csv", index=False)

default_args = {
    "owner": "Татаренко Кирилл",
    "start_date": datetime(2026, 2, 2),
    "retries": 1,
}

with DAG(
    dag_id="weather_transform",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["HSE", "ETL"],
) as dag:

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather
    )
