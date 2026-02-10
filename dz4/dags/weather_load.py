from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2.extras as extras


SOURCE_PATH = Path("/opt/airflow/source_output/weather_cleaned.csv")


def load_full():
    """
    Полная загрузка исторических данных:
    - читает результат трансформации из dz3 (weather_cleaned.csv)
    - полностью пересобирает таблицу weather_cleaned в БД
    """
    if not SOURCE_PATH.exists():
        raise FileNotFoundError(f"Источник данных не найден: {SOURCE_PATH}")

    df = pd.read_csv(SOURCE_PATH)

    df = df.rename(columns={"room_id/id": "room_id", "out/in": "out_in"})
    df["noted_date"] = pd.to_datetime(df["noted_date"]).dt.date

    hook = PostgresHook(postgres_conn_id="postgres_data")
    conn = hook.get_conn()

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE weather_cleaned;")

        insert_sql = """
            INSERT INTO weather_cleaned (id, room_id, noted_date, temp, out_in)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET room_id = EXCLUDED.room_id,
                noted_date = EXCLUDED.noted_date,
                temp = EXCLUDED.temp,
                out_in = EXCLUDED.out_in;
        """

        rows = [
            (row["id"], row["room_id"], row["noted_date"], row["temp"], row["out_in"])
            for _, row in df.iterrows()
        ]

        extras.execute_batch(cur, insert_sql, rows)

    conn.commit()
    conn.close()


def load_incremental(days_back: int = 3):
    """
    Инкрементальная загрузка:
    - берём только записи за последние `days_back` дней по колонке noted_date
    - добрасываем/обновляем их в таблицу weather_cleaned
    """
    if not SOURCE_PATH.exists():
        raise FileNotFoundError(f"Источник данных не найден: {SOURCE_PATH}")

    df = pd.read_csv(SOURCE_PATH)
    df = df.rename(columns={"room_id/id": "room_id", "out/in": "out_in"})
    df["noted_date"] = pd.to_datetime(df["noted_date"]).dt.date

    today = datetime.utcnow().date()
    cutoff = today - timedelta(days=days_back)

    df_inc = df[df["noted_date"] >= cutoff]

    if df_inc.empty:
        return

    hook = PostgresHook(postgres_conn_id="postgres_data")
    conn = hook.get_conn()

    with conn.cursor() as cur:
        insert_sql = """
            INSERT INTO weather_cleaned (id, room_id, noted_date, temp, out_in)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET room_id = EXCLUDED.room_id,
                noted_date = EXCLUDED.noted_date,
                temp = EXCLUDED.temp,
                out_in = EXCLUDED.out_in;
        """

        rows = [
            (row["id"], row["room_id"], row["noted_date"], row["temp"], row["out_in"])
            for _, row in df_inc.iterrows()
        ]

        extras.execute_batch(cur, insert_sql, rows)

    conn.commit()
    conn.close()


default_args = {
    "owner": "Татаренко Кирилл",
    "start_date": datetime(2026, 2, 3),
    "retries": 1,
}


with DAG(
    dag_id="weather_load_hw4",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["HSE", "ETL"],
) as dag:

    load_full_task = PythonOperator(
        task_id="load_weather_full",
        python_callable=load_full,
    )

    load_incremental_task = PythonOperator(
        task_id="load_weather_incremental",
        python_callable=load_incremental,
        op_kwargs={"days_back": 3},
    )

    load_full_task >> load_incremental_task

