from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


from extract_weather import extract_weather
from extract_polutan import extract_polutan
from fill_order_daily import fill_table_order

db_path = "/opt/airflow/database"

default_args = {
    "owner": "Irfan Maulana",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="jakarta_daily_weather_and_polutan",
    default_args=default_args,
    description="Extract Weather, Pollutant, and Orders Data Hourly",
    start_date=datetime(2026, 2, 20),
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["jakarta", "weather", "hourly"],
) as dag:

    extract_weather_jakarta = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    extract_polutan_jakarta = PythonOperator(
        task_id="extract_polutan",
        python_callable=extract_polutan,
    )

    fill_order_daily = PythonOperator(
        task_id="fill_orders", python_callable=fill_table_order
    )

    extract_weather_jakarta >> extract_polutan_jakarta >> fill_order_daily
