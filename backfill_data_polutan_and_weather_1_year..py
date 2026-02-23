import requests
import pandas as pd
from datetime import datetime, timedelta
import sqlite3

db_path = r"C:\Users\Axioo Pongo\Portofolio DA\Jakarta Daily\airflow_automation\data\jakarta_weather_order.db"


def backfill_weather_data_1_year():
    latitude = -6.1818
    longitude = 106.8223
    hourly = "temperature_2m,wind_speed_10m,precipitation"
    timezone = "Asia/Bangkok"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly={hourly}&timezone={timezone}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Gagal menarik data historis! Status: {response.status_code}")

    raw_data = response.json()

    df_weather = pd.DataFrame(
        {
            "time": raw_data["hourly"]["time"],
            "temperature": raw_data["hourly"]["temperature_2m"],
            "wind_speed": raw_data["hourly"]["wind_speed_10m"],
            "precipitation": raw_data["hourly"]["precipitation"],
        }
    )
    df_weather["extraction_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_weather = df_weather.dropna()
    df_weather["time"] = pd.to_datetime(df_weather["time"])

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for index, row in df_weather.iterrows():
        # Karena kita pakai INSERT OR IGNORE, kalau tanggal kemarin sudah ada, dia tidak akan dobel!
        sql_query = """
            INSERT OR IGNORE INTO weather 
            (time, temperature, wind_speed, precipitation, extraction_timestamp)
            VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(
            sql_query,
            (
                str(row["time"]),
                row["temperature"],
                row["wind_speed"],
                row["precipitation"],
                row["extraction_timestamp"],
            ),
        )

    conn.commit()
    print(f"Selesai! Koneksi ditutup.")
    conn.close()

    print("Backfilling Weather Completed")


def backfill_polutan_data_1_year():
    latitude = -6.1818
    longitude = 106.8223
    hourly = "pm2_5,nitrogen_dioxide"
    timezone = "Asia/Bangkok"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly={hourly}&timezone={timezone}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Gagal menarik data historis! Status: {response.status_code}")
    raw_data = response.json()
    df_polutan = pd.DataFrame(
        {
            "time": raw_data["hourly"]["time"],
            "pm_25": raw_data["hourly"]["pm2_5"],
            "nitrogen_dioxide": raw_data["hourly"]["nitrogen_dioxide"],
        }
    )

    df_polutan = df_polutan.dropna()
    df_polutan["extraction_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_polutan["time"] = pd.to_datetime(df_polutan["time"])

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for index, row in df_polutan.iterrows():
        # Karena kita pakai INSERT OR IGNORE, kalau tanggal kemarin sudah ada, dia tidak akan dobel!
        sql_query = """
            INSERT OR IGNORE INTO polutan 
            (time, pm_25, nitrogen_dioxide, extraction_timestamp)
            VALUES (?, ?, ?, ?)
        """
        cursor.execute(
            sql_query,
            (
                str(row["time"]),
                row["pm_25"],
                row["nitrogen_dioxide"],
                row["extraction_timestamp"],
            ),
        )

    conn.commit()
    print(f"Selesai! Koneksi ditutup.")
    conn.close()

    print("Backfilling Polutan Completed")


if __name__ == "__main__":
    backfill_weather_data_1_year()
    backfill_polutan_data_1_year()
