import requests
import pandas as pd
from datetime import datetime, timedelta
import sqlite3


def extract_polutan(**kwargs):
    exec_date = kwargs.get("data_interval_end") or kwargs.get("execution_date")

    if exec_date:
        target_dt = (
            pd.to_datetime(exec_date).tz_convert("Asia/Jakarta").tz_localize(None)
        )
    else:
        target_dt = pd.Timestamp.now("Asia/Jakarta").floor("H").tz_localize(None)

    target_date_str = target_dt.strftime("%Y-%m-%d")

    latitude = -6.1818
    longitude = 106.8223
    hourly = "pm2_5,nitrogen_dioxide"
    timezone = "Asia/Bangkok"
    start_date = target_date_str
    end_date = target_date_str

    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly={hourly}&timezone={timezone}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Gagal menarik data historis! Status: {response.status_code}")
    raw_data = response.json()

    df_polutan = pd.DataFrame(
        {
            "time": pd.to_datetime(raw_data["hourly"]["time"]),
            "pm_25": raw_data["hourly"]["pm2_5"],
            "nitrogen_dioxide": raw_data["hourly"]["nitrogen_dioxide"],
        }
    )

    df_filtered = df_polutan[df_polutan["time"] == target_dt].copy()
    if not df_filtered.empty:
        df_filtered["extraction_timestamp"] = pd.Timestamp.now("Asia/Jakarta").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        df_filtered["time"] = pd.to_datetime(df_filtered["time"])
        db_path = "/opt/airflow/database"

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for index, row in df_filtered.iterrows():
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
                    float(row["pm_25"]),
                    float(row["nitrogen_dioxide"]),
                    str(row["extraction_timestamp"]),
                ),
            )

        conn.commit()
        conn.close()

        print(f"Polutan data for {target_dt} completed inserted/ignored to database!")
    else:
        print(f"Polutan data for {target_dt} not yet available from API.")
