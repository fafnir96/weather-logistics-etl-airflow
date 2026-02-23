import requests
import pandas as pd
import sqlite3


def extract_weather(**kwargs):
    exec_date = kwargs.get("data_interval_end") or kwargs.get("execution_date")

    if exec_date:
        # Konversi waktu UTC Airflow ke Waktu Jakarta
        target_dt = (
            pd.to_datetime(exec_date).tz_convert("Asia/Jakarta").tz_localize(None)
        )
    else:
        target_dt = pd.Timestamp.now("Asia/Jakarta").floor("H").tz_localize(None)

    target_date_str = target_dt.strftime("%Y-%m-%d")

    db_path = "/opt/airflow/database"
    latitude = -6.1818
    longitude = 106.8223
    hourly = "temperature_2m,wind_speed_10m,precipitation"
    timezone = "Asia/Bangkok"
    start_date = target_date_str
    end_date = target_date_str

    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly={hourly}&timezone={timezone}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Gagal menarik data historis! Status: {response.status_code}")

    raw_data = response.json()
    df_weather = pd.DataFrame(
        {
            "time": pd.to_datetime(raw_data["hourly"]["time"]),
            "temperature": raw_data["hourly"]["temperature_2m"],
            "wind_speed": raw_data["hourly"]["wind_speed_10m"],
            "precipitation": raw_data["hourly"]["precipitation"],
        }
    )

    df_filtered = df_weather[df_weather["time"] == target_dt].copy()

    if not df_filtered.empty:
        df_filtered["extraction_timestamp"] = pd.Timestamp.now("Asia/Jakarta").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        # df_weather = df_weather.dropna()
        # df_weather = df_weather.drop_duplicates(subset="time")
        df_filtered["time"] = pd.to_datetime(df_weather["time"])

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for index, row in df_filtered.iterrows():
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
                    float(row["temperature"]),
                    float(row["wind_speed"]),
                    float(row["precipitation"]),
                    str(row["extraction_timestamp"]),
                ),
            )

        conn.commit()
        conn.close()

        print(f"Weather data for {target_dt} completed inserted/ignored to database!")
    else:
        print(f"Weather data for {target_dt} not yet available from API.")
