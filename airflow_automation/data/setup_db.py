import sqlite3
import os

db_path = "jakarta_weather_order.db"
os.makedirs(os.path.dirname(db_path), exist_ok=True)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

try:
    cursor.execute(
        """
        CREATE TABLE weather (
            time DATETIME PRIMARY KEY,
            temperature REAL,
            wind_speed REAL,
            precipitation REAL,
            extraction_timestamp DATETIME
        )
    """
    )
    print("Weather Table Created!")

    # Tabel Polutan
    cursor.execute(
        """
        CREATE TABLE polutan (
            time DATETIME PRIMARY KEY,
            pm_25 REAL,
            nitrogen_dioxide REAL,
            extraction_timestamp DATETIME
        )
    """
    )
    print("Weather Polutan Created!")

    # Tabel Daily Orders (Mockup)
    cursor.execute(
        """
        CREATE TABLE daily_orders (
            time DATETIME PRIMARY KEY,
            total_orders INTEGER,
            average_delivery_time REAL,
            extraction_timestamp DATETIME
        )
    """
    )
    print("Weather Daily Order Created!")

    # Simpan perubahan
    conn.commit()

except Exception as e:
    print(f"Error on database: {e}")
    conn.rollback()

finally:
    conn.close()
