import sqlite3
import pandas as pd
import numpy as np

time = pd.date_range(start="2025-02-22 00:00:00", end="2026-02-21 23:00:00", freq="h")


def fill_table_order():
    df_orders = pd.DataFrame({"time": time})
    hour = df_orders["time"].dt.hour

    order_condition = [
        (hour >= 0) & (hour <= 5),
        (hour >= 11) & (hour <= 13),
        (hour >= 17) & (hour <= 19),
    ]

    order_choose = [
        np.random.randint(10, 30, size=len(hour)),
        np.random.randint(150, 300, size=len(hour)),
        np.random.randint(180, 350, size=len(hour)),
    ]

    df_orders["total_orders"] = np.select(
        order_condition, order_choose, default=np.random.randint(10, 20, len(hour))
    )

    additional_delay = np.where(
        df_orders["total_orders"] > 200, np.random.randint(10, 20, len(hour)), 0
    )
    df_orders["average_delivery_time"] = (
        np.random.randint(15, 25, len(hour)) + additional_delay
    )

    df_orders["time"] = df_orders["time"].astype(str)

    db_path = r"C:\Users\Axioo Pongo\Portofolio DA\Jakarta Daily\airflow_automation\data\jakarta_weather_order.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for index, row in df_orders.iterrows():
        sql_query = """
            INSERT OR IGNORE INTO daily_orders
            (time, total_orders, average_delivery_time)
            VALUES (?, ?, ?)
        """
        cursor.execute(
            sql_query,
            (str(row["time"]), row["total_orders"], row["average_delivery_time"]),
        )

    conn.commit()
    conn.close()
    print("Done Fill table orders")


if __name__ == "__main__":
    fill_table_order()
