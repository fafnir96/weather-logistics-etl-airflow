import sqlite3
import pandas as pd
import numpy as np


def fill_table_order(**kwargs):
    exec_date = kwargs.get("data_interval_end") or kwargs.get("execution_date")

    if exec_date:
        target_dt = (
            pd.to_datetime(exec_date)
            .tz_convert("Asia/Jakarta")
            .floor("H")
            .tz_localize(None)
        )
    else:
        target_dt = pd.Timestamp.now("Asia/Jakarta").floor("H").tz_localize(None)

    df_orders = pd.DataFrame({"time": [target_dt]})
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

    db_path = "/opt/airflow/database"
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
            (
                str(row["time"]),
                int(row["total_orders"]),
                float(row["average_delivery_time"]),
            ),
        )

    conn.commit()
    cursor.execute("SELECT * FROM daily_orders")
    semua_data = cursor.fetchall()

    conn.close()
    print("Done Fill table orders")
    conn.close()
    print("Done Fill table orders")
