# 🏙️ Jakarta Daily Pulse: Urban Logistics, Weather & Pollution ETL

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.x-green.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)
![SQLite](https://img.shields.io/badge/SQLite-Database-lightgrey.svg)
![Pandas](https://img.shields.io/badge/Pandas-Data_Analysis-yellow.svg)

## 📌 Project Overview

**Jakarta Daily Pulse** is an end-to-end Data Engineering and Business Analytics project. It demonstrates the complete lifecycle of data: from establishing an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow, to deriving actionable business insights regarding the impact of weather and air pollution on daily delivery logistics in Jakarta.

This project bridges the gap between **Data Engineering** (building robust, idempotent pipelines) and **Data Analytics** (answering real-world business questions).

## 🏗️ Architecture & Pipeline

The data pipeline is designed to be fully automated, idempotent, and real-time (hourly).

1. **Extract**: Pulls real-time weather and pollutant (PM2.5, NO2) data via the Open-Meteo API. Generates simulated logistic order data based on time-of-day and weekend multipliers.
2. **Transform**: Cleans the data, handles timezone conversions (UTC to Asia/Jakarta), and formats it into Pandas DataFrames.
3. **Load**: Inserts the data into a centralized SQLite database (`jakarta_weather_order.db`). Utilizes `INSERT OR IGNORE` SQL clauses combined with `PRIMARY KEY` constraints to ensure strict **idempotency** (preventing data duplication during Airflow retries).
4. **Automate**: Apache Airflow (running via Docker) orchestrates the DAG to execute these tasks hourly.

### 📂 Repository Structure

```text
raw/
 ├── airflow_automation/
 │    ├── dags/
 │    │    ├── airflow_automation.py
 │    │    ├── extract_polutan.py
 │    │    ├── extract_weather.py
 │    │    └── fill_order_daily.py
 │    └── data/
 │         └── setup_db.py
 │
 ├── docker-compose.yaml
 ├── analysis using python.ipynb
 ├── analysis using SQL.ipynb
 ├── backfill_data_polutan_and_weather_1_year.py
 ├── backfill_data_order_daily_1_year.py
 └── README.md
```

## 📊 Key Business Insights & Recommendations

Based on the extensive exploratory data analysis (EDA) found in the Jupyter Notebooks, several critical business insights were uncovered:

### 1. Surge Pricing Algorithm Optimization (Weather Impact on Delivery)

- **Observation:** There is a clear correlation between weather conditions and operational metrics. As weather shifts from 'Sunny' to 'Drizzling' and 'Raining', order volumes spike significantly (from 64.4 to 90.6 orders/hour) while average delivery times increase. The heatmap analysis proves that operations are most severely impacted when rain is coupled with moderate-to-strong winds.
- **Business Recommendation:** Implement a multi-tiered Surge Pricing algorithm. Trigger Level 1 Surge during 'Drizzling' conditions to capture the demand spike while maintaining driver supply. Trigger Level 2 Surge (maximum tier) during 'Raining' conditions with 'Moderate/Strong Winds' to compensate for the severe drop in delivery speed and higher safety risks for drivers.

### 2. NO2 Emissions as a Traffic Proxy: The "Photolysis" Revelation

- **Observation:** The initial hypothesis that Nitrogen Dioxide (NO2) peaks during daytime rush hours was proven false. Due to the Photolysis effect (sunlight breaking down NO2) and Thermal Inversion, NO2 levels actually plummet during the day and skyrocket at night (peaking at ~80 µg/m³ between 22:00 - 23:00). Macro-level satellite data lacks the granularity to differentiate daytime street-level traffic.
- **Business Recommendation:** Pivot from using NO2 as a traffic ETA proxy. Instead, address a critical operational risk: Driver Welfare. Night-shift logistics drivers are exposed to hazardous NO2 levels. The company should mandate and provide N95/KN95 masks as part of standard safety gear for the night fleet to mitigate long-term health liabilities.

### 3. The "Washout" Effect: Post-Rain PM2.5 Dynamics

- **Observation:** Heavy precipitation (> 7.5 mm) acts as a natural air purifier. The data proves a significant drop in PM2.5 concentrations immediately during and in the hours following heavy rain events.
- **Business Recommendation:** Optimize marketing pushes or delivery promos for "Clean Air Delivery Hours" immediately following rainstorms, capitalizing on higher outdoor activity intent.

### 4. Health-Tech Marketing Opportunity: Air Temperature vs PM2.5

- **Observation:** There is a distinct correlation between the drop in air temperature at night and the concentration of PM2.5 particles.
- **Business Recommendation:** Implement dynamic app notifications targeting users to order food/groceries delivery during high-pollution/low-temperature evening hours, emphasizing the health benefits of staying indoors.

## 🚀 Getting Started (How to Run)

### Prerequisites

- Docker & Docker Compose installed
- Python 3.8+

### Step-by-Step Execution

**1. Initialize the Database** Run the setup script to create the SQLite database and establish the `PRIMARY KEY` constraints required for the pipeline's idempotency.

```bash
python raw/airflow_automation/data/setup_db.py

```

**2. Backfill Historical Data (Optional)** opulate the database with 1 year of historical data for immediate analysis.

```bash
python backfill_data_polutan_and_weather_1_year.py
python backfill_data_order_daily_1_year.py
```

**3. Start Apache Airflow** Spin up the Airflow environment using Docker Compose.

```bash
docker-compose up -d
```

**4. Explore the Analysis** Open the Jupyter Notebooks to dive deep into the visualizations and SQL queries.

```bash
jupyter notebook "analysis using python.ipynb"
jupyter notebook "analysis using sQL.ipynb"
```
