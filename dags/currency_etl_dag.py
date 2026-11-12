"""
Currency Exchange Rate ETL Pipeline
-----------------------------------
Extract  : daily exchange rates from the Frankfurter API (v2)
Transform: reshape every rate into the required document format
Load     : upsert the documents into MongoDB

Tasks pass data to each other with *named* XComs (explicit keys),
not the default "return_value" key.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import pendulum
import requests
from pymongo import ASCENDING, MongoClient, UpdateOne

# Airflow 3 (current Astro Runtime) imports, with a fallback for Airflow 2
try:
    from airflow.sdk import DAG
except ImportError:  # Airflow 2.x
    from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator


log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY"]
SOURCE_NAME = "Frankfurter API"

# MONGO_URI comes from the project's .env file (see README).
# "mongo" is the Docker service name, resolved over the shared Docker network.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongo:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

# Named XCom keys and task ids (kept in one place so push/pull always match)
EXTRACT_TASK_ID = "extract_exchange_rates"
TRANSFORM_TASK_ID = "transform_exchange_rate_data"
LOAD_TASK_ID = "load_data_into_mongodb"

XCOM_RAW_RATES = "raw_exchange_rates"
XCOM_RETRIEVED_AT = "retrieved_at"
XCOM_TRANSFORMED = "transformed_exchange_rates"
XCOM_LOAD_SUMMARY = "mongodb_load_summary"


# ---------------------------------------------------------------------------
# Task 1: Extract
# ---------------------------------------------------------------------------
def extract_exchange_rates(**context):
    """Call the Frankfurter API and push the raw JSON to XCom."""
    params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
    log.info("Requesting %s with params %s", API_URL, params)

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()

    if not raw_data:
        raise ValueError("Frankfurter API returned an empty response")

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    count = len(raw_data) if isinstance(raw_data, list) else len(raw_data.get("rates", {}))
    log.info("Extracted %d exchange rates at %s", count, retrieved_at)

    ti = context["ti"]
    ti.xcom_push(key=XCOM_RAW_RATES, value=raw_data)
    ti.xcom_push(key=XCOM_RETRIEVED_AT, value=retrieved_at)


# ---------------------------------------------------------------------------
# Task 2: Transform
# ---------------------------------------------------------------------------
def _normalise(raw_data):
    """
    Turn the API response into a flat list of (base, target, rate, date).

    v2 response (current):  [{"date": "...", "base": "EUR", "quote": "USD", "rate": 1.11}, ...]
    v1 response (fallback): {"base": "EUR", "date": "...", "rates": {"USD": 1.11, ...}}
    """
    if isinstance(raw_data, list):
        return [(r["base"], r["quote"], r["rate"], r["date"]) for r in raw_data]
    return [
        (raw_data["base"], target, rate, raw_data["date"])
        for target, rate in raw_data["rates"].items()
    ]


def transform_exchange_rate_data(**context):
    """Pull raw data from XCom, reshape it, and push the clean records to XCom."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids=EXTRACT_TASK_ID, key=XCOM_RAW_RATES)
    retrieved_at = ti.xcom_pull(task_ids=EXTRACT_TASK_ID, key=XCOM_RETRIEVED_AT)

    if not raw_data:
        raise ValueError(f"No data found in XCom key '{XCOM_RAW_RATES}'")

    records = []
    for base, target, rate, rate_date in _normalise(raw_data):
        rate = float(rate)
        if rate <= 0:
            log.warning("Skipping invalid rate %s for %s/%s", rate, base, target)
            continue
        records.append(
            {
                "base_currency": base.upper(),
                "target_currency": target.upper(),
                "exchange_rate": round(rate, 6),
                "rate_date": rate_date,
                "source": SOURCE_NAME,
                "retrieved_at": retrieved_at,
            }
        )

    if not records:
        raise ValueError("Transformation produced no valid records")

    log.info("Transformed %d records. Sample: %s", len(records), records[0])
    ti.xcom_push(key=XCOM_TRANSFORMED, value=records)


# ---------------------------------------------------------------------------
# Task 3: Load
# ---------------------------------------------------------------------------
def load_data_into_mongodb(**context):
    """Pull clean records from XCom and upsert them into MongoDB."""
    ti = context["ti"]
    records = ti.xcom_pull(task_ids=TRANSFORM_TASK_ID, key=XCOM_TRANSFORMED)

    if not records:
        raise ValueError(f"No data found in XCom key '{XCOM_TRANSFORMED}'")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]

        # One document per (base, target, date): re-runs update instead of duplicating
        collection.create_index(
            [("base_currency", ASCENDING), ("target_currency", ASCENDING), ("rate_date", ASCENDING)],
            unique=True,
            name="uniq_pair_per_day",
        )

        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(operations)

        summary = {
            "database": MONGO_DB,
            "collection": MONGO_COLLECTION,
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "matched": result.matched_count,
            "total_in_collection": collection.count_documents({}),
        }
        log.info("MongoDB load complete: %s", summary)
        ti.xcom_push(key=XCOM_LOAD_SUMMARY, value=summary)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates: Frankfurter API -> transform -> MongoDB",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "currency", "mongodb"],
) as dag:

    extract = PythonOperator(
        task_id=EXTRACT_TASK_ID,
        python_callable=extract_exchange_rates,
    )

    transform = PythonOperator(
        task_id=TRANSFORM_TASK_ID,
        python_callable=transform_exchange_rate_data,
    )

    load = PythonOperator(
        task_id=LOAD_TASK_ID,
        python_callable=load_data_into_mongodb,
    )

    extract >> transform >> load