from __future__ import annotations

from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

BUCKET = "gs://zoomcamp-food-prices-abdelkarem"
PROJECT_DIR = Path(__file__).resolve().parents[2]


with DAG(
    dag_id="food_prices_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["zoomcamp", "food-prices"],
) as dag:
    upload_to_gcs = BashOperator(
        task_id="upload_to_gcs",
        bash_command=(
            "set -e\n"
            f'cd "{PROJECT_DIR}"\n'
            "gsutil cp data/raw/faostat_prices.csv "
            f"{BUCKET}/raw/faostat_prices.csv"
        ),
    )

    run_spark = BashOperator(
        task_id="run_spark",
        bash_command=(
            "set -e\n"
            f'cd "{PROJECT_DIR}"\n'
            "spark-submit spark/faostat_transform.py "
            f"--input {BUCKET}/raw/faostat_prices.csv "
            f"--output {BUCKET}/processed/faostat_clean"
        ),
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=(
            "set -e\n"
            f'cd "{PROJECT_DIR}"\n'
            "bq load --source_format=PARQUET --autodetect "
            "food_prices.faostat_food_prices "
            f"{BUCKET}/processed/faostat_clean/*"
        ),
    )

    upload_to_gcs >> run_spark >> load_to_bigquery
