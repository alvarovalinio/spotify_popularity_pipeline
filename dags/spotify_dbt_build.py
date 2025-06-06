from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.datasets import Dataset

default_args = {
    "owner": "avalino",
    "start_date": datetime(2025, 6, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

BASH_COMMAND = """
cd /opt/airflow/dbt/spotify_popularity &&
dbt build --vars '{start_date: {{ data_interval_end | ds }}}' --profiles-dir .
"""

with DAG(
    dag_id="spotify_dbt_build",
    default_args=default_args,
    schedule=[Dataset("/data/raw/spotify/")],
    catchup=False,
    tags=["SPOTIFY", "DBT", "STAGING", "ANALYTICS"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=BASH_COMMAND,
    )

    dbt_build
