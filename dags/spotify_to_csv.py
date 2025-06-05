from airflow.models import DAG
from airflow.operators.python import PythonOperator
from spotify_to_csv.transfer_to_csv import artist_to_csv, tracks_to_csv, albums_to_csv
from datetime import datetime, timedelta
import yaml
from typing import Any
from airflow.datasets import Dataset


def _load_yaml(path: str) -> Any:
    """
    Load a YAML file and return its contents as a Python object.

    Args:
        path (str): Path to the YAML file.

    Returns:
        Any: Parsed Python object from the YAML file (typically a dict or list).
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


CONFIG = _load_yaml("dags/spotify_to_csv/config.yml")

default_args = {
    "owner": "avalino",
    "start_date": datetime(2025, 5, 26),
    "provide_context": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="spotify_to_csv",
    default_args=default_args,
    schedule_interval=None,
    is_paused_upon_creation=True,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["SPOTIFY", "CSV", "RAW"],
):

    load_artists_data = PythonOperator(
        task_id="load_artists_data",
        python_callable=artist_to_csv,
        op_kwargs={
            "config": CONFIG,
            "loaded_at": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
        },
    )

    load_tracks_data = PythonOperator(
        task_id="load_tracks_data",
        python_callable=tracks_to_csv,
        op_kwargs={
            "config": CONFIG,
            "artist_ids": "{{ task_instance.xcom_pull(task_ids='load_artists_data', key='artist_ids') }}",
            "loaded_at": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
        },
    )

    load_albums_data = PythonOperator(
        task_id="load_albums_data",
        python_callable=albums_to_csv,
        op_kwargs={
            "config": CONFIG,
            "album_ids": "{{ task_instance.xcom_pull(task_ids='load_tracks_data', key='album_ids') }}",
            "loaded_at": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
        },
        outlets=[Dataset("/data/raw/spotify/")],
    )

    load_artists_data >> load_tracks_data >> load_albums_data
