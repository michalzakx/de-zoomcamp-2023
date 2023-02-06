import logging
from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd
from airflow.decorators import dag, task  # type: ignore
from airflow.models import Param, Variable
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor  # type: ignore

# from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (  # type: ignore
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (  # type: ignore
    GCSToBigQueryOperator,
)


default_args = {
    "owner": "michal",
    "email": "michal@mail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag_params = {
    "v_type": Param(
        default="yellow",
        type="string",
        enum=["yellow", "green", "fhv"],
        description="Type of vehicles, for which you want the data.",
    ),
    "year": Param(
        default=2019,
        type="integer",
        minimum=2019,
        maximum=2021,
        description="Year for which you want the data.",
    ),
    "month": Param(
        default=1,
        type="integer",
        minimum=1,
        maximum=12,
        description="Month for which you want the data.",
    ),
}


@dag(
    params=dag_params,
    schedule=None,  # Has to be set to None to use mandatory Param. "0 5 1 * *"
    start_date=datetime(2023, 1, 1),
    description="Loads NYC TLC Trip Record Data into BigQuery",
    tags=["de_zoomcamp", "michal"],
    catchup=False,
    default_args=default_args,
)
def taxi_pipeline():
    task_logger = logging.getLogger("airflow.task")

    @task.short_circuit(task_id="validate_date", provide_context=True)  # type: ignore
    def _validate_date(**kwargs) -> bool:  # type: ignore
        year = kwargs["dag_run"].conf.get("year")  # type: ignore
        month = kwargs["dag_run"].conf.get("month")  # type: ignore

        if year == 2021 and month > 7:
            task_logger.error(f"Tripdata for {year}-{month} is unavailable.")
            return False
        return True

    is_tripdata_available = HttpSensor(
        task_id="is_tripdata_available",
        http_conn_id="taxi_api",
        endpoint="/DataTalksClub",
        poke_interval=5,
        timeout=20,
    )

    @task(task_id="fetch_data")
    def _fetch_data(**kwargs: Dict[Any, Any]) -> str:
        v_type = kwargs["dag_run"].conf.get("v_type")  # type: ignore
        year = kwargs["dag_run"].conf.get("year")  # type: ignore
        month = kwargs["dag_run"].conf.get("month")  # type: ignore

        if month < 10:
            month = f"0{month}"

        tripdata_url = (
            "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
            f"{v_type}/{v_type}_tripdata_{year}-{month}.csv.gz"
        )
        tripdata = pd.read_csv(tripdata_url)  # type: ignore

        local_storage_path = Variable.get("local_storage_path")
        local_csv_path = local_storage_path + f"{v_type}_{year}_{month}.csv"
        tripdata.to_csv(local_csv_path, index=False)
        task_logger.info(
            f"Saved {tripdata.shape[0]} rows locally at {local_csv_path}."
        )

        return local_csv_path

    @task(task_id="clean_data")
    def _clean_data(local_csv_path: str, **kwargs: Dict[Any, Any]) -> str:
        date_cols = Variable.get(
            "tripdata_date_columns", deserialize_json=True
        )
        v_type = kwargs["dag_run"].conf.get("v_type")  # type: ignore

        tripdata = pd.read_csv(local_csv_path)  # type: ignore
        for col in date_cols[v_type]:
            tripdata[col] = pd.to_datetime(tripdata[col])  # type: ignore

        local_parquet_path = local_csv_path.replace(".csv", ".parquet")
        tripdata.to_parquet(local_parquet_path, compression="gzip")
        task_logger.info(
            f"Saved {tripdata.shape[0]} rows locally at {local_parquet_path}."
        )

        return local_parquet_path

    write_gcs = LocalFilesystemToGCSOperator(
        task_id="write_gcs",
        src='{{ti.xcom_pull(task_ids="clean_data")}}',
        dst="cleaned/{{ params.v_type }}_{{ params.year }}_{{ params.month }}.parquet",
        bucket="{{var.value.gcs_data_lake}}",
        gcp_conn_id="gcp_conn",
    )

    local_storage_cleanup = BashOperator(
        task_id="local_storage_cleanup", bash_command="rm /tmp/data/*"
    )

    write_bq = GCSToBigQueryOperator(
        task_id="write_bq",
        bucket="{{ var.value.gcs_data_lake }}",
        source_objects="cleaned/{{ params.v_type }}_{{ params.year }}_{{ params.month }}.parquet",
        destination_project_dataset_table="{{ var.value.bq_taxi_data }}.{{ params.v_type }}_tripdata",
        source_format="parquet",
        compression="GZIP",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        gcp_conn_id="gcp_conn",
    )

    # send_email_notification = EmailOperator(
    #     task_id="send_email_notification",
    #     to="{{ var.value.email }}",
    #     subject="Ingestion completed",
    #     html_content="Date: {{ ds }}",
    # )

    validate_date = _validate_date()
    fetch_data = _fetch_data()
    clean_data = _clean_data(fetch_data)

    validate_date >> is_tripdata_available >> fetch_data  # type: ignore
    fetch_data >> clean_data >> write_gcs >> local_storage_cleanup  # type: ignore
    local_storage_cleanup >> write_bq  # type: ignore


pipeline = taxi_pipeline()
