from datetime import datetime, timedelta

from airflow.decorators import dag, task  # type: ignore
from airflow.models import Param
from airflow.providers.http.sensors.http import HttpSensor  # type: ignore

default_args = {
    "owner": "michal",
    "email": "mzak.study@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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
    @task.short_circuit(task_id="validate_date", provide_context=True)  # type: ignore
    def _validate_date(**kwargs) -> bool:  # type: ignore
        year = kwargs["dag_run"].conf.get("year")  # type: ignore
        month = kwargs["dag_run"].conf.get("month")  # type: ignore

        if year == 2021 and month > 7:
            return False
        return True

    is_tripdata_available = HttpSensor(
        task_id="is_tripdata_available",
        http_conn_id="taxi_api",
        endpoint="/DataTalksClub",
        poke_interval=5,
        timeout=20,
    )

    validate_date = _validate_date()

    validate_date >> is_tripdata_available  # type: ignore


pipeline = taxi_pipeline()


# import logging
# from datetime import datetime, timedelta

# import pandas as pd
# from airflow.decorators import dag, task  # type: ignore
# from airflow.models import Variable
# from airflow.models.param import Param
# from airflow.providers.http.sensors.http import HttpSensor  # type: ignore
# from airflow.providers.google.cloud.transfers.local_to_gcs import (  # type: ignore
#     LocalFilesystemToGCSOperator,
# )

# default_args = {
#     "owner": "michal",
#     "email": "mzak.study@gmail.com",
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# dag_params = {
#     "v_type": Param(
#         default="yellow",
#         type="string",
#         enum=["yellow", "green", "fhv"],
#         description="Type of vehicles, for which you want the data.",
#     ),
#     "year": Param(
#         default=2019,
#         type="integer",
#         minimum=2019,
#         maximum=2021,
#         description="Year for which you want the data.",
#     ),
#     "month": Param(
#         default=1,
#         type="integer",
#         minimum=1,
#         maximum=12,
#         description="Month for which you want the data.",
#     ),
# }


# @dag(
#     params=dag_params,
#     schedule=None,  # Has to be set to None to use mandatory Param. "0 5 1 * *"
#     start_date=datetime(2023, 1, 1),
#     description="Loads NYC TLC Trip Record Data into GCS bucket",
#     tags=["de_zoomcamp", "michal"],
#     catchup=False,
#     default_args=default_args,
# )
# def gcs_data_load():
#     task_logger = logging.getLogger("airflow.task")

#     is_tripdata_available = HttpSensor(
#         task_id="is_tripdata_available",
#         http_conn_id="api_source",
#         endpoint="/DataTalksClub",
#         poke_interval=5,
#         timeout=20,
#     )

#     # @task.short_circuit(task_id="check_date", provide_context=True)  # type: ignore
#     # def _check_date(**kwargs) -> bool:  # type: ignore
#     #     year = kwargs["dag_run"].conf.get("year")  # type: ignore
#     #     month = kwargs["dag_run"].conf.get("month")  # type: ignore

#         # if year == 2021 and month > 7:
#         #     task_logger.info(f"The data for {year}-{month} in unavailable.")
#         #     return False
#         # return True

#     # @task(task_id="fetch_raw_data")
#     # def _fetch_raw_data(**kwargs) -> None:  # type: ignore
#     #     v_type = kwargs["dag_run"].conf.get("v_type")  # type: ignore
#     #     year = kwargs["dag_run"].conf.get("year")  # type: ignore
#     #     month = kwargs["dag_run"].conf.get("month")  # type: ignore
#     #     if month < 10:
#     #         month = f"0{month}"

#     #     url = (
#     #         "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
#     #         f"{v_type}/{v_type}_tripdata_{year}-{month}.csv.gz"
#     #     )
#     #     data = pd.read_csv(url)  # type: ignore

#     #     local_path = Variable.get("local_path")
#     #     data.to_csv(local_path, index=False)

#     #     task_logger.info(
#     #         f"Saved {data.shape[0]} rows locally at {local_path}."
#     #     )

#     # transfer_raw_to_gcp = LocalFilesystemToGCSOperator(
#     #     task_id="transfer_raw_to_gcp",
#     #     src=Variable.get("local_path"),
#     #     dst="/taxi.csv",
#     #     bucket=Variable.get("gcs_raw"),
#     #     gcp_conn_id="conn_gcp",
#     # )

#     # check_date = _check_date()
#     # fetch_raw_data = _fetch_raw_data()

#     # (  # type: ignore
#     #     is_tripdata_available
#     #     >> check_date
#     #     >> fetch_raw_data
#     #     >> transfer_raw_to_gcp
#     # )

#     # @task(task_id="fetch", provide_context=True)
#     # def _fetch():

#     #         if month < 10:
#     #             month = f"0{month}"
#     #     data_url =
#     #     data = pd.read_csv()


# # load_data = gcs_data_load()

# # DATE_COLS = {
# #     "yellow": ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
# #     "green": ["lpep_pickup_datetime", "lpep_dropoff_datetime"],
# #     "fhv": ["pickup_datetime", "dropOff_datetime"],
# # }
