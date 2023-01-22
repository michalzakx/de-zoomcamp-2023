"""Ingest NYC TLC Trip Record Data into the database.

This script works with the CSV backup data performed by DataTalksClub, which
is available at https://github.com/DataTalksClub/nyc-tlc-data. Original data
is available at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
stored in Parquet format.

As of the time of writing this script, the data available in the backup
repository contains records for Yellow Taxi Trips, Green Taxi Trips and
For-Hire Vehicle Trips from January 2019 to July 2021. Therefore, the script
should be used only to ingest trip data within that time period. Ingestion for
High Volume For-Hire Vehicle Trips is not available, as the data has not been
backed up in the repository.

The script is meant to be used as a standalone tool and asumes that the
appropriate tables have been already created in the database, i.e. it will try
to ingest data in the append mode.


Example:
    python ingest_data.py green 2019 1 --db_name nyc_tlc_trips --db_port 54320
"""

import argparse
import logging
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

DATE_COLS = {
    "yellow": ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    "green": ["lpep_pickup_datetime", "lpep_dropoff_datetime"],
    "fhv": ["pickup_datetime", "dropOff_datetime"],
}


class DateOutOfRange(Exception):
    """Raised when the data for this time period is not available."""

    def __init__(self, year: int, month: int):
        self.message = (
            f"The data for {year}-{month} is not available in the repository."
        )
        super().__init__(self.message)


def main(
    v_type: str,
    year: int,
    month: int,
    db_dialect: str,
    db_login: str,
    db_pwd: str,
    db_host: str,
    db_port: int,
    db_name: str,
):
    try:
        if year == 2021 and month > 7:
            raise DateOutOfRange(year, month)
        else:
            if month < 10:
                month = f"0{month}"
    except DateOutOfRange as e:
        print(e)
        sys.exit(1)

    # TODO CHECK IF THIS DATA HAS BEEN ALREADY INGESTED

    url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
        f"{v_type}/{v_type}_tripdata_{year}-{month}.csv.gz"
    )
    db_url = (
        f"{db_dialect}://{db_login}:{db_pwd}@{db_host}:{db_port}/{db_name}"
    )
    if v_type in ["yellow", "green"]:
        table_name = f"{v_type}_taxi_trip"
    else:
        table_name = f"{v_type}_trip"

    data = pd.read_csv(url)
    for col in DATE_COLS[v_type]:
        data[col] = pd.to_datetime(data[col])

    # TODO DATA HAS OUTLIERS - CLEAN IT

    engine = create_engine(db_url)
    # TODO Handle invalid connection
    data.to_sql(name=table_name, con=engine, if_exists="append", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="ingest_data.py",
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    url_group = parser.add_argument_group("URL")
    url_group.add_argument(
        "type",
        help="Type of vehicles, for which you want the data.",
        choices=["yellow", "green", "fhv"],
    )
    url_group.add_argument(
        "year",
        help="Year for which you want the data.",
        type=int,
        choices=[2019, 2020, 2021],
    )
    url_group.add_argument(
        "month",
        help="Month for which you want the data.",
        type=int,
        choices=range(1, 13),
        metavar="[1-12]",
    )

    db_group = parser.add_argument_group("DATABASE CONNECTION")
    db_group.add_argument(
        "--db_dialect",
        help="Database dialect supported by SQLAlchemy. Default: postgresql.",
        default="postgresql",
    )
    db_group.add_argument(
        "--db_login", help="Database username. Default: root.", default="root"
    )
    db_group.add_argument(
        "--db_pwd", help="Database password. Default: root.", default="root"
    )
    db_group.add_argument(
        "--db_host",
        help="Database host, default: localhost.",
        default="localhost",
    )
    db_group.add_argument(
        "--db_port",
        help="Database port, default: 5432.",
        type=int,
        default=5432,
    )
    db_group.add_argument(
        "--db_name",
        help="Database to which you want to connect.",
        required=True,
    )

    args = parser.parse_args()
    main(
        args.type,
        args.year,
        args.month,
        args.db_dialect,
        args.db_login,
        args.db_pwd,
        args.db_host,
        args.db_port,
        args.db_name,
    )
