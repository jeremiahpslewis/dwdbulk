import datetime
import os
import pathlib
import re
from glob import glob

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import Flow, task
from prefect.engine.cache_validators import all_inputs
from prefect.engine.executors import DaskExecutor
from prefect.utilities.tasks import unmapped

from .api import forecasts, observations
from .util import (
    get_resource_index,
    partitioned_df_write_to_parquet,
    get_stations_lookup,
)


@task
def fetch_resolutions():
    return observations.get_resolutions()


@task
def fetch_measurement_parameters(resolution):
    return observations.get_measurement_parameters(resolution)


@task(
    max_retries=3,
    retry_delay=datetime.timedelta(minutes=10),
    cache_for=datetime.timedelta(hours=5),
)
def fetch_stations(res_param_obj):
    resolution = res_param_obj["resolution"]
    parameter = res_param_obj["parameter"]

    folder_name = "stations"
    full_folder_name = f"data/{folder_name}"
    file_name = f"{full_folder_name}/{parameter}.parquet"

    if not os.path.exists(full_folder_name):
        os.makedirs(full_folder_name)

    stations_df = observations.get_stations(resolution, parameter)
    partitioned_df_write_to_parquet(stations_df, data_folder=full_folder_name)

    return stations_df["station_id"].tolist()


@task
def fetch_measurement_data_locations(res_param_obj):
    resolution = res_param_obj["resolution"]
    parameter = res_param_obj["parameter"]

    urls = observations.get_measurement_data_urls(resolution, parameter)
    return [
        {"url": url, "resolution": resolution, "parameter": parameter} for url in urls
    ]


@task
def unlist(list_object):
    return [i for l in list_object for i in l]


@task(
    max_retries=3,
    retry_delay=datetime.timedelta(minutes=10),
    cache_for=datetime.timedelta(days=3),
    cache_validator=all_inputs,
)
def fetch_measurement_data(measurement_spec):
    resolution = measurement_spec["resolution"]
    parameter = measurement_spec["parameter"]
    url = measurement_spec["url"]

    full_folder_name = f"data/{resolution}/{parameter}"

    if not os.path.exists(full_folder_name):
        os.makedirs(full_folder_name)

    try:
        file_name = re.match(
            r".*\/([^/]+_[a-z]+_[0-9]+_[^/]+).zip", url, flags=re.IGNORECASE
        )
        file_name = file_name.group(1)
        full_file_path = f"{full_folder_name}/{file_name}.parquet"
        # If file already downloaded, do not refetch
        if os.path.exists(full_file_path):
            return

        df = observations.get_measurement_data_from_url(url)

        partitioned_df_write_to_parquet(df, data_folder=full_folder_name)

    except:
        with open("errors.txt", "a") as f:
            f.write(url + "\n")
        raise


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def gather_forecast_urls():
    """Identify all available forecast files."""
    forecast_urls = get_resource_index(
        "https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/all_stations/kml/"
    )
    forecast_urls = [f for f in forecast_urls if "LATEST" not in f]
    return forecast_urls


@task
def get_berlin_brandenburg_station_ids():
    """Identify active Berlin / Brandenburg weather stations."""
    # TODO: Replace with csv lookup util
    df = get_stations_lookup()
    return {
        "forecasts": df["forecasts_station_id"].tolist(),
        "observations": df["observations_station_id"].tolist(),
    }


@task(
    max_retries=3,
    retry_delay=datetime.timedelta(minutes=10),
    cache_for=datetime.timedelta(days=3),
    cache_validator=all_inputs,
)
def process_forecast(forecast_url, station_ids):
    """Process XML forecast, store output and remove xml file."""
    forecast_file_path = forecasts.fetch_raw_forecast_xml(forecast_url)
    forecasts.convert_xml_to_parquet(forecast_file_path, station_ids)
    os.remove(forecast_file_path)


with Flow("Fetch DWD Germany Forecast Data") as forecasts_flow:
    bb_stations = get_berlin_brandenburg_station_ids()["forecasts"]
    forecast_urls = gather_forecast_urls()
    process_forecast.map(forecast_urls, unmapped(bb_stations))


with Flow("Fetch Full DWD Germany Observation Data") as observations_flow:
    # Fetch available resolutions
    # res_list = ["10_minutes"]

    # Fetch resolution-measurement parameter objects per resolution
    # measurement_parameters_per_res = fetch_measurement_parameters.map(res_list)
    measurement_parameters_per_res = [
        [{"resolution": "10_minutes", "parameter": "air_temperature"}]
    ]

    # Collapse list of lists to single list
    res_param_list = unlist(measurement_parameters_per_res)

    # Fetch and save station data, return list of stations for each res / cat combination
    station_list = fetch_stations.map(res_param_list)

    data_file_urls = fetch_measurement_data_locations.map(res_param_list)
    data_file_url_list = unlist(data_file_urls)

    # Fetch data
    fetch_measurement_data.map(data_file_url_list)

if __name__ == "__main__":

    # executor = DaskExecutor(local_processes=True)
    executor = None
    forecasts_flow.run(executor=executor)
    # observations_flow.run(executor=executor)
