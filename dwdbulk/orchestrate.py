import datetime
import os
import pathlib
import re
from glob import glob

import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
from dwdbulk import api
from prefect import Flow, task
from prefect.engine.executors import DaskExecutor


@task
def fetch_resolutions():
    return api.observations.get_resolutions()


@task
def fetch_measurement_parameters(resolution):
    return api.observations.get_measurement_parameters(resolution)


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def fetch_stations(res_param_obj):
    resolution = res_param_obj["resolution"]
    parameter = res_param_obj["parameter"]

    folder_name = "stations"
    full_folder_name = f"data/{folder_name}"
    file_name = f"{full_folder_name}/{parameter}.parquet"

    if not os.path.exists(full_folder_name):
        os.makedirs(full_folder_name)

    stations_df = api.observations.get_stations(resolution, parameter)
    stations_pa = pa.Table.from_pandas(stations_df, preserve_index=False)

    pq.write_table(stations_pa, file_name)

    return stations_df["station_id"].tolist()


@task
def fetch_measurement_data_locations(res_param_obj):
    resolution = res_param_obj["resolution"]
    parameter = res_param_obj["parameter"]

    uris = api.observations.get_measurement_data_uris(resolution, parameter)
    return [
        {"uri": uri, "resolution": resolution, "parameter": parameter} for uri in uris
    ]


@task
def unlist(list_object):
    return [i for l in list_object for i in l]


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def fetch_measurement_data(measurement_spec):
    resolution = measurement_spec["resolution"]
    parameter = measurement_spec["parameter"]
    uri = measurement_spec["uri"]

    folder_name = f"{resolution}__{parameter}"
    full_folder_name = f"data/measurements/{folder_name}"
    if not os.path.exists(full_folder_name):
        os.makedirs(full_folder_name)

    try:
        file_name = re.match(
            r".*\/([^/]+_[a-z]+_[0-9]+_[^/]+).zip", uri, flags=re.IGNORECASE
        )
        file_name = file_name.group(1)
        full_file_path = f"{full_folder_name}/{file_name}.parquet"
        # If file already downloaded, do not refetch
        if os.path.exists(full_file_path):
            return

        df = api.observations.get_measurement_data_from_uri(uri)

        df["date_start__year"] = df.date_start.dt.year

        df_pa = pa.Table.from_pandas(df, preserve_index=False)

        pq.write_table(df_pa, full_file_path)

    except:
        with open("errors.txt", "a") as f:
            f.write(uri + "\n")
        raise


@task
def run_partition_dataset(dataset_folder):
    api.observations.generate_partitioned_dataset(dataset_folder)


with Flow("Fetch Full DWD Germany Data") as full_flow:
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

    data_file_uris = fetch_measurement_data_locations.map(res_param_list)
    data_file_uri_list = unlist(data_file_uris)

    # Fetch data
    fetch_measurement_data.map(data_file_uri_list)

with Flow("Fetch Full DWD Germany Data") as partition_flow:
    dataset_folders = glob("data/measurements/10_minutes__air_temperature/")

    run_partition_dataset.map(dataset_folders)

if __name__ == "__main__":

    # executor = DaskExecutor(local_processes=True)
    # full_flow.run(executor=executor)
    partition_flow.run()
