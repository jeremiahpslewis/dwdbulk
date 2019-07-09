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
    return api.get_resolutions()


@task
def fetch_measurement_categories(resolution):
    return api.get_measurement_categories(resolution)


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def fetch_stations(res_cat_obj):
    resolution = res_cat_obj["resolution"]
    category = res_cat_obj["category"]

    folder_name = "stations"
    full_folder_name = f"data/{folder_name}"
    file_name = f"{full_folder_name}/{category}.parquet"

    if not os.path.exists(full_folder_name):
        os.makedirs(full_folder_name)

    stations_df = api.get_stations(resolution, category)
    stations_pa = pa.Table.from_pandas(stations_df, preserve_index=False)

    pq.write_table(stations_pa, file_name)

    return stations_df["station_id"].tolist()


@task
def fetch_measurement_data_locations(res_cat_obj):
    resolution = res_cat_obj["resolution"]
    category = res_cat_obj["category"]

    uris = api.get_measurement_data_uris(resolution, category)
    return [
        {"uri": uri, "resolution": resolution, "category": category} for uri in uris
    ]


@task
def unlist(list_object):
    return [i for l in list_object for i in l]


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def fetch_measurement_data(measurement_spec):
    resolution = measurement_spec["resolution"]
    category = measurement_spec["category"]
    uri = measurement_spec["uri"]

    folder_name = f"{resolution}__{category}"
    full_folder_name = f"data/measurements/{folder_name}"
    if not os.path.exists(full_folder_name):
        os.makedirs(full_folder_name)

    try:
        file_name = re.match(
            r".*\/([^/]+_[a-z]+_[0-9]+_[^/]+).zip", str(uri), flags=re.IGNORECASE
        )
        file_name = file_name.group(1)
        full_file_path = f"{full_folder_name}/{file_name}.parquet"
        # If file already downloaded, do not refetch
        if os.path.exists(full_file_path):
            return

        df = api.get_measurement_data_from_uri(uri)

        df["date_start__year"] = df.date_start.dt.year

        df_pa = pa.Table.from_pandas(df, preserve_index=False)

        pq.write_table(df_pa, full_file_path)

    except:
        with open("errors.txt", "a") as f:
            f.write(str(uri) + "\n")
        raise


@task
def run_partition_dataset(dataset_folder):
    api.generate_partitioned_dataset(dataset_folder)


with Flow("Fetch Full DWD Germany Data") as full_flow:
    # Fetch available resolutions
    # res_list = ["10_minutes"]

    # Fetch resolution-measurement category objects per resolution
    # measurement_categories_per_res = fetch_measurement_categories.map(res_list)
    measurement_categories_per_res = [
        [{"resolution": "10_minutes", "category": "air_temperature"}]
    ]

    # Collapse list of lists to single list
    res_cat_list = unlist(measurement_categories_per_res)

    # Fetch and save station data, return list of stations for each res / cat combination
    station_list = fetch_stations.map(res_cat_list)

    data_file_uris = fetch_measurement_data_locations.map(res_cat_list)
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
