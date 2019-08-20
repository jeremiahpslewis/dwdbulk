import logging
import pathlib
import re
from pathlib import Path
from typing import List
from urllib.parse import urljoin

import dask.dataframe as dd
import pandas as pd
import requests
from dwdbulk.util import (
    germany_climate_uri,
    get_resource_index,
    measurement_colnames_kv,
    measurement_coltypes_kv,
    measurement_datetypes_kv,
    station_colnames_kv,
    station_coltypes_kv,
    station_datetypes_kv,
)

log = logging.getLogger(__name__)


na_values = ["-999", "-999   "]
# Hierarchical Structure
# - resolution -> measurement -> time bucket -> station availability


def __gather_resource_files(resolution, parameter):
    """
    Given time resolution and measurement parameter, return list of available resources (observations and metadata).
    """

    index_uri = urljoin(germany_climate_uri, resolution, parameter)

    resource_list = get_resource_index(index_uri)

    # Extract station lists from all data buckets
    subfolder_resource_lists = [
        x
        for x in resource_list
        if any([y in x for y in ["now", "recent", "historical"]])
    ]

    for f in subfolder_resource_lists:
        resource_list.extend(get_resource_index(f))

    return resource_list


def get_resolutions() -> List[str]:
    """
    Get available resolutions for DWD CDC Climate Data.
    If `resolution_name` is None, all resolutions are returned.


    :param str resolution_name: A string describing the resolution, either "hourly" or "10_minutes"

    :returns: A list of resolution objects
    """

    return get_resource_index(germany_climate_uri, "", full_uri=False)


def get_measurement_parameters(resolution: str) -> List[str]:
    return [
        {"resolution": resolution, "parameter": parameter}
        for parameter in get_resource_index(
            urljoin(germany_climate_uri, resolution), "", full_uri=False
        )
    ]


def get_stations(resolution: str, parameter: str) -> pd.DataFrame:
    """
    Load station meta data from DWD server.
    """
    log.info("Loading station data from CDC")

    resource_list = __gather_resource_files(resolution, parameter)
    # Get directory contents.
    resource_list = [x for x in resource_list if "Beschreibung_Stationen.txt" in x]

    resource_df_list = []

    for resource_uri in resource_list:
        log.info(f"Fetching resource {resource_uri}")

        df = get_stations_list_from_uri(resource_uri)
        resource_df_list.append(df)

    resource_df = pd.concat(resource_df_list, axis=0)
    resource_df.drop_duplicates(inplace=True)
    return resource_df


def get_measurement_data_from_uri(uri: str):
    df = pd.read_csv(
        uri,
        sep=";",
        dtype=measurement_coltypes_kv,
        encoding="utf-8",
        parse_dates=measurement_datetypes_kv,
        skipinitialspace=True,
        date_parser=lambda col: pd.to_datetime(col, format="%Y%m%d%H%M", utc=True),
        na_values=na_values,
    )
    df.drop(columns="eor", inplace=True, errors="ignore")
    df.rename(columns=measurement_colnames_kv, inplace=True)

    return df


def get_stations_list_from_uri(uri: str):
    col_names = pd.read_csv(uri, sep=" ", encoding="latin1", nrows=0).columns.tolist()

    df = pd.read_fwf(
        uri,
        header=None,
        names=col_names,
        dtype=station_coltypes_kv,  # TODO: Figure out how to handle nullables...
        encoding="latin1",
        skiprows=2,
        parse_dates=station_datetypes_kv,
        date_parser=lambda col: pd.to_datetime(col, format="%Y%m%d", utc=True),
        na_values=na_values,
    )
    df.rename(columns=station_colnames_kv, inplace=True)

    return df


def get_measurement_data_uris(resolution, parameter):
    available_resources = __gather_resource_files(resolution, parameter)

    # Filter by File Format
    # NOTE: We assume that all files that pass this filter are data files
    download_list = [f for f in available_resources if ".zip" in f]

    return download_list


def generate_partitioned_dataset(dataset_folder):
    path = pathlib.PurePath(dataset_folder)

    df = dd.read_parquet(dataset_folder)

    df["date_start__year"] = df.date_start.dt.year
    df["date_start__month"] = df.date_start.dt.month
    df["date_start__year_month"] = df["date_start__year"] + df["date_start__month"]
    df["resolution"] = path.stem.split("__")[0]
    df["parameter"] = path.stem.split("__")[1]

    unique_partitions = df.date_start__year_month.unique().compute()

    for filter_partition in unique_partitions:
        df_subset = df[(df.date_start__year_month == filter_partition)].compute()
        df_subset.to_parquet(
            f"data/partitioned_measurements/{path.stem}",
            partition_cols=["date_start__year", "date_start__month"],
            index=False,
        )
