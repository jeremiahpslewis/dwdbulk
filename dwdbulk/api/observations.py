import pathlib
import re
from pathlib import Path
from typing import List
from urllib.parse import urljoin

import dask.dataframe as dd
import pandas as pd
import requests
from ..util import (
    germany_climate_url,
    get_resource_index,
    measurement_colnames_kv,
    measurement_coltypes_kv,
    measurement_datetypes_kv,
    station_colnames_kv,
    station_coltypes_kv,
    station_datetypes_kv,
)

na_values = ["-999", "-999   "]
# Hierarchical Structure
# - resolution -> measurement -> time bucket -> station availability


def __gather_resource_files(resolution, parameter):
    """
    Given time resolution and measurement parameter, return list of available resources (observations and metadata).
    """

    index_url = urljoin(germany_climate_url, str(Path(resolution) / parameter))

    resource_list = get_resource_index(index_url)

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

    return get_resource_index(germany_climate_url, "", full_url=False)


def get_measurement_parameters(resolution: str) -> List[str]:
    return [
        {"resolution": resolution, "parameter": parameter}
        for parameter in get_resource_index(
            urljoin(germany_climate_url, resolution), "", full_url=False
        )
    ]


def get_stations(resolution: str, parameter: str) -> pd.DataFrame:
    """
    Load station meta data from DWD server.
    """
    resource_list = __gather_resource_files(resolution, parameter)
    # Get directory contents.
    resource_list = [x for x in resource_list if "Beschreibung_Stationen.txt" in x]

    resource_df_list = []

    for resource_url in resource_list:
        df = get_stations_list_from_url(resource_url)
        resource_df_list.append(df)

    resource_df = pd.concat(resource_df_list, axis=0)
    resource_df.drop_duplicates(inplace=True)
    return resource_df


def get_measurement_data_from_url(url: str):
    df = pd.read_csv(
        url,
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


def get_stations_list_from_url(url: str):
    col_names = pd.read_csv(url, sep=" ", encoding="latin1", nrows=0).columns.tolist()

    df = pd.read_fwf(
        url,
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


def get_measurement_data_urls(resolution, parameter):
    available_resources = __gather_resource_files(resolution, parameter)

    # Filter by File Format
    # NOTE: We assume that all files that pass this filter are data files
    download_list = [f for f in available_resources if ".zip" in f]

    return download_list
