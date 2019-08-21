import os
import shutil
import tempfile
from pathlib import Path
from typing import List
from urllib.parse import urlparse
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests

from ..util import partitioned_df_write_to_parquet
from lxml import etree


def fetch_raw_forecast_xml(url, xml_directory_path="forecast_xml"):
    """
    Fetch weather forecast file (zipped xml) and extract xml into folder specified by xml_directory_path.
    """
    if not os.path.exists(xml_directory_path):
        os.makedirs(xml_directory_path)

    r = requests.get(url, stream=True)
    file_name = urlparse(url).path.replace("/", "__")

    if r.status_code == 200:
        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(tmpdirname + "/" + file_name, "wb") as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)

            with ZipFile(tmpdirname + "/" + file_name, "r") as zipObj:
                # Extract all the contents of zip file in current directory
                zipObj.extractall(path=xml_directory_path)
                return Path(xml_directory_path) / zipObj.namelist()[0]


def convert_xml_to_parquet(filepath, station_ids: List = None):
    """
    Convert DWD XML Weather Forecast File of Type MOSMIX_S to parquet files.
    """

    tree = etree.parse(str(filepath))
    root = tree.getroot()

    prod_items = {
        "product_id": "ProductID",
        "generating_process": "GeneratingProcess",
        "date_issued": "IssueTime",
    }

    # Get Basic Metadata
    prod_definition = root.findall(
        "kml:Document/kml:ExtendedData/dwd:ProductDefinition", root.nsmap
    )[0]
    prod_items = {
        "product_id": "ProductID",
        "generating_process": "GeneratingProcess",
        "date_issued": "IssueTime",
    }
    metadata = {
        k: prod_definition.find(f"{{{root.nsmap['dwd']}}}{v}").text
        for k, v in prod_items.items()
    }

    # Get Time Steps
    timesteps = root.findall(
        "kml:Document/kml:ExtendedData/dwd:ProductDefinition/dwd:ForecastTimeSteps",
        root.nsmap,
    )[0]
    timesteps = [pd.Timestamp(i.text) for i in timesteps.getchildren()]

    # Get Station Forecasts
    forecast_items = root.findall("kml:Document/kml:Placemark", root.nsmap)

    station_df = [
        {
            "coordinates": station_forecast.find(
                "kml:Point/kml:coordinates", root.nsmap
            ).text.split(","),
            "station_id": station_forecast.find("kml:name", root.nsmap).text,
            "station_name": station_forecast.find("kml:description", root.nsmap).text,
        }
        for station_forecast in forecast_items
    ]
    station_df = pd.DataFrame(station_df)
    station_df["geo_lon"] = station_df["coordinates"].apply(lambda x: float(x[0]))
    station_df["geo_lat"] = station_df["coordinates"].apply(lambda x: float(x[1]))
    station_df["height"] = station_df["coordinates"].apply(lambda x: float(x[2]))
    del station_df["coordinates"]

    partitioned_df_write_to_parquet(
        station_df, use_date_partitions=False, data_folder="data/forecast_stations"
    )

    for station_forecast in forecast_items:
        station_id = station_forecast.find("kml:name", root.nsmap).text

        if (station_ids is None) or station_id in station_ids:
            measurement_list = station_forecast.findall(
                "kml:ExtendedData/dwd:Forecast", root.nsmap
            )
            df = pd.DataFrame({"date_start": timesteps})

            for measurement_item in measurement_list:

                measurement_parameter = measurement_item.get(
                    f"{{{root.nsmap['dwd']}}}elementName"
                )
                measurement_string = measurement_item.getchildren()[0].text
                measurement_values = " ".join(measurement_string.split()).split(" ")
                measurement_values = [
                    np.nan if i == "-" else float(i) for i in measurement_values
                ]

                assert len(measurement_values) == len(
                    timesteps
                ), "Number of timesteps does not match number of measurement values."
                df[measurement_parameter] = measurement_values

            df["station_id"] = station_id
            for k, v in metadata.items():
                df[k] = v

            partitioned_df_write_to_parquet(df, data_folder="data/forecasts")
