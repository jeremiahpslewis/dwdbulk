import os
import shutil
import tempfile
from typing import List
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests

from lxml import etree


def fetch_weather_forecast(url, xml_directory_path="xml_zip"):

    if not os.path.exists(xml_directory_path):
        os.makedirs(xml_directory_path)

    r = requests.get(url, stream=True)
    file_name = "__".join(url.split(".de/", 1)[1].split("/"))

    if r.status_code == 200:
        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(tmpdirname + "/" + file_name, "wb") as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)

            with ZipFile(tmpdirname + "/" + file_name, "r") as zipObj:
                # Extract all the contents of zip file in current directory
                zipObj.extractall(path=xml_directory_path)


def convert_xml_to_parquet(path, station_ids: List = None):
    """Convert DWD XML Weather Forecast File of Type MOSMIX_S to parquet files."""

    tree = etree.parse(path)
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
    timesteps = [i.text for i in timesteps.getchildren()]

    # Get Station Forecasts
    forecast_items = root.findall("kml:Document/kml:Placemark", root.nsmap)

    for station_forecast in forecast_items:
        station_id = station_forecast.find("kml:name", root.nsmap).text

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

            assert len(measurement_values) == len(
                timesteps
            ), "Number of timesteps does not match number of measurement values."
            df[measurement_parameter] = measurement_values

        df["station_id"] = station_id
        for k, v in metadata.items():
            df[k] = v
        df.replace("-", np.nan, inplace=True)
        return df
