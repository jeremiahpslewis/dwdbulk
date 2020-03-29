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

from lxml import etree

from ..util import get_resource_index, mosmix_s_forecast_url


def fetch_raw_forecast_xml(url, directory_path):
    """
    Fetch weather forecast file (zipped xml) and extract xml into folder specified by xml_directory_path.
    """
    directory_path = Path(directory_path)

    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    r = requests.get(url, stream=True)
    file_name = urlparse(url).path.replace("/", "__")

    if r.status_code == 200:
        with open(directory_path / file_name, "wb") as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)

        with ZipFile(directory_path / file_name, "r") as zipObj:
            # Extract all the contents of zip file in current directory
            zipObj.extractall(path=directory_path)
            return directory_path / zipObj.namelist()[0]


def convert_xml_to_pandas(
    filepath,
    station_ids: List = None,
    parameters: List = None,
    return_station_data=False,
):
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

    metadata = {
        k: prod_definition.find(f"{{{root.nsmap['dwd']}}}{v}").text
        for k, v in prod_items.items()
    }
    metadata["date_issued"] = pd.Timestamp(metadata["date_issued"])

    # Get Time Steps
    timesteps = root.findall(
        "kml:Document/kml:ExtendedData/dwd:ProductDefinition/dwd:ForecastTimeSteps",
        root.nsmap,
    )[0]
    timesteps = [pd.Timestamp(i.text) for i in timesteps.getchildren()]

    # Get Per Station Forecasts
    forecast_items = root.findall("kml:Document/kml:Placemark", root.nsmap)

    df_list = []

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

                if parameters is None or measurement_parameter in parameters:

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

            df_list.append(df)

    df = pd.concat(df_list, axis=0)

    if return_station_data:
        station_df = [
            {
                "coordinates": station_forecast.find(
                    "kml:Point/kml:coordinates", root.nsmap
                ).text.split(","),
                "station_id": station_forecast.find("kml:name", root.nsmap).text,
                "station_name": station_forecast.find(
                    "kml:description", root.nsmap
                ).text,
            }
            for station_forecast in forecast_items
        ]
        station_df = pd.DataFrame(station_df)
        station_df["geo_lon"] = station_df["coordinates"].apply(lambda x: float(x[0]))
        station_df["geo_lat"] = station_df["coordinates"].apply(lambda x: float(x[1]))
        station_df["height"] = station_df["coordinates"].apply(lambda x: float(x[2]))
        del station_df["coordinates"]

        return df, station_df
    else:
        return df


def get_data(station_ids=None, parameters=None):
    """Fetch weather forecast data (KML/MOSMIX_S dataset).
    Parameters
    ----------
    station_ids : List
        If not None, station_ids are a list of station ids for which data is desired. If None, data for all stations is returned.

    parameters: List
        If not None, list of parameters, per MOSMIX definition, here: https://www.dwd.de/DE/leistungen/opendata/help/schluessel_datenformate/kml/mosmix_elemente_pdf.pdf?__blob=publicationFile&v=2
    run_checks :
        Checks that all aspects of the data request are valid.

    """
    if station_ids:
        assert isinstance(station_ids, list), "station_ids must be None or a list"
        station_ids = [str(station_id) for station_id in station_ids]

    if parameters:
        assert isinstance(parameters, list), "parameters must be None or a list"
        parameters = [str(param) for param in parameters]

    urls = get_resource_index(mosmix_s_forecast_url)

    df_list = []
    for url in urls:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            kml_path = fetch_raw_forecast_xml(url, tmp_dir_name)
            df = convert_xml_to_pandas(
                kml_path, station_ids, parameters, return_station_data=False
            )

        df_list.append(df)

    df = pd.concat(df_list, axis=0)

    return df
