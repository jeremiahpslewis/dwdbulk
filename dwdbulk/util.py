import argparse
import os
import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import pkg_resources
import requests


# DWD CDC HTTP server.
baseurl = "https://opendata.dwd.de/"

station_metadata = {
    "Stations_id": {"name": "station_id", "type": "str"},
    "von_datum": {"name": "date_start", "type": "date", "format": "%Y%m%d"},
    "bis_datum": {"name": "date_end", "type": "date", "format": "%Y%m%d"},
    "Stationshoehe": {"name": "height", "type": "int64"},
    "geoBreite": {"name": "geo_lat", "type": "float64"},
    "geoLaenge": {"name": "geo_lon", "type": "float64"},
    "Stationsname": {"name": "name", "type": "str"},
    "Bundesland": {"name": "state", "type": "str"},
}

station_colnames_kv = {k: v["name"] for k, v in station_metadata.items()}
station_coltypes_kv = {
    k: v["type"] for k, v in station_metadata.items() if v["type"] is not "date"
}
station_datetypes_kv = [k for k, v in station_metadata.items() if v["type"] is "date"]

measurement_metadata = {
    "STATIONS_ID": {"name": "station_id", "type": "str"},
    "MESS_DATUM": {"name": "date_start", "type": "date", "format": "%Y%m%d%H%M"},
    "QN": {"name": "QN", "type": "int64"},
    "PP_10": {"name": "PP_10", "type": "float64"},
    "TT_10": {"name": "TT_10", "type": "float64"},
    "TM5_10": {"name": "TM5_10", "type": "float64"},
    "RF_10": {"name": "RF_10", "type": "float64"},
    "TD_10": {"name": "TD_10", "type": "float64"},
}

measurement_colnames_kv = {k: v["name"] for k, v in measurement_metadata.items()}
measurement_coltypes_kv = {
    k: v["type"] for k, v in measurement_metadata.items() if v["type"] is not "date"
}
measurement_datetypes_kv = [
    k for k, v in measurement_metadata.items() if v["type"] is "date"
]


# Observations in Germany.
germany_climate_url = urljoin(
    baseurl, "climate_environment/CDC/observations_germany/climate/"
)
mosmix_s_forecast_url = urljoin(
    baseurl, "weather/local_forecasts/mos/MOSMIX_S/all_stations/kml/"
)


def parse_htmllist(baseurl, content, extension=None, full_url=True):
    class ListParser(HTMLParser):
        def __init__(self):
            HTMLParser.__init__(self)
            self.data = []

        def handle_starttag(self, tag, attrs):
            if tag == "a":
                for attr in attrs:
                    if attr[0] == "href" and attr[1] != "../":
                        self.data.append(attr[1])

    parser = ListParser()
    parser.feed(content)
    paths = parser.data
    parser.close()

    if extension:
        paths = [path for path in paths if extension in path]

    if full_url:
        return [urljoin(baseurl + "/", path) for path in paths]
    else:
        return [path.rstrip("/") for path in paths]


def get_resource_index(url, extension="", full_url=True):
    """
    Extract link list from HTML, given a url

    :params str url: url of a webpage with simple HTML link list
    :params str extension: String that should be matched in the link list; if "", all are returned
    """

    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Fetching resource {url} failed")
    resource_list = parse_htmllist(url, response.text, extension, full_url)
    return resource_list


def get_stations_lookup():
    """Return station lookup."""
    csv_file = pkg_resources.resource_filename("dwdbulk", "station_lookup.csv")
    return pd.read_csv(csv_file, dtype=str)
