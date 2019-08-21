import argparse
import logging
import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import pkg_resources
import requests

log = logging.getLogger(__name__)

# DWD CDC HTTP server.
baseuri = "https://opendata.dwd.de/climate_environment/CDC/"

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
germany_climate_uri = urljoin(baseuri, "observations_germany/climate/")


def setup_logging(level=logging.INFO):
    log_format = "%(asctime)-15s [%(name)-20s] %(levelname)-7s: %(message)s"
    logging.basicConfig(format=log_format, stream=sys.stderr, level=level)


def parse_htmllist(baseurl, content, extension=None, full_uri=True):
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

    if full_uri:
        return [urljoin(baseurl + "/", path) for path in paths]
    else:
        return [path.rstrip("/") for path in paths]


def get_resource_index(uri, extension="", full_uri=True):
    """
    Extract link list from HTML, given a URI

    :params str uri: Uri of a webpage with simple HTML link list
    :params str extension: String that should be matched in the link list; if "", all are returned
    """

    log.info("Requesting %s", uri)
    response = requests.get(uri)
    if response.status_code != 200:
        raise ValueError(f"Fetching resource {uri} failed")
    resource_list = parse_htmllist(uri, response.text, extension, full_uri)
    return resource_list


def partitioned_df_write_to_parquet(df, data_folder="data/", use_date_partitions=True):
    """Write dataframe to parquet."""
    partition_cols = None

    if use_date_partitions:
        partition_cols = ["date_start__year", "date_start__month", "date_start__day"]
        df["date_start__year"] = df.date_start.dt.year
        df["date_start__month"] = df.date_start.dt.month
        df["date_start__day"] = df.date_start.dt.day

    df["date_accessed"] = pd.Timestamp.today()

    df.to_parquet(
        data_folder,
        partition_cols=partition_cols,
        index=False,
        allow_truncated_timestamps=True,
    )


def get_observations_forecasts_lookup():
    """Return station lookup."""
    csv_file = pkg_resources.resource_filename("dwdbulk", "station_lookup.csv")
    return pd.read_csv(csv_file)
