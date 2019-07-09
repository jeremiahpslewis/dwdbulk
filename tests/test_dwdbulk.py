import random

import pandas as pd
import pytest
import requests
from dwdbulk.api import (
    __gather_resource_files,
    get_measurement_categories,
    get_measurement_data_from_uri,
    get_measurement_data_uris,
    get_resolutions,
    get_stations,
    get_stations_list_from_uri,
)
from dwdbulk.util import (
    germany_climate_uri,
    get_resource_index,
    https,
    parse_htmllist,
    station_metadata,
)

measurement_categories_10_minutes = [
    "air_temperature",
    "extreme_temperature",
    "extreme_wind",
    "precipitation",
    "solar",
    "wind",
]

measurement_categories_hourly = [
    "air_temperature",
    "cloud_type",
    "cloudiness",
    "dew_point",
    "precipitation",
    "pressure",
    "soil_temperature",
    "solar",
    "sun",
    "visibility",
    "wind",
    "wind_synop",
]

measurement_categories_daily = [
    "kl",
    "more_precip",
    "soil_temperature",
    "solar",
    "water_equiv",
    "weather_phenomena",
]

# TODO: Fill in measurement categories for other resolutions

resolution_and_measurement_standards = {
    "10_minutes": measurement_categories_10_minutes,
    "1_minute": ["precipitation"],
    # TODO: Below data series have different format (zipped raw & metadata); need to adapt parser
    # "annual": ["more_precip", "weather_phenomena", "kl"],
    # "daily": measurement_categories_daily,
    # "hourly": measurement_categories_hourly,
    # "monthly": ["more_precip", "weather_phenomena", "kl"],
    # "multi_annual": [],
    # "subdaily": [],
}


@pytest.mark.parametrize(
    "resolution",
    [k for k, v in resolution_and_measurement_standards.items() if v != []],
)
def test_parse_htmllist(resolution):
    url = f"opendata.dwd.de/climate_environment/CDC/observations_germany/climate/{resolution}"
    r = requests.get(https(url))
    link_list = parse_htmllist(url, r.text)

    extracted_links = [str(link) for link in link_list]

    expected_links = resolution_and_measurement_standards[resolution]
    expected_links = [
        str(germany_climate_uri / resolution / link) for link in expected_links
    ]
    assert sorted(extracted_links) == sorted(expected_links)


def test_get_resource_index():
    url = germany_climate_uri / "10_minutes/"
    link_list = get_resource_index(url, "/")

    extracted_links = [str(link) for link in link_list]

    expected_links = resolution_and_measurement_standards["10_minutes"]
    expected_links = [str(url / link) for link in expected_links]

    assert sorted(extracted_links) == sorted(expected_links)


@pytest.mark.parametrize(
    "resolution,category",
    [(k, v_i) for k, v in resolution_and_measurement_standards.items() for v_i in v],
)
def test_gather_resource_files_helper(resolution, category):
    files = __gather_resource_files(resolution, category)
    assert len([x for x in files if "Beschreibung_Stationen.txt" in str(x)]) > 0


def test_get_resource_all():
    """
    Test that all links are returned when extension is not specified.
    """

    extracted_links = get_resource_index(germany_climate_uri)
    extracted_links = [str(link) for link in extracted_links]

    expected_links = [
        str(germany_climate_uri / link)
        for link in resolution_and_measurement_standards.keys()
    ]
    assert set(expected_links).issubset(extracted_links)


def test_get_all_resolutions():
    resolutions = get_resolutions()

    assert set(resolution_and_measurement_standards.keys()).issubset(resolutions)


@pytest.mark.parametrize(
    "resolution,expected_measurement_categories",
    [(k, v) for k, v in resolution_and_measurement_standards.items()],
)
def test_get_measurement_categories(resolution, expected_measurement_categories):
    # If measurement categories not currently specified, then skip test.
    if expected_measurement_categories == []:
        return

    expected_measurement_categories = [
        {"resolution": resolution, "category": i}
        for i in expected_measurement_categories
    ]
    extracted_measurement_categories = get_measurement_categories(resolution)

    for i in extracted_measurement_categories:
        assert i in extracted_measurement_categories

    for j in extracted_measurement_categories:
        assert j in extracted_measurement_categories


@pytest.mark.parametrize(
    "resolution,category",
    [(k, v_i) for k, v in resolution_and_measurement_standards.items() for v_i in v],
)
def test_get_stations(resolution, category):
    "Test fetching station data. Test randomly chooses a measurement category for each of the three supported time frames."
    # If measurement categories not currently specified, then skip test.
    if category == []:
        return

    df = get_stations(resolution, category)

    assert df.date_start.min() > pd.Timestamp("1700-01-01", tz="UTC")
    assert df.date_start.max() < pd.Timestamp("2200-01-01", tz="UTC")
    states = [
        "Baden-Württemberg",
        "Nordrhein-Westfalen",
        "Hessen",
        "Bayern",
        "Niedersachsen",
        "Sachsen-Anhalt",
        "Rheinland-Pfalz",
        "Sachsen",
        "Mecklenburg-Vorpommern",
        "Schleswig-Holstein",
        "Brandenburg",
        "Thüringen",
        "Saarland",
        "Berlin",
        "Bremen",
        "Hamburg",
        "Tirol",
    ]
    assert set(df.state.unique()).issubset(set(states))

    assert df.height.min() >= 0
    assert df.geo_lat.min() >= -90
    assert df.geo_lat.max() <= 90

    assert df.geo_lon.min() >= -180
    assert df.geo_lon.max() <= 180

    expected_colnames = [v["name"] for k, v in station_metadata.items()]
    assert sorted(df.columns) == sorted(expected_colnames)
    assert df.shape[0] > 5


def test_https_helper():
    link = "https://google.com"
    assert https(link) == link

    link = "google.com"
    assert https(link) == "https://" + link


@pytest.mark.parametrize(
    "resolution,category",
    [(k, v_i) for k, v in resolution_and_measurement_standards.items() for v_i in v],
)
def test_get_measurement_data_uris_and_data(resolution, category):
    files = get_measurement_data_uris(resolution, category)
    assert len(files) > 0

    files_sample = random.sample(files, 2)

    for uri in files_sample:
        df = get_measurement_data_from_uri(uri)
        df.head()

    assert set(set(["station_id", "date_start"])).issubset(df.columns)
    assert df.date_start.min() > pd.Timestamp("1700-01-01", tz="UTC")
    assert df.date_start.max() < pd.Timestamp("2200-01-01", tz="UTC")
