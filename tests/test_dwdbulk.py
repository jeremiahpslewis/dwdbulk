import random
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

import pytest
from dwdbulk.api.observations import (
    __gather_resource_files,
    get_measurement_data_from_url,
    get_measurement_data_urls,
    get_measurement_parameters,
    get_resolutions,
    get_stations,
    get_stations_list_from_url,
)
from dwdbulk.util import (
    germany_climate_url,
    get_resource_index,
    parse_htmllist,
    station_metadata,
)

measurement_parameters_10_minutes = [
    "air_temperature",
    "extreme_temperature",
    "extreme_wind",
    "precipitation",
    "solar",
    "wind",
]

measurement_parameters_hourly = [
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

measurement_parameters_daily = [
    "kl",
    "more_precip",
    "soil_temperature",
    "solar",
    "water_equiv",
    "weather_phenomena",
]

# TODO: Fill in measurement parameters for other resolutions

resolution_and_measurement_standards = {
    "10_minutes": measurement_parameters_10_minutes,
    "1_minute": ["precipitation"],
    # TODO: Below data series have different format (zipped raw & metadata); need to adapt parser
    # "annual": ["more_precip", "weather_phenomena", "kl"],
    # "daily": measurement_parameters_daily,
    # "hourly": measurement_parameters_hourly,
    # "monthly": ["more_precip", "weather_phenomena", "kl"],
    # "multi_annual": [],
    # "subdaily": [],
}


@pytest.mark.parametrize(
    "resolution",
    [k for k, v in resolution_and_measurement_standards.items() if v != []],
)
def test_parse_htmllist(resolution):
    url = urljoin(
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/",
        resolution,
    )
    r = requests.get(url)
    extracted_links = parse_htmllist(url, r.text)

    expected_links = resolution_and_measurement_standards[resolution]
    expected_links = [
        urljoin(germany_climate_url, str(Path(resolution) / link))
        for link in expected_links
    ]
    assert sorted(extracted_links) == sorted(expected_links)


def test_get_resource_index():
    url = urljoin(germany_climate_url, "10_minutes/")
    extracted_links = get_resource_index(url, "/")

    expected_links = resolution_and_measurement_standards["10_minutes"]
    expected_links = [urljoin(url, link) for link in expected_links]

    assert sorted(extracted_links) == sorted(expected_links)


@pytest.mark.parametrize(
    "resolution,parameter",
    [(k, v_i) for k, v in resolution_and_measurement_standards.items() for v_i in v],
)
def test_gather_resource_files_helper(resolution, parameter):
    files = __gather_resource_files(resolution, parameter)
    assert len([x for x in files if "Beschreibung_Stationen.txt" in x]) > 0


def test_get_resource_all():
    """
    Test that all links are returned when extension is not specified.
    """

    extracted_links = get_resource_index(germany_climate_url)

    expected_links = [
        urljoin(germany_climate_url, link)
        for link in resolution_and_measurement_standards.keys()
    ]
    assert set(expected_links).issubset(extracted_links)


def test_get_all_resolutions():
    resolutions = get_resolutions()

    assert set(resolution_and_measurement_standards.keys()).issubset(resolutions)


@pytest.mark.parametrize(
    "resolution,expected_measurement_parameters",
    [(k, v) for k, v in resolution_and_measurement_standards.items()],
)
def test_get_measurement_parameters(resolution, expected_measurement_parameters):
    # If measurement parameters not currently specified, then skip test.
    if expected_measurement_parameters == []:
        return

    expected_measurement_parameters = [
        {"resolution": resolution, "parameter": i}
        for i in expected_measurement_parameters
    ]
    extracted_measurement_parameters = get_measurement_parameters(resolution)

    for i in extracted_measurement_parameters:
        assert i in extracted_measurement_parameters

    for j in extracted_measurement_parameters:
        assert j in extracted_measurement_parameters


@pytest.mark.parametrize(
    "resolution,parameter",
    [(k, v_i) for k, v in resolution_and_measurement_standards.items() for v_i in v],
)
def test_get_stations(resolution, parameter):
    "Test fetching station data. Test randomly chooses a measurement parameter for each of the three supported time frames."
    # If measurement parameters not currently specified, then skip test.
    if parameter == []:
        return

    df = get_stations(resolution, parameter)

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


@pytest.mark.parametrize(
    "resolution,parameter",
    [(k, v_i) for k, v in resolution_and_measurement_standards.items() for v_i in v],
)
def test_get_measurement_data_urls_and_data(resolution, parameter):
    files = get_measurement_data_urls(resolution, parameter)
    assert len(files) > 0

    files_sample = random.sample(files, 2)

    for url in files_sample:
        df = get_measurement_data_from_url(url)
        df.head()

    assert set(set(["station_id", "date_start"])).issubset(df.columns)
    assert df.date_start.min() > pd.Timestamp("1700-01-01", tz="UTC")
    assert df.date_start.max() < pd.Timestamp("2200-01-01", tz="UTC")
