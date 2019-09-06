import random
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

import pytest
from dwdbulk.api import observations
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
    get_stations_lookup,
    parse_htmllist,
    station_metadata,
    y2k_date_parser,
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
        urljoin(germany_climate_url, str(Path(resolution) / link) + "/")
        for link in expected_links
    ]
    assert sorted(extracted_links) == sorted(expected_links)


def test_get_resource_index():
    url = urljoin(germany_climate_url, "10_minutes/")
    extracted_links = get_resource_index(url, "/")

    expected_links = resolution_and_measurement_standards["10_minutes"]
    expected_links = [urljoin(url, link + "/") for link in expected_links]

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
        urljoin(germany_climate_url, link + "/")
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

    assert set(["station_id", "date_start"]).issubset(df.columns)
    assert df.date_start.min() > pd.Timestamp("1700-01-01", tz="UTC")
    assert df.date_start.max() < pd.Timestamp("2200-01-01", tz="UTC")


def test_observations_stations_available_in_lookup():
    """Test all stations available in lookup are also available in observations data."""
    # NOTE: Different stations available for different resolutions and parameters; need to adjust this...
    resolution = "10_minutes"
    parameter = "air_temperature"
    df_stations = get_stations_lookup()
    df_stations.observations_station_id
    available_stations = (
        get_stations(resolution, parameter)["station_id"].unique().tolist()
    )
    assert set(df_stations.observations_station_id.unique().tolist()).issubset(
        set(available_stations)
    )


def test_observations_get_data_all():
    """Test that get_data returns reasonable results for all data, for a single station."""

    resolution = "10_minutes"
    parameter = "air_temperature"
    df_stations = get_stations_lookup()
    station_ids = df_stations.observations_station_id.head(1).tolist()
    df = observations.get_data(
        parameter,
        station_ids=station_ids,
        date_start=None,
        date_end=None,
        resolution=resolution,
    )

    date_end = pd.Timestamp.now(tz="UTC").floor("h") - pd.Timedelta("1.5h")

    assert sorted(station_ids), sorted(df.station_id.unique().tolist())
    assert df.duplicated(subset=["station_id", "date_start"]).sum() == 0
    assert df.date_start.min() < pd.Timestamp("2000-01-01", tz="UTC")
    assert df.date_start.max() >= date_end


def test_observations_get_data_recent():
    """Test that get_data returns reasonable results for recent data, for a subset of stations."""
    resolution = "10_minutes"
    parameter = "air_temperature"
    date_end = pd.Timestamp.now(tz="UTC").floor("h")
    date_start = date_end - pd.Timedelta("5 days")
    df_stations = get_stations_lookup()
    station_ids = df_stations.observations_station_id.sample(10).unique().tolist()
    df = observations.get_data(
        parameter,
        station_ids=station_ids,
        date_start=date_start,
        date_end=date_end,
        resolution=resolution,
    )

    assert sorted(station_ids), sorted(df.station_id.unique().tolist())
    assert df.duplicated(subset=["station_id", "date_start"]).sum() == 0
    assert df.date_start.min() == date_start
    assert df.date_start.max() >= date_end - pd.Timedelta("120 minutes")


def test_y2k_datetime_parser_for_pre_2000_dates():
    date_test = y2k_date_parser(
        ["201801010130", "199901010230", "199906010330", "200603260200", "199509240200"]
    )

    date_expect = (
        pd.to_datetime(
            [
                "2018-01-01 01:30",
                "1999-01-01 01:30",
                "1999-06-01 01:30",
                "2006-03-26 02:00",
                pd.NaT,
            ],
            utc=True,
        )
        .to_series(keep_tz=True)
        .reset_index(drop=True)
    )

    pd.testing.assert_series_equal(date_test, date_expect)
