from prefect import Flow, task
from prefect import Flow, task
from prefect.utilities.tasks import unmapped

from .orchestrate import (
    unlist,
    fetch_stations,
    fetch_measurement_data_locations,
    fetch_measurement_data,
)

with Flow("Fetch Full DWD Germany Observation Data") as observations_flow:
    # Fetch available resolutions
    # res_list = ["10_minutes"]

    # Fetch resolution-measurement parameter objects per resolution
    # measurement_parameters_per_res = fetch_measurement_parameters.map(res_list)
    measurement_parameters_per_res = [
        [{"resolution": "10_minutes", "parameter": "air_temperature"}]
    ]

    # Collapse list of lists to single list
    res_param_list = unlist(measurement_parameters_per_res)

    # Fetch and save station data, return list of stations for each res / cat combination
    station_list = fetch_stations.map(res_param_list)

    data_file_urls = fetch_measurement_data_locations.map(res_param_list)
    data_file_url_list = unlist(data_file_urls)


    # Fetch data
    fetch_measurement_data.map(data_file_url_list)

if __name__ == "__main__":

    observations_flow.run(executor=None)
