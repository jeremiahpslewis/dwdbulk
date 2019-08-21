from prefect import Flow, task
from prefect.utilities.tasks import unmapped

from .orchestrate import (
    gather_forecast_urls,
    get_berlin_brandenburg_station_ids,
    process_forecast,
)

with Flow("Fetch DWD Germany Forecast Data") as forecasts_flow:
    bb_stations = get_berlin_brandenburg_station_ids()["forecasts"]
    forecast_urls = gather_forecast_urls()
    process_forecast.map(forecast_urls, unmapped(bb_stations))


if __name__ == "__main__":
    forecasts_flow.run(executor=None)
