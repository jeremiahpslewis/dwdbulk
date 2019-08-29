# dwdbulk: Easy Access to the World of Open German Weather Data

## What is it?

**dwdbulk** is a library that enables users to access open German weather data provided by the Deutsche Wetterdienst (DWD). It aspires to acheive the following objectives:

- Comprehensive: Covers all relevant data made available by DWD.
- Accessible: With minimal code and similar interfaces, users can access full longitudinal span for weather stations, both for forecasts and historical observations; similarly users can easily access cross-sectional data for a Germany-wide view.
- Consistent: Data fields across different time frames and datasets are aligned (e.g. latitude and longitude always in decimal format).
- Python 3.7+ & Pandas: Uses 'modern' Python. Does not aspire to have a CLI or other interfaces. Is not backed by a database. Yields data in DataFrame format.
- Linux: Given prevalence of containers and the potential maintenance overhead of Mac & Windows, only Linux is targeted for support.

## Library

[![Build Status](https://dev.azure.com/jlewis91/dwdbulk/_apis/build/status/jlewis91.dwdbulk?branchName=master)](https://dev.azure.com/jlewis91/dwdbulk/_build/latest?definitionId=1&branchName=master)

Python client to access weather data from Deutscher Wetterdienst
(`DWD <https://www.dwd.de/>`__), the federal meteorological service in
Germany.

## Installation

`pip install dwdbulk`

## Usage as library

```python

...
...

```

## Licenses


### Code license

Licensed under the MIT license. See file ``LICENSE`` for details.

### Data license

The DWD has information about their re-use policy in
[German](https://www.dwd.de/DE/service/copyright/copyright_node.html) and
[English](https://www.dwd.de/EN/service/copyright/copyright_node.html).


## Credits

Thanks to [Andreas Motl](https://github.com/amotl), [Marian Steinbach](https://github.com/marians), [Philipp Klaus](https://github.com/pklaus) and all people from [DWD](https://www.dwd.de/). This project is based on [dwdbulk2](https://github.com/hiveeyes/dwdbulk2).

## Changelog
=========
See file [CHANGES.md]().
