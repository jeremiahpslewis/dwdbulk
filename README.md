# dwdbulk


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

## Run Bulk Download

```sh
docker-compose -f docker-orchestrate.yml up --build
```


## Licenses


### Code license

Licensed under the MIT license. See file ``LICENSE`` for details.

### Data license

The DWD has information about their re-use policy in
[German](https://www.dwd.de/DE/service/copyright/copyright_node.html) and
[English](https://www.dwd.de/EN/service/copyright/copyright_node.html).

## Run tests locally

`docker-compose -f compose/docker-test.yml --project-directory . -p dwdbulk-tests up --build`

## Credits

Thanks to [Andreas Motl](https://github.com/amotl), [Marian Steinbach](https://github.com/marians), [Philipp Klaus](https://github.com/pklaus) and all people from [DWD](https://www.dwd.de/). This project is based on [dwdbulk2](https://github.com/hiveeyes/dwdbulk2).

Source for XSLT transform: Source: http://www.wetterstationen.info/forum/neues-board/mosmix-vorhersagen-im-neuen-format/
## Changelog
=========
See file [CHANGES.md]().
