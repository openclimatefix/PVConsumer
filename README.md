# PVConsumer

<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

[![codecov](https://codecov.io/gh/openclimatefix/PVConsumer/branch/main/graph/badge.svg?token=R0VM4YBUJS)](https://codecov.io/gh/openclimatefix/PVConsumer)

Consumer PV data from pvoutput.org. The idea is to expand to different data sources in the future.


# Live

This application pulls live data from PV output.org and stores it in a database

## Dependencies

* [poetry][poetry]

## Run locally

To run the application locally

```bash
# Install the python dependencies
poetry install

poetry run pvconsumer/app.py
```

## Logic

The app has the following high-level strucuture
```mermaid
  graph TD;
      A[1. Get PV system]-->B;
      B[2. Filter PV Systems]-->C;
      C[3. Pull Data]-->D[4. Save data];
```

1. Get PV System
```mermaid
   graph TD
    A0(Start) --> A1
    A0(Start) --> A2
    A1[Load local PV systems] --> A3{Are all PV system in the database}
    A2[Load Database PV systems] --> A3
    A3 --> |No| A4[Load the extra <br/> PV systems from pvoutput.org]
    A3 --> |yes| A5(Finish)
    A4 --> A5
```

2. Filter PV Systems
```mermaid
   graph TD
    B0(Start) --> B1{Is there any PV data in <br/> our database for this PV system?}
    B1 --> |No| B2[Keep PV system]
    B1--> |yes| B3{Is there any more PV data, <br/> from pv output.org, <br/>available for this PV system?}
    B3 --> |yes| B2
    B3 --> |No| B5[Dischagre PV system]
    B2 --> B6(Finish)
    B5 --> B6
```
3. Pull Data
```mermaid
   graph TD
    C0(Start) --> C1[Pull Data from pvoutput.prg]
    C1 --> C2{Is this data <br/> in our database already?}
    C2 --> |yes| C3[Keep PV data]
    C2 --> |No| C4[Dischagre PV data]
    C3 --> C5(Finish)
    C4 --> C5

```

## Code style

Format the python codebase **in place**.

```bash
make format
```

Test that the codebase is formatted

```bash
make lint
```


## 🩺 Testing

Run only the unit tests

```bash
poetry run pytest tests/unittest
```

Run all the tests (including the "integration" tests that require credentials to call an external API)

```bash
poetry run pytest tests
```

## Environmental Variables

- DB_URL: The database url which the forecasts will be saved too
- API_KEY: API key for pvoutput.org
- SYSTEM_ID: System id for pvoutput.org
- DATA_SERVICE_URL: data service url for pvoutput.org
- DB_URL: Save in database to the pv database
- DB_URL_FORECAST: Let the database not that this service has run, `input_data_last_updatded` table
- DB_URL_PV_SITE: PV Site database

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center"><a href="http://lostcoding.com"><img src="https://avatars.githubusercontent.com/u/20285369?v=4?s=100" width="100px;" alt="Brandon Ly"/><br /><sub><b>Brandon Ly</b></sub></a><br /><a href="https://github.com/openclimatefix/PVConsumer/commits?author=branberry" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!

[poetry]: https://python-poetry.org/
