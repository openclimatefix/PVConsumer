# PVConsumer

[![codecov](https://codecov.io/gh/openclimatefix/PVConsumer/branch/main/graph/badge.svg?token=R0VM4YBUJS)](https://codecov.io/gh/openclimatefix/PVConsumer)

Consumer PV data from various sources


# Live

This application pull live data from PV output.org and stores it in our own database

To run the application locally
```bash
python pvconsumer/app.py
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
## 🩺 Testing

Tests are run by using the following command
```bash
docker-compose -f test-docker-compose.yml run pvconsumer
```

These sets up `postgres` in a docker container and runs the tests in another docker container.
This slightly more complicated testing framework is needed (compared to running `pytest`)
as some queries can not be fully tested on a `sqlite` database

## Environmental Variables

- DB_URL: The database url which the forecasts will be saved too
- API_KEY: API key for pvoutput.org
- SYSTEM_ID: System id for pvoutput.org
- DATA_SERVICE_URL: data service url for pvoutput.org

