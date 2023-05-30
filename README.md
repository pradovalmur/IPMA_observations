Overview
========

IPMA is the acronym for Portuguese Institute of the Sea and the Atmosphere. This project uses two API's made available by this institute, where we have data from atmospheric sensors installed throughout mainland Portugal and Islands (Azores and Madeira).

These data are stored in an instance of Postgres (running on docker), as well as Astronomer to orchestrate the data collection.

Project Contents
================

Stations:

https://api.ipma.pt/open-data/observation/meteorology/stations/stations.json

Result (formato json): [ { "geometry": { "type": "Point", "coordinates": [-7.821, 37.033] }, "type": "Feature", "properties": { "idEstacao": 1210881, "localEstacao": "Olh\u00e3o, EPPO" } }, ...]
coordinates: coordinate geographic of station [longitude, latitude] (degrees decimais)
idEstacao: id of station
localEstacao: location of station

DAG: get_stations.py

Observations:

https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json
Notes: Hourly update rate. (value "-99.0" = nodata)

Result (json format): [{ "{YYYY-mm-ddThh:mi}": { "{Stationid}": { "WindintensityKM": 0.0, "temperature": 7.7, "WindDirectidid": 3, "Accumulated prec": 0.0 , "wind intensity": 0.0, "humidity": 89.0, "pressure": -99.0, "radiation": -99.0 }, ...}}]
YYYY-mm-ddThh:mi: observation date/time
idEstacao: station identifier (see auxiliary service "List of weather station identifiers")
windintensityKM: wind intensity recorded at a height of 10 meters (km/h)
temperature: air temperature recorded at a height of 1.5 meters, hourly average (ºC)
idDireccVento: wind direction class to the prevailing wind direction recorded at 10 meters height (0: no direction, 1 or 9: "N", 2: "NE", 3: "E", 4: "SE", 5: "S", 6: "SW", 7: "W", 8: "NW")
PrecAccumulated: precipitation recorded at a height of 1.5 meters, accumulated hourly value (mm)
wind intensity: wind intensity recorded at a height of 10 meters (m/s)
humidity: relative humidity recorded at a height of 1.5 meters, hourly average (%)
pressure: atmospheric pressure, reduced to mean sea level (MSL), hourly average (hPa)
radiation: solar radiation (kJ/m2)

DAG: get_observations.py