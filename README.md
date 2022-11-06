# Description

Examnple Python program to download last hour of buoy data from http://osmc.noaa.gov/erddap/tabledap/OSMC_Points, convert to 
BUFR and publish at https://mqtthq.com/client on topic wis2/oceans/.

## Setup / install
```
git clone https://github.com/david-i-berry/erddap-watcher.git .
docker build -t erddap-watcher .
docker run -it -v ${pwd}:/local erddap-watcher bash
cd /local
python main.py
```

## Dependencies

- Docker
- ecCodes (library + python wrapper) from ECMWF (installed via Docker)
- csv2bufr from https://github.com/wmo-im/csv2bufr (installed via Docker)

## About the name
This was named errdap-watcher as it came about via a discussion on polling an ERDDAP server via a cron job and then
publishing new mesages for the past hour. Coding got as far as writing the main code.