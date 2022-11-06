import base64
from copy import deepcopy
import csv
from csv2bufr import transform
from datetime import datetime, timedelta
from io import StringIO
import json
import paho.mqtt.publish as publish
import pandas as pd
import requests

broker_url = "public.mqtthq.com"
broker_port = 1883

def pub(messages):
    if not isinstance(messages, list):
        messages = [messages]
    publish.multiple(messages, broker_url)


def main():
    with open("bufr_template.json") as fh:
        bufr_template = json.load(fh)

    endtime = datetime.now()
    starttime = endtime - timedelta(hours=1)
    t1 = starttime.strftime("%Y-%m-%dT%H:%M:%SZ")
    t2 = endtime.strftime("%Y-%m-%dT%H:%M:%SZ")
    endpoint = "http://osmc.noaa.gov/erddap/tabledap/OSMC_Points.csv?"
    properties = ["PLATFORM_CODE", "time","latitude","longitude","platform","parameter","OBSERVATION_VALUE"]
    filters = [
        'platform="WEATHER+BUOYS"',
        f'time>={t1}',
        f'time<{t2}'
    ]

    properties = "%2C".join(x for x in properties)
    filters = "%26".join(x for x in filters)

    url_ = f"{endpoint}{properties}%26{filters}"

    print(url_)

    dtypes = {
        "PLATFORM_CODE": "string",
        "time": "string",
        "latitude": "float",
        "longitude": "float",
        "platform": "string",
        "parameter": "string",
        "OBSERVATION_VALUE": "float"
    }

    # load data
    df = pd.read_csv(url_, dtype=dtypes, skiprows = [1])

    # drops rows with parameter == LOCATION
    df = df.loc[-(df['parameter']=="LOCATION"),:]

    # pivot wider
    df = df.pivot(index=["PLATFORM_CODE","time", "latitude", "longitude"], columns = "parameter", values="OBSERVATION_VALUE" )
    df.reset_index(inplace=True)

    # split date/time
    df['year'] = df.time.apply(lambda x: x[0:4])
    df['month'] = df.time.apply(lambda x: x[5:7])
    df['day'] = df.time.apply(lambda x: x[8:10])
    df['hour'] = df.time.apply(lambda x: x[11:13])
    df['minute'] = df.time.apply(lambda x: x[14:16])
    df['second'] = df.time.apply(lambda x: x[17:19])

    df = df.to_dict(orient="records")

    messages = []
    for d in df:
        # First get station identifier
        tsi = d['PLATFORM_CODE']
        time = d['time']
        # set URL for later use
        url_ = f'http://osmc.noaa.gov/erddap/tabledap/OSMC_Points.csv?&PLATFORM_CODE="{tsi}"&time={time}'
        # expand to 7 digits if only 5 (commented out as there appears to be duplicate but different data)
        # if len(tsi) == 5:
        #    tsi = tsi[0:2] + "00" + tsi[2:5]
        # now update data
        d['PLATFORM_CODE'] = int(tsi)
        # if ocean ops had a usable API we could merge in the metadata here (caching on first load)
        # ...
        # check we have data
        for elem in bufr_template['data']:
            key = elem['csv_column']
            if key in d:
                val = d[key]
            else:
                val = None
            d[key] = val
        # csv2bufr expects a string, transform to string
        fh = StringIO()
        df1 = pd.DataFrame(d, index = [0])
        df1.to_csv(fh, index=False, sep=",", na_rep="NA", quoting=csv.QUOTE_NONNUMERIC)
        # minimal metadata required, guessing WSI. This will need to be fixed
        # to map from TSI to wSI
        metadata = {
            "wigosIds":[
                {"wid": f"0-22000-0-{tsi}"}
            ]
        }
        # convert to BUFR
        res = transform(fh.getvalue(), metadata, bufr_template)

        # now publish
        for r in res:
            # make message to publish
            msg = make_message(topic = "wis2/oceans/test/",data=deepcopy(r), source_url=url_)
            # add to list for batch publishing
            messages.append(deepcopy(msg))
            # save for dev / debugging purposes
            msg_test = json.loads(msg[1])
            with open(f"./output/{msg_test['id']}.bin", "wb") as fh:
                fh.write( base64.b64decode(msg_test['properties']['content']['value']) )

    # now publish
    pub(messages)


def make_message(topic, data, source_url):
    msg = {
        "id": f"{data['_meta']['identifier']}",
        "type": "Feature",
        "version": "v04",
        "geometry": data['_meta']['geometry'],
        "properties": {
            "data_id": f"{topic}{data['_meta']['identifier']}",
            "pubtime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "content": {
                "length": None,
                "value": deepcopy(base64.b64encode(data['bufr4']).decode('utf8'))
            },
            "integrity": {"method": "md5", "value": data['_meta']['md5']},
            "wigos_station_identifier": data['_meta']['wigos_station_identifier']},
        "links": [
            {"rel": "canonical", "type": "text/csv", "href": source_url}
        ]
    }
    print(msg)
    msg = (topic, json.dumps(msg))
    return msg


if __name__ == "__main__":
    main()