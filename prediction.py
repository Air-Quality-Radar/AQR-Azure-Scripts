#!/usr/bin/env python
from download import gps,std_time
from datetime import datetime,timedelta
import AQR
import azure_service_wrapper as asw
def get_prediction(input_row):
    import urllib2
    import json 

    data =  {

            "Inputs": {

                    "input1":
                {
                    "ColumnNames": ["Year", "Date", "Time", "Latitude", "Longitude", "Temperature_Hist_0", "Humidity_Hist_0", "Wind_Speed_Hist_0", "PM10_Hist_0", "PM25_Hist_0", "NOx_Hist_0", "Temperature_Curr", "Humidity_Curr", "Wind_Speed_Curr", "PM10_Curr", "PM25_Curr", "NOx_Curr"],
                    "Values": [ input_row ]
                },        },
        "GlobalParameters": {
        }
    }

    body = str.encode(json.dumps(data))

    url = 'https://europewest.services.azureml.net/workspaces/5b00d0b41dc34a3db5351efe7b9c72a3/services/ffc169a672f94c099bdbc7af5d09f5ef/execute?api-version=2.0'

    import os
    api_key = open(os.getenv("HOME")+"/"+"web_apikey",'r').read() # Replace this with the API key for the web service
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

    req = urllib2.Request(url, body, headers) 

    try:
        response = urllib2.urlopen(req)

        # If you are using Python 3+, replace urllib2 with urllib.request in the above code:
        # req = urllib.request.Request(url, body, headers) 
        # response = urllib.request.urlopen(req)

        result = response.read()
        return json.loads(result)['Results']["output1"]["value"]["Values"][0]
    except urllib2.HTTPError, error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the request ID and the timestamp, which are useful for debugging the failure
        print(error.info())

        print(json.loads(error.read()))

def get_next_row(current_row,weather):
    ## need to deal with no weather data
    missing = [False for x in current_row]
    for i,v in enumerate(current_row):
        if not v:
            missing[i] = True
            current_row[i] = '0'

    current_time = AQR.to_timestamp(*current_row[:3])
    location = current_row[3:5]
    next_time = current_time + timedelta(hours=1)

    current_weather = weather[current_time]
    next_weather = weather[next_time]

    def select_cols(weather_row):
        return [weather_row["Temperature"],weather_row["Humidity"],weather_row["WindSpeed"]]
    input_row = AQR.std_time(next_time) + location + select_cols(current_weather) + current_row[5:] + select_cols(next_weather) + [0,0,0]
    # print input_row
    output_row = get_prediction(input_row)
    for i,v in enumerate(missing):
        if v:
            output_row[i] = ""
    return output_row


def generate_prediction(until_time):
    ## VERY INACCURATE

    weather_start = datetime.now()
    for lat,lon in gps.values():
        last_uploaded = AQR.to_timestamp(*AQR.get_latest_timestamp(str(lat)+str(lon)))
        if last_uploaded < weather_start:
            weather_start = last_uploaded
        # print last_uploaded,weather_start
    weather_end = AQR.to_timestamp(*AQR.get_latest_timestamp("clweather"))
    assert(weather_start < weather_end)

    weather_raw = AQR.get_weather(weather_start,weather_end)
    weather = dict()
    for row in weather_raw:
        timestamp = AQR.from_search_timestamp(row["SearchTimestamp"])
        weather[timestamp] = row
        print timestamp

    # print weather_end,weather_start
    for lat,lon in gps.values():
        last_uploaded = AQR.to_timestamp(*AQR.get_latest_timestamp(str(lat)+str(lon)))
        if last_uploaded >= weather_end:
            continue
        last_row = asw.get_entities(AQR.pollution_table,"SearchTimestamp eq '" + AQR.to_search_timestamp(last_uploaded) + "' and Latitude eq " + str(lat))
        for row in last_row:
            last_row = row
            break
        else:
            print "Empty!"
            return
        current_row = AQR.std_time(last_uploaded) + [lat,lon] + [last_row["PM10"],last_row["PM25"],last_row["NOx"]]
        while AQR.to_timestamp(*current_row[:3]) < weather_end:
            current_row = get_next_row(current_row,weather)
            AQR.upload_pollution(current_row[:-3] + [current_row[-1]]+current_row[-3:-1],table="prediction")



if __name__ == "__main__":
    # print get_prediction([0 for x in range(17)])
    generate_prediction(datetime.now())
