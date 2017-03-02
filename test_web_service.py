import urllib2

import json 

data =  {

        "Inputs": {

                "input1":
                {
                    "ColumnNames": ["Year", "Date", "Time", "Latitude", "Longitude", "Temperature_Hist_0", "Humidity_Hist_0", "Pressure_Hist_0", "Wind_Speed_Hist_0", "Wind_Direction_Hist_0", "Rainfall_Hist_0", "PM10_Hist_0", "PM25_Hist_0", "NOx_Hist_0", "Temperature_Hist_1", "Humidity_Hist_1", "Pressure_Hist_1", "Wind_Speed_Hist_1", "Wind_Direction_Hist_1", "Rainfall_Hist_1", "PM10_Hist_1", "PM25_Hist_1", "NOx_Hist_1", "Temperature_Hist_2", "Humidity_Hist_2", "Pressure_Hist_2", "Wind_Speed_Hist_2", "Wind_Direction_Hist_2", "Rainfall_Hist_2", "PM10_Hist_2", "PM25_Hist_2", "NOx_Hist_2", "Temperature_Curr", "Humidity_Curr", "Pressure_Curr", "Wind_Speed_Curr", "Wind_Direction_Curr", "Rainfall_Curr", "PM10_Curr", "PM25_Curr", "NOx_Curr"],
                    "Values": [ [ "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0" ]]
                },        },
            "GlobalParameters": {
}
    }

body = str.encode(json.dumps(data))

url = 'https://europewest.services.azureml.net/workspaces/5b00d0b41dc34a3db5351efe7b9c72a3/services/ef0a8df103ab41cf9586beee4a5f6327/execute?api-version=2.0'
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

    # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
    print(error.info())

    print(json.loads(error.read()))
