import azure_service_wrapper as asw
import os
from datetime import datetime, timedelta,date

config_directory = os.getenv("HOME")+"/.aqrconf"
pollution_table = "pollution"
weather_table = "weather"
forecast_table = "forecast"
search_timestamp_format = "%Y-%m-%dT%H:%M:%SZ"
pollutants = ["PM10","PM25","NOx"]
weathers = "Temperature,Humidity,WindSpeed,WindDir".split(',')
if not os.path.exists(config_directory):
    os.makedirs(config_directory)

# Get the lastest uploaded timestamp from a given source
# Input: cache_key - name of the cache file
# Output: [Year,Days,Minutes]
def get_latest_timestamp(cache_key):
    path_to_file = config_directory + '/' + cache_key
    if not os.path.isfile(path_to_file):
        return []
    f = open(path_to_file,'r')
    result = f.read()
    f.close()
    return result.split(',')

# Input: Datetime Object.
# Output: String in the format of search timestamp
def to_search_timestamp(timestamp):
    return timestamp.strftime(search_timestamp_format)

# Input: String in the format of search timestamp
# Output: Datetime Object.
def from_search_timestamp(search_timestamp):
    return datetime.strptime(search_timestamp,search_timestamp_format)

# Input: start and end time both in Datetime Object
# Output: List generator containing all entities
def get_weather(start,end):
    start = to_search_timestamp(start)
    end = to_search_timestamp(end)
    filter_str = "SearchTimestamp ge '"  + start + "' and SearchTimestamp le '" + end + "'"
    return asw.get_entities(weather_table,filter = filter_str)

# Input: ts1, ts2: timestamps in the format of [Year,Days,Minutes]
# Output: True if ts1 represents an earlier time than ts2
def is_timestamp_earlier(ts1,ts2):
    ts1 = map(int,ts1)
    ts2 = map(int,ts2)
    return tuple(ts1) < tuple(ts2)

# Set uploaded timestamp
# Input: cache_key - name of the file to store
#        ts - timestamp in Datetime Object
def set_timestamp(cache_key,ts):
    path_to_file = config_directory + '/' + cache_key
    f = open(path_to_file,'w')
    f.write(','.join(map(str,ts)))
    f.close()

# Convert from [Year,Days,Minutes] to timestamp
def to_timestamp(year,days,minutes):
    return datetime(int(year),1,1) + timedelta(days=int(days),minutes=int(minutes))

# Convert to [Year,Days,Minutes] from timestamp
def std_time(t):
    return map(str, [t.year,(t.date()-date(t.year,1,1)).days,t.hour * 60 + t.minute])

# Upload a row
# Input: table - name of the table
#        row - the actual data row
#        force - if we want to skip the duplication check
#        heading - the row headings. Must be in the same order as row
#        cache_key - the name of cache file
#        row_keys - the RowKey in the form of a list
def upload_row(table, row, force, heading, cache_key,row_keys):
    if not force:
        force = os.getenv("FORCE")
    if not force:
        current_latest_timestamp = get_latest_timestamp(cache_key)
        if "".join(current_latest_timestamp) and is_timestamp_earlier(row[:3],current_latest_timestamp):
            return
    timestamp = to_timestamp(*row[:3])
    timestamp = to_search_timestamp(timestamp)
    partition_key = row[0]
    row_key = ",".join(map(str,row_keys))
    asw.upload_row(table,[partition_key,row_key,timestamp]+row,heading)
    if not force:
        set_timestamp(cache_key,row[:3])

def field_empty(field):
    return field == '0' or not field
def all_empty(l):
    return all(map(field_empty,l))
def upload_pollution(row,force = False,table = pollution_table):
    ## to handle empty rows
    if all_empty(row[-3:]):
        return
    heading = "PartitionKey,RowKey,SearchTimestamp,Year,Days,Minutes,Latitude,Longitude,NOx,PM10,PM25".split(',')
    upload_row(table,row,force,heading,str(row[3]) + str(row[4]),row[:5])

def upload_weather(row,force = False):
    heading = "PartitionKey,RowKey,SearchTimestamp,Year,Days,Minutes,Temperature,Humidity,Pressure,WindSpeed,WindDir,Rain".split(',')
    upload_row(weather_table,row,force,heading,"clweather",row[:3])

def upload_forecast(row,force = False):
    heading = "PartitionKey,RowKey,SearchTimestamp,Year,Days,Minutes,Temperature,Humidity,WindSpeed,WindDir".split(',')
    upload_row(forecast_table,row,force,heading,"metweather",row[:3])

# Delete rows matching a predicate from a table
def remove_rows(table,should_remove):
    all_rows = asw.get_entities(table)
    no_removed = 0
    for row in all_rows:
        if should_remove(row):
            asw.delete_entity(table,row['PartitionKey'],row['RowKey'])
            no_removed += 1
    return no_removed

# Get prediction

def get_prediction(input_row,headings,url,key_file):
    import urllib2
    import json 
    data =  {

            "Inputs": {

                    "input1":
                {
                    "ColumnNames": headings,
                    "Values": [ input_row ]
                },        },
        "GlobalParameters": {
        }
    }

    body = str.encode(json.dumps(data))

    import os
    api_key = open(os.getenv("HOME")+"/"+key_file,'r').read() 
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
def get_hourly_prediction(input_row):
    headings = ["Year", "Date", "Time", "Latitude", "Longitude", "Temperature_Hist_0", "Humidity_Hist_0", "Wind_Speed_Hist_0", "PM10_Hist_0", "PM25_Hist_0", "NOx_Hist_0", "Temperature_Curr", "Humidity_Curr", "Wind_Speed_Curr", "PM10_Curr", "PM25_Curr", "NOx_Curr"]
    url = 'https://europewest.services.azureml.net/workspaces/5b00d0b41dc34a3db5351efe7b9c72a3/services/ffc169a672f94c099bdbc7af5d09f5ef/execute?api-version=2.0'

    return get_prediction(input_row,headings,url,"web_hour_apikey")

def get_daily_prediction(input_row):
    headings = ["Year", "Date", "Latitude", "Longitude", "Temperature_Avg_Hist_0", "Humidity_Avg_Hist_0", "Wind_Speed_Avg_Hist_0", "Wind_Direction_Avg_Hist_0", "PM10_Avg_Hist_0", "PM25_Avg_Hist_0", "NOx_Avg_Hist_0", "Temperature_Avg_Curr", "Humidity_Avg_Curr", "Wind_Speed_Avg_Curr", "Wind_Direction_Avg_Curr", "PM10_Avg_Curr", "PM25_Avg_Curr", "NOx_Avg_Curr"]
    url = 'https://europewest.services.azureml.net/workspaces/5b00d0b41dc34a3db5351efe7b9c72a3/services/b0ab455c628843958ea040e0f93ad4a5/execute?api-version=2.0&details=true'
    return get_prediction(input_row,headings,url,"web_day_apikey")
