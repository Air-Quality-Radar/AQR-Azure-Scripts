from azure.storage.table import TableService, Entity
import os
from datetime import datetime, timedelta
apikey = open(os.getenv("HOME")+'/'+'apikey','r').read()

table_service = TableService(account_name='aqrstorage', account_key=apikey)

config_directory = os.getenv("HOME")+"/.aqrconf"
pollution_table = "pollution"
weather_table = "weather"
forecast_table = "forecast"
search_timestamp_format = "%Y-%m-%dT%H:%M:%SZ"
if not os.path.exists(config_directory):
    os.makedirs(config_directory)

def create_table(table):
    table_service.create_table(table)
def delete_table(table):
    table_service.delete_table(table)
def get_entities(table_name, filter="",num_results=None):
    return table_service.query_entities(table_name, filter = filter,num_results = num_results)
def delete_entity(table,PartitionKey, RowKey):
    table_service.delete_entity(table,PartitionKey,RowKey)
def upload_row(table_name,row,heading):
    assert(len(row) == len(heading))
    entity = dict()
    for i,v in enumerate(row):
        entity[heading[i]] = v
    table_service.insert_or_replace_entity(table_name,entity)

def get_latest_timestamp(cache_key):
    path_to_file = config_directory + '/' + cache_key
    if not os.path.isfile(path_to_file):
        return []
    f = open(path_to_file,'r')
    result = f.read()
    f.close()
    return result.split(',')

def to_search_timestamp(timestamp):
    return timestamp.strftime(search_timestamp_format)
def from_search_timestamp(search_timestamp):
    return datetime.strptime(search_timestamp,search_timestamp_format)
def get_weather(start,end):
    start = to_search_timestamp(start)
    end = to_search_timestamp(end)
    filter_str = "SearchTimestamp ge '"  + start + "' and SearchTimestamp le '" + end + "'"
    return get_entities(weather_table,filter = filter_str)

def is_timestamp_earlier(ts1,ts2):
    ts1 = map(int,ts1)
    ts2 = map(int,ts2)
    return tuple(ts1) < tuple(ts2)

def set_timestamp(cache_key,ts):
    path_to_file = config_directory + '/' + cache_key
    f = open(path_to_file,'w')
    f.write(','.join(map(str,ts)))
    f.close()

def to_timestamp(year,days,minutes):
    return datetime(int(year),1,1) + timedelta(days=int(days),minutes=int(minutes))

def upload_row_wrapper(table, row, force, heading, cache_key,row_keys):
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
    upload_row(table,[partition_key,row_key,timestamp]+row,heading)
    if not force:
        set_timestamp(cache_key,row[:3])

def upload_pollution(row,force = False,table = pollution_table):
    heading = "PartitionKey,RowKey,SearchTimestamp,Year,Days,Minutes,Latitude,Longitude,NOx,PM10,PM25".split(',')
    upload_row_wrapper(table,row,force,heading,str(row[3]) + str(row[4]),row[:5])

def upload_weather(row,force = False):
    heading = "PartitionKey,RowKey,SearchTimestamp,Year,Days,Minutes,Temperature,Humidity,Pressure,WindSpeed,WindDir,Rain".split(',')
    upload_row_wrapper(weather_table,row,force,heading,"clweather",row[:3])

def upload_forecast(row,force = False):
    heading = "PartitionKey,RowKey,SearchTimestamp,Year,Days,Minutes,Temperature,Humidity,WindSpeed,WindDir".split(',')
    upload_row_wrapper(forecast_table,row,force,heading,"metweather",row[:3])
