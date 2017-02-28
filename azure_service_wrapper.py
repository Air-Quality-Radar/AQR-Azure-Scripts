from azure.storage.table import TableService, Entity
apikey = open('apikey','r').read()

table_service = TableService(account_name='aqrstorage', account_key=apikey)

config_directory = os.getenv("HOME")+"/.aqrconf"
import os
if not os.path.exists(config_directory):
    os.makedirs(config_directory)
def get_entities(table_name, filter="",num_results=None):
    return table_service.query_entities(table_name, filter = filter,num_results = None)

def upload_row(table_name,row,heading):
    assert(len(row) == len(heading))
    entity = dict()
    for i,v in enumerate(row):
        entity[heading[i]] = v
    table_service.insert_or_replace_entity(table_name,entity)

def get_latest_timestamp(lat,lon):
    path_to_file = config_directory + '/' + str(lat) + str(lon)
    if not os.path.isfile(path_to_file):
        return None
    f = open(path_to_file,'r')
    result = f.read()
    f.close()
    return result.split(',')

def is_timestamp_earlier(ts1,ts2):
    ts1 = map(int,ts1)
    ts2 = map(int,ts2)
    return tuple(ts1) < tuple(ts2)

def set_timestamp(lat,lon,ts):
    path_to_file = config_directory + '/' + str(lat) + str(lon)
    f = open(path_to_file,'w')
    f.write(','.join(map(str,ts)))
    f.close()

def upload_pollution(row):
    current_latest_timestamp = get_latest_timestamp(row[3],row[4])
    if current_latest_timestamp and is_timestamp_earlier(row[:3],current_latest_timestamp):
        return
    heading = "PartitionKey,RowKey,Year,Days,Minutes,Latitude,Longitude,NOx,PM10,PM25".split(',')
    partition_key = row[0]
    row_key = ",".join(map(str,row[:5]))
    upload_row("rawpollution",[partition_key,row_key]+row,heading)
    set_timestamp(row[3],row[4],row[:3])
