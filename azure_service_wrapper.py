from azure.storage.table import TableService, Entity
import os
apikey = open(os.getenv("HOME")+'/'+'apikey','r').read()

table_service = TableService(account_name='aqrstorage', account_key=apikey)

config_directory = os.getenv("HOME")+"/.aqrconf"
pollution_table = "pollution"
if not os.path.exists(config_directory):
    os.makedirs(config_directory)
def create_table(table):
    table_service.create_table(table)
def delete_table(table):
    table_service.delete_table(table)
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

def upload_pollution(row,force = False):
    if not force:
        current_latest_timestamp = get_latest_timestamp(row[3],row[4])
        if current_latest_timestamp and is_timestamp_earlier(row[:3],current_latest_timestamp):
            return
    heading = "PartitionKey,RowKey,SearchTimestamp,Year,Days,Minutes,Latitude,Longitude,NOx,PM10,PM25".split(',')
    from datetime import datetime,timedelta
    timestamp = datetime(int(row[0]),1,1) + timedelta(days = int(row[1]), minutes=int(row[2]))
    timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    partition_key = row[0]
    row_key = ",".join(map(str,row[:5]))
    upload_row(pollution_table,[partition_key,row_key,timestamp]+row,heading)
    set_timestamp(row[3],row[4],row[:3])
