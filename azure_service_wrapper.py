from azure.storage.table import TableService, Entity
import os
apikey = open(os.getenv("HOME")+'/'+'apikey','r').read()

table_service = TableService(account_name='aqrstorage', account_key=apikey)


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
