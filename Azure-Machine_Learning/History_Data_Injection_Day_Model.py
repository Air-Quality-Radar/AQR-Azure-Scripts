# The script MUST contain a function named azureml_main
# which is the entry point for this module.


# imports up here can be used to 
import pandas as pd
import calendar

# The entry point function can contain up to two input arguments:
#   Param<dataframe1>: a pandas.DataFrame
#   Param<dataframe2>: a pandas.DataFrame
MAX_HISTORY = 1
NO_LOCATIONS = 5
HISTORY_OFFSET = 1
LOCATION_OFFSET = 2
LOCATION_LEN = 2
columns_basic = ["Year", "Date", "Latitude", "Longitude"]
columns_weather = ["Temperature_Avg", "Humidity_Avg", "Wind_Speed_Avg", "Wind_Direction_Avg"]
columns_pollutant = ["PM10_Avg", "PM25_Avg", "NOx_Avg"]

## FOR THIS SCRIPT, WE TEST IF THE PREDICTION IS MORE ACCURATE IF WE ONLY INCLUDE FEATURES FROM SAME LOCATION
def get_location(row):
    return row[LOCATION_OFFSET:LOCATION_OFFSET+LOCATION_LEN]
class DataRow:
    def __init__(self, row):
        self.own_row = row.tolist()
        self.past_rows = None
    def is_complete(self):
        if not self.past_rows or len(self.past_rows) != MAX_HISTORY:
            return False
        # for v in self.past_rows.values():
        #     if len(v) != NO_LOCATIONS:
        #         return False
        return True
    def raw_row(self):
        return self.own_row
    def to_array(self):
        """ For EACH history time, we present it in the following format:
        temperature, humidity, pressure, wind speed, wind direction, rainfall, loc1 pm10, loc2 pm10, loc2 pm10 ... , loc1 pm25, loc2 pm25, loc3 pm25, ... NOx. then own data is appended"""
        result = []
        # time and loc stuff
        result += self.own_row[:(len(columns_basic))] # Year, Date, Lat, Long
        for i in range(HISTORY_OFFSET,MAX_HISTORY+HISTORY_OFFSET):
            weather = None
            for row in self.past_rows[i].values():
                if weather == None:
                    weather = row.raw_row()[(len(columns_basic)):(len(columns_basic) + len(columns_weather))]
                    result += weather
                if get_location(row.raw_row()) == get_location(self.own_row):
                    result += row.raw_row()[(-1 * (len(columns_pollutant))):]
        result += self.own_row[(len(columns_basic)):]
        return result

# Subtract offset in terms of days from time_key
def prev_time_key(current, offset):
    year,date = current
    date -= offset
    if (date < 0):
        year -= 1
        if (calendar.isleap(year - 1)):
            date += 366
        else:
            date += 365
    return (year, date)

def azureml_main(dataframe1 = None, dataframe2 = None):

    # Execution logic goes here

    # Create Dictionaries, first indexed by year, date, time and second indexed by lat, long
    data_raw = dataframe1.values
    data_by_time = dict()
    for row in data_raw:
        time_key = (row[0],row[1]) # Year, Day, Time
        if not time_key in data_by_time:
            data_by_time[time_key] = dict()
        loc_key = (row[2],row[3]) # Lat, Long
        data_by_time[time_key][loc_key] = DataRow(row)

    for time_key, loc_dict in data_by_time.items():
        complete = True
        past_rows = dict()

        for i in range(HISTORY_OFFSET,MAX_HISTORY+HISTORY_OFFSET):
            new_key = prev_time_key(time_key,i)
            if not new_key in data_by_time:
                complete = False
                break
            past_rows[i] = data_by_time[new_key]
        if not complete:
            continue
        for loc_key, row in loc_dict.items():
            row.past_rows = past_rows
    result = []
    for a in data_by_time.values():
        for r in a.values():
            if r.is_complete():
                result.append(r.to_array())
    # Return value must be of a sequence of pandas.DataFrame
    # columns_all_pollutants = [pollutant + "_Loc_" + str(i) for i in range(NO_LOCATIONS) for pollutant in columns_pollutant ]
    columns_all_pollutants = columns_pollutant 
    columns_history = [item + "_Hist_" + str(i) for i in range(MAX_HISTORY) for item in (columns_weather + columns_all_pollutants) ]
    columns_current = [item + "_Curr" for item in (columns_weather + columns_pollutant)]
    print (columns_basic + columns_history + columns_current)
    return pd.DataFrame(result, columns = columns_basic + columns_history + columns_current),
