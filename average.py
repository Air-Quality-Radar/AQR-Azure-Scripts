## Return format:
##       dict: key - (latitude,longitude)
##             value - dict: key - "PM25","PM10","NOx"
##                           value: dictionary of length 24 being the average ratio of value of that pollutant of that hour to that day's average in the history

import download as dl
import azure_service_wrapper as asw
from datetime import date
import AQR

AVG_HISTORY = 5

class AverageGenerator:
    def __init__(self):
        self.sum = 0.0
        self.count = 0

    def add(self,num):
        try:
            self.sum += float(num)
            self.count += 1
        except ValueError:
            # ignore empty or ill-formated values
            pass

    def get(self):
        if self.count == 0:
            return ""
        else:
            return self.sum / self.count

def merge(avgs, new_row):
    for k in avgs.keys():
        if not k in new_row:
            continue
        avgs[k].add(new_row[k])

THRESHOLD = 1e-6
def div_with_null(a,b):
    try:
        a = float(a)
        b = float(b)
        if abs(b) < THRESHOLD or abs(a) < THRESHOLD:
            return ""
        return a/b
    except ValueError:
        return ""
def get_average():
    start_year = str(date.today().year - AVG_HISTORY)
    raw_data = asw.get_entities(AQR.pollution_table,filter = "Year ge '" + start_year + "'")
    result = dict()

    ## Data is a dictionary of dictionary of dictionaries of dictionaries
    ## it is first keyed by location then by date then by hour then by pollutant
    data = dict()

    for row in raw_data:
        time_key = (row["Year"], row["Days"])
        loc_key = (row["Latitude"], row["Longitude"])
        if not loc_key in data:
            data[loc_key] = dict()
        if not time_key in data[loc_key]:
            data[loc_key][time_key] = dict()
        data[loc_key][time_key][int(row['Minutes'])] = ({k:row[k] for k in AQR.pollutants})

    for loc_key in data.keys():
        if not loc_key in result:
            result[loc_key] = dict()
            for k in AQR.pollutants:
                result[loc_key][k] = {k*60:AverageGenerator() for k in range(24)}
        for day in data[loc_key].values():

            daily_average = {k:AverageGenerator() for k in AQR.pollutants}
            for hour in day.values():
                merge(daily_average, hour)
            daily_average = {k:v.get() for k,v in daily_average.items()}

            day_row = {k: dict() for k in AQR.pollutants}
            for hour,values in day.items():
                for k,v in values.items():
                    day_row[k][hour] = div_with_null(v,daily_average[k])
            # print day_row
            for k in result[loc_key]:
                merge(result[loc_key][k],day_row[k])
    for lk in result:
        for k in result[lk]:
            for h in result[lk][k]:
                result[lk][k][h] = result[lk][k][h].get()
    return result

averages = None
average_file = "averages"

import os
if not os.path.exists(average_file):
    f = open(average_file,'w')
    f.close()
f = None
try:
    f = open(average_file,'r')
    averages = eval(f.read())
except SyntaxError:
    f.close()
    f = open(average_file,'w')
    averages = get_average()
    f.write(averages.__str__())
    f.close()

