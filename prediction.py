#!/usr/bin/env python
from download import gps,std_time
from datetime import datetime,timedelta
from upload import show_banner
import AQR
import azure_service_wrapper as asw

NON_DATA = 4
def retain_empty_fields(func):
    def func_wrapper(*args,**kwargs):
        empty_cols = []
        for i in range(NON_DATA,len(args[0])):
            if AQR.field_empty(args[0][i]):
                empty_cols.append(i)
                args[0][i] = '0'
        result = func(*args,**kwargs)
        # print result,empty_cols
        for i in empty_cols:
            # print i
            result[i] = "0"
        return result
    return func_wrapper

@retain_empty_fields
def get_next_hour_row(current_row,weather):
    ## TODO need to deal with no weather data

    current_time = AQR.to_timestamp(*current_row[:3])
    location = current_row[3:5]
    next_time = current_time + timedelta(hours=1)

    current_weather = weather[current_time]
    next_weather = weather[next_time]

    def select_cols(weather_row):
        return [weather_row["Temperature"],weather_row["Humidity"],weather_row["WindSpeed"]]
    input_row = AQR.std_time(next_time) + location + select_cols(current_weather) + current_row[5:] + select_cols(next_weather) + [0,0,0]
    # print input_row
    output_row = AQR.get_hourly_prediction(input_row)
    return output_row


def generate_hourly_prediction(until_time):

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
        # print timestamp

    print "Predicting up to: " + weather_end.__str__()

    # print weather_end,weather_start
    for loc,(lat,lon) in gps.items():
        show_banner(loc)

        last_uploaded = AQR.to_timestamp(*AQR.get_latest_timestamp(str(lat)+str(lon)))

        print "Last time we have data: " + last_uploaded.__str__()

        if last_uploaded >= weather_end:
            continue
        last_row = asw.get_entities(AQR.pollution_table,"SearchTimestamp eq '" + AQR.to_search_timestamp(last_uploaded) + "' and Latitude eq " + str(lat))
        for row in last_row:
            last_row = row
            break
        else:
            print "Empty!" + str(lat) + last_uploaded.__str__()
            return
        current_row = AQR.std_time(last_uploaded) + [lat,lon] + [last_row["PM10"],last_row["PM25"],last_row["NOx"]]
        while AQR.to_timestamp(*current_row[:3]) < weather_end:
            current_row = get_next_hour_row(current_row,weather)
            # print current_row
            AQR.upload_pollution(current_row[:-3] + [current_row[-1]]+current_row[-3:-1],table="prediction",force=True)
    return weather_end


## returns a dictionary keyed by (lat,long) and then by pollutants
import average as avg
def get_day_avgs_pollution(time,heading):
    entities = asw.get_entities(AQR.pollution_table,filter = "Year eq '" + str(time.year) +"' and Days eq '" + AQR.std_time(time)[1] + "'")
    result = dict()
    for entity in entities:
        loc_key = (entity["Latitude"],entity["Longitude"])
        if not loc_key in result:
            result[loc_key] = {k:avg.AverageGenerator() for k in heading}
        for k in heading:
            assert (k in entity)
            result[loc_key][k].add(entity[k])
    for loc in result:
        for h in result[loc]:
            result[loc][h] = result[loc][h].get()
    return result

def to_number(wd):
    if len(wd) == 1:
        return {"N":0.0,"S":180.0,"E":90.0,"W":270.0}[wd]
    a = to_number(wd[0])
    b = to_number(wd[1:])
    if abs(a-b) > 180:
        a += 360
    return (a+b)/2

def get_day_avgs_weather(weather,time,heading):
    entities = asw.get_entities(AQR.forecast_table,filter = "Year eq '" + str(time.year) +"' and Days eq '" + AQR.std_time(time)[1] + "'")
    result = {k:avg.AverageGenerator() for k in heading}
    for entity in entities:
        for k in heading:
            ## ugly hack
            if k == "WindDir":
                entity[k] = to_number(entity[k])
            assert (k in entity)
            result[k].add(entity[k])
    for h in result:
        result[h] = result[h].get()
    return result

def construct_weather(weather):
    return [weather["Temperature"],weather["Humidity"],weather["WindSpeed"],weather["WindDir"]]
def construct_pollution(pollution):
    return [pollution["PM10"],pollution["PM25"],pollution["NOx"]]

@retain_empty_fields
def get_next_day_row(row,cur_time,loc,previous_weather,cur_weather):
    def construct_day_query():
        return map(str,AQR.std_time(cur_time)[:2] + list(loc) + construct_weather(previous_weather) + row[-3:] + construct_weather(cur_weather) + [0,0,0])
    return AQR.get_daily_prediction(construct_day_query())


import average as avg
def mul_with_null(a,b):
    try:
        a = float(a)
        b = float(b)
        return a * b
    except ValueError:
        return ""
        pass
def upload_hourly_row(loc,avgs,date):
    ratios = avg.averages[loc]
    for minutes in range(0,60*24,60):
        cur_pollution = dict()
        # print avgs.keys(),ratios.keys()
        for p in AQR.pollutants:
            cur_pollution[p] = mul_with_null(avgs[p] , ratios[p][minutes])
        cur_row = map(str,AQR.std_time(date)[:2] + [minutes] + list(loc) + [cur_pollution["NOx"],cur_pollution["PM10"],cur_pollution["PM25"]])
        AQR.upload_pollution(cur_row,force=True,table="prediction")

def generate_daily_prediction(start_time):
    previous_day = start_time - timedelta(1)
    previous_day_pollution = get_day_avgs_pollution(previous_day,AQR.pollutants)
    previous_day_weather = get_day_avgs_weather(AQR.forecast_table,previous_day,AQR.weathers)
    weather_end = AQR.to_timestamp(*AQR.get_latest_timestamp("metweather"))
    print "Predicting up to: " + weather_end.__str__()
    cur_day = start_time
    while cur_day.date() <= weather_end.date():
        print "\nPredicting " + cur_day.date().__str__()
        cur_day_weather = get_day_avgs_weather(AQR.forecast_table,cur_day,AQR.weathers)
        cur_day_pollution = dict()
        for loc in previous_day_pollution:
            # hack #2
            dummy_row = [0] * NON_DATA+ construct_pollution(previous_day_pollution[loc])
            cur_pollution = get_next_day_row(dummy_row,cur_day,loc,previous_day_weather,cur_day_weather)[-3:]
            # print cur_pollution
            cur_pollution = {k:v for k,v in zip(["PM10","PM25","NOx"],cur_pollution)}
            upload_hourly_row(loc,cur_pollution,cur_day)
            cur_day_pollution[loc] = cur_pollution
        previous_day_pollution = cur_day_pollution
        previous_day_weather = cur_day_weather
        cur_day += timedelta(1)

if __name__ == "__main__":
    print "\nStart uploading predictions at " + datetime.now().__str__()
    last_hourly_prediction = generate_hourly_prediction(datetime.now())
    generate_daily_prediction(last_hourly_prediction)
    # generate_daily_prediction(datetime.now())
