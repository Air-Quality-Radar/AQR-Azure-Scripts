#!/usr/bin/env python
from datetime import datetime,timedelta
from download import *
from azure_service_wrapper import *

def show_banner(heading):
    print "\n\n"+"="*20 + " "+ heading +" "+ "="*20

current_time = datetime.now()

print "Start uploading pollution data at " + current_time.ctime()
show_banner("DEFRA")
result = defra(current_time.year)
if result:
    for row in result:
        upload_pollution(row)
    print "Last updated: " + ",".join(result[-1][:3])
print "DEFRA: " + str(len(result)) + " records downloaded"

show_banner("Air Quality England")
result = aqe(current_time - timedelta(1),current_time)
if result:
    for row in result:
        upload_pollution(row)
    print "Last updated: " + ",".join(result[-1][:3])
print "AirQualityEngland:" + str(len(result)) + " records downloaded"


show_banner("Weather - CL")
result = cl(current_time)
if result:
    for row in result:
        upload_weather(row)
    print "Last updated: " + ",".join(result[-1][:3])
print "AirQualityEngland:" + str(len(result)) + " records downloaded"
