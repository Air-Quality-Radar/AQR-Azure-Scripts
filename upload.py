#!/usr/bin/env python
from datetime import datetime,timedelta
from download import *
from azure_service_wrapper import *

def show_banner(heading):
    print "\n\n"+"="*20 + " "+ heading +" "+ "="*20

current_time = datetime.now()


def upload(download_fn, download_args, upload_fn, name):
    show_banner(name)
    result = download_fn(*download_args)
    if result:
        for row in result:
            upload_fn(row)
        print "Last updated: " + to_timestamp(*result[-1][:3]).__str__()
    print name + ": " + str(len(result)) + " records downloaded"

if __name__ == "__main__":
    print "\n\n\nStart uploading data at " + current_time.ctime()
    upload(defra,[current_time.year],upload_pollution,"DEFRA")
    upload(aqe,[current_time - timedelta(1),current_time],upload_pollution,"Air Quality England")
    upload(cl,[current_time],upload_weather,"CL")
    upload(metoffice,[],upload_forecast,"MetOffice")




