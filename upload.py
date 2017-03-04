#!/usr/bin/env python
from datetime import datetime,timedelta
import download as dl
import AQR

def show_banner(heading):
    print "\n\n"+"="*20 + " "+ heading +" "+ "="*20

current_time = datetime.now()


def upload(download_fn, download_args, upload_fn, name):
    show_banner(name)
    result = download_fn(*download_args)
    if result:
        for row in result:
            upload_fn(row)
        print "Last updated: " + AQR.to_timestamp(*result[-1][:3]).__str__()
    print name + ": " + str(len(result)) + " records downloaded"

if __name__ == "__main__":
    print "\n\n\nStart uploading data at " + current_time.ctime()
    upload(dl.defra,[current_time.year],AQR.upload_pollution,"DEFRA")

    upload(dl.aqe,[current_time - timedelta(1),current_time],AQR.upload_pollution,"Air Quality England")

    upload(dl.cl,[current_time],AQR.upload_weather,"CL")

    upload(dl.metoffice,[],AQR.upload_forecast,"MetOffice")




