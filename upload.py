from datetime import datetime
from download import *
from azure_service_wrapper import *

def show_banner(heading):
    print "="*10 + heading + "="*10

current_time = datetime.now()
print "Start uploading pollution data at " + current_time.ctime()
print ""
show_banner("DEFRA")
result = defra(current_time.year)
for row in result:
    upload_pollution(row)
print "DEFRA success"
