import download as dl
import AQR
from datetime import datetime,timedelta
from urllib2 import HTTPError

## this file is not refactored since it is only used once
do_upload = dict()
do_upload['defra'] = True
do_upload['aqe'] = True
do_upload['cl'] = True

start_time = dict()
end_time = dict()
start_time['aqe'] = datetime.now() - timedelta(1)
end_time['aqe'] = datetime(1990,1,1)
start_time['cl'] = start_time['aqe']
end_time['cl'] = end_time['aqe']

def log(x):
    print datetime.now().__str__() + ": " + x
def main():
    current_time = datetime.now()

    print "Start uploading pollution data at " + current_time.ctime()

    def show_banner(heading):
        print "\n\n"+"="*20 + " "+ heading +" "+ "="*20
    if do_upload['defra']:
        show_banner("DEFRA")
        for i in range(2017,1990,-1):
            try:
                print "Starting upload DEFRA year " + str(i)
                data = dl.defra(i)
                for row in data:
                    AQR.upload_pollution(row,force=True)
                print "Done. " + str(len(data)) + " records uploaded"
            except HTTPError:
                print "No more data. Last year " + str(i)
                break
    if do_upload['aqe']:
        show_banner("Air Quality England")
        cur_time = start_time['aqe']
        while cur_time > end_time['aqe']:
            if cur_time.day == 1:
                log(cur_time.strftime("%Y-%m") + " finished. ") 
            data = dl.aqe(cur_time + timedelta(-1), cur_time)
            if not data:
                print "No more data. Last data row at " + cur_time.__str__()
                break
            for row in data:
                AQR.upload_pollution(row,force=True)
            cur_time += timedelta(-1)

    if do_upload['cl']:
        # there are intermittent missing days
        MISSING_LIMIT = 60
        current_missing = 0
        show_banner("CL")
        cur_time = start_time['cl']
        while cur_time > end_time['cl']:
            try:
                if cur_time.day == 1:
                    log(cur_time.strftime("%Y-%m") + " finished. ") 
                data = dl.cl(cur_time)
                current_missing = 0
                for row in data:
                    AQR.upload_weather(row,force=True)
            except ValueError:
                current_missing += 1
                log(cur_time.strftime("%Y-%m-%d") + " missing. Missing count " + str(current_missing))
                if current_missing > MISSING_LIMIT:
                    print "No more data. Last data row at " + cur_time.__str__()
                    break
            cur_time += timedelta(-1)
if __name__ == "__main__":
    import getopt,sys
    optlist,_ = getopt.getopt(sys.argv[1:],"",["ignore=","start="])
    for opt,args in optlist:
        if opt == "--ignore":
            args = args.split(',')
            for arg in args:
                if arg in do_upload:
                    do_upload[arg] = False
        if opt == "--start":
            args = args.split(',')
            assert(len(args) == 2)
            if args[0] in start_time:
                start_time[args[0]] = datetime.strptime(args[1],"%Y-%m-%d")
        if opt == "--end":
            args = args.split(',')
            assert(len(args) == 2)
            if args[0] in start_time:
                end_time[args[0]] = datetime.strptime(args[1],"%Y-%m-%d")
    # print start_time,end_time
    main()
