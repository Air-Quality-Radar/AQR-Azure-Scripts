import requests
import urllib2
from datetime import datetime,date,timedelta
import csv

gps = dict()
gps['gonville'] = (5220085,129144)
gps['defra'] = (52202370,124456)
gps['newmarket'] = (52211353,191325)
gps['montague'] = (52214239,136262)
gps['rail'] = (52194075,137125)
gps['parker'] = (52204644,125857)

def std_time(t):
    return map(str, [t.year,(t.date()-date(t.year,1,1)).days,t.hour * 60 + t.minute])

def fix_hour(s):
    return str(int(s[:2])%24) + s[2:]
def defra(year):
    url = "https://uk-air.defra.gov.uk/data_files/site_data/CAM_" + str(year) + ".csv"
    response = urllib2.urlopen(url)
    reader = csv.reader(response)
    for i in range(6):
        next(reader,None)
    result = []
    for row in reader:
        try:
            timestamp = row[0] + ' ' + fix_hour(row[1])
            timestamp = datetime.strptime(timestamp,"%d-%m-%Y %H:%M")
            result.append(std_time(timestamp)+list(gps['defra'])+[row[-3],'',''])
        except ValueError:
            # datetime format wrong
            # some malformed row
            print "Row Malformed"
            pass
    return sorted(result,key = lambda x: map(int,x[:3]))
def aqe(start,end):
    import re
    url = "http://www.airqualityengland.co.uk/local-authority/data.php"
    params = dict()
    params["site_id[]"] = ["CAM1","CAM3","CAM4","CAM5"]
    params["parameter_id[]"] = ["NOXasNO2","GE10","PM25"]
    params["f_date_started"] = start.strftime("%Y-%m-%d")
    params["f_date_ended"] = end.strftime("%Y-%m-%d")
    params["la_id"] = 51
    params["action"] = "download"
    params["submit"] = "Download+Data"
    response = requests.get(url,params = params)
    for line in response.text.split('\n'):
        result = re.search("(http://.+?\.csv)",line)
        if result:
            url =  result.group(0)
            break
    response = urllib2.urlopen(url)
    reader = csv.reader(response)
    next(reader,None)
    next(reader,None)
    locations = next(reader,None)
    headings = next(reader,None)
    location_short = dict()
    location_short["Cambridge Parker Street"] = "parker"
    location_short["Cambridge Gonville Place"] = "gonville"
    location_short["Cambridge Newmarket Road"] = "newmarket"
    location_short["Cambridge Montague Road"] = "montague"

    result = []
    # process locations
    for i in range(1,len(locations)):
        if locations[i] in location_short:
            locations[i] = location_short[locations[i]]
        else:
            locations[i] = locations[i-1]
    # print locations,headings
    for row in reader:
        if len(row) < 2:
            continue
        timestamp = row[0] + ' ' + fix_hour(row[1])
        timestamp = datetime.strptime(timestamp,"%d/%m/%Y %H:%M:%S")
        heading_to_index = {"NOXasNO2":0,"PM10":1,"PM25":2}
        # data is keyed by short location string and contains a length-3
        # list of the data above
        data = dict()
        for i,v in enumerate(row[2:]):
            loc = locations[i+2]
            hd = headings[i+2]
            if hd in heading_to_index:
                index = heading_to_index[hd]
                if not loc in data:
                    data[loc] = ["","",""]
                data[loc][index] = v
        for loc in data:
            result.append(std_time(timestamp) + list(gps[loc])+data[loc])
    return sorted(result,key = lambda x: map(int,x[:3]))

def cl(date):
    def accept(timestamp):
        # ignore half-an-hourly things
        return abs(timestamp.minute - 30) >= 20
    def standard(timestamp):
        if timestamp.minute > 30:
            offset = 60 - timestamp.minute
        else:
            offset = - timestamp.minute
        return timestamp + timedelta(minutes = offset)
    url = "https://www.cl.cam.ac.uk/research/dtg/weather/daily-text.cgi?" + date.strftime("%Y-%m-%d")
    response = requests.get(url)
    last_rain = 0.00
    result = []
    for line in response.text.split('\n'):
        if not line or line[0] == '#':
            continue
        line = line.split('\t')
        hour,minute = map(int,line[0].split(':'))
        timestamp = date.replace(hour=hour,minute=minute)
        if not accept(timestamp):
            continue
        timestamp = standard(timestamp)
        _,temp,humid,_,press,windsp,winddir,_,rain,_,_ = line
        rain = str(float(rain) - last_rain)
        result.append(std_time(timestamp)+[temp,humid,press,windsp,winddir,rain])
    return sorted(result,key = lambda x: map(int,x[:3]))

if __name__ == "__main__":
    # print aqe(datetime(2017,2,27),datetime(2017,2,28))
    print cl(datetime(2017,1,1))



# def rail(start,end):
#     """ WARNING: THIS FUNCTION IS NOT USED SINCE THIS SOURCE PROVIDES INSENSIBLE DATA"""
#     """ CHANGING THE COLUMNS SELECTED WILL ACTUALLY CHANGE THE VALUES GIVEN BACK"""
#     from HTMLParser import HTMLParser
#     class TableParser(HTMLParser):
#         result = []
#         in_body = False
#         in_cell = False
#         def handle_starttag(self,tag,attrs):
#             if tag == "tbody":
#                 self.in_body = True
#             if self.in_body and tag == "tr":
#                 self.result.append([])
#             if self.in_body and tag == "td":
#                 self.in_cell = True
#         def handle_endtag(self,tag):
#             if self.in_body and tag == "td":
#                 self.in_cell = False
#         def handle_data(self,data):
#             if self.in_cell:
#                 self.result[-1].append(data)
#         def get_array(self):
#             return self.result

#     def handle_nox(nox):
#         try:
#             nox = float(nox)
#         except ValueError:
#             return ""
#     params = dict()
#     params['station_id'] = 221
#     params['station_name'] = 'Brookgate CB1'
#     params['output_type'] = 'excel'
#     params['report_length'] = 'daily'
#     params['monitors[]'] = [4,119,10,93]
#     params['start_date'] = start.strftime("%d/%m/%Y")
#     params['start_time'] = start.strftime("%H:%M")
#     params['end_date'] = end.strftime("%d/%m/%Y")
#     params['end_time'] = end.strftime("%H:%M")
#     params['result_type'] = 'AVG'
#     params['time_base'] = '1'
#     url = "http://www.wecare4air.co.uk/non-aurn-results"
#     response = requests.post(url,data = params)
#     parser = TableParser()
#     parser.feed(response.text)
#     data = parser.get_array()
#     for row in data:
#         # last element is NOx in ugm-3 unit
#         # value seems wrong
#         # cn't remember which monitor id is this
#         row = row[:-1]
#         timestamp = datetime.strptime(row[0],"%Y-%m-%d %H:%M:%S")
