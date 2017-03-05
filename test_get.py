import azure_service_wrapper as asw

## A simple script for verifying the rows in the table during development

# for row in asw.get_entities("pollution",filter="RowKey eq '2017,62,0,52204644'"):
#     print [row['SearchTimestamp'],row['NOx'],row['PM10'],row['PM25'],row['Latitude']]


for row in asw.get_entities("prediction",filter="Year eq '2017' and Days ge '67'"):
    print [row['SearchTimestamp'],row['NOx'],row['PM10'],row['PM25'],row['Latitude']]
