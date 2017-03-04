import azure_service_wrapper as asw
for row in asw.get_entities("prediction",filter="Days eq '61'",num_results=1):
    print row

for row in asw.get_entities("pollution",filter="Year eq '2017' and Days eq '61' and Minutes eq '60'",num_results=1):
    print row
