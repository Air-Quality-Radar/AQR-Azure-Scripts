import azure_service_wrapper
for row in azure_service_wrapper.get_entities("pollution",filter="Days eq '60' and Year eq '2017'"):
    print row
