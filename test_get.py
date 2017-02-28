import azure_service_wrapper
for row in azure_service_wrapper.get_entities("rawpollution",filter="Year eq 2017 and Days eq 58"):
    print row
