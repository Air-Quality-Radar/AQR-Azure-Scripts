import azure_service_wrapper
for row in azure_service_wrapper.get_entities("weather",filter=""):
    print row
