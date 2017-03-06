# The script MUST contain a function named azureml_main
# which is the entry point for this module.

# imports up here can be used to 
import pandas as pd

# The entry point function can contain up to two input arguments:
#   Param<dataframe1>: a pandas.DataFrame
#   Param<dataframe2>: a pandas.DataFrame
def azureml_main(dataframe1 = None, dataframe2 = None):
    scored_PM10 = 'PM10_Prediction'
    # Execution logic goes here
    # print('Input pandas.DataFrame #1:\r\n\r\n{0}'.format(dataframe1))
    mean_PM10 = 22.1290428485
    std_PM10 = 12.2620627263
    
    scored_PM25 = 'PM25_Prediction'
    mean_PM25 = 12.6108015377
    std_PM25 = 9.56347080449

    scored_NOx = 'NOx_Prediction'
    mean_NOx = 83.6153666776
    std_NOx = 71.4807540813

    result = []
    denormalise_PM10 = lambda x : x * std_PM10 + mean_PM10
    denormalise_PM25 = lambda x : x * std_PM25 + mean_PM25
    denormalise_NOx = lambda x : x * std_NOx + mean_NOx
    result.append(dataframe1['Year'])
    result.append(dataframe1['Date'])
    result.append(dataframe1['Time'])
    result.append(dataframe1['Latitude'])
    result.append(dataframe1['Longitude'])
    result.append(dataframe1[scored_PM10].map(denormalise_PM10))
    result.append(dataframe1[scored_PM25].map(denormalise_PM25))
    result.append(dataframe1[scored_NOx].map(denormalise_NOx))
    # Return value must be of a sequence of pandas.DataFrame
    return pd.DataFrame(result).T,
