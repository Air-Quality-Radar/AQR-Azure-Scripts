# The script MUST contain a function named azureml_main
# which is the entry point for this module.

# imports up here can be used to 
import pandas as pd

# The entry point function can contain up to two input arguments:
#   Param<dataframe1>: a pandas.DataFrame
#   Param<dataframe2>: a pandas.DataFrame
def azureml_main(dataframe1 = None, dataframe2 = None):
    
    # Execution logic goes here
    # print('Input pandas.DataFrame #1:\r\n\r\n{0}'.format(dataframe1))
    
    # print ("PM10 Mean " + str(dataframe1["PM10_Avg_Curr"].mean()))
    # print("PM10 Std " + str(dataframe1["PM10_Avg_Curr"].std()))
    # print("PM25 Mean " + str(dataframe1["PM25_Avg_Curr"].mean()))
    # print("PM25 Std " + str(dataframe1["PM25_Avg_Curr"].std()))
    # print("NOx Mean " + str(dataframe1["NOx_Avg_Curr"].mean()))
    # print("NOx Std " + str(dataframe1["NOx_Avg_Curr"].std()))

    scored_PM10 = 'PM10_Avg_Prediction'
    mean_PM10 = 22.2932349187
    std_PM10 = 9.35727931517
    
    scored_PM25 = 'PM25_Avg_Prediction'
    mean_PM25 = 12.773614727
    std_PM25 = 7.99246220046

    scored_NOx = 'NOx_Avg_Prediction'
    mean_NOx = 84.2631061027
    std_NOx = 50.2017920052

    result = []
    denormalise_PM10 = lambda x : x * std_PM10 + mean_PM10
    denormalise_PM25 = lambda x : x * std_PM25 + mean_PM25
    denormalise_NOx = lambda x : x * std_NOx + mean_NOx
    
    dataframe1[scored_PM10] = dataframe1[scored_PM10].map(denormalise_PM10)
    dataframe1[scored_PM25] = dataframe1[scored_PM25].map(denormalise_PM25)
    dataframe1[scored_NOx] = dataframe1[scored_NOx].map(denormalise_NOx)
    # Return value must be of a sequence of pandas.DataFrame
    return dataframe1,