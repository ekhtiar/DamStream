# This function takes a string object of JSON as input and returns various other formats
import json
import pandas


# return dataframe, input: string json, output: dataframe
def dataframetojson(df):
    return df.to_json()
