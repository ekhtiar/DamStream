# This function takes a string object of JSON as input and returns various other formats
import json
import pandas


# return dataframe, input: string json, output: dataframe
def jsontodataframe(jsonstring):
    jsonobj = json.loads(jsonstring)
    jsondf = pandas.DataFrame.from_dict(jsonobj)
    return jsondf
