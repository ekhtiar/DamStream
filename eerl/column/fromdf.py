import json
import pandas

# return df column, input: df, output: df
def keep(df, column):
    series = df[column]
    df = series.to_frame()
    return df