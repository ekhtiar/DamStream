import json
import pandas


def getreport(jsonmsg):

    tmpdata = json.loads(jsonmsg)
    tmpdf = pandas.DataFrame.from_dict(tmpdata)
    return tmpdf.count()
