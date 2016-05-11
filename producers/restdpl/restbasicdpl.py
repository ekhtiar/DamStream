import requests
import json
import pandas
import datetime
from sqlalchemy.orm import sessionmaker
from dbmodels.restdpl.restbasicdpldb import RestbasicdplInfo, RestbasicdplMetadata
from dbmodels.connection import getengine
import ast
from urllib import urlencode


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Description: This function has the logic to get the data. If the data is available
# then it returns true and writes data to output directory. If it isn't available then
# it returns false.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Variable Description
# increment = (String) the new value for the incremental variable
# payload = (String) String variable of the payload data, the string will be converted to dict
# url = (String) url of the endpoint
def get_data(increment, payload, url, headers, directory):
    # Concatanate the incremental value with the url string (assuming the incremental variable
    # has been formatted as the last variable in the url parameter
    url = url + str(increment)
    # convert to dict from string
    payload = ast.literal_eval(payload)
    headers = ast.literal_eval(headers)
    # use requests library to get data
    r = requests.post(url,
                      headers=headers,
                      data=json.dumps(payload))
    # check if reply is ok, if not then exit
    if not r.ok:
        return False
    # convert it to json
    js = json.loads(r.content)
    # change it to data frame
    df = pandas.DataFrame.from_dict(js)
    # if data frame is empty exit function with False as output
    if df.empty:
        return False
    # if we don't have 100 data then don't commit
    if df.shape[0] != 100:
        return False
    # if it isn't empty write to output and return true
    df.to_csv(directory + str(increment) + ".csv", index=False)
    return True


def pull(dplname):
    # Connect to database and get session object
    engine = getengine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the info object for this dpl
    restbasicdplinfo = session.query(RestbasicdplInfo).filter(RestbasicdplInfo.dplid == dplname).first()
    # Try to get the initial value where we are supposed to start from
    # Get the last executed value, if it is there, increase it by one to get the next value
    try:
        restbasicdplmetadata = session.query(RestbasicdplMetadata).filter(
            RestbasicdplMetadata.dplid == dplname).order_by(RestbasicdplMetadata.id.desc()).first()
        incrementvalue = restbasicdplmetadata.incrementvalue + 1
    # if there is no last executed value then get initial incremental value
    except:
        incrementvalue = restbasicdplinfo.initialincrementvalue

    # get payload
    payload = restbasicdplinfo.payload
    # get url parameters and convert them to dict
    urlparameters = ast.literal_eval(restbasicdplinfo.urlparameters)
    # form the url with help of urlencode, attach increment variable at the end
    url = restbasicdplinfo.url + "?" + urlencode(urlparameters) + "&" + restbasicdplinfo.incrementvariable + "="
    # get headers
    headers = restbasicdplinfo.headers
    # get output directory
    directory = "C:/Python_Workspace/test"

    while get_data(incrementvalue, payload=payload, url=url, headers=headers, directory=directory):
        # print output for logging
        print 'got data for ' + str(incrementvalue)
        # create data object
        restbasicdplmetadata = RestbasicdplMetadata(dplid=dplname,
                                                    executiondatetime=datetime.datetime.now(),
                                                    incrementvalue=incrementvalue)
        # store data object
        session.add(restbasicdplmetadata)
        session.commit()
        # increase value by one
        incrementvalue += restbasicdplinfo.incrementby
