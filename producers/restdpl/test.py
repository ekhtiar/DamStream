import ast
import datetime
import json
from urllib import urlencode
import requests
from sqlalchemy.orm import sessionmaker
from connections.mysqlconn import getengine
from dbmodels.restdpl.restbasicdpldb import RestbasicdplInfo, RestbasicdplMetadata
from outpdrivers.tokafka import sendtokafka


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Description: This function has the logic to get the data. If the data is available
# then it returns true and writes data to output directory. If it isn't available then
# it returns false.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Variable Description
# increment = (String) the new value for the incremental variable
# payload = (String) String variable of the payload data, the string will be converted to dict
# url = (String) url of the endpoint

# Connect to database and get session object

def get_data(dplid, url, payload, headers):
    # Concatanate the incremental value with the url string (assuming the incremental variable
    # has been formatted as the last variable in the url parameter
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

    # if write to output and return true
    sendtokafka(dplid=dplid, msg=r.content)
    print 'send data to kafka for ' + url
    return True


def check_next(nexturl, payload, headers):
    # convert to dict from string
    payload = ast.literal_eval(payload)
    headers = ast.literal_eval(headers)

    nextr = requests.post(nexturl,
                          headers=headers,
                          data=json.dumps(payload))
    if not nextr.ok:
        return False

    return True

def pull(dplid):
    engine = getengine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the info object for this dpl
    restbasicdplinfo = session.query(RestbasicdplInfo).filter(RestbasicdplInfo.dplid == dplid).first()
    print 'retreived information from database for dpl: ' + restbasicdplinfo.dplid

    # Get the increment type
    incrementtype = restbasicdplinfo.incrementtype

    print incrementtype


pull('CurrencyConverter')