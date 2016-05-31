import ast
import datetime
import json
from urllib import urlencode

import requests
from sqlalchemy.orm import sessionmaker

from connections.mysqlconn import getengine
from dbmodels.restdpl.restbasicdpldb import RestbasicdplInfo, RestbasicdplMetadata


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Description: This function has the logic to get the data. If the data is available
# then it returns true and writes data to output directory. If it isn't available then
# it returns false.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Variable Description
# increment = (String) the new value for the incremental variable
# payload = (String) String variable of the payload data, the string will be converted to dict
# url = (String) url of the endpoint

# This function pulls the data and sends it to a kafka
def get_data(dplid, fullurl, method, payload, headers):
    # convert to dict from string
    if payload:
        payload = ast.literal_eval(payload)
    if headers:
        headers = ast.literal_eval(headers)
    # use requests library to get data
    if method == 'POST':
        r = requests.post(fullurl,
                          headers=headers,
                          data=json.dumps(payload))
    if method == 'GET':
        r = requests.get(fullurl,
                          headers=headers,
                          data=json.dumps(payload))
    # check if reply is ok, if not then exit
    if not r.ok:
        print 'could not retreive data for ' + fullurl
        return False

    # if write to output and return true
    # sendtokafka(dplid=dplid, msg=r.content)
    print 'send data to kafka for ' + fullurl
    return True


def check_next(nexturl, method, payload, headers):
    # convert to dict from string
    payload = ast.literal_eval(payload)
    headers = ast.literal_eval(headers)

    nextr = requests.post(nexturl,
                          headers=headers,
                          data=json.dumps(payload))
    if not nextr.ok:
        print 'data for next url is not available'
        return False
    print 'data for next url is available'
    return True


def offset_url_builder(url, urlparameters, incrementvariable, incrementvalue):
    # Concatanate the incremental value with the url string (assuming the incremental variable
    # form the url with help of urlencode, attach increment variable at the end
    if urlparameters:
        url = url + "?" + urlencode(
            urlparameters) + "&" + incrementvariable + "=" + str(incrementvalue)
        return url
    if not urlparameters:
        url = url + "?" + incrementvariable + "=" + str(incrementvalue)
        return url


def pull(dplid):
    # Connect to database and get session object
    engine = getengine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the info object for this dpl
    restbasicdplinfo = session.query(RestbasicdplInfo).filter(RestbasicdplInfo.dplid == dplid).first()
    print 'retreived information from database for dpl: ' + dplid
    # Get configurations
    incrementtype = restbasicdplinfo.incrementtype
    url = restbasicdplinfo.url
    incrementvariable = restbasicdplinfo.incrementvariable
    payload = restbasicdplinfo.payload
    urlparameters = restbasicdplinfo.urlparameters
    if urlparameters:
        urlparameters = ast.literal_eval(urlparameters)
    headers = restbasicdplinfo.headers
    method = restbasicdplinfo.method

    # if the increment strategy is offset
    if incrementtype == 'offset':
        # Try to get the initial value where we are supposed to start from
        # Get the last executed value, if it is there, increase it by one to get the next value
        try:
            restbasicdplmetadata = session.query(RestbasicdplMetadata).filter(
                RestbasicdplMetadata.dplid == dplid).order_by(RestbasicdplMetadata.id.desc()).first()
            incrementvalue = restbasicdplmetadata.incrementvalue + 1
        # if there is no last executed value then get initial incremental value
        except:
            incrementvalue = restbasicdplinfo.initialincrementvalue



        # form the url with help of urlencode, attach increment variable at the end
        fullurl = offset_url_builder(url=url, urlparameters=urlparameters, incrementvariable=incrementvariable,
                                     incrementvalue=incrementvalue)
        nextfullurl = offset_url_builder(url=url, urlparameters=urlparameters, incrementvariable=incrementvariable,
                                         incrementvalue=incrementvalue + 1)

        while check_next(nexturl=nextfullurl, payload=payload, method=method, headers=headers):
            # call get_data
            if get_data(dplid=dplid, payload=payload, method=method, fullurl=fullurl, headers=headers):
                # print output for logging
                print 'wrote data to kafka for offset ' + str(incrementvalue)
            else:
                print 'did not write to kafka for' + str(incrementvalue)
            # create data object
            restbasicdplmetadata = RestbasicdplMetadata(dplid=dplid,
                                                        executiondatetime=datetime.datetime.now(),
                                                        incrementvalue=incrementvalue)
            # store data object
            session.add(restbasicdplmetadata)
            session.commit()
            # increase value by one
            incrementvalue += restbasicdplinfo.incrementby

            fullurl = offset_url_builder(url=url, urlparameters=urlparameters, incrementvariable=incrementvariable,
                                         incrementvalue=incrementvalue)
            nextfullurl = offset_url_builder(url=url, urlparameters=urlparameters, incrementvariable=incrementvariable,
                                             incrementvalue=incrementvalue + 1)

    # if the increment strategy is none
    if incrementtype == 'none':

        # call get_data
        get_data(dplid=dplid, method=method, payload=payload, fullurl=url, headers=headers)
        # print output for logging
        print 'got data'
        # create data object
        restbasicdplmetadata = RestbasicdplMetadata(dplid=dplid,
                                                    executiondatetime=datetime.datetime.now(),
                                                    incrementvalue=0)
        # store data object
        session.add(restbasicdplmetadata)
        session.commit()

pull('CurrencyConverter')