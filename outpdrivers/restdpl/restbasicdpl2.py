from dbmodels.connection import getengine
from sqlalchemy.orm import sessionmaker
from dbmodels.restdpl.restbasicdpldb import RestbasicdplInfo
from dbmodels.outpdrivers.hdfs import HDFSConfig
from connections.kafkaconn import getkafkaclient
from connections.hdfsconn import gethdfsclient
import logging


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Description: This function has the logic to get the data. If the data is available
# then it returns true and writes data to output directory. If it isn't available then
# it returns false.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Variable Description
# increment = (String) the new value for the incremental variable
# payload = (String) String variable of the payload data, the string will be converted to dict
# url = (String) url of the endpoint
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def write(dplid):
    # Connect to database
    engine = getengine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the info object for this dpl
    outputs = [{'output_type': 'hdfs', 'output_name': 'original', 'directory': '/user/admin/', 'filename': 'payconiq',
                'unique_file_stamp':
                    'unique_id'},
               {'output_type': 'kafka', 'output_name': 'enhanced1', 'topic_name': 'enhanced1'}
               ]

    print type(outputs)

    transfuncs = [{'output_name': 'original', 'funcs': 'a, b, d'},
                  {'output_name': 'enhanced1', 'funcs': 'a, d, e'}]

    for output in outputs:
        # Use generator function
        print (item for item in transfuncs if item["output_name"] == output['output_name']).next()


write('test')
