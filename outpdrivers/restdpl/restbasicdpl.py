from dbmodels.connection import getengine
from sqlalchemy.orm import sessionmaker
from dbmodels.restdpl.restbasicdpldb import RestbasicdplInfo
from dbmodels.outpdrivers.hdfs import HDFSConfig
from outpconns.kafkaconn import getkafkaclient
from outpconns.hdfsconn import gethdfsclient
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
    restbasicdplinfo = session.query(RestbasicdplInfo).filter(RestbasicdplInfo.dplid == dplid).first()
    hdfsconfig = session.query(HDFSConfig).filter(HDFSConfig.dplid == dplid).first()
    # If original output type corresponds to kafka queue then do nothing
    if restbasicdplinfo.orgoputtype == 'kafka': return None
    # If the original output type corresponds to hdfs then pick up the new messages
    # from kafka queue and put it to HDFS

    # Get the client for Kafka
    kafkaclient = getkafkaclient()
    # Get the appropriate topic
    topic = kafkaclient.topics['damstream.' + restbasicdplinfo.dplid]
    logging.info('subscribed to topic: damstream.' + restbasicdplinfo.dplid)
    # Get balanced consumer
    balanced_consumer = topic.get_balanced_consumer(consumer_group='damstream',
                                                    auto_commit_enable=True,
                                                    consumer_timeout_ms=6000,
                                                    zookeeper_connect='DSambari.novalocal:2181')
    # Get hdfs client
    hdfsclient = gethdfsclient()

    # Extract the message from kafka queue
    for msg in balanced_consumer:
        if msg is not None:
            if restbasicdplinfo.orgoputtype == 'hdfs':
                logging.info('writing to '+hdfsconfig.directory + hdfsconfig.filename + str(msg.offset))
                hdfsclient.write(hdfs_path=hdfsconfig.directory + hdfsconfig.filename + str(msg.offset), data=msg.value)
