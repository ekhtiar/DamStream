import ast
from connections.kafkaconn import getkafkaclient, zookeeperaddress
from connections.mysqlconn import getengine
from sqlalchemy.orm import sessionmaker
from dbmodels.dplmain import DplMain
from transformers.transformer import transform
from outpdrivers.tohdfs import writetohdfs


# This function reads the ingested data from Kafka queue and writes to defined outputs
# after doing the required transformation
def write(dplid):
    # Connect to database
    engine = getengine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the info object for this dpl
    dplmain = session.query(DplMain).filter(DplMain.dplid == dplid).first()
    outputs = ast.literal_eval(dplmain.outputs)
    funcconfigs = ast.literal_eval(dplmain.funcconfigs)

    # Logging
    print 'writer running for ' + dplmain.dplid

    # Get new msg from Kafka queue by using balanced consumer

    # Get the client for Kafka
    kafkaclient = getkafkaclient()

    # Get the appropriate topic
    topic = kafkaclient.topics['damstream.' + dplmain.dplid]
    # Logging
    print 'subscribed to topic: damstream.' + dplmain.dplid

    # Get balanced consumer
    balanced_consumer = topic.get_balanced_consumer(consumer_group='damstream',
                                                    auto_commit_enable=True,
                                                    consumer_timeout_ms=6000,
                                                    zookeeper_connect=zookeeperaddress())

    # Loop through each msg from Kafka Queue
    for msg in balanced_consumer:
        # Get the message from kafka
        msg = msg.value
        # go through the string that has the output configurations and act according to the output_type
        # Sample string:
        # [{'output_type': 'hdfs', 'output_name': 'original', 'directory': '/user/admin/', 'filename': 'payconiq',
        # 'unique_file_stamp': 'unique_id'}, {'output_type': 'kafka', 'output_name': 'enhanced1',
        # 'topic_name': 'enhanced1'}]

        for output in outputs:
            if msg is not None:
                # if transformation is not none pass the msg to the transformer function
                if output['tran_funcs'].lower().strip() != 'none':
                    tranfuncs = output['tran_funcs'].split(',')
                    msg = transform(msg=msg, tranfuncs=tranfuncs, funcconfigs=funcconfigs)

                if output['output_type'] == 'hdfs':
                    # Loging
                    print 'writing to hdfs'
                    # Get hdfs client
                    outputname = output['output_name']
                    directory = output['directory']
                    filename = output['filename']
                    uniquefilestamp = output['unique_file_stamp']
                    writetohdfs(dplid=dplid, outputname=outputname, directory=directory,
                                filename=filename, uniquefilestamp=uniquefilestamp, msg=msg)
                    # Loging
                    print 'wrote to hdfs succesfully'
