import ast
import logging
from connections.kafkaconn import getkafkaclient
from dbmodels.connection import getengine
from sqlalchemy.orm import sessionmaker
from dbmodels.dplmain import DplMain
from transformers.transformer import transform


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

    for output in outputs:
        if output['output_type'] == 'hdfs':
            print output['directory']


write('Payconiq')
