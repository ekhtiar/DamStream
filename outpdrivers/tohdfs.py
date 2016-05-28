from connections.hdfsconn import gethdfsclient
from connections.mysqlconn import getengine
import logging
from sqlalchemy.orm import sessionmaker
from dbmodels.outpdrivers.hdfs import HDFSMetadata


def writetohdfs(dplid, outputname, directory, filename, uniquefilestamp, msg):
    # Connect to database and get session object
    engine = getengine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # If uniquefilestamp is unique_id then get the latest unique id
    if uniquefilestamp == 'unique_id':
        # Get the last object from database
        try:
            hdfsmetadata = session.query(HDFSMetadata) \
                .filter(HDFSMetadata.dplid == dplid) \
                .filter(HDFSMetadata.outputname == outputname) \
                .order_by(HDFSMetadata.id.desc()).first()
            uniquefilevalue = hdfsmetadata.incrementvalue + 1
        # if there is no last executed value then use 0
        except:
            uniquefilevalue = 0

    # Get hdfs client
    hdfsclient = gethdfsclient()
    logging.info('writing to ' + directory + filename + uniquefilevalue)
    hdfsclient.write(hdfs_path=directory + filename + uniquefilevalue, data=msg)
    # Write to metadata
    # create data object
    hdfsmetadata = HDFSMetadata(dplid=dplid, outputname=outputname, uniquefilevalue=uniquefilevalue)
    # store data object
    session.add(hdfsmetadata)
    session.commit()
