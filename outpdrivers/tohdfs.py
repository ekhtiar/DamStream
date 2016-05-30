from connections.hdfsconn import gethdfsclient
from connections.mysqlconn import getengine
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
            uniquefilevalue = int(hdfsmetadata.uniquefilevalue) + 1
        # if there is no last executed value then use 0
        except:
            uniquefilevalue = 0

    # Get hdfs client
    hdfsclient = gethdfsclient()
    print 'writing to ' + directory + filename + str(uniquefilevalue)
    # This will create the hdfs directory if it isn't created already, if it is this will do nothing
    hdfsclient._mkdirs(directory)
    # write to write to the given directory
    hdfsclient.write(hdfs_path=directory + filename + str(uniquefilevalue), data=msg)
    # Write to metadata
    # create data object
    hdfsmetadata = HDFSMetadata(dplid=dplid, outputname=outputname, uniquefilevalue=uniquefilevalue)
    # store data object
    session.add(hdfsmetadata)
    session.commit()
