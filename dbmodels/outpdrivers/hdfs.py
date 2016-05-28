from sqlalchemy import Column, String, INT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class HDFSConfig(Base):
    __tablename__= "hdfs_config"
    dplid = Column('dpl_id', String(20), primary_key=True)
    outputtype = Column('output_type', String(10))
    directory = Column('directory', String(255))
    filename = Column('filename', String(20))
    uniquefilestamp = Column('unique_file_stamp', String(50))

    def __init__(self, dplid, outputtype, directory, filename, uniquefilestamp):
        self.dplid = dplid
        self.outputtype = outputtype
        self.directory = directory
        self.filename = filename
        self.uniquefilestamp = uniquefilestamp

class HDFSMetadata(Base):
    __tablename__ = 'hdfs_metadata'
    id = Column('id', INT, primary_key=True)
    dplid = Column('dpl_id', String(20), primary_key=True)
    outputname = Column('output_name', String(20))
    uniquefilevalue = Column('unique_file_value', String(50))

    def __init__(self, id, dplid, outputname, uniquefilevalue):
        self.id = id
        self.dplid = dplid
        self.outputname = outputname
        self.uniquefilevalue = uniquefilevalue
