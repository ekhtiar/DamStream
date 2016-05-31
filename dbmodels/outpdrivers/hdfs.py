from sqlalchemy import Column, String, INT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class HDFSConfig(Base):
    __tablename__= "hdfs_config"
    dplid = Column('dpl_id', String(50), primary_key=True)
    outputtype = Column('output_type', String(50))
    directory = Column('directory', String(255))
    filename = Column('filename', String(50))
    uniquefilestamp = Column('unique_file_stamp', String(50))

    def __init__(self, dplid, outputtype, directory, filename, uniquefilestamp):
        self.dplid = dplid
        self.outputtype = outputtype
        self.directory = directory
        self.filename = filename
        self.uniquefilestamp = uniquefilestamp

class HDFSMetadata(Base):
    __tablename__ = 'hdfs_metadata'
    id = Column('id', INT, primary_key=True, autoincrement=True)
    dplid = Column('dpl_id', String(50), primary_key=True)
    outputname = Column('output_name', String(50))
    uniquefilevalue = Column('unique_file_value', String(50))

    def __init__(self, dplid, outputname, uniquefilevalue):
        self.dplid = dplid
        self.outputname = outputname
        self.uniquefilevalue = uniquefilevalue
