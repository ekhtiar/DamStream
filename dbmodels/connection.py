# This function sets up connection to database and returns metadata

from sqlalchemy import create_engine, MetaData

def getengine():
    engine =  create_engine('mysql://root:bigdata@172.17.32.112:3306/damstream')
    return engine