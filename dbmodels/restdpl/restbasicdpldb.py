from sqlalchemy import Column, String, Date, DateTime, INT, TEXT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class RestbasicdplMetadata(Base):
    __tablename__ = "restbasicdpl_metadata"
    id = Column('id', INT, primary_key=True)
    dplid = Column('dpl_id', String(10))
    executiondatetime = Column('execution_datetime', DateTime)
    incrementvalue = Column('increment_value', INT)

    def __init__(self, dplid, executiondatetime, incrementvalue):
        self.dplid = dplid
        self.executiondatetime = executiondatetime
        self.incrementvalue = incrementvalue


class RestbasicdplInfo(Base):
    __tablename__ = "restbasicdpl_info"
    dplid = Column('dpl_id', String(20), primary_key=True)
    createdat = Column('created_at', Date)
    startdate = Column('start_date', Date)
    url = Column('url', String(50))
    incrementtype = Column('increment_type', String(10))
    incrementvariable = Column('increment_variable', String(20))
    initialincrementvalue = Column('initial_increment_value', INT)
    incrementby = Column('increment_by', INT)
    urlparameters = Column('url_parameters', String(50))
    scheduleinterval = Column('schedule_interval', String(10))
    method = Column('method', String(50))
    headers = Column('headers', TEXT)
    payload = Column('payload', TEXT)
    outputs = Column('outputs', TEXT)
    funcconfigs = Column('function_configurations', TEXT)

    def __init__(self, dplid, createdat, startdate, url, incrementtype, incrementvariable, initialincrementvalue,
                 incrementby, urlparameters, scheduleinterval, method, headers, payload, outputs, funcconfigs):
        self.dplid = dplid
        self.createdat = createdat
        self.startdate = startdate
        self.url = url
        self.incrementtype = incrementtype
        self.incrementvariable = incrementvariable
        self.initialincrementvalue = initialincrementvalue
        self.incrementby = incrementby
        self.urlparameters = urlparameters
        self.scheduleinterval = scheduleinterval
        self.method = method
        self.headers = headers
        self.payload = payload
        self.outputs = outputs
        self.funcconfigs = funcconfigs
