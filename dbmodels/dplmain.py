from sqlalchemy import Column, String, Date, DateTime, INT, TEXT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DplMain(Base):
    __tablename__ = "dpl_main"
    dplid = Column('dpl_id', String(50), primary_key=True)
    createdat = Column('created_at', Date)
    startdate = Column('start_date', Date)
    dpltype = Column('dpl_type', String(50))
    scheduleinterval = Column('schedule_interval', String(50))
    outputs = Column('outputs', TEXT)
    funcconfigs = Column('function_configurations', TEXT)

    def __init__(self, dplid, createdat, startdate, dpltype, scheduleinterval, outputs, funcconfigs):
        self.dplid = dplid
        self.createdat = createdat
        self.startdate = startdate
        self.scheduleinterval = scheduleinterval
        self.dpltype = dpltype
        self.outputs = outputs
        self.funcconfigs = funcconfigs
