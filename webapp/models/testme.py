from sqlalchemy import create_engine
from damstream import Dplinfo

example = Dplinfo.query.all()

print (example.data)
