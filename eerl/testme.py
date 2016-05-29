from eerl.dummystring import getstring
from eerl.parser.fromjson import jsontodataframe
from eerl.column.fromdf import keep

from transformers.transformer import transform

jsonstring = getstring()
funcconfigs = [{'func_name': 'keep', 'column':'history'}]
tranfuncs = 'jsontodataframe,keep,dataframetojson'.split(',')
result = transform(jsonstring,tranfuncs=tranfuncs , funcconfigs = funcconfigs)

print type(result)