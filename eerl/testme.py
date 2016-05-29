from eerl.dummystring import getstring
from eerl.parser.fromjson import jsontodataframe
from eerl.column.fromdf import keep

jsonstring = getstring()

result = jsontodataframe(jsonstring)
result = keep(result, 'history')

print type(result)