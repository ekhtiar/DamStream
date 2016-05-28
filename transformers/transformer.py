from eerl.parser.fromjson import jsontodataframe
from eerl.report.fromdf import countrows
from eerl.column.fromdf import keep

# tranfuncs = ['todataframe', 'keep']
# funcconfigs = [{'func_name': 'keep', 'column': 'history'}]

def transform(msg, tranfuncs, funcconfigs):

    eerl = {'todataframe': jsontodataframe,
            'countrows': countrows,
            'keep': keep,
            }

    for func in tranfuncs:
        if func == 'keep':
            column = (item['column'] for item in funcconfigs if item["func_name"] == 'keep').next()
            msg = eerl[func](msg, column)
        else:
            msg = eerl[func](msg)

    return msg
