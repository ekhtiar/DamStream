from sqlalchemy.orm import sessionmaker
from dbmodels.outpdrivers.hdfs import HDFSConfig
from connections.mysqlconn import getengine
from flask import redirect


def postreq(request):
    # Get all the parameters from form
    dplid = request.form['pipeline_name']
    outputtype = request.form['output_type']
    directory = request.form['directory']
    filename = request.form['filename']
    uniquefilestamp = request.form['unique_file_stamp']
    enhoputtype = request.form['enhanced_output_type']

    # Store in
    engine = getengine()
    Session = sessionmaker(bind=engine)

    try:
        hdfsConfig = HDFSConfig(dplid=dplid,
                                outputtype=outputtype,
                                directory=directory,
                                filename=filename,
                                uniquefilestamp=uniquefilestamp)

        session = Session()
        session.add(hdfsConfig)
        session.commit()

    except KeyError, e:
        print 'KeyError  - reason %s' % str(e)
    except IndexError, e:
        print 'IndexError  - reason %s' % str(e)

    if enhoputtype == 'none':
        return redirect("/")
    if enhoputtype != 'none':
        return redirect('outpdrivers/' + enhoputtype + '?dplid=' + dplid + '&outptype=enhanced')
