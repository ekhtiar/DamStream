from webapp.models.dagtemplates.restdpl.restbasicdag import createdag
from sqlalchemy.orm import sessionmaker
from dbmodels.restdpl.restbasicdpldb import RestbasicdplInfo
from dbmodels.connection import getengine
from datetime import date
from flask import redirect


def post(request):
    # Get all the parameters from form
    dplid = request.form['pipeline_name']
    startdate = request.form['start_date']
    url = request.form['url_name']
    incrementtype = request.form['increment_type']
    incrementvariable = request.form['increment_variable']
    initialincrementvalue = request.form['initial_increment_value']
    incrementby = request.form['increment_by']
    urlparameters = request.form['url_parameters']
    scheduleinterval = request.form['schedule_interval']
    method = request.form['method']
    headers = request.form['headers']
    payload = request.form['payload']
    orgoputtype = request.form['original_output_type']
    enhoputtype = request.form['enhanced_output_type']
    # Make Created At
    createdat = date.today()

    # Store in
    engine = getengine()
    Session = sessionmaker(bind=engine)

    try:
        restbasicdpldb = RestbasicdplInfo(dplid=dplid,
                                          createdat=createdat,
                                          startdate=startdate,
                                          url=url,
                                          incrementtype=incrementtype,
                                          incrementvariable=incrementvariable,
                                          initialincrementvalue=initialincrementvalue,
                                          incrementby=incrementby,
                                          urlparameters=urlparameters,
                                          scheduleinterval=scheduleinterval,
                                          method=method,
                                          headers=headers,
                                          payload=payload,
                                          orgoputtype=orgoputtype,
                                          enhoputtype=enhoputtype)

        session = Session()
        session.add(restbasicdpldb)
        session.commit()

        # create dag
        createdag(dplid=dplid, scheduleinterval=scheduleinterval)

    except KeyError, e:
        print 'KeyError  - reason %s' % str(e)
    except IndexError, e:
        print 'IndexError  - reason %s' % str(e)

    # According to the output option, load the page to enter output configs
    if orgoputtype == 'kafka':
        return redirect('outpdrivers/kafka?dplid=' + dplid + '&outptype=original')
    if orgoputtype == 'hdfs':
        return redirect('outpdrivers/hdfs?dplid=' + dplid + '&outptype=original')
    if orgoputtype == 'hive':
        return redirect('outpdrivers/hive?dplid=' + dplid + '&outptype=original')
    # If original output type is none check for enhanced output type
    if orgoputtype == 'none':
        if enhoputtype == 'kafka':
            return redirect('outpdrivers/kafka?dplid=' + dplid + '&outptype=enhanced')
        if enhoputtype == 'hdfs':
            return redirect('outpdrivers/hdfs?dplid=' + dplid + '&outptype=enhanced')
        if enhoputtype == 'hive':
            return redirect('outpdrivers/hive?dplid=' + dplid + '&outptype=enhanced')
        if enhoputtype == 'none':
            return redirect('/')
