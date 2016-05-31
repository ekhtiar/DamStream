from webapp.models.dagtemplates.restdpl.restbasicdag import createdag
from sqlalchemy.orm import sessionmaker
from dbmodels.restdpl.restbasicdpldb import RestbasicdplInfo
from dbmodels.dplmain import DplMain
from connections.mysqlconn import getengine
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
    outputs = request.form['outputs']
    funcconfigs = request.form['function_configurations']
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
                                          outputs=outputs,
                                          funcconfigs=funcconfigs)
        dplmain = DplMain(dplid=dplid,
                          createdat=createdat,
                          startdate=startdate,
                          dpltype='restbasicdpl',
                          scheduleinterval=scheduleinterval,
                          outputs=outputs,
                          funcconfigs=funcconfigs)

        session = Session()
        session.add(dplmain)
        session.add(restbasicdpldb)
        session.commit()

        # create dag
        createdag(dplid=dplid, dt=startdate, scheduleinterval=scheduleinterval)

    except KeyError, e:
        print 'KeyError  - reason %s' % str(e)
    except IndexError, e:
        print 'IndexError  - reason %s' % str(e)

    return redirect("/")
