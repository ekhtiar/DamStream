from webapp.run import app
from flask import Flask, render_template, request
from models.dagtemplates.restdpl.restbasicdag import createdag
from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy.orm import sessionmaker
from dbmodels.restdpl.restbasicdpldb import RestbasicdplInfo
from dbmodels.connection import getengine
from datetime import date

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:bigdata@172.17.32.112/damstream'
db = SQLAlchemy(app)

if __name__ == '__main__':
    app.run(debug=True)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/createdpl', methods=['GET'])
def create():
    return render_template('createdpl.html')


@app.route('/createrestdpl', methods=['GET'])
def createrestdpl():
    return render_template('restdpl/createrestdpl.html')


@app.route('/createrestbasicdpl', methods=['GET', 'POST'])
def createrestjsondpl():
    if request.method == 'POST':
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
        # Make Created At
        createdat = date.today()

        # Store in database
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
                                              payload=payload)

            session = Session()
            session.add(restbasicdpldb)
            session.commit()

            # create dag
            createdag(dplid=dplid, scheduleinterval=scheduleinterval)

        except KeyError, e:
            print 'KeyError  - reason %s' % str(e)
        except IndexError, e:
            print 'IndexError  - reason %s' % str(e)

        return render_template('restdpl/restbasicdpl/createrestbasicdpl.html')
    else:
        return render_template('restdpl/restbasicdpl/createrestbasicdpl.html')


@app.route('/create/offset', methods=['POST'])
def create_offset():
    if request.method == 'POST':
        pipelinename = request.form['pipeline_name']
        url = request.form['url']
        scheduleinterval = request.form['schedule_interval']
        headers = request.form['headers']
        payload = request.form['payload']
        createdag(dplid=pipelinename, url=url, scheduleinterval=scheduleinterval,
                  headers=headers, payload=payload)
        return render_template('create2.html')
    else:
        return render_template('create2.html')


@app.route('/view')
def view():
    return render_template('view.html')
