from webapp.run import app
from flask import Flask, render_template, request
from models.dagtemplates.restdpl.restbasicdag import createdag
from flask.ext.sqlalchemy import SQLAlchemy
from view import createrestbasicdpl, hdfs
import os
from flask import send_from_directory

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
        # Call the appropriate function
        response = createrestbasicdpl.post(request)
        return response
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


@app.route('/outpdrivers/kafka')
def outpdriverskafka():
    return render_template('outpdrivers/kafka.html')


@app.route('/outpdrivers/hive')
def outpdrivershive():
    return render_template('outpdrivers/hive.html')


@app.route('/outpdrivers/hdfs', methods=['GET', 'POST'])
def outpdrivershdfs():
    if request.method == 'POST':
        response = hdfs.post(request)
        return response
    else:
        return render_template('outpdrivers/hdfs.html')


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')
