from webapp.run import app
from flask import Flask, render_template, request, redirect
from models.dagtemplates.restdpl.restbasicdag import createdag
from flask.ext.sqlalchemy import SQLAlchemy
from view import createrestbasicdpl, outpdrivershdfs
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


@app.route('/view')
def view():
    return render_template('view.html')


@app.route('/createdpl', methods=['GET'])
def create():
    return render_template('createdpl.html')


@app.route('/createrestdpl', methods=['GET'])
def createrestdpl():
    return render_template('restdpl/createrestdpl.html')


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

# -------------------------------------------------------------------------------
# DamStream Templates
# -------------------------------------------------------------------------------


@app.route('/createrestbasicdpl', methods=['GET', 'POST'])
def createrestjsondpl():
    if request.method == 'POST':
        # Call the appropriate function
        response = createrestbasicdpl.post(request)
        return response
    else:
        return render_template('restdpl/restbasicdpl/createrestbasicdpl.html')


