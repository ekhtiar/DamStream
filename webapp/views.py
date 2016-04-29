from flask import Flask, render_template, request
from models.dagtemplates.restdag import createdag

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/create', methods=['GET', 'POST'])
def create():
    if request.method == 'POST':
        pipelinename = request.form['pipeline_name']
        url = request.form['url']
        scheduleinterval = request.form['schedule_interval']
        headers = request.form['headers']
        payload = request.form['payload']
        createdag(pipelinename=pipelinename, url=url, scheduleinterval=scheduleinterval,
                  headers=headers, payload=payload)
        return render_template('create.html')
    else:
        return render_template('create.html')


@app.route('/view')
def view():
    return render_template('view.html')


if __name__ == '__main__':
    app.run(debug=True)
