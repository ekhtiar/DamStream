__author__ = 'Ekhtiar'

from flask import Flask

application = Flask('Portfolio')
app = application

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
