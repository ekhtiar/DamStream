__author__ = 'Ekhtiar'

import os
from webapp.views import app

port = int(os.environ.get('PORT', 5000))
app.run(host='127.0.0.1', port=port, debug=True)
