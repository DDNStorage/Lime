# Copyright (c) 2017 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Web inteface of LIME
"""
import json
import os
import time
import signal
import subprocess
import threading
import StringIO
import select
import logging
import logging.handlers
import sys
from gevent.wsgi import WSGIServer
from gevent import monkey
from geventwebsocket.handler import WebSocketHandler

import utils
import watched_io

from flask import Flask, render_template, request, jsonify
app = Flask(__name__)

@app.route("/")
def app_root():
    return render_template("index.html")


@app.route("/metric_post", methods=['POST'])
def app_metric_post():
    logging.error(request.json)
    return "Succeeded"


if __name__ == "__main__":
    logdir = "log"
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    elif not os.path.isdir(logdir):
        logging.error("[%s] is not a directory", logdir)
    utils.configure_logging("./")
    monkey.patch_all()
    http_server = WSGIServer(('0.0.0.0', 24), app,
                             handler_class=WebSocketHandler)
    http_server.serve_forever()
