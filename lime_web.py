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
import lustre_config

from flask import Flask, render_template, request, jsonify
app = Flask(__name__)

@app.route("/")
def app_root():
    return render_template("index.html")


@app.route("/metric_post", methods=['POST'])
def app_metric_post():
    logging.error(json.dumps(request.json, indent=4))
    return "Succeeded"


@app.route("/console_websocket")
def app_console_websocket():
    if request.environ.get('wsgi.websocket'):
        websocket = request.environ['wsgi.websocket']
        config_string = websocket.receive()
        config = json.loads(config_string)
        logging.error("received config: %s", config)
        fsname = config["name"]
        hosts = []
        for host in config["hosts"]:
            hosts.append(host["name"])
        ssh_identity_file = config["ssh_identity_file"]
        logging.error("fsname: [%s], hosts: %s", fsname, hosts)
        cluster = lustre_config.LustreCluster(fsname, hosts,
            ssh_identity_file=ssh_identity_file)
        cluster.lc_detect_devices()
        cluster.lc_enable_tbf_for_ost_io("nid")
        cluster.lc_set_jobid_var("procname_uid")
        while True:
            time.sleep(1)
            websocket.send("looping with config\n")
        return "Success"
    else:
        logging.info("run command is not websocket: %s")
        return "Failure"


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
