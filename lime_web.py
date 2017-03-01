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
from geventwebsocket.exceptions import WebSocketError

import utils
import watched_io
import lustre_config

from flask import Flask, render_template, request, jsonify
app = Flask(__name__)

class WatchedJobs(object):
    def __init__(self):
        self.wjs_jobs = {}
        self.wjs_condition = threading.Condition()

    def _wjs_find_job(self, job_id):
        if job_id in self.wjs_jobs:
            return self.wjs_jobs[job_id]
        else:
            return None

    def wjs_find_job(self, job_id):
        self.wjs_condition.acquire()
        job = self._wjs_find_job(job_id)
        self.wjs_condition.release()
        return job

    def wjs_watch_job(self, job_id, websocket):
        self.wjs_condition.acquire()
        job = self._wjs_find_job(job_id)
        if job is None:
            job = WatchedJob(job_id, watched_jobs)
            self.wjs_jobs[job_id] = job
        job.wj_websockets.append(websocket)
        self.wjs_condition.release()

    def wjs_unwatch_job(self, job_id, websocket):
        self.wjs_condition.acquire()
        job = self._wjs_find_job(job_id)
        if job is None:
            self.wjs_condition.release()
            return -1
        if websocket in job.wj_websockets:
            job.wj_websockets.remove(websocket)
        if len(job.wj_websockets) == 0:
            del self.wjs_jobs[job_id]
        self.wjs_condition.release()
        return 0

    def wjs_metric_received(self, service_id, job_id, timestamp, value):
        self.wjs_condition.acquire()
        job = self._wjs_find_job(job_id)
        if job is None:
            self.wjs_condition.release()
            return 1
        job.wj_datapoint_add(service_id, timestamp, value)
        self.wjs_condition.release()
        return 0

WAIT_INTERVAL = 1.0
METRIC_INTERVAL = WAIT_INTERVAL * 2

class WatchedJob(object):
    def __init__(self, job_id, jobs):
        self.wj_websockets = []
        self.wj_job_id = job_id
        self.wj_jobs = jobs
        self.wj_value = None
        self.wj_timestamp = None
        self.wj_services = {}
        timer = threading.Timer(WAIT_INTERVAL, self.wj_datapoint_send)
        timer.start()

    # Return the rate, if no rate return 0
    def wj_datapoint_add(self, service_id, timestamp, value):
        if service_id not in self.wj_services:
            service = JobPerService()
            self.wj_services[service_id] = service
        else:
            service = self.wj_services[service_id]
        service.jps_datapoint_add(timestamp, value)

    def wj_datapoint_send(self):
        dead_websockets = []
        rate = self.wj_get_rate();
        json_string = json.dumps({
            "type": "datapoint",
            "rate": rate,
            "job_id": self.wj_job_id})
        for websocket in self.wj_websockets:
            logging.debug("sending: %s", json_string)
            try:
                websocket.send(json_string)
            except WebSocketError:
                websocket.closed = True
                dead_websockets.append(websocket)

        for websocket in dead_websockets:
            self.wj_websockets.remove(websocket)
        if len(self.wj_websockets) == 0:
            del self.wj_jobs.wjs_jobs[self.wj_job_id]
        else:
            timer = threading.Timer(METRIC_INTERVAL, self.wj_datapoint_send)
            timer.start()

    def wj_get_rate(self):
        rate = 0
        for service_id in self.wj_services:
            service = self.wj_services[service_id]
            service_rate = service.jps_rate
            if service_rate is not None:
                rate += service_rate
        return rate

class JobPerService(object):
    def __init__(self):
        self.jps_value = None;
        self.jps_timestamp = None;
	self.jps_rate = None

    def jps_datapoint_add(self, timestamp, value):
        # If overflow happens, rate will be kept unchanged for one interval
        if (self.jps_timestamp is not None and
            value >= self.jps_value and
            timestamp > self.jps_timestamp):
            diff = value - self.jps_value
            time_diff = timestamp - self.jps_timestamp
            self.jps_rate = diff / time_diff / 1000000
        self.jps_timestamp = timestamp
        self.jps_value = value


watched_jobs = WatchedJobs()

@app.route("/")
def app_root():
    return render_template("index.html")


def tsdb_tags_parse(tsdb_tags, tag_dict):
    tags = tsdb_tags.split()
    for tag in tags:
        pair = tag.split("=")
        if len(pair) != 2:
            logging.error("tsdb tags [%s] is invalid", tsdb_tags);
            return -1
        tag_dict[pair[0]] = pair[1]
    return 0


@app.route("/metric_post", methods=['POST'])
def app_metric_post():
    logging.debug(json.dumps(request.json, indent=4))
    for metric in request.json:
        meta = metric["meta"]
        tsdb_name = meta["tsdb_name"]
        if tsdb_name != "ost_jobstats_samples":
            continue
        tsdb_tags = meta["tsdb_tags"]
        tag_dict = {}
        ret = tsdb_tags_parse(tsdb_tags, tag_dict)
        if ret:
            continue
        logging.debug("tag_dict: %s", tag_dict)
        if tag_dict["optype"] != "sum_write_bytes":
            continue
        service_id = tag_dict["ost_index"]
        logging.debug(json.dumps(metric, indent=4))
        job_id = tag_dict["job_id"]
        value = metric["values"][0]
        timestamp = metric["time"]
        watched_jobs.wjs_metric_received(service_id, job_id, timestamp, value)
        logging.debug("service_id :%s, job_id: %s, time: %d, value: %d",
            service_id, job_id, timestamp, value)
    return "Succeeded"


@app.route("/console_websocket")
def app_console_websocket():
    if request.environ.get('wsgi.websocket'):
        websocket = request.environ['wsgi.websocket']
        config_string = websocket.receive()
        config = json.loads(config_string)
        logging.debug("received config: %s", config)
        cluster = config["cluster"]
        fsname = cluster["name"]
        hosts = []
        for host in cluster["hosts"]:
            hosts.append(host["name"])
        ssh_identity_file = cluster["ssh_identity_file"]
        logging.debug("fsname: [%s], hosts: %s", fsname, hosts)
        cluster = lustre_config.LustreCluster(fsname, hosts,
            ssh_identity_file=ssh_identity_file)
        cluster.lc_detect_devices()
        cluster.lc_enable_fifo_for_ost_io()
        cluster.lc_enable_tbf_for_ost_io("jobid")
        cluster.lc_set_jobid_var("procname_uid")

        jobs = config["jobs"]
        for job in jobs:
            job_id = job["job_id"]
            tbf_name = lustre_config.tbf_escape_name(job_id)
            watched_jobs.wjs_watch_job(job_id, websocket)
            cluster.lc_start_tbf_rule(tbf_name, job_id, 1000)
            
        while not websocket.closed:
            control_command = websocket.receive()
            logging.debug("command: %s", control_command);
            control = json.loads(control_command)
            job_id = control["job_id"]
            tbf_name = lustre_config.tbf_escape_name(job_id)
            rate = control["rate"]
            ret = cluster.lc_change_tbf_rate(tbf_name, int(rate))
            if ret:
                result = "failure"
            else:
                result = "success"
            json_string = json.dumps({
                "type": "command_result",
                "command": "change_rate",
                "rate": rate,
                "job_id": job_id,
                "result": result})
            logging.debug("sent result")
            websocket.send(json_string)

        for job in jobs:
            watched_jobs.wjs_unwatch_job(job_id, websocket)
        logging.debug("websocket is closed");
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
