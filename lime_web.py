# Copyright (c) 2017 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Web inteface of LIME
"""
import json
import os
import threading
import logging
import logging.handlers
import time
from gevent.wsgi import WSGIServer
from gevent import monkey,sleep
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket.exceptions import WebSocketError

import utils
import lustre_config

from flask import Flask, render_template, request
APP = Flask(__name__)


METRIC_INTERVAL = 1
CLUSTER = None
DEFAULT_RATE_LIMIT = 10000

class WatchedJobs(object):
    """
    All the watched Jobs will be group here
    """
    def __init__(self):
        self.wjs_jobs = {}
        self.wjs_condition = threading.Condition()
        utils.thread_start(self.wjs_datapoints_send, ())

    def _wjs_find_job(self, job_id):
        """
        Find job according to its job ID
        """
        if job_id in self.wjs_jobs:
            return self.wjs_jobs[job_id]
        else:
            return None

    def wjs_find_job(self, job_id):
        """
        Find job according to its job ID
        """
        self.wjs_condition.acquire()
        job = self._wjs_find_job(job_id)
        self.wjs_condition.release()
        return job

    def wjs_watch_job(self, job_id, websocket):
        """
        A websocket connected, so watch the job
        """
        self.wjs_condition.acquire()
        job = self._wjs_find_job(job_id)
        if job is None:
            job = WatchedJob(job_id, WATCHED_JOBS)
            self.wjs_jobs[job_id] = job
        job.wj_websockets.append(websocket)
        self.wjs_condition.release()

    def wjs_unwatch_job(self, job_id, websocket):
        """
        A websocket disconnected, so unwatch the job
        """
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
        """
        Recived a datapoint
        """
        self.wjs_condition.acquire()
        job = self._wjs_find_job(job_id)
        if job is None:
            self.wjs_condition.release()
            return 1
        job.wj_datapoint_add(service_id, timestamp, value)
        self.wjs_condition.release()
        return 0

    def wjs_datapoints_send(self):
        """
        Send datapoints of jobs
        """
        while True:
            logging.debug("sending datapoints of jobs")
            self.wjs_condition.acquire()
            deleted_jobs = []
            for job_id, job in self.wjs_jobs.iteritems():
                ret = job.wj_datapoint_send()
                if ret == 1:
                    deleted_jobs.append(job_id)

            for job_id in deleted_jobs:
                # TODO: remove the limiations
                del self.wj_jobs.wjs_jobs[job_id]

            self.wjs_condition.release()
            logging.debug("sent datapoints of jobs")
            sleep(METRIC_INTERVAL)


class HostForJob(object):
    """
    Each host has an object of HostForJob for each job
    """
    def __init__(self, host):
        self.hfj_host = host
        # Array of services for job
        self.hfj_services = {}
        self.hfj_rate_limit = None


class WatchedJob(object):
    """
    Each wathed job has an object of WatchedJob
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, job_id, jobs):
        self.wj_websockets = []
        self.wj_job_id = job_id
        self.wj_jobs = jobs
        self.wj_value = None
        self.wj_timestamp = None
        self.wj_services = {}
        self.wj_rate_limit = None
        self.wj_current_rate_limit = None
        # Host for each job
        self.wj_hosts = {}
        self.wj_tbf_name = lustre_config.tbf_escape_name(job_id)

    def wj_datapoint_add(self, service_id, timestamp, value):
        """
        Recived a datapoint of this job
        """
        if service_id not in self.wj_services:
            service = ServiceForJob()
            host = CLUSTER.lc_map_service_host[service_id]
            hostname = host.sh_hostname
            if hostname not in self.wj_hosts:
                logging.error("service [%s] is on host [%s]", service_id,
                              hostname)
                host_for_job = HostForJob(host)
                self.wj_hosts[hostname] = host_for_job
            else:
                host_for_job = self.wj_hosts[hostname]
            host_for_job.hfj_services[service_id] = service
            self.wj_services[service_id] = service
        else:
            service = self.wj_services[service_id]
        service.sfj_datapoint_add(timestamp, value)

    def wj_datapoint_send(self):
        """
        Send a datapoint to clients
        """
        dead_websockets = []
        rate = self.wj_rate_get()
        json_string = json.dumps({
            "type": "datapoint",
            "time": time.time(),
            "rate": rate,
            "job_id": self.wj_job_id})
        for websocket in self.wj_websockets:
            try:
                websocket.send(json_string)
            except WebSocketError:
                websocket.closed = True
                dead_websockets.append(websocket)

        for websocket in dead_websockets:
            self.wj_websockets.remove(websocket)
        if len(self.wj_websockets) == 0:
            return 1
        return 0

    def wj_rate_tune(self, rate):
        if self.wj_rate_limit is None:
            for hostname in self.wj_hosts:
                host = self.wj_hosts[hostname]
                if host.hfj_rate_limit is not None:
                    host.hfj_rate_limit = None
                    host.hfj_host.lh_change_tbf_rate(self.wj_tbf_name,
                                                     DEFAULT_RATE_LIMIT)
            return
        if self.wj_current_rate_limit != self.wj_rate_limit:
            # TODO: not very good algorithm
            rate_limit = self.wj_rate_limit / len(self.wj_hosts)
            for hostname in self.wj_hosts:
                host = self.wj_hosts[hostname]
                host.hfj_rate_limit = rate_limit
                host.hfj_host.lh_change_tbf_rate(self.wj_tbf_name,
                                                 rate_limit)
            self.wj_current_rate_limit = self.wj_rate_limit

    def wj_rate_get(self):
        """
        Return the current rate according the datapoints
        """
        rate = 0
        for service_id in self.wj_services:
            service = self.wj_services[service_id]
            service_rate = service.sfj_rate
            if service_rate is not None:
                rate += service_rate
        self.wj_rate_tune(rate)
        return rate


class ServiceForJob(object):
    """
    Each service (OST) has an object of ServiceForJob for each job
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        # Data collected from collectd
        self.sfj_value = None
        self.sfj_timestamp = None
        self.sfj_rate = None

    def sfj_datapoint_add(self, timestamp, value):
        """
        A datapoint is recived for this job and this service
        """
        # If overflow happens, rate will be kept unchanged for one interval
        if (self.sfj_timestamp is not None and
                value >= self.sfj_value and
                timestamp > self.sfj_timestamp):
            diff = value - self.sfj_value
            time_diff = timestamp - self.sfj_timestamp
            self.sfj_rate = diff / time_diff / 1000000
        self.sfj_timestamp = timestamp
        self.sfj_value = value


WATCHED_JOBS = WatchedJobs()


@APP.route("/")
def app_root():
    """
    Root of web
    """
    return render_template("index.html")


def tsdb_tags_parse(tsdb_tags, tag_dict):
    """
    Parse a TSDB tag string to dictionary
    """
    tags = tsdb_tags.split()
    for tag in tags:
        pair = tag.split("=")
        if len(pair) != 2:
            logging.error("tsdb tags [%s] is invalid", tsdb_tags)
            return -1
        tag_dict[pair[0]] = pair[1]
    return 0


@APP.route("/metric_post", methods=['POST'])
def app_metric_post():
    """
    A metric datapoint is recieved from Collectd
    """
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
        WATCHED_JOBS.wjs_metric_received(service_id, job_id, timestamp, value)
        logging.debug("service_id :%s, job_id: %s, time: %d, value: %d",
                      service_id, job_id, timestamp, value)
    return "Succeeded"


@APP.route("/console_websocket")
def app_console_websocket():
    """
    Start the websocket connection
    """
    # pylint: disable=too-many-locals,too-many-branches
    # pylint: disable=too-many-return-statements,too-many-statements
    if request.environ.get('wsgi.websocket'):
        websocket = request.environ['wsgi.websocket']
        config_string = websocket.receive()
        config = json.loads(config_string)
        jobs = config["jobs"]
        for job in jobs:
            job_id = job["job_id"]
            tbf_name = lustre_config.tbf_escape_name(job_id)
            WATCHED_JOBS.wjs_watch_job(job_id, websocket)
            CLUSTER.lc_start_tbf_rule(tbf_name, job_id, DEFAULT_RATE_LIMIT)

        while not websocket.closed:
            control_command = websocket.receive()
            logging.debug("command: %s", control_command)
            control = json.loads(control_command)
            job_id = control["job_id"]
            tbf_name = lustre_config.tbf_escape_name(job_id)
            rate = control["rate"]
            job = WATCHED_JOBS.wjs_find_job(job_id)
            if job is None:
                result = "failure"
            else:
                job.wj_rate_limit = int(rate)
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
            WATCHED_JOBS.wjs_unwatch_job(job_id, websocket)
        logging.debug("websocket is closed")
        return "Success"
    else:
        logging.info("run command is not websocket: %s")
        return "Failure"


def load_config():
    # pylint: disable=global-statement,too-many-return-statements
    """
    Load configuration file and do some initialization
    """
    global CLUSTER
    json_data = open('static/lime_config.json')
    config = json.load(json_data)

    logging.debug("config: %s", config)
    cluster = config["cluster"]
    fsname = cluster["name"]
    hosts = []
    for host in cluster["hosts"]:
        hosts.append(host["name"])
    identity = cluster["ssh_identity_file"]
    logging.debug("fsname: [%s], hosts: %s", fsname, hosts)
    CLUSTER = lustre_config.LustreCluster(fsname, hosts,
                                          ssh_identity_file=identity)
    ret = CLUSTER.lc_restart_collectd()
    if ret:
        return -1

    ret = CLUSTER.lc_detect_services()
    if ret:
        return -1

    ret = CLUSTER.lc_check_cpt_for_oss()
    if ret:
        return -1

    ret = CLUSTER.lc_enable_fifo_for_ost_io()
    if ret:
        return -1

    ret = CLUSTER.lc_enable_tbf_for_ost_io("jobid")
    if ret:
        return -1

    ret = CLUSTER.lc_set_jobid_var("procname_uid")
    if ret:
        return -1

    return 0


def start_web():
    """
    Start the web server of LIME
    """
    logdir = "log"
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    elif not os.path.isdir(logdir):
        logging.error("[%s] is not a directory", logdir)
    utils.configure_logging("./")
    ret = load_config()
    if ret:
        logging.error("failed to load config")
    monkey.patch_all()
    http_server = WSGIServer(('0.0.0.0', 24), APP,
                             handler_class=WebSocketHandler)
    http_server.serve_forever()


if __name__ == "__main__":
    start_web()
