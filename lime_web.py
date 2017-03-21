# pylint: disable=too-many-lines
# Copyright (c) 2017 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Web inteface of LIME
"""
import collections
import json
import os
import threading
import logging
import logging.handlers
import time
import sys
import random
from gevent.wsgi import WSGIServer
from gevent import monkey, sleep
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket.exceptions import WebSocketError

import utils
import lustre_config

from flask import Flask, render_template, request
APP = Flask(__name__)


METRIC_INTERVAL = 1
CLUSTER = None
DEFAULT_RATE_LIMIT = 10000
MIN_RATE_LIMIT = 10


class RatePolicy(object):
    # pylint: disable=too-few-public-methods
    """
    One kind of policy to tune the rate
    """
    def __init__(self, name, comment, tune_func):
        self.rp_name = name
        self.rp_comment = comment
        self.rp_tune_func = tune_func


class IndependentRatePolicy(RatePolicy):
    """
    The policy of tuning rates of jobs regardless of other jobs
    """
    def __init__(self):
        comment = ("The policy of tuning rates of jobs regardless of other "
                   "jobs. This policy is suitable for following sitations: "
                   "1) cluster only has one job;\n"
                   "2) cluster has enough bandwiths for all jobs;\n"
                   "3) jobs are doing fake I/O and also network bandwidth is "
                   "not performance bottleneck.")
        super(IndependentRatePolicy, self).__init__("independent", comment,
                                                    self.irp_tune)

    def irp_job_tune(self, job):
        # pylint: disable=too-many-branches,no-self-use
        """
        Tune the setting according to rate
        """
        rate = job.wj_rate
        if job.wj_rate_limit is None:
            for hostname in job.wj_hosts:
                host = job.wj_hosts[hostname]
                if host.hfj_rate_limit < DEFAULT_RATE_LIMIT:
                    host.hfj_rate_limit = DEFAULT_RATE_LIMIT
                    host.hfj_host.lh_change_tbf_rate(job.wj_tbf_name,
                                                     DEFAULT_RATE_LIMIT)
            return
        if job.wj_current_rate_limit != job.wj_rate_limit:
            # IMPROVE: not perfect algorithm, need to set on active hosts,
            # rather than all hosts.
            if len(job.wj_hosts) == 0:
                return
            rate_limit = job.wj_rate_limit / len(job.wj_hosts)
            if rate_limit > DEFAULT_RATE_LIMIT:
                rate_limit = DEFAULT_RATE_LIMIT
            for hostname in job.wj_hosts:
                host = job.wj_hosts[hostname]
                host.hfj_rate_limit = rate_limit
                host.hfj_host.lh_change_tbf_rate(job.wj_tbf_name,
                                                 rate_limit)
            job.wj_current_rate_limit = job.wj_rate_limit
            return

        if rate > job.wj_rate_limit * 11 / 10:
            job.wj_decrease_highest_host(job.wj_rate - job.wj_rate_limit)
            return

        if rate < job.wj_rate_limit * 9 / 10:
            job.wj_increase_lowest_host()

    def irp_tune(self, qos_task):
        """
        Tune the jobs
        """
        for job_id in qos_task.wjs_jobs:
            job = qos_task.wjs_jobs[job_id]
            self.irp_job_tune(job)


class ActionHistory(object):
    """
    Action history
    """
    # pylint: disable=too-many-instance-attributes
    RESULT_RISE = "rise"
    RESULT_DECLINE = "decline"
    RESULT_UNCHANGED = "unchanged"
    ACTION_INCREASE_MYSELF = "increase myself"
    ACTION_DECREASE_MYSELF = "decrease myself"
    ACTION_DECREASE_OTHERS = "decrease others"
    STAGE_ORIGIN = "origin"
    STAGE_ACTED = "acted"
    STAGE_REGRETTED = "regretted"

    def __init__(self, qos_task, job_id, action_type, action_job_id,
                 action_hostname, limit_before, limit_after, expected_result):
        # pylint: disable=too-many-arguments
        self.ah_qos_task = qos_task
        self.ah_job_id = job_id
        self.ah_rates_original = qos_task.wjs_save_rates(job_id, action_job_id)
        self.ah_stage = ActionHistory.STAGE_ORIGIN

        self.ah_action_good = None
        self.ah_action_job_id = action_job_id
        self.ah_action_hostname = action_hostname
        self.ah_action_limit_before = limit_before
        self.ah_action_limit_after = limit_after

        self.ah_rates_after_action = None
        self.ah_action_type = action_type
        self.ah_action_expected_result = expected_result

        self.ah_rates_after_regret = None
        self.ah_regret_type = None
        self.ah_regret_expected_result = None

        self.ah_failure_time = 0

    def ah_prior_declined_after_action(self):
        """
        Check whether this action causes decline of jobs with higher priority
        """
        for job_id in self.ah_rates_after_action:
            if self.ah_job_id == job_id:
                break
            rate_after_action = self.ah_rates_after_action[job_id]
            if job_id not in self.ah_rates_original:
                continue
            rate_original = self.ah_rates_original[job_id]
            if rate_after_action + MIN_RATE_LIMIT / 2 < rate_original:
                return True
        return False

    def ah_acted_declined_after_action(self):
        """
        Check whether this action causes decline of the tuned job
        """
        rate_original = self.ah_rates_original[self.ah_action_job_id]
        rate_after_action = self.ah_rates_after_action[self.ah_action_job_id]
        if rate_after_action + MIN_RATE_LIMIT / 2 < rate_original:
            return True
        return False

    def ah_expected_action_result(self):
        """
        Whether the action has expected result
        """
        # pylint: disable=too-many-return-statements
        job_id = self.ah_job_id
        if job_id not in self.ah_rates_original:
            logging.error("can't get original rate of job [%s]", job_id)
            return False
        if job_id not in self.ah_rates_after_action:
            logging.error("can't get rate of job [%s] after action", job_id)
            return False
        rate_original = self.ah_rates_original[job_id]
        rate_after_action = self.ah_rates_after_action[job_id]
        if self.ah_action_expected_result == ActionHistory.RESULT_RISE:
            if rate_after_action < rate_original + MIN_RATE_LIMIT:
                return False
            else:
                return True
        else:
            assert (self.ah_action_expected_result ==
                    ActionHistory.RESULT_DECLINE)
            if rate_after_action + MIN_RATE_LIMIT > rate_original:
                return False
            else:
                return True
        return False

    def ah_declined_after_regret(self):
        """
        Whether the hosts with higher priority got performance decline
        """
        for job_id in self.ah_rates_after_regret:
            if self.ah_job_id == job_id:
                break
            rate_after_regret = self.ah_rates_after_regret[job_id]
            if job_id not in self.ah_rates_original:
                continue
            rate_original = self.ah_rates_original[job_id]
            if rate_after_regret + MIN_RATE_LIMIT < rate_original:
                return True
        return False

    def ah_regret(self):
        """
        Regret the action
        """
        assert self.ah_stage == ActionHistory.STAGE_ACTED
        logging.error("changing rate of host [%s] for job [%s] from [%d] "
                      "back to [%d]",
                      self.ah_action_hostname,
                      self.ah_action_job_id,
                      self.ah_action_limit_after,
                      self.ah_action_limit_before)
        if self.ah_action_job_id not in self.ah_qos_task.wjs_jobs:
            return -1
        job = self.ah_qos_task.wjs_jobs[self.ah_action_job_id]
        if self.ah_action_hostname not in job.wj_hosts:
            return -1
        host = job.wj_hosts[self.ah_action_hostname]
        ret = host.hfj_change_tbf_rate(self.ah_action_limit_before)
        if ret:
            return ret
        self.ah_stage = ActionHistory.STAGE_REGRETTED
        return 0

    def ah_act(self):
        """
        Do the action
        """
        assert self.ah_stage == ActionHistory.STAGE_ORIGIN
        if (self.ah_action_type == ActionHistory.ACTION_DECREASE_MYSELF or
                self.ah_action_type == ActionHistory.ACTION_INCREASE_MYSELF or
                self.ah_action_type == ActionHistory.ACTION_DECREASE_OTHERS):
            logging.error("changing rate of host [%s] for job [%s] from [%d] "
                          "to [%d]",
                          self.ah_action_hostname,
                          self.ah_action_job_id,
                          self.ah_action_limit_before,
                          self.ah_action_limit_after)
            if self.ah_action_job_id not in self.ah_qos_task.wjs_jobs:
                return -1
            job = self.ah_qos_task.wjs_jobs[self.ah_action_job_id]
            if self.ah_action_hostname not in job.wj_hosts:
                return -1
            host = job.wj_hosts[self.ah_action_hostname]
            ret = host.hfj_change_tbf_rate(self.ah_action_limit_after)
            if ret:
                return ret
        else:
            assert 0
        self.ah_stage = ActionHistory.STAGE_ACTED
        return 0

    def ah_process(self, qos_task):
        """
        Return True if action processed, return false if the action ended
        """
        job_id = self.ah_job_id
        action_id = self.ah_action_job_id
        logging.error("processing action with stage [%s]", self.ah_stage)
        if self.ah_stage == ActionHistory.STAGE_ACTED:
            self.ah_rates_after_action = qos_task.wjs_save_rates(job_id,
                                                                 action_id)
            self_benefit = self.ah_expected_action_result()
            if (self.ah_prior_declined_after_action() or
                    ((not self_benefit) and
                     self.ah_acted_declined_after_action())):
                self.ah_failure_time += 1
                self.ah_regret()
                self.ah_action_good = False
            elif not self_benefit:
                self.ah_failure_time += 1
                self.ah_action_good = False
            else:
                self.ah_action_good = True
        else:
            assert (self.ah_stage ==
                    ActionHistory.STAGE_REGRETTED)
            self.ah_rates_after_regret = qos_task.wjs_save_rates(job_id,
                                                                 action_id)
            if self.ah_declined_after_regret():
                logging.error("action caused declining and regetting "
                              "didn't recover it")
            else:
                logging.error("action caused declining and regetting "
                              "recovered it")
        return False


class PriorityRatePolicy(RatePolicy):
    """
    The policy that tries to satisfy the requests of jobs with highest priority
    first
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        comment = ("The policy that tries to satisfy the limits of jobs "
                   "with highest priority first. If the job with highest "
                   "priority doesn't "
                   "get enough rate, try two options:\n"
                   "1) Increase rate limitation of itself\n"
                   "2) Decrease rate limitation of others\n"
                   "Option 1) will be tried first, and if it fails, option 2)"
                   "will be tried.")
        super(PriorityRatePolicy, self).__init__("priority", comment,
                                                 self.prp_tune)
        self.prp_last_action = None
        self.prp_max_failures = 3
        # Interval of changing rate
        self.prp_interval = 2
        self.prp_count = 0

    def prp_rate_limit_update(self, qos_task):
        """
        The rate is updated by GUI
        """
        # pylint: disable=no-self-use
        changed = False
        for job_id in qos_task.wjs_jobs:
            job = qos_task.wjs_jobs[job_id]
            if job.wj_current_rate_limit == job.wj_rate_limit:
                continue
            job.wj_current_rate_limit = job.wj_rate_limit
            if len(job.wj_hosts) == 0:
                continue
            # IMPROVE: not perfect algorithm, need to set on active hosts,
            # rather than all hosts.
            rate_limit = job.wj_rate_limit / len(job.wj_hosts)
            if rate_limit > DEFAULT_RATE_LIMIT:
                rate_limit = DEFAULT_RATE_LIMIT
            for hostname in job.wj_hosts:
                host = job.wj_hosts[hostname]
                host.hfj_rate_limit = rate_limit
                host.hfj_host.lh_change_tbf_rate(job.wj_tbf_name,
                                                 rate_limit)
                changed = True
                logging.error("updated rate limit of job [%s] on host [%s] "
                              "from GUI", job_id, hostname)
        return changed

    def prp_increase_self(self, qos_task, job, job_id, failure_time):
        """
        Try to increase the rate of itself
        """
        hosts = job.wj_hosts_random()
        selected = None
        for host in hosts:
            logging.error("checking host [%s] with total throughput [%d]",
                          host.hfj_host.sh_hostname, host.hfj_rate)
            diff = MIN_RATE_LIMIT * 2
            limit_after = host.hfj_rate_limit + diff
            if limit_after > DEFAULT_RATE_LIMIT:
                limit_after = DEFAULT_RATE_LIMIT
            if limit_after == host.hfj_rate_limit:
                logging.error("not able to start an increase action for job "
                              "[%s] on host [%s] because the action would "
                              "change nothing", job_id,
                              host.hfj_host.sh_hostname)
                continue
            selected = host
            break
        if selected is None:
            return -1
        new_act = ActionHistory(qos_task, job_id,
                                ActionHistory.ACTION_INCREASE_MYSELF,
                                job_id, selected.hfj_host.sh_hostname,
                                selected.hfj_rate_limit, limit_after,
                                ActionHistory.RESULT_RISE)
        logging.error("trying to increase rate of job [%s] by "
                      "increasing its limitation",
                      job_id)
        new_act.ah_failure_time = failure_time
        ret = new_act.ah_act()
        if ret:
            return ret
        self.prp_last_action = new_act
        return 0

    def prp_decrease_others(self, qos_task, job, job_id, failure_time):
        """
        Decrease the rate of some other job.
        """
        logging.error("trying to increase rate of job [%s] by "
                      "decreasing rates of other jobs", job_id)
        host = None
        for hostname in job.wj_hosts:
            logging.error("checking any job to decrease rate on host [%s] ",
                          hostname)
            higher_priority = True
            if hostname not in job.wj_hosts:
                logging.error("not going to decrease jobs on host [%s] "
                              "because job [%s] has no rate on host [%s]",
                              hostname, job_id, hostname)
                continue
            for tmp_job_id in qos_task.wjs_jobs:
                tmp_job = qos_task.wjs_jobs[tmp_job_id]
                if tmp_job_id == job_id:
                    higher_priority = False
                    continue
                if higher_priority:
                    logging.error("not going to decrease job [%s] because "
                                  "it has higher priority", tmp_job_id)
                    continue
                if tmp_job.wj_rate == 0:
                    logging.error("not going to decrease job [%s] because "
                                  "it has no rate", tmp_job_id)
                    continue
                if hostname not in tmp_job.wj_hosts:
                    logging.error("not going to decrease job [%s] because "
                                  "it has no rate on host [%s]", tmp_job_id,
                                  hostname)
                    continue
                tmp_host = tmp_job.wj_hosts[hostname]
                if host is None or host.hfj_rate < tmp_host.hfj_rate:
                    host = tmp_host
        if host is None:
            logging.error("no job to decrease rate in order to "
                          "increase rate of job [%s]", job_id)
            return -1
        logging.error("selected job [%s] to decrease rate in order to "
                      "increase rate of job [%s]", host.hfj_job.wj_job_id,
                      job_id)
        # Decrease the rate to provide extra rate for job with higher priority
        #if job.wj_rate_limit is None:
        #    diff = DEFAULT_RATE_LIMIT
        #else:
        #    diff = job.wj_rate_limit - job.wj_rate
        #limit_after = host.hfj_rate_limit - diff
        #if limit_after < MIN_RATE_LIMIT:
        #    limit_after = MIN_RATE_LIMIT
        limit_after = MIN_RATE_LIMIT
        if host.hfj_rate_limit == limit_after:
            logging.error("no job to decrease rate in order to "
                          "increase rate of job [%s]", job_id)
            return -1
        new_act = ActionHistory(qos_task, job_id,
                                ActionHistory.ACTION_DECREASE_OTHERS,
                                host.hfj_job.wj_job_id,
                                host.hfj_host.sh_hostname,
                                host.hfj_rate_limit, limit_after,
                                ActionHistory.RESULT_RISE)
        new_act.ah_failure_time = failure_time
        ret = new_act.ah_act()
        if ret:
            return ret
        self.prp_last_action = new_act
        return 0

    def prp_start_action(self, qos_task, job_id, failure_time):
        # pylint: disable=too-many-branches,too-many-return-statements
        """
        Start an action. If started, return 0, else -1.
        """
        logging.error("checking whether to start an action for job [%s]",
                      job_id)
        if job_id not in qos_task.wjs_jobs:
            return -1
        job = qos_task.wjs_jobs[job_id]
        rate = job.wj_rate
        if (job.wj_rate_limit is not None and
                rate > job.wj_rate_limit * 11 / 10):
            host = job.wj_highest_throughput_host()
            if host is None or host.hfj_rate < MIN_RATE_LIMIT:
                logging.error("not able to start a decrease action for job "
                              "[%s] because all host has very small rate",
                              job_id)
                return -1

            diff = rate - job.wj_rate_limit
            limit_after = host.hfj_rate - diff
            if limit_after < MIN_RATE_LIMIT:
                limit_after = MIN_RATE_LIMIT
            new_act = ActionHistory(qos_task, job_id,
                                    ActionHistory.ACTION_DECREASE_MYSELF,
                                    job_id, host.hfj_host.sh_hostname,
                                    host.hfj_rate_limit, limit_after,
                                    ActionHistory.RESULT_DECLINE)

            new_act.ah_failure_time = failure_time
            ret = new_act.ah_act()
            if ret:
                return ret
            self.prp_last_action = new_act
            logging.error("trying to decrease rate of job [%s]",
                          job_id)
            return 0

        action = self.prp_last_action
        if job.wj_rate_limit is None or rate < job.wj_rate_limit * 9 / 10:
            if job.wj_rate_limit is None or action is None:
                increase = True
            elif action.ah_action_type == ActionHistory.ACTION_INCREASE_MYSELF:
                assert action.ah_job_id == job_id
                if action.ah_action_good:
                    increase = True
                else:
                    increase = False
            elif action.ah_action_type == ActionHistory.ACTION_DECREASE_MYSELF:
                assert action.ah_job_id == job_id
                if action.ah_action_good:
                    increase = True
                else:
                    increase = False
            else:
                assert (action.ah_action_type ==
                        ActionHistory.ACTION_DECREASE_OTHERS)
                assert action.ah_job_id == job_id
                if action.ah_action_good:
                    increase = False
                else:
                    increase = True

            if increase:
                ret = self.prp_increase_self(qos_task, job,
                                             job_id, failure_time)
                if ret == 0:
                    return 0
                ret = self.prp_decrease_others(qos_task, job,
                                               job_id, failure_time)
            else:
                ret = self.prp_decrease_others(qos_task, job,
                                               job_id, failure_time)
                if ret == 0:
                    return 0
                ret = self.prp_increase_self(qos_task, job,
                                             job_id, failure_time)
            return ret
        return -1

    def prp_tune(self, qos_task):
        # pylint: disable=too-many-branches
        """
        Tune the jobs
        """
        # If anything changed in the rate configuration, set the limitation
        # and loop back from top priority.
        self.prp_count += 1
        if self.prp_count < self.prp_interval:
            return
        self.prp_count = 0
        ret = self.prp_rate_limit_update(qos_task)
        if ret:
            self.prp_last_action = None
            return

        # Continue the action if there is one
        job_id = None
        if self.prp_last_action is not None:
            action = self.prp_last_action
            job_id = action.ah_job_id
            ret = self.prp_last_action.ah_process(qos_task)
            if ret:
                return
            else:
                if action.ah_failure_time > self.prp_max_failures:
                    self.prp_last_action = None
                    logging.error("too many action failures for job [%s], "
                                  "won't try any more", job_id)
                else:
                    ret = self.prp_start_action(qos_task, job_id,
                                                action.ah_failure_time)
                    if ret == 0:
                        return
                    else:
                        self.prp_last_action = None

        # Start an action if no on-going action
        assert self.prp_last_action is None
        found = False
        if job_id is None:
            found = True
        for tmp_id in qos_task.wjs_jobs:
            if found:
                ret = self.prp_start_action(qos_task, tmp_id, 0)
                if ret == 0:
                    return
            elif tmp_id == job_id:
                found = True


class WatchedJobs(object):
    """
    All the watched Jobs will be group here
    """
    def __init__(self, fake_io):
        self.wjs_jobs = collections.OrderedDict()
        self.wjs_condition = threading.Condition()

        self.wjs_rate_policies = []
        self.wjs_independent_rate_policy = IndependentRatePolicy()
        self.wjs_rate_policies.append(self.wjs_independent_rate_policy)
        self.wjs_priority_policy = PriorityRatePolicy()
        self.wjs_rate_policies.append(self.wjs_priority_policy)
        self.wjs_current_policy = self.wjs_priority_policy
        self.wjs_current_fake_io = fake_io
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
            tbf_name = lustre_config.tbf_escape_name(job_id)
            CLUSTER.lc_start_tbf_rule(tbf_name, job_id, DEFAULT_RATE_LIMIT)
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
            tbf_name = lustre_config.tbf_escape_name(job_id)
            CLUSTER.lc_stop_tbf_rule(tbf_name)
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
                tbf_name = lustre_config.tbf_escape_name(job_id)
                CLUSTER.lc_stop_tbf_rule(tbf_name)
                del self.wjs_jobs[job_id]

            self.wjs_current_policy.rp_tune_func(self)
            self.wjs_condition.release()
            logging.debug("sent datapoints of jobs")
            sleep(METRIC_INTERVAL)

    def wjs_save_rates(self, end_job_id, action_job_id):
        """
        Save the rates before a job_id
        """
        rates = collections.OrderedDict()
        for job_id in self.wjs_jobs:
            job = self.wjs_jobs[job_id]
            rates[job_id] = job.wj_rate
            if job_id == end_job_id:
                break
        if action_job_id not in rates:
            rates[action_job_id] = self.wjs_jobs[action_job_id].wj_rate
        return rates

    def wjs_update_config(self, config):
        """
        Update the configuration, usually through GUI
        """
        cluster = config["cluster"]
        policy_name = cluster["policy"]
        jobs = cluster["jobs"]
        fake_io = cluster["fake_io"]
        self.wjs_condition.acquire()
        if self.wjs_current_policy.rp_name != policy_name:
            for policy in self.wjs_rate_policies:
                if policy.rp_name == policy_name:
                    logging.error("changing policy to %s", policy_name)
                    self.wjs_current_policy = policy
                    break
        if fake_io != self.wjs_current_fake_io:
            logging.error("changing fake I/O to %s", fake_io)
            if fake_io:
                ret = CLUSTER.lc_enable_fake_io_for_oss()
            else:
                ret = CLUSTER.lc_clear_loc_for_oss()
            if ret:
                logging.error("failed to enable/disable fake I/O")
            else:
                self.wjs_current_fake_io = fake_io
        for config_job in jobs:
            job_id = config_job["job_id"]
            thoughput = config_job["throughput"]
            job = self.wjs_jobs[job_id]
            job.wj_rate_limit = int(thoughput)
        self.wjs_condition.release()


class HostForJob(object):
    """
    Each host has an object of HostForJob for each job
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, job, host):
        self.hfj_host = host
        # Array of services for job
        self.hfj_services = {}
        self.hfj_rate_limit = DEFAULT_RATE_LIMIT
        self.hfj_rate = 0
        self.hfj_job = job

    def hfj_change_tbf_rate(self, rate_limit):
        """
        Change the job's rate on this host
        """
        ret = self.hfj_host.lh_change_tbf_rate(self.hfj_job.wj_tbf_name,
                                               rate_limit)
        if ret == 0:
            self.hfj_rate_limit = rate_limit
        return ret


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
        self.wj_rate = None
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
                host_for_job = HostForJob(self, host)
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

    def wj_rate_get(self):
        """
        Return the current rate according the datapoints
        """
        rate = 0
        for hostname in self.wj_hosts:
            host = self.wj_hosts[hostname]
            host.hfj_rate = 0
            for service_id in host.hfj_services:
                service = host.hfj_services[service_id]
                service_rate = service.sfj_rate
                if service_rate is not None:
                    rate += service_rate
                    host.hfj_rate += service_rate
        self.wj_rate = rate
        return rate

    def wj_highest_limit_host(self):
        """
        Return the host with the highest rate limit
        """
        selected = None
        for hostname in self.wj_hosts:
            host = self.wj_hosts[hostname]
            if (selected is None or
                    selected.hfj_rate_limit < host.hfj_rate_limit):
                selected = host
        return selected

    def wj_highest_throughput_host(self):
        """
        Return the host with the highest throughput
        """
        selected = None
        for hostname in self.wj_hosts:
            host = self.wj_hosts[hostname]
            if (selected is None or
                    selected.hfj_rate < host.hfj_rate):
                selected = host
        return selected

    def wj_hosts_sort_by_throughput(self):
        """
        Sort the host by its throughput
        """
        hosts = []
        for hostname in self.wj_hosts:
            host = self.wj_hosts[hostname]
            hosts.append(host)
        return sorted(hosts, key=lambda host: host.hfj_rate)

    def wj_hosts_random(self):
        """
        Put the host randomly
        """
        hosts = []
        for hostname in self.wj_hosts:
            host = self.wj_hosts[hostname]
            hosts.append(host)
        random.shuffle(hosts)
        return hosts

    def wj_decrease_highest_host(self, diff):
        """
        Decrease the limit of the host with highest rate limit
        """
        # IMPROVEMENT: this is not perfect way to select the host
        # instead, should select the host with the higest rate
        selected = self.wj_highest_limit_host()
        if selected is None:
            logging.error("no selected host to decrease rate")
            return -1
        old = selected.hfj_rate_limit
        # The rate is lower than the limit, there is other bottleneck
        # Set the rate limit to the real limit to speedup the decrease process
        if old > selected.hfj_rate * 11 / 10:
            selected.hfj_rate_limit = selected.hfj_rate

        if diff + MIN_RATE_LIMIT > selected.hfj_rate_limit:
            selected.hfj_rate_limit = MIN_RATE_LIMIT
        else:
            selected.hfj_rate_limit -= diff
        logging.info("decreasing rate of host [%s] for job [%s] from [%d] "
                     "to [%d]",
                     selected.hfj_host.sh_hostname,
                     self.wj_job_id,
                     old, selected.hfj_rate_limit)
        selected.hfj_host.lh_change_tbf_rate(self.wj_tbf_name,
                                             selected.hfj_rate_limit)
        return 0

    def wj_increase_lowest_host(self):
        """
        Increase the limit of the host with lowest rate limit
        """
        selected = None
        for hostname in self.wj_hosts:
            host = self.wj_hosts[hostname]
            if host.hfj_rate_limit >= DEFAULT_RATE_LIMIT:
                continue
            if (selected is None or
                    selected.hfj_rate_limit > host.hfj_rate_limit):
                selected = host
        if selected is None:
            logging.error("no selected host to increase rate")
            return
        old = selected.hfj_rate_limit
        diff = self.wj_rate_limit - self.wj_rate
        selected.hfj_rate_limit += diff
        if selected.hfj_rate_limit > DEFAULT_RATE_LIMIT:
            selected.hfj_rate_limit = DEFAULT_RATE_LIMIT
        logging.info("increasing rate of host [%s] for job [%s] from [%d] "
                     "to [%d]",
                     selected.hfj_host.sh_hostname,
                     self.wj_job_id,
                     old, selected.hfj_rate_limit)
        selected.hfj_host.lh_change_tbf_rate(self.wj_tbf_name,
                                             selected.hfj_rate_limit)
        return


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


WATCHED_JOBS = None


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
        cluster = config["cluster"]
        jobs = cluster["jobs"]
        for job in jobs:
            job_id = job["job_id"]
            WATCHED_JOBS.wjs_watch_job(job_id, websocket)

        while not websocket.closed:
            data = websocket.receive()
            logging.debug("command: %s", data)
            config = json.loads(data)
            ret = WATCHED_JOBS.wjs_update_config(config)
            if ret:
                result = "failure"
            else:
                result = "success"

            json_string = json.dumps({
                "type": "command_result",
                "command": "change_config",
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
    fake_io = cluster["fake_io"]
    jobs = cluster["jobs"]
    logging.debug("fsname: [%s], hosts: %s", fsname, hosts)
    CLUSTER = lustre_config.LustreCluster(fsname, hosts,
                                          ssh_identity_file=identity)
    logging.debug("detecting services")
    global WATCHED_JOBS
    WATCHED_JOBS = WatchedJobs(fake_io)
    ret = CLUSTER.lc_detect_services()
    if ret:
        return -1

    ret = CLUSTER.lc_restart_collectd()
    if ret:
        return -1

    if fake_io:
        ret = CLUSTER.lc_enable_fake_io_for_oss()
        if ret:
            return -1
    else:
        ret = CLUSTER.lc_clear_loc_for_oss()
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

    ret = CLUSTER.lc_start_io(jobs)
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
        sys.exit(-1)
    utils.configure_logging(logdir)
    ret = load_config()
    if ret:
        logging.error("failed to load config")
        sys.exit(ret)
    monkey.patch_all()
    http_server = WSGIServer(('0.0.0.0', 24), APP,
                             handler_class=WebSocketHandler)
    http_server.serve_forever()
    sys.exit(0)


if __name__ == "__main__":
    start_web()
