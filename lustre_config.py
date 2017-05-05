# Copyright (c) 2017 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Config Lustre using SSH connection
"""

import re
import logging

# local libs
import ssh_host
import utils

class LustreService(object):
    # pylint: disable=too-few-public-methods
    """
    Each Lustre service has an object of LustreService
    """
    TYPE_OST = "OST"
    TYPE_MDT = "MDT"
    TYPE_MGS = "MGS"
    TYPE_CLIENT = "CLIENT"

    def __init__(self, cluster, service_type, service_name, host,
                 mount_point=None):
        # pylint: disable=too-many-arguments
        self.ls_cluster = cluster
        self.ls_service_type = service_type
        self.ls_service_name = service_name
        self.ls_host = host
        self.ls_mount_point = mount_point


def version_value(major, minor, patch):
    """
    Return a numeric version code based on a version string.  The version
    code is useful for comparison two version strings to see which is newer.
    """
    value = (major << 16) | (minor << 8) | patch
    return value


def tbf_escape_name(name):
    """
    The valid name of a TBF rule is only alpha, number and "_"
    """
    good_name = ""
    for char in name:
        if char.isalnum() or char == "_":
            good_name += char
        else:
            good_name += "_"
    return good_name


class LustreHost(ssh_host.SSHHost):
    """
    Eacho host in a Lustre clustre has an object of LustreHost
    """
    # pylint: disable=too-many-public-methods,too-many-instance-attributes
    def __init__(self, cluster, hostname, identity_file=None):
        super(LustreHost, self).__init__(hostname, identity_file=identity_file)
        self.lh_services = {}
        self.lh_cluster = cluster
        self.lh_lustre_version_string = None
        self.lh_lustre_version_major = None
        self.lh_lustre_version_minor = None
        self.lh_lustre_version_patch = None
        self.lh_lustre_version_fix = None
        self.lh_version_value = None
        self.lh_detect_lustre_version()

    def lh_detect_services(self, cluster_services, map_service_host):
        # pylint: disable=too-many-statements
        """
        Detect the services on this host
        """
        logging.debug("detecting services on host [%s]", self.sh_hostname)
        services = {}

        command = ("lctl dl")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        logging.debug("command [%s] output on host [%s]: [%s]",
                      command, self.sh_hostname, retval.cr_stdout)
        for line in retval.cr_stdout.splitlines():
            logging.debug("checking line [%s]", line)
            match = self.lh_cluster.lc_mdt_regular.match(line)
            if match:
                mdt_index = match.group("mdt_index")
                service_name = ("MDT%s" % (mdt_index))
                logging.debug("service [%s] running on host [%s]",
                              service_name, self.sh_hostname)
                if service_name in cluster_services:
                    service = cluster_services[service_name]
                    service = cluster_services[service_name]
                    logging.error("two hosts [%s] and [%s] for service [%s]",
                                  service.ls_host.sh_hostname,
                                  self.sh_hostname, service_name)
                    return -1
                service = LustreService(self.lh_cluster,
                                        LustreService.TYPE_MDT,
                                        service_name,
                                        self)
                cluster_services[service_name] = service
                services[service_name] = service
                map_service_host[service_name] = self

            match = self.lh_cluster.lc_ost_regular.match(line)
            if match:
                ost_index = match.group("ost_index")
                service_name = ("OST%s" % (ost_index))
                logging.debug("service [%s] running on host [%s]",
                              service_name, self.sh_hostname)
                if service_name in cluster_services:
                    service = cluster_services[service_name]
                    logging.error("two hosts [%s] and [%s] for service [%s]",
                                  service.ls_host.sh_hostname,
                                  self.sh_hostname, service_name)
                    return -1
                service = LustreService(self.lh_cluster,
                                        LustreService.TYPE_OST, service_name,
                                        self)
                cluster_services[service_name] = service
                services[service_name] = service
                map_service_host[service_name] = self

            match = self.lh_cluster.lc_mgs_regular.match(line)
            if match:
                service_name = "MGS"
                logging.debug("service [%s] might be running on host [%s]",
                              service_name, self.sh_hostname)

                filesystems = self.lh_mgs_get_filesystems()
                if self.lh_cluster.lc_fsname not in filesystems:
                    continue                

                if service_name in cluster_services:
                    service = cluster_services[service_name]
                    logging.error("two hosts [%s] and [%s] for service [%s]",
                                  service.ls_host.sh_hostname,
                                  self.sh_hostname, service_name)
                    return -1
                service = LustreService(self.lh_cluster,
                                        LustreService.TYPE_MGS,
                                        service_name, self)
                cluster_services[service_name] = service
                services[service_name] = service
                map_service_host[service_name] = self

        # Detect Lustre client
        command = ("cat /proc/mounts | grep lustre")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            logging.debug("checking line [%s]", line)
            match = self.lh_cluster.lc_client_regular.match(line)
            if match:
                mount_point = match.group("mount_point")
                service_name = ("client:%s:%s" %
                                (self.sh_hostname, mount_point))
                service = LustreService(self.lh_cluster,
                                        LustreService.TYPE_CLIENT,
                                        service_name, self,
                                        mount_point=mount_point)
                cluster_services[service_name] = service
                services[service_name] = service
                map_service_host[service_name] = self
                logging.debug("service [%s] running on host [%s]",
                              service_name, self.sh_hostname)

        self.lh_services = services
        return 0

    def lh_enable_tbf_for_ost_io(self, tbf_type):
        """
        Change the OST IO NRS policy to TBF
        """
        command = ("echo -n tbf %s > "
                   "/proc/fs/lustre/ost/OSS/ost_io/nrs_policies" % tbf_type)
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_enable_fifo_for_ost_io(self):
        """
        Change the OST IO NRS policy to FIFO
        """
        command = ("echo -n fifo > "
                   "/proc/fs/lustre/ost/OSS/ost_io/nrs_policies")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_set_jobid_var(self, jobid_var):
        """
        Set the job ID variable
        """
        command = ("lctl conf_param %s.sys.jobid_var=%s" %
                   (self.lh_cluster.lc_fsname, jobid_var))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_start_tbf_rule(self, name, expression, rate):
        """
        Start an TBF rule
        """
        if self.lh_version_value >= version_value(2, 8, 54):
            command = ("echo -n start %s jobid={%s} rate=%d > "
                       "/proc/fs/lustre/ost/OSS/ost_io/nrs_tbf_rule" %
                       (name, expression, rate))
        else:
            command = ("echo -n start %s {%s} %d > "
                       "/proc/fs/lustre/ost/OSS/ost_io/nrs_tbf_rule" %
                       (name, expression, rate))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_stop_tbf_rule(self, name):
        """
        Start an TBF rule
        """
        command = ("echo -n stop %s > "
                   "/proc/fs/lustre/ost/OSS/ost_io/nrs_tbf_rule" %
                   (name))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_change_tbf_rate(self, name, rate):
        """
        Change the TBF rate of a rule
        """
        if self.lh_version_value >= version_value(2, 8, 54):
            command = ("echo -n change %s rate=%d > "
                       "/proc/fs/lustre/ost/OSS/ost_io/nrs_tbf_rule" %
                       (name, rate))
        else:
            command = ("echo -n change %s %d > "
                       "/proc/fs/lustre/ost/OSS/ost_io/nrs_tbf_rule" %
                       (name, rate))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_detect_lustre_version(self):
        """
        Detect the Lustre version
        """
        command = ("cat /proc/fs/lustre/version | grep lustre: | "
                   "awk '{print $2}'")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        self.lh_lustre_version_string = retval.cr_stdout.strip()
        version_pattern = (r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)\."
                           r"(?P<fix>\d+)$")
        version_regular = re.compile(version_pattern)
        match = version_regular.match(self.lh_lustre_version_string)
        if match:
            self.lh_lustre_version_major = int(match.group("major"))
            self.lh_lustre_version_minor = int(match.group("minor"))
            self.lh_lustre_version_patch = int(match.group("patch"))
            self.lh_lustre_version_fix = int(match.group("fix"))
        else:
            logging.error("unexpected version string format: %s",
                          self.lh_lustre_version_string)
            return -1

        self.lh_version_value = version_value(self.lh_lustre_version_major,
                                              self.lh_lustre_version_minor,
                                              self.lh_lustre_version_patch)
        logging.debug("version_string: %s %d", self.lh_lustre_version_string,
                      self.lh_version_value)
        return 0

    def lh_check_cpt(self):
        """
        Check whether the cpu_npartitions module param of libcfs is 1
        """
        command = ("cat /sys/module/libcfs/parameters/cpu_npartitions")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        cpu_npartitions = retval.cr_stdout.strip()
        if cpu_npartitions != "1":
            logging.error("The cpu_npartitions module param of libcfs is [%s]."
                          "This is not good since TBF only works well with "
                          "one CPT",
                          cpu_npartitions)
            return -1
        return 0

    def lh_enable_fake_io(self):
        """
        Enable the fake IO
        """
        command = ("lctl set_param fail_loc=0x238")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_clear_loc(self):
        """
        Clearn LOC
        """
        command = ("lctl set_param fail_loc=0")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_restart_collectd(self):
        """
        Restart collectd
        """
        # Sometimes collectd is dead, and service restart collectd stuck
        # there forever, so kill it by signal 9
        command = ("ps aux | grep /usr/sbin/collectd | grep -v grep "
                   "| awk '{print $2}'")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        process_id = retval.cr_stdout.strip()
        if process_id != "":
            command = ("kill -9 %s" % process_id)
            retval = self.sh_run(command)
            if retval.cr_exit_status != 0:
                logging.error("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command, self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1

        command = ("service collectd restart")
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_io_thread(self, login_name, fname):
        """
        The thread of doing I/O
        """
        while True:
            command = ("dd if=/dev/zero of=%s bs=1M" % (fname))
            retval = self.sh_run(command, login_name=login_name)
            if retval.cr_exit_status != 0:
                logging.error("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command, self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1
        return 0

    def lh_start_io(self, service, index, stripe_count=None,
                    login_name="root"):
        """
        Start IO with user
        """
        fname = ("%s/%s_%s_%s" % (service.ls_mount_point, login_name,
                                  service.ls_host.sh_hostname, index))

        command = ("rm -f %s" % (fname))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        stripe = ""
        if stripe_count is not None:
            stripe = ("-c %d" % stripe_count)

        command = ("lfs setstripe %s %s" % (stripe, fname))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        command = ("chmod 777 %s" % (fname))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        utils.thread_start(self.lh_io_thread, (login_name, fname))
        return 0

    def lh_stop_io(self, service):
        """
        Start IO with user
        """
        command = ("fuser -km %s" % (service.ls_mount_point))
        retval = self.sh_run(command)
        if (retval.cr_exit_status != 0 and retval.cr_exit_status != 1 and
                retval.cr_stderr != ""):
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_benchmark(self, service, stripe_count = None):
        """
        Benchmark the system performance from clients.
        """
        fname = ("%s/%s_benchmark" % (service.ls_mount_point,
                                  service.ls_host.sh_hostname))

        command = ("rm -f %s" % (fname))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        stripe = ""
        if stripe_count is not None:
            stripe = ("-c %d" % stripe_count)

        command = ("lfs setstripe %s %s" % (stripe, fname))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        command = ("chmod 777 %s" % (fname))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        command = ("dd if=/dev/zero of=%s bs=1M count=5000 2>&1 | "
                   "awk '/bytes/ {print $8\" \"$9 }'" % (fname))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        retlist = retval.cr_stdout.split()
        logging.error(retlist)
        #max_iops = 0
        if retlist[1] == "GB/s" :
            max_iops = float(retlist[0]) * 1000
        elif retlist[1] == "MB/s" :
            max_iops = float(retlist[0])
        else :
            logging.error("Unknown dd ouput [%s]!", retval.cr_stdout)
            max_iops = 0

        ret = self.lh_remove_files(service)
        if ret:
            return ret

        return max_iops

    def lh_remove_files(self, service):
        """
        Remove files under root directory
        """
        command = ("rm -f %s/*" % (service.ls_mount_point))
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        return 0

    def lh_mgs_get_filesystems(self):
        """
        This host is a MGS, get the filesystems on this MGS
        """
        command = "cat /proc/fs/lustre/mgs/MGS/filesystems"
        retval = self.sh_run(command)
        if retval.cr_exit_status != 0:
            logging.error("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return []
        return retval.cr_stdout.split()


class LustreCluster(object):
    """
    Each Lustre cluster has an object of LustreCluster
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, fsname, server_hostnames, ssh_identity_file=None):
        self.lc_hosts = []
        self.lc_fsname = fsname
        mdt_pattern = (r"^.+ UP mdt %s-MDT(?P<mdt_index>\S+) .+$" %
                       self.lc_fsname)
        logging.debug("mdt_pattern: [%s]", mdt_pattern)
        self.lc_mdt_regular = re.compile(mdt_pattern)
        ost_pattern = (r"^.+ UP obdfilter %s-OST(?P<ost_index>\S+) .+$" %
                       self.lc_fsname)
        logging.debug("ost_pattern: [%s]", ost_pattern)
        self.lc_ost_regular = re.compile(ost_pattern)
        mgs_pattern = (r"^.+ UP mgs MGS MGS .+$")
        logging.debug("mgs_pattern: [%s]", mgs_pattern)
        self.lc_mgs_regular = re.compile(mgs_pattern)
        client_pattern = (r"^.+:/%s (?P<mount_point>\S+) lustre .+$" %
                          self.lc_fsname)
        self.lc_client_regular = re.compile(client_pattern)
        logging.debug("client_pattern: [%s]", client_pattern)
        for hostname in server_hostnames:
            host = LustreHost(self, hostname, identity_file=ssh_identity_file)
            self.lc_hosts.append(host)
        self.lc_services = {}
        # Mapping from service name to host
        self.lc_map_service_host = {}
        self.lc_ost_number = 0
        self.lc_client_number = 0
        self.lc_max_real_iops = 0
        self.lc_max_fake_iops = 0

    def lc_detect_services(self):
        """
        Detect the services in this Lustre cluster
        """
        services = {}
        map_service_host = {}
        for host in self.lc_hosts:
            ret = host.lh_detect_services(services, map_service_host)
            if ret:
                logging.error("failed to detect services on host [%s]",
                              host.sh_hostname)
                return ret
        self.lc_services = services
        self.lc_map_service_host = map_service_host
        for service_name, service in self.lc_services.iteritems():
            logging.debug("itering on service [%s]", service_name)
            if service.ls_service_type == LustreService.TYPE_OST:
                self.lc_ost_number += 1
            elif service.ls_service_type == LustreService.TYPE_CLIENT:
                self.lc_client_number += 1
        return 0

    def lc_check_cpt_for_oss(self):
        """
        Check whether the cpu_npartitions module param of libcfs is 1
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != "OST":
                continue
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_check_cpt()
            if ret:
                logging.error("failed to check CPT on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_enable_fake_io_for_oss(self):
        """
        Enable fake IO on OSS
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != LustreService.TYPE_OST:
                continue
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_enable_fake_io()
            if ret:
                logging.error("failed to enable fake IO on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_clear_loc_for_oss(self):
        """
        Clear LOC, thus fake IO on OSS will be disabled
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != LustreService.TYPE_OST:
                continue
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_clear_loc()
            if ret:
                logging.error("failed to clear LOC on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_enable_tbf_for_ost_io(self, tbf_type):
        """
        Change the OST IO NRS policy to TBF
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != LustreService.TYPE_OST:
                continue
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_enable_tbf_for_ost_io(tbf_type)
            if ret:
                logging.error("failed to enable TBF for ost_io on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_set_jobid_var(self, jobid_var):
        """
        Change the Job ID variable on this cluster
        """
        done = False
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != "MGS":
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_set_jobid_var(jobid_var)
            if ret:
                logging.error("failed to set jobid_var on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            done = True
        if not done:
            logging.error("no MGS host found for cluster [%s]",
                          self.lc_fsname)
            return -1
        return 0

    def lc_enable_fifo_for_ost_io(self):
        """
        Change the OST IO NRS policy to FIFO
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != LustreService.TYPE_OST:
                continue
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_enable_fifo_for_ost_io()
            if ret:
                logging.error("failed to disable TBF on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_start_tbf_rule(self, name, expression, rate):
        """
        Start a TBF rule
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != LustreService.TYPE_OST:
                continue
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_start_tbf_rule(name, expression, rate)
            if ret:
                logging.error("failed to start TBF rule [%s] on host [%s]",
                              name, service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_stop_tbf_rule(self, name):
        """
        Start a TBF rule
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != LustreService.TYPE_OST:
                continue
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_stop_tbf_rule(name)
            if ret:
                logging.error("failed to stop TBF rule [%s] on host [%s]",
                              name, service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_change_tbf_rate(self, name, rate):
        """
        Change rate of a TBF rule
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_service_type != LustreService.TYPE_OST:
                continue
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_change_tbf_rate(name, rate)
            if ret:
                logging.error("failed to start TBF rule [%s] on host [%s]",
                              name, service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_restart_collectd(self):
        """
        Restart collectd
        """
        hosts = []
        for service_name, service in self.lc_services.iteritems():
            if service.ls_host.sh_hostname in hosts:
                continue
            logging.debug("itering on service [%s]", service_name)
            ret = service.ls_host.lh_restart_collectd()
            if ret:
                logging.error("failed to restart collectd on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            hosts.append(service.ls_host.sh_hostname)
        return 0

    def lc_benchmark(self):
        """
        Benchmark the performance: fake I/O and real I/O
        """
        stripe_count = None
        if self.lc_ost_number != 0:
            stripe_count = self.lc_ost_number

        for service_name, service in self.lc_services.iteritems():
            logging.debug("itering on service [%s]", service_name)
            if service.ls_service_type != LustreService.TYPE_CLIENT:
                continue
            ret = self.lc_enable_fifo_for_ost_io()
            if ret:
                return -1

            ret = self.lc_clear_loc_for_oss()
            if ret:
                return -1

            ret = service.ls_host.lh_benchmark(service,
                                              stripe_count=stripe_count)
            if ret < 0:
                logging.error("failed to benchmark real I/O on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            self.lc_max_real_iops = ret

            self.lc_enable_fake_io_for_oss()
            ret = service.ls_host.lh_benchmark(service,
                                               stripe_count=stripe_count)
            if ret < 0:
                logging.error("failed to benchmark fake I/O on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            self.lc_max_fake_iops = ret
            break

        logging.debug("Benchmark performce max_real_iops [%d] max_fake_iops [%d]",
                      self.lc_max_real_iops, self.lc_max_fake_iops)


    def lc_start_io(self, jobs):
        """
        Start IO
        """
        if len(jobs) > self.lc_client_number:
            logging.error("not enough client [%d] for jobs [%d]",
                          self.lc_client_number, len(jobs))
            return -1

        stripe_count = None
        if self.lc_ost_number != 0:
            stripe_count = self.lc_ost_number

        for service_name in self.lc_services:
            service = self.lc_services[service_name]
            if service.ls_service_type != LustreService.TYPE_CLIENT:
                continue
            ret = service.ls_host.lh_stop_io(service)
            if ret:
                logging.error("failed to stop I/O on host [%s]",
                              service.ls_host.sh_hostname)
                return ret

        for service_name in self.lc_services:
            service = self.lc_services[service_name]
            if service.ls_service_type == LustreService.TYPE_CLIENT:
                ret = service.ls_host.lh_remove_files(service)
                if ret:
                    logging.error("failed to remove files on host [%s]",
                                  service.ls_host.sh_hostname)
                    return ret
                break

        index = 0
        for service_name, service in self.lc_services.iteritems():
            job = jobs[index]
            login_name = job["login_name"]
            logging.debug("itering on service [%s]", service_name)
            if service.ls_service_type != LustreService.TYPE_CLIENT:
                continue
            ret = service.ls_host.lh_start_io(service, index,
                                              stripe_count=stripe_count,
                                              login_name=login_name)
            if ret:
                logging.error("failed to start I/O on host [%s]",
                              service.ls_host.sh_hostname)
                return ret
            index += 1
        return 0


def test_tbf():
    """
    Check whether TBF works well
    """
    cluster = LustreCluster("lustre", ["10.0.0.24"])
    cluster.lc_detect_services()
    cluster.lc_enable_tbf_for_ost_io("nid")
    cluster.lc_set_jobid_var("procname_uid")


if __name__ == "__main__":
    test_tbf()
