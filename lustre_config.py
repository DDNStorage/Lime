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


class LustreDevice(object):
    """
    Each Lustre device has an object of LustreDevice
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, cluster, service_type, service_index, host):
        self.ld_cluster = cluster
        self.ld_service_type = service_type
        self.ld_service_index = service_index
        self.ld_host = host


def find_device_in_list(devices, service_type, service_index):
    """
    Find a device according to the service type and index
    """
    for device in devices:
        if (device.ld_service_type == service_type and
                device.ld_service_index == service_index):
            return device
    return None


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
        self.lh_devices = []
        self.lh_cluster = cluster
        self.lh_lustre_version_string = None
        self.lh_lustre_version_major = None
        self.lh_lustre_version_minor = None
        self.lh_lustre_version_patch = None
        self.lh_lustre_version_fix = None
        self.lh_version_value = None
        self.lh_detect_lustre_version()

    def lh_detect_devices(self, cluster_devices):
        """
        Detect the devices on this host
        """
        logging.debug("detecting devices on host [%s]", self.sh_hostname)
        devices = []

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
                device_name = ("%s-MDT%s" % (self.lh_cluster.lc_fsname,
                                             mdt_index))
                logging.debug("device [%s] running on host [%s]",
                              device_name, self.sh_hostname)
                device = find_device_in_list(cluster_devices, "MDT", mdt_index)
                if device is not None:
                    logging.error("two hosts [%s] and [%s] for device [%s]",
                                  device.ld_host.sh_hostname,
                                  self.sh_hostname, device_name)
                    return -1
                device = LustreDevice(self.lh_cluster, "MDT", mdt_index, self)
                cluster_devices.append(device)
                devices.append(device)

            match = self.lh_cluster.lc_ost_regular.match(line)
            if match:
                ost_index = match.group("ost_index")
                device_name = ("%s-OST%s" % (self.lh_cluster.lc_fsname,
                                             ost_index))
                logging.debug("device [%s] running on host [%s]",
                              device_name, self.sh_hostname)
                device = find_device_in_list(cluster_devices, "OST", ost_index)
                if device is not None:
                    logging.error("two hosts [%s] and [%s] for device [%s]",
                                  device.ld_host.sh_hostname,
                                  self.sh_hostname, device_name)
                    return -1
                device = LustreDevice(self.lh_cluster, "OST", ost_index, self)
                cluster_devices.append(device)
                devices.append(device)

            match = self.lh_cluster.lc_mgs_pattern.match(line)
            if match:
                device_name = "MGS"
                logging.debug("device [%s] running on host [%s]",
                              device_name, self.sh_hostname)
                device = find_device_in_list(cluster_devices, "MGS", 0)
                if device is not None:
                    logging.error("two hosts [%s] and [%s] for device [%s]",
                                  device.ld_host.sh_hostname,
                                  self.sh_hostname, device_name)
                device = LustreDevice(self.lh_cluster, "MGS", 0, self)
                cluster_devices.append(device)
                devices.append(device)
        self.lh_devices = devices
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


class LustreCluster(object):
    """
    Each Lustre cluster has an object of LustreCluster
    """
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
        self.lc_mgs_pattern = re.compile(mgs_pattern)
        for hostname in server_hostnames:
            host = LustreHost(self, hostname, identity_file=ssh_identity_file)
            self.lc_hosts.append(host)
        self.lc_devices = []

    def lc_detect_devices(self):
        """
        Detect the devices in this Lustre cluster
        """
        devices = []
        for host in self.lc_hosts:
            ret = host.lh_detect_devices(devices)
            if ret:
                logging.error("failed to detect devices on host [%s]",
                              host.sh_hostname)
                return ret
        self.lc_devices = devices
        return 0

    def lc_check_cpt_for_oss(self):
        """
        Check whether the cpu_npartitions module param of libcfs is 1
        """
        hosts = []
        for device in self.lc_devices:
            if device.ld_service_type != "OST":
                continue
            if device.ld_host.sh_hostname in hosts:
                continue
            ret = device.ld_host.lh_check_cpt()
            if ret:
                logging.error("failed to check CPT on host [%s]",
                              device.ld_host.sh_hostname)
                return ret
            hosts.append(device.ld_host.sh_hostname)
        return 0

    def lc_enable_fake_io_for_oss(self):
        """
        Enable fake IO on OSS
        """
        hosts = []
        for device in self.lc_devices:
            if device.ld_service_type != "OST":
                continue
            if device.ld_host.sh_hostname in hosts:
                continue
            ret = device.ld_host.lh_enable_fake_io()
            if ret:
                logging.error("failed to enable fake IO on host [%s]",
                              device.ld_host.sh_hostname)
                return ret
            hosts.append(device.ld_host.sh_hostname)
        return 0

    def lc_enable_tbf_for_ost_io(self, tbf_type):
        """
        Change the OST IO NRS policy to TBF
        """
        hosts = []
        for device in self.lc_devices:
            if device.ld_service_type != "OST":
                continue
            if device.ld_host.sh_hostname in hosts:
                continue
            ret = device.ld_host.lh_enable_tbf_for_ost_io(tbf_type)
            if ret:
                logging.error("failed to enable TBF for ost_io on host [%s]",
                              device.ld_host.sh_hostname)
                return ret
            hosts.append(device.ld_host.sh_hostname)
        return 0

    def lc_set_jobid_var(self, jobid_var):
        """
        Change the Job ID variable on this cluster
        """
        done = False
        for device in self.lc_devices:
            if device.ld_service_type != "MGS":
                continue
            ret = device.ld_host.lh_set_jobid_var(jobid_var)
            if ret:
                logging.error("failed to set jobid_var on host [%s]",
                              device.ld_host.sh_hostname)
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
        for device in self.lc_devices:
            if device.ld_service_type != "OST":
                continue
            if device.ld_host.sh_hostname in hosts:
                continue
            ret = device.ld_host.lh_enable_fifo_for_ost_io()
            if ret:
                logging.error("failed to disable TBF on host [%s]",
                              device.ld_host.sh_hostname)
                return ret
            hosts.append(device.ld_host.sh_hostname)
        return 0

    def lc_start_tbf_rule(self, name, expression, rate):
        """
        Start a TBF rule
        """
        hosts = []
        for device in self.lc_devices:
            if device.ld_service_type != "OST":
                continue
            if device.ld_host.sh_hostname in hosts:
                continue
            ret = device.ld_host.lh_start_tbf_rule(name, expression, rate)
            if ret:
                logging.error("failed to start TBF rule [%s] on host [%s]",
                              name, device.ld_host.sh_hostname)
                return ret
            hosts.append(device.ld_host.sh_hostname)
        return 0

    def lc_change_tbf_rate(self, name, rate):
        """
        Change rate of a TBF rule
        """
        hosts = []
        for device in self.lc_devices:
            if device.ld_service_type != "OST":
                continue
            if device.ld_host.sh_hostname in hosts:
                continue
            ret = device.ld_host.lh_change_tbf_rate(name, rate)
            if ret:
                logging.error("failed to start TBF rule [%s] on host [%s]",
                              name, device.ld_host.sh_hostname)
                return ret
            hosts.append(device.ld_host.sh_hostname)
        return 0


def test_tbf():
    """
    Check whether TBF works well
    """
    cluster = LustreCluster("lustre", ["10.0.0.24"])
    cluster.lc_detect_devices()
    cluster.lc_enable_tbf_for_ost_io("nid")
    cluster.lc_set_jobid_var("procname_uid")


if __name__ == "__main__":
    test_tbf()
