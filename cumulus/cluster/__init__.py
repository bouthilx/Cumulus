import copy
import datetime
import logging
import os
import socket
import struct
import time

from .. import config
from ..ssh import (
    rsync, open_ssh_tunnel, receive, send, exec_command, line_buffered)
from ..scheduler.base import AbstractScheduler


logger = logging.getLogger(__name__)


# TODO, maybe the interface of ChildScheduler should have the same status than
# AbstractScheduler and just save internally the ones particular to each
# Scheduler. The informations spit out of get_queues do not contains
# ChildScheduler.status but AbstractScheduler.status after all.

SERVER_READY_LOG = (
    "INFO:cumulus.bin.cumulus_socket:Socket server now listening")

SERVER_ALREADY_RUNNING_LOG = (
    "WARNING:cumulus.bin.cumulus_socket:Socket server already running")


class Cluster(object):

    SETUP, SUBMIT, MONITOR, CANCEL, PING, CLOSE = range(6)

    def __init__(self, name, home, hostnames, username=None, password=None,
                 priority=None, lazy=False):

        self.name = name
        self.home = home
        self.priority = priority

        self.hostnames = hostnames
        if username is None:
            username = config["username"]
        self.username = username
        self.password = password
        self.sshtunnel = None
        self.connected_hostname = None
        if not lazy:
            self.start_remote_server()

    def detach_ssh_tunnel(self):
        ssh_tunnel = self.sshtunnel
        self.sshtunnel = None
        cluster = copy.deepcopy(self)
        self.sshtunnel = ssh_tunnel
        return cluster, ssh_tunnel

    def start_remote_server(self):
        logger.debug("Starting remote socket server")
        if self.connected_hostname is not None:
            raise RuntimeError("Remote socket server already started")

        # open socket server on cluster
        # use sockets to requests info from cluster
        # hostname, ssh_client = open_ssh_client(
        #     self.hostnames, self.username, self.password)
        # # ssh_client.exec_command(
        # _, stdout, stderr = ssh_client.exec_command(
        #     "nohup %(home)s/cumulus/cumulus/bin/cumulus-particle "
        #     "--work-on cumulus cumulus-socket open --port %(port)d -vv"
        #     ">>%(home)s/cumulus-%(name)s-open-socket.log 2>&1 </dev/null &" %
        #     dict(home=self.home, name=self.name,
        #          port=config["remote_port"]))

        # while not ssh_client.exit_status_ready():
        #     time.sleep(0.5)

        # ssh_client.close()

        # remote_addr = "127.0.0.1"
        self.connected_hostname, self.local_port, self.sshtunnel = (
            open_ssh_tunnel(
                self.hostnames, self.username, self.password,
                ssh_pkey="%s/.ssh/id_rsa" % os.environ["HOME"],
                remote_addr="localhost", remote_port=config["remote_port"]))

        _, stdout, stderr, nohup_channel = exec_command(
            self.sshtunnel._transport,
            "nohup %(home)s/cumulus/cumulus/bin/cumulus-particle "
            "--work-on cumulus cumulus-socket open --port %(port)d -vv"
            ">>%(home)s/cumulus-%(name)s-open-socket.log 2>&1 </dev/null &" %
            dict(home=self.home, name=self.name,
                 port=config["remote_port"]))

        nohup_channel.close()

        _, stdout, stderr, tail_channel = exec_command(
            self.sshtunnel._transport,
            "tail -n 1 -f %(home)s/cumulus-%(name)s-open-socket.log" %
            dict(home=self.home, name=self.name))

        logger.info("Waiting for server status confirmation")
        MAX_WAIT = 2 * 60
        start = time.clock()
        for line in line_buffered(stdout):
            line = line.strip()
            logger.debug(line)
            if (line == SERVER_READY_LOG or
                    line == SERVER_ALREADY_RUNNING_LOG or
                    time.clock() - start > MAX_WAIT):
                break

        logger.debug("Close tail channel")
        tail_channel.close()
        logger.debug("Received: %s" % line)

        # while not ssh_client.exit_status_ready():
        #     time.sleep(0.5)

        logger.debug("Remote socket server ready. Now verifying.")

        # logger.debug("SSH-client STDOUT: %s" % stdout.read())
        # logger.debug("SSH-client STDERR: %s" % stderr.read())

        logger.debug("Probing socket")
        MAX_WAIT = 5 * 60
        WAIT_STEP = 10
        waited = 0
        start = time.clock()
        while waited <= MAX_WAIT:
            try:
                rval = self.ping(date=str(datetime.datetime.now()),
                                 resilience=0)
            except (socket.error, struct.error):
                waited = time.clock() - start
                time.sleep(WAIT_STEP - (waited % WAIT_STEP))
                waited = time.clock() - start
                success = False
            else:
                logger.debug("PING: %s" % str(rval))
                success = True
                break

        if success:
            logger.info("Remote socket server ready.")
        else:
            raise RuntimeError("Failed to start remote server.")

    def close_remote_server(self):
        logger.info("Closing remote socket server")

        if self.connected_hostname is None:
            if self.sshtunnel is not None:
                self.sshtunnel.stop()
            raise RuntimeError("No remote server started")

        try:
            feedback = self._command(self.CLOSE, resilience=0)
        except socket.error:
            logger.warning("Tried to close remote socket server %s but it was "
                           "inaccessible." % self.connected_hostname)
        else:
            logger.info("Remote socket server closed.")
            logger.debug("Remote socket server closed with message: %s" %
                         str(feedback))
        finally:
            self.connected_hostname = None
            if self.sshtunnel is not None:
                self.sshtunnel.stop()
                self.sshtunnel = None

    def __del__(self):
        if self.connected_hostname is not None:
            self.close_remote_server()

    def get_client_socket(self, resilience=1):
        if self.connected_hostname is None and not self.lazy:
            raise RuntimeError("No remote server started")
        elif self.connected_hostname is None and self.lazy:
            self.start_remote_server()

        logger.debug("Opening a socket")
        s = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)

        # bind_host = "0.0.0.0"
        # bind_port = 0
        # logger.debug("Binding to (%s, %d)" % (bind_host, bind_port))
        # s.bind((bind_host, bind_port))

        host = self.connected_hostname
        ip = "localhost"  # socket.gethostbyname(host)
        port = self.local_port  # config["port"]
        logger.debug("Connecting to (%s=%s, %d)" % (host, ip, port))
        s.settimeout(5)
        try:
            s.connect((ip, port))
        except socket.error:
            if resilience <= 0:
                raise

            logger.debug("Socket connection to server failed. Trying to "
                         "relaunch the socket server")
            self.connected_hostname = None
            self.start_remote_server()
            s = self.get_client_socket(resilience - 1)
        finally:
            s.settimeout(None)

        logger.debug("Connected to (%s=%s, %d)" % (host, ip, port))

        return s

    def _command(self, command_id, resilience=1, **kwargs):
        client_socket = self.get_client_socket(resilience)
        logger.debug("Sending command=%d" % command_id)
        send(client_socket, command=command_id)
        logger.debug("Sending %s" % str(kwargs))
        send(client_socket, **kwargs)
        response = receive(client_socket)
        logger.debug("Received %s" % str(response))
        client_socket.close()
        logger.debug("Client socket closed.")
        return response

    def ping(self, **kwargs):
        return self._command(self.PING, **kwargs)

    def setup(self, **kwargs):
        return self._command(self.SETUP, **kwargs)

    def deploy(self, **kwargs):
        return self._command(self.SUBMIT, **kwargs)

    def queue(self, **kwargs):
        return self._command(self.MONITOR, **kwargs)["queue"]

    def cancel(self, **kwargs):
        return self._command(self.CANCEL, **kwargs)

    def get_free_slots(self):
        jobs = self.get_queues()
        logger.info("%d jobs found by parser" % len(jobs))
        submitted_jobs = 0
        queued_jobs = 0
        for job in jobs.itervalues():
            if AbstractScheduler.COMPLETED in job['job_array']:
                del job['job_array'][AbstractScheduler.COMPLETED]
            submitted_job_arrays = sum(job['job_array'].itervalues())
            submitted_jobs += submitted_job_arrays
            running_jobs = job['job_array'][AbstractScheduler.RUNNING]
            queued_jobs += (submitted_job_arrays - running_jobs)

        logger.info("%d queued jobs found by parser" % queued_jobs)
        logger.info("%d submitted_jobs found by parser" % submitted_jobs)

        if queued_jobs < self.threshold:  # and threshold < submitted_jobs:
            return self.max_jobs - submitted_jobs
        else:
            return 0

    def retrieve_logs(self, output_dir):
        stdout, stderr = rsync(self, output_dir)
