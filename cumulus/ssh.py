import json
import logging
import struct
import subprocess

import numpy
import paramiko
import sshtunnel

logger = logging.getLogger(__name__)

SUBMIT, MONITOR, CANCEL, CLOSE = range(4)


def receive(channel):
    try:
        size = struct.unpack("i", channel.recv(struct.calcsize("i")))[0]
        data = ""
        while len(data) < size:
            msg = channel.recv(size - len(data))
            if not msg:
                return None
            data += msg
        return json.loads(data.strip())
    except OSError as e:
        print e
        return False


def send(channel, **kwargs):
    message = json.dumps(kwargs)
    channel.send(struct.pack('i', len(message)) + message)


def open_ssh_client(hostnames, username, password):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(
        paramiko.AutoAddPolicy())

    error = None
    for hostname in hostnames:
        try:
            client.connect(hostname, username=username, password=password)
            connected = True
        except BaseException, e:
            connected = False
            error = e

        if connected:
            break

    # No connection work so we try again outside of To raise and error
    if not connected:
        raise error

    return hostname, client  # .get_transport().open_session()


def open_ssh_tunnel(
        hostname, username, password, remote_addr, remote_port,
        ssh_pkey=None, local_port=None,
        local_bind_address="0.0.0.0"):

    if local_port is None:
        local_ports = list(numpy.random.random_integers(1024, 65535, size=20))
    elif not isinstance(local_port, (list, tuple)):
        local_ports = [local_port]
    else:
        local_ports = local_ports

    # TODO Support many hostnames in case one is down
    hostname = hostname[0]

    for local_port_candidate in local_ports:
        logger.debug("Probing ssh-tunnel: ssh %s@%s -L %s:%s:%s:%s" %
                     (username, hostname, local_bind_address,
                      local_port_candidate, remote_addr, remote_port))
        server = sshtunnel.SSHTunnelForwarder(
            hostname,
            ssh_username=username,
            ssh_password=password,
            ssh_pkey=ssh_pkey,
            remote_bind_address=(remote_addr, remote_port),
            local_bind_address=("0.0.0.0", local_port_candidate)
        )  # port))
        server.start()
        break

    return hostname, local_port_candidate, server


# Inspired from https://stackoverflow.com/a/33334998
def line_buffered(f, bufsize=1):
    line_buf = ""
    while not f.channel.exit_status_ready():
        new_chunk = f.read(bufsize)
        line_buf += new_chunk
        if '\n' in new_chunk:
            lines = line_buf.split("\n")
            for line in lines[:-1]:
                yield line

            line_buf = lines[-1]


# From paramiko.client.SSHClient
def exec_command(
        transport,
        command,
        bufsize=-1,
        timeout=None,
        get_pty=False,
        environment=None):
        """
        Execute a command on the SSH server.  A new `.Channel` is opened and
        the requested command is executed.  The command's input and output
        streams are returned as Python ``file``-like objects representing
        stdin, stdout, and stderr.
        :param str command: the command to execute
        :param int bufsize:
            interpreted the same way as by the built-in ``file()`` function in
            Python
        :param int timeout:
            set command's channel timeout. See `.Channel.settimeout`
        :param dict environment:
            a dict of shell environment variables, to be merged into the
            default environment that the remote command executes within.
            .. warning::
                Servers may silently reject some environment variables; see the
                warning in `.Channel.set_environment_variable` for details.
        :return:
            the stdin, stdout, and stderr of the executing command, as a
            3-tuple
        :raises: `.SSHException` -- if the server fails to execute the command
        """
        chan = transport.open_session(timeout=timeout)
        if get_pty:
            chan.get_pty()
        chan.settimeout(timeout)
        if environment:
            chan.update_environment(environment)
        chan.exec_command(command)
        stdin = chan.makefile('wb', bufsize)
        stdout = chan.makefile('r', bufsize)
        stderr = chan.makefile_stderr('r', bufsize)
        return stdin, stdout, stderr, chan


def remotely(fct):

    def call(cluster, *args, **kwargs):
        client = open_ssh_client(cluster)

        result = fct(cluster, SSHClient(client), *args, **kwargs)

        client.close()

        return result

    return call


def rsync(cluster, output_dir):
    command = (
        ("rsync -arvu %(username)s:%(hostname)s "
         "%(cluster_log_dir)s %(output_dir)s") %
        dict(username=cluster.username, hostname=cluster.hostname,
             cluster_log_dir=cluster.log_dir, output_dir=output_dir))

    subprocess.check_call(command)
    return


def SSHClient(object):
    def __init__(self, client):
        self.client = client

    def submit(self, *args):
        _, stdout, stderr = self.client.exec_command(
            "cumulus-particle submit")
        return stdout.read(), stderr.read()

    def monitor(self):
        _, stdout, stderr = self.client.exec_command(
            "cumulus-particle monitor")
        return stdout.read(), stderr.read()

    def cancel(self, job_ids):
        _, stdout, stderr = self.client.exec_command(
            "cumulus-particle cancel \"%s\"" % job_ids)
        return stdout.read(), stderr.read()


def start_remote_server(self):
    _, stdout, stderr = self.client.exec_command("cumulus-socket open")
    return stdout.read(), stderr.read()
