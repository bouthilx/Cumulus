"""
    Cluster starts a socket server and keeps it open until it is done with its
    requests.
    Ex:
        Cumulus([a single cluster config], lazy=False)
            Cluster.start_remote_server()
        Clumulus.deploy()
            Cluster.queues()
            Cluster.deploy()
        del Cumulus
            Cluster.close_remote_server()

    There could be many Cluster built in parallel. Then start_remote_server
    registers the number of request and direct them all to the same socket
    server. The socket server doesn't stop on close_remote_server until all
    Cluster has called close_remote_server.
"""

import atexit
import argparse
import glob
import json
import logging
import multiprocessing
import os
import re
import select
import socket
import sys
import time

import lockfile

from cumulus.cluster import Cluster
from cumulus.scheduler import Scheduler
from cumulus.ssh import receive, send


logger = logging.getLogger(__name__)


OPEN = "open"

META_LOCK_FILE = os.path.join(os.environ["HOME"], "lock.lock")
LOCK_FILE = os.path.join(os.environ["HOME"], "cumulus-open-socket.lock")
lock_regex = re.compile('%s[.lock]*$' % LOCK_FILE)

CLOSE_BUFFER = 60  # 1 minute

scheduler = Scheduler()


def get_lock_files():
    candidates = glob.glob(LOCK_FILE.replace(".lock", ".lock*"))
    return sorted([lock_file for lock_file in candidates
                   if bool(lock_regex.match(lock_file))])


def request_lock():
    logging.debug("Requesting lock")
    with lockfile.FileLock(META_LOCK_FILE):
        lock_files = get_lock_files()
        logging.debug("%d locks found" % len(lock_files))
        if len(lock_files) > 0:
            lock_file = lock_files[-1] + ".lock"
        else:
            lock_file = LOCK_FILE

        with open(lock_file, 'a'):
            os.utime(lock_file, None)

        logging.debug("Adding %s" % lock_file)

    logging.debug("Now have %d locks" % (len(lock_files) + 1))
    return len(lock_files) + 1


def release_lock():
    logging.debug("Requesting lock")
    with lockfile.FileLock(META_LOCK_FILE):
        lock_files = get_lock_files()
        if len(lock_files) == 0:
            raise RuntimeError("Tried to released lock when none was "
                               "requested")
        lock_file = lock_files.pop(-1)
        logging.debug("Removing %s" % lock_file)
        os.remove(lock_file)

    logging.debug("Now have %d locks" % (len(lock_files) + 1))
    return len(lock_files)


def release_all_locks():
    request_lock()
    while release_lock() > 0:
        pass


def setup(client_socket, address):
    kwargs = json.loads(receive(client_socket))
    # TODO Oh oh, setup should be done by cluster, not scheduler...
    submitted = dict(returncode=1, message="Not implemented yet")
    send(client_socket, json.dumps(submitted))


def submit(client_socket, address):
    kwargs = json.loads(receive(client_socket))
    submitted = scheduler.queue(**kwargs)
    submitted["returncode"] = 0
    send(client_socket, json.dumps(submitted))


def monitor(client_socket, address):
    kwargs = json.loads(receive(client_socket))
    queue = scheduler.queue(**kwargs)
    queue["returncode"] = 0
    send(client_socket, json.dumps(queue))


def cancel(client_socket, address):
    kwargs = json.loads(receive(client_socket))
    rval = scheduler.cancel(**kwargs)
    rval["returncode"] = 0
    send(client_socket, json.dumps(rval))


def close(clientsocket, address):
    logger.debug("Waiting %d seconds before closing" % CLOSE_BUFFER)
    time.sleep(CLOSE_BUFFER)
    try:
        release_lock()
    except RuntimeError, e:
        message = dict(
            returncode=1,
            message=e.message)
    else:
        message = dict(
            returncode=0,
            message="")

    send(clientsocket, json.dumps(message))


def invalid(clientsocket, address, command):
    message = (
        "invalid command from address %s: %s" %
        (address, command))
    logger.error(message)

    message = (
        "invalid command: %s" %
        (address, command))
    message = dict(
        returncode=1,
        message=message)

    send(clientsocket, json.dumps(message))


def start_server(self, port):
    if request_lock() > 1:
        release_lock()
        logger.warning("Socket server already running")
        return

    atexit.register(release_all_locks)

    serversocket = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((socket.gethostname(), port))
    serversocket.listen(10)

    (clientsocket, address) = serversocket.accept()
    p = multiprocessing.Process(
        target=cmd, args=[clientsocket, address])
    p.start()
    processes = [p]
    read_list = [serversocket, clientsocket]
    while 1:
        # Accept connections from outside
        readable, writable, errored = select.select(read_list, [], [])

        if len(get_lock_files()) == 0:
            logger.debug("All requests to socket server are done. Closing "
                         "socket server")
            break

        for s in readable:
            # Add a new client
            if s is serversocket:
                # Set a small timeout to avoid blocking on accept.
                # It is very likely that no other socket get connected
                # and the server cannot receive the close command if it is
                # blocked on accept()
                # defaulttimeout = socket.getdefaulttimeout()
                # Gives 30 seconds for a new client to connect if any other
                # socket.setdefaulttimeout(30)
                try:
                    (clientsocket, address) = serversocket.accept()
                except socket.timeout:
                    pass
                else:
                    read_list.append(clientsocket)
                    p = multiprocessing.Process(
                        target=cmd, args=[clientsocket, address])
                    p.start()
                    processes.append(p)
                # finally:
                #     socket.setdefaulttimeout(defaulttimeout)
            # Remove any client which is done
            else:
                s_index = read_list.index(s)
                process = processes[s_index - 1]
                if not process.is_alive():
                    del processes[s_index - 1]
                    del read_list[s_index]

    serversocket.close()


def cmd(clientsocket, address):
    command = receive(clientsocket)
    if command == Cluster.SETUP:
        setup(clientsocket, address)
    if command == Cluster.SUBMIT:
        submit(clientsocket, address)
    elif command == Cluster.MONITOR:
        monitor(clientsocket, address)
    elif command == Cluster.CANCEL:
        cancel(clientsocket, address)
    elif command == Cluster.CLOSE:
        close(clientsocket, address)
    else:
        invalid(clientsocket, address, command)

    clientsocket.close()


def get_options(argv):

    parser = argparse.ArgumentParser(description="WRITEME")

    subparsers = parser.add_subparsers(dest="command")

    open_subparser = subparsers.add_parser(
        OPEN, help="Start a server to listen to socket commands")

    open_subparser.add_argument(
        "--port", default="80",
        help="Update stack if already installed")

    return parser.parse_args(argv)


def main(argv=None):
    options = get_options(argv)

    if options.command == OPEN:
        start_server(options.port)


if __name__ == "__main__":
    main(sys.argv[1:])
