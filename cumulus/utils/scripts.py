import distutils.spawn
import logging
import subprocess
import sys


logger = logging.getLogger(__name__)


def command_is_available(command):

    return distutils.spawn.find_executable(command) is not None

    # logger.debug("Test if command \"%s\" is available in bash shell." %
    #              command)
    # process = subprocess.Popen([command],
    #                            stdout=subprocess.PIPE,
    #                            stderr=subprocess.PIPE,
    #                            shell=True)

    # process.wait()

    # logger.debug("returncode: %s" % str(process.returncode))
    # logger.debug("stdout: %s" % process.stdout.read())
    # logger.debug("stderr: %s" % process.stderr.read())

    # if process.returncode is not None and process.returncode < 0:
    #     sys.stderr.write(process.stderr.read())
    #     sys.exit(1)

    # return not ("%s: command not found" % command) in process.stderr.read()


def cure(d):
    new_d = {}
    for k, v in d.iteritems():
        new_d[k.replace("-", "_")] = v

    return new_d
