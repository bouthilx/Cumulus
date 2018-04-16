import logging
import subprocess


logger = logging.getLogger(__name__)


def submit(queue, job_name, time, row_ids, arguments):
    command = build_command(queue, job_name, time, row_ids, arguments)
    process = subprocess.Popen(command)
    stdoutdata, stderrdata = process.communicate()
    if process.returncode > 0:
        print stdoutdata
        raise RuntimeError(stderrdata)

    lines = stdoutdata.split("\n")
    while lines:
        line = lines.pop(0)
        if line.strip() == "Jobs id:":
            break

    if lines:
        job_ids = lines[0].split(" ")
    else:
        job_ids = []

    return job_ids


def build_command(queue, job_name, time, row_ids, arguments):
    command = ["smart-dispatch", "-q", queue, "-C", "1", "-G", "1",
               "-t", time, "launch"]

    arguments = arguments.split()
    if "--select" in arguments:
        logger.warning("Options --select should not be given in arguments. "
                       "It is built using given row_ids. "
                       "Dropping it from arguments...")
        select_arg_index = arguments.index("--select")
        # Delete the option flag
        del arguments[select_arg_index]
        # Delete its value
        del arguments[select_arg_index]

    command += arguments
    command += ["--select", "[%s]" % " ".join("random" for _ in row_ids)]

    return command
