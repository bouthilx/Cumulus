import logging

logger = logging.getLogger(__name__)


def build_command(queue, job_name, time, ids, arguments):
    command = ["smart-dispatch", "-q", queue, "-C", "1", "-G", "1",
               "-t", time, "launch"]

    arguments = arguments.split()
    if "--select" in arguments:
        logger.warning("Options --select should not be given in arguments. "
                       "It is built using given ids. "
                       "Dropping it from arguments...")
        select_arg_index = arguments.index("--select")
        # Delete the option flag
        del arguments[select_arg_index]
        # Delete its value
        del arguments[select_arg_index]

    command += arguments
    command += ["--select", "[%s]" % " ".join("random" for _ in ids)]

    return command
