from research.framework.utils.scripts import command_is_available

from .qstat import qstat
from .squeue import squeue


def jobs_info(username=None, job_id=None):
    if command_is_available("qstat"):
        return qstat(username, job_id)
    elif command_is_available("squeue"):
        return squeue(username, job_id)
    else:
        raise RuntimeError("Cannot find a job queue command")
