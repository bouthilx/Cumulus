from collections import OrderedDict, defaultdict
import datetime
import os
import subprocess
import sys
import tempfile

from cumulus.scheduler.base import AbstractScheduler
from cumulus.scheduler import smartdispatch
from cumulus.utils.script import command_is_available
from cumulus.utils.num import is_int


SBATCH_SCRIPT = """
#!/bin/bash

%(options)s

######################
# Begin work section #
######################

# Print this sub-job's task ID
# Do some work based on the SLURM_ARRAY_TASK_ID

%(script)s

echo "Task ${SLURM_ARRAY_TASK_ID} completed"
"""


class Scheduler(AbstractScheduler):
    JOB_ID = "SLURM_JOBID"
    JOBARRAY_ID = "SLURM_ARRAY_TASK_ID"

    FAILED = "F"
    RUNNING = "R"
    COMPLETED = "CD"
    QUEUED = "PD"
    HOLD = "UNKNOWN"

    def __init__(self):
        pass

    def submit(self, cluster, job_name, job_ids, time, command):
        if command_is_available("smart-dispatch"):
            smartdispatch.submit(cluster.queue, job_name, time)
        pass

    def queue(self):
        pass

    def cancel(self):
        pass


def build_command(job_name, time, ids, script, arguments):

    # TODO Stop building a new directory, just make a temp script file
    #      We alredy build a tmp directory with cumulus-particle run
    working_dir = tempfile.mkdtemp(
        prefix=job_name + "_",
        suffix=datetime.datetime.now().strftime("_%Y-%m-%d_%H:%M:%S"))
    working_dir = os.realpath(working_dir)

    options = OrderedDict()
    options["--time"] = time
    options["--array"] = "0-%d" % (len(ids) - 1)
    options["--work-dir"] = working_dir
    if job_name is not None:
        options["--job-name"] = job_name
        options["--output"] = job_name + "_%A_%a.out"

    script_path = os.path.join(working_dir, "script.sh")
    with open(script_path, 'w') as f:
        f.write(SBATCH_SCRIPT % dict(
            options="\n".join("%s = %s" % (name.strip("--"), value)
                              for name, value in options.iteritems()),
            script=arguments))

    command = ["sbatch", script_path]

    return command


_slurm_state_to_moab = {
    "F": "C",
    "R": "R",
    "CD": "C",
    "PD": "Q"}


def convert_to_moab(state, scheduler="slurm"):
    if state not in _slurm_state_to_moab[state]:
        raise ValueError("Convertion unknown for scheduler %s with state %s" %
                         (state, scheduler))

    return _slurm_state_to_moab[state]


DEFAULT_FORMAT_OPTION = "arrayjobid,arraytaskid,statecompact"


def squeue(username=None, job_id=None, format_option=DEFAULT_FORMAT_OPTION):

    if username is None:
        command = "squeue --Format=%s" % format_option
    else:
        command = "qstat -u %s --Format=%s" % (username, format_option)

    process = subprocess.Popen([command],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)

    if process.returncode is not None and process.returncode < 0:
        sys.stderr.write(process.stderr.read())
        sys.exit(1)

    return parse_squeue(process.stdout.read())


def parse_squeue(row_squeue):
    rows = filter(lambda a: a.strip(), row_squeue.split("\n"))

    header = rows.pop(0)

    if "ARRAY_JOB_ID" not in header:
        raise ValueError("format_option should include arrayjobid")
    if "ARRAY_TASK_ID" not in header:
        raise ValueError("format_option should include arraytaskid")
    if "ST" not in header:
        raise ValueError("format_option should include statecompact")

    job_id_index = header.index("ARRAY_JOB_ID")
    job_array_id_index = header.index("ARRAY_TASK_ID")
    state_index = header.index("ST")

    jobs = {}
    for row in map(str.split, rows):
        if not is_int(row[job_id_index]):
            continue

        job_id = row[job_id_index]
        job_array_id_index = row[job_array_id_index]
        state = row[state_index]
        if job_id not in jobs:
            jobs[job_id] = dict(job_array=defaultdict(int))

        for name, value in zip(header, row):
            jobs[job_id][name] = value

        jobs[job_id]["job_array"][state] += 1

    return jobs
