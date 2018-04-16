from collections import defaultdict
import subprocess
import sys

from research import config

from research.framework.utils.num import is_int


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


def squeue(username=None, job_id=None, format_option=None):

    if format_option is None:
        format_option = config["cluster"]["squeue"]["format_option"]

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

