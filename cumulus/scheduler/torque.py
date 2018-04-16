from collections import defaultdict
import re
import subprocess
import sys
import time

from cumulus.utils.scripts import command_is_available
from cumulus.utils.num import is_int

from .base import AbstractScheduler
from . import smartdispatch


class Scheduler(AbstractScheduler):
    JOB_ID = "PBS_ARRAYID"
    JOBARRAY_ID = "PBS_JOBID"

    FAILED = "E"
    RUNNING = "R"
    COMPLETED = "C"
    QUEUED = "Q"
    HOLD = "H"

    def __init__(self):
        pass

    def submit(self, queue, job_name, time, row_ids, arguments):
        if command_is_available("smart-dispatch"):
            return smartdispatch.submit(queue, job_name, time, row_ids,
                                        arguments)
        raise NotImplementedError("Torque qsub scheduler not implemented yet")

    def queue(self, username=None, job_id=None):
        return self._standardize_status(
            qstat(username=username, job_id=job_id))

    def cancel(self, job_id, job_array_id):
        cancellable_job_ids = self._get_cancellable_jobs(job_id)

        command = "qdel %s" % " ".join(cancellable_job_ids)

        process = subprocess.Popen(command)
        stdoutdata, stderrdata = process.communicate()
        if process.returncode > 0:
            print stdoutdata
            raise RuntimeError(stderrdata)

        # stdoutdata does not give information about cancelled jobs to we need
        # to monitor the queue and see which got cancelled succesfully
        cancelled_job_ids = []
        failed_cancel_job_ids = []
        job_ids_buffer = cancellable_job_ids
        WAIT_STEP = 5
        MAX_WAIT = 60
        waited = 0
        while waited <= MAX_WAIT:
            job_ids = self._get_cancellable_jobs(job_id)
            while job_ids_buffer:
                job_id = job_ids_buffer.pop(0)
                if job_id in job_ids:
                    failed_cancel_job_ids.append(job_id)
                else:
                    cancelled_job_ids.append(job_id)

            if len(failed_cancel_job_ids) == 0:
                break

            time.sleep(WAIT_STEP)
            waited += WAIT_STEP

        return cancelled_job_ids, failed_cancel_job_ids


def qstat(username=None, job_id=None):
    if username is None:
        command = "qstat -f"
    else:
        command = "qstat -f -u %s" % username

    process = subprocess.Popen([command],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)

    if process.returncode is not None and process.returncode < 0:
        sys.stderr.write(process.stderr.read())
        sys.exit(1)

    full_qstat = process.stdout.read()

    if username is None:
        command = "qstat -t"
    else:
        command = "qstat -t -u %s" % username

    process = subprocess.Popen([command],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)

    if process.returncode is not None and process.returncode < 0:
        sys.stderr.write(process.stderr.read())
        sys.exit(1)

    row_qstat = process.stdout.read()

    jobs = parse_qstat(row_qstat, full_qstat)

    if job_id is not None:
        return filter(lambda job: job["id"] == job_id, jobs)
    else:
        return jobs


id_regex = re.compile("^[0-9]*")


def parse_qstat(row_qstat, full_qstat):

    jobs = {}
    for job_desc in full_qstat.split("\n\n"):
        if job_desc.strip() == "":
            continue

        job = dict(id=job_desc.split("\n")[0].split(":")[-1].strip())

        last_key = None

        for line in job_desc.split("\n")[1:]:

            if line[0] == "\t":
                if last_key is None:
                    raise ValueError("qstat stdout is not formatted correctly")
                job[last_key] += line[1:]
            else:
                splits = line.split(" = ")
                key = splits[0].strip()
                value = " = ".join(splits[1:]).strip()
                job[key] = value

            last_key = key

        job["job_array"] = defaultdict(int)

        if job["id"] in jobs:
            return ValueError("Two jobs have the same ids: %s" % job["id"])

        jobs[job["id"]] = job

    # R: Running
    # Q: Queued
    # H: On hold
    job_id_index = 0
    state_index = None  # Need to find it
    for row in filter(lambda a: a.strip(), row_qstat.split("\n")):
        row = row.split()
        if is_int(row[0][0]):
            assert state_index is not None
            job_id = row[job_id_index]
            state = row[state_index]
            jobs[job_id]["job_array"][state] += 1
        elif row[4] == "S":
            state_index = 4
        elif (len(row) > 9 and row[9] == "S"):
            state_index = 9

    return jobs
