from abc import ABCMeta, abstractmethod
import copy
import os
import re


class AbstractScheduler(object):
    __meta__ = ABCMeta

    JOB_ID = "PBS_ARRAYID"
    JOBARRAY_ID = "PBS_JOBID"

    FAILED = "FAILED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    QUEUED = "QUEUED"
    HOLD = "HOLD"

    _status = ["FAILED", "RUNNING", "COMPLETED", "QUEUED", "HOLD"]

    @abstractmethod
    def submit(self):
        pass

    @abstractmethod
    def queue(self):
        pass

    @abstractmethod
    def cancel(self):
        pass

    def _get_cancellable_jobs(self, job_id, job_array_id=None):
        username = os.environ["USER"]

        if job_array_id is not None:
            raise NotImplementedError("Does not support cancelling jobarray "
                                      "yet")

        if job_id.replace(".", "").isalnum():
            jobs = self.queue(username=username, job_id=job_id)
        elif job_id.strip() == "*":
            jobs = self.queue(username=username)
        else:
            regex = re.compile(job_id)
            jobs = self.queue(username=username)
            job = [
                job for job in jobs if regex.match(job["job_id"])]

        cancellable_status = set(self.RUNNING, self.QUEUED, self.HOLD)
        job_ids = []
        for job in jobs:
            if any(status in cancellable_status
                   for status in job['status'].keys()):
                job_ids.append(job['job_id'])

        return job_ids

    def get_status_key(self, status):
        status_key = [status_key for status_key in self._status
                      if getattr(self, status_key) == status]

        # TODO Maybe support many to one mapping, meaning we could return a
        # list of status keys
        assert len(status_key) == 1

        return status_key[0]

    def _standardize_status(self, jobs, inplace=True):

        if not inplace:
            jobs = copy.deepcopy(jobs)

        for job_id, informations in jobs.iteritems():
            for job_array in informations["job_array"]:
                for custom_status in list(job_array.keys()):
                    status_key = self.get_status_key(custom_status)
                    standard_status = getattr(AbstractScheduler, status_key)
                    if standard_status in job_array:
                        job_array[standard_status] += job_array[custom_status]
                    del job_array[custom_status]

        return jobs
