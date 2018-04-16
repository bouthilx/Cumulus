from ..utils.process import pmap

from . import Database


class Project(Database):

    def __init__(self, experiments_config):
        self.databases = [Database(experiment_config)
                          for experiment_config in experiments_config]

    def get(self, job_ids=None, **kwargs):
        experiments = pmap(
            _get, (database for database in self.databases),
            initializer=_initialize_get, initargs=(job_ids, kwargs))

        experiment_names = map(lambda d: getattr(d, "name"), self.databases)
        return dict(zip(experiment_names, experiments))

    def set(self, job_ids=None, **kwargs):
        experiments = pmap(
            _set, (database for database in self.databases),
            initializer=_initialize_set, initargs=(job_ids, kwargs))

        experiment_names = map(lambda d: getattr(d, "name"), self.databases)
        return dict(zip(experiment_names, experiments))


# Get
################

def _initialize_get(_job_ids, _kwargs):
    # TODO Test if we do 2 pmap simultaneously if it breaks global
    # found_job_ids values across both pmaps
    global get_job_ids, get_kwargs
    get_job_ids = _job_ids
    get_kwargs = _kwargs


def _get(dataset):
    dataset.get(get_job_ids, **get_kwargs)


# Set
################

def _initialize_set(_job_ids, _kwargs):
    # TODO Test if we do 2 pmap simultaneously if it breaks global
    # found_job_ids values across both pmaps
    global set_job_ids, set_kwargs
    set_job_ids = _job_ids
    set_kwargs = _kwargs


def _set(dataset):
    dataset.set(set_job_ids, **set_kwargs)

    # non_failed_jobs = [
    #     row for row in dataset.set(job_ids=set_job_ids)
    #     if row[dataset.STATUS] != status.FAILED]
    # if non_failed_jobs:
    #     # TODO
    #     raise RuntimeError("Something went wrong... detail more")

    # return dataset.set(job_ids=set_job_ids, status=dataset.CANCELLED)
