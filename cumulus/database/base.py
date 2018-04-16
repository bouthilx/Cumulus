from abc import ABCMeta, abstractmethod, abstractproperty
import json


class AbstractStatus(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def QUEUED(self):
        pass

    @abstractproperty
    def RUNNING(self):
        pass

    @abstractproperty
    def COMPLETED(self):
        pass

    @abstractproperty
    def FAILED(self):
        pass

    @abstractproperty
    def INTERRUPTED(self):
        pass

    @abstractproperty
    def TIMED_OUT(self):
        pass

    @abstractproperty
    def CANCELLED(self):
        pass


class AbstractDatabase(object):
    __metaclass__ = ABCMeta

    CLUSTER = "cluster"
    NODE = "node"
    JOB_ID = "job_id"
    JOBARRAY_ID = "job_id"
    REPOSITORIES = "repositories"
    STATUS = "status"

    _fields = ["CLUSTER", "NODE", "JOB_ID", "JOBARRAY_ID", "REPOSITORIES",
               "STATUS"]

    def __init__(self):
        pass

    @abstractmethod
    def get(self, job_id):
        pass

    def validate_keys(self, keys):
        invalid_keys = tuple(key for key in keys
                             if hasattr(self, key, None) is not None)
        if invalid_keys:
            raise KeyError("Invalid keys for Database: %s" % str(invalid_keys))

    def validate_values(self, values):
        invalid_values = []
        for value in values:
            if isinstance(value, basestring):
                continue
            try:
                if json.dumps(value) == str(value):
                    continue
            except TypeError:
                pass

            invalid_values.append(value)

        if invalid_values:
            raise KeyError("Invalid values for Database: %s" %
                           str(invalid_values))

    @abstractmethod
    def set(self):
        pass

    # repos - Copy repositories
    # job_id - Set job_id
    # cluster - Set cluster
    # node - Set node
