from base import AbstractStatus, AbstractDatabase

from ..utils.json import flatten, expand


class Status(AbstractStatus):
    QUEUED = ["QUEUED", "INTERRUPTED"]
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"


status = Status()


class Database(AbstractDatabase):
    CLUSTER = "info.cluster"
    NODE = "info.node"
    JOB_ID = "info.job_id"
    JOBARRAY_ID = "info.jobarray_id"
    REPOSITORIES = "experiment.repositories"
    STATUS = "status"

    def __init__(self):
        pass

    @property
    def projection(self):
        return {getattr(self, field): 1 for field in self._fields}

    def _find(self, query):
        rows = self.table.find(query, self.projection)
        projected_rows = []
        for row in rows:
            flattened_row = flatten(row)
            projected_row = expand({
                getattr(AbstractDatabase, field): (
                    flattened_row[getattr(self, field)])
                for field in self._fields})
            projected_rows.append(projected_row)

        return projected_rows

    def get_query(self, job_ids=None, row_ids=None, query=None):
        if (all(a is None for a in [job_ids, row_ids]) or
                all(a is not None for a in [job_ids, row_ids])):
            raise ValueError("Either job_ids or row_ids should be given")

        if job_ids is not None:
            ids = job_ids
            key = self.JOB_ID
        else:
            ids = row_ids
            key = self.ROW_ID

        if isinstance(ids, (list, tuple)):
            compare_to = {"$in": ids}
        else:
            compare_to = ids

        if query is None:
            query = {}

        query[key] = compare_to

        return query

    def get(self, job_ids=None, row_ids=None, query=None):
        query = self.get_query(job_ids, row_ids, query)
        projection = self.projection
        row = self._find(query, projection)
        return row

    def set(self, job_ids=None, row_ids=None, query=None, **kwargs):
        self.validate_keys(kwargs.iterkeys())
        self.validate_values(kwargs.itervalues())

        query = self.get_query(job_ids, row_ids, query)
        updates = {"$set": {getattr(self, k): v}
                   for k, v in kwargs.iteritems()}
        return self.table.update_many(query, updates).modified_count
