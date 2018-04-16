import os

import numpy

from ..cluster import Cluster
from ..database import Database
from ..utils.process import pmap


class Cumulus(object):

    def __init__(self, clusters_config, lazy=True):
        self.clusters = [Cluster(lazy=lazy, **cluster_config)
                         for cluster_config in clusters_config]

    @property
    def cluster_names(self):
        return [c.name for c in self.clusters]

    @property
    def clusters_dict(self):
        return dict(zip(self.cluster_names, self.clusters))

    @property
    def clusters_sorted_by_priority(self):
        if any(cluster.priority is not None for cluster in self.clusters):
            upper_bound = max(cluster.priority for cluster in self.clusters
                              if cluster.priority is not None)
        else:
            upper_bound = 1

        for cluster in self.clusters:
            if cluster.priority is None:
                cluster.priority = numpy.random.uniform(upper_bound + 1)

        return sorted(self.clusters, key=lambda c: c.priority)

    def connect(self):
        rval = pmap(
            _connect, ((c.name, ) for c in self.clusters),
            initializer=_initialize_clusters,
            initargs=[self.clusters_dict])

        return rval

    def deploy(self, project):
        sorted_clusters = self.clusters_sorted_by_priority
        cluster_names = map(lambda c: getattr(c, "name"), sorted_clusters)

        deployed = {}
        queued_experiments = project.get(status=project.QUEUED)

        if all(len(e) == 0 for e in queued_experiments):
            return dict(zip(cluster_names, [{} for _ in cluster_names))

        free_slots = self.get_free_slots()

        ids_to_deploy = self.distribute_experiments(
            free_slots, queued_experiments)

        deployed = pmap(
            _deploy,
            ((cluster, ids_to_deploy[cluster.name], project)
             for cluster in sorted_clusters))
        cluster_names = map(lambda c: getattr(c, "name"), sorted_clusters)
        return dict(zip(cluster_names, deployed))

    def distribute_experiments(self, free_slots, experiments):
        nb_of_experiments = numpy.array(
            len(e) for e in experiments.itervalues())

        ids = {}
        for cluster in self.clusters_sorted_by_priority:
            pvals = (nb_of_experiments /
                     nb_of_experiments.sum().astype(float))

            samples = numpy.random.multinomial(free_slots, pvals)
            samples = numpy.min(samples, nb_of_experiments)
            nb_of_experiments -= samples

            ids_for_cluster = {}
            for number_of_jobs_to_launch, [experiment_name, experiment] in \
                    zip(samples, experiments):
                ids_for_cluster[experiment_name] = [
                    row[Database.ROW_ID] for row in
                    experiment[:number_of_jobs_to_launch]]

            ids[cluster.name] = ids_for_cluster

        return ids

    def get_queues(self):
        sorted_clusters = self.clusters_sorted_by_priority
        queues = pmap(
            _get_queues,
            ((c.detach_ssh_tunnel()[0], ) for c in sorted_clusters))
        cluster_names = map(lambda c: getattr(c, "name"), sorted_clusters)
        return dict(zip(cluster_names, queues))

    def cancel(self, job_ids):
        cancelled_jobs = pmap(
            _cancel_jobs, ((c.name, ) for c in self.clusters),
            initializer=_initialize_cancel,
            initargs=[self.clusters_dict, job_ids])
        return dict(zip(self.cluster_names, cancelled_jobs))

    def get_free_slots(self):
        free_slots = pmap(_get_free_slots, self.clusters)
        cluster_names = map(lambda c: getattr(c, "name"), self.clusters)
        return dict(zip(cluster_names, free_slots))

    def retrieve_logs(self, output_dir):
        rval = pmap(
            _retrieve_logs,
            ((c, os.path.join(output_dir, c.name)) for c in self.clusters))
        cluster_names = map(lambda c: getattr(c, "name"), self.clusters)
        return dict(zip(cluster_names, rval))


def _initialize_clusters(_clusters):
    global clusters
    clusters = _clusters


# Connect
##########

def _connect(cluster_name):
    return clusters[cluster_name].connect()


# Deploy
#########

def _deploy(cluster_name, experiment_ids):
    return clusters[cluster_name].deploy(experiment_ids)


# Queues
#########

def _get_queues(cluster):
    return cluster.queue(username=None, job_id=None)


# Cancel
#########

def _initialize_cancel(_clusters, _job_ids):
    _initialize_clusters(clusters)
    global job_ids
    job_ids = _job_ids


def _cancel_jobs(cluster_name):
    return clusters[cluster_name].cancel(job_ids)


# Free slots
#############

def _get_free_slots(cluster):
    return cluster.get_free_slots()


# Retrieve Logs
################

def _retrieve_logs(cluster, output_dir):
    return cluster.retrieve_logs(output_dir)
