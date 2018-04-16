import argparse
import json
import logging
import sys
import time

import numpy

from ..cluster.cumulus import Cumulus
from ..database import status
from ..database.project import Project
from ..scheduler.base import AbstractScheduler
from ..utils.process import pmap


logger = logging.getLogger(__name__)


# commands :
#    1. setup
#    2. distribute
#    3. squeue
#    4. scancel

SETUP = "setup"
DEPLOY = "deploy"
MONITOR = "monitor"
CANCEL = "cancel"


def report(some_dicts, summarize=False):
    print some_dicts


def report_queue(some_dicts, summarize=False):
    keys = ["id", "exec_host", "Job_Name"]
    table = []
    for cluster, queue in some_dicts.iteritems():
        print "Cluster:", cluster
        table.append(keys + AbstractScheduler._status)
        print " ".join(keys + AbstractScheduler._status)
        for job_id, info in queue.iteritems():
            table.append([info.get(key, "") for key in keys])
            table[-1] += [str(info["job_array"].get(s, 0))
                          for s in AbstractScheduler._status]
            print " ".join(info.get(key, "") for key in keys), " ",
            print " ".join(str(info["job_array"].get(s, 0))
                           for s in AbstractScheduler._status)

    row_format = "{:>15}" * (len(table[0]))
    for row in table:
        print row_format.format(*row)


def setup(clusters_config, experiments_config, update):
    for cluster_config in clusters_config:
        # scp cluster_config['install_script']
        # ssh cluster_config['url']
        # ./install.sh --update
        # rm install.sh
        for experiment_config in experiments_config:
            # scp experiment_config["install_script"]
            # ./install.sh
            # rm install.sh
            pass


def sort_by_priority(dictionnaries, priority_key="priority"):
    for d in dictionnaries:
        if priority_key not in d:
            d[priority_key] = numpy.random.uniform()

    return sorted(dictionnaries, key=priority_key)


def deploy(clusters_config, experiments_config, retrieve_logs):
    cumulus = Cumulus(clusters_config)
    queues = cumulus.get_queues()
    # for cluster_config in sort_by_priority(clusters_config):
    print queues

    project = Project(experiments_config)
    queued_experiments = project.get(status=status.QUEUED)
    deployed = cumulus.deploy(queued_experiments)
    report(deployed)
    if retrieve_logs:
        cumulus.retrieve_logs(retrieve_logs)


def monitor(clusters_config, experiments_config, summarize):
    cumulus = Cumulus(clusters_config)
    queues = cumulus.get_queues()
    # for cluster_config in sort_by_priority(clusters_config):
    report_queue(queues, summarize)

    if experiments_config is not None:
        jobs = Project(experiments_config).get()
        report(jobs, summarize=True)


def _initialize_cancel_db_jobs(_found_job_ids):
    global found_job_ids
    found_job_ids = _found_job_ids


def _cancel_db_jobs(experiment_config):
    dataset = "for reference"  # Database()
    non_failed_jobs = [
        row for row in dataset.get(job_ids=found_job_ids)
        if row[dataset.STATUS] != status.FAILED]
    if non_failed_jobs:
        # TODO
        raise RuntimeError("Something went wrong... detail more")

    return dataset.set(job_ids=found_job_ids, status=dataset.CANCELLED)


def cancel(clusters_config, experiments_config, job_id):
    cumulus = Cumulus(clusters_config)
    cancelled = cumulus.cancel(job_id)

    report(cancelled)

    found_job_ids = sum((job_ids for job_ids in cancelled.itervalues()), [])
    logger.info("%d jobs cancelled" % len(found_job_ids))

    if experiments_config is not None:
        project = Project(experiments_config)

        WAIT_STEP = 5
        MAX_WAIT = 60
        waited = 0
        while waited <= MAX_WAIT:
            non_failed_jobs = [
                row[Project.JOB_ID]
                for row in project.get(job_ids=found_job_ids)
                if row[Project.STATUS] != status.FAILED]

            if len(non_failed_jobs) > 1:
                break

            time.sleep(5)
            waited += WAIT_STEP

        if non_failed_jobs:
            logger.warning("%d jobs status were not updated properly.")
            logger.debug("Jobs not updated are: %s" % str(non_failed_jobs))

        job_ids_to_update = list(set(found_job_ids) - set(non_failed_jobs))
        results = project.set(job_ids=job_ids_to_update,
                              status=status.CANCELLED)
        logger.info("%d jobs status updated" % sum(results))

    # found_job_ids = pmap(
    #     _cancel_jobs, clusters_config, _initialize_cancel_jobs,
    #     initargs=(job_id, ))
    # found_job_ids = sum(found_job_ids, [])
    # logger.info("%d jobs cancelled" % len(found_job_ids))

    results = pmap(
        _cancel_db_jobs, experiments_config,
        initializer=_initialize_cancel_db_jobs, initargs=(found_job_ids, ))
    logger.info("%d jobs status updated" % sum(results))


def get_options(argv):

    parser = argparse.ArgumentParser(description="WRITEME")

    parser.add_argument(
        "--clusters-config", help="Config file")

    parser.add_argument(
        "--cluster", help="Only apply command to specified cluster")

    parser.add_argument(
        "--experiments-config", help="Config file")

    parser.add_argument(
        "--experiment", help="Only apply to the specified experiment")

    parser.add_argument(
        '-v', '--verbose', action='count', default=0,
        help="Print informations about the process.\n"
             "     -v: INFO\n"
             "     -vv: DEBUG")

    subparsers = parser.add_subparsers(dest="command")

    setup_subparser = subparsers.add_parser(
        SETUP, help="Install base stack on clusters")

    setup_subparser.add_argument(
        "--update", action="store_true",
        help="Update stack if already installed")

    deploy_subparser = subparsers.add_parser(
        DEPLOY, help="Query clusters and submits jobs")

    deploy_subparser.add_argument(
        "--retrieve-logs", action="store_true",
        help="Retrieve currents logs of the currents while deploying")

    monitor_subparser = subparsers.add_parser(
        MONITOR, help="Query clusters and print out queues")

    monitor_subparser.add_argument(
        "--summarize", action="store_true",
        help="Summarize the queues and experiment status")

    cancel_subparser = subparsers.add_parser(
        CANCEL, help="Cancel jobs on clusters")

    cancel_subparser.add_argument(
        "job_id", metavar="job-id",
        help="Job id to cancel. Can specify many using regex.")

    return parser.parse_args(argv)


def load_configs(config_path, name):
    if config_path is None:
        return config_path

    with open(config_path, 'r') as f:
        configs = json.load(f)

    if name is not None:
        return [
            config for config in configs
            if config['name'] == name]

    return configs


def main(argv=None):
    options = get_options(argv)
    print options

    options.clusters_config = load_configs(
        options.clusters_config, options.cluster)
    options.experiments_config = load_configs(
        options.experiments_config, options.experiment)

    if options.verbose == 0:
        logging.basicConfig(level=logging.WARNING)
    elif options.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    elif options.verbose == 2:
        logging.basicConfig(level=logging.DEBUG)

    if options.command == SETUP:
        setup(options.clusters_config, options.experiments_config,
              options.update)
    elif options.command == DEPLOY:
        deploy(options.clusters_config, options.experiments_config,
               options.retrieve_logs)
    elif options.command == MONITOR:
        monitor(options.clusters_config, options.experiments_config,
                options.summarize)
    elif options.command == CANCEL:
        cancel(options.clusters_config, options.job_id)


if __name__ == "__main__":
    main(sys.argv[1:])
