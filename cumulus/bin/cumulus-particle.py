import argparse
import logging
import sys

from cumulus.cluster import Scheduler


logger = logging.getLogger(__name__)


# commands :
#    1. setup
#    2. distribute
#    3. squeue
#    4. scancel

SETUP = "setup"
RUN = "run"
SUBMIT = "submit"
CANCEL = "cancel"
MONITOR = "monitor"


def report(some_dicts, summarize=False):
    print some_dicts


def setup(install_script_path, update):
    pass


def submit():
    pass


def monitor():
    pass


def cancel(job_id):
    pass


def get_options(argv):

    parser = argparse.ArgumentParser(description="WRITEME")

    subparsers = parser.add_subparsers(dest="command")

    setup_subparser = subparsers.add_parser(
        SETUP, help="Run install script on cluster")

    setup_subparser.add_argument(
        "script", help="Script to run to install")

    setup_subparser.add_argument(
        "--update", action="store_true",
        help="Update stack if already installed")

    submit_subparser = subparsers.add_parser(
        SUBMIT, help="Submits jobs with cluster's scheduler")

    monitor_subparser = subparsers.add_parser(
        MONITOR, help="Execute scheduler's queue command")

    cancel_subparser = subparsers.add_parser(
        CANCEL, help="Cancel jobs on cluster")

    cancel_subparser.add_argument(
        "job_id", metavar="job-id",
        help="Job id to cancel. Can specify many using regex.")

    return parser.parse_args(argv)


def main(argv=None):
    options = get_options(argv)

    if options.command == SETUP:
        setup(options.script, options.update)
    elif options.command == SUBMIT:
        submit(options.clusters_config, options.experiments_config,
               options.retrieve_logs)
    elif options.command == MONITOR:
        monitor(options.clusters_config, options.experiments_config,
                options.summarize)
    elif options.command == CANCEL:
        cancel(options.clusters_config, options.job_id)


if __name__ == "__main__":
    main(sys.argv[1:])
