import logging

from cumulus import config
from cumulus.utils.scripts import command_is_available


logger = logging.getLogger(__name__)


if command_is_available("msub") or command_is_available("qsub"):
    logger.info("Using Torque with (Moab|Maui) scheduler")
    from .torque import *
elif command_is_available("sbatch"):
    logger.info("Using slurm scheduler")
    from .slurm import *
elif config["if_no_scheduler"] == "warn":
    logger.warning("Cannot find a supported scheduler. You can silence this "
                   "warning by setting if_no_scheduler=ignore in .cumulusrc "
                   "configuration file.")
