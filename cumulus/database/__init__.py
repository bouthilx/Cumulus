import logging

logger = logging.getLogger("database")

_DATABASE_BACKEND = "sacred"

# Import backend functions.
if _DATABASE_BACKEND == 'cumulus':
    logger.info('Using cumulus database backend')
    from .cumulus import *
elif _DATABASE_BACKEND == 'sacred':
    logger.info('Using sacred database backend')
    from .sacred import *
elif _DATABASE_BACKEND == 'hyperopt':
    logger.info('Using hyperopt database backend')
    from .hyperopt import *
else:
    raise ValueError('Unknown database backend: ' + str(_DATABASE_BACKEND))
