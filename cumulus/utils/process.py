import logging
import multiprocessing


logger = logging.getLogger(__name__)


def pmap(fct, args, n_pools=None, initializer=None, initargs=None,
         _async=True):

    if logger.getEffectiveLevel() == logging.DEBUG:
        if _async:
            logging.warning("Forcing pmap to be serial because logging"
                            "level is DEBUG")
        _async = False

    results = []
    if _async:
        pool = multiprocessing.Pool(n_pools, initializer=initializer,
                                    initargs=initargs)

        for process_args in args:
            results.append(pool.apply_async(fct, args=process_args))

        # Get fct output and replace pool.result object
        for i in range(len(results)):
            results[i] = results[i].get()
    else:
        if initializer is not None:
            initializer(initargs)

        for process_args in args:
            results.append(fct(*process_args))

    return results
