import time
from queue import Queue
import contextlib
import logging as system_logging
import sys

from barbante.utils.logging import get_logger


def profile(method):
    """ A decorator to log method execution times.

    from profiling import profile

    @profile
    def foo():
        # ...

    """
    def profiled(*args, **kwargs):
        logger = get_logger(method.__module__)
        if logger.isEnabledFor(system_logging.DEBUG):
            msg = "{0}...".format(method.__name__)
            logger.debug(msg)
            temp_start_time = time.time()
            result = method(*args, **kwargs)
            end_time = time.time()
            msg = "{0} (duration = {1:2.6f}s)".format(method.__name__,
                                                      end_time - temp_start_time)
            logger.debug(msg)
        else:
            result = method(*args, **kwargs)
        return result

    return profiled


class Reporter():
    """

    A simple console profiler to help with tests.

    Usage:

        from profiling import Reporter as r
        with r.profile('Terraforming'):
            terraform(planet)

        # ... logs more stuff ...

        # dumps a report to stdout
        r.dump()

    Report format:

        <timestamp> <delta> | <message>

        * timestamp - Elapsed time in miliseconds since first log message
        * delta - Elapsed time in miliseconds since some timestamp passed as a parameter
        * message - the log message

    """

    _mq = Queue()
    _start_time = 0

    _THRES_SLOW = 50  # miliseconds
    """ Benchmarks greater than this measure will appear red """
    _THRES_WARN = 10  # miliseconds
    """ Benchmarks greater than this measure will appear yellow """

    @staticmethod
    @contextlib.contextmanager
    def profile(message):
        global _start_time

        now = time.time() * 1000
        if Reporter._mq.empty():
            _start_time = now
        from_start_1 = now - _start_time
        from_previous = None
        msg_tuple = (from_start_1, from_previous, 'begin: ' + message)
        Reporter._mq.put(msg_tuple)

        yield

        now = time.time() * 1000
        from_start_2 = now - _start_time
        from_previous = from_start_2 - from_start_1
        msg_tuple = (from_start_2, from_previous, 'end:   ' + message)
        Reporter._mq.put(msg_tuple)

    @staticmethod
    def dump(file=sys.stdout):
        """ Dumps a complete report to file.

        :param file: a file descriptor, stdout by default
        """
        print(file=file)
        while not Reporter._mq.empty():
            ms, delta, message = Reporter._mq.get()
            no_color = '\033[0m'
            delta_color = '\033[92m'
            if delta is not None:
                if delta > Reporter._THRES_SLOW:
                    delta_color = '\033[91m'
                elif delta > Reporter._THRES_WARN:
                    delta_color = '\033[93m'
                delta_fmt = '{:+.0f}ms'.format(round(delta))
            else:
                delta_fmt = ''
            print('{:7.1f} {}{:10}{} | {}'.format(ms, delta_color, delta_fmt, no_color, message), file=file)
