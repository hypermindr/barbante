import sys
import os
import contextlib
import inspect
import logging as system_logging
import functools

from barbante.utils.logging import get_logger
from barbante.context.context_manager import global_context_manager as gcm

_TRACE_BEGIN = 'B'
_TRACE_END = 'E'
tracer_logger = get_logger('barbante.utils.logging.trace')


def _log(msg, tracer_id, endpoint, method_name, class_name, stage):
    tracer_logger.info(msg if msg is not None else '', extra={'stage': stage, 'tracerid': tracer_id,
                       'endpoint': endpoint, 'method_name': method_name, 'class_name': class_name})


def _doublewrap(f):
    """
    A decorator decorator, allowing the decorator to be used as:
    @decorator(with, arguments, and=kwargs)
    or
    @decorator

    See http://stackoverflow.com/a/14412901/778272
    """
    @functools.wraps(f)
    def new_dec(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            # actual decorated function
            return f(args[0])
        else:
            # decorator arguments
            return lambda realf: f(realf, *args, **kwargs)
    return new_dec


@_doublewrap
def trace(func, msg=None):
    """ Traces the execution of a method call.

    Example:
        @trace("Creating universe")
        def make_universe():
            universe = Universe(god=None)
            universe.bang()

    May be called without parenthesis too:

        @trace
        def foo():
            # ...

    :param msg: Message to log
    :return: A decorator function
    """
    @functools.wraps(func)
    def trace_wrapper(*args, **kwargs):
        context = gcm.get_context()
        if tracer_logger.isEnabledFor(system_logging.INFO):

            baseline = 1  # how many frames should we go down the stack to obtain info about the method that matters
            frame = inspect.stack()[baseline][0]

            try:
                # For a discussion about obtaining module name, see http://stackoverflow.com/a/2011168/778272
                module_name = inspect.getmodule(frame).__spec__.name
                # And for a discussion about method name, see http://stackoverflow.com/a/1140513/778272
            except AttributeError:
                module_name = "undefined"

            try:
                method_name = func.__qualname__
            except AttributeError:
                method_name = func.__name__

            _log(msg, context.tracer_id, context.endpoint, method_name, module_name, _TRACE_BEGIN)
            try:
                result = func(*args, **kwargs)
            finally:
                _log(msg, context.tracer_id, context.endpoint, method_name, module_name, _TRACE_END)
        else:
            result = func(*args, **kwargs)
        return result
    return trace_wrapper


@contextlib.contextmanager
def trace_block(msg=None):
    """ Traces the execution of a code block.

    Example:
        with trace_block("Creating universe"):
            universe = Universe(god=None)
            universe.bang()

    :param msg: Message to log
    :return: A decorator function
    """
    context = gcm.get_context()
    if tracer_logger.isEnabledFor(system_logging.INFO):

        baseline = 2  # how many frames should we go down the stack to obtain info about the method that matters
        frame = inspect.stack()[baseline][0]
        try:
            module_name = inspect.getmodule(frame).__spec__.name
        except AttributeError:
            module_name = os.path.basename(sys.argv[0])  # case when barbante is called from the command line
        method_name = frame.f_code.co_name  # See http://stackoverflow.com/a/1140513/778272

        # See http://stackoverflow.com/a/2544639/778272
        try:
            method_name = frame.f_locals['__class__'].__name__ + '.' + method_name
        except KeyError:
            pass

        _log(msg, context.tracer_id, context.endpoint, method_name, module_name, _TRACE_BEGIN)
        yield
        _log(msg, context.tracer_id, context.endpoint, method_name, module_name, _TRACE_END)
    else:
        yield
