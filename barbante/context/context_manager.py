""" Context Manager

Manages session contexts.

This module manages session contexts so that every Barbante module is able to fetch information from the session in a
safe way, i.e., respecting the fact that more than one request can be handled by a single thread and just using
threading.local() won't work as expected.

    from barbante.context.context_manager import new_session

    # Within some Tornado RequestHandler...
    def get(self, *args):
        with new_session(self.get_header('tracerid')):
            process_request()

    # Within some command line script...
    with new_session():
        do_some_task()

"""

import threading
import contextlib
import uuid
from functools import partial

from tornado.stack_context import run_with_stack_context, StackContext
from tornado.concurrent import wrap as tornado_wrap

from barbante.utils.logging import get_logger

log = get_logger(__name__)


class RequestContext:
    """ An object responsible for keeping data that must be globally accessible relative to a certain request.
    """

    def __init__(self, tracer_id: str='UNAVAILABLE', endpoint: str='UNAVAILABLE', environment: str='UNAVAILABLE'):
        self.tracer_id = tracer_id
        self.endpoint = endpoint
        self.environment = environment


class GlobalContextManager(threading.local):
    """ Keeps a stack context for each thread in Barbante.

    This is a singleton and shouldn't be instanced elsewhere but inside this module.
    """

    def __init__(self):
        super().__init__()
        self.stack = []

    def reset(self):
        self.stack.clear()

    def get_context(self) -> RequestContext:
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return RequestContext()

    @contextlib.contextmanager
    def push_context(self, context: RequestContext):
        """ Stacks a new context for this thread.
        """
        self.stack.append(context)
        yield
        self.stack.pop()

    @staticmethod
    def prepare_context(tracer_id: str, endpoint: str, environment: str):
        """
        :param tracer_id: request tracer id, if there is one
        :param endpoint: name of the endpoint that was requested
        :return: a new RequestContext object
        """
        tracer_id = GlobalContextManager.parse_guid(tracer_id)
        return RequestContext(tracer_id, endpoint, environment)

    @staticmethod
    def run_with_new_context(func, tracer_id: str=None, endpoint: str=None, environment: str=None):
        """ Stacks a new context in the thread's current stack and then run the method ``work``.

        Does the same as ``new_session`` but in an uglier fashion. Use ``new_session`` if possible.

        :param func: a function (usually a partial function) with the work to be done.
        :param tracer_id: an optional tracer id used when logging
        :param endpoint: name of the endpoint that was requested
        """
        context = GlobalContextManager.prepare_context(tracer_id, endpoint, environment)
        stack_context = StackContext(partial(global_context_manager.push_context, context))
        return run_with_stack_context(stack_context, func)

    @staticmethod
    def generate_guid():
        return uuid.uuid4()

    @staticmethod
    def parse_guid(guid: str, create_if_invalid: bool=True) -> str:
        """ Tries to parse a given string containing a GUID.

        If the GUID is not valid or the string is None, generates a new GUID and returns it.

        :param guid: string with some GUID to be parsed
        :param create_if_invalid: if True and `guid_str` is None or invalid, the method generates and returns a new UUID
        :return: the parsed GUID, or a new GUID if the one given is invalid. None if `guid_str` is invalid and
        `create_if_invalid` is False.
        """
        if guid is None:
            # No tracer id was passed. Generate one now.
            tid = GlobalContextManager.generate_guid() if create_if_invalid else None
        else:
            try:
                tid = uuid.UUID(guid)
            except ValueError:
                # An invalid UUID was passed. Ignore it and generate a new one.
                log.warn('An invalid UUID was given: "{}"'.format(guid))
                tid = GlobalContextManager.generate_guid() if create_if_invalid else None
        return str(tid).replace('-', '')  # our convention uses UUID without hyphens

global_context_manager = GlobalContextManager()
""" The global context stack.

Every thread will have a unique stack. Each entry in the stack is an object containing data relevant to a certain
request being handled.

Tornado is responsible for maintaining the stack and automatically switches context every time a new asynchronous
operation starts executing or is preempted in behalf of another operation.
"""


@contextlib.contextmanager
def new_context(tracer_id: str=None, endpoint: str=None, environment: str=None):
    """ Opens a new context.

    :param tracer_id: string with tracer log information. If not given, one will be generated.
    :param endpoint: name of the endpoint that was requested
    :param environment: the customer identifier

    Usage:

        with new_context(my_tracer_id):
            do_some_task_using_the_new_context()
    """
    context = GlobalContextManager.prepare_context(tracer_id, endpoint, environment)
    stack_context = StackContext(partial(global_context_manager.push_context, context))
    with stack_context:
        yield


def get_context() -> RequestContext:
    """ See GlobalContextManager.get_context()
    """
    return global_context_manager.get_context()


def wrap(func):
    """ See tornado.concurrent.wrap
    """
    return tornado_wrap(func)
