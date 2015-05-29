import inspect
import os
import os.path
import logging as system_logging
import logging.config as logging_config
import socket
import sys
from collections import defaultdict
from copy import deepcopy

import barbante
import barbante.config as config


# Reserved messages for performance monitoring
PERF_BEGIN = "B"
PERF_END = "E"

TENANT_ID_SEPARATOR = '#'


class JsonFormatter(system_logging.Formatter):
    """ Prints a JSON representation suitable for automated processing of log files. """

    _hostname = socket.gethostname()

    def format(self, rec):
        # Imported from here to avoid cyclic dependency problem
        from barbante.context.context_manager import global_context_manager as gcm

        tracer = gcm.get_context().tracer_id
        endpoint = gcm.get_context().endpoint

        json = '{"class_name": "%s", "method_name": "%s", ' % (rec.module, rec.funcName)

        if rec.msg in [PERF_BEGIN, PERF_END]:
            json += '"stage": "%s", ' % rec.msg
        else:
            json += '"message": "%s", ' % rec.msg

        json += '"timestamp": %.3f, "server": "%s", ' % (rec.created, JsonFormatter._hostname)
        json += '"tracerid": "%s", "endpoint": "%s", "module_name": "%s"}' % (tracer, endpoint, barbante.__app_name__)

        rec.json = json
        out = super().format(rec)
        return out


class BarbanteLogger(system_logging.getLoggerClass()):
    def __init__(self, name):
        super().__init__(name)
        self.tenants = None
        self.default_handlers = None

    def prepare_tenant_handlers(self):
        self.tenants = defaultdict(list)
        self.default_handlers = []

        c = self
        while c:
            for hdlr in c.handlers:
                hdlr_key = hdlr.get_name()
                if TENANT_ID_SEPARATOR in hdlr_key:
                    tenant = hdlr_key.split(TENANT_ID_SEPARATOR)[1]
                    self.tenants[tenant].append(hdlr)
                else:
                    self.default_handlers.append(hdlr)
            if not c.propagate:
                c = None
            else:
                c = c.parent

    def handle(self, record):
        """ Overrides base method to demultiplex log records based on tenant context.
        """
        # Imported from here to avoid cyclic dependency problem
        from barbante.context.context_manager import global_context_manager as gcm
        customer = gcm.get_context().environment

        if not self.tenants:
            self.prepare_tenant_handlers()

        if (not self.disabled) and self.filter(record):
            if customer in self.tenants.keys():
                handlers = self.tenants[customer]
            else:
                handlers = self.default_handlers

            for hdlr in handlers:
                if record.levelno >= hdlr.level:
                    hdlr.handle(record)

    def debug_progress(self, done, total, step):
        """ Logs a debug-level msg stating the percentage already done
            of a task, given the number of subtasks processed over the
            informed total.

            Parameters:
                done: The absolute number of subtasks already processed.
                total: The total number of subtasks, corresponding to 100% done.
                step: The absolute number of subtasks that must be processed between
                    two consecutive outputs of this function.
        """
        if (done % step == 0) or (done == total):
            self.debug("%.5f%% done" % (100.0 * done / total))

    def is_debug_enabled(self):
        return self.isEnabledFor(system_logging.DEBUG)

    @staticmethod
    def _get_method(frame):
        return frame.f_code.co_name  # See http://stackoverflow.com/a/1140513/778272

    @staticmethod
    def _get_module(frame):
        # For a discussion about obtaining module name, see http://stackoverflow.com/a/2011168/778272
        try:
            module_name = inspect.getmodule(frame).__spec__.name
        except AttributeError:
            module_name = os.path.basename(sys.argv[0])  # case when barbante is called from the command line
        return module_name

system_logging.JsonFormatter = JsonFormatter
system_logging.setLoggerClass(BarbanteLogger)


def get_logger(name):
    return system_logging.getLogger(name)


def apply_suffix_to_log_filenames(log_config, filename_modifier):
    handlers = log_config['handlers']
    for handler_key, handler_def in handlers.items():
        for key, value in handler_def.items():
            if key == 'filename':
                filename, extension = os.path.splitext(value)
                handler_def[key] = "{0}-{1}{2}".format(filename, filename_modifier, extension)
    return log_config


def add_multi_tenant_log_handlers(log_config):
    """ List all handlers that log to a file and clone those handlers once for each tenant.
    """
    handlers_to_be_cloned = {}

    # List all handlers that log to a file:
    handlers = log_config['handlers']
    for handler_key, handler_def in handlers.items():
        if 'filename' in handler_def:
            handlers_to_be_cloned[handler_key] = handler_def

    # Clone those handlers once for each tenant in the config file:
    for handler_key, handler_def in handlers_to_be_cloned.items():
        for tenant in config.database:
            tenant_handler_key = '{}{}{}'.format(handler_key, TENANT_ID_SEPARATOR, tenant)
            handlers[tenant_handler_key] = deepcopy(handler_def)
            filename, extension = os.path.splitext(handlers[tenant_handler_key]['filename'])
            handlers[tenant_handler_key]['filename'] = '{}-{}{}'.format(filename, tenant, extension)

            # Add cloned handlers to every logger that uses the original handler:
            for logger_key, logger_def in log_config['loggers'].items():
                if 'handlers' in logger_def and handler_key in logger_def['handlers']:
                    logger_def['handlers'].append(tenant_handler_key)

    log_config['handlers'] = handlers

    return log_config


def setup_logging(default_path='logging.yml',
                  default_level=system_logging.INFO,
                  env_key='LOG_CFG',
                  filename_modifier=None):
    log_path = default_path
    value = os.getenv(env_key, None)
    if value:
        log_path = value

    log_config = config.load(log_path)

    log_config = add_multi_tenant_log_handlers(log_config)

    # Applies a suffix to the filename (e.g., the port where the server is running)
    if filename_modifier:
        log_config = apply_suffix_to_log_filenames(log_config, filename_modifier)

    if log_config:
        logging_config.dictConfig(log_config)
    else:
        system_logging.basicConfig(level=default_level)


def caller_module():
    frame_records = inspect.stack(1)
    return inspect.getmodulename(frame_records[2][1])


setup_logging()
