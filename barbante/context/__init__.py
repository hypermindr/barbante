""" Entry point for the different contexts supported by the application: customer (general customer configurations),
    user (user specific data, always associated to a customer context) and session (request information, can be
    associated to a customer context for general environment batch calls or to a user context in the case of user
    requests)
"""
import copy

from barbante import config
from barbante.context.customer_context import CustomerContext
from barbante.context.session_context import SessionContext
from barbante.data.MongoDBProxy import MongoDBProxy
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


DEFAULT_DB_PROXY_CLASS = MongoDBProxy


_customer_contexts_by_env = {}
""" Dict of lazily loaded customer contexts.

    Each context is responsible for keeping its own data proxy with its own pool of connections to the database.

    In a production environment, a Barbante instance will hold one customer context for each customer served.
    That context will be used by all open SessionContext instances in all Tornado workers handling connections
    in Reel for that environment.

    A single customer context should be able to serve as many connections as there are tornado workers times the
    number of concurrent recommendation algorithms running on each hybrid recommender instance.
"""


""" Configuration Constants
"""
BEFORE_SCORING = 'BEFORE_SCORING'
AFTER_SCORING = 'AFTER_SCORING'


class _DatabaseConfig(object):
    """ Private module level class used to hold database configuration settings.
    """

    def __init__(self, host, host_raw, name, name_raw, pool_size, read_preference, replica_set, replica_set_raw):

        self.host = host
        """ The database hostname or ip addresses for recommendation-specific collections (Barbante)
        """
        self.host_raw = host_raw
        """ The database hostname or ip addresses for raw data collections (API)
        """
        self.name = name
        """ The database name for recommendation-specific collections (Barbante)
        """
        self.name_raw = name_raw
        """ The database name for raw data collections (API)
        """
        self.pool_size = pool_size
        """ The size of the database pool
        """
        self.read_preference = read_preference
        """ The read preference when working in a cluster, defaults to primary
        """
        self.replica_set = replica_set
        """ The name of the replica set.
            When connecting to a Mongo Replica Set Cluster, this is a required parameter.
        """
        self.replica_set_raw = replica_set_raw
        """ The name of the replica set for raw data connections.
            When connecting to a Mongo Replica Set Cluster, this is a required parameter.
        """


class _CacheConfig(object):
    """ Private module level class used to hold cache configuration settings.
    """

    def __init__(self, env, hosts):
        self.environment = env
        """ The environment name, used
        """
        self.hosts = hosts
        """ The cache hostnames (or ip addresses) and ports
        """


class ConfigException(Exception):
    """ Raised when exceptions are found in configuration files or a non-existing environment config is requested
    """
    pass


def create_customer_context(env, data_proxy=None):
    """ Creates a new CustomerContext instance referencing the envinroment ``env``.
        Should only be called directly when testing. For production code, see get_preloaded_customer_context().

        :param env: The environment name.
        :param data_proxy: A data proxy instance to reuse instead of creating a new one.

        :returns: A CostumerContext instance.
    """
    try:
        env_config = config.database[env]
        customer = env_config['customer']
        env_default_settings = env_config['sessions']['default']
        db_options = env_default_settings['options']

        hosts = env_default_settings['hosts']
        hosts_raw = env_default_settings['hosts_raw']
        database = env_default_settings['database']
        database_raw = env_default_settings['database_raw']
        pool_size = db_options.get('pool_size')
        read_preferences = db_options['read']
        replica_set = db_options.get('replica_set') if db_options else None
        replica_set_raw = db_options.get('replica_set_raw') if db_options else None
        cache_settings = env_default_settings.get('cache')
        cache_hosts = cache_settings.get('hosts') if cache_settings else None

        context = CustomerContext(customer, data_proxy if data_proxy else DEFAULT_DB_PROXY_CLASS,
                                  _DatabaseConfig(hosts, hosts_raw, database, database_raw,
                                                  pool_size, read_preferences, replica_set, replica_set_raw),
                                  _CacheConfig(env, cache_hosts) if cache_hosts else None)
        log.info('Environment "{0}" configurations loaded successfully'.format(env))

        return context

    except (AttributeError, KeyError) as err:
        log.error('Error loading environment "{0}" configurations: {1}'.format(env, err))


def clone_preloaded_customer_context(env, data_proxy=None, make_new_data_proxy=False):
    """ Clones an existing customer context.

        :param env: environment whose customer context will be cloned
        :param data_proxy: sets an optional custom data_proxy

        :returns: The CustomerContext clone.
    """
    original_context = get_preloaded_customer_context(env)
    original_proxy = original_context.data_proxy

    # Before copying, temporarily remove the data proxy so that deepcopy doesn't see it, otherwise it would throw
    original_context.data_proxy = None
    clone = copy.deepcopy(original_context)
    original_context.data_proxy = original_proxy

    if data_proxy is None:
        if make_new_data_proxy:
            data_proxy = DEFAULT_DB_PROXY_CLASS
        else:
            data_proxy = original_proxy

    clone.set_data_proxy(data_proxy)

    return clone


def get_preloaded_customer_context(env):
    """ Gets the singleton customer context corresponding to the environment ``env``.
        Creates the context if it doesn't exist yet.

        :param env: The environment name
        :returns: A CostumerContext instance
    """
    if env not in _customer_contexts_by_env:
        _customer_contexts_by_env[env] = create_customer_context(env)
    return _customer_contexts_by_env[env]


def init_session(environment=None, user_id=None, context_filter_string=None, customer_ctx=None, algorithm=None):
    """ Initializes a session.

        :param environment: Session environment. Mandatory if customer_ctx is None, otherwise optional.
        :param user_id: External user id. If None, UserContext won't be created.
        :param context_filter_string: A ContextFilter instance.
        :param customer_ctx: A CustomerContext instance. Mandatory if environment is None, otherwise optional.
            Used only in tests; should be None for production.

        :returns: A new SessionContext
    """
    if customer_ctx is None:
        # If customer context was not specified, use the env's corresponding singleton instance
        customer_ctx = get_preloaded_customer_context(environment)

    return SessionContext(customer_ctx, user_id=user_id,
                          context_filter_string=context_filter_string, algorithm=algorithm)
