import yaml

import barbante.utils as utils


def load(filename):
    """ Loads the specified configuration file.

        :param filename: the configuration file to be loaded
        :returns: a map with the configuration from filename
    """
    with utils.resource_stream(filename, __name__) as config_file:
        return yaml.load(config_file)


def load_barbante_config(name):
    return load('barbante_{0}.yml'.format(name))


def load_customer_config(env):
    customer = database[env]['customer']
    return load_barbante_config(customer)


def load_environment_config(env):
    return database[env]


def load_customers(config):
    """ Load all customer configuration files based on the provided environment configurations.

        :param config: the configuration file that contain the customers list
        :returns: a dictionary in the form customer: {customer configuration dictionary}
    """
    all_customers = set()
    all_configs = {}
    for env, definitions in config.items():
        all_customers.add(definitions['customer'])
    for customer in all_customers:
        all_configs[customer] = load_barbante_config(customer)
    return all_configs


def is_anonymous(user_id):
    """ Returns whether or not the given user is an anonymous user.

        :param user_id: The id of the user.
        :return: True, if the user is anonymous; False, otherwise.
    """
    return user_id.startswith("hmrtmp")


def is_valid_customer_identifier(customer_id):
    """
        :param customer_id: the environment, i.e., the customer identifier
        :return: True if the customer exists
    """
    return customer_id in database.keys()


database = load('mongoid.yml')
""" Pre-load mongoid
"""
customers = load_customers(database)
""" Pre-load customer configurations
"""
