""" Base class for maintenance tests.
"""

import barbante.maintenance.product as product_maintenance
import barbante.tests.dummy_data_populator as dp
import barbante.tests as tests


class MaintenanceFixture():
    """ Class for testing barbante.maintenance module.
    """
    session_context = None
    """ The test session context. """
    db_proxy = None
    """ A reference to the database proxy. """
    custom_settings = None
    """ A map with customer settings intended to override those in the config file. """

    def __init__(self, custom_settings=None):
        MaintenanceFixture.custom_settings = custom_settings

    @classmethod
    def setup_class(cls):
        cls.session_context = tests.init_session(cls.custom_settings)
        cls.db_proxy = cls.session_context.data_proxy

        cls.db_proxy.drop_database()
        cls.db_proxy.ensure_indexes(create_ttl_indexes=False)

        # Populate the database
        dp.populate_users(cls.session_context)
        dp.populate_products(cls.session_context)
        product_maintenance.process_products_from_scratch(cls.session_context)
        dp.populate_activities(cls.session_context)

        # Make a backup of the database so that after each test we can send it back to its original state
        cls.db_proxy.backup_database()

    def setup(self):
        # Restore the database from the backup copy before running another test
        self.db_proxy.restore_database()

    def teardown(self):
        # Drop the dirty copy of the database after the test is executed
        self.db_proxy.drop_database()
