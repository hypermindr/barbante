import unittest.mock as mock

import nose
import nose.tools
from pymongo.read_preferences import ReadPreference

from barbante.data.MongoDBProxy import MongoDBProxy
import barbante.tests as tests
from pymongo import MongoClient


class TestMongoDBProxyCreation:
    """ Class for testing the creation of a MongoDBProxy.
    """
    def __init__(self):
        self.context = tests._init_context(None)

    def test_read_defaults(self):
        self.context.database_settings.read_preference = None
        mongo_proxy = MongoDBProxy(self.context)
        nose.tools.eq_(mongo_proxy.database.read_preference, ReadPreference.PRIMARY,
                       "Default connection to primary")

    def test_read_primary(self):
        self.context.database_settings.read_preference = 'primary'
        mongo_proxy = MongoDBProxy(self.context)
        nose.tools.eq_(mongo_proxy.database.read_preference, ReadPreference.PRIMARY,
                       "Connection should be set to primary")

    @mock.patch('barbante.data.MongoDBProxy.pymongo.MongoClient')
    def test_read_secondary(self, mock_proxy):
        self.context.database_settings.read_preference = 'secondary'
        mock_proxy.side_effect = lambda *args, **kwargs: MongoClient(*args, **kwargs)
        mongo_proxy = MongoDBProxy(self.context)
        nose.tools.eq_(mongo_proxy.database.read_preference, ReadPreference.SECONDARY,
                       "Should connect to secondary")

    @mock.patch('barbante.data.MongoDBProxy.pymongo.MongoClient')
    def test_read_secondary_preferred(self, mock_proxy):
        self.context.database_settings.read_preference = 'secondary_preferred'
        mock_proxy.side_effect = lambda *args, **kwargs: MongoClient(*args, **kwargs)
        mongo_proxy = MongoDBProxy(self.context)
        nose.tools.eq_(mongo_proxy.database.read_preference, ReadPreference.SECONDARY_PREFERRED,
                       "Should connect to secondary")
