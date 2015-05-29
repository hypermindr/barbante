import memcache
import hashlib

import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)

class Cache(object):
    """  Barbante interface to memcache
    """

    def __init__(self, settings, prefix):
        """
        :param settings: config's _CacheSetting instance that provides environment and hosts
        :param prefix: the cache name
        :return: an instance of the cache
        """
        self.hosts = settings.hosts
        self.environment = settings.environment
        self.prefix = prefix
        self.key_prefix = "{0}.{1}.".format(self.environment, self.prefix)
        self.memcached = memcache.Client(self.hosts, server_max_key_length=10*1024, debug=0)

    def set(self, key, value):
        self.set_dictionary({key: value})

    def set_dictionary(self, dictionary):
        hashed_keys_dictionary = {self._hash(key): value for key, value in dictionary.items()}
        self.memcached.set_multi(hashed_keys_dictionary, key_prefix=self.key_prefix)

    def get(self, key):
        values = [value for value in self.get_dictionary([key], False).values()]
        value = values[0] if len(values) == 1 else None
        log.info("cache {0}".format("hit" if value else "miss"))
        return value

    def get_dictionary(self, keys, keys_already_hashed=True):
        hashed_keys = [self._hash(key) for key in keys] if not keys_already_hashed else keys
        result = self.memcached.get_multi(hashed_keys, key_prefix=self.key_prefix)
        return result

    def clear(self):
        self.memcached.flush_all()

    def get_stats(self):
        stats = {}
        full_stats = self.memcached.get_stats()
        if len(full_stats) > 0:
            full_stats = {key.decode('utf-8'): value.decode('utf-8') for key, value in full_stats[0][1].items()}
            if full_stats:
                stats = {"size": full_stats['total_items'],
                         "hits": full_stats['get_hits'],
                         "misses": full_stats['get_misses'],
                         "current_connections": full_stats['curr_connections']}
        return stats

    def _hash(self, string):
        return hashlib.md5(string.encode('utf-8')).hexdigest()



