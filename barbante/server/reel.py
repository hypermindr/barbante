from datetime import date
import getopt
import signal
import sys
import socket
import re

from tornado import gen
import tornado.escape
import tornado.ioloop
import tornado.web

import barbante
import barbante.api.process_activity_slowlane as process_activity_slowlane
import barbante.api.process_activity_fastlane as process_activity_fastlane
import barbante.api.process_impression as process_impression
import barbante.api.process_product as process_product
import barbante.api.delete_product as delete_product
import barbante.api.cache_stats as cache_stats
import barbante.api.clear_cache as clear_cache
import barbante.api.recommend as recommend
import barbante.api.consolidate_product_templates as consolidate_product_templates
import barbante.api.consolidate_user_templates as consolidate_user_templates
import barbante.api.get_user_templates as user_templates
import barbante.utils.logging as barbante_logging
from barbante.config import is_valid_customer_identifier
from barbante.context.context_manager import new_context


_TRACER_ID_HEADER = 'tracerid'  # HTTP header is case-insensitive
""" The HTTP header carrying the UUID used to identify log messages. """


log = barbante_logging.get_logger('barbante.server.reel')


class FutureHandler(tornado.web.RequestHandler):
    """ Implements the async calls for POST and GET.
    """

    endpoint_pattern = re.compile(r'^/([^/]+)')
    """ A pattern used for obtaining the endpoint name.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = socket.gethostname()

    def do_get(self, *args):
        """ Handles all GET requests.

            :param args: tuple of GET arguments
        """
        raise NotImplementedError()

    def do_post(self, *args):
        """ Handles all POST requests.

            :param args: tuple of POST arguments
        """
        raise NotImplementedError()

    def _get_tracer_id(self) -> str:
        """ :returns: the tracer id obtained from the HTTP header or None otherwise
        """
        return self.request.headers.get(_TRACER_ID_HEADER)

    def _get_endpoint_name(self) -> str:
        """ :returns: the name of the endpoint being requested
        """
        m = self.endpoint_pattern.match(self.request.path)
        try:
            return m.group(1) if m else None
        except IndexError:
            return None

    @gen.coroutine
    def handle_request(self, method, *args):
        def work():
            env = args[0] if len(args) > 0 and is_valid_customer_identifier(args[0]) else None
            with new_context(tracer_id=self._get_tracer_id(), endpoint=self._get_endpoint_name(), environment=env):
                return method(*args)
        response = work()
        self.write(response)

    @gen.coroutine
    def get(self, *args):
        yield self.handle_request(self.do_get, *args)

    @gen.coroutine
    def post(self, *args):
        yield self.handle_request(self.do_post, *args)


class GetUserTemplatesHandler(FutureHandler):
    """ Get user templates web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_get(self, *args):
        return user_templates.main(args)


class ProcessActivitySlowlaneHandler(FutureHandler):
    """ Process activity web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_post(self, *args):
        env = self.get_argument('env')
        user_id = self.get_argument('external_user_id')
        product_id = self.get_argument('external_product_id', None)
        activity_type = self.get_argument('activity_type')
        activity_date = self.get_argument('activity_date')
        return process_activity_slowlane.main([env, user_id, product_id, activity_type, activity_date])


class ProcessActivityFastlaneHandler(FutureHandler):
    """ Process activity web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_post(self, *args):
        env = self.get_argument('env')
        user_id = self.get_argument('external_user_id')
        product_id = self.get_argument('external_product_id', None)
        activity_type = self.get_argument('activity_type')
        activity_date = self.get_argument('activity_date')
        return process_activity_fastlane.main([env, user_id, product_id, activity_type, activity_date])


class ProcessImpressionHandler(FutureHandler):
    """ Process impression web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_post(self, *args):
        env = self.get_argument('env')
        user_id = self.get_argument('external_user_id')
        product_id = self.get_argument('external_product_id')
        impression_date = self.get_argument('impression_date')
        return process_impression.main([env, user_id, product_id, impression_date])


class ProcessProductHandler(FutureHandler):
    """ Process product web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_post(self, *args):
        env = self.get_argument('env')
        product = self.get_argument('product')
        return process_product.main([env, product])


class DeleteProductHandler(FutureHandler):
    """ Delete product web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_post(self, *args):
        env = self.get_argument('env')
        product_id = self.get_argument('product_id')
        deletion_date = self.get_argument('deleted_on')
        if deletion_date == '':
            deletion_date = None
        return delete_product.main([env, product_id, deletion_date])


class RecommendationHandler(FutureHandler):
    """ Recommendation web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_get(self, *args):
        params = [arg for arg in args if arg is not None]
        context_filter = self.get_query_argument('filter', default=None)
        if context_filter:
            params.append(context_filter)
        return recommend.main(params)


class ConsolidateProductTemplatesHandler(FutureHandler):
    """ Consolidate product templates web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_post(self, *args):
        env = self.get_argument('env')
        return consolidate_product_templates.main([env])


class ConsolidateUserTemplatesHandler(FutureHandler):
    """ Consolidate user templates web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_post(self, *args):
        env = self.get_argument('env')
        return consolidate_user_templates.main([env])


class VersionHandler(FutureHandler):
    """ Version web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_get(self, *args):
        return {'version': barbante.__version__,
                'last_build': date.today().isoformat()}


class CacheStatsHandler(FutureHandler):
    """ Version web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_get(self, *args):
        return cache_stats.main(args)


class ClearCacheHandler(FutureHandler):
    """ Version web handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_get(self, *args):
        return clear_cache.main(args)


def handlers():
    """ Available endpoints:

        /get_user_templates/<env>/<user_id>/<n_templates>
        /process_activity - BODY:<env>,<external_user_id>,<external_product_id>,<activity_type>,<activity_date>
        /process_product/<env>/<product_id>
        /process_impression - BODY:<env>,<external_user_id>,<external_product_id>,<impression_date>
        /delete_product - BODY:<env>,<product_id>,<deleted_on>
        /recommend/<env>/<user_id>/<count_recommendations>/<algorithm>/<context_filter_string>
        /consolidate_product_templates/<env>
        /consolidate_user_templates/<env>
        /version
    """
    return [
        (r"/cache_stats/?([^/]+)?", CacheStatsHandler),
        (r"/clear_cache/?([^/]+)?", ClearCacheHandler),
        (r"/get_user_templates/?([^/]+)?/?([^/]+)?/?([^/]+)?", GetUserTemplatesHandler),
        (r"/process_activity_slowlane", ProcessActivitySlowlaneHandler),
        (r"/process_activity_fastlane", ProcessActivityFastlaneHandler),
        (r"/process_product/?([^/]+)?/?([^/]+)?", ProcessProductHandler),
        (r"/process_impression", ProcessImpressionHandler),
        (r"/delete_product", DeleteProductHandler),
        (r"/recommend/([^/]+)/([^/]+)/([^/]+)/([^/]+)?(?:\?filter=([^&]+).*)?", RecommendationHandler),
        (r"/consolidate_product_templates/?([^/]+)?", ConsolidateProductTemplatesHandler),
        (r"/consolidate_user_templates/?([^/]+)?", ConsolidateUserTemplatesHandler),
        (r"/version/?", VersionHandler),
    ]


def start_tornado(port):
    application = tornado.web.Application(handlers())
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()


def stop_tornado():
    tornado.ioloop.IOLoop.instance().stop()


def set_exit_handler(func):
    signal.signal(signal.SIGTERM, func)
    signal.signal(signal.SIGINT, func)


def sig_handler(sig, _):
    log.info("Exiting signal {0} received".format(sig))
    stop_tornado()
    sys.exit(1)


def main(argv):
    port = '8888'
    options, remainder = getopt.getopt(argv, "p:", ['port='])
    for opt, args in options:
        if opt in ('-p', '--port'):
            port = args
    barbante_logging.setup_logging(filename_modifier=port)
    log.info("Reel server running on port [{0}]".format(port))

    set_exit_handler(sig_handler)
    start_tornado(port)


if __name__ == "__main__":
    main(sys.argv[1:])
