import http.client
import json
import urllib.parse

from tornado import web, testing
from tornado.httpclient import HTTPRequest
import nose.tools
from nose.tools import nottest

import barbante.server.reel as reel
from barbante.server.reel import FutureHandler
import barbante.tests as tests
from barbante.context.context_manager import global_context_manager as gcm
from barbante.tests import dummy_data_populator as dp


class ReelTest(testing.AsyncHTTPTestCase):

    @classmethod
    def setup_class(cls):
        cls.session_context = tests.init_session()
        cls.db_proxy = cls.session_context.data_proxy

        cls.db_proxy.drop_database()

        dp.populate_products(cls.session_context)
        dp.populate_users(cls.session_context)
        dp.populate_activities(cls.session_context)
        dp.populate_impressions(cls.session_context)
        cls.db_proxy.ensure_indexes(create_ttl_indexes=False)

        cls.db_proxy.backup_database()

    def setUp(self):
        super().setUp()
        self.db_proxy.restore_database()

    def tearDown(self):
        self.db_proxy.drop_database()
        super().tearDown()

    def get_app(self):
        return web.Application(reel.handlers())

    def test_version(self):
        response = self.fetch('/version')
        self.assertEqual(response.code, http.client.OK)

    def test_process_product(self):
        product_id = "p_eco_1"
        product = self.db_proxy.fetch_products(product_ids=[product_id],
                                               fields_to_project=["language",
                                                                  "category",
                                                                  "resources.title",
                                                                  "full_content"]).get(product_id)
        product["_id"] = None
        product["external_id"] = product_id
        product["date"] = "2015-03-20T12:30.000Z"
        product["expiration_date"] = "2016-03-20T12:30.000Z"

        post_data = {'env': tests.TEST_ENV,
                     'product': json.dumps(product)}
        body = urllib.parse.urlencode(post_data)

        # Creates the product model for the first time
        response = self.fetch('/process_product', method='POST', headers=None, body=body)
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

        # Sanity check
        product_model = self.db_proxy.fetch_product_models(product_ids=[product_id]).get(product_id)
        self.assertEqual(product_model.get_attribute("category"), "Economia", "Wrong field in persisted product model")

        # Creates another post, after updating the 'category' and 'full_content' fields with different values
        product["category"] = "Politica"
        product["full_content"] = "Conteúdo alterado para fins de teste"

        post_data = {'env': tests.TEST_ENV,
                     'product': json.dumps(product)}
        body = urllib.parse.urlencode(post_data)

        # Triggers the update of the product model via reel endpoint
        response = self.fetch('/process_product', method='POST', headers=None, body=body)
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

        # Checks whether the product model has been updated accordingly
        product_model = self.db_proxy.fetch_product_models(product_ids=[product_id]).get(product_id)
        self.assertEqual(product_model.get_attribute("category"), "Politica", "Wrong field in persisted product model")

        # Checks the product terms
        tf_map = self.db_proxy.fetch_tf_map("full_content", [product_id]).get(product_id)
        self.assertEqual(len(tf_map), 5, "Wrong number of terms")
        self.assertTrue("alter" in tf_map, "Missing term")

    def test_process_activity_fastlane(self):
        post_data = {'env': tests.TEST_ENV,
                     'external_user_id': 'u_eco_1',
                     'external_product_id': 'p_eco_2',
                     'activity_type': 'buy',
                     'activity_date': '1988-11-07T10:00:00Z'}
        body = urllib.parse.urlencode(post_data)
        response = self.fetch('/process_activity_fastlane', method='POST', headers=None, body=body)
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_process_activity_slowlane(self):
        post_data = {'env': tests.TEST_ENV,
                     'external_user_id': 'u_eco_1',
                     'external_product_id': 'p_eco_2',
                     'activity_type': 'buy',
                     'activity_date': '1988-11-07T10:00:00Z'}
        body = urllib.parse.urlencode(post_data)
        response = self.fetch('/process_activity_slowlane', method='POST', headers=None, body=body)
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_process_impression(self):
        post_data = {'env': tests.TEST_ENV,
                     'external_user_id': 'u_eco_1',
                     'external_product_id': 'u_mus_2',
                     'impression_date': '1988-11-07T10:00:00Z'}
        body = urllib.parse.urlencode(post_data)
        response = self.fetch('/process_impression', method='POST', headers=None, body=body)
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_delete_product(self):
        post_data = {'env': tests.TEST_ENV,
                     'product_id': 'u_eco_1',
                     'deleted_on': '1988-11-07T10:00:00Z'}
        body = urllib.parse.urlencode(post_data)
        response = self.fetch('/delete_product', method='POST', headers=None, body=body)
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_recommend_success_without_filters(self):
        response = self.fetch('/recommend/' + tests.TEST_ENV + '/u_eco_1/10/HRChunks')
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_recommend_success_with_filters(self):
        response = self.fetch('/recommend/' + tests.TEST_ENV + '/u_eco_1/10/HRChunks?filter={"language":"english"}')
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_recommend_failure_ill_formed_filter(self):
        response = self.fetch('/recommend/' + tests.TEST_ENV + '/u_eco_1/10/HRChunks?filter=invalid_filter')
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], False, "Wrong success indicator")

    def test_consolidate_user_templates(self):
        post_data = {'env': tests.TEST_ENV}
        body = urllib.parse.urlencode(post_data)
        response = self.fetch('/consolidate_user_templates', method='POST', headers=None, body=body)
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_consolidate_product_templates(self):
        post_data = {'env': tests.TEST_ENV}
        body = urllib.parse.urlencode(post_data)
        response = self.fetch('/consolidate_product_templates', method='POST', headers=None, body=body)
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_get_user_templates(self):
        response = self.fetch('/get_user_templates/' + tests.TEST_ENV + '/u_eco_1')
        self.assertEqual(response.code, http.client.OK)
        self.assertEqual(json.loads(response.body.decode("utf-8"))["success"], True, "Wrong success indicator")

    def test_failure_invalid_endpoint(self):
        response = self.fetch('/xxxxxxx/')
        self.assertEqual(response.code, http.client.NOT_FOUND)


@nottest
class TracerIdTestHandler(FutureHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_get(self, *args):
        return {"success": True, "tracerid": str(gcm.get_context().tracer_id)}


class TracerIdTest(testing.AsyncHTTPTestCase):

    def get_app(self):
        return web.Application([
            (r"/echo", TracerIdTestHandler)
        ])

    def test_tracer_id(self):
        """ Should echo back the same tracer id passed as a paramenter in the HTTP request header.
        """
        tracer_id = '6c006821-0cb1-42e8-8ab1-fb6e059cabab'.replace('-', '')

        def on_fetch(response):
            response = json.loads(response.body.decode('utf-8'))
            nose.tools.eq_(tracer_id, response.get('tracerid'))
            self.stop()

        request = HTTPRequest(url=self.get_url('/echo'),
                              headers={'tracerid': tracer_id})
        self.http_client.fetch(request, on_fetch)
        self.wait()
