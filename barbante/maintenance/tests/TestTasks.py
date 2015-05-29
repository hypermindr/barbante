""" Tests for barbante.maintenance.tasks.py.
"""

import datetime as dt
import nose.tools

import barbante.maintenance.tasks as tasks
from barbante.maintenance.tests.fixtures.MaintenanceFixture import MaintenanceFixture
import barbante.tests as tests


class TestTasks(MaintenanceFixture):
    """ Test class for maintenance tasks.
    """

    def __init__(self):
        super().__init__()

    def test_increment_product_popularity(self):
        product_1 = "p_mus_1"
        product_2 = "p_empty"
        product_ids = [product_1, product_2]
        popularity_map = self.session_context.data_proxy.fetch_product_popularity(product_ids=product_ids)

        # sanity check
        nose.tools.eq_(popularity_map[product_1], 3, "Wrong initial popularity")
        nose.tools.eq_(popularity_map.get(product_2), None, "Popularity should be None since no one consumed it")

        activity = {"external_user_id": "u_eco_1",
                    "external_product_id": product_1,
                    "activity": "buy",
                    "created_at": self.session_context.get_present_date() - dt.timedelta(2)}
        tasks.update_summaries(self.session_context, activity)

        popularity_map = self.session_context.data_proxy.fetch_product_popularity(product_ids=product_ids)
        nose.tools.ok_(abs(popularity_map[product_1] - 2) < tests.FLOAT_DELTA, "Wrong popularity")

        # another activity by the same user, without extending the date range

        activity = {"external_user_id": "u_eco_1",
                    "external_product_id": product_1,
                    "activity": "buy",
                    "created_at": self.session_context.get_present_date() - dt.timedelta(2)}
        tasks.update_summaries(self.session_context, activity)

        popularity_map = self.session_context.data_proxy.fetch_product_popularity(product_ids=product_ids)
        nose.tools.ok_(abs(popularity_map[product_1] - 2) < tests.FLOAT_DELTA, "Wrong popularity")

        # another activity by the same user, now extending the date range

        activity = {"external_user_id": "u_eco_1",
                    "external_product_id": product_1,
                    "activity": "buy",
                    "created_at": self.session_context.get_present_date() - dt.timedelta(3)}
        tasks.update_summaries(self.session_context, activity)

        popularity_map = self.session_context.data_proxy.fetch_product_popularity(product_ids=product_ids)
        nose.tools.ok_(abs(popularity_map[product_1] - 4/3) < tests.FLOAT_DELTA, "Wrong popularity")

    def test_delete_product(self):
        product_id = "p_mus_1"
        tasks.delete_product(self.session_context, product_id)
        product_models = self.db_proxy.fetch_product_models()
        tf_map = self.db_proxy.fetch_tf_map("full_content", [product_id])
        tfidf_map = self.db_proxy.fetch_tfidf_map("full_content", [product_id])
        nose.tools.ok_(product_id not in product_models,
                       "Product model should have been physically deleted")
        nose.tools.ok_(product_id not in tf_map,
                       "TF's should have been physically deleted")
        nose.tools.ok_(product_id not in tfidf_map,
                       "TFIDF's should have been physically deleted")


