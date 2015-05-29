""" Tests barbante.data.MongoDBProxy.
"""

import datetime as dt
import pytz
import nose
import nose.tools

import barbante.tests.dummy_data_populator as dp
import barbante.tests as tests


class TestDataProxy:
    """ Class for the chosen implementation of testing barbante.data.BaseProxy
        (as per barbante.context.DEFAULT_DB_PROXY_CLASS).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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

    def setup(self):
        self.db_proxy.restore_database()

    def teardown(self):
        self.db_proxy.drop_database()

    def test_fetch_products_allowing_deleted(self):
        products = [p for p in self.db_proxy.fetch_all_product_ids(allow_deleted=True)]
        old_products_count = len(products)
        self.db_proxy.delete_product(products[0], self.session_context.get_present_date())
        products = [p for p in self.db_proxy.fetch_all_product_ids(allow_deleted=True)]
        new_products_count = len(products)
        nose.tools.eq_(new_products_count, old_products_count,
                       "Deleted product not being fetched even when explicitly asked to")

    def test_fetch_products_not_allowing_deleted(self):
        products = [p for p in self.db_proxy.fetch_all_product_ids(allow_deleted=False)]
        old_products_count = len(products)
        self.db_proxy.delete_product(products[0], self.session_context.get_present_date())
        products = [p for p in self.db_proxy.fetch_all_product_ids(allow_deleted=False)]
        new_products_count = len(products)
        nose.tools.eq_(new_products_count, old_products_count - 1,
                       "Deleted product being fetched even when explicitly asked not to")

    def test_sample_users(self):
        selected_users = self.db_proxy.sample_users(2)
        nose.tools.eq_(len(selected_users), 2, "Wrong sampling of users")

    def test_fetch_impressions_grouped_by_user(self):
        """ Tests the gathering of impressions by user.
        """
        product_count = self.db_proxy.get_product_count()
        all_users = self.db_proxy.fetch_all_user_ids()
        user_count = self.db_proxy.get_user_count()
        impressions = self.db_proxy.fetch_products_with_impressions_by_user(anonymous=False)
        anonymous_impressions = self.db_proxy.fetch_products_with_impressions_by_user(anonymous=True)
        users_with_impressions = impressions.keys() | anonymous_impressions.keys()
        nose.tools.eq_(len(users_with_impressions), user_count,
                       "Wrong number of users as keys of the impressions map")
        for user in all_users:
            nose.tools.eq_(len(impressions.get(user, set()) | anonymous_impressions.get(user, set())), product_count,
                           "Wrong number of products with impressions")

    def test_fetch_impressions_grouped_by_product(self):
        """ Tests the gathering of impressions by user.
        """
        user_count = self.db_proxy.get_user_count()
        all_products = self.db_proxy.fetch_all_product_ids()
        product_count = self.db_proxy.get_product_count()
        impressions = self.db_proxy.fetch_users_with_impressions_by_product(anonymous=False)
        anonymous_impressions = self.db_proxy.fetch_users_with_impressions_by_product(anonymous=True)
        products_with_impressions = impressions.keys() | anonymous_impressions.keys()
        nose.tools.eq_(len(products_with_impressions), product_count,
                       "Wrong number of products with impressions")
        for product in all_products:
            nose.tools.eq_(len(impressions.get(product, set()) | anonymous_impressions.get(product, set())), user_count,
                           "Wrong number of users with impressions")

    def test_fetch_latest_activity_day_identified_user(self):
        """ Tests BaseProxy.fetch_latest_activity_day for non-anonymous users.
        """
        day = self.db_proxy.fetch_day_of_latest_user_activity("u_eco_1", anonymous=False)
        expected_day = pytz.utc.localize(dt.datetime(1988, 11, 5, 0, 0))
        nose.tools.eq_(day, expected_day, "Wrong latest activity")

    def test_fetch_latest_activity_day_anonymous_user(self):
        """ Tests BaseProxy.fetch_latest_activity_day for anonymous users.
        """
        day = self.db_proxy.fetch_day_of_latest_user_activity("hmrtmp_AnonymousUser1", anonymous=True)
        expected_day = pytz.utc.localize(dt.datetime(1988, 11, 5, 0, 0))
        nose.tools.eq_(day, expected_day, "Wrong latest activity")

    def test_fetch_activities_by_user_without_created_at(self):
        users = ["u_eco_1", "u_mus_1"]
        expected_day = pytz.utc.localize(dt.datetime(1988, 11, 5, 0, 0))
        activities_by_user = self.db_proxy.fetch_activity_summaries_by_user(user_ids=users, anonymous=False)
        for user, activities in activities_by_user.items():
            theme = user[2:5]
            for activity in activities:
                nose.tools.eq_(activity["external_user_id"], user, "Wrong field 'external_user_id'")
                nose.tools.eq_(activity["external_product_id"][2:5], theme, "Wrong field 'external_product_id'")
                nose.tools.eq_(activity["activity"], "buy", "Wrong activity type returned")
                nose.tools.eq_(activity["day"], expected_day, "Wrong field 'day'")

    def test_fetch_activities_by_user_with_created_at(self):
        users = ["u_eco_1"]
        activities_by_user = self.db_proxy.fetch_activity_summaries_by_user(user_ids=users,
                                                                            num_activities=2,
                                                                            indexed_fields_only=False,
                                                                            anonymous=False)
        for user, activities in activities_by_user.items():
            nose.tools.eq_(len(activities), 2, "Wrong number of activities")
            for activity in activities:
                nose.tools.ok_(activity.get("created_at") is not None, "Field 'created_at' was not returned")

    def test_fetch_popularity_by_product(self):
        products = ["p_eco_1", "p_esp_1", "p_mus_3", "p_tec_7"]
        count_by_product = self.db_proxy.fetch_product_popularity(product_ids=products, n_products=2)
        nose.tools.eq_(count_by_product["p_tec_7"], 5, "Wrong user count")
        nose.tools.eq_(count_by_product["p_mus_3"], 5, "Wrong user count")

    def test_fetch_users_by_rating_by_product(self):
        users_by_rating_by_product = self.db_proxy.fetch_users_by_rating_by_product(
            product_ids=["p_mus_2", "p_eco_2"])[0]
        nose.tools.ok_("u_mus_1" in users_by_rating_by_product["p_mus_2"][5], "Missing user")
        nose.tools.ok_("u_mus_2" not in users_by_rating_by_product["p_mus_2"][5], "Wrong user")
        nose.tools.eq_(len(users_by_rating_by_product["p_mus_2"][4]), 0, "Wrong set of users")

    def test_fetch_products_by_rating_by_user(self):
        products_by_rating_by_user = self.db_proxy.fetch_products_by_rating_by_user(
            user_ids=["u_mus_2", "u_eco_2"])[0]
        nose.tools.ok_("p_mus_1" in products_by_rating_by_user["u_mus_2"][5], "Missing product")
        nose.tools.ok_("p_mus_2" not in products_by_rating_by_user["u_mus_2"][5], "Wrong product")
        nose.tools.eq_(len(products_by_rating_by_user["u_mus_2"][4]), 0, "Wrong set of products")



