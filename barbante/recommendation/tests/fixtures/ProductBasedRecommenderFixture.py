""" Test fixture for product-based recommendations.
"""

import nose.tools
import datetime as dt

import barbante.tests as tests
import barbante.maintenance.product as prd
import barbante.maintenance.tasks as tasks
from barbante.recommendation.tests.fixtures.RecommenderFixture import RecommenderFixture


class ProductBasedRecommenderFixture(RecommenderFixture):
    """ Class for testing barbante.recommendation.ProductBasedRecommender subclasses.
    """
    def __init__(self):
        super().__init__()

    def test_deleted_base_product(self):
        """ Tests whether a base product which has been deleted will not cause the recommender to fail.
        """
        user_id = "u_tec_1"
        session = tests.init_session(user_id=user_id, algorithm=self.algorithm)
        prd.delete_product(session, "p_tec_2")
        session.refresh()
        recommender = session.get_recommender()
        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "Should have recommended even from a deleted base product")

    def test_unprocessed_base_product(self):
        """ Tests whether a base product which has not yet been processed (i.e., it lacks a product model)
            will not cause the recommender to fail.
        """
        user_id = "u_tec_1"
        session = tests.init_session(user_id=user_id, algorithm=self.algorithm)
        activity = {"external_user_id": user_id,
                    "external_product_id": "unprocessed_product",
                    "activity": "buy",
                    "created_at": session.get_present_date()}
        tasks.update_summaries(self.session_context, activity)
        session.refresh()
        recommender = session.get_recommender()
        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "Should have recommended even from an unprocessed base product")

    def test_recommend_anonymous_user(self):
        """ Tests whether valid recommendations are returned for an anonymous user.
        """
        session = tests.init_session(user_id="hmrtmp_AnonymousUser1", algorithm=self.algorithm)
        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)
        nose.tools.ok_(len(results) > 0, "Product-based recommenders should recommend even for anonymous users")
        nose.tools.ok_(results[0][1].startswith("p_mus"), "Anonymous user got a weird recommendation")

    def test_in_boost(self):
        pass

    def test_product_age_decay_exponential(self):
        pass

    def test_near_identical(self):
        pass

    # On the three overriden tests above:
    #
    # Product-based recommenders use base products in a round-robin fashion, so it is not exactly easy
    # to test whether recommended products are being age-decayed or in-boosted or near-identical-filtered
    # using our current test strategies. Moreover, it is not really necessary to have these recommenders
    # be thoroughly tested, as long as all *hybrid* recommenders pass the test.

    def test_base_product_democracy(self):
        """ Tests whether all base products can send their templates to the final recommendation list.
        """
        user_id = "new_user"
        session = tests.init_session(user_id=user_id, algorithm=self.algorithm)
        types = ["esp", "tec", "eco"]
        for idx, product_type in enumerate(types):
            activity = {"external_user_id": user_id,
                        "external_product_id": "p_" + product_type + "_1",
                        "activity": "buy",
                        "created_at": session.get_present_date() + dt.timedelta(seconds=idx)}
            tasks.update_summaries(self.session_context, activity)

        session.refresh()

        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)

        for idx, product_type in enumerate(types[-1::-1]):
            nose.tools.eq_(results[idx][1][2:5], product_type,
                           "A product of type '%s' should have appeared at position %d in the list"
                           % (product_type, idx))



