""" Test module for barbante.recommendation.RecommenderPOP class.
"""

import nose.tools

from barbante.recommendation.tests.fixtures.RecommenderFixture import RecommenderFixture
import barbante.maintenance.tasks as tasks
import barbante.tests as tests


class TestRecommenderPOP(RecommenderFixture):
    """ Class for testing barbante.recommendation.RecommenderPOP.
    """

    def __init__(self):
        super().__init__()
        self.set_algorithm('POP')

    def test_recommend(self, test_recommendation_quality=True):
        """ Tests whether meaningful recommendations were obtained according to Alg 0.
        """
        target = "u_user_empty"

        all_users = self.db_proxy.fetch_all_user_ids()
        for user in all_users:
            if user != target:
                activity = {"external_user_id": user,
                            "external_product_id": "p_TOP_POPULAR",
                            "activity": "buy",
                            "created_at": self.session_context.get_present_date()}
                tasks.update_summaries(self.session_context, activity)

                if user != "u_user_dummy":
                    activity = {"external_user_id": user,
                                "external_product_id": "p_2ndTOP_POPULAR",
                                "activity": "buy",
                                "created_at": self.session_context.get_present_date()}
                    tasks.update_summaries(self.session_context, activity)

        # Checks whether all users got recommendations
        super().test_recommend(test_recommendation_quality=False)

        # Checks whether the recommendations conform to the top-popularity criterion

        session = tests.init_session(user_id=target, algorithm=self.algorithm)
        recommender = session.get_recommender()
        recommendations = recommender.recommend(2)

        nose.tools.ok_(len(recommendations) > 0,
                       "No recommendations were retrieved")

        nose.tools.eq_(recommendations[0][1], "p_TOP_POPULAR",
                       "Weird recommendation -- should be the most popular product")

        nose.tools.eq_(recommendations[1][1], "p_2ndTOP_POPULAR",
                       "Weird recommendation -- should be the 2nd most popular product")

    def test_recommend_non_existing_user(self):
        """ Tests whether valid recommendations are returned for an unknown user.
        """
        session = tests.init_session(user_id="Invalid user id", algorithm=self.algorithm)
        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)
        nose.tools.ok_(len(results) > 0, "Recommender 0 should recommend even for non-existing users.")

    def test_recommend_anonymous_user(self):
        """ Tests whether valid recommendations are returned for an anonymous user.
        """
        session = tests.init_session(user_id="hmrtmp_AnonymousUser1", algorithm=self.algorithm)
        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)
        nose.tools.ok_(len(results) > 0, "Recommender 0 should recommend even for anonymous users.")

    def test_in_boost(self):
        pass  # Saving a boosted activity may imply changes in overall popularity -- we should not test that in Alg 0.

