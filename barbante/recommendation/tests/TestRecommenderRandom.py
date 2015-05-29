""" Test module for barbante.recommendation.RecommenderRandom class.
"""

import nose.tools

from barbante.recommendation.tests.fixtures.RecommenderFixture import RecommenderFixture
import barbante.tests as tests


class TestRecommenderRandom(RecommenderFixture):
    """ Class for testing barbante.recommendation.RecommenderRandom.
    """

    def __init__(self):
        super().__init__()
        self.set_algorithm('Random')

    def test_recommend(self, test_recommendation_quality=True):
        super().test_recommend(test_recommendation_quality=False)
        target = "u_user_empty"
        session = tests.init_session(user_id=target, algorithm=self.algorithm)
        recommender = session.get_recommender()
        recommendations = recommender.recommend(5)
        nose.tools.ok_(len(recommendations) == 5, "No recommendations were retrieved")

    def test_recommend_non_existing_user(self):
        """ Tests whether an empty set is returned when an invalid user id is
            passed as parameter to the Recommender constructor.
        """
        session = tests.init_session(user_id="Invalid user id", algorithm=self.algorithm)
        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)
        nose.tools.ok_(len(results) > 0, "The random recommender should recommend even for non-existing users")

    def test_recommend_anonymous_user(self):
        """ Tests whether valid recommendations are returned for an anonymous user.
        """
        session = tests.init_session(user_id="hmrtmp_AnonymousUser1", algorithm=self.algorithm)
        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)
        nose.tools.ok_(len(results) > 0, "The random recommender should recommend even for anonymous users")

    def test_history_decay_rational(self):
        pass  # It is meaningless to test decays for random recommendation.

    def test_history_decay_exponential(self):
        pass  # It is meaningless to test decays for random recommendation.

    def test_history_decay_linear(self):
        pass  # It is meaningless to test decays for random recommendation.

    def test_history_decay_step(self):
        pass  # It is meaningless to test decays for random recommendation.

    def test_in_boost(self):
        pass  # It is meaningless to test decays for random recommendation.

    def test_product_age_decay_exponential(self):
        pass  # It is meaningless to test decays for random recommendation.

    def test_near_identical(self):
        pass  # It would be hard (and useless) to test this correctly for the random recommender. Let's not bother.