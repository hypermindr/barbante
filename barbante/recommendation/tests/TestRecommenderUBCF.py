""" Test module for barbante.recommendation.RecommenderUBCF class.
"""

from barbante.recommendation.tests.fixtures.UserBasedRecommenderFixture import UserBasedRecommenderFixture


class TestRecommenderUBCF(UserBasedRecommenderFixture):
    """ Class for testing barbante.recommendation.RecommenderUBCF.
    """

    def __init__(self):
        super().__init__()
        self.set_algorithm('UBCF')

    def test_recommend(self, test_recommendation_quality=True):
        """ Tests whether meaningful recommendations were obtained according to Alg UBCF.
        """
        super().test_recommend(test_recommendation_quality=True)
