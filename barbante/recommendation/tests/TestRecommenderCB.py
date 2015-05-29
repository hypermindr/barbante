""" Test module for barbante.recommendation.RecommenderCB class.
"""

import barbante.maintenance.product_templates_tfidf as pttfidf
from barbante.recommendation.tests.fixtures.ProductBasedRecommenderFixture import ProductBasedRecommenderFixture


class TestRecommenderCB(ProductBasedRecommenderFixture):
    """ Class for testing barbante.recommendation.RecommenderCB.
    """

    def __init__(self):
        super().__init__()
        self.set_algorithm('CB')

    def setup(self):
        super().setup()
        pttfidf.generate_templates(self.session_context)

    def test_recommend(self, test_recommendation_quality=True):
        """ Tests whether meaningful recommendations were obtained according to Alg CB.
        """
        super().test_recommend(test_recommendation_quality=False)
