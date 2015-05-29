""" Test module for barbante.recommendation.RecommenderPBCF class.
"""

import barbante.maintenance.product_templates as pt
from barbante.recommendation.tests.fixtures.ProductBasedRecommenderFixture import ProductBasedRecommenderFixture


class TestRecommenderPBCF(ProductBasedRecommenderFixture):
    """ Class for testing barbante.recommendation.RecommenderPBCF.
    """

    def __init__(self):
        super().__init__()
        self.set_algorithm('PBCF')

    def setup(self):
        super().setup()
        pt.generate_templates(self.session_context)

    def test_recommend(self, test_recommendation_quality=True):
        """ Tests whether meaningful recommendations were obtained according to Alg PBCF.
        """
        super().test_recommend(test_recommendation_quality=True)
