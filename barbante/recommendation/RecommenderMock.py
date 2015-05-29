""" Mock Recommender for tests purposes only.
"""

from barbante.recommendation.Recommender import Recommender


class RecommenderMock(Recommender):
    """ Returns weird things.
    """
    def __init__(self, session_context):
        super().__init__(session_context)

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "Mock"

    def is_hybrid(self):
        """ See barbante.recommendation.Recommender.
        """
        return False

    def gather_candidate_products(self, n_recommendations):
        """ See barbante.recommendation.Recommender.
        """
        raise KeyError("Mocked error situation")

    def gather_recommendation_scores(self, candidate_product_ids_by_algorithm, n_recommendations):
        """ See barbante.recommendation.Recommender.
        """
        raise ValueError("Mocked error situation")
