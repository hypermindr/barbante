""" Recommender Random.
"""

import random

from barbante.recommendation.Recommender import Recommender


class RecommenderRandom(Recommender):
    """ Returns all products in a random order.
    """
    def __init__(self, session_context):
        super().__init__(session_context)

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "Random"

    def is_hybrid(self):
        """ See barbante.recommendation.Recommender.
        """
        return False

    def gather_candidate_products(self, n_recommendations):
        """ See barbante.recommendation.Recommender.
        """
        product_ids = {p for p in self.session_context.data_proxy.fetch_all_product_ids(
            allow_deleted=False, max_date=self.session_context.get_present_date())}

        if len(product_ids) == 0:
            return []

        product_ids_set = product_ids.difference(self.session_context.blocked_products)
        if n_recommendations > len(product_ids_set):
            n_recommendations = len(product_ids_set)
        candidate_products = random.sample(product_ids_set, n_recommendations)

        return {self.get_suffix(): set(candidate_products)}

    def gather_recommendation_scores(self, candidate_product_ids_by_algorithm, n_recommendations):
        """ See barbante.recommendation.Recommender.
        """
        score = 0
        scored_recommendations = []
        for product_id in self.pick_candidate_products(candidate_product_ids_by_algorithm):
            scored_recommendations += [[[1 - score / (n_recommendations - 1)], product_id]]
            score += 1

        return scored_recommendations
