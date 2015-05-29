""" Recommender 0.
"""

from barbante.recommendation.Recommender import Recommender


class RecommenderPOP(Recommender):
    """ Assigns scores to the candidate recommendations based on the overall
        popularity in the short term time window.

        Candidate products: all products recently consumed.

        1st criterion: the greatest number of system users who recently
        consumed the product.
    """

    def __init__(self, session_context):
        super().__init__(session_context)

        self.popularity_by_product = None
        """ Map with the popularity of each product.
        """

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "POP"

    def _load_user_counts(self, n_recommendations, product_ids=None):
        self.popularity_by_product = self.session_context.data_proxy.fetch_product_popularity(
            product_ids=product_ids,
            n_products=n_recommendations,
            min_day=self.session_context.popularity_cutoff_date)

    def gather_candidate_products(self, n_recommendations):
        if self.popularity_by_product is None:
            self._load_user_counts(n_recommendations)
        return {self.get_suffix(): set(self.popularity_by_product.keys())}

    def gather_recommendation_scores(self, candidate_product_ids_by_algorithm, n_recommendations):
        candidates = self.pick_candidate_products(candidate_product_ids_by_algorithm)
        if self.popularity_by_product is None:
            self._load_user_counts(n_recommendations, list(candidates))
        scored_recommendations = []
        for product_id in candidates:
            score = self.popularity_by_product.get(product_id, 0)
            if score > 0:
                score_as_list = [score]
                scored_recommendations += [[score_as_list, product_id]]

        return scored_recommendations
