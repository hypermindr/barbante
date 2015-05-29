""" Hybrid Recommender HRVoting.
"""

import heapq

from barbante.recommendation.HybridRecommender import HybridRecommender
from barbante.utils.decay_functions import exponential

import barbante.utils.logging as barbante_logging
log = barbante_logging.get_logger(__name__)


HALF_LIFE_FORMULA_1_SCORING = 4

NO_BONUS_DIRECTIVE = "nobonus"


class RecommenderHRVoting(HybridRecommender):
    """ Hybrid Recommender HRVoting.
        It merges different algorithms using a voting system, whereby each product receives a score that is
        the sum of the reverse ranks assigned to it by each strategy, weighted by the strategy importance.
    """
    def __init__(self, session_context):
        super().__init__(session_context)

        self.bonus_period = 8  # TODO (Vinicius) Move to customer config, maybe?
        """ At every *bonus_period* items added to the merged ranking,
            HRVoting forces the inclusion of the next ranked item of each recommender, if such item has not appeared yet.
        """

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "HRVoting"

    @staticmethod
    def _include_product(product, original_score, final_ranking, ranked_products_set, recommendations_count):
        if product not in ranked_products_set:
            final_ranking += [([recommendations_count - len(ranked_products_set)] + original_score, product)]
            ranked_products_set.add(product)

    def merge_algorithm_contributions(self, sorted_scores_by_algorithm, n_recommendations):
        """ See barbante.recommend.HybridRecommender.
        """
        log.debug("Merging contributions...")

        # Implements the voting system.

        votes_by_product = {}
        max_vote_value = max(1000, n_recommendations)  # considers at least the top 1000 products of each algorithm

        for algorithm_recipe in self.session_context.algorithm_weights[self.get_suffix()]:
            alg = algorithm_recipe[0]
            weight = algorithm_recipe[1]
            sorted_scores = sorted_scores_by_algorithm.get(alg, {})
            for idx, (_, product) in enumerate(sorted_scores):
                if idx == n_recommendations:
                    break
                vote_value = max_vote_value * exponential(idx, HALF_LIFE_FORMULA_1_SCORING)
                votes = votes_by_product.get(product, 0)
                votes_by_product[product] = votes + vote_value * weight
            log.info("Algorithm [%s] ranked [%d] products" % (alg, len(sorted_scores)))

        recommendations = [([votes], product) for product, votes in votes_by_product.items()]
        sorted_recommendations = heapq.nlargest(n_recommendations, recommendations)

        # Applies periodic bonuses to prevent a monopoly of the highest weighted algorithm in the top standings.

        final_ranking = []
        ranked_products_set = set()
        recommendations_count = len(sorted_recommendations)

        for idx in range(recommendations_count):

            # Bonus time?
            if idx % self.bonus_period == 0:
                bonus_count = idx // self.bonus_period

                for algorithm_recipe in self.session_context.algorithm_weights[self.get_suffix()]:
                    alg = algorithm_recipe[0]
                    if NO_BONUS_DIRECTIVE in algorithm_recipe:
                        continue

                    sorted_scores = sorted_scores_by_algorithm.get(alg)
                    if sorted_scores is None:
                        continue

                    if bonus_count < len(sorted_scores):
                        score_and_product = sorted_scores[bonus_count]
                        if score_and_product is not None:
                            self._include_product(score_and_product[1],  # the forced product (benefited by bonus)
                                                  score_and_product[0],  # its original score
                                                  final_ranking, ranked_products_set, recommendations_count)

            score_and_product = sorted_recommendations[idx]
            self._include_product(score_and_product[1],  # the current product
                                  score_and_product[0],  # its original score
                                  final_ranking, ranked_products_set, recommendations_count)
        return final_ranking
