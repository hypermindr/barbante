""" Hybrid Recommender HRRandom.
"""

import random

from barbante.recommendation.HybridRecommender import HybridRecommender
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


class RecommenderHRRandom(HybridRecommender):
    """ Hybrid Recommender HRRandom.
        It merges different algorithms randomly, respecting the probability assigned to each algorithm
        and the relative orders of the recommendations produces by each strategy.
    """

    def __init__(self, session_context):
        super().__init__(session_context)

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "HRRandom"

    def obtain_cdf(self, recommendations_by_algorithm, item_idx_by_algorithm):
        """ Computes the cumulative distribution function associated to the algorithms
            that are used by this recommender.

            :param recommendations_by_algorithm: The set of algorithms to be sampled.
            :param item_idx_by_algorithm: Points to the next item to be consumed from the list of contributions
                of a given algorithm. When such index is equal to the size of the list, that algorithm has
                nothing left to contribute.
            :returns: A list with (Sum(p_i), alg) pairs, where Sum(p_i) is the cumulative
                probability for algorithm *alg*.
        """
        cdf = []
        cumulative_prob = 0
        for algorithm, prob in self.session_context.algorithm_weights[self.get_suffix()]:
            if item_idx_by_algorithm[algorithm] < len(recommendations_by_algorithm.get(algorithm, {})):
                cumulative_prob += prob
                cdf += [(algorithm, cumulative_prob)]
        # normalizes
        cdf = [(cdf_item[0], cdf_item[1] / cumulative_prob) for cdf_item in cdf]
        return cdf

    @staticmethod
    def choose_algorithm(cdf):
        result = None
        rand = random.random()
        for algorithm, cumulative_prob in cdf:
            if rand < cumulative_prob:
                result = algorithm
                break
        return result

    def merge_algorithm_contributions(self, sorted_scores_by_algorithm, n_recommendations):
        """ See barbante.recommend.HybridRecommender.
        """
        log.debug("Merging contributions...")

        recommendations = []
        recommendations_set = set()  # avoids duplicates among different algorithms

        contributions_by_algorithm = {alg: 0 for alg in self.algorithms}  # for logging
        item_idx_by_algorithm = {alg: 0 for alg in self.algorithms}  # to keep track of traversal position

        # Selects recommendations randomly, based on the probability distribution given by the algorithm weights.

        cdf = self.obtain_cdf(sorted_scores_by_algorithm, item_idx_by_algorithm)
        n_items_left_to_fill = n_recommendations - len(recommendations)

        while n_items_left_to_fill > 0:
            algorithm = self.choose_algorithm(cdf)
            if algorithm is None:
                break  # all algorithm contributions have been exhausted

            sorted_candidate_scores = sorted_scores_by_algorithm.get(algorithm)
            if sorted_candidate_scores is None:
                continue

            while item_idx_by_algorithm[algorithm] < len(sorted_candidate_scores):
                score, candidate = sorted_candidate_scores[item_idx_by_algorithm[algorithm]]
                item_idx_by_algorithm[algorithm] += 1

                if candidate not in recommendations_set:
                    recommendations_set.add(candidate)
                    contributions_by_algorithm[algorithm] += 1
                    # prepends the identification of the source algorithm in the score tuple
                    recommendations += [([algorithm] + score, candidate)]
                    break

            updated_n_items_left_to_fill = n_recommendations - len(recommendations)
            if updated_n_items_left_to_fill == n_items_left_to_fill:
                # chosen algorithm has no more contributions to give -- let's update the cdf
                cdf = self.obtain_cdf(sorted_scores_by_algorithm, item_idx_by_algorithm)

            n_items_left_to_fill = updated_n_items_left_to_fill

        for alg in self.algorithms:
            log.info("Algorithm [%s] contributed [%d] items" % (alg, contributions_by_algorithm[alg]))

        return recommendations
