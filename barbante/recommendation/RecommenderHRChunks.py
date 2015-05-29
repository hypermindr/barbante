""" Hybrid Recommender HRChunks.
"""

import math
import fractions

from barbante.recommendation.HybridRecommender import HybridRecommender
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


class RecommenderHRChunks(HybridRecommender):
    """ Hybrid Recommender HRChunks.
    """

    def __init__(self, session_context):
        super().__init__(session_context)

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "HRChunks"

    def obtain_normalizing_factor(self):
        """ Calculates the number that must be multiplied by each weight so that:
            - all weights are integer numbers;
            - the greatest common divisor of the integer weights is 1.
        """
        # makes every weight an integer assuming we won't ever bother to use more than two decimal places)
        scale_factor = 100

        weights = [item[1] * scale_factor for item in self.session_context.algorithm_weights[self.get_suffix()]]

        gcd = weights[0]
        for i in range(1, len(weights)):
            gcd = fractions.gcd(gcd, weights[i])

        return scale_factor / gcd

    def merge_algorithm_contributions(self, sorted_scores_by_algorithm, n_recommendations):
        """ See barbante.recommendation.HybridRecommender.
        """
        log.debug("Merging contributions...")

        recommendations = []
        recommendations_set = set()  # avoids duplicates among different algorithms

        contributions_by_algorithm = {alg: 0 for alg in self.algorithms}  # for logging
        item_idx_by_algorithm = {alg: 0 for alg in self.algorithms}  # to keep track of traversal position

        # Selects from each algorithm a number of recommendations which is proportional
        # to the weight assigned to that algorithm.

        normalizing_factor = self.obtain_normalizing_factor()
        desired_algorithm_contributions = [(item[0], math.ceil(item[1] * normalizing_factor))
                                           for item in self.session_context.algorithm_weights[self.get_suffix()]]

        n_items_left_to_fill = n_recommendations - len(recommendations)
        while n_items_left_to_fill > 0:

            for algorithm, desired_contributions in desired_algorithm_contributions:

                sorted_candidate_scores = sorted_scores_by_algorithm.get(algorithm)
                if sorted_candidate_scores is None:
                    continue

                new_contributions = 0

                while item_idx_by_algorithm[algorithm] < len(sorted_candidate_scores) \
                        and new_contributions < desired_contributions:
                    score, candidate = sorted_candidate_scores[item_idx_by_algorithm[algorithm]]
                    item_idx_by_algorithm[algorithm] += 1

                    if candidate not in recommendations_set:
                        recommendations_set.add(candidate)
                        new_contributions += 1
                        contributions_by_algorithm[algorithm] += 1
                        # prepends the identification of the source algorithm in the score tuple
                        recommendations += [([algorithm] + score, candidate)]

            updated_n_items_left_to_fill = n_recommendations - len(recommendations)
            if updated_n_items_left_to_fill == n_items_left_to_fill:
                break  # not making progress: recommenders are all empty
            n_items_left_to_fill = updated_n_items_left_to_fill

        for alg in self.algorithms:
            log.info("Algorithm [%s] contributed [%d] items" % (alg, contributions_by_algorithm[alg]))

        return recommendations

