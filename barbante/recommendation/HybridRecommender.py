""" Hybrid Recommender.
"""

import abc
import concurrent.futures
from time import time
import traceback

from barbante.recommendation.Recommender import Recommender
import barbante.utils.logging as barbante_logging
from barbante.context.context_manager import wrap


log = barbante_logging.get_logger(__name__)


class HybridRecommender(Recommender):
    """ Base abstract class for hybrid recommenders.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, session_context):
        super().__init__(session_context)

        self.recommenders = {}
        """ The concrete recommenders.
        """
        self.algorithms = [alg[0] for alg in session_context.algorithm_weights[self.get_suffix()]]
        """ The algorithm suffixes.
        """
        self.weight_by_algorithm = {alg[0]: alg[1] for alg in session_context.algorithm_weights[self.get_suffix()]}
        """ Algorithm-to-weight map, for convenience. 
        """
        self.fill_in_algorithm = session_context.fill_in_algorithm
        """ Algorithm used to obtain fill-in items, when no other strategy has items left to contribute.
        """
        self.algorithms_including_fill_in = self.algorithms[:]
        """ All algorithm prefixes, including the fill-in algorithm (if any).
        """
        if self.fill_in_algorithm is not None and self.fill_in_algorithm not in self.algorithms_including_fill_in:
            self.algorithms_including_fill_in += [self.fill_in_algorithm]


    def is_hybrid(self):
        """ See barbante.recommendation.Recommender.
        """
        return True

    def gather_candidate_products(self, n_recommendations):
        """ See barbante.recommendation.Recommender.
        """
        log.info(barbante_logging.PERF_BEGIN)

        n_algorithms = len(self.algorithms_including_fill_in)

        candidate_products_by_algorithm = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=n_algorithms) as executor:
                future_to_algorithm = {
                    executor.submit(wrap(self._gather_candidate_products), algorithm, n_recommendations): algorithm
                    for algorithm in self.algorithms_including_fill_in}
                for future in concurrent.futures.as_completed(future_to_algorithm,
                                                              timeout=self.session_context.recommendation_timeout):
                    candidate_products_by_algorithm.update(future.result())

        except concurrent.futures._base.TimeoutError as err:
            log.error("Specialist recommender timeout error: {0}".format(str(err)))
            log.info("Specialists that returned within time limit: {0}".format(candidate_products_by_algorithm.keys()))
            log.info("Specialists that timed out: {0}".format(
                set(self.algorithms_including_fill_in) - set(candidate_products_by_algorithm.keys())))

        log.info(barbante_logging.PERF_END)
        return candidate_products_by_algorithm

    def gather_recommendation_scores(self, candidate_product_ids_by_algorithm, n_recommendations):
        """ See barbante.recommendation.Recommender.
        """
        log.info(barbante_logging.PERF_BEGIN)

        n_algorithms = len(self.algorithms_including_fill_in)

        for algorithm in self.algorithms_including_fill_in:
            if self.recommenders.get(algorithm) is None:
                recommender = self.session_context.get_recommender(algorithm)
                self.recommenders[algorithm] = recommender

        if self.session_context.supported_activities is not None:
            log.info("supported activities: " + ", ".join(self.session_context.supported_activities))
        else:
            log.info("NO SUPPORTED ACTIVITIES")
        log.info("5-star activities: " + ", ".join(self.session_context.activities_by_rating[5]))

        log.debug("Querying %d distinct recommenders" % n_algorithms)

        sorted_scores_by_algorithm = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=n_algorithms) as executor:
                future_to_algorithm = {
                    executor.submit(wrap(self._query_recommender), self.recommenders[algorithm],
                                    candidate_product_ids_by_algorithm, n_recommendations): algorithm
                    for algorithm in self.algorithms_including_fill_in}

                for future in concurrent.futures.as_completed(future_to_algorithm,
                                                              timeout=self.session_context.recommendation_timeout):
                    sorted_scores_by_algorithm[future_to_algorithm[future]] = future.result()

        except concurrent.futures._base.TimeoutError as err:
            log.error("Specialist recommender timeout error: {0}".format(str(err)))
            log.info("Specialists that returned within time limit: {0}".format(sorted_scores_by_algorithm.keys()))
            log.info("Specialists that timed out: {0}".format(
                set(self.algorithms_including_fill_in) - set(sorted_scores_by_algorithm.keys())))

        # Merges the contributions of different specialists.
        recommendations = self.merge_algorithm_contributions(sorted_scores_by_algorithm, n_recommendations)

        # Calls for the fill-in algorithm, when need be.
        self.include_fill_in_recommendations(recommendations, sorted_scores_by_algorithm, n_recommendations)

        log.info(barbante_logging.PERF_END)
        return recommendations

    def include_fill_in_recommendations(self, recommendations, sorted_scores_by_algorithm, n_recommendations):
        """ Because it is possible that not enough recommendations have been obtained,
            we fill in the remaining recommendations with products from the fill-in algorithm.
        """
        if self.fill_in_algorithm is not None:
            recommendations_set = set([rec[1] for rec in recommendations])
            n_items_left_to_fill = n_recommendations - len(recommendations)

            if n_items_left_to_fill > 0:
                log.info("fill-in items required: %d" % n_items_left_to_fill)

                sorted_candidate_scores = sorted_scores_by_algorithm.get(self.fill_in_algorithm)
                if sorted_candidate_scores is None:
                    log.info("No items were returned by the fill-in algorithm.")
                    return

                fill_in_item_idx = 0

                while len(recommendations) < n_recommendations and fill_in_item_idx < len(sorted_candidate_scores):
                    score, candidate = sorted_candidate_scores[fill_in_item_idx]
                    if candidate not in recommendations_set:
                        recommendations += [([self.fill_in_algorithm] + score, candidate)]
                        recommendations_set.add(candidate)
                    fill_in_item_idx += 1

                log.info("fill-in items used: %d" % (n_items_left_to_fill - (n_recommendations - len(recommendations))))

    @abc.abstractmethod
    def merge_algorithm_contributions(self, sorted_scores_by_algorithm, n_recommendations):
        """ Merges the contributions of different algorithms, producing a total order
            of ranked recommendations.

            :param sorted_scores_by_algorithm: A dict {alg_suffix: list of [score_tuple, external_product_id] pairs}.
            :param n_recommendations: The intended number of recommendations on the output list.

            :returns: A list of *n_recommendations* totally ordered [score_tuple, external_product_id] pairs.
        """

    def post_process_scores(self, scored_recommendations):
        """ The HybridRecommender does not post_process because it delegates
            the task to each of the algorithms, so it basically returns the assembled list.

            Parameters:
                *scored_recommendations* - list of [score, product_id] pairs
        """
        return scored_recommendations

    def _nlargest(self, n_recommendations, scored_recommendations):
        """ See barbante.recommendation.Recommender.
        """
        return scored_recommendations[:n_recommendations]

    def _gather_candidate_products(self, algorithm, n_recommendations):
        if self.recommenders.get(algorithm) is None:
            # lazily populates the recommenders map
            self.recommenders[algorithm] = self.session_context.get_recommender(algorithm)
        recommender = self.recommenders[algorithm]

        try:
            log.info('Gathering candidate products for {}...'.format(algorithm))
            result = recommender.gather_candidate_products(n_recommendations)
        except Exception as error:
            result = {algorithm: set()}
            log.error("Recommender [{0}] gather_candidate_products failed with exception: {1}. stack_trace: {2}".
                      format(type(recommender).__name__, error, traceback.format_exc()))

        return result

    @staticmethod
    def _query_recommender(recommender, candidate_product_ids, n_recommendations):
        try:
            log.info('Querying [{}]...'.format(type(recommender).__name__))
            start = time()

            log.info('Gathering scores for [{}]...'.format(type(recommender).__name__))
            product_scores = recommender.gather_recommendation_scores(candidate_product_ids, n_recommendations)

            log.info('Post-processing scores for [{}]...'.format(type(recommender).__name__))
            processed_scores = recommender.post_process_scores(product_scores)
            sorted_scores = sorted(processed_scores, reverse=True)

            log.info('[%s] returned [%d] scores before post-processing ([%d] after post-processing). '
                     'Took %d milliseconds.'
                     % (type(recommender).__name__, len(product_scores), len(sorted_scores), 1000 * (time() - start)))

            result = sorted_scores

        except Exception as error:
            result = []
            log.error("[{0}] failed with exception: {1}. stack_trace: {2}".format(
                type(recommender).__name__, error, traceback.format_exc()))

        return result
