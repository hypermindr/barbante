""" Recommender base class.
"""

import abc
import heapq
import random
from time import time

from barbante.maintenance.product import pinpoint_near_identical_products
from barbante.utils.profiling import profile
import barbante.utils.logging as barbante_logger
import barbante.context as ctx


log = barbante_logger.get_logger(__name__)


PRE_FILTER = "pf"
""" This constant is used as a replacement for the suffix of the concrete algorithm
    in the pre-fetched map of candidate products by algorithm.
    The rationale is, if the filter strategy is BEFORE_SCORING, then all algorithms
    are supposed to be provided with the same set of candidates. A single key (PRE_FILTER)
    avoids candidate replication over all algorithms used by hybrid recommenders.
"""


class Recommender():
    """ Base abstract class for recommendation.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, session_context):
        if session_context is None:
            raise AttributeError("Context cannot be None")

        self.session_context = session_context
        """ Session context with user-specific data for the present session.
        """

    @abc.abstractmethod
    def get_suffix(self):
        """ Retrieves the algorithm suffix (internal name).
            Should be overridden by each concrete subclass.
        """
        pass

    @abc.abstractmethod
    def is_hybrid(self):
        """ Determines whether the current recommender is hybrid, i.e., if the works
            by merging together the results of other Recommender objects.

            :returns: a boolean indicating hybridness.
        """
        pass

    @abc.abstractmethod
    def gather_candidate_products(self, n_recommendations):
        """ Generates the candidate products list based on a specific algorithm or family of algorithms.

            :param n_recommendations: The minimum number of candidates to be returned.
            :returns: A map {algorithm_suffix: set of candidate product ids}.
        """
        pass

    def _gather_pre_filtered_product_ids(self):
        """ Retrieves the pre-filtered products and sets the product models.

            :returns: map {algorithm_suffix: set of candidate product ids}.
        """
        return {PRE_FILTER: self.session_context.filtered_products}

    def _gather_pos_filtered_product_ids(self, n_recommendations):
        """ Gathers the candidate products of this recommender and pos-filters them, caching their product models.

            :param: n_recommendations: The minimum number of candidates to be returned.
            :returns: A map {algorithm_suffix: set of candidate product ids}.
        """
        products_by_algorithm = self.gather_candidate_products(n_recommendations)

        # Builds a set of unique product ids, already filtering out those which have blocking activities.

        products_set = set()
        for _, products in products_by_algorithm.items():
            products_set |= products
        products_set = {product_id for product_id in products_set if not self._has_blocking_activity(product_id)}

        # Filters out products which do not pass the context filter.

        filtered_products = self.session_context.apply_pos_filter_to_products(list(products_set))
        for algorithm, algorithm_products in products_by_algorithm.items():
            products_by_algorithm[algorithm] = algorithm_products & filtered_products

        return products_by_algorithm

    def _gather_processed_candidate_products(self, n_recommendations):
        """ Process the candidate products list applying filters and blocked products.

            :param n_recommendations: The minimum number of candidates to be returned.
            :returns: A map {algorithm_suffix: list of candidate product ids}.
        """
        if self.session_context.filter_strategy == ctx.BEFORE_SCORING:
            candidate_products_by_algorithm = self._gather_pre_filtered_product_ids()
        else:
            candidate_products_by_algorithm = self._gather_pos_filtered_product_ids(n_recommendations)

        return candidate_products_by_algorithm

    @abc.abstractmethod
    def gather_recommendation_scores(self, candidate_product_ids_by_algorithm, n_recommendations):
        """ Assigns recommendation scores to the products in the database, based on the
            given candidate_product_ids.

            PS.: Each concrete Recommender subclass should pick its
            due set of candidate products by a call to self.pick_candidate_products(candidate_product_ids).
            Such method has the logic to pick the appropriate set of candidates depending on the
            customer-defined strategy: BEFORE_SCORING or AFTER_SCORING.

            :param candidate_product_ids_by_algorithm: A map {algorithm_suffix: set of product_ids to be scored}.
            :param n_recommendations: The number of recommendations to be retrieved.

            :returns: A list of [score, product_id] pairs.
        """
        pass

    def _has_blocking_activity(self, product_id):
        """ Returns whether the target user has consumed (i.e. made a blocking activity with)
            the informed product.

            :param product_id: The id of the intended product.
            :returns: A boolean indicating a previous consumption.
        """
        return product_id in self.session_context.blocked_products
        # TODO use Bloom filter instead

    def _nlargest(self, n_recommendations, scored_recommendations):
        return heapq.nlargest(n_recommendations, scored_recommendations)

    @profile
    def recommend(self, n_recommendations):
        """ Returns the top-scored recommendations for the target user.

            :param n_recommendations: The intended number of recommendations.
        """
        start_time = time()
        log.info("Retrieving {0} recommendations for user [{1}]".format(n_recommendations,
                                                                        self.session_context.user_id))
        # Obtains the candidate products.
        candidate_products_by_algorithm = self._gather_processed_candidate_products(
            max(500, 3 * n_recommendations)
            # Hack to add some slack and make sure we bring enough products to overcome a possible subsequent pruning
            # by history decay, deleted and already consumed products.
        )

        if self.session_context.filter_strategy == ctx.BEFORE_SCORING:
            number_of_recommendations_to_ask_for = min(3 * n_recommendations,
                                                       len(candidate_products_by_algorithm[PRE_FILTER]))
        else:
            number_of_recommendations_to_ask_for = 3 * n_recommendations
        # Here again we leave some slack, so we can post-process and still retain the intended number of products.

        # Scores the products.
        scored_recommendations = self.gather_recommendation_scores(candidate_products_by_algorithm,
                                                                   number_of_recommendations_to_ask_for)
        if log.is_debug_enabled():
            log.debug('full recommendations: [{0}] => [{1}]'.format(len(scored_recommendations),
                                                                    scored_recommendations))
        else:
            log.info('full recommendations: [{0}]'.format(len(scored_recommendations)))

        # Post-processes the scores (boosts, decays, etc.).
        scored_recommendations = self.post_process_scores(scored_recommendations)

        if log.is_debug_enabled():
            log.debug('post-processed recommendations: [{0}] => [{1}]'.format(len(scored_recommendations),
                                                                              scored_recommendations))
        else:
            log.info('post-processed recommendations: [{0}]'.format(len(scored_recommendations)))

        # Makes sure that all pre-filtered products have made their way into the recommendations list.
        if self.session_context.filter_strategy == ctx.BEFORE_SCORING:
            all_candidates = candidate_products_by_algorithm[PRE_FILTER]
            if len(scored_recommendations) < len(all_candidates):
                recommended_products = {p[1] for p in scored_recommendations}
                missing_candidates = list(all_candidates - recommended_products)
                random.shuffle(missing_candidates)
                for missing_candidate in missing_candidates:
                    fill_in_score = ["PRE-FILTER", 0] if self.is_hybrid() else [0]
                    scored_recommendations += [[fill_in_score, missing_candidate]]

        should_worry_about_near_identical = (self.session_context.near_identical_filter_field is not None) and \
                                            (self.session_context.near_identical_filter_threshold is not None)

        # Ranks.
        slack_for_near_identical = 2 if should_worry_about_near_identical else 1
        ranked_recommendations = self._nlargest(slack_for_near_identical * n_recommendations, scored_recommendations)

        if log.is_debug_enabled():
            log.debug('ranked recommendations: [{0}] => [{1}]'.format(len(ranked_recommendations),
                                                                      ranked_recommendations))
        else:
            log.info('ranked recommendations: [{0}]'.format(len(ranked_recommendations)))

        # Identifies near-identical products within a same page and sends them to the end of the list.

        if should_worry_about_near_identical:
            products = [r[1] for r in ranked_recommendations]
            products_to_disregard = pinpoint_near_identical_products(self.session_context, products,
                                                                     self.session_context.product_models)
            result = []
            near_identical = []
            count_recommendations = 0
            for score_and_product in ranked_recommendations:
                product = score_and_product[1]
                if product in products_to_disregard:
                    score = score_and_product[0]
                    new_score = ["NI"] + score  # indicates it was decayed for being 'near-identical'
                    near_identical += [(new_score, product)]
                else:
                    result += [score_and_product]
                    count_recommendations += 1
                    if count_recommendations == n_recommendations:
                        break

            hole = n_recommendations - count_recommendations
            if hole > 0:
                result += near_identical[:hole]

            if log.is_debug_enabled():
                log.debug('recommendations after near-identical filter [count({0})] => [{1}]'.format(
                    len(ranked_recommendations), ranked_recommendations))
            else:
                log.info('recommendations after near-identical filter [count({0})]'.format(
                    len(ranked_recommendations)))
        else:
            result = ranked_recommendations  # There was no need to filter near-identical products...

        log.info("Recommender{0} took [{1:2.6f}] seconds for user [{2}]".format(
            self.get_suffix(), time() - start_time, self.session_context.user_id))

        return result

    def post_process_scores(self, scored_recommendations):
        """ Post-processes the scores of each recommendation candidate by applying appropriate weights based on
            previous consumption, impressions, etc.

            :param scored_recommendations: a list of [score, product_id] pairs
            :returns: another list of [score, product_id] pairs.
        """
        # Determines the index of the first actual value in the score tuples
        # produced by the recommender (note that hybrid recommenders use the first
        # position to indicate the algorithm number)
        if self.is_hybrid():
            start_index = 1
        else:
            start_index = 0

        new_scored_recommendations = []
        for score, product in scored_recommendations:

            # product age decay factor
            product_date = self.session_context.product_models.get(product).get_attribute(
                self.session_context.default_product_date_field)
            product_age_decay_factor = self.session_context.obtain_product_age_decay_factor(product_date)
            if product_age_decay_factor <= 0:
                continue  # should never recommend items with non-positive scores

            # history decay factor
            history_decay_factor = self.session_context.obtain_history_decay_factor(product)
            if history_decay_factor <= 0:
                continue  # should never recommend items with non-positive scores

            # previous consumption factor
            previous_consumption_factor = self.session_context.obtain_previous_consumption_factor(product)
            if previous_consumption_factor <= 0:
                continue  # should never recommend items with non-positive scores

            # in-boost
            in_boost = self.session_context.obtain_in_boost(product)

            new_score = []
            for idx, score_item in enumerate(score):
                if idx < start_index:
                    new_score += [score_item]
                else:
                    new_score += [score_item * product_age_decay_factor * history_decay_factor \
                                             * previous_consumption_factor * in_boost]
            new_scored_recommendations += [[new_score, product]]

        return new_scored_recommendations

    def pick_candidate_products(self, candidate_product_ids):
        """ Selects the appropriate set of pre-fetched candidate products from the given map.
            If the customer-defined filter strategy is BEFORE_SCORING, it picks the single set of
            (pre-filtered) candidates. If the filter strategy is AFTER_SCORING, it picks the set of
            pre-fetched candidates for this algorithm (whose suffix is given by the concrete Recommender subclass).

            :param candidate_product_ids: a map of {algorithm_suffix: set of candidate ids}, where algorithm
                suffix is PRE_FILTER in case the filter strategy is BEFORE_SCORING.

            :return: a set of candidate product ids.
        """
        if self.session_context.filter_strategy == ctx.BEFORE_SCORING:
            candidates = candidate_product_ids.get(PRE_FILTER, set())
        else:
            candidates = candidate_product_ids.get(self.get_suffix(), set())

        return candidates

    def _is_product_valid(self, product_id):
        """ Checks whether a given product passes the context filters.

            :param product_id: The intended product.
            :returns: True, if the product passes the filters; False, otherwise.
        """
        return product_id in self.session_context.filtered_products

    def _can_recommend(self, product_id):
        """ Checks whether a product can be recommended for the target user.

            :param product_id: The intended product.
            :returns: True, if the product has not been logically deleted and has not been consumed by
                the target user; False, otherwise.
        """
        if self._has_blocking_activity(product_id):
            return False  # may not be recommendable if already consumed by target

        return self._is_product_valid(product_id)  # cannot be recommended if logically deleted
