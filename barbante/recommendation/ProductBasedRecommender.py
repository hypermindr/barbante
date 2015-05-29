""" ProductBasedRecommender.
"""

import abc

from barbante.recommendation.Recommender import Recommender
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


class ProductBasedRecommender(Recommender):
    """ Recommender based on the similarity of products inferred from user
        activities.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, session_context):
        super().__init__(session_context)

        self.base_products = None
        """ A list of base product ids, in descending order of consumption date.
        """
        self.strengths_and_templates_by_product = None
        """ A dict {product_id: list of [score_tuple, template_id] pairs}.
        """

    @abc.abstractmethod
    def get_suffix(self):
        pass

    def is_hybrid(self):
        return False

    def _load_product_templates(self):
        if self.base_products is None:
            self.base_products = self.session_context.most_recently_consumed_products[:(
                3 * self.session_context.base_products_count)]  # we leave a certain slack here

        if self.strengths_and_templates_by_product is None:
            self.strengths_and_templates_by_product = self._obtain_all_product_templates(
                self.base_products, list(self.session_context.blocked_products))

    def gather_candidate_products(self, n_recommendations):
        """ For each article in the top k recently consumed products,
            fetches the most similar products and adds to a list of candidates
            (if not already consumed by the target user).
        """
        self._load_product_templates()

        candidate_product_ids_set = set()

        if self.strengths_and_templates_by_product is not None:
            for idx, base_product in enumerate(self.strengths_and_templates_by_product):
                product_templates_with_scores = self.strengths_and_templates_by_product[base_product]
                product_ids = {ptws[1] for ptws in product_templates_with_scores}
                candidate_product_ids_set |= product_ids
                if len(candidate_product_ids_set) >= n_recommendations and \
                        idx >= self.session_context.base_products_count:
                    break  # we have already considered the (customer-defined) minimum number of recent products
                           # and we have already got the intended number of recommendations --- we may call it a day

        return {self.get_suffix(): candidate_product_ids_set}

    def gather_recommendation_scores(self, candidate_product_ids_by_algorithm, n_recommendations):
        scores_by_candidate = {}
        candidates = self.pick_candidate_products(candidate_product_ids_by_algorithm)
        self._load_product_templates()

        used_base_products = 0
        total_base_products_available = len(self.base_products)

        for idx, base_product in enumerate(self.base_products):
            base_product_factor = 20 if idx <= self.session_context.base_products_count else 10
            # this is to make sure that the templates of the desired number of recently consumed products
            # will be upfront, and only then the remaining templates will appear

            strengths_and_templates = self.strengths_and_templates_by_product.get(base_product)
            if strengths_and_templates:
                used_base_products += 1
                used_templates_of_this_product = 0
                for template_strength, product_template in strengths_and_templates:
                    used_templates_of_this_product += 1
                    if product_template in candidates:
                        score = [[base_product_factor * n_recommendations - used_templates_of_this_product + 1,
                                  total_base_products_available - used_base_products + 1,
                                  template_strength], product_template]
                        should_include_product = True
                        if product_template in scores_by_candidate:
                            # we update the score if the current base product ranks it higher
                            should_include_product = score[0] > scores_by_candidate[product_template][0]
                        if should_include_product:
                            scores_by_candidate[product_template] = score

        log.debug("Number of base products which actually contributed = %d" % used_base_products)

        return scores_by_candidate.values()

    @abc.abstractmethod
    def _obtain_all_product_templates(self, products, blocked_products):
        """ Retrieves the correct set of pre-calculated product templates.
            Each subclass must define the intended set of templates it cares
            about.

            :param products: A list with the ids of the intended products.
            :param blocked_products: A list of products which should *not* be retrieved (they will not
                be recommended anyway).

            :returns A map {product_id: list of (*strength*, *template_id*) tuples}.
        """
        pass
