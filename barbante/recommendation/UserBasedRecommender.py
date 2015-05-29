""" UserBasedRecommender.
"""

import abc

import barbante.config as config
from barbante.recommendation.Recommender import Recommender


class UserBasedRecommender(Recommender):
    """ Recommender based on user similarity.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, session_context):
        super().__init__(session_context)

    @abc.abstractmethod
    def get_suffix(self):
        pass

    def is_hybrid(self):
        """ See barbante.recommendation.Recommender.
        """
        return False

    def get_out_boost_for_product(self, template_user, product):
        result = 1
        template_activities_by_product = self.session_context.recent_activities_by_product_by_template_user.get(
            template_user, {})
        product_activity = template_activities_by_product.get(product)
        if product_activity is not None:
            activity_type = product_activity[1]
            result = self.session_context.out_boost_by_activity.get(activity_type, 1)
        return result

    def gather_candidate_products(self, n_recommendations):
        product_ids_set = set()
        if not config.is_anonymous(self.session_context.user_id):
            for strength, template_id in self.session_context.user_templates:
                template_user_activities = self.session_context.recent_activities_by_template_user.get(template_id, {})
                product_ids = {act["external_product_id"] for act in template_user_activities}
                product_ids_set |= product_ids
        return {self.get_suffix(): product_ids_set}

    @abc.abstractmethod
    def calculate_score(self, strength, product_id, template_id):
        """ Obtains the strength associated to a (user, product) pair.

            :param strength: The strength associated to that user template.
            :param product_id: The product id.
            :param template_id:  The template user id.

            :returns: A score.
        """
        pass

    def gather_recommendation_scores(self, candidate_product_ids_by_algorithm, n_recommendations):
        scores_by_recommendation_candidate = {}

        if not config.is_anonymous(self.session_context.user_id):
            candidates = self.pick_candidate_products(candidate_product_ids_by_algorithm)

            for strength, template_id in self.session_context.user_templates:
                template_user_activities = self.session_context.recent_activities_by_template_user.get(template_id, {})
                products = {act["external_product_id"] for act in template_user_activities
                            if self.session_context.rating_by_activity[act["activity"]] >=
                            self.session_context.min_rating_recommendable_from_user}
                for product_id in products:
                    if product_id in candidates:
                        score = scores_by_recommendation_candidate.get(product_id, [[0.0], product_id])
                        score_increment = self.calculate_score(strength, product_id, template_id)
                        if score_increment != 0:
                            score[0][0] += score_increment
                            scores_by_recommendation_candidate[product_id] = score

        return scores_by_recommendation_candidate.values()

