""" Recommender UBCF.
"""

from barbante.recommendation.UserBasedRecommender import UserBasedRecommender


class RecommenderUBCF(UserBasedRecommender):
    """ Assigns scores to the candidate recommendations based on the strengths
        of the user templates who have consumed the same products.

        Candidate products: only those recently consumed by the target's
        user templates.

        1st criterion: the greatest sum of the strengths of the templates
        who recently consumed that product.
    """

    def __init__(self, session_context):
        super().__init__(session_context)

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "UBCF"

    def calculate_score(self, strength, product_id, template_id):
        return strength * self.get_out_boost_for_product(template_id, product_id)
