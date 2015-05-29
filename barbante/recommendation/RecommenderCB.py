""" Recommender CB.
"""

from barbante.recommendation.ProductBasedRecommender import ProductBasedRecommender
import barbante.maintenance.product_templates_tfidf as pttfidf


class RecommenderCB(ProductBasedRecommender):
    """ Recommender based on the similarity of products based on their
        contents.

        Candidate products: all.

        1st criterion: the greatest similarity with a product that was
        recently consumed by the target.
    """

    def __init__(self, session_context):
        super().__init__(session_context)

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "CB"

    def _obtain_all_product_templates(self, products, blocked_products):
        return pttfidf.get_product_templates_tfidf(self.session_context, products, blocked_products)
