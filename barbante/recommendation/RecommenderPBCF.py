""" Recommender PBCF.
"""

from barbante.recommendation.ProductBasedRecommender import ProductBasedRecommender
import barbante.maintenance.product_templates as pt


class RecommenderPBCF(ProductBasedRecommender):
    """ Recommender based on the similarity of products based on their
        contents.

        Candidate products: all.

        1st criterion: the greatest attraction by a product that was
        recently consumed by the target.
    """

    def __init__(self, session_context):
        super().__init__(session_context)

    def get_suffix(self):
        """ See barbante.recommendation.Recommender.
        """
        return "PBCF"

    def _obtain_all_product_templates(self, products, blocked_products):
        return pt.get_product_templates(self.session_context, products, blocked_products)
