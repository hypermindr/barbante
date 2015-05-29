""" Test module for barbante.maintenance.product_templates when impressions are enabled.
"""

from barbante.maintenance.tests.fixtures.ProductTemplatesFixture import ProductTemplatesFixture
import barbante.tests.dummy_data_populator as dp
import barbante.maintenance.product_templates as pt


class TestProductTemplatesWithImpressions(ProductTemplatesFixture):
    """ Class for testing barbante.maintenance.product_templates when impressions are enabled.
        All tests are run starting from a db already populated with plenty of impressions.
    """

    def __init__(self):
        super().__init__({'impressions_enabled': True})

    def setup(self):
        super().setup()
        dp.populate_impressions(self.session_context)
        pt.generate_templates(self.session_context)
