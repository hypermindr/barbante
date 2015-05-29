""" Test module for barbante.maintenance.product_templates when impressions are enabled,
    starting from a database with no impressions at all.
"""

from barbante.maintenance.tests.fixtures.ProductTemplatesFixture import ProductTemplatesFixture
import barbante.maintenance.product_templates as pt


class TestProductTemplatesWithImpressionsFromEmptyDatabase(ProductTemplatesFixture):
    """ Class for testing barbante.maintenance.product_templates when impressions are enabled,
        starting from a database with no impressions at all.
    """

    def __init__(self):
        super().__init__({'impressions_enabled': True})

    def setup(self):
        super().setup()
        pt.generate_templates(self.session_context)

    def test_templates(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_product_product_strengths_incremental_new_product_5star(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_product_product_strengths_incremental_new_product_3star(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_product_product_strengths_incremental_old_product_5_to_3star(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_product_product_strengths_incremental_new_product_5_to_3_to_5star(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_product_product_strengths_incremental_random(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

