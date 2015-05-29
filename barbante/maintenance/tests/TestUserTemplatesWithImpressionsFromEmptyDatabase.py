""" Test module for barbante.maintenance.user_templates when impressions are enabled,
    starting from a database with no impressions at all.
"""

from barbante.maintenance.tests.fixtures.UserTemplatesFixture import UserTemplatesFixture
import barbante.maintenance.user_templates as ut


class TestUserTemplatesWithImpressionsFromEmptyDatabase(UserTemplatesFixture):
    """ Class for testing barbante.maintenance.user_templates when impressions are enabled,
        starting from a database with no impressions at all.
    """

    def __init__(self):
        super().__init__({'impressions_enabled': True})

    def setup(self):
        super().setup()
        ut.generate_templates(self.session_context)

    def test_templates(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_user_user_strengths_incremental_new_product_5star(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_user_user_strengths_incremental_new_product_3star(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_user_user_strengths_incremental_old_product_5_to_3star(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_user_user_strengths_incremental_new_product_5_to_3_to_5star(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass

    def test_user_user_strengths_incremental_random(self):
        # With no previous impressions, no templates will be generated in this test --- let's skip it.
        pass
