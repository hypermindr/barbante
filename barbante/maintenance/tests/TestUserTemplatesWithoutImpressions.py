""" Test module for barbante.maintenance.user_templates when impressions are disabled.
"""

import pytz
import dateutil
import datetime as dt
import random

from barbante.maintenance.tests.fixtures.UserTemplatesFixture import UserTemplatesFixture
import barbante.tests as tests
import barbante.maintenance.user_templates as ut
import barbante.maintenance.tasks as tasks


class TestUserTemplatesWithoutImpressions(UserTemplatesFixture):
    """ Class for testing barbante.maintenance.user_templates when impressions are disabled.
    """

    def __init__(self):
        super().__init__({'impressions_enabled': False})

    def setup(self):
        super().setup()
        ut.generate_templates(self.session_context)

    def test_user_user_strengths_incremental_with_new_impressions_two_new_products(self):
        # This test is meaningless when impressions are not used.
        pass

    def test_user_user_strengths_incremental_with_new_impressions_random(self):
        # This test is meaningless when impressions are not used.
        pass

    def test_user_user_strengths_incremental_random(self):
        """ Tests whether the user x user strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves several random activities in a row,
            checking whether all strengths were correctly updated.
        """
        if not tests.INCLUDE_RANDOM_TESTS:
            return

        all_users = [u for u in self.db_proxy.fetch_all_user_ids()]
        all_products = [p for p in self.db_proxy.fetch_all_product_ids()]

        for i in range(100):
            user = random.choice(all_users)
            product = random.choice(all_products)
            activity_type = random.choice(self.session_context.supported_activities)
            date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 6:00:00")) + dt.timedelta(seconds=2 * i)

            activity = {"external_user_id": user,
                        "external_product_id": product,
                        "activity": activity_type,
                        "created_at": date}
            ut.update_templates(self.session_context, activity)
            tasks.update_summaries(self.session_context, activity)

            self.compare_incremental_vs_from_scratch(
                target_users=[user] if self.session_context.impressions_enabled else None)

