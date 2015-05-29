""" Tests for barbante.maintenance.user_templates.py.
"""

import nose.tools
import pytz
import dateutil.parser
import datetime as dt
import random

import barbante.config as config
from barbante.maintenance.tests.fixtures.MaintenanceFixture import MaintenanceFixture
import barbante.maintenance.user_templates as ut
import barbante.maintenance.tasks as tasks
import barbante.tests.dummy_data_populator as dp
import barbante.tests as tests

import barbante.utils.logging as barbante_logging
log = barbante_logging.get_logger(__name__)


class UserTemplatesFixture(MaintenanceFixture):
    """ Test class for user templates.
    """

    def test_templates(self):
        """ Tests the user templates that are retrieved and saved in the db.
        """
        # Economia
        for i in range(1, dp.N_USR_ECONOMIA + 1):
            target = "u_eco_" + str(i)
            templates = ut.get_user_templates(self.session_context, target)
            template_idx = 0
            for template in templates:
                template_idx += 1
                if template_idx > dp.N_USR_ECONOMIA:
                    break
                nose.tools.eq_(template[1][:6], "u_eco_",
                               "A questionable template order was obtained " +
                               "for user %s: %s" % (target, templates))
            nose.tools.ok_(template_idx > 0, "No templates were generated")

        # Esportes
        for i in range(1, dp.N_USR_ESPORTES + 1):
            target = "u_esp_" + str(i)
            templates = ut.get_user_templates(self.session_context, target)
            template_idx = 0
            for template in templates:
                template_idx += 1
                if template_idx > dp.N_USR_ESPORTES:
                    break
                nose.tools.eq_(template[1][:6], "u_esp_",
                               "A questionable template order was obtained " +
                               "for user %s: %s" % (target, templates))
            nose.tools.ok_(template_idx > 0, "No templates were generated")

        # Música
        for i in range(1, dp.N_USR_MUSICA + 1):
            target = "u_mus_" + str(i)
            templates = ut.get_user_templates(self.session_context, target)
            template_idx = 0
            for template in templates:
                template_idx += 1
                if template_idx > dp.N_USR_MUSICA:
                    break
                nose.tools.eq_(template[1][:6], "u_mus_",
                               "A questionable template order was obtained " +
                               "for user %s: %s" % (target, templates))
            nose.tools.ok_(template_idx > 0, "No templates were generated")

        # Tecnologia
        for i in range(1, dp.N_USR_TECNOLOGIA + 1):
            target = "u_tec_" + str(i)
            templates = ut.get_user_templates(self.session_context, target)
            template_idx = 0
            for template in templates:
                template_idx += 1
                if template_idx > dp.N_USR_TECNOLOGIA:
                    break
                nose.tools.eq_(template[1][:6], "u_tec_",
                               "A questionable template order was obtained " +
                               "for user %s: %s" % (target, templates))
            nose.tools.ok_(template_idx > 0, "No templates were generated")

    def test_user_user_strengths_incremental_new_product_5star(self):
        """ Tests whether the user x user strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a new activity with a 5-star product that had never been consumed by
            the target user and checks whether whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_mus_1"
        activity_type = self.session_context.activities_by_rating[5][0]

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        ut.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch(target_users=[user]
                                                 if self.session_context.impressions_enabled else None)

    def test_user_user_strengths_incremental_new_product_3star(self):
        """ Tests whether the user x user strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a new activity with a 3-star product that had never been consumed by
            the target user and checks whether whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_mus_1"
        activity_type = self.session_context.activities_by_rating[3][0]

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        ut.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch(
            target_users=[user] if self.session_context.impressions_enabled else None)

    def test_user_user_strengths_incremental_old_product_5_to_3star(self):
        """ Tests whether the user x user strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a new 3-star activity with a product that had been consumed before by
            the target user with a 5-star activity, and checks whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_eco_2"
        activity_type = self.session_context.activities_by_rating[3][0]

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        ut.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch(
            target_users=[user] if self.session_context.impressions_enabled else None)

    def test_user_user_strengths_incremental_new_product_5_to_3_to_5star(self):
        """ Tests whether the user x user strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a 3-star activity with a 5-star product that had never been consumed,
            then another activity with the same product (this time a 5-star activity),
            checking whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_mus_1"
        activity_type = self.session_context.activities_by_rating[3][0]
        date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 9:00:00"))

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": date}
        ut.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch(
            target_users=[user] if self.session_context.impressions_enabled else None)

        activity_type = self.session_context.activities_by_rating[5][0]
        date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 9:01:00"))

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": date}
        ut.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch(target_users=[user]
                                                 if self.session_context.impressions_enabled else None)

    def test_user_user_strengths_incremental_with_new_impressions_two_new_products(self):
        """ Tests whether the user x user strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves two new, identical products, with impressions for only one user.
            After activities of a like-minded user have been saved involving those products,
            checks whether all strengths were correctly updated.
        """
        # Saves two new, identical products. Initially, no users will have impressions on them.
        id_twin_product_1 = "p_tec_TWIN_1"
        id_twin_product_2 = "p_tec_TWIN_2"

        date = self.session_context.get_present_date() - dt.timedelta(days=2)

        twin_product_1 = {"external_id": id_twin_product_1,
                          "language": "english",
                          "date": date,
                          "resources": {"title": "Whatever Gets You Through The Night"},
                          "full_content": """Begin. Technology. Technology. This is all we got. End.""",
                          "category": "Nonsense"}

        twin_product_2 = {"external_id": id_twin_product_2,
                          "language": "english",
                          "date": date,
                          "resources": {"title": "Whatever Gets You Through The Night"},
                          "full_content": """Begin. Technology. Technology. This is all we got. End.""",
                          "category": "Nonsense"}

        self.db_proxy.insert_product(twin_product_1)
        self.db_proxy.insert_product(twin_product_2)

        user1 = "u_eco_1"
        user2 = "u_eco_2"
        activity_type = self.session_context.activities_by_rating[5][0]

        # Saves an impression on just one of the new products
        date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 9:00:00"))
        self.db_proxy.increment_impression_summary(user_id=user1, product_id=id_twin_product_1,
                                                   date=date, anonymous=False)

        # Saves a couple of activities for another user using the new products

        activity = {"external_user_id": user2,
                    "external_product_id": id_twin_product_1,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        ut.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch(target_users=[user2]
                                                 if self.session_context.impressions_enabled else None)

        activity = {"external_user_id": user2,
                    "external_product_id": id_twin_product_2,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        ut.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch(
            target_users=[user2] if self.session_context.impressions_enabled else None)

    def test_user_user_strengths_incremental_with_new_impressions_identified_users(self):
        """ Tests whether the user x user strengths generated on a step-by-step basis
            match exactly those created from scratch.
        """
        test_descriptions = [("u_esp_4", "p_nonsense_1", "p_empty_with_missing_category", "p_filter_2", "buy")]

        for idx, (user, product1, product2, product3, activity_type) in enumerate(test_descriptions):
            # Saves a couple of impressions for the chosen user
            date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 6:00:00") + dt.timedelta(seconds=(2 * idx + 1)))
            self.db_proxy.increment_impression_summary(user_id=user, product_id=product1,
                                                       date=date, anonymous=False)
            self.db_proxy.increment_impression_summary(user_id=user, product_id=product2,
                                                       date=date, anonymous=False)

            ut.generate_templates(self.session_context)
            # it is important to regenerate from scratch (with all new impressions)

            # Saves one activity for that same user
            date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 6:00:00") + dt.timedelta(seconds=(2 * idx + 2)))

            activity = {"external_user_id": user,
                        "external_product_id": product3,
                        "activity": activity_type,
                        "created_at": date}

            ut.update_templates(self.session_context, activity)
            tasks.update_summaries(self.session_context, activity)

            self.compare_incremental_vs_from_scratch(
                target_users=[user] if self.session_context.impressions_enabled else None)

    def test_user_user_strengths_incremental_with_new_impressions_random(self):
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
            is_anonymous = config.is_anonymous(user)

            print("user: %s" % user)

            # Saves a couple of impressions for the chosen user
            date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 6:00:00")) + dt.timedelta(seconds=2 * i)
            product1 = random.choice(all_products)
            product2 = random.choice(all_products)
            self.db_proxy.increment_impression_summary(user_id=user, product_id=product1,
                                                       date=date, anonymous=is_anonymous)
            self.db_proxy.increment_impression_summary(user_id=user, product_id=product2,
                                                       date=date, anonymous=is_anonymous)

            print("impressions --> %s, %s" % (product1, product2))

            ut.generate_templates(self.session_context)
            # it is important to regenerate from scratch (with all new impressions)

            # Saves one activity for that same user
            product3 = random.choice(all_products)
            activity_type = random.choice(self.session_context.supported_activities)
            date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 6:00:00")) + dt.timedelta(seconds=2 * i + 1)

            activity = {"external_user_id": user,
                        "external_product_id": product3,
                        "activity": activity_type,
                        "created_at": date}

            print("activity --> " + str(activity))

            ut.update_templates(self.session_context, activity)
            tasks.update_summaries(self.session_context, activity)

            self.compare_incremental_vs_from_scratch(
                target_users=[user] if self.session_context.impressions_enabled else None)

    @nose.tools.nottest
    def compare_incremental_vs_from_scratch(self, target_users=None):
        """ Helper method to compare strengths generated incrementally vs from-scratch.
            When using impressions we only care about target users whose activities triggered the updates.

            :param target_users: If not None, then only target users informed in this set will be considered.
        """
        def compare_strengths(incremental, from_scratch, pair_of_users):
            strength1 = incremental[pair_of_users]
            strength2 = from_scratch[pair_of_users]
            nose.tools.ok_(abs(strength1 - strength2) < 0.00001,
                           "Strengths do not match for " + str(pair_of_users) + ": " +
                           "[incremental --> %.6f] [from scratch --> %.6f]"
                           % (strength1, strength2))

        def compare_templates(incremental, from_scratch, user):
            templates1 = incremental[user]
            templates2 = from_scratch[user]
            nose.tools.eq_(templates1, templates2,
                           "Templates do not match for " + str(user) + ": " +
                           "[incremental --> %s] [from scratch --> %s]"
                           % (str(templates1), str(templates2)))

        users = target_users if target_users is not None else list(self.db_proxy.fetch_all_user_ids())

        # saves locally the strengths and the templates that were obtained incrementally
        strengths_incremental = self.db_proxy.fetch_user_user_strengths()
        templates_incremental = self.db_proxy.fetch_user_templates(users)

        # regenerates all strengths from scratch
        ut.generate_templates(self.session_context)

        # saves locally the strengths and the templates that were obtained from scratch
        strengths_from_scratch = self.db_proxy.fetch_user_user_strengths()
        templates_from_scratch = self.db_proxy.fetch_user_templates(users)

        for user_pair in strengths_from_scratch:
            if target_users is not None:
                if user_pair[0] not in target_users:
                    continue
                    # The incremental (on-the-fly) updates take care of "user as target" strengths only.
                    # This is ok, since "user as template" updates will be triggered indirectly
                    # (by other target users).
            compare_strengths(strengths_incremental, strengths_from_scratch, user_pair)
            compare_templates(templates_incremental, templates_from_scratch, user_pair[0])

        for user_pair in strengths_incremental:
            if target_users is not None:
                if user_pair[0] not in target_users:
                    continue
                    # Idem.
            compare_strengths(strengths_incremental, strengths_from_scratch, user_pair)
            compare_templates(templates_incremental, templates_from_scratch, user_pair[0])
