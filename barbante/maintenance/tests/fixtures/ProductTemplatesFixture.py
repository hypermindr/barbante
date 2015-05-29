""" Tests for barbante.maintenance.product_templates.py.
"""

import nose.tools
import pytz
import datetime as dt
import dateutil.parser
import random

import barbante.config as config
import barbante.maintenance.product_templates as pt
from barbante.maintenance.tests.fixtures.MaintenanceFixture import MaintenanceFixture
import barbante.maintenance.tasks as tasks
import barbante.tests.dummy_data_populator as dp
import barbante.tests as tests


class ProductTemplatesFixture(MaintenanceFixture):
    """ Test class for product templates.
    """

    def test_templates(self):
        """ Tests the product templates that are computed and saved in the db.
        """
        # Economia
        for i in range(1, dp.N_PROD_ECONOMIA + 1):
            target = "p_eco_" + str(i)
            templates = pt.get_product_templates(self.session_context, [target]).get(target, {})
            template_idx = 0
            for template in templates:
                template_idx += 1
                nose.tools.ok_("expired" not in template[1], "An expired template was obtained")
                if template_idx <= dp.N_PROD_ECONOMIA + dp.N_PROD_SUPERPOPULAR - 1:
                    nose.tools.eq_(template[1][:6], "p_eco_",
                                   "A questionable template order was obtained " +
                                   "for product %s: %s" % (target, templates))
            nose.tools.ok_(template_idx > 0, "No templates were generated")

        # Esportes
        for i in range(1, dp.N_PROD_ESPORTES + 1):
            target = "p_esp_" + str(i)
            templates = pt.get_product_templates(self.session_context, [target]).get(target, {})
            template_idx = 0
            for template in templates:
                template_idx += 1
                nose.tools.ok_("expired" not in template[1], "An expired template was obtained")
                if template_idx <= dp.N_PROD_ESPORTES + dp.N_PROD_SUPERPOPULAR - 1:
                    nose.tools.eq_(template[1][:6], "p_esp_",
                                   "A questionable template order was obtained " +
                                   "for product %s: %s" % (target, templates))
            nose.tools.ok_(template_idx > 0, "No templates were generated")

        # Musica
        for i in range(1, dp.N_PROD_MUSICA + 1):
            target = "p_mus_" + str(i)
            templates = pt.get_product_templates(self.session_context, [target]).get(target, {})
            template_idx = 0
            for template in templates:
                template_idx += 1
                nose.tools.ok_("expired" not in template[1], "An expired template was obtained")
                if template_idx <= dp.N_PROD_MUSICA + dp.N_PROD_SUPERPOPULAR - 1:
                    nose.tools.eq_(template[1][:6], "p_mus_",
                                   "A questionable template order was obtained " +
                                   "for product %s: %s" % (target, templates))
            nose.tools.ok_(template_idx > 0, "No templates were generated")

        # Tecnologia
        for i in range(1, dp.N_PROD_TECNOLOGIA + 1):
            target = "p_tec_" + str(i)
            templates = pt.get_product_templates(self.session_context, [target]).get(target, {})
            template_idx = 0
            for template in templates:
                template_idx += 1
                nose.tools.ok_("expired" not in template[1], "An expired template was obtained")
                if template_idx <= dp.N_PROD_TECNOLOGIA + dp.N_PROD_SUPERPOPULAR - 1:
                    nose.tools.eq_(template[1][:6], "p_tec_",
                                   "A questionable template order was obtained " +
                                   "for product %s: %s" % (target, templates))
            nose.tools.ok_(template_idx > 0, "No templates were generated")

    def test_product_product_strengths_incremental_new_user_5star(self):
        """ Tests whether the product x product strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a new activity with a 5-star product that had never been consumed by
            the user and checks whether whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_mus_1"
        activity_type = self.session_context.activities_by_rating[5][0]

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch()

    def test_product_product_strengths_incremental_new_user_3star(self):
        """ Tests whether the product x product strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a new activity with a 3-star product that had never been consumed by
            the user and checks whether whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_mus_1"
        activity_type = self.session_context.activities_by_rating[3][0]

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch()

    def test_product_product_strengths_incremental_new_user_2star(self):
        """ Tests whether the product x product strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a new activity with a 2-star product that had never been consumed by
            the user and checks whether whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_mus_1"
        activity_type = self.session_context.activities_by_rating[2][0]

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch()

    def test_product_product_strengths_incremental_old_user_5_to_3star(self):
        """ Tests whether the product x product strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a new 3-star activity for a product consumed before by the user
            with a 5-star activity, and checks whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_eco_2"
        activity_type = self.session_context.activities_by_rating[3][0]

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch()

    def test_product_product_strengths_incremental_old_user_5_to_2star(self):
        """ Tests whether the product x product strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a new 2-star activity for a product consumed before by the user
            with a 5-star activity, and checks whether all strengths were correctly updated.
        """
        user = "u_eco_1"
        product = "p_eco_2"
        activity_type = self.session_context.activities_by_rating[2][0]

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch()

    def test_product_product_strengths_incremental_new_user_5_to_2_to_5star(self):
        """ Tests whether the product x product strengths generated on a step-by-step basis
            match exactly those created from scratch.
            This test saves a 2-star activity with a 5-star product that had never been consumed,
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
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        activity_type = self.session_context.activities_by_rating[5][0]
        date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 9:01:00"))

        activity = {"external_user_id": user,
                    "external_product_id": product,
                    "activity": activity_type,
                    "created_at": date}
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch()

    def test_product_product_strengths_incremental_with_new_impressions_two_new_products(self):
        """ Tests whether the product x product strengths generated on a step-by-step basis
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
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch()

        activity = {"external_user_id": user2,
                    "external_product_id": id_twin_product_2,
                    "activity": activity_type,
                    "created_at": self.session_context.get_present_date()}
        pt.update_templates(self.session_context, activity)
        tasks.update_summaries(self.session_context, activity)

        self.compare_incremental_vs_from_scratch()

    def test_product_product_strengths_incremental_with_new_impressions_random(self):
        """ Tests whether the product x product strengths generated on a step-by-step basis
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

            # Saves a couple of impressions for the chosen user
            date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 6:00:00")) + dt.timedelta(seconds=2 * i)
            product1 = random.choice(all_products)
            product2 = random.choice(all_products)
            self.db_proxy.increment_impression_summary(user_id=user, product_id=product1,
                                                       date=date, anonymous=is_anonymous)
            self.db_proxy.increment_impression_summary(user_id=user, product_id=product2,
                                                       date=date, anonymous=is_anonymous)

            pt.generate_templates(self.session_context)
            # it is important to regenerate from scratch (with all new impressions)

            # Saves one activity for that same user
            product3 = random.choice(all_products)
            activity_type = random.choice(self.session_context.supported_activities)
            date = pytz.utc.localize(dateutil.parser.parse("1988-11-06 6:00:00")) + dt.timedelta(seconds=2 * i + 1)

            activity = {"external_user_id": user,
                        "external_product_id": product3,
                        "activity": activity_type,
                        "created_at": date}
            pt.update_templates(self.session_context, activity)
            tasks.update_summaries(self.session_context, activity)

            self.compare_incremental_vs_from_scratch()

    @nose.tools.nottest
    def compare_incremental_vs_from_scratch(self):
        """ Helper method to compare strengths generated incrementally vs from-scratch.
        """
        def compare_strengths(incremental, from_scratch, pair_of_products):
            strength1 = incremental[pair_of_products]
            strength2 = from_scratch[pair_of_products]
            nose.tools.ok_(abs(strength1 - strength2) < 0.00001,
                           "Strengths do not match " + str(pair_of_products) + ": " +
                           "[incremental --> %.6f] [from scratch --> %.6f]"
                           % (strength1, strength2))

        def compare_templates(incremental, from_scratch, product):
            templates1 = incremental[product][0]
            templates2 = from_scratch[product][0]
            nose.tools.eq_(templates1, templates2,
                           "Templates do not match for " + str(product) + ": " +
                           "[incremental --> %s] [from scratch --> %s]"
                           % (str(templates1), str(templates2)))

        products = list(self.db_proxy.fetch_all_product_ids())

        # saves locally the strengths and the templates that were obtained incrementally
        strengths_incremental = self.db_proxy.fetch_product_product_strengths()
        templates_incremental = self.db_proxy.fetch_product_templates(products)

        # regenerates all strengths from scratch
        pt.generate_templates(self.session_context)

        # saves locally the strengths obtained from scratch
        strengths_from_scratch = self.db_proxy.fetch_product_product_strengths()
        templates_from_scratch = self.db_proxy.fetch_product_templates(products)

        for product_pair in strengths_from_scratch:
            compare_strengths(strengths_incremental, strengths_from_scratch, product_pair)
            compare_templates(templates_incremental, templates_from_scratch, product_pair[0])

        for product_pair in strengths_incremental:
            compare_strengths(strengths_incremental, strengths_from_scratch, product_pair)
            compare_templates(templates_incremental, templates_from_scratch, product_pair[0])
