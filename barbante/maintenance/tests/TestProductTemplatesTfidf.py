""" Tests for barbante.maintenance.product_templates_tfidf.py.
"""

import nose.tools
import datetime as dt

import barbante.maintenance.product_templates_tfidf as pttfidf
import barbante.maintenance.tasks as maintenance
from barbante.maintenance.tests.fixtures.MaintenanceFixture import MaintenanceFixture
import barbante.tests.dummy_data_populator as dp
import barbante.tests as tests


ACCEPTABLE_ON_THE_FLY_VS_FROM_SCRATCH_DEVIATION = 0.05
""" Since the DF of terms is dynamically updated, and we do not want to cope with
    a strength update for each and every pair of products affected by a tiny modification
    of some term's DF, we allow for a subtle variation between on-the-fly and from-scratch
    values, by design.
"""


class TestProductTemplatesTfidf(MaintenanceFixture):
    """ Test class for product templates based on tfidf similarity.
    """

    def setup(self):
        super().setup()
        pttfidf.generate_templates(self.session_context)

    def _find_language(self, product_id):
        product = self.session_context.data_proxy.fetch_products([product_id], ["language"]).get(product_id)
        if product is None:
            raise AttributeError("invalid product_id")
        return product.get("language", "unknown")

    def test_templates(self):
        """ Tests the product templates (tfidf) that are computed and saved in the db.
        """
        # Economia
        for i in range(1, dp.N_PROD_ECONOMIA + 1):
            target = "p_eco_" + str(i)
            templates = pttfidf.get_product_templates_tfidf(self.session_context, [target]).get(target, {})

            language = self._find_language(target)
            nose.tools.ok_(language not in [None, "unknown"], "Could not retrieve the language correctly")
            if language == "portuguese":
                nose.tools.ok_(len(templates) > 0,
                               "No templates were generated for product " + target)
                nose.tools.eq_(templates[0][1][:6], "p_eco_",
                               "A questionable template order was obtained " +
                               "for product %s: %s" % (target, templates))
                for _, template_id in templates:
                    nose.tools.ok_("expired" not in template_id, "An expired template was obtained")

        # Esportes
        for i in range(1, dp.N_PROD_ESPORTES + 1):
            target = "p_esp_" + str(i)
            templates = pttfidf.get_product_templates_tfidf(self.session_context, [target]).get(target, {})

            language = self._find_language(target)
            nose.tools.ok_(language not in [None, "unknown"], "Could not retrieve the language correctly")
            if language == "portuguese":
                nose.tools.ok_(len(templates) > 0,
                               "No templates were generated for product " + target)
                nose.tools.eq_(templates[0][1][:6], "p_esp_",
                               "A questionable template order was obtained " +
                               "for product %s: %s" % (target, templates))
                for _, template_id in templates:
                    nose.tools.ok_("expired" not in template_id, "An expired template was obtained")

        # Musica
        for i in range(1, dp.N_PROD_MUSICA + 1):
            target = "p_mus_" + str(i)
            templates = pttfidf.get_product_templates_tfidf(self.session_context, [target]).get(target, {})

            language = self._find_language(target)
            nose.tools.ok_(language not in [None, "unknown"], "Could not retrieve the language correctly")
            if language == "portuguese":
                nose.tools.ok_(len(templates) > 0,
                               "No templates were generated for product " + target)
                nose.tools.eq_(templates[0][1][:6], "p_mus_",
                               "A questionable template order was obtained " +
                               "for product %s: %s" % (target, templates))


        # Tecnologia
        for i in range(1, dp.N_PROD_TECNOLOGIA + 1):
            target = "p_tec_" + str(i)
            templates = pttfidf.get_product_templates_tfidf(self.session_context, [target]).get(target, {})

            language = self._find_language(target)
            nose.tools.ok_(language not in [None, "unknown"], "Could not retrieve the language correctly")
            if language == "portuguese":
                nose.tools.ok_(len(templates) > 0,
                               "No templates were generated for product " + target)
                nose.tools.eq_(templates[0][1][:6], "p_tec_",
                               "A questionable template order was obtained " +
                               "for product %s: %s" % (target, templates))

    def test_templates_avoiding_almost_identical_products(self):
        """ Tests whether nearly identical products are NOT templates of one another.
        """
        for i in range(1, dp.N_PROD_NONSENSE):
            target = "p_nonsense_" + str(i)
            templates = pttfidf.get_product_templates_tfidf(self.session_context, [target]).get(target, {})

            nose.tools.ok_(len(templates) > 0,
                           "No templates were generated for product " + target)
            template_products = [t[1] for t in templates]
            way_too_similar = "p_nonsense_" + str(i + 1)
            nose.tools.ok_(way_too_similar not in template_products,
                           "Nearly identical templates!")

    def test_product_product_strengths_tfidf_from_scratch_versus_incremental(self):
        """ Tests whether the product x product strengths (TFIDF) generated on a step-by-step basis
            match exactly those created from scratch.
        """
        # inner method to compare strengths
        def compare_strengths(pair_of_products):
            strength1 = strengths_incremental.get(pair_of_products, 0.0)
            strength2 = strengths_from_scratch[pair_of_products]
            nose.tools.ok_(
                "Strengths do not match for product pair (%s, %s): " % (pair_of_products[0], pair_of_products[1]) +
                "[incremental --> %.6f] [from scratch --> %.6f]" % (strength1, strength2),
                abs(strength1 - strength2) < ACCEPTABLE_ON_THE_FLY_VS_FROM_SCRATCH_DEVIATION)
        # ---

        # inner method to compare templates tfidf
        def compare_templates(product):
            templates1 = templates_incremental.get(product, (None, []))
            templates2 = templates_from_scratch.get(product, (None, []))
            nose.tools.eq_(len(templates1[1]), len(templates2[1]),
                           "Numbers of incremental and from-scratch templates do not match")
            for idx in range(len(templates1[1])):
                strength_incremental = templates1[1][idx][0]
                strength_from_scratch = templates2[1][idx][0]
                nose.tools.ok_(
                    abs(strength_incremental - strength_from_scratch) < ACCEPTABLE_ON_THE_FLY_VS_FROM_SCRATCH_DEVIATION,
                    "Templates do not approximately match for product %s: " % product +
                    "[incremental --> %s] [from scratch --> %s]" % (str(templates1), str(templates2)))
        # ---

        all_products = list(self.db_proxy.fetch_all_product_ids())

        sentence = " produto para teste de atualização de similaridade via tfidf"
        products = [{"external_id": product[0],
                     "resources": {"title": product[0]},
                     "date": self.session_context.get_present_date(),
                     "expiration_date": self.session_context.get_present_date() + dt.timedelta(days=30),
                     "full_content": product[1],
                     "language": "portuguese"} for product in
                    [("p_new_1", "Primeiro" + sentence),
                     ("p_new_2", "Segundo" + sentence),
                     ("p_new_3", "Terceiro" + sentence),
                     ("p_new_4", "Quarto" + sentence)]]

        # updates strengths after each new product
        for product in products:
            self.db_proxy.insert_product(product)
            maintenance.process_product(self.session_context, product["external_id"])

        # saves locally the strengths and the templates that were obtained incrementally
        strengths_incremental = self.db_proxy.fetch_product_product_strengths_tfidf()
        templates_incremental = self.db_proxy.fetch_product_templates(all_products)

        # regenerates all strengths from scratch
        pttfidf.generate_templates(self.session_context)

        # saves locally the strengths and the templates that were obtained from scratch
        strengths_from_scratch = self.db_proxy.fetch_product_product_strengths_tfidf()
        templates_from_scratch = self.db_proxy.fetch_product_templates(all_products)

        nose.tools.eq_(len(strengths_incremental), len(strengths_from_scratch),
                       "Number of non-zero strengths tfidf do not match")

        for product_pair in strengths_from_scratch:
            compare_strengths(product_pair)

        for product_pair in strengths_incremental:
            compare_strengths(product_pair)

        for product in all_products:
            compare_templates(product)

    def test_multi_attribute_similarity(self):
        """ Tests whether the product-product similarities respect the customer-defined weights and filters.

            WARNING: This test relies heavily on the attributes' weight distribution below:
                     - full_content: 0.6    (TEXT - non-persistent)
                     - resources.title: 0.1 (TEXT - persistent)
                     - category: 0.1        (FIXED)
                     - source: 0.1          (LIST)
                     - price: 0.1           (NUMERIC)
                     The 'language' attribute is the only attribute configured to be used as filter.
                     Changing these settings in config/barbante_UnitTest.yml is a sure way to break the present test.
        """
        product = "p_empty"
        strengths = self.db_proxy.fetch_product_product_strengths_tfidf()

        other_product = "p_empty_with_disjoint_title"
        nose.tools.ok_(abs(strengths[(product, other_product)] - 0.9) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (product, other_product))

        other_product = "p_empty_with_two_thirds_of_source_list"
        nose.tools.ok_(abs(strengths[(product, other_product)] - (0.9 + 0.1 * 2/3)) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (product, other_product))
        nose.tools.ok_(abs(strengths[(other_product, product)] - 1) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (other_product, product))

        other_product = "p_empty_with_different_language"
        nose.tools.ok_((product, other_product) not in strengths,
                       "Wrong similarity (%s, %s)" % (product, other_product))

        other_product = "p_empty_with_different_category"
        nose.tools.ok_(abs(strengths[(product, other_product)] - 0.9) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (product, other_product))

        other_product = "p_empty_with_missing_category"
        nose.tools.ok_(abs(strengths[(product, other_product)] - 0.9) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (product, other_product))

        other_product = "p_empty_with_half_price"
        nose.tools.ok_(abs(strengths[(product, other_product)] - 0.95) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (product, other_product))
        nose.tools.ok_(abs(strengths[(other_product, product)] - 0.95) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (other_product, product))

        # TODO (Vinicius) We are missing a test using a date-type attribute here

        other_product = "p_empty_with_many_differences"
        nose.tools.ok_(abs(strengths[(product, other_product)] - (0.6 + 0.1 * 2/3 + 0.1 * 0.5)) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (product, other_product))
        nose.tools.ok_(abs(strengths[(other_product, product)] - (0.7 + 0.1 * 0.5)) < tests.FLOAT_DELTA,
                       "Wrong similarity (%s, %s)" % (other_product, product))
