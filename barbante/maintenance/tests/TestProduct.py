""" Tests for barbante.maintenance.product.py.
"""

import nose.tools
import datetime as dt

import barbante.maintenance.tasks as maintenance
from barbante.maintenance.tests.fixtures.MaintenanceFixture import MaintenanceFixture
import barbante.tests as tests


class TestProduct(MaintenanceFixture):
    """ Test class for product templates.
    """

    def __init__(self):
        super().__init__()
        self.text_field = "full_content"

    def test_tf_all_documents(self):
        """ Tests the tf of the terms in a document after processing documents in bulk.
        """
        product = "p_aut_1"
        tf_map = self.db_proxy.fetch_tf_map(self.text_field, [product]).get(product)
        nose.tools.eq_(tf_map["civic"], 2)
        nose.tools.eq_(tf_map["coroll"], 2)
        nose.tools.eq_(tf_map["merc"], 2)
        nose.tools.eq_(tf_map["consum"], 1)

    def test_tf_repeated_calls(self):
        """ Tests the tf of the terms in a document after processing the document more than once.
        """
        product = "p_aut_1"

        maintenance.process_product(self.session_context, product)
        maintenance.process_product(self.session_context, product)

        tf_map = self.db_proxy.fetch_tf_map(self.text_field, [product]).get(product)
        nose.tools.eq_(tf_map["civic"], 2)
        nose.tools.eq_(tf_map["coroll"], 2)
        nose.tools.eq_(tf_map["merc"], 2)
        nose.tools.eq_(tf_map["consum"], 1)

    def test_df_all_documents(self):
        """ Tests the df of (language, term) pairs after processing documents in bulk.
        """
        nose.tools.eq_(self.db_proxy.find_df("portuguese", "rock"), 4)
        nose.tools.eq_(self.db_proxy.find_df("english", "rock"), 1)
        nose.tools.eq_(self.db_proxy.find_df("english", "merc"), 0)

    def test_df_repeated_calls(self):
        """ Tests the df of (language, term) pairs after processing a same document several times.
        """
        maintenance.process_product(self.session_context, "p_mus_4")
        maintenance.process_product(self.session_context, "p_mus_4")

        nose.tools.eq_(self.db_proxy.find_df("portuguese", "rock"), 4)
        nose.tools.eq_(self.db_proxy.find_df("english", "rock"), 1)
        nose.tools.eq_(self.db_proxy.find_df("english", "merc"), 0)

    def test_tfidf_all_documents(self):
        """ Tests the df of (language, term) pairs after processing documents in bulk.
        """
        tfidf_by_term = self.db_proxy.fetch_tfidf_map(self.text_field, ["p_mus_4"]).get("p_mus_4", {})
        nose.tools.ok_(abs(tfidf_by_term.get("músic") - 1) < tests.FLOAT_DELTA)

    def test_tfidf_repeated_calls(self):
        """ Tests the df of (language, term) pairs after processing a same document several times.
        """
        maintenance.process_products(self.session_context)
        maintenance.process_product(self.session_context, "p_mus_4")
        maintenance.process_product(self.session_context, "p_mus_4")

        tfidf_by_term = self.db_proxy.fetch_tfidf_map(self.text_field, ["p_mus_4"]).get("p_mus_4", {})
        nose.tools.ok_(abs(tfidf_by_term.get("músic") - 1) < tests.FLOAT_DELTA)

    def test_missing_required_field_shouldnt_return_when_defined(self):
        required_fields = self.session_context.product_model_factory.get_custom_required_attributes()
        product_id = "p_mus_U"
        product = {"external_id": product_id,
                   "date": self.session_context.get_present_date(),
                   "resources": {"language": "english",
                                 "full_content": "Obladi-Oblada, life goes on bra, lala how the life goes on..."}}
        self.db_proxy.insert_product(product)
        product_ids = self.session_context.data_proxy.fetch_all_product_ids(allow_deleted=True,
                                                                            required_fields=required_fields)
        nose.tools.ok_(product_id not in product_ids,
                       "Products with missing required fields should not be returned")

    def test_missing_required_field_should_return_when_undefined(self):
        product_id = "p_mus_V"
        product = {"external_id": product_id,
                   "resources": {"language": "english"},
                   "date": self.session_context.get_present_date(),
                   "full_content": "Obladi-Oblada, life goes on bra, lala how the life goes on..."}
        self.db_proxy.insert_product(product)
        product_ids = self.session_context.data_proxy.fetch_all_product_ids(allow_deleted=True)
        nose.tools.ok_(product_id in product_ids,
                       "Products with missing required fields should be returned when no required fields are specified")

    def test_required_field_with_null_shouldnt_return_when_defined(self):
        required_fields = self.session_context.product_model_factory.get_custom_required_attributes()
        product_id = "p_mus_W"
        product = {"external_id": product_id,
                   "resources": {"title": None},
                   "date": self.session_context.get_present_date(),
                   "full_content": "Obladi-Oblada, life goes on bra, lala how the life goes on..."}
        self.db_proxy.insert_product(product)
        product_ids = self.session_context.data_proxy.fetch_all_product_ids(allow_deleted=True,
                                                                            required_fields=required_fields)
        nose.tools.ok_(product_id not in product_ids,
                       "Product with null required fields should not be returned")

    def test_required_field_with_null_should_return_when_undefined(self):
        product_id = "p_mus_X"
        product = {"external_id": product_id,
                   "resources": {"title": None},
                   "language": "english",
                   "date": self.session_context.get_present_date(),
                   "full_content": "Obladi-Oblada, life goes on bra, lala how the life goes on..."}
        self.db_proxy.insert_product(product)
        product_ids = self.session_context.data_proxy.fetch_all_product_ids(allow_deleted=True)
        nose.tools.ok_(product_id in product_ids,
                       "Products with 'null' required fields should be returned when no required fields are specified")

    def test_missing_required_field_should_return_when_has_default_value(self):
        required_fields = self.session_context.product_model_factory.get_custom_required_attributes()
        product_id = "p_mus_Y"
        product = {"external_id": product_id,
                   "resources": {"title": "Obladi Oblada"},
                   "full_content": "Obladi-Oblada, life goes on bra, lala how the life goes on...",
                   "date": self.session_context.get_present_date(),
                   "expiration_date": self.session_context.get_present_date() + dt.timedelta(days=30)}
                   # "language" is missing, but it has a default value
        self.db_proxy.insert_product(product)
        product_ids = self.session_context.data_proxy.fetch_all_product_ids(allow_deleted=True,
                                                                            required_fields=required_fields)
        nose.tools.ok_(product_id in product_ids,
                       "Products with missing required fields should be returned when the attribute has a default")

