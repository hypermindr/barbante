import datetime as dt
import pytz

import nose.tools
from nose.tools import raises

import barbante.model.product_model as pm
from barbante.model.product_model_factory import ProductModelFactory
import barbante.tests as tests
import barbante.utils.text as text


def test_external_product_parse_valid_attributes():
    model_definition = {'language': {'type': 'fixed',
                                     'required': True,
                                     'persisted': True},
                        'source': {'type': 'list',
                                   'required': True,
                                   'persisted': True}}
    factory = ProductModelFactory(model_definition)
    product1 = {'language': 'english', 'source': ['NYT']}
    model = factory.build('product1', product1)
    nose.tools.eq_(model.get_attribute('language'), product1['language'])
    nose.tools.eq_(model.get_attribute('source'), product1['source'])


def test_stemmed_field():
    model_definition = {'language': {'type': 'fixed',
                                     'required': True,
                                     'persisted': True},
                        'resources.title': {'type': 'text',
                                            'required': True,
                                            'persisted': True}}
    product1 = {'language': 'english', 'resources': {'title': 'Roger Federer Ousts Novak Djokovic'}}
    stems = ['roger', 'feder', 'oust', 'novak', 'djokov']
    factory = ProductModelFactory(model_definition)
    model = factory.build('product1', product1)
    nose.tools.eq_(model.get_attribute('resources.title'), stems)


def test_key_field():
    model_definition = {'language': {'type': 'fixed',
                                     'required': True,
                                     'persisted': True},
                        'source': {'type': 'list',
                                   'required': True,
                                   'persisted': True},
                        'resources.title': {'type': 'text',
                                            'required': True,
                                            'persisted': True}}
    product1 = {'language': 'english', 
                'resources': {'title': 'Roger Federer Ousts Novak Djokovic'},
                'source': ['NYT']}
    factory = ProductModelFactory(model_definition)
    model = factory.build('product1', product1)
    nose.tools.ok_(
        len(frozenset(model.keys()).intersection(frozenset(['source', 'resources.title', 'language']))) == 3
    )


@raises(AttributeError)
def test_missing_mandatory_attributes():
    """ Tests whether an exception is raised when a mandatory attribute does not belong
        to the product model definition.
    """
    model_definition = {'source': {'type': 'list',
                                   'required': True,
                                   'persisted': True},
                        'resources.title': {'type': 'text',
                                            'required': True,
                                            'persisted': True}}
                        # missing language in the model
    _ = ProductModelFactory(model_definition)

@raises(ValueError)
def test_required_field_values_are_present():
    """ Tests whether an exception is raised when a required attribute is missing from
        the product model.
    """

    model_definition = {'language': {'type': 'fixed',
                                     'required': True,
                                     'persisted': True},
                        'source': {'type': 'list',
                                   'required': False,
                                   'persisted': True},
                        'resources.title': {'type': 'text',
                                            'required': True,
                                            'persisted': True}}
    product1 = {'language': 'english'}
    factory = ProductModelFactory(model_definition)
    factory.build('product1', product1)


def test_alright_when_non_required_field_is_missing():
    """ Tests whether the factory successfully validates a model when a non-required attribute is missing from
        the product model.
    """

    model_definition = {'language': {'type': 'fixed',
                                     'required': True,
                                     'persisted': True},
                        'source': {'type': 'list',
                                   'required': False,
                                   'persisted': True},
                        'resources.title': {'type': 'text',
                                            'required': False,
                                            'persisted': True}}
    product1 = {'language': 'english'}
    factory = ProductModelFactory(model_definition)
    factory.build('product1', product1)
    # Ok. No exceptions were raised.


def test_alright_when_required_field_is_missing_but_default_is_given():
    """ Tests whether the factory successfully validates a model when a required attribute is missing from
        the product model, but a default value is given.
    """

    model_definition = {'language': {'type': 'fixed',
                                     'required': True,
                                     'persisted': True,
                                     'default': 'portuguese'},
                        'source': {'type': 'list',
                                   'required': False,
                                   'persisted': True}}
    product1 = {'source': ['Whatever']}
    factory = ProductModelFactory(model_definition)
    factory.build('product1', product1)
    # Ok. No exceptions were raised.


@raises(AttributeError)
def test_invalid_setting_values():
    model_definition = {'language': {'type': 'fixed',
                                     'required': True,
                                     'persisted': True},
                        'source': {'type': 'list',
                                   'required': 'maybe',
                                   'persisted': True},
                        'resources.title': {'type': 'text',
                                            'required': True,
                                            'persisted': True}}
    product1 = {'language': 'english'}
    factory = ProductModelFactory(model_definition)
    factory.build('product1', product1)


def test_default_attribute_values():
    model_definition = {'language': {'type': 'list',
                                     'required': True,
                                     'persisted': True,
                                     'default': 'portuguese'},
                        'resources.title': {'type': 'text',
                                            'required': True,
                                            'persisted': True}}
    product1 = {'resources': {'title': 'O rato roeu a roupa do rei de Roma'}, 'source': ['NYT']}
    factory = ProductModelFactory(model_definition)
    model = factory.build('product1', product1)
    nose.tools.eq_(model.get_attribute('language'), 'portuguese')


def test_numeric_type():
    model_definition = {'language': {'type': 'list',
                                     'required': True,
                                     'persisted': True,
                                     'default': 'portuguese'},
                        'price': {'type': 'numeric',
                                  'required': True,
                                  'persisted': True}}
    product1 = {'price': 5.7}
    factory = ProductModelFactory(model_definition)
    model = factory.build('product1', product1)
    nose.tools.eq_(model.get_attribute('price'), 5.7)


def test_date_type():
    context = tests.init_session()
    factory = context.product_model_factory
    product1 = {'language': 'portuguese',
                'full_content': 'empty',
                'resources': {'title': 'whatever'},
                'date': '2014-01-01T12:34:56.7890',
                'expiration_date': '2018-01-01T12:34:56.7890'}
    model = factory.build('product1', product1)
    nose.tools.eq_(model.get_attribute('date'), dt.datetime(2014, 1, 1, 12, 34, 56, 789000))
    product1 = {'language': 'portuguese',
                'full_content': 'empty',
                'resources': {'title': 'whatever'},
                'date': '2014-01-01T12:34:56Z',
                'expiration_date': '2018-01-01T12:34:56.7890'}
    model = factory.build('product1', product1)
    nose.tools.eq_(model.get_attribute('date'), dt.datetime(2014, 1, 1, 12, 34, 56, tzinfo=pytz.utc))
    product1 = {'language': 'portuguese',
                'full_content': 'empty',
                'resources': {'title': 'whatever'},
                'date': '2014-01-01T12:34:56.7890Z',
                'expiration_date': '2018-01-01T12:34:56.7890'}
    model = factory.build('product1', product1)
    nose.tools.eq_(model.get_attribute('date'), dt.datetime(2014, 1, 1, 12, 34, 56, 789000, tzinfo=pytz.utc))


def test_similarity_numeric():
    """ Tests the calculation of the similarity of two products based on a 'numeric' attribute.
    """
    similarity = pm.compute_similarity_for_numeric(900, 800)
    nose.tools.ok_(abs(similarity - 8/9) < tests.FLOAT_DELTA, "Wrong numeric similarity")


def test_similarity_date():
    """ Tests the calculation of the similarity of two products based on a 'date' attribute.
    """
    date1 = dt.datetime(2000, 11, 24, 10, 0)
    date2 = dt.datetime(2000, 11, 26, 10, 0)
    similarity = pm.compute_similarity_for_date(date1, date2, halflife=2)
    nose.tools.ok_(abs(similarity - 0.5) < tests.FLOAT_DELTA, "Wrong date similarity")


def test_similarity_fixed():
    """ Tests the calculation of the similarity of two products based on a 'fixed' attribute.
    """
    similarity = pm.compute_similarity_for_fixed("Rio de Janeiro", "São Paulo")
    nose.tools.eq_(similarity, 0, "Wrong fixed similarity")
    similarity = pm.compute_similarity_for_fixed("Rio de Janeiro", "Rio de Janeiro")
    nose.tools.eq_(similarity, 1, "Wrong fixed similarity")


def test_similarity_list():
    """ Tests the calculation of the similarity of two products based on a 'list' attribute.
    """
    list1 = ["a", "b", "c"]
    list2 = ["b", "c", "d", "e"]
    similarity = pm.compute_similarity_for_list(list1, list2)
    nose.tools.ok_(abs(similarity - 2/3) < tests.FLOAT_DELTA,  "Wrong list similarity")
    similarity = pm.compute_similarity_for_list(list2, list1)  # intentionally asymmetric
    nose.tools.ok_(abs(similarity - 1/2) < tests.FLOAT_DELTA,  "Wrong list similarity")


def test_conversion_to_dict():
    """ Tests conversion from a ProductModel instance to a dict.
    """
    model_definition = {
        'language': {'type': 'fixed', 'default': 'english'},
        'a': {'type': 'fixed', 'persisted': True},
        'b.c': {'type': 'fixed', 'persisted': True},
        'b.d.e': {'type': 'text', 'persisted': True},
        'b.d.f': {'type': 'numeric', 'persisted': True}
    }
    factory = ProductModelFactory(model_definition)
    raw_product = {
        'a': 'foo',
        'b': {
            'c': 'bar',
            'd': {
                'e': 'some nested stuff',
                'f': 12345
            }
        }
    }
    stemmed = text.parse_text_to_stems('english', raw_product['b']['d']['e'])
    model = factory.build('test_product', raw_product)
    model_dict = model.to_dict()
    nose.tools.eq_(model_dict['a'], raw_product['a'], 'Attribute does not match')
    nose.tools.eq_(model_dict['b']['c'], raw_product['b']['c'], 'Attribute does not match')
    nose.tools.assert_list_equal(model_dict['b']['d']['e'], stemmed, 'Attribute does not match')
    nose.tools.eq_(model_dict['b']['d']['f'], raw_product['b']['d']['f'], 'Attribute does not match')


def test_conversion_from_dict():
    """ Tests conversion from a dict to a ProductModel instance.
    """
    model_definition = {
        'language': {'type': 'fixed', 'default': 'english'},
        'a': {'type': 'fixed', 'persisted': True},
        'b.c': {'type': 'fixed', 'persisted': True},
        'b.d.e': {'type': 'text', 'persisted': True},
        'b.d.f': {'type': 'numeric', 'persisted': True}
    }
    factory = ProductModelFactory(model_definition)
    stemmed = text.parse_text_to_stems('english', 'a value that should be stemmed')
    model_dict = {
        'a': 'test',
        'b': {
            'c': 'foo',
            'd': {
                'e': stemmed,
                'f': 54321
            }
        }
    }
    product = pm.ProductModel.from_dict('test_product', model_dict, factory)
    nose.tools.eq_(product.get_attribute('a'), model_dict['a'], 'Attribute does not match')
    nose.tools.eq_(product.get_attribute('b.c'), model_dict['b']['c'], 'Attribute does not match')
    nose.tools.assert_list_equal(product.get_attribute('b.d.e'),
                                 model_dict['b']['d']['e'], 'Attribute does not match')
    nose.tools.eq_(product.get_attribute('b.d.f'), model_dict['b']['d']['f'], 'Attribute does not match')
