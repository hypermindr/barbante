""" The ProductModelFactory parses and validates the product model configuration
    according to the defined product model configuration rules.
"""

import datetime as dt
import dateutil.parser

import barbante.utils.text as text
import barbante.model.product_model as pm
from barbante.model.product_model import ProductModel
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


class ProductModelFactory(object):
    ATTRIBUTE_SETTINGS = {'type': [pm.FIXED, pm.LIST, pm.TEXT, pm.DATE, pm.NUMERIC],
                          'required': [True, False],
                          'persisted': [True, False],
                          'default': None,  # we accept anything here
                          'context_filter': [True, False],
                          'similarity_filter': [True, False],
                          'similarity_weight': None  # we accept anything here
                          }

    LANGUAGE = 'language'

    MANDATORY_ATTRIBUTES = frozenset([LANGUAGE])

    _ID_ATTRIBUTE = 'external_product_id'

    def __init__(self, model_attributes):
        self.attributes_by_type = {}
        """ The attributes by type dictionary is used to easily retrieve the attributes of a given type for processing.
        """
        self.persisted_attributes = {self._ID_ATTRIBUTE}
        """ The persisted attributes set is used to hold the list of attributes that should be persisted in the model.
        """
        self.required_attributes = set([self._ID_ATTRIBUTE] + list(self.MANDATORY_ATTRIBUTES))
        """ The required attributes set is used to hold the list of attributes that cannot be missing when building a
            product model instance.
        """
        self.default_values = {}
        """ The default values dictionary is used to hold the default value associated with an attribute.
        """
        self.model_attributes = self.parse_model_attributes(model_attributes)
        """ Holds the complete list of attributes as defined in the product model configuration for a customer.
        """

    def validate_mandatory_attributes(self, model_attributes):
        attribute_keys = set(model_attributes.keys())
        if not attribute_keys.issuperset(self.MANDATORY_ATTRIBUTES):
            raise AttributeError(
                'Mandatory attribute(s) {0} missing'.format(list(self.MANDATORY_ATTRIBUTES - attribute_keys)))

    def parse_model_attributes(self, model_attributes):
        self.validate_mandatory_attributes(model_attributes)

        for attribute_name, attribute_properties in model_attributes.items():
            for attribute_property, property_value in attribute_properties.items():
                self.parse_attribute_property(attribute_name, attribute_property, property_value)

        self.persisted_attributes = frozenset(self.persisted_attributes)
        self.required_attributes = frozenset(self.required_attributes)

        return model_attributes

    def parse_attribute_property(self, attribute_name, attribute_property, value):
        if attribute_property in self.ATTRIBUTE_SETTINGS.keys():
            if attribute_property == 'default':
                self.default_values[attribute_name] = value
            elif self.ATTRIBUTE_SETTINGS[attribute_property] is not None:
                if value in self.ATTRIBUTE_SETTINGS[attribute_property]:
                    if attribute_property == 'type':
                        attributes = self.attributes_by_type.get(value, [])
                        attributes.append(attribute_name)
                        self.attributes_by_type[value] = attributes
                    elif attribute_property == 'persisted' and value:
                        self.persisted_attributes.add(attribute_name)
                    elif attribute_property == 'required' and value:
                        self.required_attributes.add(attribute_name)
                else:
                    msg = "Attribute [{0}] type [{1}] doesn't support value [{2}]. Valid values are [{3}]". \
                        format(attribute_name, attribute_property, value, self.ATTRIBUTE_SETTINGS[attribute_property])
                    raise AttributeError(msg)
        else:
            msg = "Attribute [{0}] type [{1}] is invalid. Valid types are [{2}]". \
                format(attribute_name, attribute_property, self.ATTRIBUTE_SETTINGS.keys())
            raise AttributeError(msg)

    def validate_required_field(self, attribute_name, value):
        if value is None and attribute_name in self.required_attributes:
            raise ValueError("Attribute [{0}] is required".format(attribute_name))

    def get_language(self, product):
        default_language = self.default_values.get(self.LANGUAGE)
        language = product.get(self.LANGUAGE)
        if language is None:  # it may happen that the product has a 'null' persisted language
            language = default_language
        return language

    @staticmethod
    def _get_product_attribute(product, attribute_name, default):
        path = attribute_name.split('.')
        result = product
        try:
            if len(path) > 1:
                for attr in path[:-1]:
                    result = result[attr]
        except KeyError:
            return default  # we simply return the default value here

        value = result.get(path[-1], default)

        if value is None:
            value = default  # this is necessary, for sometimes the persisted value itself is a 'null',
                             # in which case we must replace by the default value
        return value

    def build(self, product_id, product_fields):
        """ Validates and creates an instance of ProductModel from an {attribute: value} dict.

            :param product_id: The id of the product to be encapsulated in a ProductModel object.
            :param product_fields: A dict with the product' contents.

            :return: A validated ProductModel object. Or raises exception if product is invalid.
        """
        if product_id is None:
            raise ValueError("The Product ID is a required attribute")

        model_values = {self._ID_ATTRIBUTE: product_id}

        for attribute_name, attribute_properties in self.model_attributes.items():
            default = self.default_values.get(attribute_name)
            value = self._get_product_attribute(product_fields, attribute_name, default)

            try:
                self.validate_required_field(attribute_name, value)
                if value is not None:
                    if attribute_properties['type'] in [pm.FIXED, pm.LIST, pm.NUMERIC]:
                        model_values[attribute_name] = value
                    elif attribute_properties['type'] == pm.DATE:
                        if not isinstance(value, dt.datetime):
                            value = dateutil.parser.parse(value)
                        model_values[attribute_name] = value
                    else:
                        language = self.get_language(product_fields)
                        if language is None:
                            raise AttributeError("Text attributes are only supported when a language is defined")
                        model_values[attribute_name] = text.parse_text_to_stems(language, value)

            except Exception as err:
                log.error('Exception: {0}'.format(str(err)))
                log.error('Offending product: {0}'.format(product_fields))
                raise err

        return ProductModel(self, product_id, model_values)

    def get_custom_required_attributes(self):
        """
            This method is used to return the list of required attributes to be checked in a product when retrieving
            the list of products to be persisted in the product model
            :return: A set of the custom required fields (the required fields defined in the customer configuration)
        """
        attributes = self.required_attributes - {self._ID_ATTRIBUTE}
        custom_required_attributes = {attribute for attribute in attributes if not self.default_values.get(attribute)}
        return custom_required_attributes

    def get_persisted_attributes(self):
        """
            :return: A set of the persisted fields (the persisted product model fields as
                     defined in the customer configuration)
        """
        return self.persisted_attributes

    def get_attributes_by_type(self):
        return self.attributes_by_type.copy()