import barbante.utils.decay_functions as df
import barbante.utils as utils


FIXED = 'fixed'
LIST = 'list'
DATE = 'date'
NUMERIC = 'numeric'
TEXT = 'text'


class ProductModel(object):

    def __init__(self, validator, product_id, product_model_values):
        """ Constructor.
            :param validator: a ProductModelFactory object.
            :param product_id: The id of the intended product.
            :param product_model_values: a flattened list of attributes.
        """
        if product_model_values is None:
            raise ValueError('The product cannot be null')
        self.id = product_id
        self.values = product_model_values
        self.validator = validator

    def get_attribute(self, field):
        return self.values.get(field)

    @staticmethod
    def from_dict(product_id, product_model_dict, validator):
        """ Converts a product model in the form of a dict into an instance of ProductModel.
            It differs from the constructor in that from_dict() expects a product model, as the
            constructor expects a raw product.

            :param product_id: The id of the intended product.
            :param product_model_dict: A flat dict of attributes.
            :param validator: an instance of a ProductModelFactory.
            :returns: a ProductModel instance.
        """
        product_id = product_id
        product_model_values = utils.flatten_dict(product_model_dict)
        return ProductModel(validator, product_id, product_model_values)

    def to_dict(self):
        """ Converts the ProductModel instance into a dict.
            Only persisted attributes will be checked.
            Flattened attributes are expanded accordingly.

            :return: a dict with the attributes and corresponding values.
        """
        result = {}
        for key in self.validator.persisted_attributes:
            value = self.values.get(key)
            path = key.split('.')
            d = result

            if len(path) > 1:
                for attr in path[:-1]:
                    if attr not in d:
                        d[attr] = {}
                    d = d[attr]

            d[path[-1]] = value

        return result

    def keys(self):
        return [key for key in self.values]


def parse_external_product(fields, product_id, external_product):
    return ProductModel(fields, product_id, external_product)


def parse_product_model_fields(product_model_config):
    return [key for config_row in product_model_config for key, _ in config_row.items()]


def compute_similarity_for_numeric(product_attr_value, template_attr_value):
    if product_attr_value == 0 and template_attr_value == 0:
        return 1
    return min(product_attr_value, template_attr_value) / max(product_attr_value, template_attr_value)


def compute_similarity_for_date(product_attr_value, template_attr_value, halflife):
    time_delta = max(product_attr_value, template_attr_value) - min(product_attr_value, template_attr_value)
    days = time_delta.days
    return df.exponential(days, halflife)


def compute_similarity_for_fixed(product_attr_value, template_attr_value):
    return 1 if product_attr_value == template_attr_value else 0


def compute_similarity_for_list(product_attr_value, template_attr_value):
    if len(product_attr_value) == 0:
        return 1  # there is nothing in the list to be covered
    product_values = set(product_attr_value)
    template_values = set(template_attr_value)
    intersection = product_values & template_values
    return len(intersection) / len(product_values)
