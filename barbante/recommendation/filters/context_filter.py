""" Encapsulates
"""
import dateutil.parser
import json

import barbante.model.product_model as pm
import barbante.utils.logging as barbante_logging

log = barbante_logging.get_logger(__name__)

logical_operators = ['$and', '$or']
comparison_operators = ['$ne', '$lt', '$lte', '$gt', '$gte']


class ContextFilter(object):
    def __init__(self, context, context_filter_string):
        self.attributes = self._load_attributes_by_type(context)
        self._load_attributes_by_type(context)
        if context_filter_string:
            self.json_filter = self.validate_json(context_filter_string)
        else:
            self.json_filter = {}
        self.json_filter = self.validate_fields(self.json_filter)

    @staticmethod
    def validate_json(context_filter_string):
        try:
            return json.loads(context_filter_string)
        except ValueError as ex:
            error_message = 'Invalid JSON: {0}. Error message: {1}'.format(context_filter_string, ex)
            log.warn(error_message)
            raise ValueError(error_message)

    def validate_fields(self, json_filter):
        for key, value in json_filter.items():
            if key in logical_operators:
                values = []
                for entry in value:
                    values.append(self.validate_fields(entry))
                json_filter[key] = values
            elif key in self.attributes:
                if self.attributes[key] == pm.DATE:
                    try:
                        if isinstance(value, str):
                            json_filter[key] = dateutil.parser.parse(value)
                        elif isinstance(value, dict):
                            for op, op_value in value.items():
                                value[op] = dateutil.parser.parse(op_value)
                        else:
                            raise ValueError("Unknown value type for attribute [{0}] => [{1}]".format(key, value))
                    except:
                        raise ValueError("Cannot parse date [{0}] to ISO8601".format(value))
            else:
                raise ValueError("Unknown filter attribute [{0}]".format(key))
        return json_filter

    def to_json(self):
        return self.json_filter.copy()

    def _load_attributes_by_type(self, context):
        attributes = {}
        attributes_by_type = context.product_model_factory.get_attributes_by_type()
        for _type in attributes_by_type:
            for attribute in attributes_by_type[_type]:
                attributes[attribute] = _type
        return attributes
