""" Tests barbante.api.delete_product.
"""

import json

import nose.tools

import barbante.api.delete_product as script
import barbante.utils.logging as barbante_logging
import barbante.tests as tests


log = barbante_logging.get_logger(__name__)


def test_script():
    """ Tests a call to script barbante.api.delete_product.
    """
    result = script.main([tests.TEST_ENV, "ppp"])
    log.debug(result)
    result_json = json.dumps(result)
    nose.tools.ok_(result_json)  # a well-formed json is enough


if __name__ == '__main__':
    test_script()