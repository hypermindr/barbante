""" Tests barbante.api.get_user_templates.
"""

import json

import nose.tools

import barbante.api.get_user_templates as script
import barbante.utils.logging as barbante_logging
import barbante.tests as tests


log = barbante_logging.get_logger(__name__)


def test_script():
    """ Tests a call to script barbante.api.get_user_templates.
    """
    result = script.main([tests.TEST_ENV, "xxx", 10])  # non-existing user (on purpose)
    log.debug(result)
    result_json = json.dumps(result)
    nose.tools.ok_(result_json)  # a well-formed json is enough


if __name__ == '__main__':
    test_script()