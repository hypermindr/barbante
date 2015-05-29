""" Tests barbante.api.calculate_tf.
"""

import json

import nose.tools

import barbante.api.calculate_tf as script
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


def test_script():
    """ Tests a call to script barbante.api.calculate_tf.
    """
    result = script.main(["english", "one two two"])
    log.debug(result)
    result_json = json.dumps(result)
    nose.tools.ok_(result_json)  # a well-formed json is enough


if __name__ == '__main__':
    test_script()