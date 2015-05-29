#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Logically deletes a product, saving the (last) date of its deletion.
"""

import sys
import traceback

import barbante.maintenance.tasks as maintenance
import barbante.utils.logging as barbante_logging
from barbante.context import init_session
from barbante.context.context_manager import new_context


log = barbante_logging.get_logger(__name__)


def main(argv):
    if len(argv) < 2:
        msg = "You must specify the environment and the external product id"
        log.error(msg)
        return {"success": False, "message": msg}
    try:
        # command-line arguments
        env = argv[0]
        product_id = argv[1]

        session = init_session(env)
        maintenance.delete_product(session, product_id)

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))
        return {"success": False, "message": traceback.format_exc()}

    return {"success": True}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
