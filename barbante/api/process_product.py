#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Triggers maintenance tasks when a new product is saved, including:
    - indexing of the TF's and DF's of the product terms
    - update of product vs product strengths (tfidf based)
"""

import json
import sys
import traceback

import barbante.maintenance.tasks as maintenance
import barbante.utils.logging as barbante_logging
from barbante.context import init_session
from barbante.context.context_manager import new_context


log = barbante_logging.get_logger(__name__)


def main(argv):
    if len(argv) < 2:
        msg = "You must specify the environment and the document id"
        log.error(msg)
        return {"success": False, "message": msg}
    try:
        # command-line arguments
        env = argv[0]
        product = json.loads(argv[1])
        product_id = product.get("external_id")

        if product_id is None:
            msg = "Product has no external_id"
            log.error(msg)
            return {"success": False, "message": msg}

        session = init_session(env)

        maintenance.process_product(session, product_id, product=product, force_update=True)

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))
        return {"success": False, "message": traceback.format_exc()}

    return {"success": True}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
