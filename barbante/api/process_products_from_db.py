#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Processes several products in batch, including:
    - indexing of the TF's and DF's of the product terms
    - update of product vs product strengths (tfidf based)
"""

import pymongo
import sys
import traceback

import barbante.maintenance.tasks as maintenance
import barbante.utils.logging as barbante_logging
from barbante.context import init_session
from barbante.context.context_manager import new_context


log = barbante_logging.get_logger(__name__)


def main(argv):
    if len(argv) < 2:
        msg = "You must specify the environment, " \
              "the product id (or --all, or --resume) and the number of days (or --complete) if using --all."
        log.error(msg)
        return {"success": False, "message": msg}
    try:
        # command-line arguments
        env = argv[0]
        product_id = argv[1]

        session = init_session(env)

        if product_id == "--all":
            if argv[2] == "--complete":
                days = None
            else:
                days = int(argv[2])
            maintenance.process_products(session, days)

        elif product_id == "--resume":
            maintenance.process_products(session, resume=True)

        else:
            if len(argv) == 3 and argv[2] == '--force':
                force = True
            else:
                force = False
            maintenance.process_product(session, product_id, force_update=force)

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))
        return {"success": False, "message": traceback.format_exc()}

    return {"success": True}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
