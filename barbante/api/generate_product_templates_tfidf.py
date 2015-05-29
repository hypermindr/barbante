#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Computes all content-based product-to-product strengths
    from scratch and saves them in the database.
"""

import sys
import traceback

import barbante.maintenance.product_templates_tfidf as pttfidf
import barbante.utils.logging as barbante_logging
from barbante.context import init_session
from barbante.context.context_manager import new_context


log = barbante_logging.get_logger(__name__)


def main(argv):
    if len(argv) < 1:
        msg = "You must specify the environment"
        log.error(msg)
        return {"success": False, "message": msg}
    try:
        # command-line arguments
        env = argv[0]

        session = init_session(env)
        pttfidf.generate_templates(session)
        return {"success": True}

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))
        return {"success": False, "message": traceback.format_exc()}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
