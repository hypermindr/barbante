#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Triggers maintenance tasks when a new activity is saved, including updates to:
    - user vs user strengths
    - product vs product strengths
"""

import dateutil.parser
import sys
import traceback

import barbante.maintenance.tasks as maintenance
import barbante.utils.logging as barbante_logging
from barbante.context import init_session
from barbante.context.context_manager import new_context


log = barbante_logging.get_logger(__name__)


def main(argv):
    if len(argv) < 5:
        msg = "You must specify the environment, the external_user_id, " \
              "the external_product_id, the activity type and the activity date"
        log.error(msg)
        return {"success": False, "message": msg}
    try:
        # command-line arguments
        env = argv[0]
        user = argv[1]
        product = argv[2]
        activity_type = argv[3]
        activity_date = dateutil.parser.parse(argv[4])

        activity = {"external_user_id": user, "external_product_id": product, "activity": activity_type,
                    "created_at": activity_date}

        session = init_session(env)

        maintenance.update_collaborative_filtering_strengths(session, activity)

        return {"success": True}

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))
        return {"success": False, "message": traceback.format_exc()}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
