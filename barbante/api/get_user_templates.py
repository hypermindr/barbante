#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Given a User ID, returns the list of user templates and associated strengths
"""

import sys
import traceback

import barbante.config as config
from barbante.context import init_session
import barbante.maintenance.user_templates as user_templates
import barbante.utils.logging as barbante_logging
from barbante.context.context_manager import new_context


log = barbante_logging.get_logger(__name__)


def main(argv):
    if len(argv) < 2:
        msg = "You must specify the environment and the external user id"
        log.error(msg)
        return {"success": False, "message": msg}
    try:
        env = argv[0]
        user_id = argv[1]

        session = init_session(env)
        templates = [t for t in user_templates.get_user_templates(session, user_id)]

        return {"success": True, "user_id": user_id, "template_users": templates}

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))
        return {"success": False, "message": traceback.format_exc()}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
