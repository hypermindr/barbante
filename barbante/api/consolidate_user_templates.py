#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Pre-renders collaborative filtering templates for users and saves them in the database.

    **Command-line parameters**

        *environment*
            The intended environment, as defined in mongoid.yml.

        *user_ids*
            A list of comma-separated user ids to be processed.
            If not informed, all users will be considered.

    **Examples of usage**

        ``python consolidate_user_templates.py development``
        ``python consolidate_user_templates.py development 1111,22,333,444``

"""

import sys
import traceback
from time import time

from barbante.maintenance.template_consolidation import consolidate_user_templates
import barbante.utils.logging as barbante_logging
from barbante.context.context_manager import new_context
from barbante.context import init_session


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

        user_ids = None
        if len(argv) >= 2:
            user_ids = argv[1].split(",")

        timestamp = session.get_present_date()
        start = time()

        latest_run = session.data_proxy.fetch_latest_batch_info_user_template_consolidation()
        if latest_run:
            if latest_run.get("status") == "running":
                msg = "An old consolidation batch is still running. Won't start another one."
                log.info(msg)
                return {"success": False, "message": msg}

        session.data_proxy.save_timestamp_user_template_consolidation(
            status="running", timestamp=timestamp)

        consolidate_user_templates(session, user_ids)
        session.data_proxy.ensure_indexes_cache()

        elapsed_time = time() - start

        session.data_proxy.save_timestamp_user_template_consolidation(
            status="success", timestamp=timestamp, elapsed_time=elapsed_time)

        return {"success": True}

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))

        session.data_proxy.save_timestamp_user_template_consolidation(
            status="failed", timestamp=timestamp)

        return {"success": False, "message": traceback.format_exc()}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
