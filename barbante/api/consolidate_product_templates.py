#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Pre-renders templates (for both collaborative filtering and content-based use)
    for all products and saves them in the database.

    **Command-line parameters**

        *environment*
            The intended environment, as defined in mongoid.yml.

        *product_ids*
            A list of comma-separated product ids to be processed.
            If not informed, all products in the product_product_strengths_window will be considered.
            If --all, all products (unfiltered) will be considered.

    **Examples of usage**

        ``python consolidate_product_templates.py development``
        ``python consolidate_product_templates.py development 1111,22,333,444``
"""

import sys
import traceback
from time import time

from barbante.maintenance.template_consolidation import consolidate_product_templates
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

        product_ids = None
        if len(argv) >= 2:
            product_ids = argv[1]
            if product_ids != "--all":
                product_ids = argv[1].split(",")

        timestamp = session.get_present_date()
        start = time()

        latest_run = session.data_proxy.fetch_latest_batch_info_product_template_consolidation()
        if latest_run:
            if latest_run.get("status") == "running":
                msg = "An old consolidation batch is still running. Won't start another one."
                log.info(msg)
                return {"success": False, "message": msg}

        session.data_proxy.save_timestamp_product_template_consolidation(
            status="running", timestamp=timestamp)

        consolidate_product_templates(session, product_ids)
        session.data_proxy.ensure_indexes_cache()

        elapsed_time = time() - start

        session.data_proxy.save_timestamp_product_template_consolidation(
            status="success", timestamp=timestamp, elapsed_time=elapsed_time)

        return {"success": True}

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))

        session.data_proxy.save_timestamp_product_template_consolidation(
            status="failed", timestamp=timestamp)

        return {"success": False, "message": traceback.format_exc()}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
