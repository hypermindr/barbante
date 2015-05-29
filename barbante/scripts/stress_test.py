#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Performs several write operations in the database, logging their durations.

    **Command-line parameters**

        *host*
            The host name.
        *database_name*
            The db.
        An optional third parameter is ``--overwhelmed``. If provided, the db will perform a bunch
        of write operations immediately before the target updates are triggered.

    **Example of usage**

        ``python3 stress_test.py legiao.hypermindr.com db_foo --overwhelmed``

    The script runs forever, until it is killed.
"""


from time import time
import json
import pymongo
import sys
import traceback
import random
import nose.tools

import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)
WRITE_CONCERN = 0


@nose.tools.nottest
def stress_test(database, database_bulk, users, products, overwhelmed):
    log.info("Flooding mongo with updates to impressions_summary...")

    i = 1
    while True:
        if overwhelmed:
            overwhelm(database_bulk, users)

        user = random.choice(users)
        product = random.choice(products)
        new_count = random.randint(1, 100)
        spec = {"u_id": user, "p_id": product}
        update_clause = {"$set": {"count": new_count}}

        log.info("Running update %d (u_id=%s, p_id=%s, count=%d)..." % (i, user, product, new_count))
        start = time()
        database.impressions_summary.update(spec,
                                            update_clause,
                                            upsert=False,
                                            write_concern={'w': WRITE_CONCERN})
        log.info("Update %d done. Took %d milliseconds" % (i, 1000 * (time() - start)))
        i += 1


def overwhelm(database, users):
    log.info("Preparing to overwhelm mongo with updates to user_user_strengths...")
    start = time()

    bulk_op = database.user_user_strengths.initialize_unordered_bulk_op()

    for i in range(2000):
        user = random.choice(users)
        template = None
        while template in [None, user]:
            template = random.choice(users)
        strength = random.uniform(0.1, 0.5)

        spec = {"user": user, "template_user": template}
        update_doc = {"strength": strength}
        update_clause = {"$set": update_doc}
        bulk_op.find(spec).upsert().update(update_clause)

    log.info("Done preparing. Took %d milliseconds." % (1000 * (time() - start)))

    start = time()
    log.info("Overwhelming mongo with updates to user_user_strengths...")
    bulk_op.execute(write_concern={'w': WRITE_CONCERN})
    log.info("Done overwhelming. Took %d milliseconds." % (1000 * (time() - start)))


def fetch_users(database):
    result = set()
    cursor = database.impressions_summary.find({}, ["u_id"])
    for rec in cursor:
        result.add(rec["u_id"])
        if len(result) >= 100:
            break
    return list(result)


def fetch_products(database):
    result = set()
    cursor = database.impressions_summary.find({}, ["p_id"])
    for rec in cursor:
        result.add(rec["p_id"])
        if len(result) >= 100:
            break
    return list(result)


def main(argv):
    if len(argv) < 2:
        msg = "You must specify the host and the db"
        log.error(msg)
        return json.dumps({"success": False, "message": msg})
    try:
        # command-line arguments
        host_addr = argv[0]
        db_name = argv[1]
        database = pymongo.MongoClient(host_addr, tz_aware=True)[db_name]
        database_bulk = pymongo.MongoClient(host_addr, tz_aware=True)[db_name]

        overwhelmed = False
        if len(argv) >= 3:
            overwhelmed = argv[2] == "--overwhelmed"

        log.info("Fetching some users and products...")
        users = fetch_users(database)
        products = fetch_products(database)

        stress_test(database, database_bulk, users, products, overwhelmed)

    except Exception:
        log.exception('Exception on {0}'.format(__name__))
        return json.dumps({"success": False,
                           "message": traceback.format_exc()})


if __name__ == '__main__':
    print(main(sys.argv[1:]))
