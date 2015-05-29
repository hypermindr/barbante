#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Performs several write operations in the database, logging their durations.

    **Command-line parameters**

        *host*
            The host name.
        *database_name*
            The db.

    **Example of usage**

        ``python3 stress_test.py legiao.hypermindr.com db_foo``

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

UPSERT = "UPSERT"
BULK_INSERT = "BULK INSERT"
INSERT = "COLLECTION.INSERT"


@nose.tools.nottest
def stress_test(database, users, op):

    while True:
        log.info("Preparing docs for %s..." % op)
        start = time()

        if op in [UPSERT, BULK_INSERT]:
            bulk_op = database.user_user_numerators.initialize_unordered_bulk_op()

        insert_list = []

        for i in range(100000):
            user = random.choice(users)
            template = None
            while template in [None, user]:
                template = random.choice(users)
            nc = random.randint(1, 100000)
            na = random.randint(1, nc)

            spec = {"user": user, "template_user": template}
            doc = spec
            doc.update({"nc": nc, "na": na})

            if op == UPSERT:
                update_clause = {"$set": doc}
                bulk_op.find(spec).upsert().update(update_clause)
            elif op == BULK_INSERT:
                bulk_op.insert(doc)
            else:
                insert_list += [doc]

        log.info("Done preparing. Took %d milliseconds." % (1000 * (time() - start)))

        start = time()
        log.info("Writing...")
        if op in [UPSERT, BULK_INSERT]:
            bulk_op.execute(write_concern={'w': WRITE_CONCERN})
        else:
            database.user_user_numerators.insert(insert_list, w=WRITE_CONCERN)
        log.info("Done writing. Took [%d] milliseconds." % (1000 * (time() - start)))


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

        users = [i for i in range(1, 1000)]

        stress_test(database, users, BULK_INSERT)

    except Exception:
        log.exception('Exception on {0}'.format(__name__))
        return json.dumps({"success": False,
                           "message": traceback.format_exc()})


if __name__ == '__main__':
    print(main(sys.argv[1:]))
