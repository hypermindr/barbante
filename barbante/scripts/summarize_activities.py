#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Summarizes existing activities.

    **Command-line parameters**

        *source_host*
            The host name.
        *source_database_name*
            The db.
        *destination_host*
            The host name.
        *destination_database_name*
            The db.
        *latest_processed_activity_date*
            A string representing the date ("created_at" field, in ISO format) of the latest activity to be processed.
            If informed, the script will only consider activities whose dates are greater than or equal to
            this parameter; otherwise, no minimum date clause will be imposed.
            If --complete, it will process from the very beginning of the activitites collection.
            If --resume, it will fetch its value from the maintenance collection.
        *max_date*
            The max cutoff_date in ISO format (or --unbounded).
        *comma_separated_activity_types*
            A list of activity types to be processed.

    **Example of usage**

        ``python3 -m summarize_activities legiao.hypermindr.com db_foo legiao.hypermindr.com db_foo
              --complete 2014-11-19T14:00:57.740Z read,read-more``

    **Output**

    Returns a JSON object as follows:
        {"success": "true"}, if the summary operation ran fine;
        {"message": "some error message", "success": "false"}, otherwise.

    The script populates an "activities_summary" collection, which is emptied in case it already exists.
"""

import json
import pymongo
import signal
import sys
import traceback

import dateutil.parser
import datetime as dt
from time import time

import barbante.utils.date as du

import barbante.utils.logging as barbante_logging
log = barbante_logging.get_logger(__name__)


# settings
FLUSH_SIZE = 100000

# constants
ACT = 0
IMP = 1
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# global (we need this, so we can stop the script at any point)
should_continue = True


def summarize_activities(source_database, destination_database, latest_processed_activity_date,
                         max_date, activity_types):
    log.info("----------")
    log.info("Start.")

    timestamp = dt.datetime.now()
    real_time_start = time()

    if latest_processed_activity_date is None:
        log.info("Dropping old collection (if any) and recreating indexes...")
        recreate_summary_collection(destination_database)

    latest_activity_date = None

    log.info("Opening cursor for activities...")

    activities_cursor = fetch_activities_cursor(source_database, latest_processed_activity_date,
                                                max_date, activity_types)
    total_activities = activities_cursor.count()

    activity = yield_activity(activities_cursor)
    if activity is None:
        log.info("No activities to summarize.")
        log.info("End.")
        return

    log.info("Summarizing activities...")

    latest_activity_by_user_and_product = {}
    done = 0

    while activity is not None and should_continue:
        user = activity.get("external_user_id")
        if user is None:
            continue
        product = activity.get("external_product_id")
        if product is None:
            continue
        user_and_product = (user, product)
        latest_activity_date = activity["created_at"]
        latest_activity_by_user_and_product[user_and_product] = activity

        done += 1
        if done % (FLUSH_SIZE // 4) == 0 or done == total_activities:
            log.info("Processed %d out of %d activities (%.2f%% done)." %
                     (done, total_activities, 100.0 * done / total_activities))

        if len(latest_activity_by_user_and_product) >= FLUSH_SIZE:
            flush_summaries(destination_database, latest_activity_by_user_and_product)

        activity = yield_activity(activities_cursor)

    if len(latest_activity_by_user_and_product) > 0:
        flush_summaries(destination_database, latest_activity_by_user_and_product)

    if latest_activity_date:
        log.info("Activities summarized successfully.")

        log.info("Saving completion of activities summary task with latest processed activity date " + \
                 str(latest_activity_date) + "...")
        save_summary_task(destination_database, timestamp, latest_activity_date, time() - real_time_start)
    else:
        log.info("No activities to summarize.")

    log.info("End.")


def yield_activity(cursor):
    try:
        activity = next(cursor)
    except StopIteration:
        activity = None
    return activity


def fetch_activities_cursor(database, latest_processed_activity_date, max_date, activity_types):
    where = {"anonymous": False,
             "activity": {"$in": activity_types}}
    if latest_processed_activity_date is not None:
        where["created_at"] = {"$gt": latest_processed_activity_date}
    else:
        where["created_at"] = {"$gt": dt.datetime(2000, 1, 1)}
    if max_date is not None:
        date_clause = where.get("created_at", {})
        date_clause.update({"$lt": max_date})
        where["created_at"] = date_clause
    fields = {"_id": False,
              "activity": True,
              "created_at": True,
              "external_user_id": True,
              "external_product_id": True}
    cursor = database.activities.find(
        where, fields,
        sort=[("anonymous", pymongo.ASCENDING),
              ("created_at", pymongo.ASCENDING)],
        timeout=False).hint([("anonymous", pymongo.ASCENDING),
                             ("created_at", pymongo.ASCENDING),
                             ("activity", pymongo.ASCENDING),
                             ("external_user_id", pymongo.ASCENDING),
                             ("external_product_id", pymongo.ASCENDING)])
    return cursor


def save_summary_task(database, timestamp, latest_activity_date, elapsed_time):
    database.maintenance.insert({"type": "summarize_activities",
                                 "timestamp": timestamp,
                                 "latest_activity_date": latest_activity_date,
                                 "elapsed_time_sec": elapsed_time})


def recreate_summary_collection(database):
    database.activities_summary.drop()

    database.activities_summary.ensure_index([("external_user_id", pymongo.ASCENDING),
                                              ("external_product_id", pymongo.ASCENDING),
                                              ("day", pymongo.DESCENDING),
                                              ("activity", pymongo.ASCENDING)])

    database.activities_summary.ensure_index([("external_user_id", pymongo.ASCENDING),
                                              ("day", pymongo.DESCENDING),
                                              ("activity", pymongo.ASCENDING),
                                              ("external_product_id", pymongo.ASCENDING)])

    database.activities_summary.ensure_index([("external_product_id", pymongo.ASCENDING),
                                              ("day", pymongo.DESCENDING),
                                              ("activity", pymongo.ASCENDING),
                                              ("external_user_id", pymongo.ASCENDING)])


def flush_summaries(database, latest_activity_by_user_and_product):
    log.info("Saving %d summaries..." % len(latest_activity_by_user_and_product))

    bulk_op = database.activities_summary.initialize_unordered_bulk_op()

    for user_and_product, activity in latest_activity_by_user_and_product.items():
        # upserts the (u,p) pair
        spec = {"external_user_id": user_and_product[0],
                "external_product_id": user_and_product[1]}
        operator = "$set"
        activity_date = activity["created_at"]
        day = du.get_day(activity_date)
        update_clause = {operator: {"activity": activity["activity"],
                                    "day": day,
                                    "created_at": activity_date}}
        bulk_op.find(spec).upsert().update(update_clause)

    bulk_op.execute()

    latest_activity_by_user_and_product.clear()


def set_exit_handler(func):
    signal.signal(signal.SIGTERM, func)
    signal.signal(signal.SIGINT, func)


def sig_handler(sig, _):
    global should_continue
    log.info("Exit signal {0} received".format(sig))
    should_continue = False


def main(argv):
    if len(argv) < 7:
        msg = "You must specify the source host, the source db, " \
              "the destination host, the destination db, " \
              "the latest processed activity date, the max date " \
              "and the activity types"
        log.error(msg)
        return json.dumps({"success": False, "message": msg})
    try:
        # command-line arguments
        source_host_addr = argv[0]
        source_db_name = argv[1]
        source_database = pymongo.MongoClient(source_host_addr, tz_aware=True)[source_db_name]
        destination_host_addr = argv[2]
        destination_db_name = argv[3]
        destination_database = pymongo.MongoClient(destination_host_addr, tz_aware=True)[destination_db_name]

        latest_processed_activity_date = argv[4]
        max_date_str = argv[5]
        activity_types = argv[6].split(",")

        if latest_processed_activity_date == "--complete":
            latest_processed_activity_date = None
        elif latest_processed_activity_date == "--resume":
            cursor = destination_database.maintenance.find(
                {"type": "summarize_activities"},
                ["latest_activity_date"]).sort([("timestamp", pymongo.DESCENDING)])
            if cursor is not None:
                latest_processed_activity_date = cursor[0]["latest_activity_date"]
            else:
                latest_processed_activity_date = None
        else:
            latest_processed_activity_date = dateutil.parser.parse(latest_processed_activity_date)

        if max_date_str == "--unbounded":
            max_date = None
        else:
            max_date = dateutil.parser.parse(max_date_str)

        set_exit_handler(sig_handler)
        summarize_activities(source_database, destination_database,
                             latest_processed_activity_date, max_date, activity_types)

    except Exception:
        log.exception('Exception on {0}'.format(__name__))
        return json.dumps({"success": False,
                           "message": traceback.format_exc()})

    return_json = json.dumps({"success": True})
    return return_json


if __name__ == '__main__':
    print(main(sys.argv[1:]))
