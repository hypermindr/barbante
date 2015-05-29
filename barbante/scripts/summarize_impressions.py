#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Summarizes existing impressions.

    **Command-line parameters**

        *source_host*
            The host name.
        *source_database_name*
            The db.
        *destination_host*
            The host name.
        *destination_database_name*
            The db.
        *latest_processed_impression_id*
            The objectid of the latest impression to be processed. If informed, the script will resume from the first
            impression whose objectid is greater that *latest_processed_impression_id*.
            If --complete, it will process from the very beginning of the impressions collection.
            If --resume, it will fetch its value from the maintenance collection.
        *max_date*
            The max cutoff_date in ISO format (or --unbounded).
        *comma_separated_activity_types*
            A list of activity types which trigger a reset in the count of activities.

    **Example of usage**

        ``python3 -m summarize_impressions legiao.hypermindr.com db_foo legiao.hypermindr.com db_foo
              --complete 2014-11-19T14:00:57.740Z buy,cart,view``

    **Output**

    Returns a JSON object as follows:
        {"success": "true"}, if the summary operation ran fine;
        {"message": "some error message", "success": "false"}, otherwise.

    The script populates an "impressions_summary" collection, which is emptied in case it already exists.
"""

import json
import pymongo
import bson.objectid
import signal
import sys
import traceback

import dateutil.parser
import datetime as dt
from time import time

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


def summarize_impressions(source_database, destination_database, latest_processed_impression_objectid,
                          max_date, activity_types):
    log.info("----------")
    log.info("Start.")

    timestamp = dt.datetime.now()
    real_time_start = time()

    if latest_processed_impression_objectid is None:
        log.info("Dropping old collection (if any) and recreating indexes...")
        recreate_summary_collection(destination_database)

    log.info("Opening cursor for impressions...")

    impressions_cursor = fetch_impressions_cursor(source_database, latest_processed_impression_objectid, max_date)
    total_impressions = impressions_cursor.count()

    impression, impression_date = yield_impression(impressions_cursor)
    if impression is None:
        log.info("No impressions to summarize.")
        log.info("End.")
        return

    activities_cursor = fetch_activities_cursor(source_database, impression_date, max_date, activity_types)
    activity, activity_date = yield_activity(activities_cursor)

    log.info("Summarizing impressions...")

    done = 0
    impressions_summary_by_user_and_product = {}
    user_and_product_reset = set()

    while (impression is not None or activity is not None) and should_continue:

        if impression_date <= activity_date:
            event_type = IMP
            event_date = impression_date
            user_and_product = (impression["external_user_id"], impression["external_product_id"])
            object_id_just_processed = impression["_id"]
            impression, impression_date = yield_impression(impressions_cursor)
        else:
            event_type = ACT
            event_date = activity_date
            user_and_product = (activity["external_user_id"], activity["external_product_id"])
            activity, activity_date = yield_activity(activities_cursor)

        summary = impressions_summary_by_user_and_product.get(user_and_product, [0, None])
        if event_type == ACT:
            if summary[1] is not None:
                summary[0] = 0  # resets the count
            user_and_product_reset.add(user_and_product)  # the u,p pair has been reset in this time frame
        elif event_type == IMP:
            if summary[1] is None:
                summary[1] = event_date  # date of the first impression of the u,p pair in this time frame
            summary[0] += 1  # increments the impressions count
            impressions_summary_by_user_and_product[user_and_product] = summary
            done += 1
            if done % (FLUSH_SIZE // 4) == 0 or done == total_impressions:
                log.info("Processed %d out of %d impressions (%.2f%% done)."
                    % (done, total_impressions, 100.0 * done / total_impressions))

        if len(impressions_summary_by_user_and_product) + len(user_and_product_reset) >= FLUSH_SIZE:
            flush_summaries(destination_database, impressions_summary_by_user_and_product, user_and_product_reset)

    if len(impressions_summary_by_user_and_product) + len(user_and_product_reset) > 0:
        flush_summaries(destination_database, impressions_summary_by_user_and_product, user_and_product_reset)

    log.info("Impressions summarized successfully.")

    log.info("Saving completion of impressions summary task with latest processed impression " +
        str(object_id_just_processed) + "...")
    save_summary_task(destination_database, timestamp, object_id_just_processed, time() - real_time_start)

    log.info("End.")


def yield_impression(cursor):
    try:
        impression = next(cursor)
        impression_date = impression["created_at"]
    except StopIteration:
        impression = None
        impression_date = dt.datetime(2100, 1, 1)  # so it shall be defeated in all comparisons
    return impression, impression_date


def yield_activity(cursor):
    try:
        activity = next(cursor)
        activity_date = activity["created_at"]
    except StopIteration:
        activity = None
        activity_date = dt.datetime(2100, 1, 1)  # so it shall be defeated in all comparisons
    return activity, activity_date


def fetch_activities_cursor(database, min_date, max_date, activity_types):
    where = {"anonymous": False}
    if min_date is not None:
        where["created_at"] = {"$gte": min_date}
    if max_date is not None:
        date_clause = where.get("created_at", {})
        date_clause.update({"$lt": max_date})
        where["created_at"] = date_clause
    if activity_types is not None:
        where["activity"] = {"$in": activity_types}
    fields = {"_id": False,
              "activity": True,
              "created_at": True,
              "external_user_id": True,
              "external_product_id": True}
    cursor = database.activities.find(
        where, fields,
        sort=[("anonymous", pymongo.ASCENDING),
              ("created_at", pymongo.ASCENDING)],
        timeout=False)
    return cursor


def fetch_impressions_cursor(database, latest_processed_impression_id, max_date):
    where = {"anonymous": False}
    if latest_processed_impression_id is not None:
        where["_id"] = {"$gt": latest_processed_impression_id}
    if max_date is not None:
        date_clause = where.get("created_at", {})
        date_clause.update({"$lt": max_date})
        where["created_at"] = date_clause
    cursor = database.impressions.find(
        where, ["external_user_id",
                "external_product_id",
                "created_at"],
        sort=[("_id", pymongo.ASCENDING)],
        timeout=False)
    return cursor


def save_summary_task(database, timestamp, latest_impression_object_id, elapsed_time):
    database.maintenance.insert({"type": "summarize_impressions",
                                 "timestamp": timestamp,
                                 "latest_impression_id": latest_impression_object_id,
                                 "elapsed_time_sec": elapsed_time})


def recreate_summary_collection(database):
    database.impressions_summary.drop()

    database.impressions_summary.ensure_index([("u_id", pymongo.ASCENDING),
                                               ("p_id", pymongo.ASCENDING),
                                               ("count", pymongo.ASCENDING),
                                               ("first", pymongo.DESCENDING)])

    database.impressions_summary.ensure_index([("p_id", pymongo.ASCENDING),
                                               ("u_id", pymongo.ASCENDING),
                                               ("count", pymongo.ASCENDING),
                                               ("first", pymongo.DESCENDING)])


def flush_summaries(database, impressions_summary_by_user_and_product, user_and_product_reset):
    log.info("Saving %d summaries and %d reset (user,product) pairs..."
        % (len(impressions_summary_by_user_and_product), len(user_and_product_reset)))

    bulk_op = database.impressions_summary.initialize_unordered_bulk_op()

    for user_and_product, summary in impressions_summary_by_user_and_product.items():
        # upserts the (u,p) pair, setting the "first" attribute on the insert (but not on updates)
        spec = {"u_id": user_and_product[0],
                "p_id": user_and_product[1]}
        if user_and_product in user_and_product_reset:
            operator = "$set"  # if the pair was reset in this time frame, we should overwrite its count
        else:
            operator = "$inc"  # otherwise we should increment it with the count found in this time frame
        update_clause = {operator: {"count": summary[0]},
                         "$setOnInsert": {"first": summary[1]}}
        bulk_op.find(spec).upsert().update(update_clause)

    for user_and_product in user_and_product_reset - impressions_summary_by_user_and_product.keys():
        spec = {"u_id": user_and_product[0],
                "p_id": user_and_product[1],
                "count": {"$gt": 0}}
        bulk_op.find(spec).update({"$set": {"count": 0}})

    bulk_op.execute()

    impressions_summary_by_user_and_product.clear()
    user_and_product_reset.clear()


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

        latest_processed_impression_id = argv[2]
        max_date_str = argv[3]
        activity_types = argv[4].split(",")

        if latest_processed_impression_id == "--complete":
            latest_processed_impression_objectid = None
        elif latest_processed_impression_id == "--resume":
            cursor = destination_database.maintenance.find(
                {"type": "summarize_impressions"},
                ["latest_impression_id"]).sort([("timestamp", pymongo.DESCENDING)])
            if cursor is not None:
                latest_processed_impression_objectid = cursor[0]["latest_impression_id"]
            else:
                latest_processed_impression_objectid = None
        else:
            latest_processed_impression_objectid = bson.objectid.ObjectId(latest_processed_impression_id)

        if max_date_str == "--unbounded":
            max_date = None
        else:
            max_date = dateutil.parser.parse(max_date_str)

        set_exit_handler(sig_handler)
        summarize_impressions(source_database, destination_database,
                              latest_processed_impression_objectid, max_date, activity_types)

    except Exception:
        log.exception('Exception on {0}'.format(__name__))
        return json.dumps({"success": False,
                           "message": traceback.format_exc()})

    return_json = json.dumps({"success": True})
    return return_json


if __name__ == '__main__':
    print(main(sys.argv[1:]))
