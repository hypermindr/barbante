#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Summarizes existing product popularities.

    **Command-line parameters**

        *host*
            The host name.
        *database_name*
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

        ``python3 -m summarize_popularities legiao.hypermindr.com db_foo
              --complete 2014-11-19T14:00:57.740Z read,read-more``

    **Output**

    Returns a JSON object as follows:
        {"success": "true"}, if the summary operation ran fine;
        {"message": "some error message", "success": "false"}, otherwise.

    The script populates an "popularities_summary" collection, which is emptied in case it already exists.
"""

import json
import pymongo
import signal
import sys
import traceback

import dateutil.parser
import datetime as dt
from time import time
import pytz

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


def summarize_popularities(database, latest_processed_activity_date, max_date, activity_types):
    log.info("----------")
    log.info("Start.")

    timestamp = dt.datetime.now()
    real_time_start = time()

    if latest_processed_activity_date is None:
        log.info("Dropping old collection (if any) and recreating indexes...")
        recreate_summary_collection(database)

    latest_activity_date = None

    log.info("Opening cursor for activities...")

    activities_cursor = fetch_activities_cursor(database, latest_processed_activity_date, max_date, activity_types)
    total_activities = activities_cursor.count()

    activity = yield_activity(activities_cursor)
    if activity is None:
        log.info("No popularities to summarize.")
        log.info("End.")
        return

    log.info("Summarizing popularities...")

    popularity_summaries_by_product = {}
    done = 0

    while activity is not None and should_continue:
        user = activity.get("external_user_id")
        if user is None:
            continue
        product = activity.get("external_product_id")
        if product is None:
            continue
        activity_type = activity["activity"]
        if activity_type not in activity_types:
            continue
        latest_activity_date = activity["created_at"]

        should_increment_popularity = True
        cursor = database.activities_summary.find({"external_user_id": user,
                                                   "external_product_id": product},
                                                  {"contributed_for_popularity": True})
        if cursor.count() > 0:
            should_increment_popularity = not cursor[0].get("contributed_for_popularity", False)

        log.info("Updating product {0} popularity".format(product))
        popularity_summary = popularity_summaries_by_product.get(product, {
            "count": 0,
            "first": pytz.utc.localize(dt.datetime(3000, 1, 1)),
            "latest": pytz.utc.localize(dt.datetime(1, 1, 1))})
        popularity_summary["first"] = min(latest_activity_date, popularity_summary["first"])
        popularity_summary["latest"] = max(latest_activity_date, popularity_summary["latest"])
        popularity_summary["count"] += 1 if should_increment_popularity else 0
        popularity_summaries_by_product[product] = popularity_summary

        done += 1
        if done % (FLUSH_SIZE // 4) == 0 or done == total_activities:
            log.info("Processed %d out of %d activities (%.2f%% done)." %
                     (done, total_activities, 100.0 * done / total_activities))

        if len(popularity_summaries_by_product) >= FLUSH_SIZE:
            flush_summaries(database, popularity_summaries_by_product)

        activity = yield_activity(activities_cursor)

    if len(popularity_summaries_by_product) > 0:
        flush_summaries(database, popularity_summaries_by_product)

    if latest_activity_date:
        log.info("Popularities summarized successfully.")

        log.info("Saving completion of popularities summary task with latest processed activity date " + \
                 str(latest_activity_date) + "...")
        save_summary_task(database, timestamp, latest_activity_date, time() - real_time_start)
    else:
        log.info("No popularities to summarize.")

    log.info("End.")


def yield_activity(cursor):
    try:
        activity = next(cursor)
    except StopIteration:
        activity = None
    return activity


def fetch_activities_cursor(database, latest_processed_activity_date, max_date, activity_types):
    where = {"activity": {"$in": activity_types}}
    if latest_processed_activity_date is not None:
        where["day"] = {"$gte": du.get_day(latest_processed_activity_date)}
    if max_date is not None:
        date_clause = where.get("created_at", {})
        date_clause.update({"$lt": max_date})
        where["created_at"] = date_clause
    fields = {"_id": False,
              "activity": True,
              "created_at": True,
              "external_user_id": True,
              "external_product_id": True}
    cursor = database.activities_summary.find(
        where, fields,
        timeout=False)
    return cursor


def save_summary_task(database, timestamp, latest_activity_date, elapsed_time):
    database.maintenance.insert({"type": "summarize_popularities",
                                 "timestamp": timestamp,
                                 "latest_activity_date": latest_activity_date,
                                 "elapsed_time_sec": elapsed_time})


def recreate_summary_collection(database):
    database.popularities_summary.drop()

    database.popularities_summary.ensure_index([("p_id", pymongo.ASCENDING),
                                                ("latest", pymongo.DESCENDING),
                                                ("popularity", pymongo.DESCENDING)])

    database.popularities_summary.ensure_index([("latest", pymongo.DESCENDING),
                                                ("p_id", pymongo.ASCENDING),
                                                ("popularity", pymongo.DESCENDING)])


def flush_summaries(database, popularity_summaries_by_product):
    log.info("Saving %d summaries..." % len(popularity_summaries_by_product))

    bulk_op = database.popularities_summary.initialize_unordered_bulk_op()

    where = {"p_id": {"$in": list(popularity_summaries_by_product.keys())}}
    fields = {"p_id": True, "count": True, "first": True, "latest": True, "_id": False}

    cursor = database.popularities_summary.find(where, fields)
    current_summaries_by_product = {rec["p_id"]: rec for rec in cursor}

    for product, popularity_summary in popularity_summaries_by_product.items():
        current_summary = current_summaries_by_product.get(product, {
            "count": 0,
            "first": pytz.utc.localize(dt.datetime(3000, 1, 1)),
            "latest": pytz.utc.localize(dt.datetime(1, 1, 1))})

        current_count = current_summary["count"]
        current_first = current_summary["first"]
        current_latest = current_summary["latest"]

        first = min(current_first, popularity_summary["first"])
        latest = max(current_latest, popularity_summary["latest"])
        new_count = current_count + popularity_summary["count"]

        if first != current_first or latest != current_latest or new_count != current_count:
            first_day = du.get_day(first)
            latest_day = du.get_day(latest)
            day_span = (latest_day - first_day).days + 1
            new_popularity = new_count / day_span

            spec = {"p_id": product}
            update_clause = {"$set": {"first": first_day,
                                      "latest": latest_day,
                                      "count": new_count,
                                      "popularity": new_popularity}}
            bulk_op.find(spec).upsert().update(update_clause)

    bulk_op.execute()

    popularity_summaries_by_product.clear()


def set_exit_handler(func):
    signal.signal(signal.SIGTERM, func)
    signal.signal(signal.SIGINT, func)


def sig_handler(sig, _):
    global should_continue
    log.info("Exit signal {0} received".format(sig))
    should_continue = False


def main(argv):
    if len(argv) < 5:
        msg = "You must specify the host, the db, the latest processed activity objectid, the max date " \
              "and the activity types"
        log.error(msg)
        return json.dumps({"success": False, "message": msg})
    try:
        # command-line arguments
        host_addr = argv[0]
        db_name = argv[1]
        database = pymongo.MongoClient(host_addr, tz_aware=True)[db_name]

        latest_processed_activity_date = argv[2]
        max_date_str = argv[3]
        activity_types = argv[4].split(",")

        if latest_processed_activity_date == "--complete":
            latest_processed_activity_date = None
        elif latest_processed_activity_date == "--resume":
            cursor = database.maintenance.find({"type": "summarize_popularities"},
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
        summarize_popularities(database, latest_processed_activity_date, max_date, activity_types)

    except Exception:
        log.exception('Exception on {0}'.format(__name__))
        return json.dumps({"success": False,
                           "message": traceback.format_exc()})

    return_json = json.dumps({"success": True})
    return return_json


if __name__ == '__main__':
    print(main(sys.argv[1:]))
