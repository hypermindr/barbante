#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Gets candidate product recommendations for all users in the database and
    save them to file.

    Command-line parameters:
        *environment* - The db host name.

        *n_recommendations* - The intended number of recommendations.

        *algorithm* - The algorithm to be used.

    Example of usage

        ``python recommend.py development 10 UBCF``

    **Output**

    Returns a JSON object as follows:
        {"products": a map {user: {product_id : {"score" : score, "rank",
        rank}}}, "success": "true"}, if recommendation ran fine; {"message":
        "some error message", "success": "false"}, otherwise.

    Notice that the returned map of recommendations is not ordered. This is
    not an issue, since the recommendations *must* be ordered anyway after
    querying the database for satellite data.
"""

import sys
import traceback

from barbante.utils.profiling import profile
import barbante.utils.logging as barbante_logging
from barbante.context import init_session
from barbante.context.context_manager import new_context


log = barbante_logging.get_logger(__name__)


@profile
def recommend_products_all_users(env, count_recommendations, algorithm):
    result = {}
    session = init_session(env)

    all_users = session.data_proxy.fetch_all_user_ids()
    done = 0
    for user_id in all_users:
        session = init_session(env, user_id, algorithm=algorithm)
        recommender = session.get_recommender()
        recommendations = recommender.recommend(count_recommendations)
        result[user_id] = recommendations
        done += 1
        if done % 1000 == 0:
            log.debug("Processed %d users." % done)

    return result


def map_to_csv(recommendations_map, separator, n_recommendations):
    csv_as_list = []
    for user_id in recommendations_map:
        scores_by_product = recommendations_map[user_id]
        product_tuples = []
        for product_id in scores_by_product:
            product_tuple = (scores_by_product[product_id]["rank"], product_id)
            product_tuples += [product_tuple]
        sorted_product_tuples = sorted(product_tuples)
        row = [str(user_id)]
        for product_tuple in sorted_product_tuples:
            row += [str(product_tuple[1])]
        row_as_string = separator.join(row)
        csv_as_list += [row_as_string]
    csv = "USER" + separator
    for i in range(n_recommendations):
        csv += "PRODUCT_" + str(i + 1)
        if i < n_recommendations - 1:
            csv += separator
    csv += "\n" + "\n".join(csv_as_list)
    return csv


def main(argv):
    if len(argv) == 0:
        return {"success": False, "message": "You must specify the environment, the number of "
                                             "recommendations and the algorithm."}
    try:
        # command-line arguments
        export_to_csv = False
        if argv[0] == "--csv":
            export_to_csv = True
            del (argv[0])
        env = argv[0]
        count_recommendations = int(argv[1])
        algorithm = argv[2]

        results = recommend_products_all_users(env, count_recommendations, algorithm)

    except Exception:
        log.exception('Exception on {0}:'.format(__name__))
        return {"success": False, "message": traceback.format_exc()}

    if export_to_csv:
        return map_to_csv(results, ";", count_recommendations)
    else:
        if results is not None:
            return_json = {"success": True, "products": results}
            return return_json
        else:
            msg = "No recommendations"
            log.error(msg)
            return {"success": False, "message": msg}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
