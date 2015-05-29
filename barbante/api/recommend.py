#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Gets candidate product recommendations for a target user by calling the
    appropriate algorithm. See Recommender classes for a better explanation of
    each algorithm.

    **Command-line parameters**

        *environment*
            The intended environment, as defined in mongoid.yml.

        *user_id*
            The id of the target user.

        *n_recommendations*
            The intended number of recommendations.

        *algorithm*
            The algorithm to be used.

        *filter*
            An optional context filter in json format.

    **Example of usage**

        ``python recommend.py development 02398547023 10 UBCF``

    **Output**

    Returns a JSON object as follows:
        {"products": a map {product_id : {"score" : score, "rank", rank}},
        "success": "true"}, if recommendation ran fine; {"message":
        "some error message", "success": "false"}, otherwise.

    Observe that the returned map of recommendations is not ordered. This is
    not an issue, since the recommendations *must* be ordered anyway after
    querying the database for satellite data.
"""

import sys
import traceback
from time import time

from barbante.context import init_session
import barbante.utils.logging as barbante_logging
from barbante.context.context_manager import new_context


log = barbante_logging.get_logger(__name__)


def main(argv):
    if len(argv) < 4:
        msg = "You must specify the environment, the user_id, the number of recommendations " \
            "and the algorithm."
        log.error(msg)
        return {"success": False, "message": msg}
    try:
        start = time()

        # command-line arguments
        env = argv[0]
        user_id = argv[1]
        count_recommendations = int(argv[2])
        algorithm = argv[3]
        if len(argv) >= 5:
            context_filter_string = argv[4]
        else:
            context_filter_string = None

        log.info('Initializing session...')
        session = init_session(environment=env, user_id=user_id,
                               context_filter_string=context_filter_string, algorithm=algorithm)

        log.info('Calling recommender...')
        recommender = session.get_recommender()
        results = recommender.recommend(count_recommendations)

    except Exception as ex:
        log.exception('Exception on {0}'.format(__name__))
        return {"success": False, "message": ex.args[0], "stack_trace": traceback.format_exc()}

    if results is not None:
        log.info("Recommendation took [%.6f] seconds overall" % (time() - start))
        return {"success": True, "products": results}
    else:
        msg = "No recommendations"
        log.error(msg)
        return {"success": False, "message": msg}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
