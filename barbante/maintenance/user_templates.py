import concurrent.futures
import datetime as dt
from random import shuffle
from time import time

import barbante.config as config
from barbante.maintenance.template_consolidation import consolidate_user_templates
from barbante.utils.profiling import profile
from barbante.context.context_manager import wrap
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)

CONSERVATIVE = 0
AGGRESSIVE = 1

MIN_ACCEPTABLE_UU_STRENGTH = 0.0001


def generate_templates(session_context):
    generate_strengths(session_context)
    consolidate_user_templates(session_context)

@profile
def generate_strengths(session_context):
    """ Computes user x user strengths (from scratch) based on their past activities.
        It uses the context data proxy to read input data and write the strengths back to the database.

        :param session_context: The session context.
    """
    # drops the collections and recreates the necessary indexes
    session_context.data_proxy.reset_user_user_strength_auxiliary_data()

    # registers the start of the operation and the cutoff date
    timestamp = session_context.get_present_date()
    cutoff_date = timestamp - dt.timedelta(session_context.user_user_strengths_window)
    real_time_start = time()

    products_list = [p for p in session_context.data_proxy.fetch_all_product_ids(
        allow_deleted=True, min_date=session_context.long_term_cutoff_date,
        max_date=session_context.get_present_date())]
    total_products = len(products_list)

    # shuffles the list to balance the workers
    shuffle(products_list)

    # auxiliary in-memory maps (probably ok, linear-size in the overall number of recommendable activities)
    target_users = set()
    products_by_template_user = {}
    products_by_template_user_size = 0  # let's monitor the number of products closely, just in case

    # We process the numerators first, in parallel threads.

    n_pages = total_products // session_context.page_size_user_user_numerators + 1
    max_workers = session_context.max_workers_user_user_strengths
    pages_processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(wrap(__compute_strength_numerators), session_context,
                            page, products_list, session_context.flush_size / max_workers): page
            for page in range(n_pages)}
        for future in concurrent.futures.as_completed(future_to_page):
            products_by_template_user_partial, target_users_partial = future.result()
            target_users |= target_users_partial
            for user, other_products in products_by_template_user_partial.items():
                products = products_by_template_user.get(user, set())
                old_size_for_this_user = len(products)
                products |= other_products
                new_size_for_this_user = len(products)
                products_by_template_user[user] = products
                products_by_template_user_size += new_size_for_this_user - old_size_for_this_user
            pages_processed += 1
            log.info("Processed [{0}] pages out of [{1}] during u-u strengths generation (numerators)".format(
                pages_processed, n_pages))
            log.info("In-memory products_by_template_user map size = %d users, %d products"
                     % (len(products_by_template_user), products_by_template_user_size))

    log.info("All numerators saved")

    del products_list

    # Now we know the user pairs that have non-zero strengths, we can process the denominators and the strengths.

    target_users_list = list(target_users)
    total_users = len(target_users_list)
    n_pages = total_users // session_context.page_size_user_user_denominators + 1
    pages_processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(wrap(__compute_denominators_and_strengths), session_context, page,
                            target_users_list, products_by_template_user,
                            session_context.flush_size / max_workers): page
            for page in range(n_pages)}

        for _ in concurrent.futures.as_completed(future_to_page):
            pages_processed += 1
            log.info("Processed [{0}] pages out of [{1}] during u-u strengths generation (denominators)".format(
                pages_processed, n_pages))

    # Finalizes batch write.

    log.info("Persisting data about activities considered in this batch...")
    session_context.data_proxy.copy_all_latest_activities_for_user_user_strengths(cutoff_date)

    session_context.data_proxy.hotswap_uu_strengths()

    session_context.data_proxy.save_timestamp_user_user_strengths(
        timestamp, cutoff_date, time() - real_time_start)

    log.info("User-user strengths generated successfully")


@profile
def update_templates(session_context, new_activity,
                     u_p_activities_summary=None, first_impression_date=None,
                     should_lookup_activities_summary=True,
                     should_lookup_first_impression=True):
    """ Updates user x user strengths based on a single new activity.

        :param session_context: The session context.
        :param new_activity: a dict {"external_user_id": user_id,
                                     "external_product_id": product_id,
                                     "activity": activity_type,
                                     "created_at": datetime}.
        :param u_p_activities_summary: The summary of activities for that (user, product) pair, if any,
            in the form of a dict {"external_user_id": the user id,
                                   "external_product_id": the product id,
                                   "activity": the latest activity type,
                                   "created_at": the datetime of the latest activity,
                                   "uu_latest_type": the type of the latest activity to be processed for
                                                     that pair during u-u strengths calculation,
                                   "uu_latest_date": the date of the latest activity to be processed for
                                                     that pair during u-u strengths calculation}.
        :param first_impression_date: The date of the first impression, if any, the activity user has received on
            the activity product.
        :param should_lookup_activities_summary: If True and previous_activity is None, it queries the database
            for the previous activity.
        :param should_lookup_first_impression: If True and first_impression_date is None, it queries the database
            for the first impression.
    """
    log.info("Computing user-user strengths...")

    user = new_activity["external_user_id"]
    if config.is_anonymous(user):
        log.info("Anonymous users should not affect user-user strengths! Exiting now.")
        return

    product = new_activity["external_product_id"]
    activity_date = new_activity["created_at"]
    activity_type = new_activity["activity"]
    rating = session_context.rating_by_activity.get(activity_type)
    if rating is None:
        log.error("Unsupported activity type: %s" % activity_type)
        return

    suggested_cutoff_date = session_context.get_present_date() - \
                            dt.timedelta(session_context.user_user_strengths_window)
    latest_batch_info = session_context.data_proxy.fetch_latest_batch_info_user_user_strengths()
    if latest_batch_info is not None:
        latest_batch_timestamp = latest_batch_info["timestamp"]
        persisted_cutoff_date = latest_batch_info.get("cutoff_date")
        if persisted_cutoff_date is None:
            cutoff_date = suggested_cutoff_date
        else:
            cutoff_date = max(persisted_cutoff_date, suggested_cutoff_date)
    else:
        latest_batch_timestamp = None
        cutoff_date = suggested_cutoff_date

    if session_context.impressions_enabled and first_impression_date is None and should_lookup_first_impression:
        product_user_impressions_summary = session_context.data_proxy.fetch_impressions_summary(
            product_ids=[product],
            user_ids=[user],
            group_by_product=True,
            anonymous=False).get(product, {}).get(user, (0, None))
        first_impression_date = product_user_impressions_summary[1]

    if u_p_activities_summary is None and should_lookup_activities_summary:
        u_p_activities_summary_as_singleton_list = session_context.data_proxy.fetch_activity_summaries_by_user(
            user_ids=[user],
            product_ids=[product],
            indexed_fields_only=False,
            anonymous=False).get(user, [])
        if len(u_p_activities_summary_as_singleton_list) > 0:
            u_p_activities_summary = u_p_activities_summary_as_singleton_list[0]

    previous_activity_rating = 0
    if u_p_activities_summary is not None:
        previous_activity_type = u_p_activities_summary.get("uu_latest_type")
        if previous_activity_type is not None:
            previous_activity_rating = session_context.rating_by_activity[previous_activity_type]
            previous_activity_date = u_p_activities_summary["uu_latest_date"]

    if previous_activity_rating == rating and not session_context.impressions_enabled:
        return  # repeating the latest activity --- there is nothing to do here
                # (if using impressions, must recalculate anyway to account for latest impressions)

    numerator_diff = [0, 0]
    denominator_diff = 0

    remove_previous_activity_contribution = \
        previous_activity_rating >= min(session_context.min_rating_conservative,
                                        session_context.min_rating_recommendable_from_user)
    if remove_previous_activity_contribution:
        if session_context.impressions_enabled:
            if first_impression_date is not None:
                # must remove former contribution if impression was already processed incrementally
                remove_previous_activity_contribution = previous_activity_date >= first_impression_date
                # must remove also if generation from scratch happened after the first impression
                if not remove_previous_activity_contribution and latest_batch_timestamp is not None:
                    remove_previous_activity_contribution = latest_batch_timestamp >= first_impression_date

    # Removes the former contribution of the previous commanding activity for that (user, product) pair.
    if remove_previous_activity_contribution:
        if previous_activity_rating >= session_context.min_rating_conservative:
            numerator_diff[CONSERVATIVE] -= 1
        if previous_activity_rating >= session_context.min_rating_aggressive:
            numerator_diff[AGGRESSIVE] -= 1
        if previous_activity_rating >= session_context.min_rating_recommendable_from_user:
            denominator_diff -= 1

    # Adds the contribution of this activity
    if rating >= session_context.min_rating_conservative:
        numerator_diff[CONSERVATIVE] += 1
    if rating >= session_context.min_rating_aggressive:
        numerator_diff[AGGRESSIVE] += 1
    if rating >= session_context.min_rating_recommendable_from_user:
        denominator_diff += 1

    # Fetches all the users who consumed this product
    users_by_rating = session_context.data_proxy.fetch_users_by_rating_by_product(
        product_ids=[product],
        min_date=cutoff_date,
        max_date=session_context.get_present_date())[0].get(product, {})

    # Includes the user of the current activity (remember: this activity might not have been saved yet)
    users_set = users_by_rating.get(rating, set())
    users_set.add(user)
    users_by_rating[rating] = users_set
    if u_p_activities_summary is not None:
        users_set = users_by_rating.get(previous_activity_rating, set())
        if user in users_set:
            users_set.remove(user)
            users_by_rating[previous_activity_rating] = users_set

    users_who_rated_conservatively_high = set()
    for r in range(session_context.min_rating_conservative, 6):
        users_who_rated_conservatively_high |= users_by_rating.get(r, set())
    users_who_rated_aggressively_high = set()
    for r in range(session_context.min_rating_aggressive, 6):
        users_who_rated_aggressively_high |= users_by_rating.get(r, set())
    users_who_rated_sufficiently_for_recommendation = set()
    for r in range(session_context.min_rating_recommendable_from_user, 6):
        users_who_rated_sufficiently_for_recommendation |= users_by_rating.get(r, set())

    numerators_with_user_as_target = None
    denominators_with_user_as_target = None
    numerators_with_user_as_template = None
    denominators_with_user_as_template = None
    strengths_map_for_insert = {}
    strengths_map_for_update = {}

    # This user as TARGET

    # If this user has consumed this product without previous impressions, then it shall not contribute
    # for user-user strengths with this user as target.
    update_user_as_target = True
    if session_context.impressions_enabled:
        update_user_as_target = first_impression_date is not None

    # Existing pairs with user as target.

    if update_user_as_target and numerator_diff != [0, 0]:
        strength_operands_with_user_as_target = session_context.data_proxy.fetch_user_user_strength_operands(
            users=[user])
        numerators_with_user_as_target = strength_operands_with_user_as_target[0]
        denominators_with_user_as_target = strength_operands_with_user_as_target[1]

        for user_and_template, numerator_tuple in numerators_with_user_as_target.items():
            template = user_and_template[1]
            if template in users_who_rated_sufficiently_for_recommendation:
                new_numerator_tuple = [numerator_tuple[0] + numerator_diff[0], numerator_tuple[1] + numerator_diff[1]]
                numerators_with_user_as_target[user_and_template] = new_numerator_tuple
                update_doc = strengths_map_for_update.get(user_and_template, {})
                update_doc["nc"] = new_numerator_tuple[CONSERVATIVE]
                update_doc["na"] = new_numerator_tuple[AGGRESSIVE]
                strengths_map_for_update[user_and_template] = update_doc

    # New pairs with user as target.

    if update_user_as_target and numerator_diff[0] == 1:  # if this user has *just* rated this product high...
        new_templates = []
        for template in users_who_rated_sufficiently_for_recommendation:
            if template != user and (user, template) not in numerators_with_user_as_target:  # new pair
                new_templates += [template]
                new_numerator_tuple = [1 if rating >= session_context.min_rating_conservative else 0,
                                       1 if rating >= session_context.min_rating_aggressive else 0]
                numerators_with_user_as_target[(user, template)] = new_numerator_tuple
                update_doc = strengths_map_for_insert.get((user, template), {})
                update_doc["nc"] = new_numerator_tuple[CONSERVATIVE]
                update_doc["na"] = new_numerator_tuple[AGGRESSIVE]
                strengths_map_for_insert[(user, template)] = update_doc

        products_by_rating_by_new_template = session_context.data_proxy.fetch_products_by_rating_by_user(
            user_ids=new_templates,
            min_date=cutoff_date,
            max_date=session_context.get_present_date())[0]

        for new_template in new_templates:
            recommendable_products = set()
            for r in range(session_context.min_rating_recommendable_from_user, 6):
                recommendable_products |= products_by_rating_by_new_template[new_template][r]
            if session_context.impressions_enabled:
                # Retrieves the intersection of the recommendable products of the template user
                # with the products with impressions for the target user
                recommendable_products_with_impressions = \
                    session_context.data_proxy.fetch_products_with_impressions_by_user(
                        user_ids=[user],
                        product_ids=list(recommendable_products),
                        anonymous=False).get(user, set())
                new_denominator = len(recommendable_products_with_impressions)
            else:
                new_denominator = len(recommendable_products)
            denominators_with_user_as_target[(user, new_template)] = new_denominator
            insert_doc = strengths_map_for_insert.get((user, new_template), {})
            insert_doc["denominator"] = new_denominator
            strengths_map_for_insert[(user, new_template)] = insert_doc

    # This user as TEMPLATE

    # Existing pairs with user as template.

    if session_context.bidirectional_uu_strength_updates and denominator_diff != 0:
        user_user_strength_operands = session_context.data_proxy.fetch_user_user_strength_operands(
            templates=[user])
        numerators_with_user_as_template = user_user_strength_operands[0]
        denominators_with_user_as_template = user_user_strength_operands[1]

        for user_and_template in denominators_with_user_as_template:
            # updates the denominator...
            denominator = denominators_with_user_as_template[user_and_template]
            new_denominator = denominator + denominator_diff
            denominators_with_user_as_template[user_and_template] = new_denominator
            update_doc = strengths_map_for_update.get(user_and_template, {})
            update_doc["denominator"] = new_denominator
            strengths_map_for_update[user_and_template] = update_doc

            # ...and the numerator, in case the target user has consumed this product
            if user_and_template[0] in users_who_rated_conservatively_high and \
                    user_and_template in numerators_with_user_as_template:
                numerator_tuple = numerators_with_user_as_template[user_and_template]
                numerator_tuple[CONSERVATIVE] += denominator_diff
                if user_and_template[0] in users_who_rated_aggressively_high:
                    numerator_tuple[AGGRESSIVE] += denominator_diff
                numerators_with_user_as_template[user_and_template] = numerator_tuple
                update_doc = strengths_map_for_update.get(user_and_template, {})
                update_doc["nc"] = numerator_tuple[CONSERVATIVE]
                update_doc["na"] = numerator_tuple[AGGRESSIVE]
                strengths_map_for_update[user_and_template] = update_doc

    # New pairs with user as template.

    if session_context.bidirectional_uu_strength_updates and denominator_diff == 1:
    # if this user has *just* rated this product aggressively high...
        new_targets = []
        for target in users_who_rated_conservatively_high:
            if target != user and (target, user) not in denominators_with_user_as_template:  # it is a new pair indeed
                new_targets += [target]

        if len(new_targets) > 0:
            products_of_user_as_template = session_context.data_proxy.fetch_products_by_rating_by_user(
                user_ids=[user],
                min_date=cutoff_date,
                max_date=session_context.get_present_date())[0].get(user, {})
            # Includes the product of the current activity (remember again: this activity might not have been saved yet)
            products_set = products_of_user_as_template.get(rating, set())
            products_set.add(product)
            products_of_user_as_template[rating] = products_set

            recommendable_products_of_user_as_template = set()
            for r in range(session_context.min_rating_recommendable_from_user, 6):
                recommendable_products_of_user_as_template |= products_of_user_as_template.get(r, set())

            if session_context.impressions_enabled:
                product_impressions_by_target = session_context.data_proxy.fetch_impressions_summary(
                    user_ids=new_targets,
                    product_ids=list(recommendable_products_of_user_as_template),
                    group_by_product=False,
                    anonymous=False)

            for new_target in new_targets:
                if session_context.impressions_enabled:
                    new_denominator = len(product_impressions_by_target.get(new_target, []))
                else:
                    new_denominator = len(recommendable_products_of_user_as_template)
                denominators_with_user_as_template[(new_target, user)] = new_denominator
                insert_doc = strengths_map_for_insert.get((new_target, user), {})
                insert_doc["denominator"] = new_denominator
                strengths_map_for_insert[(new_target, user)] = insert_doc

            for new_target in new_targets:
                if new_target in users_who_rated_conservatively_high:
                    numerator_tuple = numerators_with_user_as_template.get((new_target, user), [0, 0])
                    numerator_tuple[CONSERVATIVE] += 1
                    if new_target in users_who_rated_aggressively_high:
                        numerator_tuple[AGGRESSIVE] += 1
                    numerators_with_user_as_template[(new_target, user)] = numerator_tuple
                    insert_doc = strengths_map_for_insert.get((new_target, user), {})
                    insert_doc["nc"] = numerator_tuple[CONSERVATIVE]
                    insert_doc["na"] = numerator_tuple[AGGRESSIVE]
                    strengths_map_for_insert[(new_target, user)] = insert_doc

    # Computes all affected strengths for UPDATE

    if len(strengths_map_for_update) > 0:
        _prepare_strengths_map(session_context, user, strengths_map_for_update,
                               numerators_with_user_as_target, denominators_with_user_as_target,
                               numerators_with_user_as_template, denominators_with_user_as_template)

        log.info("Saving user-user strengths (UPDATE)...")
        session_context.data_proxy.save_uu_strengths(strengths_map_for_update, upsert=True)
        log.info("[{0}] user-user strengths updated".format(len(strengths_map_for_update)))
    else:
        log.info("No old strengths to update.")

    # Computes all affected strengths for INSERT

    if len(strengths_map_for_insert) > 0:
        _prepare_strengths_map(session_context, user, strengths_map_for_insert,
                               numerators_with_user_as_target, denominators_with_user_as_target,
                               numerators_with_user_as_template, denominators_with_user_as_template)

        log.info("Saving user-user strengths (INSERT)...")
        session_context.data_proxy.save_uu_strengths(strengths_map_for_insert, upsert=False)
        log.info("[{0}] user-user strengths inserted".format(len(strengths_map_for_insert)))
    else:
        log.info("No new strengths to insert.")

    # Consolidates cached user templates

    log.info("Determining users whose templates must be consolidated...")
    users_to_consolidate = {user_and_template[0] for user_and_template in strengths_map_for_insert}

    updated_users = {user_and_template[0] for user_and_template in strengths_map_for_update}
    old_templates_map = session_context.data_proxy.fetch_user_templates(list(updated_users))
    for user_and_template, strength_doc in strengths_map_for_update.items():
        target_user = user_and_template[0]
        template_user = user_and_template[1]
        old_templates = old_templates_map.get(target_user)
        if old_templates:
            cutoff_strength = old_templates[-1][0]  # the strength of the weakest template
            if isinstance(cutoff_strength, str):
                cutoff_strength = 0
            old_template_ids = {t[1] for t in old_templates}
        else:
            cutoff_strength = 0
            old_template_ids = set()
        if strength_doc["strength"] > cutoff_strength or \
           template_user in old_template_ids or \
           len(old_template_ids) < session_context.user_templates_count:
            users_to_consolidate.add(target_user)

    if session_context.should_consolidate_user_templates_on_the_fly:
        if len(users_to_consolidate) > 0:
            log.info("Consolidating templates of %d users..." % len(users_to_consolidate))
            consolidate_user_templates(session_context, users_list=list(users_to_consolidate))
        else:
            log.info("No users with templates to consolidate.")

    session_context.data_proxy.save_latest_activity_for_user_user_strengths(
        user, product, activity_type, activity_date)

    log.info("UU strengths and templates updated successfully.")


def _compute_strength_value(session_context, numerator_tuple, denominator):
    result = (numerator_tuple[AGGRESSIVE] * session_context.risk_factor +
              numerator_tuple[CONSERVATIVE] * (1 - session_context.risk_factor)) / denominator
    if result < MIN_ACCEPTABLE_UU_STRENGTH:
        result = 0
    return result


def _prepare_strengths_map(session_context, user, strengths_map,
                           numerators_with_user_as_target, denominators_with_user_as_target,
                           numerators_with_user_as_template, denominators_with_user_as_template):
    for user_and_template in strengths_map:
        if user_and_template[0] == user:
            numerator_tuple = numerators_with_user_as_target.get(user_and_template, [0, 0])
            denominator = denominators_with_user_as_target.get(user_and_template, 1)
        else:
            numerator_tuple = numerators_with_user_as_template.get(user_and_template, [0, 0])
            denominator = denominators_with_user_as_template.get(user_and_template, 1)

        # Computes the strength based on the customer-defined risk factor.
        if denominator == 0 or numerator_tuple[CONSERVATIVE] < session_context.min_user_user_strength_numerator:
            strength = 0
        else:
            strength = _compute_strength_value(session_context, numerator_tuple, denominator)

        strengths_map[user_and_template]["strength"] = strength
        strengths_map[user_and_template]["user"] = user_and_template[0]
        strengths_map[user_and_template]["template_user"] = user_and_template[1]


def __compute_strength_numerators(context, page, products_list, flush_size):
    context = context.new_session()
    start_idx = page * context.page_size_user_user_numerators
    end_idx = min((page + 1) * context.page_size_user_user_numerators, len(products_list))
    cutoff_date = context.get_present_date() - dt.timedelta(context.user_user_strengths_window)

    strength_numerators = {}
    products_by_template_user = {}
    all_target_users = set()

    page_product_ids = products_list[start_idx:end_idx]
    log.info("[Page %d] Querying db for products [%d] to [%d] out of [%d]..." %
             (page + 1, start_idx + 1, end_idx, len(products_list)))
    users_by_rating_by_product, count_users_by_rating = context.data_proxy.fetch_users_by_rating_by_product(
        product_ids=page_product_ids,
        min_date=cutoff_date,
        max_date=context.get_present_date())
    page_products_count = len(page_product_ids)
    log.info("[Page %d] Retrieved %d (implicit) ratings %s for [%d] products in the last %d days" %
             (page + 1, sum(count_users_by_rating), count_users_by_rating,
              page_products_count, context.user_user_strengths_window))

    log.info("[Page %d] Processing numerators..." % (page + 1))
    for product, users_by_rating in users_by_rating_by_product.items():

        target_users = set()
        for rating in range(context.min_rating_conservative, 6):
            target_users |= users_by_rating[rating]

        template_users = set()
        for rating in range(context.min_rating_recommendable_from_user, 6):
            template_users |= users_by_rating[rating]

        if context.impressions_enabled:
            # gathers users with impressions on the product
            # (the product might have been consumed by a user without a previous impression,
            #  and considering such users would bring about a serious bias)
            log.debug("[Page %d] Retrieving users with impressions..." % (page + 1))
            target_users = context.data_proxy.fetch_users_with_impressions_by_product(
                product_ids=[product],
                user_ids=list(target_users),
                anonymous=False).get(product, set())

        all_target_users |= target_users

        for template_user in template_users:
            template_user_products = products_by_template_user.get(template_user, set())
            template_user_products.add(product)
            products_by_template_user[template_user] = template_user_products

            for rating in range(context.min_rating_conservative, 6):
                for target_user in users_by_rating[rating]:
                    if target_user not in target_users:
                        continue

                    if target_user == template_user:
                        continue

                    strength_num = strength_numerators.get((target_user, template_user), [0, 0])
                    strength_num[CONSERVATIVE] += 1
                    if rating >= context.min_rating_aggressive:
                        strength_num[AGGRESSIVE] += 1
                    strength_numerators[(target_user, template_user)] = strength_num

                    if len(strength_numerators) >= flush_size:
                        _flush_numerators(context, strength_numerators)

    if len(strength_numerators) > 0:
        _flush_numerators(context, strength_numerators)

    return products_by_template_user, all_target_users


def __compute_denominators_and_strengths(context, page, target_users_list, products_by_template_user, flush_size):
    context = context.new_session()
    total_users = len(target_users_list)
    start_idx = page * context.page_size_user_user_denominators
    end_idx = min((page + 1) * context.page_size_user_user_denominators, total_users)
    page_user_ids = target_users_list[start_idx:end_idx]

    log.info("[Page %d] Querying db for strength numerators with target users [%d] to [%d] out of [%d]..." %
             (page, start_idx + 1, end_idx, total_users))
    strength_numerators = context.data_proxy.fetch_user_user_strength_operands(
        users=page_user_ids, group_by_target=True, numerators_only=True)[0]
    log.info("[Page %d] Retrieved user-user strength numerators for [%d] target users" %
             (page, len(strength_numerators)))

    log.info("[Page %d] Processing denominators and consolidating strengths..." % page)

    strengths_map = {}

    for target_user, numerators_by_template_user in strength_numerators.items():

        if context.impressions_enabled:
            # Retrieves the products with impressions for the target user.
            products_with_impressions = context.data_proxy.fetch_products_with_impressions_by_user(
                user_ids=[target_user], anonymous=False).get(target_user, set())

        for template_user in numerators_by_template_user:
            numerator_tuple = numerators_by_template_user[template_user]

            if context.impressions_enabled:
                # Computes the intersection of the top-rated products of the template user
                # with the products with impressions for the target user.
                intersection = products_by_template_user[template_user] & products_with_impressions
                denominator = len(intersection)
            else:
                denominator = len(products_by_template_user[template_user])

            # Computes the strength based on the customer-defined risk factor.
            if denominator == 0 or numerator_tuple[CONSERVATIVE] < context.min_user_user_strength_numerator:
                strength = 0
            else:
                strength = _compute_strength_value(context, numerator_tuple, denominator)

            strength_doc = {"user": target_user,
                            "template_user": template_user,
                            "nc": numerator_tuple[CONSERVATIVE],
                            "na": numerator_tuple[AGGRESSIVE],
                            "denominator": denominator,
                            "strength": strength}

            strengths_map[(target_user, template_user)] = strength_doc

            if len(strengths_map) >= flush_size:
                _flush_strengths(context, strengths_map)

    if len(strengths_map) > 0:
        _flush_strengths(context, strengths_map)


def _flush_numerators(context, strength_numerators):
    log.debug("Saving {0} increments to user-user strength numerators...".format(len(strength_numerators)))
    context.data_proxy.save_user_user_numerators(strength_numerators, increment=True, upsert=True)
    strength_numerators.clear()
    log.debug("User-user strength numerator increments saved")


def _flush_strengths(context, strengths_map):
    log.debug("Saving {0} user-user strengths...".format(len(strengths_map)))
    context.data_proxy.save_uu_strengths(strengths_map, upsert=False, deferred_publication=True)
    strengths_map.clear()
    log.debug("User-user strengths saved")


def get_user_templates(context, user_id):
    """ Retrieves the top n_templates product templates of all given products.

        :param context: A session context.
        :param user_id: A list with the ids of the intended users.

        :returns: A map {user_id: list of [strength, template_id] pairs}.
    """
    return context.data_proxy.fetch_user_templates([user_id]).get(user_id, [])
