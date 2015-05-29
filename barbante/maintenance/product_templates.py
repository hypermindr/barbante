import concurrent.futures
import datetime as dt
from random import shuffle
from time import time

import barbante.config as config
from barbante.maintenance.template_consolidation import consolidate_product_templates
from barbante.utils.profiling import profile
from barbante.context.context_manager import wrap
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)

CONSERVATIVE = 0
AGGRESSIVE = 1

MIN_ACCEPTABLE_PP_STRENGTH = 0.0001


def generate_templates(session_context):
    generate_strengths(session_context)
    consolidate_product_templates(session_context, collaborative=True, tfidf=False)


@profile
def generate_strengths(session_context):
    """ Computes product x product strengths (from scratch) based on the users' activities.
        It uses the context data proxy to read input data and write the strengths back to the database.

        :param session_context: The session context.
    """
    # drops the collections and recreates the necessary indexes
    session_context.data_proxy.reset_product_product_strength_auxiliary_data()

    # registers the start of the operation and the cutoff_date
    timestamp = session_context.get_present_date()
    cutoff_date = timestamp - dt.timedelta(session_context.product_product_strengths_window)
    real_time_start = time()

    users_list = [u for u in session_context.data_proxy.fetch_all_user_ids()]
    total_users = len(users_list)

    # shuffles the list to balance the workers
    shuffle(users_list)

    # auxiliary in-memory maps (probably ok, linear-size in the overall number of recommendable activities)
    template_products = set()
    users_by_base_product = {}
    users_by_base_product_size = 0  # let's monitor the number of users closely, just in case

    # We process the numerators first, in parallel threads.

    n_pages = total_users // session_context.page_size_product_product_numerators + 1
    max_workers = session_context.max_workers_product_product_strengths
    pages_processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(wrap(__compute_strength_numerators), session_context,
                            page, users_list, session_context.flush_size / max_workers): page
            for page in range(n_pages)}
        for future in concurrent.futures.as_completed(future_to_page):
            users_by_base_product_partial, template_products_partial = future.result()
            template_products |= template_products_partial
            for product, other_users in users_by_base_product_partial.items():
                users = users_by_base_product.get(product, set())
                old_size_for_this_product = len(users)
                users |= other_users
                new_size_for_this_product = len(users)
                users_by_base_product[product] = users
                users_by_base_product_size += new_size_for_this_product - old_size_for_this_product
            pages_processed += 1
            log.info("Processed [{0}] pages out of [{1}] during p-p strengths generation (numerators)".format(
                pages_processed, n_pages))
            log.info("In-memory users_by_base_product map size = %d products, %d users"
                     % (len(users_by_base_product), users_by_base_product_size))

    log.info("All numerators saved")

    del users_list

    # Now we know the product pairs that have non-zero strengths, we can process the denominators and the strengths.

    template_products_list = list(template_products)
    total_products = len(template_products_list)
    n_pages = total_products // session_context.page_size_product_product_denominators + 1
    pages_processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(wrap(__compute_denominators_and_strengths), session_context, page,
                            template_products_list, users_by_base_product,
                            session_context.flush_size / max_workers): page
            for page in range(n_pages)}

        for _ in concurrent.futures.as_completed(future_to_page):
            pages_processed += 1
            log.info("Processed [{0}] pages out of [{1}] during p-p strengths generation (denominators)".format(
                pages_processed, n_pages))

    # Finalizes batch write.

    log.info("Persisting data about activities considered in this batch...")
    session_context.data_proxy.copy_all_latest_activities_for_product_product_strengths(cutoff_date)

    session_context.data_proxy.hotswap_pp_strengths()

    session_context.data_proxy.save_timestamp_product_product_strengths(
        timestamp, cutoff_date, time() - real_time_start)

    log.info("Product-product strengths generated successfully")


@profile
def update_templates(session_context, new_activity,
                     u_p_activities_summary=None, first_impression_date=None,
                     should_lookup_activities_summary=True,
                     should_lookup_first_impression=True):
    """ Updates product x product strengths based on a single new activity.

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
                                   "pp_latest_type": the type of the latest activity to be processed for
                                                     that pair during p-p strengths calculation,
                                   "pp_latest_date": the date of the latest activity to be processed for
                                                     that pair during p-p strengths calculation}.
        :param first_impression_date: The date of the first impression, if any, the activity user has received on
            the activity product.
        :param should_lookup_activities_summary: If True and previous_activity is None, it queries the database
            for the previous activity.
        :param should_lookup_first_impression: If True and first_impression_date is None, it queries the database
            for the first impression.
    """
    log.info("Computing product-product strengths...")

    user = new_activity["external_user_id"]
    if config.is_anonymous(user):
        log.info("Anonymous users should not affect product-product strengths! Exiting now.")
        return

    product = new_activity["external_product_id"]
    activity_date = new_activity["created_at"]
    activity_type = new_activity["activity"]
    rating = session_context.rating_by_activity.get(activity_type)
    if rating is None:
        log.error("Unsupported activity type: %s" % activity_type)
        return

    suggested_cutoff_date = session_context.get_present_date() - \
                            dt.timedelta(session_context.product_product_strengths_window)
    latest_batch_info = session_context.data_proxy.fetch_latest_batch_info_product_product_strengths()
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
        previous_activity_type = u_p_activities_summary.get("pp_latest_type")
        if previous_activity_type is not None:
            previous_activity_rating = session_context.rating_by_activity[previous_activity_type]
            previous_activity_date = u_p_activities_summary["pp_latest_date"]

    if previous_activity_rating == rating and not session_context.impressions_enabled:
        return  # repeating the latest activity --- there is nothing to do here
                # (if using impressions, must recalculate anyway to account for latest impressions)

    numerator_diff = [0, 0]
    denominator_diff = 0

    remove_previous_activity_contribution = \
        previous_activity_rating >= min(session_context.min_rating_conservative,
                                        session_context.min_rating_recommendable_from_product)
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
        if previous_activity_rating >= session_context.min_rating_recommendable_from_product:
            denominator_diff -= 1

    # Adds the contribution of this activity.
    if rating >= session_context.min_rating_conservative:
        numerator_diff[CONSERVATIVE] += 1
    if rating >= session_context.min_rating_aggressive:
        numerator_diff[AGGRESSIVE] += 1
    if rating >= session_context.min_rating_recommendable_from_product:
        denominator_diff += 1

    # Fetches all the products consumed by this user.
    products_by_rating = session_context.data_proxy.fetch_products_by_rating_by_user(
        user_ids=[user],
        min_date=cutoff_date,
        max_date=session_context.get_present_date())[0].get(user, {})

    # Includes the product of the current activity (remember: this activity might not have been saved yet)
    products_set = products_by_rating.get(rating, set())
    products_set.add(product)
    products_by_rating[rating] = products_set
    if u_p_activities_summary is not None:
        products_set = products_by_rating.get(previous_activity_rating, set())
        if product in products_set:
            products_set.remove(product)
            products_by_rating[previous_activity_rating] = products_set

    products_rated_conservatively_high = set()
    for r in range(session_context.min_rating_conservative, 6):
        products_rated_conservatively_high |= products_by_rating.get(r, set())
    products_rated_aggressively_high = set()
    for r in range(session_context.min_rating_aggressive, 6):
        products_rated_aggressively_high |= products_by_rating.get(r, set())
    products_rated_sufficiently_for_recommendation = set()
    for r in range(session_context.min_rating_recommendable_from_product, 6):
        products_rated_sufficiently_for_recommendation |= products_by_rating.get(r, set())

    numerators_with_product_as_template = None
    denominators_with_product_as_template = None
    numerators_with_product_as_base = None
    denominators_with_product_as_base = None
    strengths_map_for_insert = {}
    strengths_map_for_update = {}

    # This product as TEMPLATE

    # If this product has been consumed by this user without previous impressions, then it shall not contribute
    # for product-product strengths with this product as template.
    update_product_as_template = True
    if session_context.impressions_enabled:
        update_product_as_template = first_impression_date is not None

    # Existing pairs with product as template.

    if update_product_as_template and numerator_diff != [0, 0]:

        strength_operands_with_product_as_template = session_context.data_proxy.fetch_product_product_strength_operands(
            templates=[product])
        numerators_with_product_as_template = strength_operands_with_product_as_template[0]
        denominators_with_product_as_template = strength_operands_with_product_as_template[1]

        for product_and_template, numerator_tuple in numerators_with_product_as_template.items():
            base_product = product_and_template[0]
            if base_product in products_rated_sufficiently_for_recommendation:
                new_numerator_tuple = [numerator_tuple[0] + numerator_diff[0], numerator_tuple[1] + numerator_diff[1]]
                numerators_with_product_as_template[product_and_template] = new_numerator_tuple
                update_doc = strengths_map_for_update.get(product_and_template, {})
                update_doc["nc"] = new_numerator_tuple[CONSERVATIVE]
                update_doc["na"] = new_numerator_tuple[AGGRESSIVE]
                strengths_map_for_update[product_and_template] = update_doc

    # New pairs with product as template.

    if update_product_as_template and numerator_diff[0] == 1:  # if this user has *just* rated this product high...
        new_base_products = []
        for base_product in products_rated_sufficiently_for_recommendation:
            if base_product != product and (base_product, product) not in numerators_with_product_as_template:
                new_base_products += [base_product]
                new_numerator_tuple = [1 if rating >= session_context.min_rating_conservative else 0,
                                       1 if rating >= session_context.min_rating_aggressive else 0]
                numerators_with_product_as_template[(base_product, product)] = new_numerator_tuple
                update_doc = strengths_map_for_insert.get((base_product, product), {})
                update_doc["nc"] = new_numerator_tuple[CONSERVATIVE]
                update_doc["na"] = new_numerator_tuple[AGGRESSIVE]
                strengths_map_for_insert[(base_product, product)] = update_doc

        users_by_rating_by_new_base_product = session_context.data_proxy.fetch_users_by_rating_by_product(
            product_ids=new_base_products,
            min_date=cutoff_date,
            max_date=session_context.get_present_date())[0]

        for new_base_product in new_base_products:
            source_users = set()
            for r in range(session_context.min_rating_recommendable_from_product, 6):
                source_users |= users_by_rating_by_new_base_product[new_base_product][r]
            if session_context.impressions_enabled:
                # Retrieves the intersection of the top-rated users of the base product
                # with the users with impressions for the template product
                source_users_with_impressions = \
                    session_context.data_proxy.fetch_users_with_impressions_by_product(
                        product_ids=[product],
                        user_ids=list(source_users),
                        anonymous=False).get(product, set())
                new_denominator = len(source_users_with_impressions)
            else:
                new_denominator = len(source_users)
            denominators_with_product_as_template[(new_base_product, product)] = new_denominator
            insert_doc = strengths_map_for_insert.get((new_base_product, product), {})
            insert_doc["denominator"] = new_denominator
            strengths_map_for_insert[(new_base_product, product)] = insert_doc

    # This product as BASE PRODUCT

    # Existing pairs with product as base product.

    if session_context.bidirectional_pp_strength_updates and denominator_diff != 0:
        product_product_strength_operands = session_context.data_proxy.fetch_product_product_strength_operands(
            products=[product])
        numerators_with_product_as_base = product_product_strength_operands[0]
        denominators_with_product_as_base = product_product_strength_operands[1]

        for product_and_template in denominators_with_product_as_base:
            # updates the denominator...
            denominator = denominators_with_product_as_base[product_and_template]
            new_denominator = denominator + denominator_diff
            denominators_with_product_as_base[product_and_template] = new_denominator
            update_doc = strengths_map_for_update.get(product_and_template, {})
            update_doc["denominator"] = new_denominator
            strengths_map_for_update[product_and_template] = update_doc

            # ...and the numerator, in case the template product has been consumed by this user
            if product_and_template[1] in products_rated_conservatively_high and \
                    product_and_template in numerators_with_product_as_base:
                numerator_tuple = numerators_with_product_as_base[product_and_template]
                numerator_tuple[CONSERVATIVE] += denominator_diff
                if product_and_template[1] in products_rated_aggressively_high:
                    numerator_tuple[AGGRESSIVE] += denominator_diff
                numerators_with_product_as_base[product_and_template] = numerator_tuple
                update_doc = strengths_map_for_update.get(product_and_template, {})
                update_doc["nc"] = numerator_tuple[CONSERVATIVE]
                update_doc["na"] = numerator_tuple[AGGRESSIVE]
                strengths_map_for_update[product_and_template] = update_doc

    # New pairs with product as base product.

    if session_context.bidirectional_pp_strength_updates and denominator_diff == 1:
    # if this product has *just* been rated at least conservatively high...
        new_templates = []
        for template in products_rated_conservatively_high:
            if template != product and (product, template) not in denominators_with_product_as_base:  # new pair
                new_templates += [template]

        if len(new_templates) > 0:
            users_of_product_as_base = session_context.data_proxy.fetch_users_by_rating_by_product(
                product_ids=[product],
                min_date=cutoff_date,
                max_date=session_context.get_present_date())[0].get(product, {})
            # Includes the user of the current activity (remember again: this activity might not have been saved yet)
            users_set = users_of_product_as_base.get(rating, set())
            users_set.add(user)
            users_of_product_as_base[rating] = users_set

            recommending_users_of_product_as_base = set()
            for r in range(session_context.min_rating_recommendable_from_product, 6):
                recommending_users_of_product_as_base |= users_of_product_as_base.get(r, set())

            if session_context.impressions_enabled:
                user_impressions_by_template = session_context.data_proxy.fetch_impressions_summary(
                    product_ids=new_templates,
                    user_ids=list(recommending_users_of_product_as_base),
                    group_by_product=True,
                    anonymous=False)

            for new_template in new_templates:
                if session_context.impressions_enabled:
                    new_denominator = len(user_impressions_by_template.get(new_template, []))
                else:
                    new_denominator = len(recommending_users_of_product_as_base)
                denominators_with_product_as_base[(product, new_template)] = new_denominator
                insert_doc = strengths_map_for_insert.get((product, new_template), {})
                insert_doc["denominator"] = new_denominator
                strengths_map_for_insert[(product, new_template)] = insert_doc

            for new_template in new_templates:
                if new_template in products_rated_conservatively_high:
                    numerator_tuple = numerators_with_product_as_base.get((product, new_template), [0, 0])
                    numerator_tuple[CONSERVATIVE] += 1
                    if new_template in products_rated_aggressively_high:
                        numerator_tuple[AGGRESSIVE] += 1
                    numerators_with_product_as_base[(product, new_template)] = numerator_tuple
                    insert_doc = strengths_map_for_insert.get((product, new_template), {})
                    insert_doc["nc"] = numerator_tuple[CONSERVATIVE]
                    insert_doc["na"] = numerator_tuple[AGGRESSIVE]
                    strengths_map_for_insert[(product, new_template)] = insert_doc

    # Computes all affected strengths for UPDATE

    if len(strengths_map_for_update) > 0:
        _prepare_strengths_map(session_context, product, strengths_map_for_update,
                               numerators_with_product_as_base, denominators_with_product_as_base,
                               numerators_with_product_as_template, denominators_with_product_as_template)

        log.info("Saving product-product strengths (UPDATE)...")
        session_context.data_proxy.save_pp_strengths(strengths_map_for_update, upsert=True)
        log.info("[{0}] product-product strengths updated".format(len(strengths_map_for_update)))
    else:
        log.info("No old strengths to update.")

    # Computes all affected strengths for INSERT

    if len(strengths_map_for_insert) > 0:
        _prepare_strengths_map(session_context, product, strengths_map_for_insert,
                               numerators_with_product_as_base, denominators_with_product_as_base,
                               numerators_with_product_as_template, denominators_with_product_as_template)

        log.info("Saving product-product strengths (INSERT)...")
        session_context.data_proxy.save_pp_strengths(strengths_map_for_insert, upsert=False)
        log.info("[{0}] product-product strengths inserted".format(len(strengths_map_for_insert)))
    else:
        log.info("No new strengths to insert.")

    # Consolidates cached product templates

    log.info("Determining products whose templates must be consolidated...")
    products_to_consolidate = {product_and_template[0] for product_and_template in strengths_map_for_insert}

    updated_products = {product_and_template[0] for product_and_template in strengths_map_for_update}
    old_templates_map = session_context.data_proxy.fetch_product_templates(list(updated_products))
    for product_and_template, strength_doc in strengths_map_for_update.items():
        base_product = product_and_template[0]
        template_product = product_and_template[1]
        cutoff_strength = 0
        old_template_ids = set()
        old_templates = old_templates_map.get(base_product)
        if old_templates:
            old_templates_collaborative = old_templates[0]
            if old_templates_collaborative:
                cutoff_strength = old_templates_collaborative[-1][0]  # the strength of the weakest template
                if isinstance(cutoff_strength, str):
                    cutoff_strength = 0
                old_template_ids = {t[1] for t in old_templates_collaborative}
        if strength_doc["strength"] > cutoff_strength or \
           template_product in old_template_ids or \
           len(old_template_ids) < 3 * session_context.product_templates_count:
            products_to_consolidate.add(base_product)

    if session_context.should_consolidate_product_templates_on_the_fly:
        if len(products_to_consolidate) > 0:
            log.info("Consolidating templates of %d products..." % len(products_to_consolidate))
            consolidate_product_templates(session_context, products_list=list(products_to_consolidate),
                                          collaborative=True, tfidf=False)
        else:
            log.info("No products with templates to consolidate.")

    session_context.data_proxy.save_latest_activity_for_product_product_strengths(
        user, product, activity_type, activity_date)

    log.info("PP strengths and templates updated successfully.")


def _compute_strength_value(session_context, numerator_tuple, denominator):
    result = (numerator_tuple[AGGRESSIVE] * session_context.risk_factor +
              numerator_tuple[CONSERVATIVE] * (1 - session_context.risk_factor)) / denominator
    if result < MIN_ACCEPTABLE_PP_STRENGTH:
        result = 0
    return result


def _prepare_strengths_map(session_context, product, strengths_map,
                           numerators_with_product_as_base, denominators_with_product_as_base,
                           numerators_with_product_as_template, denominators_with_product_as_template):
    for product_and_template in strengths_map:
        if product_and_template[0] == product:
            numerator_tuple = numerators_with_product_as_base.get(product_and_template, [0, 0])
            denominator = denominators_with_product_as_base.get(product_and_template, 1)
        else:
            numerator_tuple = numerators_with_product_as_template.get(product_and_template, [0, 0])
            denominator = denominators_with_product_as_template.get(product_and_template, 1)

        # Computes the strength based on the customer-defined risk factor.
        if denominator == 0 or numerator_tuple[CONSERVATIVE] < session_context.min_product_product_strength_numerator:
            strength = 0
        else:
            strength = _compute_strength_value(session_context, numerator_tuple, denominator)

        strengths_map[product_and_template]["strength"] = strength
        strengths_map[product_and_template]["product"] = product_and_template[0]
        strengths_map[product_and_template]["template_product"] = product_and_template[1]


def __compute_strength_numerators(context, page, users_list, flush_size):
    context = context.new_session()
    start_idx = page * context.page_size_product_product_numerators
    end_idx = min((page + 1) * context.page_size_product_product_numerators, len(users_list))
    cutoff_date = context.get_present_date() - dt.timedelta(context.product_product_strengths_window)

    strength_numerators = {}
    users_by_base_product = {}
    all_template_products = set()

    page_user_ids = users_list[start_idx:end_idx]
    log.info("[Page %d] Querying db for users [%d] to [%d] out of [%d]..." %
             (page + 1, start_idx + 1, end_idx, len(users_list)))
    products_by_rating_by_user, count_products_by_rating = context.data_proxy.fetch_products_by_rating_by_user(
        user_ids=page_user_ids,
        min_date=cutoff_date,
        max_date=context.get_present_date())
    page_users_count = len(page_user_ids)
    log.info("[Page %d] Retrieved %d (implicit) ratings %s from [%d] users in the last %d days" %
             (page + 1, sum(count_products_by_rating), count_products_by_rating,
              page_users_count, context.product_product_strengths_window))

    log.info("[Page %d] Processing numerators..." % (page + 1))
    for user, products_by_rating in products_by_rating_by_user.items():

        template_products = set()
        for rating in range(context.min_rating_conservative, 6):
            template_products |= products_by_rating[rating]

        base_products = set()
        for rating in range(context.min_rating_recommendable_from_product, 6):
            base_products |= products_by_rating[rating]

        if context.impressions_enabled:
            # gathers products the user has had impressions on
            # (the user might have consumed a product without a previous impression,
            # and considering such products would bring about a serious bias)
            template_products = context.data_proxy.fetch_products_with_impressions_by_user(
                user_ids=[user],
                product_ids=list(template_products),
                anonymous=False).get(user, set())

        all_template_products |= template_products

        for base_product in base_products:
            base_product_users = users_by_base_product.get(base_product, set())
            base_product_users.add(user)
            users_by_base_product[base_product] = base_product_users

            for rating in range(context.min_rating_conservative, 6):
                for template_product in products_by_rating[rating]:
                    if template_product not in template_products:
                        continue

                    if template_product == base_product:
                        continue

                    strength_num = strength_numerators.get((base_product, template_product), [0, 0])
                    strength_num[CONSERVATIVE] += 1
                    if rating >= context.min_rating_aggressive:
                        strength_num[AGGRESSIVE] += 1
                    strength_numerators[(base_product, template_product)] = strength_num

                    if len(strength_numerators) >= flush_size:
                        _flush_numerators(context, strength_numerators)

    if len(strength_numerators) > 0:
        _flush_numerators(context, strength_numerators)

    return users_by_base_product, all_template_products


def __compute_denominators_and_strengths(context, page, template_products_list, users_by_base_product, flush_size):
    context = context.new_session()
    total_products = len(template_products_list)
    start_idx = page * context.page_size_product_product_denominators
    end_idx = min((page + 1) * context.page_size_product_product_denominators, total_products)
    page_product_ids = template_products_list[start_idx:end_idx]

    log.info("[Page %d] Querying db for numerators with template products [%d] to [%d] out of [%d]..." %
             (page, start_idx + 1, end_idx, total_products))
    strength_numerators = context.data_proxy.fetch_product_product_strength_operands(
        templates=page_product_ids, group_by_template=True, numerators_only=True)[0]
    log.info("[Page %d] Retrieved product-product strength numerators for [%d] template products" %
             (page, len(strength_numerators)))

    log.info("[Page %d] Processing denominators and consolidating strengths..." % page)

    strengths_map = {}

    for template_product, numerators_by_base_product in strength_numerators.items():

        if context.impressions_enabled:
            # Retrieves the users who received impressions of the template product.
            users_with_impressions = context.data_proxy.fetch_users_with_impressions_by_product(
                product_ids=[template_product], anonymous=False).get(template_product, set())

        for base_product in numerators_by_base_product:
            numerator_tuple = numerators_by_base_product[base_product]

            if context.impressions_enabled:
                # Computes the intersection of the users who rated the base product sufficiently high
                # with the users who received impressions of the template product.
                intersection = users_by_base_product[base_product] & users_with_impressions
                denominator = len(intersection)
            else:
                denominator = len(users_by_base_product[base_product])

            # Computes the strength based on the customer-defined risk factor.
            if denominator == 0 or numerator_tuple[CONSERVATIVE] < context.min_product_product_strength_numerator:
                strength = 0
            else:
                strength = _compute_strength_value(context, numerator_tuple, denominator)

            strength_doc = {"product": base_product,
                            "template_product": template_product,
                            "nc": numerator_tuple[CONSERVATIVE],
                            "na": numerator_tuple[AGGRESSIVE],
                            "denominator": denominator,
                            "strength": strength}

            strengths_map[(base_product, template_product)] = strength_doc

            if len(strengths_map) >= flush_size:
                _flush_strengths(context, strengths_map)

    if len(strengths_map) > 0:
        _flush_strengths(context, strengths_map)


def _flush_numerators(context, strength_numerators):
    log.debug("Saving {0} increments to product-product strength numerators...".format(len(strength_numerators)))
    context.data_proxy.save_product_product_numerators(strength_numerators, increment=True, upsert=True)
    strength_numerators.clear()
    log.debug("Product-product strength numerator increments saved")


def _flush_strengths(context, strengths_map):
    log.debug("Saving {0} product-product strengths...".format(len(strengths_map)))
    context.data_proxy.save_pp_strengths(strengths_map, upsert=False, deferred_publication=True)
    strengths_map.clear()
    log.debug("Product-product strengths saved")


def get_product_templates(context, product_ids, blocked_products=None):
    """ Retrieves the top n_templates product templates of all given products.

        :param context: A session context.
        :param product_ids: A list with the ids of the intended products.

        :returns: A map {product_id: list of (strength, template_id) tuples}.
    """
    result = {}
    if blocked_products is None:
        blocked_products = []

    templates_map = context.data_proxy.fetch_product_templates(product_ids)
    for p_id, templates_tuple in templates_map.items():
        approved_templates = [t for t in templates_tuple[0] if t[1] not in blocked_products]
        result[p_id] = approved_templates

    return result
