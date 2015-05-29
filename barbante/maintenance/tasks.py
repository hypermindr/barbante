""" Maintenance tasks.
"""

from time import time
import traceback

import barbante.config as config
import barbante.maintenance.user_templates as ut
import barbante.maintenance.product_templates as pt
import barbante.maintenance.product_templates_tfidf as pt_tfidf
import barbante.maintenance.product as prd
import barbante.utils.logging as barbante_logging
import barbante.utils as utils
import barbante.utils.text as text


log = barbante_logging.get_logger(__name__)


def update_collaborative_filtering_strengths(session_context, activity):
    """ Updates user-user strengths and product-product strengths in conformity to the informed activity.

        :param session_context: The session context.
        :param activity: The activity which triggered the updates.
    """
    user = activity["external_user_id"]
    is_anonymous = config.is_anonymous(user)
    if is_anonymous:
        return  # we do NOT want anonymous users to influence collaborative filtering strengths!

    product = activity["external_product_id"]
    act_type = activity["activity"]

    log.info("Processing strengths [user=%s, product=%s, type=%s]..." % (user, product, act_type))
    start = time()

    u_p_activity_summary = _get_current_user_product_summary(session_context, user, product, anonymous=False)

    first_impression_date = None
    if session_context.impressions_enabled:
        product_user_impressions_summary = session_context.data_proxy.fetch_impressions_summary(
            product_ids=[product],
            user_ids=[user],
            group_by_product=True,
            anonymous=False).get(product, {}).get(user, (0, None))
        first_impression_date = product_user_impressions_summary[1]

    log.info("Updating user-user strengths affected by user/product pair ({0}, {1})...".format(user, product))
    ut.update_templates(session_context, activity, u_p_activity_summary, first_impression_date,
                        should_lookup_activities_summary=False, should_lookup_first_impression=False)
    log.info("Updating product-product strengths affected by user/product pair ({0}, {1})...".format(user, product))
    pt.update_templates(session_context, activity, u_p_activity_summary, first_impression_date,
                        should_lookup_activities_summary=False, should_lookup_first_impression=False)

    log.info("---Done processing strengths [user=%s, product=%s, type=%s] (took %.6f seconds)"
             % (user, product, act_type, time() - start))


def update_summaries(session_context, activity):
    """ Performs the following updates:
    
        1) If the informed activity is the first popularity-defining activity for that (user, product) pair,
           increments the popularity of the given product, and sets the popularity flag of that (user, pair) to True
           in the activities_summary.
           
        2) Resets the impressions count for that (user, product) pair.
        
        3) Updates the activities summary for that (user, product) pair.
    
        :param session_context: The session context. 
        :param activity: The activity being processed, i.e., the trigger to the updates.
    """
    user = activity["external_user_id"]
    is_anonymous = config.is_anonymous(user)
    product = activity["external_product_id"]
    act_type = activity["activity"]
    act_rating = session_context.rating_by_activity.get(act_type)
    date = activity["created_at"]

    log.info("Processing summaries [user=%s, product=%s, type=%s]..." % (user, product, act_type))
    start = time()

    u_p_activity_summary = _get_current_user_product_summary(session_context, user, product, anonymous=is_anonymous)

    # Product popularity (if need be)

    should_increment_popularity = act_rating >= session_context.min_rating_recommendable_from_user
    if u_p_activity_summary is not None and u_p_activity_summary["contributed_for_popularity"]:
        should_increment_popularity = False
    log.info("Updating product {0} popularity...".format(product))
    session_context.data_proxy.update_product_popularity(
        product, date, should_increment_popularity)

    # Impressions summary

    log.info("Resetting impressions for user/product pair ({0}, {1})...".format(user, product))
    session_context.data_proxy.reset_impression_summary(
        user, product, anonymous=is_anonymous)

    # Activities summary

    log.info("Updating activities summary for user/product pair ({0}, {1})...".format(user, product))
    session_context.data_proxy.save_activity_summary(
        activity, set_popularity_flag=should_increment_popularity, anonymous=is_anonymous)

    log.info("---Done processing summaries [user=%s, product=%s, type=%s] (took %.6f seconds)"
             % (user, product, act_type, time() - start))


def _get_current_user_product_summary(session_context, user, product, anonymous):
    """ Gets, via database proxy, the summary of the activities for the informed (user, product) pair.

        :param session_context: The session context.
        :param user: The intended user.
        :param product: The intended product.
        :anonymous: if True, the summary will be looked up on the anonymous collections.

        :returns: A dict {"external_user_id": user_id,
                          "external_product_id": product_id,
                          "activity": activity_type,
                          "created_at": datetime,
                          "contributed_for_popularity": True/False}
            representing the activities summary for that (user, product) pair.
            If there are no activities for that pair, returns None.
    """
    result = None
    user_product_summary_as_singleton_list = session_context.data_proxy.fetch_activity_summaries_by_user(
        user_ids=[user],
        product_ids=[product],
        indexed_fields_only=False,
        anonymous=anonymous).get(user)
    if user_product_summary_as_singleton_list is not None:
        result = user_product_summary_as_singleton_list[0]
    return result


def process_impression(session_context, impression):
    user = impression["external_user_id"]
    is_anonymous = config.is_anonymous(user)
    product = impression["external_product_id"]
    date = impression["created_at"]

    log.info("Processing impression [user=%s, product=%s]..." % (user, product))
    start = time()
    try:
        session_context.data_proxy.increment_impression_summary(user, product, date, anonymous=is_anonymous)
        log.info("---Done processing impression [user=%s, product=%s] (took %.6f seconds)"
                 % (user, product, time() - start))
    except Exception as ex:
        log.error("Error while processing impression [user=%s, product=%s], message=%s, stack_trace=%s"
                  % (user, product, ex.args[0], traceback.format_exc()))


def process_product(session_context, product_id, product=None, force_update=False):
    log.info("Processing product [%s]" % product_id)
    start = time()

    if product is None:
        product = session_context.data_proxy.fetch_products([product_id]).get(product_id)
        if product is None:
            raise ValueError("No product exists in the db with id [%s]" % product_id)

    log.info("Product [{0}] loaded".format(product_id))

    product_model, has_pre_existing_product_model = prd.prepare_product_model(
        session_context, product, force_update=force_update)

    if product_model is None:
        log.error("Error while processing product [%s]: product model was not generated" % product_id)
    else:
        language = product_model.get_attribute("language")

    product_as_dict = None
    product_model_as_dict = None

    if not has_pre_existing_product_model or force_update:

        for attribute in session_context.product_text_fields:
            if product_model_as_dict is None:
                product_model_as_dict = utils.flatten_dict(product_model.to_dict())  # lazily flattens the product model
            if attribute not in product_model_as_dict:
                if product_as_dict is None:
                    product_as_dict = utils.flatten_dict(product)  # lazily flattens the product
                value = product_as_dict.get(attribute)
                if value is not None:
                    stemmed_value = text.parse_text_to_stems(language, value)
                    product_model_as_dict[attribute] = stemmed_value

        _, _, tfidf_by_top_term_by_attribute = prd.prepare_product_terms(
            session_context, product_model_as_dict, reprocessing_product=has_pre_existing_product_model)

        pt_tfidf.update_templates(session_context, product_id, language, tfidf_by_top_term_by_attribute)
    log.info("---Done processing product [%s] (took %.6f seconds)" % (product_id, time() - start))

    session_context.clear_context_filters_cache()


def process_products(session_context, days=None, resume=False):
    if resume:
        # registers the start of the operation and the cutoff_date
        timestamp = session_context.get_present_date()
        real_time_start = time()

        required_fields = session_context.product_model_factory.get_custom_required_attributes()
        latest_batch_info = session_context.data_proxy.fetch_latest_batch_info_product_models()
        if latest_batch_info is not None:
            cutoff_date = latest_batch_info["timestamp"]
        else:
            cutoff_date = None

        if cutoff_date is not None:
            log.info("Loading product ids of all products updated after {0}...".format(cutoff_date))
        else:
            log.info("Loading all product ids...".format(cutoff_date))
        product_ids = [p for p in session_context.data_proxy.fetch_all_product_ids(
            allow_deleted=False, required_fields=required_fields, min_date=cutoff_date,
            product_date_field="updated_at")]

        log.info("Loaded %d products." % len(product_ids))
        for product_id in product_ids:
            process_product(session_context, product_id, force_update=True)

        session_context.data_proxy.save_timestamp_product_models(
            timestamp, cutoff_date, time() - real_time_start)

        log.info("Done.")

    else:
        prd.process_products_from_scratch(session_context, days)


def delete_product(session_context, product_id):
    prd.delete_product(session_context, product_id)
