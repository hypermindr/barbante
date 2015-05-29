import datetime as dt
from time import time

import barbante.context as ctx
from barbante.maintenance.product import pinpoint_near_identical_products
from barbante.maintenance.template_consolidation import consolidate_product_templates
from barbante.utils.profiling import profile
import barbante.utils.logging as barbante_logging
import barbante.model.product_model as pm


log = barbante_logging.get_logger(__name__)

MIN_ACCEPTABLE_PP_STRENGTH_TFIDF = 0.0001


def generate_templates(session_context):
    generate_strengths(session_context)
    consolidate_product_templates(session_context, collaborative=False, tfidf=True)


@profile
def generate_strengths(session_context):
    """ Generates (from scratch) product-product strengths based on their content.

        The attributes which are taken into consideration are those defined in the
        customer config file PRODUCT_MODEL entry. Product attributes whose 'similarity_filter'
        is set to true must be equal so that two products must have non-zero mutual similarity.
        Product attributes whose 'similarity_weight' is strictly positive are linearly combined
        according to the assigned weights.

        :param session_context: The session context.
    """
    # drops the collections and recreates the necessary indexes
    session_context.data_proxy.reset_product_product_strength_tfidf_auxiliary_data()

    # registers the start of the operation
    timestamp = session_context.get_present_date()
    cutoff_date = session_context.get_present_date() - dt.timedelta(
        session_context.product_product_strengths_tfidf_window)
    real_time_start = time()

    text_fields = session_context.product_text_fields

    log.info("Fetching recent products...")
    product_models = session_context.data_proxy.fetch_product_models(min_date=cutoff_date,
                                                                     max_date=session_context.get_present_date())

    log.info("Partitioning products by language...")
    product_ids_by_language = _partition_products_by_language(product_models)

    log.info("Processing %d languages..." % len(product_ids_by_language))

    for language_idx, language in enumerate(product_ids_by_language):
        strengths = {}
        language_name = language[0].upper() + language[1:]

        log.info("Language %d of %d: %s" % (language_idx + 1, len(product_ids_by_language), language_name))

        product_ids_list = list(product_ids_by_language[language])
        if len(product_ids_list) == 0:
            log.info("Skipped. No products.")
            continue

        # Processes the contributions of all TEXT fields.

        for attribute in text_fields:
            weight = session_context.similarity_weights_by_type[pm.TEXT].get(attribute, 0)
            if weight == 0:
                continue

            log.info("Fetching TFIDF maps for attribute [%s] in [%d] products..." % (attribute, len(product_ids_list)))
            tfidf_by_term_by_product = session_context.data_proxy.fetch_tfidf_map(attribute, product_ids_list)

            log.info("Computing strengths among [%d] %s documents..." % (len(product_ids_list), language_name))
            _process_text_attribute_contributions(strengths, tfidf_by_term_by_product, weight)

        # Now it processes the non-TEXT fields, but only for those product pairs which already have non-zero strengths
        # (in other words, if a pair zeroes all text fields, it won't be further considered and its strength will be 0).
        _process_non_text_attributes_contributions(session_context, product_models, strengths)

        # Saves in the db.

        if len(strengths) > 0:
            log.info("Saving [%d] product-product strengths (tfidf)..." % len(strengths))
            strengths_list = [{"product": product_pair[0],
                               "template_product": product_pair[1],
                               "strength": value if value >= MIN_ACCEPTABLE_PP_STRENGTH_TFIDF else 0}
                              for product_pair, value in strengths.items()]
            start_index = 0
            bulk_size = 50000
            total = len(strengths_list)
            while start_index < total:
                end_index = min(total, start_index + bulk_size)
                session_context.data_proxy.save_product_product_strengths_tfidf(strengths_list, start_index, end_index,
                                                                                deferred_publication=True)
                start_index += bulk_size
                log.info("...%.2f%% done." % (100.0 * end_index / total))

            log.info("Product-product strengths TFIDF saved")

    # Finalizes batch write.

    session_context.data_proxy.hotswap_product_product_strengths_tfidf()
    session_context.data_proxy.save_timestamp_product_product_strengths_tfidf(
        timestamp, cutoff_date, time() - real_time_start)

    log.info("Product-product strengths TFIDF generated successfully")

@profile
def update_templates(session_context, product_id, language, tfidf_by_top_term_by_attribute):
    """ Updates product-product strengths based on their content.

        The attributes which are taken into consideration are those defined in the
        customer config file PRODUCT_MODEL entry. Product attributes whose 'similarity_filter'
        is set to true must be equal so that two products must have non-zero mutual similarity.
        Product attributes whose 'similarity_weight' is strictly positive are linearly combined
        according to the assigned weights.

        This function does not recreate all strengths from scratch; rather, it updates
        the strengths of all product-product pairs containing the product whose *product_id* is given.

        :param session_context: The session context.
        :param product_id: The intended product.
        :param language: The language of the product being processed.
        :param tfidf_by_top_term_by_attribute: A map {attribute: {term: tfidf}}, containing the TFIDF's of
            the top TFIDF terms in each of the TEXT-type attribute of the product being processed.
    """
    strengths = {}

    text_fields = session_context.product_text_fields
    cutoff_date = session_context.get_present_date() - dt.timedelta(
        session_context.product_product_strengths_tfidf_window)

    product_models = {}

    # Processes each TEXT attribute.
    for attribute in text_fields:
        weight = session_context.similarity_weights_by_type[pm.TEXT].get(attribute, 0)
        if weight == 0:
            continue

        log.info("Fetching products with common terms in attribute [%s]..." % attribute)
        terms = [term for term in tfidf_by_top_term_by_attribute.get(attribute, [])]
        new_product_models = session_context.data_proxy.fetch_product_models_for_top_tfidf_terms(
            attribute, language, terms, min_date=cutoff_date, max_date=session_context.get_present_date())
        product_models.update(new_product_models)

        if len(new_product_models) > 1:  # we require at least one product model other than that of the current product
            product_ids_list = [p_id for p_id in new_product_models]

            log.info("Fetching TFIDF maps for attribute [%s] in [%d] products..." % (attribute, len(product_ids_list)))
            tfidf_by_term_by_product = session_context.data_proxy.fetch_tfidf_map(attribute, product_ids_list)

            log.info("Computing strengths...")
            _process_text_attribute_contributions(strengths, tfidf_by_term_by_product, weight, product_id)

    # Processes the non-TEXT attributes.
    _process_non_text_attributes_contributions(session_context, product_models, strengths)

    # Persists the updated strengths.
    log.info("Saving strengths tfidf...")
    strengths_list = [{"product": product_pair[0],
                       "template_product": product_pair[1],
                       "strength": value if value >= MIN_ACCEPTABLE_PP_STRENGTH_TFIDF else 0}
                      for product_pair, value in strengths.items()]
    session_context.data_proxy.save_product_product_strengths_tfidf(strengths_list)

    # Consolidates cached product templates

    log.info("Determining products whose templates tfidf must be consolidated...")
    products_to_consolidate = set()
    updated_products = {product_and_template[0] for product_and_template in strengths}
    old_templates_map = session_context.data_proxy.fetch_product_templates(list(updated_products))

    for product_and_template, strength in strengths.items():
        base_product = product_and_template[0]
        template_product = product_and_template[1]

        should_consolidate = True
        old_templates = old_templates_map.get(base_product)
        if old_templates is not None:
            if len(old_templates[1]) > 0:
                cutoff_strength = old_templates[1][-1][0]  # the strength of the weakest template tfidf
                old_template_ids = {t[1] for t in old_templates[1]}
                if strength <= cutoff_strength and \
                   template_product not in old_template_ids and \
                   len(old_templates) >= 3 * session_context.product_templates_count:
                    should_consolidate = False

        if should_consolidate:
            products_to_consolidate.add(base_product)

    if len(products_to_consolidate) > 0:
        log.info("Consolidating templates of %d products..." % len(products_to_consolidate))
        consolidate_product_templates(session_context, products_list=list(products_to_consolidate),
                                      collaborative=False, tfidf=True)


@profile
def _process_text_attribute_contributions(strengths, tfidf_map, weight=1, product_id=None):
    """ Adds the contribution of the terms in *tfidf_map* to the informed map of strengths.
        The contribution of each term is multiplied by *weight* before being summed to the existing strength.

        :param strengths: A map {(product, template_product): strength_value} with partially computed strengths
                       (possibly resulting from the contributions of other product attributes). The contribution
                       of the current attribute (whose map of terms by product is given as *tfidf_map*) will be added
                       to this same *strengths* map.
        :param tfidf_map: A map {product: {term: tfidf}} containing the tfidf of the most relevant terms
                       in each product.
        :param weight: The weight of the contributions (as per the definition of PRODUCT_MODEL in the
                       customer configuration).
        :param product_id: If not None, then it will just consider product pairs
                where one of the products has the given product_id. Otherwise, it will process
                all pairs formed by products which are keys in tfidf_map.
    """
    # Inner function to obtain the correct order of products to identify a product pair.
    def get_canonical_tuple(prod1, prod2):
        return min(prod1, prod2), max(prod1, prod2)

    # ---

    products_by_term = {}
    debt_by_product_and_template = {}
    sum_tfidf_by_product = {}
    common_terms_by_product_pair = {}

    log.info("Processing common terms...")
    total = len(tfidf_map)
    done = 0

    for p1 in tfidf_map:
        tfidf_by_term_in_p1 = tfidf_map.get(p1, {})
        sum_tfidf_p1 = sum(tfidf_by_term_in_p1.values())
        sum_tfidf_by_product[p1] = sum_tfidf_p1

        # For each *term* in p1, calculates the debt of all products containing *term* as a top term.
        for term in tfidf_by_term_in_p1:
            term_tfidf_p1 = tfidf_by_term_in_p1[term]

            products_with_term = products_by_term.get(term, set())
            for p2 in products_with_term:
                if p2 == p1:
                    continue

                if product_id is not None:
                    if product_id not in [p1, p2]:
                        continue

                sum_tfidf_p2 = sum_tfidf_by_product[p2]

                cannonical_tuple = get_canonical_tuple(p1, p2)
                common_terms = common_terms_by_product_pair.get(cannonical_tuple, set())
                common_terms.add(term)
                common_terms_by_product_pair[cannonical_tuple] = common_terms

                tfidf_by_term_in_p2 = tfidf_map.get(p2, {})
                term_tfidf_p2 = tfidf_by_term_in_p2[term]

                new_debt_p2_to_p1 = max(0., term_tfidf_p1 - term_tfidf_p2) * term_tfidf_p1 / sum_tfidf_p1
                debt_p2_to_p1 = debt_by_product_and_template.get((p1, p2), 0.)
                debt_p2_to_p1 += new_debt_p2_to_p1
                debt_by_product_and_template[(p1, p2)] = debt_p2_to_p1

                new_debt_p1_to_p2 = max(0., term_tfidf_p2 - term_tfidf_p1) * term_tfidf_p2 / sum_tfidf_p2
                debt_p1_to_p2 = debt_by_product_and_template.get((p2, p1), 0.)
                debt_p1_to_p2 += new_debt_p1_to_p2
                debt_by_product_and_template[(p2, p1)] = debt_p1_to_p2

            products_with_term.add(p1)
            products_by_term[term] = products_with_term

        done += 1
        log.debug_progress(done, total, 1000)

    log.info("Adding the debts w.r.t. missing terms...")
    total = len(debt_by_product_and_template)
    done = 0

    for product, template in debt_by_product_and_template:
        cannonical_tuple = get_canonical_tuple(product, template)
        common_terms = common_terms_by_product_pair[cannonical_tuple]
        tfidf_by_term_in_product = tfidf_map.get(product, {})
        sum_tfidf_in_product = sum_tfidf_by_product[product]
        for term in tfidf_by_term_in_product:
            if term not in common_terms:
                debt = debt_by_product_and_template.get((product, template), 0.)
                debt += tfidf_by_term_in_product[term] / sum_tfidf_in_product
                debt_by_product_and_template[(product, template)] = debt

        done += 1
        log.debug_progress(done, total, 1000)

    for (product, template), debt in debt_by_product_and_template.items():
        strength_contribution = (1 - debt) * weight
        old_strength = strengths.get((product, template), 0)
        new_strength = old_strength + strength_contribution
        strengths[(product, template)] = new_strength


def _process_non_text_attributes_contributions(context, products, strengths):
    """ Adds the contribution of the non-text product attributes to the informed map of strengths.

        :param context: The customer context.
        :param products: A map {product_id: record} where record is a dict {attribute: value}.
        :param strengths: A map {(product, template_product): strength_value} with partially computed strengths
                       (possibly resulting from the contributions of other product attributes). The contribution
                       of the current attribute (whose map of terms by product is given as *tfidf_map*) will be added
                       to this same *strengths* map.
    """
    for (product_id, template_id) in strengths:
        for attr_type, weight_by_attribute in context.similarity_weights_by_type.items():
            if attr_type == pm.TEXT:
                continue

            for attribute, weight in weight_by_attribute.items():
                product_attr_value = products[product_id].get_attribute(attribute)
                template_attr_value = products[template_id].get_attribute(attribute)

                if product_attr_value is None or template_attr_value is None:
                    log.warn("Missing atribute [{0}] value product == {1}, template == {2}".format(
                        attribute, product_attr_value, template_attr_value))
                    continue

                contribution = 0
                if attr_type == pm.NUMERIC:
                    contribution = pm.compute_similarity_for_numeric(product_attr_value, template_attr_value)
                elif attr_type == pm.FIXED:
                    contribution = pm.compute_similarity_for_fixed(product_attr_value, template_attr_value)
                elif attr_type == pm.LIST:
                    contribution = pm.compute_similarity_for_list(product_attr_value, template_attr_value)
                elif attr_type == pm.DATE:
                    contribution = pm.compute_similarity_for_date(product_attr_value, template_attr_value,
                                                                  context.date_similarity_halflife)

                strengths[(product_id, template_id)] += contribution * weight


def _partition_products_by_language(products):
    """ Partitions the given product models into language buckets of product id's.

        :param products: a list of product models.
        :returns: a map {language: set of product_id's}.
    """
    products_by_language = {}

    for product in products.values():
        product_id = product.get_attribute("external_product_id")
        language = product.get_attribute("language")

        if language is None:
            continue  # skip products without language

        product_set = products_by_language.get(language, set())
        product_set.add(product_id)
        products_by_language[language] = product_set

    return products_by_language


def get_product_templates_tfidf(context, product_ids, blocked_products=None):
    """ Retrieves the top *n_templates* product templates per given product.

        :param context: A session context.
        :param product_ids: A list with the ids of the intended products.
        :param blocked_products: A list with ids of products that should not be fetched.

        :returns: A map {product_id: list of [strength, template_id] pairs}.
    """
    result = {}
    if blocked_products is None:
        blocked_products = []

    templates_map = context.data_proxy.fetch_product_templates(product_ids)
    for p_id, templates_tuple in templates_map.items():
        approved_templates = [t for t in templates_tuple[1] if t[1] not in blocked_products]
        result[p_id] = approved_templates

    if context.user_context is not None:
        product_models = context.product_models
    else:
        product_models = {}

    all_products = set(product_ids)
    for templates_with_strengths in result.values():
        all_products |= {t[1] for t in templates_with_strengths}

    products_with_missing_product_models = all_products - product_models.keys()
    if len(products_with_missing_product_models) > 0 and context.filter_strategy == ctx.AFTER_SCORING:
        product_models.update(context.data_proxy.fetch_product_models(list(products_with_missing_product_models)))

    if (context.near_identical_filter_field is not None) and (context.near_identical_filter_threshold is not None):
        for product_id, templates_with_strengths in result.items():
            templates = [t[1] for t in templates_with_strengths if t[1] in product_models]
            templates_to_disregard = pinpoint_near_identical_products(context, templates, product_models,
                                                                      base_product_id=product_id)
            result[product_id] = [t for t in templates_with_strengths if t[1] not in templates_to_disregard]

    return result
