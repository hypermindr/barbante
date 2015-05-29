import concurrent.futures
import datetime as dt
import heapq
import math
import traceback
from time import time

from barbante.utils.profiling import profile
import barbante.utils.logging as barbante_logging
import barbante.utils.text as text
import barbante.utils as utils
from barbante.context.context_manager import wrap


log = barbante_logging.get_logger(__name__)


@profile
def process_products_from_scratch(session_context, days=None):
    """ Processes product models and product terms for all non-deleted, valid products in the database.

        :param session_context: The customer context.
        :param days: The number of days which should be considered. Only products whose date
            attribute lies within the last days
    """
    session_context.data_proxy.reset_all_product_content_data()

    # registers the start of the operation and the cutoff_date
    timestamp = session_context.get_present_date()
    if days is not None:
        cutoff_date = session_context.get_present_date() - dt.timedelta(days)
    else:
        cutoff_date = None
    real_time_start = time()

    required_fields = session_context.product_model_factory.get_custom_required_attributes()
    log.info("Loading product ids of all products with required fields: {0}".format(required_fields))
    product_ids_list = [p for p in session_context.data_proxy.fetch_all_product_ids(
        allow_deleted=False, required_fields=required_fields, min_date=cutoff_date)]
    total_products = len(product_ids_list)
    log.info("Loaded [%d] products" % total_products)

    skipped = 0

    # Auxiliary map of products by language.
    language_map = {}

    # The 1st stage of parallel processing: generates the product models and splits products by language.

    n_pages = total_products // session_context.page_size_batch_process_products + 1
    max_workers = session_context.max_workers_batch_process_products
    pages_processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(wrap(__process_product_models), session_context, page, product_ids_list,
                            session_context.flush_size / max_workers): page
            for page in range(n_pages)}

        for future in concurrent.futures.as_completed(future_to_page):
            pages_processed += 1
            result = future.result()

            products_by_language = result[0]

            skipped_products = result[1]
            skipped += skipped_products

            # Updates the map of products by language.
            for language, new_products in products_by_language.items():
                products = language_map.get(language, [])
                products += new_products
                language_map[language] = products

            if pages_processed % 100 == 0:
                log.info("Processed [{0}] pages out of [{1}] during product model creation".format(pages_processed,
                                                                                                   n_pages))

    session_context.data_proxy.hotswap_product_models()

    # With all product models duly created, we split the processing of terms by language.

    for language, language_product_ids_list in language_map.items():
        language_products_count = len(language_product_ids_list)
        log.info("Processing [%d] %s products..." % (language_products_count, language))

        # An auxiliary in-memory map to hold all DFs per language.
        df_map = {}
        # This is probably ok, since its size is linear in the overall number of product terms per language,
        # but I have added a safety pig just in case.
        #                          _
        #  _._ _..._ .-',     _.._(`))
        # '-. `     '  /-._.-'    ',/
        #    )         \            '.
        #   / _    _    |             \
        #  |  a    a    /              |
        #  \   .-.                     ;
        #   '-('' ).-'       ,'       ;
        #      '-;           |      .'
        #         \           \    /
        #         | 7  .__  _.-\   \
        #         | |  |  ``/  /`  /
        #        /,_|  |   /,_/   /
        #           /,_/      '`-'
        #
        df_map_size = 0  # to closely monitor de size of the in-memory map
        #
        # In case we have OOM issues, we can kill the pig altogether and use the database
        # to perform the aggregation of DFs. The drawback is that we'll have to do costly upserts,
        # instead of the current inserts.

        # The 2nd stage of parallel processing: saving TEXT-type attributes' terms' TFs (and aggregation of DFs).

        n_pages = language_products_count // session_context.page_size_batch_process_products + 1
        max_workers = session_context.max_workers_batch_process_products
        pages_processed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {
                executor.submit(wrap(__process_product_terms), session_context, page, language_product_ids_list,
                                language, session_context.flush_size / max_workers): page
                for page in range(n_pages)}

            for future in concurrent.futures.as_completed(future_to_page):
                pages_processed += 1
                result = future.result()

                df_by_term = result[0]

                skipped_products = result[1]
                skipped += skipped_products

                # Performs an in-memory aggregation of DF's.

                for term, df in df_by_term.items():
                    previous_df = df_map.get(term)
                    if previous_df is None:
                        previous_df = 0
                        df_map_size += 1
                    df_map[term] = previous_df + df

                if pages_processed % 100 == 0:
                    log.info("Processed [{0}] pages out of [{1}] during TF processing".format(pages_processed, n_pages))
                    log.info("In-memory df_map size = [{0}] terms".format(df_map_size))

        # The 3rd stage of parallel processing: saving the TFIDF's of the top terms per attribute.

        n_pages = language_products_count // session_context.page_size_batch_process_products + 1
        max_workers = session_context.max_workers_batch_process_products
        pages_processed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {
                executor.submit(wrap(__process_products_TFIDFs), session_context, page, language_product_ids_list,
                                total_products, language, df_map, session_context.flush_size / max_workers): page
                for page in range(n_pages)}

            for _ in concurrent.futures.as_completed(future_to_page):
                pages_processed += 1
                if pages_processed % 100 == 0:
                    log.info("Processed [{0}] pages out of [{1}] during TFIDF processing".format(pages_processed,
                                                                                                 n_pages))

        # Persists all DF's (aggregated in memory).
        log.info("Saving DF's (language: %s)..." % language)
        _flush_df_map(session_context, language, df_map)
        log.info("DF's saved")

    session_context.data_proxy.save_timestamp_product_models(
        timestamp, cutoff_date, time() - real_time_start)

    success = total_products - skipped
    log.info("Done: [%d] products were processed successfully; [%d] products were skipped." % (success, skipped))


def prepare_product_model(session_context, product, force_update=False, batch_processing=False):
    """ Creates and persists the product model for the given product.

        :param session_context: The session context.
        :param product: A dict with raw product data.
        :param force_update: If True, it will override the model of a previous version of the product (if any).
        :param batch_processing: If True, it will not trigger persistence tasks.

        :returns: - a ProductModel object for the given product, and
                  - an indication of whether a previous product model exists for the given product.
    """
    try:
        product_id = product.get("external_id")
        product_model = session_context.data_proxy.fetch_product_models([product_id]).get(product_id)
        has_pre_existing_model = product_model is not None

        if force_update or not has_pre_existing_model:
            log.debug("Creating product model for product [%s]..." % product_id)
            product_model = session_context.create_product_model(product_id, product)

            if not batch_processing:
            # We only save if we are not batch processing.
            # Otherwise, the caller will probably want to collect sufficiently many entries and save them in bulk.
                log.debug("Saving product model...")
                session_context.data_proxy.save_product_model(product_model.id, product_model)

        return product_model, has_pre_existing_model

    except Exception as error:
        if batch_processing:
            log.exception("Skipped document (id = %s) due to exception: %s." % (product_id, traceback.format_exc()))
        else:
            raise error


def prepare_product_terms(session_context, product_dict, attributes=None, reprocessing_product=False,
                          batch_processing=False):
    """ Processes the terms (cleans stop-words, stems, counts, persists DF's and TFIDF's) of
        all TEXT-type attributes of the given product.

        :param session_context: The session context.
        :param product_dict: A flattened dict with product model attributes.
            TEXT-type attributes should be a list of stems.
        :param attributes: If not None, a list with the intended TEXT-type product attributes: only those will be
            processed. If None, all TEXT-type product attributes will be considered.
        :param reprocessing_product: If True, it will first remove former entries/contributions
            persisted for that product.

        :returns: - a list of {"external_product_id": product_id,
                               "attribute": attribute,
                               "term": term,
                               "count": tf} dicts containing the TFs of all product terms by attribute.
                  - a set with all (distinct) stemmed terms (minus stop-words) of the product; and
                  - a map {attribute: {term: tfidf}} with the TFIDF's of the
                      top *k* (configured in the context) terms per attribute; and
    """
    try:
        total_products = session_context.data_proxy.get_product_model_count()  # We need this to compute TFIDF's
        product_id = product_dict.get("external_product_id")
        language = product_dict.get("language")

        if attributes is None:
            attributes = session_context.product_text_fields

        if reprocessing_product:
            # Removes the old terms of the product being re-processed.
            all_old_terms = set()
            for attribute in attributes:
                old_terms = session_context.data_proxy.fetch_tf_map(attribute, [product_id]).get(product_id, {})
                all_old_terms |= old_terms.keys()
            df_decrements = {term: -1 for term in all_old_terms}
            session_context.data_proxy.save_df(language, df_decrements, increment=True, upsert=True)
            session_context.data_proxy.remove_product_terms(attributes, product_id)
            session_context.data_proxy.remove_tfidf(attributes, product_id)

        # output structures
        tf_records = []
        all_terms = set()
        tfidf_by_top_term_by_attribute = {}

        # Generates TF records and collects distinct terms.

        tf_by_term_by_attribute = {}
        for attribute in session_context.product_text_fields:
            value = product_dict.get(attribute)
            if value is None:
                continue

            log.debug("Calculating the TFs of the terms' stems...")
            tf_by_term = text.calculate_tf_from_stems(value)

            if len(tf_by_term) > 0:
                tf_by_term_by_attribute[attribute] = tf_by_term
                terms = tf_by_term.keys()
                all_terms |= terms

                tf_records += [{"external_product_id": product_id,
                                "attribute": attribute,
                                "term": term,
                                "count": tf} for term, tf in tf_by_term.items()]

        if not batch_processing:
        # We only save if we are not batch processing.
        # Otherwise, the caller will collect sufficiently many entries and save them in bulk.

            log.debug("Saving TFs...")
            session_context.data_proxy.insert_product_terms(tf_records)

            log.debug("Saving DFs...")
            df_map = dict.fromkeys(all_terms, 1)
            session_context.data_proxy.save_df(language, df_map, increment=True, upsert=True)

            # With all DFs duly updated, we calculate the TFIDFs of the top terms.

            tfidf_records = []
            for attribute in session_context.product_text_fields:
                tf_by_term = tf_by_term_by_attribute.get(attribute, {})

                if len(tf_by_term) > 0:
                    terms = tf_by_term.keys()
                    terms_list = list(terms)

                    log.debug("Fetching DF's of %d %s terms..." % (len(terms), language))
                    df_by_term = session_context.data_proxy.fetch_df_map(language, terms_list)

                    log.debug("Building TFIDF map...")
                    tfidf_by_top_term = calculate_tf_idf_map([product_id], total_products,
                                                             session_context.top_terms_count,
                                                             df_by_term, {product_id: tf_by_term}).get(product_id)

                    tfidf_records += [{"external_product_id": product_id,
                                       "attribute": attribute,
                                       "term": term,
                                       "tfidf": tfidf} for term, tfidf in tfidf_by_top_term.items()]

                    tfidf_by_top_term_by_attribute[attribute] = tfidf_by_top_term

            log.debug("Saving TFIDFs...")
            session_context.data_proxy.insert_tfidf_records(tfidf_records)

        return tf_records, all_terms, tfidf_by_top_term_by_attribute

    except Exception as error:
        if batch_processing:
            log.exception("Skipped document (id = %s) due to exception: %s." % (product_id, traceback.format_exc()))
        else:
            raise error


def __process_product_models(session_context, page, products_list, flush_size):
    session_context = session_context.new_session()
    start_idx = page * session_context.page_size_batch_process_products
    end_idx = min((page + 1) * session_context.page_size_batch_process_products, len(products_list))

    page_product_ids = products_list[start_idx:end_idx]

    fields = session_context.product_text_fields + session_context.product_non_text_fields
    products_map = session_context.data_proxy.fetch_products(page_product_ids, fields_to_project=fields)

    products_by_language = {}
    product_model_records = []

    skipped = 0

    for product_id in page_product_ids:

        product = products_map.get(product_id)
        if product is None:
            skipped += 1
            continue

        product_model_results = prepare_product_model(session_context, product, force_update=True,
                                                      batch_processing=True)
        if product_model_results is None:
            skipped += 1
            continue
        product_model = product_model_results[0]

        language = product_model.get_attribute("language")
        products = products_by_language.get(language, [])
        products += [product_id]
        products_by_language[language] = products

        product_model_record = product_model.to_dict()
        product_model_records += [product_model_record]

        if len(product_model_records) >= flush_size:
            _flush_product_model_records(session_context, product_model_records)

    if len(product_model_records) > 0:
        _flush_product_model_records(session_context, product_model_records)

    return products_by_language, skipped


def __process_product_terms(session_context, page, products_list, language, flush_size):
    session_context = session_context.new_session()
    start_idx = page * session_context.page_size_batch_process_products
    end_idx = min((page + 1) * session_context.page_size_batch_process_products, len(products_list))

    page_product_ids = products_list[start_idx:end_idx]
    total_products = len(page_product_ids)

    tf_records = []
    df_by_term = {}

    product_models_map = session_context.data_proxy.fetch_product_models(page_product_ids)
    product_dicts_map = {p_id: utils.flatten_dict(p_model.to_dict()) for p_id, p_model in product_models_map.items()}
    skipped = total_products - len(product_dicts_map)

    non_persisted_text_fields = set(session_context.product_text_fields) - \
                                session_context.product_model_factory.persisted_attributes
    if len(non_persisted_text_fields) > 0:
        # Fetches the non-persisted text attributes from the raw products collection and stemmizes them.
        products_map = session_context.data_proxy.fetch_products(product_ids=page_product_ids,
                                                                 fields_to_project=list(non_persisted_text_fields))
        for p_id, product in products_map.items():
            attributes_ok = True
            if p_id not in product_dicts_map:
                attributes_ok = False
            if attributes_ok:
                product = utils.flatten_dict(product)
                stemmed_attributes_map = {}
                for attribute in non_persisted_text_fields:
                    value = product.get(attribute)
                    if value is not None:
                        try:
                            stemmed_attributes_map[attribute] = text.parse_text_to_stems(language, value)
                        except Exception as err:
                            log.error('Exception: {0}'.format(str(err)))
                            log.error('Offending value: {0}'.format(value))
                            attributes_ok = False
                            continue
            if attributes_ok:
                product_dicts_map[p_id].update(stemmed_attributes_map)
            else:
                skipped += 1
                if p_id in product_dicts_map:
                    product_dicts_map.pop(p_id)

    for product_dict in product_dicts_map.values():

        product_terms_results = prepare_product_terms(session_context, product_dict, batch_processing=True)
        if product_terms_results is None:
            skipped += 1
            continue
        new_tf_records, new_terms, _ = product_terms_results

        tf_records += new_tf_records

        for term in new_terms:
            df = df_by_term.get(term, 0) + 1
            df_by_term[term] = df

        if len(tf_records) >= flush_size:
                _flush_tf_records(session_context, tf_records)

    if len(tf_records) > 0:
        _flush_tf_records(session_context, tf_records)

    return df_by_term, skipped


def __process_products_TFIDFs(session_context, page, products_list, total_products, language, df_by_term, flush_size):
    session_context = session_context.new_session()
    start_idx = page * session_context.page_size_batch_process_products
    end_idx = min((page + 1) * session_context.page_size_batch_process_products, len(products_list))

    page_product_ids = products_list[start_idx:end_idx]

    tfidf_records = []
    for attribute in session_context.product_text_fields:
        log.debug("Fetching TF's (language: %s, attribute: %s)..." % (language, attribute))
        tf_by_term_by_product = session_context.data_proxy.fetch_tf_map(attribute, page_product_ids)

        log.debug("Building TFIDF map (language: %s, attribute: %s)..." % (language, attribute))
        tfidf_by_top_term_by_product = calculate_tf_idf_map(page_product_ids, total_products,
                                                            session_context.top_terms_count,
                                                            df_by_term, tf_by_term_by_product)

        for product_id, tfidf_by_top_term in tfidf_by_top_term_by_product.items():
            tfidf_records += [{"external_product_id": product_id,
                               "attribute": attribute,
                               "term": term,
                               "tfidf": tfidf} for term, tfidf in tfidf_by_top_term.items()]

        if len(tfidf_records) >= flush_size:
                _flush_tfidf_records(session_context, tfidf_records)

    if len(tfidf_records) > 0:
        _flush_tfidf_records(session_context, tfidf_records)


@profile
def delete_product(session_context, product_id):
    session_context.data_proxy.delete_product_model(product_id)
    attributes = session_context.product_text_fields
    if attributes:  # There will only be product terms and TFIDF to be deleted if there are product TEXT fields.
        session_context.data_proxy.remove_product_terms(attributes, product_id)
        session_context.data_proxy.remove_tfidf(attributes, product_id)


def calculate_tf_idf_map(products, total_products, n_top_terms, df_map, tf_by_term_by_product):
    """ Retrieves a map with the top *n_top_terms* terms (w.r.t. tfidf) in each given product.

        :param total_products: The total number of products (to be used in the idf formula).
        :param n_top_terms: The number of top terms to be considered.
        :param df_map: A map {term: number of products containing it}.
        :param tf_by_term_by_product: A map {product: {term: tf}} with the terms count by product.

        :return: A map {product: {term: tfidf}}.
    """
    tfidf_map = {}

    for product in products:
        tfidf_term_list = []

        tf_by_term = tf_by_term_by_product.get(product, {})

        if len(tf_by_term) > 0:
            max_tf = max(tf_by_term.values())

            max_tfidf = 0

            if max_tf > 0:
                for term in tf_by_term:
                    tf = tf_by_term.get(term, 0)
                    df = df_map.get(term, 1)
                    tfidf = (tf * 1.0 / max_tf) * math.log(total_products / (1 + df))
                    if tfidf > max_tfidf:
                        max_tfidf = tfidf
                    tfidf_term_list += [(tfidf, term)]

            tfidf_by_term = {}

            if max_tfidf > 0:
                tfidf_term_list = [(t[0] / max_tfidf, t[1]) for t in tfidf_term_list]
                top_terms = heapq.nlargest(n_top_terms, tfidf_term_list)

                for tfidf, term in top_terms:
                    if tfidf > 0:
                        tfidf_by_term[term] = tfidf

            tfidf_map[product] = tfidf_by_term

    return tfidf_map


def _flush_product_model_records(session_context, product_model_records):
    log.debug("Saving {0} product models...".format(len(product_model_records)))
    session_context.data_proxy.insert_product_models(product_model_records, deferred_publication=True)
    product_model_records.clear()
    log.debug("Product models saved")


def _flush_df_map(session_context, language, df_map):
    session_context.data_proxy.save_df(language, df_map, upsert=False, increment=False)
    df_map.clear()


def _flush_tf_records(session_context, tf_records):
    log.debug("Saving {0} TF records...".format(len(tf_records)))
    session_context.data_proxy.insert_product_terms(tf_records)
    tf_records.clear()
    log.debug("TF records saved")


def _flush_tfidf_records(session_context, tfidf_records):
    log.debug("Saving {0} TF records...".format(len(tfidf_records)))
    session_context.data_proxy.insert_tfidf_records(tfidf_records)
    tfidf_records.clear()
    log.debug("TFIDF records saved")


def are_too_similar(product1, product2, product_models_by_product, filter_field, threshold):
    product1_model = product_models_by_product.get(product1)
    if product1_model is None:
        return False

    product2_model = product_models_by_product.get(product2)
    if product2_model is None:
        return False

    product1_stems = product1_model.get_attribute(filter_field)
    product2_stems = product2_model.get_attribute(filter_field)
    n_common_terms = text.count_common_terms(product1_stems, product2_stems)

    return n_common_terms > threshold


def pinpoint_near_identical_products(context, product_ids, product_models, base_product_id=None):
    """ Returns a list of products which happen to be too similar ('near-identical') to
        some other, higher-ranked product, or to a given base product.

        :param context: The session context.
        :param product_ids: An ORDERED list (by score, descending) of product ids to be investigated.
        :param product_models: A dict {product_id: Product Model object} for all informed products.
        :param base_product_id: If not None, then the products in the list will only be compared
            against this base product. Otherwise, they will be compared among themselves.

        :returns: A set of near-identical products. (The highest ranked products are not included in the set;
            i.e., the set contains only products that should be discarded, or sent to the end of the recommendations
            list, etc.)
    """
    filtered_out = set()

    for product_idx, product_id in enumerate(product_ids):

        # compares with the base product

        if base_product_id is not None:
            if are_too_similar(product_id, base_product_id, product_models,
                               context.near_identical_filter_field, context.near_identical_filter_threshold):
                filtered_out.add(product_id)
                continue

        else:
            # compares with all higher-ranked templates

            if context.recommendations_page_size is None:
                start_index = 0
            else:
                start_index = max(0, product_idx - context.recommendations_page_size)

            for prior_product_index in range(start_index, product_idx):
                prior_product_id = product_ids[prior_product_index]
                if are_too_similar(product_id, prior_product_id, product_models,
                                   context.near_identical_filter_field, context.near_identical_filter_threshold):
                    filtered_out.add(product_id)
                    continue

    return filtered_out