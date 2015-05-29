import concurrent.futures
from random import shuffle
import datetime as dt

from barbante.context.context_manager import wrap
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


def consolidate_user_templates(session_context, users_list=None):
    if users_list is None:
        users_list = [u for u in session_context.data_proxy.fetch_all_user_ids()]
    total_users = len(users_list)
    if total_users == 0:
        log.info("No users to perform templates consolidation on.")
        return

    log.info("Performing consolidation of templates on %d users..." % total_users)

    # shuffles the list to balance the workers
    shuffle(users_list)

    max_workers = session_context.max_workers_template_consolidation
    n_pages = min(max_workers, total_users)
    page_sizes = [total_users // n_pages] * n_pages
    for i in range(total_users % n_pages):
        page_sizes[i] += 1

    pages_processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(wrap(__gather_user_templates), session_context,
                            page, users_list, page_sizes, session_context.flush_size / max_workers): page
            for page in range(n_pages) if page_sizes[page] > 0}
        for _ in concurrent.futures.as_completed(future_to_page):
            pages_processed += 1
            log.info("Processed [%d] pages out of %d" % (pages_processed, n_pages))


def consolidate_product_templates(session_context, products_list=None, collaborative=True, tfidf=True):
    if products_list is None:
        cutoff_date = session_context.get_present_date() - \
            dt.timedelta(session_context.product_product_strengths_window)
        products_list = [p for p in session_context.data_proxy.fetch_date_filtered_products(
            reference_date=cutoff_date)]
    elif products_list == "--all":
        products_list = [p for p in session_context.data_proxy.fetch_all_product_ids()]

    total_products = len(products_list)
    if total_products == 0:
        log.info("No products to perform templates consolidation on.")
        return

    log.info("Performing consolidation of templates on %d products..." % total_products)

    allowed_templates = None
    if session_context.recommendable_product_start_date_field or \
            session_context.recommendable_product_end_date_field:
        allowed_templates = session_context.data_proxy.fetch_date_filtered_products(
            reference_date=session_context.get_present_date(),
            lte_date_field=session_context.recommendable_product_start_date_field,
            gte_date_field=session_context.recommendable_product_end_date_field)
        log.info("(%d templates are allowed, based on due dates)" % len(allowed_templates))
    else:
        log.info("(no restrictions will be applied to templates)")

    # shuffles the list to balance the workers
    shuffle(products_list)

    max_workers = session_context.max_workers_template_consolidation
    n_pages = min(max_workers, total_products)
    page_sizes = [total_products // n_pages] * n_pages
    for i in range(total_products % n_pages):
        page_sizes[i] += 1

    pages_processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(wrap(__gather_product_templates), session_context,
                            page, products_list, page_sizes,
                            collaborative, tfidf, allowed_templates,
                            session_context.flush_size / max_workers): page
            for page in range(n_pages) if page_sizes[page] > 0}
        for _ in concurrent.futures.as_completed(future_to_page):
            pages_processed += 1
            log.info("Processed [%d] pages out of %d" % (pages_processed, n_pages))


def __gather_user_templates(context, page, users_list, page_sizes, flush_size):
    templates_map = {}
    context = context.new_session()
    start_idx = sum(page_sizes[:page])
    end_idx = start_idx + page_sizes[page]
    page_users = [users_list[i] for i in range(start_idx, end_idx)]

    done = 0
    for user in page_users:
        user_templates = [t for t in context.data_proxy.fetch_top_uu_strengths(
            user, context.user_templates_count)]
        if len(user_templates) > 0:
            templates_map[user] = user_templates
            if len(templates_map) >= flush_size:
                _flush_user_templates(context, templates_map)
        done += 1
        if done % 1000 == 0 or done == len(page_users):
            log.info("[Page %d] Completed %d out of %d users (%.2f%%)..." %
                     (page, done, len(page_users), (100 * done / len(page_users))))

    if len(templates_map) > 0:
        _flush_user_templates(context, templates_map)


def __gather_product_templates(context, page, products_list, page_sizes,
                               collaborative, tfidf, allowed_templates, flush_size):
    templates_map = {}
    context = context.new_session()
    start_idx = sum(page_sizes[:page])
    end_idx = start_idx + page_sizes[page]
    page_products = [products_list[i] for i in range(start_idx, end_idx)]

    done = 0
    for product in page_products:
        templates_tuple = context.data_proxy.fetch_top_pp_strengths(
            product, 3 * context.product_templates_count, collaborative=collaborative, tfidf=tfidf,
            allowed_products=allowed_templates)
        if len(templates_tuple[0]) > 0 or len(templates_tuple[1]) > 0:
            templates_map[product] = templates_tuple
            if len(templates_map) >= flush_size:
                _flush_product_templates(context, templates_map)
        done += 1
        if done % 10 == 0 or done == len(page_products):
            log.info("[Page %d] Completed %d out of %d products (%.2f%%)..." %
                     (page, done, len(page_products), (100 * done / len(page_products))))

    if len(templates_map) > 0:
        _flush_product_templates(context, templates_map)


def _flush_user_templates(context, templates_by_user):
    log.debug("Saving templates of {0} users...".format(len(templates_by_user)))
    context.data_proxy.save_user_templates(templates_by_user)
    templates_by_user.clear()
    log.debug("User templates saved")


def _flush_product_templates(context, templates_by_product):
    log.debug("Saving templates of {0} products...".format(len(templates_by_product)))
    context.data_proxy.save_product_templates(templates_by_product)
    templates_by_product.clear()
    log.debug("Product templates saved")
