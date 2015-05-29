""" Module that creates dummy data for testing purposes.
"""

import json
import datetime as dt
import dateutil.parser

import barbante.config as config
import barbante.utils as utils
import barbante.maintenance.tasks as tasks


# keep up-to-date

N_PROD_ECONOMIA = 4
N_PROD_ESPORTES = 9
N_PROD_MUSICA = 4
N_PROD_TECNOLOGIA = 7
N_PROD_NONSENSE = 4
N_PROD_SUPERPOPULAR = 2

N_USR_ECONOMIA = 4
N_USR_ESPORTES = 4
N_USR_MUSICA = 4
N_USR_TECNOLOGIA = 5

_collection_cache = {}


def populate_products(session_context):
    """ Creates dummy products for tests. Product data comes from barbante/tests/products.json.
    """
    product_records = _load_collection("products")
    for record in product_records:
        product = record["product_data"]
        product["external_id"] = record["product_id"]

        if "date" not in product:
            product["date"] = session_context.get_present_date()
        else:
            if not isinstance(product["date"], dt.datetime):
                product["date"] = dateutil.parser.parse(product["date"])

        if "expiration_date" not in product:
            product["expiration_date"] = session_context.get_present_date() + dt.timedelta(days=30)
        else:
            if not isinstance(product["expiration_date"], dt.datetime):
                product["expiration_date"] = dateutil.parser.parse(product["expiration_date"])
        session_context.data_proxy.insert_product(product)


def populate_users(session_context):
    """ Creates dummy users for tests. Product data comes from barbante/tests/users.json.
    """
    user_records = _load_collection("users")
    for record in user_records:
        user = record["user_data"]
        user["external_id"] = record["user_id"]
        session_context.data_proxy.insert_user(user)


def populate_activities(session_context, date=None):
    """ Creates dummy activities.

        Test users shall consume products of their main interest area,
        e.g. "u_eco_X" shall only consume products "p_eco_Y".
        User *empty* shall consume no products at all.
    """
    activity_records = _load_collection("activities")
    if date is None:
        date = session_context.get_present_date() - dt.timedelta(days=1)

    for record in activity_records:
        for i, product in enumerate(record["products"]):
            new_date = date + dt.timedelta(seconds=i)
            activity = {"external_user_id": record['user_id'],
                        "external_product_id": product,
                        "activity": "buy",
                        "created_at": new_date}
            tasks.update_summaries(session_context, activity)


def populate_impressions(context):
    """ Creates dummy impressions for all user-product pairs.
    """
    date = context.get_present_date() - dt.timedelta(days=5)

    all_users = context.data_proxy.fetch_all_user_ids()
    for user in all_users:
        all_products = context.data_proxy.fetch_all_product_ids()  # It must be here, since generators can't be rewound.
        for product in all_products:
            context.data_proxy.increment_impression_summary(user, product, date, anonymous=config.is_anonymous(user))


def is_top_popular(product):
    """ Informs whether the given product belongs to the list of ultra popular products
            (consumed by almost everyone).
        :param product: The intended product
        :returns: True, if the product is ultra popular; False, otherwise.
    """
    return product.find("POPULAR") > -1


def _load_collection(collection_name):
    filename = utils.resource_filename('{0}.json'.format(collection_name), __name__)
    if filename not in _collection_cache:
        with open(filename, 'r', encoding='utf-8') as file:
            _collection_cache[filename] = json.load(file)
    return _collection_cache[filename]
