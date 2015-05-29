import abc


class BaseProxy(object):
    """ Defines the basic interface for data read/write operations.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, context):
        """ Constructor.
            :param context: A CustomerContext instance.
        """

        self.context = context
        """ The CustomerContext instance used by this proxy.
        """

    @abc.abstractmethod
    def fetch_all_user_ids(self):
        """ Retrieves all user ids in the users collection.
            :returns: a generator yielding all user ids.
        """

    @abc.abstractmethod
    def fetch_all_product_ids(self, allow_deleted=False, required_fields=None,
                              min_date=None, max_date=None, product_date_field=None):
        """ Retrieves all products whose date is greater than or equal to *cutoff_date*,
            or all product ids if *cutoff_date* is not informed.

            :param allow_deleted: Indicates whether deleted product ids should be retrieved
            :param required_fields: A list of required fields. If defined, only the products that have
                all the informed fields can be retrieved.
            :param min_date: The date before which no products will be retrieved. If None, it will be disregarded.
            :param max_date: The date after which no products will be retrieved. If None, it will be disregarded.
            :param product_date_field: The product field against which the date clauses should be run.
                If None, the default field name will be used.

            :returns: A generator yielding all product ids.
        """

    @abc.abstractmethod
    def fetch_user_templates(self, user_ids):
        """ Retrieves a map with the pre-rendered (cached) templates by user_id.

            :param user_ids: The list of product ids.

            :returns: A map with each user_id associated to a list
                containing a [*strength*, *template_id*] entry
                for each template of the given user, where *strength* is a tuple of comparables.
        """

    @abc.abstractmethod
    def fetch_top_uu_strengths(self, user_id, n_templates):
        """ Retrieves the top n_templates user with the greatest asymmetrical strength w.r.t. the given user.

            :param user_id: The id of the user.
            :param n_templates: The intended number of top-strength users.

            :returns: A list of [strength, template_id] pairs corresponding to the templates of the
                given user in descending order of strength.
                Note that *strength* is a tuple of comparables.
        """

    @abc.abstractmethod
    def fetch_product_templates(self, product_ids):
        """ Retrieves a map with the pre-rendered (cached) templates by product_id.
            It actually returns a tuple with two maps: one for the collaborative filtering templates;
                another for the content-based templates.

            :param product_ids: The list of product ids.

            :returns: A map with each product_id associated to a tuple with two lists:
                the first one, for collaborative filtering templates;
                the second, for content-based (aka "tfidf") templates.
                Both lists contains a [*strength*, *template_id*] entry
                for each template of the given product, where *strength* is a tuple of comparables.
        """

    @abc.abstractmethod
    def fetch_top_pp_strengths(self, product_id, n_templates, blocked_products=None, allowed_products=None):
        """ Retrieves the top n_templates products with the greatest asymmetrical strength w.r.t. to each given product.

            :param product_id: The product id.
            :param n_templates: The intended number of top-strength products.
            :param blocked_products: A list with product ids not to be fetched.
            :param allowed_products: A list with products which are allowed to be fetched.
                       If None, all products are allowed.

            :returns: A tuple with two lists:
                the first one, for collaborative filtering templates;
                the second, for content-based (aka "tfidf") templates.
                Both lists contains a [*strength*, *template_id*] entry
                for each template of the given product, where *strength* is a tuple of comparables.
        """

    @abc.abstractmethod
    def fetch_activity_summaries_by_user(self, anonymous, user_ids=None, product_ids=None, activity_types=None,
                                         num_activities=None, min_day=None,
                                         indexed_fields_only=True):
        """ Retrieves a map with activities split by user, in descending chronological order.

            :param anonymous: if True, it will look for activities in the anonymous activities collection.
            :param user_ids: a list with the intended user ids. If None, then all users
                           will be considered.
            :param product_ids: a list with the intended product ids. If None, then all products
                           will be considered.
            :param activity_types: If not None, only such activity types will be considered.
            :param num_activities: The maximum number of activities to be returned,
                           ordered by activity_date in descending order. If None, no limits will be imposed.
            :param min_day: The day before which no activities will be retrieved. If None, it will be disregarded.
            :param indexed_fields_only: If True, it will not fetch:
                       - the "created_at" field,
                       - the "contributed_for_popularity" field,
                       - the "uu_latest_type" field,
                       - the "uu_latest_date" field,
                       - the "pp_latest_type" field, or
                       - the "pp_latest_date" field,
                    since these fields are not indexed.
                    If False, all fields above will be fetched as well.

            :returns: A map {*user_id*: list of {"external_user_id": user_id",
                                                 "external_product_id": product_id,
                                                 "activity": activity_type,
                                                 "day": date} dicts in anti-chronological order}.
                      If parameter indexed_fields_only is False, then the 6 aforementioned non-indexed fields will
                      also belong to the returned dict.

            Obs.: Parameters *user_ids* and *product_ids* cannot both be None, or an exception will ensue.
                  This is due to the need to use an existing index in the query, or performance degrades severely.
        """

    @abc.abstractmethod
    def fetch_day_of_latest_user_activity(self, user_id, anonymous):
        """ Retrieves the day of latest activity of the informed user.

            :param user_id: the intended user id.
            :param anonymous: if True, it will lookup the anonymous activities collection.

            :returns: a datetime object set to midnight GMT of the day of the latest activity.
        """

    @abc.abstractmethod
    def fetch_products_by_rating_by_user(self, user_ids=None, min_date=None, max_date=None):
        """ Retrieves all products consumed by each user, grouped by rating (either explicit or implicit).
            If more than one activity exists for the same (user, product) pair, then only the most recent one
            will be considered.

            :param user_ids: the intended user_ids to be used as a filter;
                                 if None, then all products will be retrieved.
            :param min_date: The date before which no activities will be considered. If None, it will be disregarded.
            :param max_date: The date after which no activities will be considered. If None, it will be disregarded.

            :returns: - a map {user_id: {rating: set of product_id's}}, and
                      - a list where the j-th position contains the number of products rated j by some user.
        """

    @abc.abstractmethod
    def fetch_users_by_rating_by_product(self, product_ids=None, min_date=None, max_date=None):
        """ Retrieves all users that consumed each product, grouped by rating (either explicit or implicit).
            If more than one activity exists for the same (user, product) pair, then only the most recent one
            will be considered.

            :param product_ids: the intended product_ids to be used as a filter;
                                    if None, then all products will be retrieved.
            :param min_date: The date before which no activities will be considered. If None, it will be disregarded.
            :param max_date: The date after which no activities will be considered. If None, it will be disregarded.

            :returns: - a map {product_id: {rating: set of user_id's}}, and
                      - a list where the j-th position contains the number of users giving rate j to some product.
        """

    @abc.abstractmethod
    def fetch_product_popularity(self, product_ids=None, n_products=None, min_day=None):
        """ Retrieves the number of users divided by the product age of each product.

            :param product_ids: A list with the product_ids.
                           If None, it shall consider all products in the database.
            :param n_products: If not None, then only the *n_products* most popular products will be retrieved.
                           If None, all products will be retrieved.
            :param min_day: If informed, then products with no activities after min_date will be disregarded.

            :returns: A map {product_id: popularity}.

            Obs.: Parameters *product_ids* and *n_products* cannot both be None.
        """

    @abc.abstractmethod
    def fetch_impressions_summary(self, anonymous, user_ids=None, product_ids=None, group_by_product=False):
        """ Retrieves the summary of impressions for the given users and products.

            :param anonymous: if True, it will look for impressions in the anonymous impressions collection.
            :param user_ids: the intended user ids. If None, all users will be considered.
            :param product_ids: the intended products. If None, all products will be considered.
            :param group_by_product: if True, the impressions will be grouped by product;
                                     if False, they will be grouped by user instead.

            :returns: - a map {product_id: {user_id: a (count, first_impression_date) tuple}},
                            if group_by_product is True;
                        OR
                      - a map {user_id: {product_id: a (count, first_impression_date) tuple}}, otherwise.
        """

    @abc.abstractmethod
    def fetch_users_with_impressions_by_product(self, anonymous, product_ids=None, user_ids=None):
        """ Retrieves the users with impressions on the specified products.

            :param anonymous: if True, it will fetch the impressions from the anonymous impressions collection.
            :param product_ids: A list with the ids of the intended products. If None, all products will be considered.
            :param user_ids: A list with the ids of the intended users. If None, all users will be considered.

            :returns: a dict {product_id: set of user ids}.
        """

    @abc.abstractmethod
    def fetch_products_with_impressions_by_user(self, anonymous, user_ids=None, product_ids=None):
        """ Retrieves the products with impressions on the specified users.

            :param anonymous: if True, it will fetch the impressions from the anonymous impressions collection.
            :param user_ids: A list with the ids of the intended users. If None, all users will be considered.
            :param product_ids: A list with the ids of the intended products. If None, all products will be considered.

            :returns: a dict {user_id: set of product ids}.
        """

    @abc.abstractmethod
    def fetch_products(self, product_ids=None, fields_to_project=None, required_fields=None,
                       min_date=None, max_date=None, product_date_field=None, allow_deleted=False):
        """ Retrieves the records corresponding to the given product_ids.
            It does not retrieve logically deleted products.

            :param product_ids: The intended product ids. If None, all products will be retrieved.
            :param fields_to_project: A list with the names of the fields to be fetched.
                If None, all fields will be fetched.
            :param required_fields: A list of required fields. If defined, only the products that have
                all the informed fields can be retrieved.
            :param min_date: If not None, it will only retrieve products whose date is greater than
                or equal to the informed date.
            :param max_date: If not None, it will only retrieve products whose date is less than
                or equal to the informed date.
            :param product_date_field: The product field against which the date clauses should be run.
                If None, the default field name will be used.
            :param allow_deleted: If False, only products which have not been (logically) deleted will be considered.

            :returns: a map {product_id: record} where *record* is a map with all desired fields.
        """

    def fetch_product_models_for_top_tfidf_terms(self, attribute, language, terms, min_date=None, max_date=None):
        """ Retrieves the product models corresponding to products whose language
            matches the given language and whose top terms contain at least one of the given terms.
            It does not retrieve logically deleted products.

            :param attribute: The product attribute where the terms must belong to.
            :param language: The intended language.
            :param terms: A list of terms.
            :param min_date: If not None, it will only consider products whose date is greater than
                or equal to the informed date.
            :param max_date: If not None, it will only consider products whose date is less than
                or equal to the informed date.

            :returns: a map {product_id: product_model} where *product_model* is an instance of ProductModel.
        """

    @abc.abstractmethod
    def fetch_product_models(self, product_ids=None, context_filter=None,
                             min_date=None, max_date=None, product_date_field=None, ids_only=False):
        """ Fetches product models satisfying the informed parameters.

            :param product_ids: A list with the intended product ids. If None, products will not be filtered by id.
            :param context_filter: If not None, will be used to filter product models in the db query.
            :param min_date: If not None, it will only consider products whose date is greater than
                or equal to the informed date.
            :param max_date: If not None, it will only consider products whose date is less than
                or equal to the informed date.
            :param product_date_field: The product field against which the date clauses should be run.
                If None, the default field name will be used.
            :param ids_only: if True, only the product ids will be retrieved, not the entire product models
                (this is useful when all one needs to know is which products pass a certain context filter).

            :returns: A dict {product_id: product_model}, where *product_model* is an instance of ProductModel,
                      if ids_only is False; or
                      a list of product ids, if ids_only is True.
        """

    @abc.abstractmethod
    def fetch_date_filtered_products(self, reference_date, lte_date_field=None, gte_date_field=None):
        """ Retrieves the ids of the products whose lte_date_field (if not None) is less than or equal to
            the reference date and whose gte_date_field (if not None) is greater than or equal to the reference
            date.

            :param reference_date: The date against which start_date and end_date will be compared.
            :param lte_date_field: The name of the start date field, e.g. "publishing_date".
            :param gte_date_field: The name of the end date field, e.g. "expiration_date".

            :returns: A list of product ids.
        """

    @abc.abstractmethod
    def fetch_user_user_strengths(self, users=None, templates=None):
        """ Retrieves the strengths between pairs of users.

            :param users: If not None, only the informed target users will be considered.
            :param templates: If not None, only the intended template users will be considered.

            :returns: A map {(user, template_user): strength value}.
        """

    @abc.abstractmethod
    def fetch_user_user_strength_operands(self, users=None, templates=None, group_by_target=False,
                                          numerators_only=False):
        """ Retrieves the strength numerators and denominators of user-to-user strengths.
            The numerators are lists of size two, where the first element is the CONSERVATIVE score numerator,
            whereas the second element is the AGGRESSIVE score numerator. The denominators are floats.

            :param users: A list containing the intended target user ids.
            :param templates: A list containing the intended template user ids.
            :param group_by_target: a boolean indicating whether the results should be grouped by target user.
            :param numerators_only: if True, the returned denominators_map will be empty, and only
                documents containing non-null, non-zero numerators will be fetched.

            :returns: A tuple (numerators_map, denominators_map), where
                      numerators_map is
                          a dict {(user_id, template_user_id): [conservative_numerator, aggressive_numerator]},
                             if group_by_target is False;
                             OR
                          a dict {user_id: {template_user_id: [conservative_numerator, aggressive_numerator]}},
                             if group_by_target is True, and
                      denominators_map is
                          a dict {(user_id, template_user_id): denominator},
                             if group_by_target is False;
                             OR
                          a dict {user_id: {template_user_id: denominator}},
                             if group_by_target is True.
        """

    @abc.abstractmethod
    def fetch_product_product_strengths(self, products=None, templates=None):
        """ Retrieves the strengths between pairs of products.

            :param products: If not None, only the informed base products will be considered.
            :param templates: If not None, only the intended template products will be considered.

            :returns: A map {(product, template_product): strength value}.
        """

    @abc.abstractmethod
    def fetch_product_product_strength_operands(self, products=None, templates=None, group_by_template=False,
                                                numerators_only=False):
        """ Retrieves the strength numerators and denominators of product-to-product strengths.
            The numerators are lists of size two, where the first element is the CONSERVATIVE score numerator,
            whereas the second element is the AGGRESSIVE score numerator. The denominators are floats.

            :param products: A list containing the intended target user ids.
            :param templates: A list containing the intended template user ids.
            :param group_by_template: a boolean indicating whether the results should be grouped by template product.
            :param numerators_only: if True, the returned denominators_map will be empty, and only
                documents containing non-null, non-zero numerators will be fetched.

            :returns: A tuple (numerators_map, denominators_map), where
                      numerators_map is
                          a dict {(product_id, template_product_id): [conservative_numerator, aggressive_numerator]},
                             if group_by_template is False;
                             OR
                          a dict {product_id: {target_product_id: [conservative_numerator, aggressive_numerator]}},
                             if group_by_template is True, and
                      denominators_map is
                          a dict {(product_id, template_product_id): denominator},
                             if group_by_template is False;
                             OR
                          a dict {product_id: {template_product_id: denominator}},
                             if group_by_template is True.
        """

    @abc.abstractmethod
    def fetch_product_product_strengths_tfidf(self, products=None, templates=None):
        """ Retrieves the strengths (via TFIDF) between some pairs of products.

            :param products: OR-filter for *product* field.
            :param templates: OR-filter for *template_product* field.

            :returns: A map {(product, template_product): strength} where the keys satisfy
                (*product* in *products*) and (*template_product* in *templates*).
        """

    @abc.abstractmethod
    def save_user_user_numerators(self, strength_numerators, increment=False, upsert=True):
        """ Increments the numerators associated to user-to-user strengths.

            :param strength_numerators: A dict {(user, template): strength_numerator_tuple}, where
                       strength_numerator_tuple is a (conservative_numerator, aggressive_numerator) pair.
            :param increment: if True, adds the informed numerators to the existing ones, if any; otherwise,
                       it overwrites whatever existing value might there be.
            :param upsert: if True, performs upserts; if False, inserts each new document.
        """

    @abc.abstractmethod
    def save_uu_strengths(self, strength_docs_map, upsert=False, deferred_publication=False):
        """ Saves a sparse matrix of user-to-user strengths, along with operands.

            :param strength_docs_map: A dict {(user, template): strength_doc}, where
                strength_doc is a dict {"user": user,
                                        "template_user": template user,
                                        "nc": conservative numerator,
                                        "na": aggressive numerator,
                                        "denominator": denominator,
                                        "strength": strength}
                whose mandatory fields are only "user" and "template_user".
            :param upsert: if True, performs upserts (by user,template_user);
                           if False, just inserts each new document.
            :param deferred_publication: if True, the informed strengths will be saved to a temporary
                       collection, and will only go live after a call to self.hotswap_uu_strengths().
        """

    @abc.abstractmethod
    def save_user_templates(self, templates_by_user):
        """ Caches pre-rendered user templates of users.
            :param templates_by_user: a dict {user_id: [(strength1, template1), (strength2, template2), ...]}
        """

    def save_product_templates(self, templates_by_product):
        """ Caches pre-rendered product templates of products.
            :param templates_by_product: dict {product_id: tuple (templates, templates_tfidf)}, where
                both templates and templates_tfidf are lists like [(strength1, template1), (strength2, template2), ...].
        """

    @abc.abstractmethod
    def save_latest_activity_for_user_user_strengths(self, user, product, activity_type, activity_date):
        """ Persists the latest activity considered for user-user strengths
            (among those activities of the informed u-p pair).

            :param user: The user id.
            :param product: The product id.
            :param activity_type: The activity type.
            :param activity_date: The activity date.
        """

    @abc.abstractmethod
    def copy_all_latest_activities_for_user_user_strengths(self, cutoff_date):
        """ Copies field values from:
                - latest activity type to latest activity type considered for user-user strengths, and
                - latest activity date to latest activity date considered for user-user strengths
            in the activities summary collection for all (user, product) pairs.
            Should be called by the end of a batch generation of u-u strengths.

            :param cutoff_date: The minimum date to be considered when traversing the activity summaries.
        """

    @abc.abstractmethod
    def save_product_product_numerators(self, strength_numerators, increment=False, upsert=True):
        """ Increments the numerators associated to product-to-product strengths.

            :param strength_numerators: A dict {(product, template): strength_numerator_tuple}, where
                       strength_numerator_tuple is a (conservative_numerator, aggressive_numerator) pair.
            :param increment: if True, adds the informed numerators to the existing ones, if any; otherwise,
                       it overwrites whatever existing value might there be.
            :param upsert: if True, performs upserts; if False, inserts each new document.
        """

    @abc.abstractmethod
    def save_pp_strengths(self, strength_docs_map, upsert=False, deferred_publication=False):
        """ Saves a sparse matrix of product-to-product strengths, along with operands.

            :param strength_docs_map: A dict {(product, template): strength_doc}, where
                strength_doc is a dict {"product": product,
                                        "template_product": template product,
                                        "nc": conservative numerator,
                                        "na": aggressive numerator,
                                        "denominator": denominator,
                                        "strength": strength}
                whose mandatory fields are only "product" and "template_product".
            :param upsert: if True, performs upserts (by product,template_product);
                           if False, just inserts each new document.
            :param deferred_publication: if True, the informed strengths will be saved to a temporary
                       collection, and will only go live after a call to self.hotswap_pp_strengths().
        """

    @abc.abstractmethod
    def save_latest_activity_for_product_product_strengths(self, user, product, activity_type, activity_date):
        """ Persists the latest activity considered for product-product strengths
            (among those activities of the informed u-p pair).

            :param user: The user id.
            :param product: The product id.
            :param activity_type: The activity type.
            :param activity_date: The activity date.
        """

    @abc.abstractmethod
    def copy_all_latest_activities_for_product_product_strengths(self, cutoff_date):
        """ Copies field values from:
                - latest activity type to latest activity type considered for product-product strengths, and
                - latest activity date to latest activity date considered for product-product strengths
            in the activities summary collection for all (user, product) pairs.
            Should be called by the end of a batch generation of p-p strengths.

            :param cutoff_date: The minimum date to be considered when traversing the activity summaries.
        """

    @abc.abstractmethod
    def save_product_product_strengths_tfidf(self, strengths, start_index=None, end_index=None,
                                             deferred_publication=False):
        """ Saves a sparse matrix of Product x Product strengths based on tfidf.

            :param strengths: A list of {"product": product_external_id,
                                         "template_product": template_product_external_id,
                                         "strength": similarity score}
                                         dicts.
            :param start_index: if not None, indicates the start index (inclusive) in the list of strengths.
            :param end_index: if not None, indicates the end index (exclusive) in the list of strengths.
            :param deferred_publication: if True, the informed strengths will be saved to a temporary
                       collection, and will only go live after a call to self.hotswap_product_product_strengths_tfidf().
        """

    @abc.abstractmethod
    def hotswap_uu_strengths(self):
        """ Triggers a swap between the actual and the temporary user-user strength collections.
            Should be called after all batch write operations have finished.
        """

    @abc.abstractmethod
    def hotswap_pp_strengths(self):
        """ Triggers a swap between the actual and the temporary product-product strength collections.
            Should be called after all batch write operations have finished.
        """

    @abc.abstractmethod
    def hotswap_product_product_strengths_tfidf(self):
        """ Triggers a swap between the actual and the temporary product-product strength (tfidf) collections.
            Should be called after all batch write operations have finished.
        """

    @abc.abstractmethod
    def hotswap_product_models(self):
        """ Triggers a swap between the actual and the temporary product_models collections.
            Should be called after all batch write operations have finished.
        """

    @abc.abstractmethod
    def delete_product(self, product_id, date):
        """ Deletes a product. Used only for testing purposes.
            :param product_id: The id of the intended product.
            :param date: The deletion date (for logical deletion).
        """

    @abc.abstractmethod
    def delete_product_model(self, product_id):
        """ Deletes a product model.
            :param product_id: The id of the intended product.
        """

    @abc.abstractmethod
    def update_product(self, product_id, field, new_value):
        """ Updates a product field in the database. Used only for testing purposes.
            :param product_id: The id of the intended product.
            :param field: The desired field.
            :param new_value: The new value for the product field.
        """

    @abc.abstractmethod
    def remove_product_terms(self, attributes, product_id):
        """ Deletes all terms from the given product.
            :param attributes: The list of attributes of the intended product.
            :param product_id: The id of the intended product.
        """

    @abc.abstractmethod
    def remove_tfidf(self, attributes, product_id):
        """ Deletes all TFIDF entries with respect to the given product.
            :param attributes: The list of attributes of the intended product.
            :param product_id: The id of the intended product.
        """

    def save_user_model(self, user_id, user_model):
        """ Saves a new user model to a Barbante-owned collection.

            :param user_id: the id of the user.
            :param user_model: a {key: value} dictionary with user attributes.
        """

    def save_product_model(self, product_id, product_model, deferred_publication=False):
        """ Saves a new product model to a Barbante-owned collection.

            :param product_id: the id of the product.
            :param product_model: a ProductModel object.
            :param deferred_publication: if True, the informed product model will be saved to a temporary
                       collection, and will only go live after a call to self.hotswap_product_models().
        """

    @abc.abstractmethod
    def insert_user(self, user):
        """ Inserts a new user.
            This is only used for tests purposes, since Barbante never writes to the users collection.

            :param user: a {key: value} dictionary representing a user.
                Mandatory field: "external_id".
        """

    @abc.abstractmethod
    def insert_product(self, product):
        """ Inserts a new product.
            This is only used for tests purposes, since Barbante never writes to the products collection.

            :param product: a {key: value} dictionary representing a product.
                Mandatory field: "external_id".
        """

    @abc.abstractmethod
    def insert_product_models(self, records, deferred_publication=False):
        """ Saves a whole bunch of product models in bulk.

            WARNING: Pure inserts are intended.

            :param records: A list with several flattened {"external_product_id": product_id, and whatever other
                    product model fields} dicts.
            :param deferred_publication: if True, the informed product models will be saved to a temporary
                       collection, and will only go live after a call to self.hotswap_product_models().
        """

    @abc.abstractmethod
    def insert_product_terms(self, tf_records):
        """ Persists the TF of each term within the given product attribute.

            WARNING: It performs pure inserts. If there are pre-existing (attribute, language, product, term) tuples,
                     the old and the new ones will co-exist (which is probably not what the caller intends).
                     A deletion should therefore take place beforehand in case an update is intended.

            :param tf_records: A list of dicts {"external_product_id": id, "attribute", attr,
                "term": term, "count": count (i.e., the term TF)}.
        """

    @abc.abstractmethod
    def insert_tfidf_records(self, tf_records):
        """ Persists the TFIDF of each top term within the given product attribute.

            WARNING: It performs pure inserts. If there are pre-existing (attribute, language, product, term) tuples,
                     the old and the new ones will co-exist (which is probably not what the caller intends).
                     A deletion should therefore take place beforehand in case an update is intended.

            :param tf_records: A list of dicts {"external_product_id": id, "attribute",
                attr, "term": term, "tfidf": tfidf}.
        """

    @abc.abstractmethod
    def reset_impression_summary(self, user_id, product_id, anonymous):
        """ Resets the count of impressions associated to a (user, product) pair.

            :param user_id: The id of the user.
            :param product_id: The product whose impressions' count will be reset.
            :param anonymous: if True, it will update the anonymous impressions collection.
        """

    @abc.abstractmethod
    def increment_impression_summary(self, user_id, product_id, date, anonymous):
        """ Saves an entry to the summary of impressions.

            :param user_id: The id of the user that received the impression.
            :param product_id: The product which the impression is all about.
            :param date: The date of the impression which triggered the increment.
            :param anonymous: if True, it will update the anonymous impressions collection.
        """

    @abc.abstractmethod
    def update_product_popularity(self, product_id, date, do_increment=True):
        """ Increments the count of users with popularity-defining activities w.r.t. the given product,
            and persists the updated "relative popularity" of the product, which is the ratio
            "user count" over "number of days between the first and the latest activity" for that product.

            :param product_id: The product whose popularity should be updated.
            :param date: The date of the activity which triggered the increment.
            :param do_increment: If True, will increment the user count.
                                 If False, will only take care of the date range, according to the date parameter.
        """

    @abc.abstractmethod
    def save_activity_summary(self, activity, anonymous, set_popularity_flag=False):
        """ Upserts an entry into the summary of activities.

            :param activity: a dict {"external_user_id": the id of the user,
                                     "external_product_id": the id of the product,
                                     "activity": the type of the activity, e.g., bought, read,
                                     "created_at": the datetime of the activity}.
            :param anonymous: if True, it will save the activity summary in the anonymous activities collection.
            :param set_popularity_flag: a boolean indicating whether the field "contributed_for_popularity"
                should be set to True. If the parameter is False, the field will not be updated.
        """

    @abc.abstractmethod
    def save_df(self, language, df_by_term, increment=False, upsert=False):
        """ Updates the DF of each term inside a given language.

            :param language: The terms' language.
            :param df_by_term: A map {term: DF} for each term in the product model attribute.
            :param increment: If True, the informed DF's will be summed to the existing ones (if any) for those terms.
            :param upsert: If True, pre-existing values will be overridden. Otherwise, a plain insert will take place.
                Notice that increment=True only makes sense when upsert=True.
        """

    @abc.abstractmethod
    def find_df(self, language, term):
        """ Retrieves the document frequency of the informed (language, term) tuple.

            :param language: The language (written in English, e.g. "portuguese").
            :param term: The term.

            :returns: The number of documents whose language is *language* containing *term*.
        """

    @abc.abstractmethod
    def fetch_df_map(self, language, terms):
        """ Retrieves the DF of all informed terms.

            :param language: The common language of all terms.
            :param terms: A list with all intended terms.

            :returns: A map {term: df} where *df* is the number of documents whose language is
                *language* containing *term*.
        """

    @abc.abstractmethod
    def fetch_tf_map(self, attribute, products):
        """ Retrieves the TF of all terms of the informed products w.r.t. to given attribute.

            :param attribute: The product attribute to be considered.
            :param products: A list with the intended product id's.

            :returns: A map {product_id: {term: TF}}.
        """

    @abc.abstractmethod
    def fetch_tfidf_map(self, attribute, products):
        """ Retrieves the TFIDF of the top terms of the informed products w.r.t. to given attribute.

            :param attribute: The product attribute to be considered.
            :param products: A list with the intended product id's.

            :returns: A map {product_id: {term: TFIDF}}.
        """

    @abc.abstractmethod
    def get_user_count(self):
        """ Returns the number of stored users.
        """

    @abc.abstractmethod
    def get_product_count(self):
        """ Returns the number of stored products.
        """

    @abc.abstractmethod
    def get_product_model_count(self):
        """ Returns the number of product models.
        """

    @abc.abstractmethod
    def sample_users(self, count):
        """ Samples users uniformly at random.

            :param count: The intended number of users.
            :returns: A set with *count* user ids.
        """

    @abc.abstractmethod
    def drop_database(self, dbname):
        """ Used *ONLY* for tests initialization.
        """

    @abc.abstractmethod
    def copy_database(self, fromdb, todb):
        """ Used *ONLY* for tests initialization.
        """

    @abc.abstractmethod
    def backup_database(self):
        """ Used *ONLY* for tests initialization.
        """

    @abc.abstractmethod
    def restore_database(self):
        """ Used *ONLY* for tests initialization.
        """

    @abc.abstractmethod
    def reset_all_product_content_data(self):
        """ Deletes all data from product models and product terms' collections,
            recreating all necessary indexes (if applicable).
        """

    @abc.abstractmethod
    def reset_user_user_strength_auxiliary_data(self, create_reverse_direction_index=True):
        """ Deletes all data from auxiliary collections devoted to user-to-user strengths,
            and creates all necessary indexes (if applicable).

            :param create_reverse_direction_index: if True, creates an index on (template_user, user) as well;
                                                   if False, only the (user, template_user) index will be created.
        """

    @abc.abstractmethod
    def reset_product_product_strength_auxiliary_data(self, create_reverse_direction_index=True):
        """ Deletes all data from auxiliary collections devoted to product-to-product strengths,
            and creates all necessary indexes (if applicable).

            :param create_reverse_direction_index: if True, creates an index on (product, template_product) as well;
                                                   if False, only the (template_product, product) index will be created.
        """

    @abc.abstractmethod
    def reset_product_product_strength_tfidf_auxiliary_data(self):
        """ Deletes all data from auxiliary collections devoted to product-to-product similarities (content-based),
            and creates all necessary indexes (if applicable).
        """

    @abc.abstractmethod
    def fetch_latest_batch_info_product_models(self):
        """ Retrieves info about the latest batch processing of products.

            :returns: A map {"timestamp": the system time corresponding to the start of the latest batch run,
                             "cutoff_date": the minimum date for products in the latest batch (in case there
                             was a date constraint)}.
        """

    @abc.abstractmethod
    def fetch_latest_batch_info_user_user_strengths(self):
        """ Retrieves info about the latest generation from scratch of user-user strengths.

            :returns: A map {"timestamp": the system time corresponding to the start of the latest batch run,
                             "cutoff_date": the minimum date for activities and impressions in the latest batch}.
        """

    @abc.abstractmethod
    def fetch_latest_batch_info_product_product_strengths(self):
        """ Retrieves info about the latest generation from scratch of product-product strengths.

            :returns: A map {"timestamp": the system time corresponding to the start of the latest batch run,
                             "cutoff_date": the minimum date for activities and impressions in the latest batch}.
        """

    @abc.abstractmethod
    def fetch_latest_batch_info_user_user_consolidation(self):
        """ Retrieves info about the latest batch consolidation of user-user strengths.

            :returns: A map {"timestamp": the system time corresponding to the start of the latest batch run,
                             "status": the status of the latest batch run: running, successful, or failed.
        """

    @abc.abstractmethod
    def fetch_latest_batch_info_product_template_consolidation(self):
        """ Retrieves info about the latest batch consolidation of product-product strengths.

            :returns: A map {"timestamp": the system time corresponding to the start of the latest batch run,
                             "status": the status of the latest batch run: running, successful, or failed.
        """

    @abc.abstractmethod
    def save_timestamp_user_user_strengths(self, timestamp, cutoff_date, elapsed_time):
        """ Persists the datetime of the latest generation from scratch of user-to-user strengths.

            :param timestamp: the system time (aka present_date) corresponding to the start of the batch.
            :param cutoff_date: the minimum date for activities and impressions considered in this batch run.
            :param elapsed_time: The elapsed time of this batch run.
        """

    @abc.abstractmethod
    def save_timestamp_product_product_strengths(self, timestamp, cutoff_date, elapsed_time):
        """ Persists the datetime of the latest generation from scratch of product-to-product strengths.

            :param timestamp: the system time (aka present_date) corresponding to the start of the batch.
            :param cutoff_date: the minimum date for activities and impressions considered in this batch run.
            :param elapsed_time: The elapsed time of this batch run.
        """

    @abc.abstractmethod
    def save_timestamp_product_product_strengths_tfidf(self, timestamp, cutoff_date, elapsed_time):
        """ Persists the datetime of the latest generation from scratch of product-to-product
            content-based similarities.

            :param timestamp: the system time (aka present_date) corresponding to the start of the batch.
            :param cutoff_date: the minimum date for activities and impressions considered in this batch run.
            :param elapsed_time: The elapsed time of this batch run.
        """

    @abc.abstractmethod
    def save_timestamp_product_models(self, timestamp, cutoff_date, elapsed_time):
        """ Persists the datetime of the latest generation from scratch of
            product models and terms (tf, df, tfidf).

            :param timestamp: the system time (aka present_date) corresponding to the start of the batch.
            :param cutoff_date: the minimum date for activities and impressions considered in this batch run.
            :param elapsed_time: The elapsed time of this batch run.
        """

    @abc.abstractmethod
    def save_timestamp_user_template_consolidation(self, timestamp, status, elapsed_time=None):
        """ Persists the datetime and the status of the latest batch consolidation of user templates.

            :param timestamp: The system time (aka present_date) corresponding to the start of the batch.
            :param status: The status of this batch run: running, successful, or failed.
            :param elapsed_time: The elapsed time.
        """

    @abc.abstractmethod
    def save_timestamp_product_template_consolidation(self, timestamp, status, elapsed_time=None):
        """ Persists the datetime and the status of the latest batch consolidation of product templates.

            :param timestamp: the system time (aka present_date) corresponding to the start of the batch.
            :param status: The status of this batch run: running, successful, or failed.
            :param elapsed_time: The elapsed time.
        """
