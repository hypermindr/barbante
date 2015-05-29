import datetime as dt
from time import time

import barbante.config as config
import barbante.context
import barbante.utils.decay_functions as df
import barbante.utils as utils
import barbante.utils.logging as barbante_logging


recommender_classes = {}
""" This static recommender_classes dict keeps a reference to pre-loaded recommender classes to avoid discovery time.
"""

log = barbante_logging.get_logger(__name__)


class UserContext(object):
    def __init__(self, session_context, user_id, context_filter=None, algorithm=None):
        super().__init__()

        if session_context is None:
            raise AttributeError("Session context cannot be None")

        if user_id is None:
            raise AttributeError("User ID cannot be None")

        if user_id.__hash__ is None:
            raise TypeError("User ID must be hashable")

        self.session_context = session_context
        """ The customer context associated to this user
        """
        self.user_id = user_id
        """ The id of the target user.
        """
        self.is_anonymous = config.is_anonymous(self.user_id)
        """ Indicates whether the target user is 'anonymous' (identified only by her cookies or something).
        """
        self.filter = context_filter
        """ The user context filter, to be applied when filtering the possible recommendations
        """
        self.algorithm = algorithm
        """ The algorithm that will be used for recommending products throughout this session.
        """
        self.specialist_recommenders = set()
        """ A list with the suffixes of all specialist recommenders employed by this session's recommendation algorithm.
        """
        self.user_impressions_summary = None
        """ A map {product: (count, first_impression_date)} summarizing the impressions of the target user.
        """
        self.user_templates = None
        """ The user templates of the target user.
        """
        self.recent_activities = []
        """ A list of {"external_product_id": product_id, "activity": activity_type, "created_at": datetime} dicts
            with all recent activities of the target user in descending order of dates.
        """
        self.recent_activities_by_product = None
        """ A dict {product: list of (date, activity_type) tuples in descending order of dates} corresponding to the
            latest activities of the target user.
        """
        self.recent_activities_by_template_user = None
        """ A map {user_id: list of {"external_product_id": product_id, "activity": activity_type,
            "created_at": datetime} dicts with all recent activities of each user in descending order of dates}.
            The users whose recent activities are pre-fetched here are those in self.user_templates.
        """
        self.recent_activities_by_product_by_template_user = None
        """ A map {user_id: {product: list of (date, activity_type) tuples in descending order of dates}}.
            The users whose recent activities are pre-fetched here are those in self.user_templates.
        """
        self.blocked_products = None
        """ A set of products which shall not be recommended (any further)
            owing to previous consumption activities of the target.
        """
        self.filtered_products = None
        """ A set of product ids corresponding to products which passes the session filter.
            When the filtering strategy is BEFORE_SCORING, these products are determined during the session
            initialization. When the strategy is AFTER_SCORING, these products are determined a posteriori,
            when the recommender calls self.apply_pos_filter_to_products() passing the intended set of
            products to be filtered.
        """
        self.product_models = {}
        """ A map {product_id: ProductModel instance} with the product models for:
            - pre-filtered products, when the filtering strategy is BEFORE_SCORING;
            - products recently consumed by the target user;
            - products recently consumed by template users of the target user.
        """
        self.most_recently_consumed_products = None
        """ A list with the target user's recently consumed product ids, in descending order of consumption.
        """

        self._determine_specialist_recommenders()

        self.refresh()

    def __getattr__(self, item):
        return getattr(self.session_context.customer_context, item)

    def refresh(self):
        self._load_recent_activities()

        if len(self.blocking_activities) > 0:
            self._load_blocked_products()

        self._determine_sorted_list_of_recently_consumed_products()
        self._load_user_impressions()

        if self.should_preload_filtered_products():
            self._determine_pre_filtered_products()

        if self.should_preload_user_user_collaborative_filtering_data():
            self._load_user_templates()
            self._load_recent_activities_of_templates()
            if self.filter_strategy == barbante.context.AFTER_SCORING:
                self._load_product_models_for_collaborative_filtering()

        self.log_stats()

    def get_recommender(self, algorithm=None):
        """ Retrieves the intended recommender instance.

            If no algorithm is passed as a parameter, then it returns the recommender that was set
            in the construction of the session context.

            Note that hybrid recommenders aggregate several concrete recommenders (a.k.a specialists),
            so it is possible to specify the intended specialist in the *algorithm* parameter.

            :param algorithm: The algorithm suffix, identifying the intended specialist recommender.

            :returns: A Recommender instance.
        """
        if algorithm is None:
            algorithm = self.algorithm

        user_id = self.user_id
        recommender_class = recommender_classes.get(algorithm, None)
        if recommender_class is None:
            recommender_name = "Recommender" + algorithm
            module = utils.local_import("barbante.recommendation." + recommender_name)
            recommender_class = getattr(module, recommender_name)
            recommender_classes[algorithm] = recommender_class

        start_time = time()
        log.info("Loading recommender [{0}] for user [{1}]".format(algorithm, user_id))
        recommender = recommender_class(self.session_context)
        log.info("Recommender [{0}] loaded in [{1:2.6f}s]".format(algorithm, time() - start_time))
        return recommender

    def _determine_specialist_recommenders(self):
        if self.algorithm is not None:
            recommender = self.get_recommender()
            if recommender.is_hybrid():
                self.specialist_recommenders = {aw[0] for aw in self.algorithm_weights[recommender.get_suffix()]}
                if self.fill_in_algorithm is not None:
                    self.specialist_recommenders.add(self.fill_in_algorithm)
            else:
                self.specialist_recommenders = {self.algorithm}

    def _load_recent_activities(self):
        log.info("Loading recent activities...")
        latest_activity_day = self.data_proxy.fetch_day_of_latest_user_activity(
            self.user_id, anonymous=self.is_anonymous)
        if latest_activity_day is None:
            self.recent_activities_by_product = {}  # the user has no activities
        else:
            relative_cutoff_day = latest_activity_day - dt.timedelta(self.session_context.short_term_window)

            self.recent_activities = self.data_proxy.fetch_activity_summaries_by_user(
                user_ids=[self.user_id],
                min_day=relative_cutoff_day,
                indexed_fields_only=False,
                anonymous=self.is_anonymous).get(self.user_id, [])

            self.recent_activities_by_product = {}
            for activity in self.recent_activities:
                product = activity["external_product_id"]
                date = activity["created_at"]
                activity_type = activity["activity"]
                self.recent_activities_by_product[product] = (date, activity_type)
        log.info("Loaded [%d] recent activities on [%d] products."
                 % (len(self.recent_activities), len(self.recent_activities_by_product)))

    def _load_recent_activities_of_templates(self):
        log.info("Loading recent activities of templates...")
        user_ids = [t[1] for t in self.user_templates]
        source_activities = []
        for r in range(self.min_rating_recommendable_from_user, 6):
            source_activities += self.activities_by_rating.get(r)
        self.recent_activities_by_template_user = self.data_proxy.fetch_activity_summaries_by_user(
            user_ids=user_ids,
            activity_types=source_activities,
            min_day=self.session_context.short_term_cutoff_date,
            anonymous=False)  # there is no such thing as an anonymous user template, anyway
        self.recent_activities_by_product_by_template_user = {}

        activities_count = 0
        for template, template_activities in self.recent_activities_by_template_user.items():
            activities_by_product = {}
            activities_count += len(template_activities)
            for activity in template_activities:
                product = activity["external_product_id"]
                day = activity["day"]
                activity_type = activity["activity"]
                activities_by_product[product] = (day, activity_type)
            self.recent_activities_by_product_by_template_user[template] = activities_by_product
        log.info("Loaded [%d] recent activities of [%d] template users."
                 % (len(self.recent_activities), len(self.recent_activities_by_template_user)))

    def _load_user_impressions(self):
        """ Loads into the context the count of impressions per product received by the user after
            the latest activity w.r.t. each product (if any).
        """
        log.info("Loading impression summaries of the target user...")
        self.user_impressions_summary = self.data_proxy.fetch_impressions_summary(
            user_ids=[self.user_id], anonymous=self.is_anonymous).get(self.user_id, {})
        log.info("Loaded [%d] user impression summaries." % len(self.user_impressions_summary))

    def _load_user_templates(self):
        """ Loads into the context the top user templates of the user.
        """
        log.info("Loading user templates...")
        self.user_templates = self.data_proxy.fetch_user_templates([self.user_id]).get(self.user_id, [])
        log.info("Loaded [%d] user templates." % len(self.user_templates))

    def _load_blocked_products(self):
        """ Determines products which cannot be recommended owing to (custom-defined) blocking activities in the past.
            :returns: A set with the IDs of products that cannot be recommended.
        """
        log.info("Loading blocked products...")
        blocked_products = set()
        for activity in self.recent_activities:
            if activity["activity"] in self.blocking_activities:
                blocked_products.add(activity["external_product_id"])
        self.blocked_products = blocked_products
        log.info("Loaded [%d] blocked products." % len(self.blocked_products))

    def _determine_pre_filtered_products(self):
        """ Fetches all products whose product models conform to the session context filter.
            It gathers the product models of those products, and adds them to the self.product_models cache.
            It also stores the id's of all such products for later reference in self.filtered_products.
        """
        log.info("Loading pre-filtered products...")
        start = time()

        context_filter_as_canonical_string = str(sorted([item for item in self.json_filter().items()]))
        if self.context_filters_cache is not None:
            self.filtered_products = self.context_filters_cache.get(context_filter_as_canonical_string)

        if self.filtered_products is None:
            log.info("Fetching product models that pass the non-cached filter...")
            this_filter_product_models = self.data_proxy.fetch_product_models(context_filter=self.json_filter())
            self.product_models.update(this_filter_product_models)
            self.filtered_products = {p for p in this_filter_product_models}
            self.add_to_context_filters_cache(context_filter_as_canonical_string, self.filtered_products)
        else:
            log.info("Pretty easy -- this filter was cached!")

        products_to_fetch = self.filtered_products.copy()

        if self.product_models:
            products_to_fetch -= self.product_models.keys()

        if len(products_to_fetch) > 0:
            log.info("Fetching [%d] non-cached product models..." % len(products_to_fetch))
            new_product_models = self.data_proxy.fetch_product_models(product_ids=list(products_to_fetch))
            self.product_models.update(new_product_models)
        log.info("Done loading [%d] pre-filtered products. Took %d milliseconds."
                 % (len(self.filtered_products), 1000 * (time() - start)))

    def _load_product_models_for_collaborative_filtering(self):
        log.info("Loading product models for user-user collaborative filtering...")
        products_to_fetch = set(self.recent_activities_by_product.keys())
        for _, products in self.recent_activities_by_product_by_template_user.items():
            for product_id in products:
                products_to_fetch.add(product_id)
        if self.product_models:
            products_to_fetch -= self.product_models.keys()
        new_product_models = self.data_proxy.fetch_product_models(
            product_ids=list(products_to_fetch), context_filter=self.json_filter())
        self.product_models.update(new_product_models)
        log.info("Loaded [%d] product models for user-user collaborative filtering." % len(new_product_models))

    def _determine_sorted_list_of_recently_consumed_products(self):
        log.info("Determining sorted list of recently consumed products...")
        products_list = []
        products_set = set()
        for activity in self.recent_activities:
            product = activity["external_product_id"]
            if product not in products_set:
                products_list += [product]
                products_set.add(product)
        self.most_recently_consumed_products = products_list
        log.info("Determined sorted list of [%d] recently consumed products."
                 % len(self.most_recently_consumed_products))

    def apply_pos_filter_to_products(self, product_ids):
        """ Retrieves product models for the informed products that pass the session context filter.

            :param product_ids: A list with the ids of the intended products.

            IMPORTANT:
            This method should be called *after* the ids of the candidate products
                are determined by the concrete recommender subclass.
            This prevents that all products would otherwise be retrieved.
        """
        product_models_map = self.data_proxy.fetch_product_models(product_ids, self.json_filter())
        self.product_models.update(product_models_map)
        self.filtered_products = set(product_models_map.keys())
        return self.filtered_products

    def obtain_history_decay_factor(self, product_id):
        """ Produces a history decay factor according to some mathematical function of
            the number of previous recommendations of a certain product to a certain user.

            :param product_id: The id of the product.
        """
        if self.history_decay_function_name is None:
            return 1

        if self.user_impressions_summary is not None:
            latest_impressions_count = self.user_impressions_summary.get(product_id, (0, None))[0]
        else:
            latest_impressions_count = 0

        result = 1

        if self.history_decay_function_name == "linear":
            ttl = self.history_decay_linear_function_ttl
            result = df.linear(latest_impressions_count, ttl)
        elif self.history_decay_function_name == "rational":
            result = df.rational(latest_impressions_count)
        elif self.history_decay_function_name == "exponential":
            halflife = self.history_decay_exponential_function_halflife
            result = df.exponential(latest_impressions_count, halflife)
        elif self.history_decay_function_name == "step":
            ttl = self.history_decay_step_function_ttl
            result = df.step(latest_impressions_count, 1, -1, ttl)

        return result

    def obtain_previous_consumption_factor(self, product_id):
        """ Produces a previous consumption decay factor according to whether the latest
            activity of the user w.r.t. the given product (if any) is a blocking activity.

            PS.: So far we only support fixed decay factors (constant functions, that is).

            :param product_id: The id of the product.
        """
        return self.previous_consumption_factor if product_id in self.blocked_products else 1

    def obtain_in_boost(self, product_id):
        """ Retrieves the in-boost factor associated to the given product.

            :param product_id: The id of the product.
        """
        in_boost = 1

        product_activity = self.recent_activities_by_product.get(product_id)
        if product_activity is not None:
            activity_type = product_activity[1]
            in_boost = self.session_context.in_boost_by_activity.get(activity_type, 1)

        return in_boost

    def json_filter(self):
        if self.filter:
            return self.filter.to_json()
        else:
            return {}

    def log_stats(self):
        log.info('User {0} stats:'.format(self.user_id))
        log.info('User {0} has [{1}] recent_activities'.format(self.user_id, len(self.recent_activities)))
        log.info(
            'User {0} consumed [{1}] products since {2}'.format(self.user_id, len(self.most_recently_consumed_products),
                                                              self.session_context.short_term_cutoff_date))
        if self.user_templates is not None:
            log.info('User {0} has [{1}] user_templates'.format(self.user_id, len(self.user_templates)))
        else:
            log.info('User templates were not loaded')
        if self.recent_activities_by_product_by_template_user is not None:
            template_products_count = len({product_id for _, products in
                                           self.recent_activities_by_product_by_template_user.items()
                                           for product_id in products})
            log.info('User {0} templates have consumed [{1}] products'.format(self.user_id, template_products_count))
        else:
            log.info('Products recently consumed by user templates were not loaded')

    def should_preload_user_user_collaborative_filtering_data(self):
        return "UBCF" in self.specialist_recommenders

    def should_preload_filtered_products(self):
        return self.filter_strategy == barbante.context.BEFORE_SCORING
