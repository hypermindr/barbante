import pytz

from barbante import config
import barbante.context
from barbante.data.BaseProxy import BaseProxy
import barbante.model.product_model as pm
from barbante.model.product_model_factory import ProductModelFactory
from barbante.utils import decay_functions as df
from barbante.utils.cache import Cache
import barbante.utils.logging as barbante_logging
log = barbante_logging.get_logger(__name__)


class CustomerContext(object):
    """ This class encapsulates a customer context, i.e., the set of configurations associated to a customer.
    """

    def __init__(self, customer, db_proxy, database_settings, cache_settings):
        """
        :param customer: The customer name.
        :param db_proxy: May be a descendant class of BaseProxy or an instance of that descendant class. If it is the
        class itself, CustomerContext will create a new instance of it; otherwise, it will reuse the instance passed.
        :param database_settings: The settings to be used for accessing the database.
        :return: A new CustomerContext instance
        """

        super().__init__()

        self.customer = customer
        """ The customer for this context.
        """
        self.default_product_date_field = self._get_setting("DEFAULT_PRODUCT_DATE_FIELD")
        """ The name of the date field to be used in product queries concerning time when no other field is informed.
        """
        self.recommendable_product_start_date_field = self._get_setting("RECOMMENDABLE_PRODUCT_START_DATE_FIELD")
        """ The name of the date field that should be less or equal than the present moment for a product to be
            recommendable.
        """
        self.recommendable_product_end_date_field = self._get_setting("RECOMMENDABLE_PRODUCT_END_DATE_FIELD")
        """ The name of the date field that should be greater or equal than the present moment for a product to be
            recommendable.
        """
        self.database_settings = database_settings
        """ The database settings (host/database name).
        """
        self.data_proxy = None
        """ The database proxy instance used by this customer context.
        """
        self.set_data_proxy(db_proxy)

        self._present_date = None
        """ The date used to define the system current date.
            It is generally used as a hard limit when querying for activities and products.
            If no present_date is provided, the OS system date is used.
        """
        self.short_term_window = self._get_setting("SHORT_TERM_WINDOW")
        """ The short term time window used as a limit when querying for recent activity.
        """
        self.long_term_window = self._get_setting("LONG_TERM_WINDOW")
        """ The long term time window used generally to limit the data universe for template generation.
        """
        self.popularity_window = self._get_setting("POPULARITY_WINDOW")
        """ The number of days of the time window used for popularity counts (usually shorter than
            the short term window).
        """
        self.risk_factor = self._get_setting("RISK_FACTOR")
        """ The risk factor (between 0 and 1) for collaborative recommendations:
            0 is conservative (focus on at least 3 stars); 1 is aggressive (focus on 5 stars).
        """
        self.top_terms_count = self._get_setting("COUNT_TOP_TERMS")
        """ The number of relevant terms per document in text-based recommendations.
        """
        self.base_products_count = self._get_setting("COUNT_RECENT_PRODUCTS")
        """ The minimum number of recently consumed items that will be used as base for product-similarity algorithms.
        """
        self.user_templates_count = self._get_setting("COUNT_USER_TEMPLATES")
        """ The number of user templates to be considered.
        """
        self.product_templates_count = self._get_setting("COUNT_PRODUCT_TEMPLATES")
        """ The number of product templates to be considered (for each recently consumed 'base product').
        """
        self.user_user_strengths_window = self._get_setting("DAYS_FOR_USER_USER_STRENGTHS")
        """ The time window used as a limit when calculating product-product strengths.
        """
        self.product_product_strengths_window = self._get_setting("DAYS_FOR_PRODUCT_PRODUCT_STRENGTHS")
        """ The time window used as a limit when calculating product-product strengths.
        """
        self.product_product_strengths_tfidf_window = self._get_setting("DAYS_FOR_PRODUCT_PRODUCT_STRENGTHS_TFIDF")
        """ The time window used as a limit when calculating product-product strengths (tfidf).
        """
        self.should_consolidate_user_templates_on_the_fly = self._get_setting(
            "SHOULD_CONSOLIDATE_USER_TEMPLATES_ON_THE_FLY")
        """ If True, pre-renderization of user templates will take place during by the end of each update of
            user-user strengths.
        """
        self.should_consolidate_product_templates_on_the_fly = self._get_setting(
            "SHOULD_CONSOLIDATE_PRODUCT_TEMPLATES_ON_THE_FLY")
        """ If True, pre-renderization of product templates will take place during by the end of each update of
            product-product strengths.
        """
        self.bidirectional_uu_strength_updates = self._get_setting("BIDIRECTIONAL_UU_STRENGTH_UPDATES")
        """ If True, user-user strengths will be updated on both directions (user <--> template) on the fly.
        """
        self.bidirectional_pp_strength_updates = self._get_setting("BIDIRECTIONAL_PP_STRENGTH_UPDATES")
        """ If True, product-product strengths will be updated on both directions (product <--> template) on the fly.
        """
        self.history_decay_function_name = self._get_setting("HISTORY_DECAY_FUNC")
        self.history_decay_linear_function_ttl = self._get_setting("HISTORY_DECAY_LINEAR_FUNCTION_TTL")
        self.history_decay_exponential_function_halflife = self._get_setting(
            "HISTORY_DECAY_EXPONENTIAL_FUNCTION_HALFLIFE")
        self.history_decay_step_function_ttl = self._get_setting("HISTORY_DECAY_STEP_FUNCTION_TTL")
        """ The function to be used for decaying scores based on past recommendations (and eventual parameters).
        """
        self.product_age_decay_function_name = self._get_setting("PRODUCT_AGE_DECAY_FUNC")
        self.product_age_decay_linear_function_ttl = self._get_setting("PRODUCT_AGE_DECAY_LINEAR_FUNCTION_TTL")
        self.product_age_decay_exponential_function_halflife = self._get_setting(
            "PRODUCT_AGE_DECAY_EXPONENTIAL_FUNCTION_HALFLIFE")
        self.product_age_decay_step_function_ttl = self._get_setting("PRODUCT_AGE_DECAY_STEP_FUNCTION_TTL")
        """ The function to be used for decaying scores based on the product age (and eventual parameters).
        """
        self.previous_consumption_factor = self._get_setting("PREVIOUS_CONSUMPTION_FACTOR")
        """ A factor to be applied to the score of pre-filtered products already consumed by the target user.
        """
        self.near_identical_filter_field = self._get_setting("NEAR_IDENTICAL_FILTER_FIELD")
        """ The field used to remove duplicates if there are too many terms in common.
        """
        self.near_identical_filter_threshold = self._get_setting("NEAR_IDENTICAL_FILTER_THRESHOLD")
        """ The field used to remove duplicates if there are too many terms in common.
        """
        self.recommendations_page_size = self._get_setting("RECOMMENDATIONS_PAGE_SIZE")
        """ The most likely size of each recommendations page
            (to be used to avoid near-identical recommendations in the same page).
        """
        self.product_text_fields = []
        """ A raw list with all TEXT-type product attributes.
        """
        self.product_non_text_fields = []
        """ A raw list with all non-TEXT-type product attributes.
        """
        self.similarity_filters_by_type = {}
        """ A dict {attribute_type: list of attribute_name's} for calculating product similarities.
            Equality is required for all such fields so that two products may have non-zero mutual similarity.
        """
        self.similarity_weights_by_type = {}
        """ A dict {attribute_type: dict {attribute_name: attribute_weight}} for calculating
            product similarities. Each type is handled differently by the similarity functions, and
            the scores assigned to each attribute's contributions are multiplied by the corresponding
            attribute's weight.
        """
        self.date_similarity_halflife = self._get_setting("DATE_SIMILARITY_HALFLIFE")
        """ The difference between two products' dates (in days) that makes their date-based similarity be 0.5.
            Note that an inverse exponential function is used. If none is informed, than date-based similarities
            will always be 1.
        """
        self.min_user_user_strength_numerator = self._get_setting("MIN_USER_USER_STRENGTH_NUMERATOR")
        """ The minimum number of common products before a user-to-user strength can be non-zero.
        """
        self.min_product_product_strength_numerator = self._get_setting("MIN_PRODUCT_PRODUCT_STRENGTH_NUMERATOR")
        """ The minimum number of common users before a product-to-product strength can be non-zero.
        """
        self.page_size_user_user_numerators = self._get_setting("PAGE_SIZE_USER_USER_NUMERATORS")
        """ The number of products contributing to user-user strengths in each processing unit of numerators.
        """
        self.page_size_user_user_denominators = self._get_setting("PAGE_SIZE_USER_USER_DENOMINATORS")
        """ The number of target users in user-user pairs in each processing unit (page) of denominators.
        """
        self.page_size_product_product_numerators = self._get_setting("PAGE_SIZE_PRODUCT_PRODUCT_NUMERATORS")
        """ The number of users contributing to product-product strengths in each processing unit of numerators.
        """
        self.page_size_product_product_denominators = self._get_setting("PAGE_SIZE_PRODUCT_PRODUCT_DENOMINATORS")
        """ The number of template products in product-product pairs in each processing unit (page) of denominators.
        """
        self.page_size_batch_process_products = self._get_setting("PAGE_SIZE_BATCH_PROCESS_PRODUCTS")
        """ The number of products to be processed in each processing unit (page) during creation of product models.
        """
        self.max_workers_user_user_strengths = self._get_setting("MAX_WORKERS_USER_USER_STRENGTHS")
        """ The maximum number of parallel threads during user-user strengths generation in batch.
        """
        self.max_workers_product_product_strengths = self._get_setting("MAX_WORKERS_PRODUCT_PRODUCT_STRENGTHS")
        """ The maximum number of parallel threads during product-product strengths generation in batch.
        """
        self.max_workers_batch_process_products = self._get_setting("MAX_WORKERS_BATCH_PROCESS_PRODUCTS")
        """ The maximum number of parallel threads during batch creation of product models.
        """
        self.max_workers_template_consolidation = self._get_setting("MAX_WORKERS_TEMPLATE_CONSOLIDATION")
        """ The maximum number of parallel threads during batch consolidation of user/product templates.
        """
        self.flush_size = self._get_setting("FLUSH_SIZE")
        """ The number of queued db operations which forces a flush.
        """
        self.max_recommendations = self._get_setting("MAX_RECOMMENDATIONS")
        """ Hard limit for recommendation queries. If a query goes beyond the limit an Exception is raised.
        """
        self.recommendation_timeout = self._get_setting("RECOMMENDATION_TIMEOUT")
        """ The timeout in seconds a hybrid recommender will wait for a specialist request to return
            If a specialist reaches timeout its results are ignored
        """
        self.min_rating_recommendable_from_user = self._get_setting("MIN_RATING_RECOMMENDABLE_FROM_USER")
        """ The minimum rating of recommendable products in user-to-user strategies.
        """
        self.min_rating_recommendable_from_product = self._get_setting("MIN_RATING_RECOMMENDABLE_FROM_PRODUCT")
        """ The minimum rating of base consumed products in item-to-item strategies.
        """
        self.min_rating_conservative = self._get_setting("MIN_RATING_CONSERVATIVE")
        """ The minimum rating in conservative strengths.
        """
        self.min_rating_aggressive = self._get_setting("MIN_RATING_AGGRESSIVE")
        """ The minimum rating in aggressive strengths.
        """
        self.product_model_factory = ProductModelFactory(self._get_setting("PRODUCT_MODEL"))
        """ Product model definition.
        """
        self.supported_activities = []
        """ The supported activities.
            Activities that are not supported are simply disregarded by the recommender engine.
        """
        self.blocking_activities = []
        """ The blocking activities.
            A blocking activity prevents a product to be further recommended to a user within
            a same short_term_window.
        """
        self.activities_by_rating = {rating: [] for rating in range(1, 6)}  # ratings from 1 to 5
        """ A dict whose key is the (implicit) rating of to the activities it maps to.
        """
        self.rating_by_activity = {}
        """ A dict whose value is the (implicit) rating of the key activity.
        """
        self.in_boost_by_activity = {}
        """ A dict whose key is the activity type corresponding to the in-boost factor it maps to.
        """
        self.out_boost_by_activity = {}
        """ A dict whose key is the activity type corresponding to the out-boost factor it maps to.
        """
        self.impressions_enabled = self._get_setting("IMPRESSIONS_ENABLED")
        """ Indicates whether impressions (products shown to users) are kept track of.
        """
        self.filter_strategy = self._get_setting("FILTER_STRATEGY")
        """ Defines the filter strategy to be used when filters are applied to the recommenders.
            The supported filtering strategies are BEFORE_SCORING and AFTER_SCORING. The best choice depends on
            the cardinality of filtered products set and the cardinality of the recommendation candidates set
            as calculated by the various algorithms. The filter should be applied to the smallest set.
        """
        self.algorithm_weights = self._get_setting("ALGORITHM_WEIGHTS")
        """ Stores, for each hybrid recommender, the weight distribution of the algorithms in the form of a list of
            [algorithm_suffix, probability, extra_comma_separated_directives] tuples}, where:
                *algorithm_suffix* identifies a specialist algorithm,
                *probability* is the importance (slice size, merge probability, vote power, etc.)
                 assigned to an algorithm, and
                *extra_comma_separated_directives* are optional, hybrid-recommender-specific settings
                 associated to an algorithm, e.g. *nobonus* indicates that the "bonus time" in HRVoting
                 shall not apply to that specific algorithm.
            The sum of the probabilities must be 1.
            We use lists because, for some hybrid recommenders, the order matters.
        """
        self.fill_in_algorithm = self._get_setting("FILL_IN_ALGORITHM")

        self._load_activity_types()
        self._load_product_similarity_attributes()

        self.initial_date = self._get_setting("PRESENT_DATE")
        """ Used only in tests to set the present date """

        self.context_filters_cache = Cache(cache_settings, 'context_filters') if cache_settings else None
        """ A cache for product_ids that correspond to recently used context filters.
        """

    def set_data_proxy(self, db_proxy):
        if isinstance(db_proxy, BaseProxy):
            self.data_proxy = db_proxy
        elif issubclass(db_proxy, BaseProxy):
            self.data_proxy = db_proxy(self)
        else:
            raise TypeError('Parameter must be an instance of or a descendant class of BaseProxy')

    def create_product_model(self, product_id, product):
        return self.product_model_factory.build(product_id, product)

    def should_pre_load_filters(self):
        return self.filter_strategy == barbante.context.BEFORE_SCORING

    def clear_context_filters_cache(self):
        if self.context_filters_cache:
            self.context_filters_cache.clear()

    def add_to_context_filters_cache(self, filter, products):
        if self.context_filters_cache:
            self.context_filters_cache.set(filter, products)

    def _load_activity_types(self):
        customer_settings = config.customers[self.customer]
        for activity_type, act in customer_settings['ACTIVITIES'].items():

            activity_rating = act['rating']

            self.supported_activities += [activity_type]
            self.activities_by_rating[activity_rating] += [activity_type]
            self.rating_by_activity[activity_type] = activity_rating
            if act['blocking']:
                self.blocking_activities += [activity_type]

            in_boost = act['in-boost']
            if in_boost is not None:
                self.in_boost_by_activity[activity_type] = in_boost
            out_boost = act['out-boost']
            if out_boost is not None:
                self.out_boost_by_activity[activity_type] = out_boost

    def _load_product_similarity_attributes(self):
        customer_settings = config.customers[self.customer]
        text_fields_set = set()
        non_text_fields_set = set()

        for attribute_name, attribute_properties in customer_settings['PRODUCT_MODEL'].items():
            _type = attribute_properties["type"]

            if _type == pm.TEXT:
                text_fields_set.add(attribute_name)
            else:
                non_text_fields_set.add(attribute_name)

            if attribute_properties.get("similarity_filter", False):
                filters = self.similarity_filters_by_type.get(_type, [])
                filters += [attribute_name]
                self.similarity_filters_by_type[_type] = filters

            weight = attribute_properties.get("similarity_weight", 0)
            if weight > 0:
                weights = self.similarity_weights_by_type.get(_type, {})
                weights[attribute_name] = weight
                self.similarity_weights_by_type[_type] = weights

        self.product_text_fields = list(text_fields_set)
        self.product_non_text_fields = list(non_text_fields_set)

    def obtain_product_age_decay_factor(self, product_date, present_date):
        """ Produces an age decay factor according to some mathematical function of the
            number of units of time (days, weeks) since the product was added to the system.

            :param product_date: The date of the product.
            :param present_date: The current date.
        """
        if self.product_age_decay_function_name is None:
            return 1

        product_age_in_days = None
        if product_date is not None:
            try:
                utc_product_date = pytz.utc.localize(product_date)
            except Exception:
                utc_product_date = product_date
            product_age = present_date - utc_product_date
            product_age_in_days = product_age.days

        result = 1

        if self.product_age_decay_function_name == "linear":
            ttl = self.product_age_decay_linear_function_ttl
            result = df.linear(product_age_in_days, ttl)
        elif self.product_age_decay_function_name == "rational":
            result = df.rational(product_age_in_days)
        elif self.product_age_decay_function_name == "exponential":
            halflife = self.product_age_decay_exponential_function_halflife
            result = df.exponential(product_age_in_days, halflife)
        elif self.product_age_decay_function_name == "step":
            ttl = self.product_age_decay_step_function_ttl
            result = df.step(product_age_in_days, 1, -1, ttl)

        return result

    def _get_setting(self, attribute):
        """ Retrieves an attribute from the customer settings.

            :param attribute: The intended attribute name.
            :return: The attribute value.
        """
        _settings = config.customers[self.customer]
        result = _settings.get(attribute)
        if result is None:
            return None
        if str(result).lower() == "none":
            return None
        return result