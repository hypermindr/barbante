import pymongo
from pymongo.read_preferences import ReadPreference
from random import random

import barbante.config as config
from barbante.data.BaseProxy import BaseProxy
from barbante.utils.profiling import profile
from barbante.model.product_model import ProductModel
import barbante.utils.date as du

import barbante.utils.logging as barbante_logging

log = barbante_logging.get_logger(__name__)


class MongoDBProxy(BaseProxy):
    """ Data proxy to be used with MongoDB.
    """

    _read_preferences = {
        'primary': ReadPreference.PRIMARY,
        'primary_preferred': ReadPreference.PRIMARY_PREFERRED,
        'secondary': ReadPreference.SECONDARY,
        'secondary_only': ReadPreference.SECONDARY_ONLY,
        'secondary_preferred': ReadPreference.SECONDARY_PREFERRED,
        'nearest': ReadPreference.NEAREST
    }

    def __init__(self, context):
        super().__init__(context)

        self.database = self._initialize_pymongo_connection(context, "main")
        """ The MongoDB to be used for normal read/write operations.
        """
        self.database_bulk = self._initialize_pymongo_connection(context, "bulk")
        """ The MongoDB to be used for bulk write operations.
        """
        self.database_raw = self._initialize_pymongo_connection(context, "raw db", raw=True)
        """ The MongoDB to be used for bulk write operations.
        """
        self.database_name = context.database_settings.name
        """ The name of the database.
        """
        self.database_backup_name = None
        """ The name of the database backup (used only for tests).
        """
        self.write_concern_level = 0  # default write concern
        """ Describes the guarantee that MongoDB provides when reporting on the success of a write operation.
        """
        self.default_product_date_field = context.default_product_date_field
        """ The name of the date field to be used in product queries concerning time when no other field is informed.
        """

    def _initialize_pymongo_connection(self, context, connection_name, raw=False):
        log.info("Initializing MongoDB %s connection..." % connection_name)

        read_preference = self.parse_read_preferences(context.database_settings.read_preference)

        host = context.database_settings.host_raw if raw else context.database_settings.host
        db_name = context.database_settings.name_raw if raw else context.database_settings.name
        replica_set = context.database_settings.replica_set_raw if raw else context.database_settings.replica_set

        if replica_set:
            # When connecting to a replica set cluster, we must use a MongoReplicaSetClient,
            # or the secondaries won't be used by pymongo.
            if len(host) > 1:
                host_connection_string = ','.join(host)
            else:
                host_connection_string = host[0]

            pymongo_connection = pymongo.MongoReplicaSetClient(host_connection_string,
                                                               replicaSet=replica_set,
                                                               read_preference=read_preference,
                                                               tz_aware=True)
        else:
            pymongo_connection = pymongo.MongoClient(host,
                                                     read_preference=read_preference,
                                                     tz_aware=True)

        log.info("MongoDB connections ready...")
        return pymongo_connection[db_name]

    def parse_read_preferences(self, read_preference):
        return self._read_preferences.get(read_preference, ReadPreference.PRIMARY)

    @profile
    def fetch_all_user_ids(self):
        """ See barbante.data.BaseProxy.
        """
        fields = {"external_id": True, "_id": False}
        cursor = self.database_raw.users.find({"anonymous": False}, fields)
        result = (rec["external_id"] for rec in cursor)
        return result

    @profile
    def fetch_all_product_ids(self, allow_deleted=False, required_fields=None,
                              min_date=None, max_date=None, product_date_field=None):
        """ See barbante.data.BaseProxy.
        """
        if product_date_field is None:
            product_date_field = self.default_product_date_field
        products_map = self.fetch_products(fields_to_project=[], required_fields=required_fields,
                                           min_date=min_date, max_date=max_date,
                                           product_date_field=product_date_field,
                                           allow_deleted=allow_deleted)
        result = (rec["external_id"] for rec in products_map.values())
        return result

    @profile
    def fetch_user_templates(self, user_ids):
        """ See barbante.data.BaseProxy.
        """
        result = {}
        where = {"external_user_id": {"$in": user_ids}}
        fields = {"external_user_id": True, "user_templates": True, "_id": False}
        cursor = self.database.user_cache.find(where, fields)
        for rec in cursor:
            result[rec["external_user_id"]] = rec.get("user_templates", [])
        return result

    @profile
    def fetch_top_uu_strengths(self, user_id, n_templates):
        """ See barbante.data.BaseProxy.
        """
        sort_order = [("user", pymongo.ASCENDING),
                      ("strength", pymongo.DESCENDING),
                      ("template_user", pymongo.ASCENDING)]
        where = {"user": user_id, "strength": {"$exists": True, "$nin": ["", 0]}}
        fields = {"user": True, "strength": True, "template_user": True, "_id": False}
        cursor = self.database.uu_strengths.find(where, fields).sort(sort_order).limit(n_templates)
        result = ((rec["strength"], rec["template_user"]) for rec in cursor)
        return result

    @profile
    def fetch_product_templates(self, product_ids):
        """ See barbante.data.BaseProxy.
        """
        result = {}
        where = {"external_product_id": {"$in": product_ids}}
        fields = {"external_product_id": True,
                  "product_templates": True,
                  "product_templates_tfidf": True,
                  "_id": False}
        cursor = self.database.product_cache.find(where, fields)
        for rec in cursor:
            result[rec["external_product_id"]] = (rec.get("product_templates", []),
                                                  rec.get("product_templates_tfidf", []))
        return result

    @profile
    def fetch_top_pp_strengths(self, product_id, n_templates, blocked_products=None,
                               collaborative=True, tfidf=True, allowed_products=None):
        """ See barbante.data.BaseProxy.
        """
        sort_order = [("product", pymongo.ASCENDING),
                      ("strength", pymongo.DESCENDING),
                      ("template_product", pymongo.ASCENDING)]
        fields = {"product": True, "strength": True, "template_product": True, "_id": False}

        where = {"product": product_id, "strength": {"$exists": True, "$nin": ["", 0]}}
        if allowed_products and blocked_products:
            where["template_product"] = {"$in": list(set(allowed_products) - set(blocked_products))}
        elif blocked_products:
            where["template_product"] = {"$nin": blocked_products}
        elif allowed_products:
            where["template_product"] = {"$in": allowed_products}

        templates = []
        if collaborative:
            cursor_templates = self.database.pp_strengths.find(
                where, fields).sort(sort_order).limit(n_templates)
            for row in cursor_templates:
                templates.append([row["strength"], row["template_product"]])

        templates_tfidf = []
        if tfidf:
            cursor_templates_tfidf = self.database.product_product_strengths_tfidf.find(
                where, fields).sort(sort_order).limit(n_templates)
            for row in cursor_templates_tfidf:
                templates_tfidf.append([row["strength"], row["template_product"]])

        result = (templates, templates_tfidf)

        return result

    @profile
    def fetch_activity_summaries_by_user(self, anonymous, user_ids=None, product_ids=None, activity_types=None,
                                         num_activities=None, min_day=None,
                                         indexed_fields_only=True):
        """ See barbante.data.BaseProxy.
        """
        result = {}

        if user_ids is None and product_ids is None:
            raise ValueError("Parameters 'user_ids' and 'product_ids' cannot both be None")

        collection = self.database.anonymous_activities_summary if anonymous else self.database.activities_summary

        if activity_types is None:
            activity_types = self.context.supported_activities

        where = {'activity': {'$in': activity_types}}

        date_clause = self._build_date_clause('day', min_day)
        if len(date_clause) > 0:
            where.update(date_clause)

        fields = None
        if indexed_fields_only:
            fields = {"external_user_id": True,
                      "external_product_id": True,
                      "activity": True,
                      "day": True,
                      "_id": False}

        sort_order = {}
        if product_ids is not None:
            where.update({"external_product_id": {"$in": product_ids}})
            sort_order = [("external_product_id", pymongo.ASCENDING),
                          ("day" if indexed_fields_only else "created_at", pymongo.DESCENDING)]
        if user_ids is not None:
            where.update({"external_user_id": {"$in": user_ids}})
            sort_order = [("external_user_id", pymongo.ASCENDING),
                          ("day" if indexed_fields_only else "created_at", pymongo.DESCENDING)]

        if num_activities:
            cursor = collection.find(where, fields).sort(sort_order).limit(num_activities)
        else:
            cursor = collection.find(where, fields).sort(sort_order)

        for rec in cursor:
            user_id = rec["external_user_id"]
            product_id = rec["external_product_id"]
            activity_type = rec["activity"]
            activity_day = rec["day"]
            activity = {"external_user_id": user_id,
                        "external_product_id": product_id,
                        "activity": activity_type,
                        "day": activity_day}
            if not indexed_fields_only:
                activity["created_at"] = rec["created_at"]
                activity["contributed_for_popularity"] = rec.get("contributed_for_popularity", False)
                activity["uu_latest_type"] = rec.get("uu_latest_type")
                activity["uu_latest_date"] = rec.get("uu_latest_date")
                activity["pp_latest_type"] = rec.get("pp_latest_type")
                activity["pp_latest_date"] = rec.get("pp_latest_date")

            user_activities = result.get(user_id, [])
            user_activities += [activity]
            result[user_id] = user_activities

        return result

    @profile
    def fetch_day_of_latest_user_activity(self, user_id, anonymous):
        """ See barbante.data.BaseProxy.
        """
        collection = self.database.anonymous_activities_summary if anonymous else self.database.activities_summary
        fields = {"day": True, "_id": False}
        sort_order = [("external_user_id", pymongo.ASCENDING),
                      ("day", pymongo.DESCENDING)]
        cursor = collection.find({"external_user_id": user_id}, fields).sort(sort_order).limit(1)
        for doc in cursor:
            return doc["day"]
        return None

    @profile
    def fetch_products_by_rating_by_user(self, user_ids=None, min_date=None, max_date=None):
        """ See barbante.data.BaseProxy.
        """
        result = {}
        count_products_by_rating = [0] * 5  # initial count 0 for all ratings from 1 to 5

        activities_by_user = self.fetch_activity_summaries_by_user(
            user_ids=user_ids, min_day=min_date, anonymous=False)

        for user, activities in activities_by_user.items():
            user_products = set()

            for activity in activities:
                product = activity["external_product_id"]
                if product in user_products:
                    continue  # only considers the most recent activity for each (user, product) pair
                user_products.add(product)

                user_results = result.get(user)
                if user_results is None:
                    user_results = {r: set() for r in range(1, 6)}  # ratings from 1 to 5

                activity_type = activity["activity"]
                rating = self.context.rating_by_activity[activity_type]
                user_results[rating].add(product)
                result[user] = user_results
                count_products_by_rating[rating - 1] += 1  # -1 because the min rating is 1 whereas the first index is 0

        return result, count_products_by_rating

    @profile
    def fetch_users_by_rating_by_product(self, product_ids=None, min_date=None, max_date=None):
        """ See barbante.data.BaseProxy.
        """
        result = {}
        count_users_by_rating = [0] * 5  # initial count 0 for all ratings from 1 to 5

        # the activities are fetched in descending order of date
        activities_by_user = self.fetch_activity_summaries_by_user(
            product_ids=product_ids, min_day=min_date, anonymous=False)

        for user, activities in activities_by_user.items():
            user_products = set()

            for activity in activities:
                product = activity["external_product_id"]
                if product in user_products:
                    continue  # only considers the most recent activity for each (user, product) pair
                user_products.add(product)

                product_results = result.get(product)
                if product_results is None:
                    product_results = {r: set() for r in range(1, 6)}  # ratings from 1 to 5

                activity_type = activity["activity"]
                rating = self.context.rating_by_activity[activity_type]
                product_results[rating].add(user)
                result[product] = product_results
                count_users_by_rating[rating - 1] += 1  # -1 because the min rating is 1 whereas the first index is 0

        return result, count_users_by_rating

    @profile
    def fetch_product_popularity(self, product_ids=None, n_products=None, min_day=None):
        """ See barbante.data.BaseProxy.
        """
        if product_ids is None and n_products is None:
            raise ValueError("Parameters 'product_ids' and 'n_products' cannot both be None")

        if n_products == 0:
            return {}

        where = {}

        date_clause = self._build_date_clause("latest", min_day)
        if len(date_clause) > 0:
            where.update(date_clause)

        if product_ids is not None:
            where["p_id"] = {"$in": product_ids}

        fields = {"p_id": True, "popularity": True, "_id": False}
        sort_order = [("popularity", pymongo.DESCENDING)]

        if n_products is not None:
            cursor = self.database.popularities_summary.find(where, fields).sort(sort_order).limit(n_products)
        else:
            cursor = self.database.popularities_summary.find(where, fields).sort(sort_order)

        return {rec["p_id"]: rec["popularity"] for rec in cursor}

    @profile
    def fetch_impressions_summary(self, anonymous, user_ids=None, product_ids=None, group_by_product=False):
        """ See barbante.data.BaseProxy.
        """
        result = {}

        collection = self.database.anonymous_impressions_summary if anonymous else self.database.impressions_summary

        where = {}
        fields = {"u_id": True, "p_id": True, "count": True, "first": True, "_id": False}
        if user_ids is not None:
            where.update({"u_id": {"$in": user_ids}})
        if product_ids is not None:
            where.update({"p_id": {"$in": product_ids}})

        cursor = collection.find(where, fields)

        for rec in cursor:
            user = rec["u_id"]
            product = rec["p_id"]
            count = rec["count"]
            first_impression_date = rec["first"]
            if group_by_product:
                product_impressions = result.get(product, {})
                product_impressions[user] = (count, first_impression_date)
                result[product] = product_impressions
            else:
                user_impressions = result.get(user, {})
                user_impressions[product] = (count, first_impression_date)
                result[user] = user_impressions

        return result

    @profile
    def fetch_users_with_impressions_by_product(self, anonymous, product_ids=None, user_ids=None):
        """ See barbante.data.BaseProxy.
        """
        collection = self.database.anonymous_impressions_summary if anonymous else self.database.impressions_summary
        fields = {"u_id": True, "p_id": True, "_id": False}
        where = {}
        if product_ids is not None:
            where["p_id"] = {"$in": product_ids}
        if user_ids is not None:
            where["u_id"] = {"$in": user_ids}

        result = {}
        cursor = collection.find(where, fields)
        for rec in cursor:
            user = rec["u_id"]
            product = rec["p_id"]
            users_of_product = result.get(product, set())
            users_of_product.add(user)
            result[product] = users_of_product

        return result

    @profile
    def fetch_products_with_impressions_by_user(self, anonymous, user_ids=None, product_ids=None):
        """ See barbante.data.BaseProxy.
        """
        collection = self.database.anonymous_impressions_summary if anonymous else self.database.impressions_summary
        fields = {"u_id": True, "p_id": True, "_id": False}
        where = {}
        if user_ids is not None:
            where["u_id"] = {"$in": user_ids}
        if product_ids is not None:
            where["p_id"] = {"$in": product_ids}

        result = {}
        cursor = collection.find(where, fields)
        for rec in cursor:
            user = rec["u_id"]
            product = rec["p_id"]
            products_of_user = result.get(user, set())
            products_of_user.add(product)
            result[user] = products_of_user

        return result

    @profile
    def fetch_products(self, product_ids=None, fields_to_project=None, required_fields=None,
                       min_date=None, max_date=None, product_date_field=None, allow_deleted=False):
        """ See barbante.data.BaseProxy.
        """
        clauses = {}
        if product_ids is not None:
            clauses["external_id"] = {"$in": product_ids}

        if product_date_field is None:
            product_date_field = self.default_product_date_field
        date_clause = self._build_date_clause(product_date_field, min_date, max_date)
        if len(date_clause) > 0:
            clauses.update(date_clause)

        if allow_deleted:
            where = {}
        else:
            where1 = clauses.copy()
            where1.update({"deleted_on": None})
            if max_date is not None:
                where2 = clauses.copy()
                where2.update({"deleted_on": {"$gte": max_date}})
                where = {"$or": [where1, where2]}
            else:
                where = where1

        if required_fields:
            for field in required_fields:
                where.update({field: {"$exists": True, "$ne": None}})

        if fields_to_project is None:
            cursor = self.database_raw.products.find(where)
        else:
            fields = {f: True for f in fields_to_project}
            fields["_id"] = False
            fields["external_id"] = True
            cursor = self.database_raw.products.find(where, fields)

        result = {record["external_id"]: record for record in cursor}

        return result

    @profile
    def fetch_product_models_for_top_tfidf_terms(self, attribute, language, terms, min_date=None, max_date=None):
        """ See barbante.data.BaseProxy.
        """
        product_ids = set()
        where = {"attribute": attribute,
                 "term": {"$in": terms}}
        fields = {"external_product_id": True, "_id": False}

        cursor = self.database.tfidf.find(where, fields)
        for record in cursor:
            product_ids.add(record["external_product_id"])

        return self.fetch_product_models(product_ids=list(product_ids), min_date=min_date, max_date=max_date)

    @profile
    def fetch_product_models(self, product_ids=None, context_filter=None,
                             min_date=None, max_date=None, product_date_field=None, ids_only=False):
        """ See barbante.data.BaseProxy.
        """
        where = {}

        fields = {}
        if not ids_only:
            all_model_fields = self.context.product_text_fields + self.context.product_non_text_fields
            fields.update({f: True for f in all_model_fields})
        fields["_id"] = False
        fields["external_product_id"] = True

        if context_filter is not None:
            where.update(context_filter)
        if product_ids is not None:
            where["external_product_id"] = {"$in": product_ids}

        if product_date_field is None:
            product_date_field = self.default_product_date_field
        date_clause = self._build_date_clause(product_date_field, min_date, max_date)
        if len(date_clause) > 0:
            where.update(date_clause)

        limit = self.context.max_recommendations
        cursor = self.database.product_models.find(where, fields).limit(limit + 1)

        if ids_only:
            result = [doc['external_product_id'] for doc in cursor]
        else:
            result = {}
            for doc in cursor:
                result[doc['external_product_id']] = ProductModel.from_dict(doc['external_product_id'], doc,
                                                                            self.context.product_model_factory)
                if len(result) > limit:
                    raise Exception("Product model query limit reached ({0}).".format(limit))

        return result

    @profile
    def fetch_date_filtered_products(self, reference_date, lte_date_field=None, gte_date_field=None):
        """ See barbante.data.BaseProxy.
        """
        date_filter = {}
        if lte_date_field:
            date_filter[lte_date_field] = {"$lte": reference_date}
        if gte_date_field:
            date_filter[gte_date_field] = {"$gte": reference_date}
        else:
            date_filter[self.default_product_date_field] = {"$gte": reference_date}
        return self.fetch_product_models(context_filter=date_filter, ids_only=True)

    @profile
    def fetch_user_user_strengths(self, users=None, templates=None):
        """ See barbante.data.BaseProxy.
        """
        result = {}
        where = {"strength": {"$exists": True, "$nin": ["", 0]}}
        if users is not None:
            where["user"] = {"$in": users}
        if templates is not None:
            where["template_user"] = {"$in": templates}
        fields = {"user": True, "template_user": True, "strength": True, "_id": False}
        cursor = self.database.uu_strengths.find(where, fields)
        result.update({(rec["user"], rec["template_user"]): rec["strength"] for rec in cursor})
        return result

    @profile
    def fetch_user_user_strength_operands(self, users=None, templates=None, group_by_target=False,
                                          numerators_only=False):
        """ See barbante.data.BaseProxy.
        """
        numerators_map = {}
        denominators_map = {}

        if numerators_only:
            where = {"nc": {"$exists": True, "$nin": ["", 0]}}
        else:
            where = {}

        if users is not None:
            where["user"] = {"$in": users}
        if templates is not None:
            where["template_user"] = {"$in": templates}

        fields = {"user": True, "template_user": True, "nc": True, "na": True, "_id": False}
        if not numerators_only:
            fields["denominator"] = True

        cursor = self.database.uu_strengths.find(where, fields)
        for rec in cursor:
            target_user = rec["user"]
            template_user = rec["template_user"]
            numerators = [rec.get("nc", 0), rec.get("na", 0)]
            denominator = rec.get("denominator", 0)
            if group_by_target:
                if numerators != [0, 0]:
                    numerators_by_template_user = numerators_map.get(target_user, {})
                    numerators_by_template_user[template_user] = numerators
                    numerators_map[target_user] = numerators_by_template_user
                if denominator != 0 and not numerators_only:
                    denominator_by_template_user = denominators_map.get(target_user, {})
                    denominator_by_template_user[target_user] = denominator
                    denominators_map[target_user] = denominator_by_template_user
            else:
                if numerators != [0, 0]:
                    numerators_map[(target_user, template_user)] = numerators
                if denominator != 0 and not numerators_only:
                    denominators_map[(target_user, template_user)] = denominator

        return numerators_map, denominators_map

    @profile
    def fetch_product_product_strengths(self, products=None, templates=None):
        """ See barbante.data.BaseProxy.
        """
        result = {}
        where = {"strength": {"$exists": True, "$nin": ["", 0]}}
        if products is not None:
            where["product"] = {"$in": products}
        if templates is not None:
            where["template_product"] = {"$in": templates}
        fields = {"product": True, "template_product": True, "strength": True, "_id": False}
        cursor = self.database.pp_strengths.find(where, fields)
        result.update(
            {(rec["product"], rec["template_product"]): rec["strength"] for rec in cursor})
        return result

    @profile
    def fetch_product_product_strength_operands(self, products=None, templates=None, group_by_template=False,
                                                numerators_only=False):
        """ See barbante.data.BaseProxy.
        """
        numerators_map = {}
        denominators_map = {}

        if numerators_only:
            where = {"nc": {"$exists": True, "$nin": ["", 0]}}
        else:
            where = {}

        if products is not None:
            where["product"] = {"$in": products}
        if templates is not None:
            where["template_product"] = {"$in": templates}

        fields = {"product": True, "template_product": True, "nc": True, "na": True, "_id": False}
        if not numerators_only:
            fields["denominator"] = True

        cursor = self.database.pp_strengths.find(where, fields)
        for rec in cursor:
            base_product = rec["product"]
            template_product = rec["template_product"]
            numerators = [rec.get("nc", 0), rec.get("na", 0)]
            denominator = rec.get("denominator", 0)
            if group_by_template:
                if numerators != [0, 0]:
                    numerators_by_base_product = numerators_map.get(template_product, {})
                    numerators_by_base_product[base_product] = numerators
                    numerators_map[template_product] = numerators_by_base_product
                if denominator != 0 and not numerators_only:
                    denominator_by_base_product = denominators_map.get(template_product, {})
                    denominator_by_base_product[template_product] = denominator
                    denominators_map[template_product] = denominator_by_base_product
            else:
                if numerators != [0, 0]:
                    numerators_map[(base_product, template_product)] = numerators
                if denominator != 0 and not numerators_only:
                    denominators_map[(base_product, template_product)] = denominator

        return numerators_map, denominators_map

    @profile
    def fetch_product_product_strengths_tfidf(self, products=None, templates=None):
        """ See barbante.data.BaseProxy.
        """
        where, result = {}, {}
        if products is not None:
            where["product"] = {"$in": products}
        if templates is not None:
            where["template_product"] = {"$in": templates}
        fields = {"product": True, "template_product": True, "strength": True, "_id": False}
        cursor = self.database.product_product_strengths_tfidf.find(where, fields)
        result.update({(rec["product"], rec["template_product"]): rec["strength"] for rec in cursor})
        return result

    @profile
    def save_user_user_numerators(self, strength_numerators, increment=False, upsert=True):
        """ See barbante.data.BaseProxy.
        """
        if len(strength_numerators) == 0:
            return

        collection = self.database_bulk.uu_strengths

        if upsert:
            operator = "$inc" if increment else "$set"

            bulk_op = collection.initialize_unordered_bulk_op()
            has_operations = False

            for user_and_template, strength_numerator_tuple in strength_numerators.items():
                has_operations = True
                user = user_and_template[0]
                template = user_and_template[1]
                spec = {"user": user, "template_user": template}
                update_clause = {operator: {"nc": strength_numerator_tuple[0],
                                            "na": strength_numerator_tuple[1]}}
                bulk_op.find(spec).upsert().update(update_clause)

            if has_operations:
                bulk_op.execute(write_concern={'w': self.write_concern_level})

        else:
            strength_numerators_list = [{"user": u, "template_user": t, "nc": n[0], "na": n[1]}
                                        for (u, t), n in strength_numerators.items()]
            collection.insert(
                strength_numerators_list, w=self.write_concern_level, manipulate=False)

    def save_uu_strengths(self, strength_docs_map, upsert=False, deferred_publication=False):
        """ See barbante.data.BaseProxy.
        """
        if len(strength_docs_map) == 0:
            return

        if deferred_publication:
            collection = self.database_bulk.uu_strengths_temp
            write_concern = 1  # forces write completion due to hotswap that follows
        else:
            collection = self.database_bulk.uu_strengths
            write_concern = self.write_concern_level

        if upsert:
            bulk_op = collection.initialize_unordered_bulk_op()
            has_operations = False

            for user_and_template, strength_doc in strength_docs_map.items():
                has_operations = True
                user = user_and_template[0]
                template = user_and_template[1]
                spec = {"user": user, "template_user": template}
                update_clause = {"$set": strength_doc}
                bulk_op.find(spec).upsert().update(update_clause)

            if has_operations:
                bulk_op.execute(write_concern={'w': write_concern})
        else:
            collection.insert(strength_docs_map.values(), w=write_concern, manipulate=False)

    @profile
    def save_user_templates(self, templates_by_user):
        """ See barbante.data.BaseProxy.
        """
        bulk_op = self.database.user_cache.initialize_unordered_bulk_op()
        has_operations = False

        for user, templates in templates_by_user.items():
            has_operations = True
            spec = {"external_user_id": user}
            update_clause = {"$set": {"user_templates": templates}}
            bulk_op.find(spec).upsert().update(update_clause)

        if has_operations:
            bulk_op.execute(write_concern={'w': self.write_concern_level})

    @profile
    def save_product_templates(self, templates_by_product):
        """ See barbante.data.BaseProxy.
        """
        bulk_op = self.database.product_cache.initialize_unordered_bulk_op()
        has_operations = False

        for product, templates_tuple in templates_by_product.items():
            update_map = {}
            if templates_tuple[0]:
                update_map["product_templates"] = templates_tuple[0]
            if templates_tuple[1]:
                update_map["product_templates_tfidf"] = templates_tuple[1]
            if len(update_map) > 0:
                has_operations = True
                spec = {"external_product_id": product}
                update_clause = {"$set": update_map}
                bulk_op.find(spec).upsert().update(update_clause)

        if has_operations:
            bulk_op.execute(write_concern={'w': self.write_concern_level})

    @profile
    def save_latest_activity_for_user_user_strengths(self, user, product, activity_type, activity_date):
        """ See barbante.data.BaseProxy.
        """
        spec = {"external_user_id": user,
                "external_product_id": product}
        update_clause = {"$set": {"uu_latest_type": activity_type,
                                  "uu_latest_date": activity_date}}

        self.database.activities_summary.update(spec,
                                                update_clause,
                                                upsert=True,
                                                w=self.write_concern_level)

    @profile
    def copy_all_latest_activities_for_user_user_strengths(self, cutoff_date):
        """ See barbante.data.BaseProxy.
        """
        code = 'function() {' \
               'db.activities_summary.find(' \
               '{day: {$gte: ISODate("' + \
               cutoff_date.isoformat() + \
               '")}}).forEach(function(doc) {' \
               'doc.uu_latest_type = doc.activity;' \
               'doc.uu_latest_date = doc.created_at;' \
               'db.activities_summary.save(doc);});}'

        self.database.command("eval", code, nolock=True)  # executes on MongoDB server

    @profile
    def save_product_product_numerators(self, strength_numerators, increment=False, upsert=True):
        """ See barbante.data.BaseProxy.
        """
        if len(strength_numerators) == 0:
            return

        collection = self.database_bulk.pp_strengths

        if upsert:
            operator = "$inc" if increment else "$set"

            bulk_op = collection.initialize_unordered_bulk_op()
            has_operations = False

            for product_and_template, strength_numerator_tuple in strength_numerators.items():
                has_operations = True
                product = product_and_template[0]
                template = product_and_template[1]
                spec = {"product": product, "template_product": template}
                update_clause = {operator: {"nc": strength_numerator_tuple[0],
                                            "na": strength_numerator_tuple[1]}}
                bulk_op.find(spec).upsert().update(update_clause)

            if has_operations:
                bulk_op.execute(write_concern={'w': self.write_concern_level})
        else:
            strength_numerators_list = [{"product": p, "template_product": t, "nc": n[0], "na": n[1]}
                                        for (p, t), n in strength_numerators.items()]
            collection.insert(
                strength_numerators_list, w=self.write_concern_level, manipulate=False)

    def save_pp_strengths(self, strength_docs_map, upsert=False, deferred_publication=False):
        """ See barbante.data.BaseProxy.
        """
        if len(strength_docs_map) == 0:
            return

        if deferred_publication:
            collection = self.database_bulk.pp_strengths_temp
            write_concern = 1  # forces write completion due to hotswap that follows
        else:
            collection = self.database_bulk.pp_strengths
            write_concern = self.write_concern_level

        if upsert:
            bulk_op = collection.initialize_unordered_bulk_op()
            has_operations = False

            for product_and_template, strength_doc in strength_docs_map.items():
                has_operations = True
                product = product_and_template[0]
                template = product_and_template[1]
                spec = {"product": product, "template_product": template}
                update_clause = {"$set": strength_doc}
                bulk_op.find(spec).upsert().update(update_clause)

            if has_operations:
                bulk_op.execute(write_concern={'w': write_concern})
        else:
            collection.insert(strength_docs_map.values(), w=write_concern, manipulate=False)

    @profile
    def save_latest_activity_for_product_product_strengths(self, user, product, activity_type, activity_date):
        """ See barbante.data.BaseProxy.
        """
        spec = {"external_user_id": user,
                "external_product_id": product}
        update_clause = {"$set": {"pp_latest_type": activity_type,
                                  "pp_latest_date": activity_date}}

        self.database.activities_summary.update(spec,
                                                update_clause,
                                                upsert=True,
                                                w=self.write_concern_level)

    @profile
    def copy_all_latest_activities_for_product_product_strengths(self, cutoff_date):
        """ See barbante.data.BaseProxy.
        """
        code = 'function() {' \
               'db.activities_summary.find(' \
               '{day: {$gte: ISODate("' + \
               cutoff_date.isoformat() + \
               '")}}).forEach(function(doc) {' \
               'doc.pp_latest_type = doc.activity;' \
               'doc.pp_latest_date = doc.created_at;' \
               'db.activities_summary.save(doc);});}'

        self.database.command("eval", code, nolock=True)  # executes on MongoDB server

    @profile
    def save_product_product_strengths_tfidf(self, strengths, start_index=None, end_index=None,
                                             deferred_publication=False):
        """ See barbante.data.BaseProxy.
        """
        if len(strengths) == 0:
            return

        if deferred_publication:
            collection = self.database_bulk.product_product_strengths_tfidf_temp
        else:
            collection = self.database_bulk.product_product_strengths_tfidf

        if start_index is None:
            start_index = 0
        if end_index is None:
            end_index = len(strengths)

        bulk_op = collection.initialize_unordered_bulk_op()
        has_operations = False

        for index in range(start_index, end_index):
            has_operations = True

            strength_doc = strengths[index]
            update_clause = {"$set": {"strength": strength_doc["strength"]}}
            bulk_op.find({"product": strength_doc["product"],
                          "template_product": strength_doc[
                              "template_product"]}).upsert().update(update_clause)

        if has_operations:
            bulk_op.execute(write_concern={'w': self.write_concern_level})

    def hotswap_uu_strengths(self):
        """ See barbante.data.BaseProxy.
        """
        if "uu_strengths_temp" in self.database_bulk.collection_names():
            self.database_bulk.uu_strengths_temp.rename("uu_strengths", dropTarget=True)

    def hotswap_pp_strengths(self):
        """ See barbante.data.BaseProxy.
        """
        if "pp_strengths_temp" in self.database_bulk.collection_names():
            self.database_bulk.pp_strengths_temp.rename("pp_strengths", dropTarget=True)

    def hotswap_product_product_strengths_tfidf(self):
        """ See barbante.data.BaseProxy.
        """
        if "product_product_strengths_tfidf_temp" in self.database_bulk.collection_names():
            self.database_bulk.product_product_strengths_tfidf_temp.rename("product_product_strengths_tfidf",
                                                                           dropTarget=True)

    def hotswap_product_models(self):
        """ See barbante.data.BaseProxy.
        """
        if "product_models_temp" in self.database.collection_names():
            self.database_bulk.product_models_temp.rename("product_models",
                                                          dropTarget=True)

    @profile
    def save_df(self, language, df_by_term, increment=False, upsert=False):
        """ See barbante.data.BaseProxy.
        """
        bulk_op = self.database_bulk.df.initialize_unordered_bulk_op()
        has_operations = False

        for term, df in df_by_term.items():
            if df <= 0:
                continue

            has_operations = True

            record = {"language": language,
                      "term": term,
                      "df": df}
            if upsert:
                if increment:
                    update_clause = {"$set": {"language": language,
                                              "term": term},
                                     "$inc": {"df": df}}
                else:
                    update_clause = {"$set": record}

                bulk_op.find({"language": language,
                              "term": term}).upsert().update(update_clause)
            else:
                bulk_op.insert(record)

        if has_operations:
            bulk_op.execute(write_concern={'w': self.write_concern_level})

    def find_df(self, language, term):
        """ See barbante.data.BaseProxy.
        """
        fields = {"df": True, "_id": False}
        cursor = self.database.df.find({"language": language, "term": term}, fields)
        for doc in cursor:
            return doc["df"]
        return 0

    def fetch_df_map(self, language, terms):
        """ See barbante.data.BaseProxy.
        """
        fields = {"term": True, "df": True, "_id": False}
        where = {"language": language, "term": {"$in": terms}}
        cursor = self.database.df.find(where, fields)
        result = {row["term"]: row["df"] for row in cursor}

        return result

    def fetch_tf_map(self, attribute, product_ids):
        """ See barbante.data.BaseProxy.
        """
        result = {}

        fields = {"external_product_id": True, "term": True, "count": True, "_id": False}
        where = {"attribute": attribute,
                 "external_product_id": {"$in": product_ids}}

        collection = self.database.product_terms
        cursor = collection.find(where, fields)
        for row in cursor:
            external_product_id = row["external_product_id"]
            term = row["term"]
            count = row["count"]
            tf_by_term = result.get(external_product_id, {})
            tf_by_term[term] = count
            result[external_product_id] = tf_by_term

        return result

    def fetch_tfidf_map(self, attribute, product_ids):
        """ See barbante.data.BaseProxy.
        """
        result = {}

        fields = {"external_product_id": True, "term": True, "tfidf": True, "_id": False}
        where = {"attribute": attribute,
                 "external_product_id": {"$in": product_ids}}

        collection = self.database.tfidf
        cursor = collection.find(where, fields)

        for row in cursor:
            external_product_id = row["external_product_id"]
            term = row["term"]
            tfidf = row["tfidf"]
            tfidf_by_term = result.get(external_product_id, {})
            tfidf_by_term[term] = tfidf
            result[external_product_id] = tfidf_by_term

        return result

    def save_user_model(self, user_id, user_model):
        """ See barbante.data.BaseProxy.
        """
        raise NotImplementedError()

    def save_product_model(self, product_id, product_model, deferred_publication=False):
        """ See barbante.data.BaseProxy.
        """
        if deferred_publication:
            collection = self.database.product_models_temp
            write_concern = 1  # forces write completion due to hotswap that follows
        else:
            collection = self.database.product_models
            write_concern = self.write_concern_level

        product_model_as_dict = product_model.to_dict()

        if "external_product_id" not in product_model_as_dict:
            product_model_as_dict["external_product_id"] = product_id

        collection.update(
            {"external_product_id": product_id}, {'$set': product_model_as_dict},
            upsert=True, w=write_concern)

    def delete_product(self, product_id, date):
        """ See barbante.data.BaseProxy.
        """
        self.database_raw.products.update({"external_id": product_id}, {"$set": {"deleted_on": date}})

    def delete_product_model(self, product_id):
        """ See barbante.data.BaseProxy.
        """
        self.database.product_models.remove({"external_product_id": product_id})

    def update_product(self, product_id, field, new_value):
        """ See barbante.data.BaseProxy.
        """
        self.database_raw.products.update({"external_id": product_id}, {"$set": {field: new_value}})

    def remove_product_terms(self, attributes, product_id):
        """ See barbante.data.BaseProxy.
        """
        self.database.product_terms.remove({"attribute": {"$in": attributes}, "external_product_id": product_id})

    def remove_tfidf(self, attributes, product_id):
        """ See barbante.data.BaseProxy.
        """
        self.database.tfidf.remove({"attribute": {"$in": attributes}, "external_product_id": product_id})

    def insert_user(self, user):
        """ See barbante.data.BaseProxy.
        """
        if "anonymous" not in user:
            user["anonymous"] = config.is_anonymous(user["external_id"])
        self.database_raw.users.insert(user, w=self.write_concern_level)

    def insert_product(self, product):
        """ See barbante.data.BaseProxy.
        """
        self.database_raw.products.insert(product, w=self.write_concern_level)

    def insert_product_models(self, records, deferred_publication=False):
        """ See barbante.data.BaseProxy.
        """
        if deferred_publication:
            self._insert_bulk(records, 'product_models_temp')
        else:
            self._insert_bulk(records, 'product_models')

    def insert_product_terms(self, records):
        """ See barbante.data.BaseProxy.
        """
        self._insert_bulk(records, 'product_terms')

    def insert_tfidf_records(self, records):
        """ See barbante.data.BaseProxy.
        """
        self._insert_bulk(records, 'tfidf')

    def reset_impression_summary(self, user_id, product_id, anonymous):
        """ See barbante.data.BaseProxy.
        """
        collection = self.database.anonymous_impressions_summary if anonymous else self.database.impressions_summary
        spec = {"u_id": user_id, "p_id": product_id}
        update_clause = {"$set": {"count": 0}}
        collection.update(spec,
                          update_clause,
                          upsert=False,  # we do not want to insert in case it does not exist
                          w=self.write_concern_level)

    def increment_impression_summary(self, user_id, product_id, date, anonymous):
        """ See barbante.data.BaseProxy.
        """
        collection = self.database.anonymous_impressions_summary if anonymous else self.database.impressions_summary
        spec = {"u_id": user_id, "p_id": product_id}
        update_clause = {"$inc": {"count": 1}, "$setOnInsert": {"first": date}}
        collection.update(spec,
                          update_clause,
                          upsert=True,
                          w=self.write_concern_level)

    def update_product_popularity(self, product_id, date, do_increment=True):
        """ See barbante.data.BaseProxy.
        """
        must_update = False
        first_day = None
        latest_day = None
        new_popularity = None

        spec = {"p_id": product_id}
        fields = {"count": True, "first": True, "latest": True, "_id": False}

        cursor = self.database.popularities_summary.find(spec, fields).limit(1)
        for doc in cursor:
            current_count = doc["count"]
            current_first = doc["first"]
            current_latest = doc["latest"]

            first = min(current_first, date)
            latest = max(current_latest, date)
            new_count = current_count + 1 if do_increment else current_count

            if first != current_first or latest != current_latest or new_count != current_count:
                first_day = du.get_day(first)
                latest_day = du.get_day(latest)
                day_span = (latest_day - first_day).days + 1
                new_popularity = new_count / day_span
                must_update = True

            break
        else:
            first_day = du.get_day(date)
            latest_day = du.get_day(date)
            new_count = 1
            new_popularity = 1
            must_update = True

        if must_update:
            update_clause = {"$set": {"first": first_day,
                                      "latest": latest_day,
                                      "count": new_count,
                                      "popularity": new_popularity}}

            self.database.popularities_summary.update(spec,
                                                      update_clause,
                                                      upsert=True,
                                                      w=self.write_concern_level)

    def save_activity_summary(self, activity, anonymous, set_popularity_flag=False):
        """ See barbante.data.BaseProxy.
        """
        collection = self.database.anonymous_activities_summary if anonymous else self.database.activities_summary

        spec = {"external_user_id": activity["external_user_id"],
                "external_product_id": activity["external_product_id"]}
        activity_date = activity["created_at"]
        day = du.get_day(activity_date)
        update_clause = {"$set": {"activity": activity["activity"],
                                  "day": day,
                                  "created_at": activity_date}}
        if set_popularity_flag:
            update_clause["$set"]["contributed_for_popularity"] = True

        collection.update(spec, update_clause, upsert=True, w=self.write_concern_level)

    def _insert_bulk(self, records, collection_name):
        bulk_op = self.database_bulk[collection_name].initialize_unordered_bulk_op()
        has_operations = len(records) > 0

        for record in records:
            bulk_op.insert(record)

        if has_operations:
            bulk_op.execute(write_concern={'w': self.write_concern_level})

    def get_user_count(self):
        """ See barbante.data.BaseProxy.
        """
        return self.database.users.find({"anonymous": False}).count()

    def get_product_count(self):
        """ See barbante.data.BaseProxy.
        """
        return self.database_raw.products.count()

    def get_product_model_count(self):
        """ See barbante.data.BaseProxy.
        """
        return self.database.product_models.count()

    def sample_users(self, count):
        """ See barbante.data.BaseProxy.
        """
        result = set()
        total_users = self.get_user_count()

        if total_users == 0:
            return result

        prob = count / total_users

        fields = {"external_id": True, "_id": False}
        cursor = self.database.users.find({}, fields)
        while len(result) < count:
            for rec in cursor:
                if random() < prob:
                    result.add(rec["external_id"])
                    if len(result) == count:
                        break
            cursor.rewind()

        return result

    def copy_database(self, fromdb, todb):
        """ See barbante.data.BaseProxy.
        """
        self.database.connection.admin.command('copydb', fromdb=fromdb, todb=todb)

    def drop_database(self, dbname=None):
        """ See barbante.data.BaseProxy.
        """
        self.database.connection.drop_database(dbname or self.context.database_settings.name)

    def backup_database(self):
        """ See barbante.data.BaseProxy.
        """
        self.database_backup_name = '{0}-backup'.format(self.database_name)

        self.drop_database(self.database_backup_name)
        self.copy_database(fromdb=self.database_name, todb=self.database_backup_name)
        self.drop_database()

    def restore_database(self):
        """ See barbante.data.BaseProxy.
        """
        self.drop_database()
        self.copy_database(fromdb=self.database_backup_name, todb=self.database_name)

    def reset_all_product_content_data(self):
        """ See barbante.data.BaseProxy.
        """
        self.database.drop_collection(self.database.product_models_temp)
        self.database.drop_collection(self.database.product_terms)
        self.database.drop_collection(self.database.df)
        self.database.drop_collection(self.database.tfidf)

        self.database.product_models_temp.ensure_index("external_product_id")

        self._ensure_indexes_product_terms()

    def reset_user_user_strength_auxiliary_data(self):
        """ See barbante.data.BaseProxy.
        """
        self.database.drop_collection(self.database.uu_strengths)
        self.database.drop_collection(self.database.uu_strengths_temp)

        self._ensure_indexes_uu_strengths(temporary=True)

        # This index will be dropped in the hotswap, but it will temporarily aid uu strength generations from scratch
        self.database.uu_strengths.ensure_index([("user", pymongo.ASCENDING),
                                                 ("template_user", pymongo.ASCENDING),
                                                 ("nc", pymongo.ASCENDING),
                                                 ("na", pymongo.ASCENDING)])

    def reset_product_product_strength_auxiliary_data(self):
        """ See barbante.data.BaseProxy.
        """
        self.database.drop_collection(self.database.pp_strengths)
        self.database.drop_collection(self.database.pp_strengths_temp)

        self._ensure_indexes_pp_strengths(temporary=True)

        # This index will be dropped in the hotswap, but it will temporarily aid pp strength generations from scratch
        self.database.pp_strengths.ensure_index([("template_product", pymongo.ASCENDING),
                                                 ("product", pymongo.ASCENDING),
                                                 ("nc", pymongo.ASCENDING),
                                                 ("na", pymongo.ASCENDING)])

    def reset_product_product_strength_tfidf_auxiliary_data(self):
        """ See barbante.data.BaseProxy.
        """
        self.database.drop_collection(self.database.product_product_strengths_tfidf_temp)

        self._ensure_indexes_product_product_strengths_tfidf(temporary=True)

    def fetch_latest_batch_info_product_models(self):
        """ See barbante.data.BaseProxy.
        """
        return self._fetch_latest_batch_info("process_all_products")

    def fetch_latest_batch_info_user_user_strengths(self):
        """ See barbante.data.BaseProxy.
        """
        return self._fetch_latest_batch_info("generate_user_user_strengths")

    def fetch_latest_batch_info_product_product_strengths(self):
        """ See barbante.data.BaseProxy.
        """
        return self._fetch_latest_batch_info("generate_product_product_strengths")

    def fetch_latest_batch_info_user_template_consolidation(self):
        """ See barbante.data.BaseProxy.
        """
        return self._fetch_latest_batch_info("user_template_consolidation")

    def fetch_latest_batch_info_product_template_consolidation(self):
        """ See barbante.data.BaseProxy.
        """
        return self._fetch_latest_batch_info("product_template_consolidation")

    def _fetch_latest_batch_info(self, task):
        where = {"type": task}
        sort_order = [("type", pymongo.ASCENDING),
                      ("timestamp", pymongo.DESCENDING)]
        cursor = self.database.maintenance.find(where).sort(sort_order).limit(1)
        for doc in cursor:
            return doc
        return None

    def save_timestamp_user_user_strengths(self, timestamp, cutoff_date, elapsed_time):
        """ See barbante.data.BaseProxy.
        """
        self._save_timestamp("generate_user_user_strengths", timestamp, cutoff_date, elapsed_time)

    def save_timestamp_product_product_strengths(self, timestamp, cutoff_date, elapsed_time):
        """ See barbante.data.BaseProxy.
        """
        self._save_timestamp("generate_product_product_strengths", timestamp, cutoff_date, elapsed_time)

    def save_timestamp_product_product_strengths_tfidf(self, timestamp, cutoff_date, elapsed_time):
        """ See barbante.data.BaseProxy.
        """
        self._save_timestamp("generate_product_product_strengths_tfidf", timestamp, cutoff_date, elapsed_time)

    def save_timestamp_product_models(self, timestamp, cutoff_date, elapsed_time):
        """ See barbante.data.BaseProxy.
        """
        self._save_timestamp("process_all_products", timestamp, cutoff_date, elapsed_time)

    def save_timestamp_user_template_consolidation(self, timestamp, status, elapsed_time=None):
        """ See barbante.data.BaseProxy.
        """
        self._save_timestamp("user_template_consolidation", timestamp, status=status,
                             elapsed_time=elapsed_time, upsert=True)

    def save_timestamp_product_template_consolidation(self, timestamp, status, elapsed_time=None):
        """ See barbante.data.BaseProxy.
        """
        self._save_timestamp("product_template_consolidation", timestamp, status=status,
                             elapsed_time=elapsed_time, upsert=True)

    def _save_timestamp(self, task, timestamp, cutoff_date=None, elapsed_time=None, status="completed", upsert=False):
        spec = {"type": task,
                "timestamp": timestamp}
        update_clause = {"$set": {"status": status}}
        if cutoff_date:
            update_clause["$set"]["cutoff_date"] = cutoff_date
        if elapsed_time:
            update_clause["$set"]["elapsed_time_sec"] = elapsed_time

        if upsert:
            self.database.maintenance.update(spec,
                                             update_clause,
                                             upsert=True,
                                             w=self.write_concern_level)
        else:
            insert_doc = spec
            insert_doc.update(update_clause["$set"])
            self.database.maintenance.insert(insert_doc, w=self.write_concern_level, manipulate=False)

    def ensure_indexes(self, create_ttl_indexes=True):
        """ Create indexes for barbante use cases.
        """
        # Activities

        log.info("Ensuring indexes for activities...")

        for activities_collection in [self.database.activities_summary, self.database.anonymous_activities_summary]:
            activities_collection.ensure_index([("external_user_id", pymongo.ASCENDING),
                                                ("external_product_id", pymongo.ASCENDING),
                                                ("day", pymongo.DESCENDING),
                                                ("activity", pymongo.ASCENDING)])

            activities_collection.ensure_index([("external_user_id", pymongo.ASCENDING),
                                                ("day", pymongo.DESCENDING),
                                                ("activity", pymongo.ASCENDING),
                                                ("external_product_id", pymongo.ASCENDING)])

            activities_collection.ensure_index([("external_product_id", pymongo.ASCENDING),
                                                ("day", pymongo.DESCENDING),
                                                ("activity", pymongo.ASCENDING),
                                                ("external_user_id", pymongo.ASCENDING)])

        if create_ttl_indexes:
            self.database.anonymous_activities_summary.ensure_index(
                [("day", pymongo.ASCENDING)], expireAfterSeconds=(60*60*24*30))  # expires in 1 month

        # Impressions

        log.info("Ensuring indexes for impressions...")
        for impressions_collection in [self.database.impressions_summary, self.database.anonymous_impressions_summary]:
            impressions_collection.ensure_index([("u_id", pymongo.ASCENDING),
                                                 ("p_id", pymongo.ASCENDING),
                                                 ("count", pymongo.ASCENDING),
                                                 ("first", pymongo.ASCENDING)])

            impressions_collection.ensure_index([("p_id", pymongo.ASCENDING),
                                                 ("u_id", pymongo.ASCENDING),
                                                 ("count", pymongo.ASCENDING),
                                                 ("first", pymongo.ASCENDING)])

        if create_ttl_indexes:
            self.database.anonymous_impressions_summary.ensure_index(
                [("latest", pymongo.ASCENDING)], expireAfterSeconds=(60*60*24*30))  # expires in 1 month

        # Product popularity
        log.info("Ensuring indexes for product popularity...")
        self.database.popularities_summary.ensure_index([("latest", pymongo.DESCENDING),
                                                         ("p_id", pymongo.ASCENDING),
                                                         ("popularity", pymongo.DESCENDING)])

        # Maintenance
        log.info("Ensuring indexes for maintenance...")
        self.database.maintenance.ensure_index([("type", pymongo.ASCENDING),
                                                ("timestamp", pymongo.DESCENDING),
                                                ("cutoff_date", pymongo.DESCENDING)])

        # Raw products
        log.info("Ensuring indexes for products (raw database)...")
        self.database_raw.products.ensure_index([("external_id", pymongo.ASCENDING),
                                                 ("deleted_on", pymongo.DESCENDING)])

        self.database_raw.products.ensure_index("deleted_on")

        self.database_raw.products.ensure_index([(self.default_product_date_field, pymongo.ASCENDING),
                                                 ("deleted_on", pymongo.DESCENDING)])

        # Product models and terms
        log.info("Ensuring indexes for product models and terms...")
        self.database.product_models_temp.ensure_index("external_product_id")
        self._ensure_indexes_product_terms()

        # Collaborative filtering strengths
        log.info("Ensuring indexes for collaborative filtering (user-user)...")
        self._ensure_indexes_uu_strengths(temporary=False)
        log.info("Ensuring indexes for collaborative filtering (product-product)...")
        self._ensure_indexes_pp_strengths(temporary=False)

        # Content-based strengths
        log.info("Ensuring indexes for content-based strengths (aka 'tfidf')...")
        self._ensure_indexes_product_product_strengths_tfidf(temporary=False)

        # User and product persisted caches
        self.ensure_indexes_cache()

    def ensure_indexes_cache(self):
        """ Create specific indexes for user and product persisted caches.
        """
        log.info("Ensuring indexes for users cache...")
        self.database.user_cache.ensure_index("external_user_id")
        log.info("Ensuring indexes for products cache...")
        self.database.product_cache.ensure_index("external_product_id")

    def _ensure_indexes_uu_strengths(self, temporary=False):
        uu_collection = self.database.uu_strengths_temp if temporary else self.database.uu_strengths

        uu_collection.ensure_index([("user", pymongo.ASCENDING),
                                    ("template_user", pymongo.ASCENDING)])

        uu_collection.ensure_index([("user", pymongo.ASCENDING),
                                    ("strength", pymongo.DESCENDING),
                                    ("template_user", pymongo.ASCENDING)])

        uu_collection.ensure_index([("template_user", pymongo.ASCENDING),
                                    ("user", pymongo.ASCENDING)])

    def _ensure_indexes_pp_strengths(self, temporary=False):
        pp_collection = self.database.pp_strengths_temp if temporary else self.database.pp_strengths

        pp_collection.ensure_index([("template_product", pymongo.ASCENDING),
                                    ("product", pymongo.ASCENDING)])

        pp_collection.ensure_index([("product", pymongo.ASCENDING),
                                    ("strength", pymongo.DESCENDING),
                                    ("template_product", pymongo.ASCENDING)])

        pp_collection.ensure_index([("product", pymongo.ASCENDING),
                                    ("template_product", pymongo.ASCENDING)])

    def _ensure_indexes_product_product_strengths_tfidf(self, temporary=False):
        collection = self.database.product_product_strengths_tfidf_temp if temporary \
            else self.database.product_product_strengths_tfidf

        collection.ensure_index([("product", pymongo.ASCENDING),
                                 ("template_product", pymongo.ASCENDING),
                                 ("strength", pymongo.DESCENDING)])

        collection.ensure_index([("product", pymongo.ASCENDING),
                                 ("strength", pymongo.DESCENDING),
                                 ("template_product", pymongo.ASCENDING)])

    def _ensure_indexes_product_terms(self):
        self.database.product_terms.ensure_index([("attribute", pymongo.ASCENDING),
                                                  ("external_product_id", pymongo.ASCENDING),
                                                  ("term", pymongo.ASCENDING),
                                                  ("count", pymongo.ASCENDING)])

        self.database.product_terms.ensure_index([("attribute", pymongo.ASCENDING),
                                                  ("term", pymongo.ASCENDING),
                                                  ("external_product_id", pymongo.ASCENDING),
                                                  ("count", pymongo.ASCENDING)])

        self.database.df.ensure_index([("language", pymongo.ASCENDING),
                                       ("term", pymongo.ASCENDING),
                                       ("df", pymongo.DESCENDING)])

        self.database.tfidf.ensure_index([("attribute", pymongo.ASCENDING),
                                          ("external_product_id", pymongo.ASCENDING),
                                          ("term", pymongo.ASCENDING),
                                          ("tfidf", pymongo.DESCENDING)])

        self.database.tfidf.ensure_index([("attribute", pymongo.ASCENDING),
                                          ("term", pymongo.ASCENDING),
                                          ("external_product_id", pymongo.ASCENDING),
                                          ("tfidf", pymongo.DESCENDING)])

    @staticmethod
    def _build_date_clause(attribute_name, min_date=None, max_date=None):
        date_clause = {}
        if min_date is not None:
            date_clause[attribute_name] = {'$gte': min_date}
        if max_date is not None:
            date_clause_value = date_clause.get(attribute_name, {})
            date_clause_value.update({'$lte': max_date})
            date_clause[attribute_name] = date_clause_value
        return date_clause