""" Base class for recommendation tests.
"""
import datetime as dt
import concurrent.futures

import nose.tools

import barbante.config as config
import barbante.context as ctx
import barbante.maintenance.tasks as tasks
import barbante.maintenance.user_templates as ut
import barbante.maintenance.product_templates as pt
import barbante.maintenance.product_templates_tfidf as pttfidf
import barbante.tests as tests
import barbante.tests.dummy_data_populator as dp
from barbante.context.context_manager import wrap


class RecommenderFixture():
    """ Class for testing barbante.recommendation.Recommender.
    """
    session_context = None
    """ The test session context. """
    db_proxy = None
    """ A reference to the database proxy. """

    def __init__(self):
        self.n_recommendations = 50
        """ The number of recommendations which will be requested in each test.
        """
        self.algorithm = None
        """ The class of the recommender that will be used throughout the tests.
        """

    @classmethod
    def setup_class(cls):
        cls.session_context = tests.init_session()
        cls.db_proxy = cls.session_context.data_proxy

        cls.db_proxy.drop_database()
        cls.db_proxy.ensure_indexes(create_ttl_indexes=False)

        cls.session_context.history_decay_function_name = None
        cls.session_context.product_age_decay_function_name = None

        # Populate the database
        dp.populate_products(cls.session_context)
        tasks.process_products(cls.session_context)
        dp.populate_users(cls.session_context)
        dp.populate_activities(cls.session_context)
        dp.populate_impressions(cls.session_context)

        # Make a backup of the database so that after each test we can send it back to its original state
        cls.db_proxy.backup_database()

    def setup(self):
        # Restore the database from the backup copy before running another test
        self.db_proxy.restore_database()

    def teardown(self):
        # Drop the dirty copy of the database after the test is executed
        self.db_proxy.drop_database()

    def set_algorithm(self, algorithm):
        self.algorithm = algorithm

    def test_recommend_non_existing_user(self):
        """ Tests whether an empty set is returned when an invalid user id is
            passed as parameter to the Recommender constructor.
        """
        session = tests.init_session(user_id="Invalid user id", algorithm=self.algorithm)
        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)
        nose.tools.eq_(len(results), 0, "No recommendations should have been returned.")

    def test_recommend_with_blocking_activities(self):
        """ Tests whether blocking activities prevent items from being recommended.
        """
        # Economia
        for i in range(1, dp.N_USR_ECONOMIA + 1):
            target = "u_eco_" + str(i)

            session = tests.init_session(user_id=target, algorithm=self.algorithm)
            recommender = session.get_recommender()
            recommendations = recommender.recommend(self.n_recommendations)

            nose.tools.ok_(len(recommendations) > 0, "Empty recommendation.")
            if len(recommendations) > 0:
                top_product = recommendations[0][1]
            else:
                return

            supported_activities = self.session_context.supported_activities
            blocking_activities = self.session_context.blocking_activities
            non_blocking_activities = list(set(supported_activities) - set(blocking_activities))

            # Meta-tests
            nose.tools.ok_(len(non_blocking_activities) > 0,
                           "Weak test. There should be at least one non_blocking activity.")
            nose.tools.ok_(len(blocking_activities) > 0,
                           "Weak test. There should be at least one blocking activity.")

            # Saves a non-blocking activity first
            activity = {"external_user_id": target,
                        "external_product_id": top_product,
                        "activity": non_blocking_activities[0],
                        "created_at": self.session_context.get_present_date()}
            tasks.update_summaries(self.session_context, activity)

            session.refresh()
            recommender = session.get_recommender()
            recommendations = recommender.recommend(self.n_recommendations)
            recommended_products = [r[1] for r in recommendations]
            nose.tools.ok_(top_product in recommended_products,
                           "A non-blocking activity should not prevent a product from being recommended")

            # Saves a blocking activity first
            activity = {"external_user_id": target,
                        "external_product_id": top_product,
                        "activity": blocking_activities[0],
                        "created_at": self.session_context.get_present_date()}
            tasks.update_summaries(self.session_context, activity)

            session.refresh()
            recommender = session.get_recommender()
            recommendations = recommender.recommend(self.n_recommendations)
            recommended_products = [r[1] for r in recommendations]
            if self.session_context.filter_strategy == ctx.AFTER_SCORING:
                nose.tools.ok_(top_product not in recommended_products,
                               "A blocking activity should prevent a product from being recommended")

    def test_history_decay_step(self):
        """ Tests the effect of applying a history decay factor based on a step function
            on recommendations. It applies to all recommendation heuristics.
        """
        target = "u_eco_2"

        history_decay = {'history_decay_function_name': 'step', 'history_decay_step_function_ttl': 3}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()
        recommendations = recommender.recommend(self.n_recommendations)

        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        former_top_product = recommendations[0][1]

        impressions_count = self.db_proxy.fetch_impressions_summary(
            user_ids=[target],
            product_ids=[former_top_product],
            group_by_product=False,
            anonymous=False).get(target, {}).get(former_top_product, (0, None))[0]

        for i in range(3 - impressions_count):
            self.db_proxy.increment_impression_summary(user_id=target,
                                                       product_id=former_top_product,
                                                       date=self.session_context.get_present_date(),
                                                       anonymous=False)

        session.refresh()
        recommendations = recommender.recommend(self.n_recommendations)
        nose.tools.ok_(former_top_product not in [rec[1] for rec in recommendations],
                       "Incorrect application of the step history decay")

    def test_history_decay_rational(self):
        """ Tests the effect of applying a history decay factor based on a rational function
            on recommendations. It applies to all recommendation heuristics.
        """
        target = "u_eco_2"
        history_decay = {'history_decay_function_name': None}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()

        # Determines the index of the first actual value in the score tuples
        # produced by the recommender (note that hybrid recommenders use the first
        # position to indicate the algorithm number)
        if recommender.is_hybrid():
            start_index = 1
        else:
            start_index = 0

        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        former_top_product = recommendations[0][1]
        old_strength = recommendations[0][0]

        impressions_count = self.db_proxy.fetch_impressions_summary(
            user_ids=[target],
            product_ids=[former_top_product],
            group_by_product=False,
            anonymous=False).get(target, {}).get(former_top_product, (0, None))[0]

        for i in range(2 - impressions_count):
            self.db_proxy.increment_impression_summary(user_id=target,
                                                       product_id=former_top_product,
                                                       date=self.session_context.get_present_date(),
                                                       anonymous=False)

        history_decay = {'history_decay_function_name': 'rational'}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()
        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        new_strength = None
        for rec in recommendations:
            if rec[1] == former_top_product:
                new_strength = rec[0]
                break

        nose.tools.ok_(new_strength is not None,
                       "The former top recommendation should have been recommended again.")
        for i in range(start_index, len(new_strength)):
            old_strength_value = old_strength[i]
            new_strength_value = new_strength[i]
            nose.tools.ok_(abs(new_strength_value / old_strength_value - 1 / 3) < tests.FLOAT_DELTA,
                           "Incorrect application of the rational function decay")

        self.db_proxy.increment_impression_summary(user_id=target,
                                                   product_id=former_top_product,
                                                   date=self.session_context.get_present_date(),
                                                   anonymous=False)

        session.refresh()
        recommendations = recommender.recommend(100)

        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        new_strength = None
        for rec in recommendations:
            if rec[1] == former_top_product:
                new_strength = rec[0]
                break

        nose.tools.ok_(new_strength is not None,
                       "The former top recommendation should have been recommended again.")
        for i in range(start_index, len(new_strength)):
            old_strength_value = old_strength[i]
            new_strength_value = new_strength[i]
            nose.tools.ok_(abs(new_strength_value / old_strength_value - 1 / 4) < tests.FLOAT_DELTA,
                           "Incorrect application of the rational history decay")

    def test_history_decay_exponential(self):
        """ Tests the effect of applying a history decay factor based on an exponential function
            on recommendations. It applies to all recommendation heuristics.
        """
        target = "u_eco_2"

        history_decay = {'history_decay_function_name': None}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()

        # Determines the index of the first actual value in the score tuples
        # produced by the recommender (note that hybrid recommenders use the first
        # position to indicate the algorithm number)
        if recommender.is_hybrid():
            start_index = 1
        else:
            start_index = 0

        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        former_top_product = recommendations[0][1]
        old_strength = recommendations[0][0]

        impressions_count = self.db_proxy.fetch_impressions_summary(
            user_ids=[target],
            product_ids=[former_top_product],
            group_by_product=False,
            anonymous=False).get(target, {}).get(former_top_product, (0, None))[0]

        for i in range(3 - impressions_count):
            self.db_proxy.increment_impression_summary(user_id=target,
                                                       product_id=former_top_product,
                                                       date=self.session_context.get_present_date(),
                                                       anonymous=False)

        history_decay = {'history_decay_function_name': 'exponential', 'history_decay_exponential_function_halflife': 3}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()
        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        new_strength = None
        for rec in recommendations:
            if rec[1] == former_top_product:
                new_strength = rec[0]
                break

        nose.tools.ok_(new_strength is not None,
                       "The former top recommendation should have been recommended again.")
        for i in range(start_index, len(new_strength)):
            old_strength_value = old_strength[i]
            new_strength_value = new_strength[i]
            nose.tools.ok_(abs(new_strength_value / old_strength_value - 0.5) < tests.FLOAT_DELTA,
                           "Incorrect application of the exponential history decay")

    def test_history_decay_linear(self):
        """ Tests the effect of applying a history decay factor based on a linear function
            on recommendations. It applies to all recommendation heuristics.
        """
        target = "u_eco_2"
        history_decay = {'history_decay_function_name': None}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()

        # Determines the index of the first actual value in the score tuples
        # produced by the recommender (note that hybrid recommenders use the first
        # position to indicate the algorithm number)
        if recommender.is_hybrid():
            start_index = 1
        else:
            start_index = 0

        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        former_top_product = recommendations[0][1]
        old_strength = recommendations[0][0]

        impressions_count = self.db_proxy.fetch_impressions_summary(
            user_ids=[target],
            product_ids=[former_top_product],
            group_by_product=False,
            anonymous=False).get(target, {}).get(former_top_product, (0, None))[0]

        for i in range(1 - impressions_count):
            self.db_proxy.increment_impression_summary(user_id=target,
                                                       product_id=former_top_product,
                                                       date=self.session_context.get_present_date(),
                                                       anonymous=False)

        history_decay = {'history_decay_function_name': 'linear', 'history_decay_linear_function_ttl': 3}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()
        recommendations = recommender.recommend(100)

        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        new_strength = None
        for rec in recommendations:
            if rec[1] == former_top_product:
                new_strength = rec[0]
                break

        nose.tools.ok_(new_strength is not None,
                       "The former top recommendation should have been recommended again.")

        for i in range(start_index, len(new_strength)):
            old_strength_value = old_strength[i]
            new_strength_value = new_strength[i]
            nose.tools.ok_(abs(new_strength_value / old_strength_value - 2 / 3) < tests.FLOAT_DELTA,
                           "Incorrect application of the linear history decay")

        self.db_proxy.increment_impression_summary(user_id=target,
                                                   product_id=former_top_product,
                                                   date=self.session_context.get_present_date(),
                                                   anonymous=False)

        session.refresh()
        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        new_strength = None
        for rec in recommendations:
            if rec[1] == former_top_product:
                new_strength = rec[0]
                break

        nose.tools.ok_(new_strength is not None,
                       "The former top recommendation should have been recommended again.")
        for i in range(start_index, len(new_strength)):
            old_strength_value = old_strength[i]
            new_strength_value = new_strength[i]
            nose.tools.ok_(abs(new_strength_value / old_strength_value - 1 / 3) < tests.FLOAT_DELTA,
                           "Incorrect application of the linear history decay")

    def test_in_boost(self):
        """ Tests the effect of applying an in-boost on recommendations for some activity types.
            It applies to all recommendation heuristics.
        """
        target = "u_eco_2"
        history_decay = {'history_decay_function_name': None}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()

        # Determines the index of the first actual value in the score tuples
        # produced by the recommender (note that hybrid recommenders use the first
        # position to indicate the algorithm number)
        if recommender.is_hybrid():
            start_index = 1
        else:
            start_index = 0

        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        former_top_product = recommendations[0][1]
        old_strength = recommendations[0][0]

        # Meta-test
        boost_activity_type = None
        in_boost = 1
        for boost_activity_type, in_boost in self.session_context.in_boost_by_activity.items():
            if in_boost != 1:
                break
        nose.tools.ok_(in_boost > 1, "Weak text fixture. There should be at least one in-boosted activity.")

        activity = {"external_user_id": target,
                    "external_product_id": former_top_product,
                    "activity": boost_activity_type,
                    "created_at": self.session_context.get_present_date()}
        tasks.update_summaries(self.session_context, activity)

        session.refresh()
        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        new_strength = None
        for rec in recommendations:
            if rec[1] == former_top_product:
                new_strength = rec[0]
                break

        nose.tools.ok_(new_strength is not None,
                       "The former top recommendation should have been recommended again.")
        for i in range(start_index, len(new_strength)):
            old_strength_value = old_strength[i]
            new_strength_value = new_strength[i]
            nose.tools.ok_(abs(new_strength_value / old_strength_value - in_boost) < tests.FLOAT_DELTA,
                           "Incorrect application of the activity in-boost")

        self.db_proxy.increment_impression_summary(user_id=target,
                                                   product_id=former_top_product,
                                                   date=self.session_context.get_present_date(),
                                                   anonymous=False)
        self.db_proxy.increment_impression_summary(user_id=target,
                                                   product_id=former_top_product,
                                                   date=self.session_context.get_present_date(),
                                                   anonymous=False)

        history_decay = {'history_decay_function_name': 'exponential', 'history_decay_exponential_function_halflife': 2}
        session = tests.init_session(user_id=target, custom_settings=history_decay, algorithm=self.algorithm)
        recommender = session.get_recommender()

        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        new_strength = None
        for rec in recommendations:
            if rec[1] == former_top_product:
                new_strength = rec[0]
                break

        nose.tools.ok_(new_strength is not None,
                       "The former top recommendation should have been recommended again.")
        for i in range(start_index, len(new_strength)):
            old_strength_value = old_strength[i]
            new_strength_value = new_strength[i]
            nose.tools.ok_(abs(new_strength_value / old_strength_value - in_boost / 2) < tests.FLOAT_DELTA,
                           "Incorrect application of the in-boost and history decay together")

    def test_product_age_decay_exponential(self):
        """ Tests the effect of applying a product age decay factor based on an exponential
            function on recommendations. It applies to all recommendation heuristics.
        """
        target = "u_tec_1"

        id_twin_product_old = "p_tec_TWIN_OLD"
        id_twin_product_new = "p_tec_TWIN_NEW"

        # makes it so that the oldest twin is 2 days (the configured half life) older
        old_date = self.session_context.get_present_date() - dt.timedelta(days=2)
        new_date = self.session_context.get_present_date()

        twin_product_old = {"external_id": id_twin_product_old,
                            "language": "english",
                            "date": old_date,
                            "expiration_date": old_date + dt.timedelta(days=30),
                            "resources": {"title": "Whatever Gets You Through The Night"},
                            "full_content": """Begin. Technology. Technology. This is all we got. End.""",
                            "category": "Nonsense"}

        twin_product_new = {"external_id": id_twin_product_new,
                            "language": "english",
                            "date": new_date,
                            "expiration_date": new_date + dt.timedelta(days=30),
                            "resources": {"title": "Whatever Gets You Through The Night"},
                            "full_content": """Begin. Technology. Technology. This is all we got. End.""",
                            "category": "Nonsense"}

        self.db_proxy.insert_product(twin_product_old)
        tasks.process_product(self.session_context, id_twin_product_old)
        self.db_proxy.insert_product(twin_product_new)
        tasks.process_product(self.session_context, id_twin_product_new)

        # makes it so that all users consume (and have impressions on) the twins, except for the target user
        users = self.db_proxy.fetch_all_user_ids()
        for user in users:
            if user != target:
                activity = {"external_user_id": user,
                            "external_product_id": id_twin_product_old,
                            "activity": "buy",
                            "created_at": self.session_context.get_present_date()}
                tasks.update_summaries(self.session_context, activity)

                activity = {"external_user_id": user,
                            "external_product_id": id_twin_product_new,
                            "activity": "buy",
                            "created_at": self.session_context.get_present_date()}
                tasks.update_summaries(self.session_context, activity)

                if self.session_context.impressions_enabled:
                    is_anonymous = config.is_anonymous(user)
                    self.db_proxy.increment_impression_summary(user,
                                                               id_twin_product_old,
                                                               date=self.session_context.get_present_date(),
                                                               anonymous=is_anonymous)
                    self.db_proxy.increment_impression_summary(user,
                                                               id_twin_product_new,
                                                               date=self.session_context.get_present_date(),
                                                               anonymous=is_anonymous)

        ut.generate_templates(self.session_context)
        pt.generate_templates(self.session_context)
        pttfidf.generate_templates(self.session_context)  # Unfortunately we need to regenerate from scratch,
                                                          # otherwise the df's of the twins will be different.

        custom_settings = {'product_age_decay_function_name': 'exponential',
                           'product_age_decay_exponential_function_halflife': 2,
                           'near_identical_filter_field': None, 'near_identical_filter_threshold': None}

        # Disables near-identical filtering
        session = tests.init_session(user_id=target, custom_settings=custom_settings, algorithm=self.algorithm)
        session.refresh()

        recommender = session.get_recommender()

        # Determines the index of the first actual value in the score tuples
        # produced by the recommender (note that hybrid recommenders use the first
        # position to indicate the algorithm number)
        if recommender.is_hybrid():
            start_index = 1
        else:
            start_index = 0

        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")

        strength_old_twin = None
        strength_new_twin = None

        for rec in recommendations:
            if rec[1] == id_twin_product_old:
                strength_old_twin = rec[0]
            if rec[1] == id_twin_product_new:
                strength_new_twin = rec[0]

        for i in range(start_index, len(strength_old_twin)):
            old_strength_value = strength_old_twin[i]
            new_strength_value = strength_new_twin[i]
            nose.tools.ok_(abs(old_strength_value / new_strength_value - 0.5) < tests.FLOAT_DELTA,
                           "Incorrect application of the product age decay")

    def test_recommend(self, test_recommendation_quality=True):
        """ Tests whether meaningful recommendations were obtained.
        """
        # pre-generates a session context and use it for all recommendation tests below
        session = tests.init_session(algorithm=self.algorithm)

        def generate_queries_for_category(category, user_count, product_count):
            for i in range(1, user_count + 1):
                target_user = 'u_{0}_{1}'.format(category, str(i))
                result = {
                    'target_user': target_user,
                    'category': category,
                    'product_count': product_count
                }
                yield result

        def recommend(target_user):
            """ Returns recommendations for a certain user.
                :param target_user: user to recommend
                :return: list of recommendations
            """
            # updates the session's user context
            session.user_id = target_user
            session.refresh()
            recommender = session.get_recommender()
            return recommender.recommend(self.n_recommendations)

        def verify_recommendations(_query, _recommendations):
            """ Verify that the recommendation was successful.
                :param _query: query parameters
                :param _recommendations: recommendation result set
            """
            recent_activities = session.user_context.recent_activities
            products_consumed = list({act["external_product_id"] for act in recent_activities})
            n_products_consumed = len(products_consumed)

            nose.tools.ok_(len(_recommendations) > 0, "No recommendations were retrieved")
            if test_recommendation_quality:
                for j in range(min(_query['product_count'] - n_products_consumed, len(_recommendations))):
                    nose.tools.eq_(_recommendations[j][1][:6], "p_{0}_".format(_query['category']),
                                   "Questionable recommendations were obtained " +
                                   "for user %s: %s" % (_query['target_user'], _recommendations))
        queries = []
        # Economia
        queries += generate_queries_for_category('eco', dp.N_USR_ECONOMIA, dp.N_PROD_ECONOMIA)
        # Esportes
        queries += generate_queries_for_category('esp', dp.N_USR_ESPORTES, dp.N_PROD_ESPORTES)
        # Música
        queries += generate_queries_for_category('mus', dp.N_USR_MUSICA, dp.N_PROD_MUSICA)
        # Tecnologia
        queries += generate_queries_for_category('tec', dp.N_USR_TECNOLOGIA, dp.N_PROD_TECNOLOGIA)

        # We did an experiment trying to parallelize the test recommendations, but there was no speedup because the
        # overhead is too cumbersome.
        n_workers = 1  # For some reason a thread pool with 1 worker is slightly faster than the nonconcurrent version
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            future_to_query = {executor.submit(wrap(recommend), q['target_user']): q
                               for q in queries}

            for future in concurrent.futures.as_completed(future_to_query):
                query = future_to_query[future]
                recommendations = future.result()
                verify_recommendations(query, recommendations)

    def test_multi_activities_blocking_vs_non_blocking(self):
        """ Checks that blocking activities prevent items from being recommended,
            and that non-blocking activities do not do so.
        """
        # Economia
        for i in range(1, dp.N_USR_ECONOMIA + 1):
            target = "u_eco_" + str(i)

            session = tests.init_session(user_id=target, algorithm=self.algorithm)
            recommender = session.get_recommender()
            recommendations = recommender.recommend(self.n_recommendations)

            nose.tools.ok_(len(recommendations) > 0, "Empty recommendation.")
            if len(recommendations) > 0:
                top_product = recommendations[0][1]
            else:
                return

            supported_activities = self.session_context.supported_activities
            blocking_activities = self.session_context.blocking_activities
            non_blocking_activities = list(set(supported_activities) - set(blocking_activities))

            # Meta-tests
            nose.tools.ok_(len(non_blocking_activities) > 0,
                           "Weak test fixture. There should be at least one non_blocking activity")
            nose.tools.ok_(len(blocking_activities) > 0,
                           "Weak test fixture. There should be at least one blocking activity")

            # Saves a non-blocking activity first
            activity = {"external_user_id": target,
                        "external_product_id": top_product,
                        "activity": non_blocking_activities[0],
                        "created_at": self.session_context.get_present_date()}
            tasks.update_summaries(self.session_context, activity)

            session = tests.init_session(user_id=target, algorithm=self.algorithm)
            recommender = session.get_recommender()            
            recommendations = recommender.recommend(self.n_recommendations)
            recommended_products = [r[1] for r in recommendations]
            nose.tools.ok_(top_product in recommended_products,
                           "A non-blocking activity should not prevent a product from being recommended")

            # Saves a blocking activity first
            activity = {"external_user_id": target,
                        "external_product_id": top_product,
                        "activity": blocking_activities[0],
                        "created_at": self.session_context.get_present_date()}
            tasks.update_summaries(self.session_context, activity)

            session = tests.init_session(user_id=target, algorithm=self.algorithm)
            recommender = session.get_recommender()
            recommendations = recommender.recommend(self.n_recommendations)
            recommended_products = [r[1] for r in recommendations]
            if self.session_context.filter_strategy == ctx.AFTER_SCORING:
                nose.tools.ok_(top_product not in recommended_products,
                               "A blocking activity should prevent a product from being recommended")

    def test_recommendation_slack(self):
        target_user = "u_tec_1"
        ttl = 3
        custom_settings = {
            'history_decay_function_name': 'step',
            'history_decay_step_function_ttl': ttl
        }
        session = tests.init_session(user_id=target_user, custom_settings=custom_settings, algorithm=self.algorithm)
        recommender = session.get_recommender()

        # this first query just measures how many products we're able to fetch
        max_results = len(recommender.recommend(1000))

        # retrieves half of those products
        initial_query = recommender.recommend(max_results // 2)

        recommended_products = [result[1] for result in initial_query]

        impressions_summary = self.db_proxy.fetch_impressions_summary(
            user_ids=[target_user],
            product_ids=[recommended_products],
            group_by_product=False,
            anonymous=False).get(target_user, {})

        # bury the first recommendation set with impressions so that those products do not appear again
        for product_id in recommended_products:
            # generate enough impressions to bury that product
            for i in range(ttl - impressions_summary.get(product_id, (0, None))[0]):
                self.db_proxy.increment_impression_summary(user_id=target_user,
                                                           product_id=product_id,
                                                           date=self.session_context.get_present_date(),
                                                           anonymous=False)
        session.refresh()

        # this query should still return some products if the recommender is internally adding a slack when fetching
        # products from the database
        recommender = session.get_recommender()
        second_query = recommender.recommend(max_results // 2)

        nose.tools.ok_(len(second_query) > 0, "Recommender '{0}' did not pass the 'slack test'".format(self.algorithm))

    def test_near_identical(self):
        """ Tests that two products considered 'near-identical' are not recommended at the same time
            (within the same page) when the filtering strategy is AFTER_SCORING.
        """
        target = "u_tec_1"

        id_twin_product_1 = "p_tec_TWIN_1"
        id_twin_product_2 = "p_tec_TWIN_2"

        date = self.session_context.get_present_date() - dt.timedelta(days=1)

        twin_product_1 = {"external_id": id_twin_product_1,
                          "language": "english",
                          "date": date,
                          "expiration_date": date + dt.timedelta(days=30),
                          "resources": {"title": "Whatever Gets You Through The Night"},
                          "full_content": """Begin. Technology. Technology. This is all we got. End.""",
                          "category": "Nonsense"}

        twin_product_2 = {"external_id": id_twin_product_2,
                          "language": "english",
                          "date": date,
                          "expiration_date": date + dt.timedelta(days=30),
                          "resources": {"title": "Whatever Gets You Through This Night is Alright"},
                          "full_content": """Begin. Technology. Technology. This is all we got. End.""",
                          "category": "Nonsense"}

        self.db_proxy.insert_product(twin_product_1)
        tasks.process_product(self.session_context, id_twin_product_1)
        self.db_proxy.insert_product(twin_product_2)
        tasks.process_product(self.session_context, id_twin_product_2)

        # makes it so that all users consume (and have impressions on) the twins, except for the target user
        users = self.db_proxy.fetch_all_user_ids()
        for user in users:
            if user != target:
                activity = {"external_user_id": user,
                            "external_product_id": id_twin_product_1,
                            "activity": "buy",
                            "created_at": self.session_context.get_present_date()}
                tasks.update_summaries(self.session_context, activity)

                activity = {"external_user_id": user,
                            "external_product_id": id_twin_product_2,
                            "activity": "buy",
                            "created_at": self.session_context.get_present_date()}
                tasks.update_summaries(self.session_context, activity)

                if self.session_context.impressions_enabled:
                    is_anonymous = config.is_anonymous(user)
                    self.db_proxy.increment_impression_summary(user,
                                                               id_twin_product_1,
                                                               date=self.session_context.get_present_date(),
                                                               anonymous=is_anonymous)
                    self.db_proxy.increment_impression_summary(user,
                                                               id_twin_product_2,
                                                               date=self.session_context.get_present_date(),
                                                               anonymous=is_anonymous)
        ut.generate_templates(self.session_context)
        pt.generate_templates(self.session_context)
        pttfidf.generate_templates(self.session_context)  # Unfortunately we need to regenerate from scratch,
                                                          # otherwise the df's of the twins will be different.

        # First, we recommend WITHOUT near-identical filtering, to check that the twins really appear consecutively.

        custom_settings = {'near_identical_filter_field': None,
                           'near_identical_filter_threshold': None}

        session = tests.init_session(user_id=target, custom_settings=custom_settings, algorithm=self.algorithm)
        session.refresh()

        recommender = session.get_recommender()

        if not recommender.is_hybrid():
        # For hybrid recommenders, this check is meaningless.

            recommendations = recommender.recommend(100)

            twin_index = -1
            for idx, recommendation in enumerate(recommendations):
                if recommendation[1].startswith("p_tec_TWIN_"):
                    if twin_index >= 0:
                        nose.tools.eq_(idx - twin_index, 1,
                                       "The two near-identical products should appear consecutively without filtering")
                        break
                    twin_index = idx

        # Now we recommend WITH near-identical filtering

        recommendation_page_size = 5
        custom_settings = {'near_identical_filter_field': 'resources.title',
                           'near_identical_filter_threshold': 2,
                           'recommendations_page_size': recommendation_page_size}

        session = tests.init_session(user_id=target, custom_settings=custom_settings, algorithm=self.algorithm)
        session.refresh()

        recommender = session.get_recommender()
        recommendations = recommender.recommend(100)

        # Sanity check
        recommended_products = {r[1] for r in recommendations}
        count_recommended_twins = len({id_twin_product_1, id_twin_product_2} & recommended_products)
        nose.tools.ok_(count_recommended_twins > 0,
                       "At least one of the twins should have been recommended, otherwise the test is meaningless")

        # Actual tests
        twin_index = -1 * recommendation_page_size - 1  # initial value, so the first twin passes the test
        for idx, recommendation in enumerate(recommendations):
            if recommendation[1].startswith("p_tec_TWIN_"):
                nose.tools.ok_(idx - twin_index > 1,  # it suffices to show that the twins have been separated
                               "Two near-identical products should not appear within the same recommendations page")
                twin_index = idx
