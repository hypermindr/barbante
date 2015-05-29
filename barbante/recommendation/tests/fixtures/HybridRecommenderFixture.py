""" Test fixture for hybrid recommendations.
"""

import datetime as dt
import nose.tools

import barbante.context as ctx
import barbante.maintenance.product_templates as pt
import barbante.maintenance.product_templates_tfidf as pt_tfidf
import barbante.maintenance.user_templates as ut
from barbante.recommendation.tests.fixtures.RecommenderFixture import RecommenderFixture
import barbante.tests as tests


class HybridRecommenderFixture(RecommenderFixture):
    """ Class for testing barbante.recommendation.HybridRecommender subclasses.
    """
    def setup(self):
        super().setup()
        ut.generate_templates(self.session_context)
        pt.generate_templates(self.session_context)
        pt_tfidf.generate_templates(self.session_context)

    def test_recommend(self, test_recommendation_quality=True):
        """ Tests whether meaningful recommendations were obtained.
        """
        super().test_recommend(test_recommendation_quality=False)

    def test_recommend_with_exception_in_one_concrete_recommender(self):
        """ Tests whether the hybrid recommender recovers from a failure in one of the specialists.
        """
        session = tests.init_session(user_id="u_eco_1", algorithm=self.algorithm)
        session.algorithm_weights = {self.algorithm: [["Mock", 1.0]]}
        session.fill_in_algorithm = None
        recommender = session.get_recommender()
        result = recommender.recommend(self.n_recommendations)
        nose.tools.eq_(result, [], "A failure in a specialist algorithm should yield an empty list")

    def test_recommend_non_existing_user(self):
        """ Tests whether meaningful recommendations are returned even for unknown users.
        """
        session = tests.init_session(user_id="Invalid user id", algorithm=self.algorithm)
        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)
        nose.tools.ok_(len(results) > 0, "Hybrid recommenders should recommend even for unknown users")

    def test_recommend_anonymous_user(self):
        """ Tests whether valid recommendations are returned for an anonymous user.
        """
        session = tests.init_session(user_id="hmrtmp_AnonymousUser1", algorithm=self.algorithm)
        recommender = session.get_recommender()
        results = recommender.recommend(self.n_recommendations)
        nose.tools.ok_(len(results) > 0, "Hybrid recommenders should recommend even for anonymous users")

    def test_fill_in_products(self):
        """ Tests the merge based on fixed slices.
        """
        n_recommendations = 30
        recommendations_by_alg = {"UBCF": [[[50], "UBCF_1"],
                                           [[30], "UBCF_2"],
                                           [[10], "UBCF_3"],
                                           [[5], "UBCF_4"],
                                           [[2], "UBCF_5"]],

                                  "PBCF": [[[50], "PBCF_1"],
                                           [[30], "PBCF_2"],
                                           [[10], "PBCF_3"],
                                           [[5], "PBCF_4"]],

                                  "CB": [[[50], "CB_1"],
                                         [[40], "CB_2"],
                                         [[30], "CB_3"],
                                         [[20], "CB_4"],
                                         [[10], "CB_5"],
                                         [[9], "CB_6"],
                                         [[8], "CB_7"],
                                         [[7], "CB_8"],
                                         [[4], "CB_9"]],

                                  "POP": [[[50], "POP_1"],
                                          [[30], "POP_2"],
                                          [[10], "POP_3"],
                                          [[5], "POP_4"],
                                          [[4], "POP_5"],
                                          [[3], "POP_6"],
                                          [[4], "POP_7"]]}

        session = tests.init_session(user_id="u_eco_1", algorithm=self.algorithm)
        recommender = session.get_recommender()
        merged_recommendations = recommender.merge_algorithm_contributions(recommendations_by_alg, n_recommendations)
        recommender.include_fill_in_recommendations(merged_recommendations, recommendations_by_alg, n_recommendations)

        products_rank = [rec[1] for rec in merged_recommendations]
        for item in products_rank[18:]:
            nose.tools.ok_(item.startswith("POP_"), "Wrong rank after merge")

    def test_history_decay_step(self):
        # It is not easy to test decays here, since the same item can be recommended by different algorithms.
        # Since the decay logic is applied by the base Recommender, no big deal we do not repeat the test here.
        pass

    def test_history_decay_rational(self):
        # It is not easy to test decays here, since the same item can be recommended by different algorithms.
        # Since the decay logic is applied by the base Recommender, no big deal we do not repeat the test here.
        pass

    def test_history_decay_exponential(self):
        # It is not easy to test decays here, since the same item can be recommended by different algorithms.
        # Since the decay logic is applied by the base Recommender, no big deal we do not repeat the test here.
        pass

    def test_history_decay_linear(self):
        # It is not easy to test decays here, since the same item can be recommended by different algorithms.
        # Since the decay logic is applied by the base Recommender, no big deal we do not repeat the test here.
        pass

    def test_in_boost(self):
        # It is not easy to test in-boosts here, since the same item can be recommended by different algorithms.
        # Since the in-boost logic is applied by the base Recommender, no big deal we do not repeat the test here.
        pass

    def test_product_age_decay_exponential(self):
        # It is not easy to test decays here, since the same item can be recommended by different algorithms.
        # Since the decay logic is applied by the base Recommender, no big deal we do not repeat the test here.
        pass

    def test_pre_filter_returning_all(self):
        target_user = "u_tec_1"
        custom_settings = {
            'filter_strategy': ctx.BEFORE_SCORING
        }
        intended_count = self.db_proxy.get_product_model_count()

        self._check_empty_filter_returning_all_products(custom_settings, intended_count, target_user)

    def test_pos_filter_returning_all(self):
        target_user = "u_tec_1"
        custom_settings = {
            'filter_strategy': ctx.AFTER_SCORING
        }
        session = tests.init_session(user_id=target_user, custom_settings=custom_settings, algorithm=self.algorithm)
        recommender = session.get_recommender()
        intended_count = len(recommender.recommend(1000))

        self._check_empty_filter_returning_all_products(custom_settings, intended_count, target_user)

    def test_pre_vs_pos_filter_without_missing_pre_filtered_candidates(self):
        filter_string = '{"language": "portuguese", "category": "Economia"}'
        n_recommendations = 4
        self._check_pre_and_pos_filters_match(filter_string, n_recommendations)

    def test_pre_vs_pos_filter_with_missing_pre_filtered_candidates(self):
        filter_string = '{"language": "portuguese"}'
        n_recommendations = 15
        self._check_pre_and_pos_filters_match(filter_string, n_recommendations)

    def test_pre_filter_returning_none(self):
        self._check_result_is_none_for_bad_filter(ctx.BEFORE_SCORING)

    def test_pos_filter_returning_none(self):
        self._check_result_is_none_for_bad_filter(ctx.AFTER_SCORING)

    def test_pre_filter_with_language(self):
        self._check_language_filter(ctx.BEFORE_SCORING)

    def test_pos_filter_with_language(self):
        self._check_language_filter(ctx.AFTER_SCORING)

    def test_pre_filter_with_german_language(self):
        strategy = ctx.BEFORE_SCORING
        intended_count = 3
        self._check_number_of_filtered_products(intended_count, strategy)

    def test_pos_filter_with_german_language(self):
        strategy = ctx.AFTER_SCORING
        intended_count = 2
        self._check_number_of_filtered_products(intended_count, strategy)

    def test_pre_filter_with_basic_and_parameters(self):
        strategy = ctx.BEFORE_SCORING
        intended_count = 3
        self._check_basic_and_filters(intended_count, strategy)

    def test_pos_filter_with_basic_and_parameters(self):
        strategy = ctx.AFTER_SCORING
        intended_count = 2
        self._check_basic_and_filters(intended_count, strategy)

    def test_pre_filter_with_basic_or_parameters(self):
        strategy = ctx.BEFORE_SCORING
        intended_count = 5
        self._check_basic_or_filters(intended_count, strategy)

    def test_pos_filter_with_basic_or_parameters(self):
        strategy = ctx.AFTER_SCORING
        intended_count = 5
        self._check_basic_or_filters(intended_count, strategy)

    def test_pre_filter_with_list_filter(self):
        product_count = self.db_proxy.get_product_model_count()
        target_user = "u_filter_1"
        custom_settings = {
            'filter_strategy': ctx.BEFORE_SCORING
        }

        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "german",'
                                                           '"source": "source2"}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(product_count > 0, 'The filter test requires products to exist')
        nose.tools.eq_(len(filtered_products), 2)

        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "german",'
                                                           '"source": "source1"}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(len(filtered_products), 1)

        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "german",'
                                                           '"source": ["source2", "source3"]}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(len(filtered_products), 2)

    def test_pos_filter_with_list_filter(self):
        product_count = self.db_proxy.get_product_model_count()
        target_user = "u_filter_1"
        custom_settings = {
            'filter_strategy': ctx.AFTER_SCORING
        }
        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "german",'
                                                           '"source": "source2"}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(product_count > 0, 'The filter test requires products to exist')
        nose.tools.eq_(len(filtered_products), 1)

        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "german",'
                                                           '"source": "source1"}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 0)

        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "german",'
                                                           '"source": ["source2", "source3"]}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(len(filtered_products), 1)

    def test_pre_filter_with_dates(self):
        product_count = self.db_proxy.get_product_model_count()
        target_user = "u_filter_1"
        custom_settings = {
            'filter_strategy': ctx.BEFORE_SCORING
        }

        date = self.session_context.get_present_date()
        date_str = date.isoformat()
        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": "{0}"}}'.format(
                date_str),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(product_count > 0, 'The filter test requires products to exist')
        nose.tools.eq_(len(filtered_products), 3)

        one_hour_before = date - dt.timedelta(hours=1)
        one_hour_after = date + dt.timedelta(hours=1)
        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$gt": "{0}"}}}}'.format(
                one_hour_before.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 3)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$lt": "{0}"}}}}'.format(
                one_hour_before.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 0)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$lt": "{0}"}}}}'.format(
                one_hour_after.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 3)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$lt": "{0}"}}}}'.format(
                one_hour_before.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 0)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$gt": "{0}", "$lt": "{1}"}}}}'.format(
                one_hour_before.isoformat(), one_hour_after.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 3)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$lt": "{0}", "$gt": "{1}"}}}}'.format(
                one_hour_before.isoformat(), one_hour_after.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 0)

    def test_pos_filter_with_dates(self):
        product_count = self.db_proxy.get_product_model_count()
        target_user = "u_filter_1"
        custom_settings = {
            'filter_strategy': ctx.AFTER_SCORING
        }

        date = self.session_context.get_present_date()
        date_str = date.isoformat()
        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": "{0}"}}'.format(date_str),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(product_count > 0, 'The filter test requires products to exist')
        nose.tools.eq_(len(filtered_products), 2)

        one_hour_before = date - dt.timedelta(hours=1)
        one_hour_after = date + dt.timedelta(hours=1)
        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$gt": "{0}"}}}}'.format(
                one_hour_before.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 2)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$lt": "{0}"}}}}'.format(
                one_hour_before.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 0)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$lt": "{0}"}}}}'.format(
                one_hour_after.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 2)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$lt": "{0}"}}}}'.format(
                one_hour_before.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 0)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$gt": "{0}", "$lt": "{1}"}}}}'.format(
                one_hour_before.isoformat(), one_hour_after.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 2)

        session = tests.init_session(
            user_id=target_user,
            custom_settings=custom_settings,
            context_filter_string='{{"language": "german","date": {{"$lt": "{0}", "$gt": "{1}"}}}}'.format(
                one_hour_before.isoformat(), one_hour_after.isoformat()),
            algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.eq_(len(filtered_products), 0)

    def _check_empty_filter_returning_all_products(self, custom_settings, intended_count, target_user):
        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string="{}",
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filter_count = len(recommender.recommend(1000))
        nose.tools.ok_(intended_count > 0, 'The filter test requires products to exist')
        nose.tools.eq_(intended_count, filter_count,
                       'An empty filter should bring all products total({0}), returned({1})'.format(
                           intended_count, filter_count))

    def _check_result_is_none_for_bad_filter(self, strategy):
        target_user = "u_tec_1"
        custom_settings = {
            'filter_strategy': strategy
        }
        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "xxxx"}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(1000)
        nose.tools.eq_(len(filtered_products), 0)

    def _check_language_filter(self, strategy):
        product_count = self.db_proxy.get_product_model_count()
        target_user = "u_tec_1"
        custom_settings = {
            'filter_strategy': strategy
        }
        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        product_ids = [product_id for _, product_id in recommender.recommend(1000)]
        products = {product_id for product_id, product in
                    self.db_proxy.fetch_product_models(product_ids=product_ids,
                                                       max_date=session.get_present_date()).items() if
                    product.get_attribute("language") == "portuguese"}

        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "portuguese"}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(1000)
        filtered_product_ids = [product_id for _, product_id in filtered_products]
        nose.tools.ok_(product_count > 0, 'The filter test requires products to exist')
        nose.tools.ok_(
            products.issubset(filtered_product_ids),
            'A filtered request should only bring the products that match the filter requirements,'
            ' total({0}), returned({1})'.format(len(product_ids), len(filtered_product_ids)))

    def _check_number_of_filtered_products(self, intended_count, strategy):
        custom_settings = {
            'filter_strategy': strategy
        }
        product_count = self.db_proxy.get_product_model_count()
        target_user = "u_filter_1"
        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string='{"language": "german"}',
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(product_count > 0, 'The filter test requires products to exist')
        nose.tools.eq_(len(filtered_products), intended_count)

    def _check_pre_and_pos_filters_match(self, filter_string, n_recommendations):
        target_user = "u_tec_1"
        custom_settings = {
            'filter_strategy': ctx.BEFORE_SCORING,
            'previous_consumption_factor': 0
        }
        session = tests.init_session(custom_settings,
                                     context_filter_string=filter_string,
                                     user_id=target_user,
                                     algorithm=self.algorithm)
        pre_filtered_candidates_count = len(session.filtered_products)
        # sanity check
        nose.tools.ok_(pre_filtered_candidates_count > 0, "Weak test. No pre-filtered candidate products.")

        recommender = session.get_recommender()
        recommendation_with_pre_filter = recommender.recommend(n_recommendations)
        ranked_products_pre_filter = [r[1] for r in recommendation_with_pre_filter]

        custom_settings = {
            'filter_strategy': ctx.AFTER_SCORING,
            'previous_consumption_factor': 0
        }
        session = tests.init_session(custom_settings,
                                     context_filter_string=filter_string,
                                     user_id=target_user,
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        recommendation_with_pos_filter = recommender.recommend(n_recommendations)
        ranked_products_pos_filter = [r[1] for r in recommendation_with_pos_filter]

        nose.tools.eq_(ranked_products_pre_filter[:pre_filtered_candidates_count],
                       ranked_products_pos_filter[:pre_filtered_candidates_count],
                       "Recommendation lists for pre- and pos-filters do not match")

    def _check_basic_and_filters(self, intended_count, strategy):
        custom_settings = {
            'filter_strategy': strategy
        }
        product_count = self.db_proxy.get_product_model_count()
        target_user = "u_filter_1"
        date = self.session_context.get_present_date()
        date_str = date.isoformat()
        context_filter_string = '{"$and": [{"language": "german"}, {"date": "' + date_str + '"}]}'
        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string=context_filter_string,
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(product_count > 0, 'The filter test requires products to exist')
        nose.tools.eq_(len(filtered_products), intended_count)

    def _check_basic_or_filters(self, intended_count, strategy):
        custom_settings = {
            'filter_strategy': strategy
        }
        product_count = self.db_proxy.get_product_model_count()
        target_user = "u_filter_1"
        date = self.session_context.get_present_date()
        date_str = date.isoformat()
        context_filter_string = '{"$or": [{"language": "german"}, {"date": "' + date_str + '"}]}'
        session = tests.init_session(user_id=target_user,
                                     custom_settings=custom_settings,
                                     context_filter_string=context_filter_string,
                                     algorithm=self.algorithm)
        recommender = session.get_recommender()
        filtered_products = recommender.recommend(5)
        nose.tools.ok_(product_count > 0, 'The filter test requires products to exist')
        nose.tools.eq_(len(filtered_products), intended_count)
