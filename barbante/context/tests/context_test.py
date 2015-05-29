import nose.tools
import datetime as dt
import pytz

from barbante import tests
from barbante import context
from barbante.config import database
import barbante.maintenance.tasks as tasks
import barbante.utils.date as du


class TestContext():

    def __init__(self):
        self.context = tests.init_session()

    def test_present_date(self):
        """ Tests the update of long and short term cutoff dates based on an updated value of the present date.
        """
        new_date = dt.datetime(1988, 11, 6, 10, 0)
        self.context.set_present_date(new_date)
        localized_day = du.get_day(pytz.utc.localize(new_date))

        new_short_term_cutoff_date = localized_day - dt.timedelta(days=self.context.short_term_window)
        new_long_term_cutoff_date = localized_day - dt.timedelta(days=self.context.long_term_window)
        nose.tools.eq_(self.context.short_term_cutoff_date, new_short_term_cutoff_date)
        nose.tools.eq_(self.context.long_term_cutoff_date, new_long_term_cutoff_date)

    def test_all_configs(self):
        for env in database:
            test_context = context.create_customer_context(env, self.context.data_proxy)
            nose.tools.ok_(test_context is not None, 'Env [{0}] context has failed to load'.format(env))

    @nose.tools.raises(ValueError)
    def test_invalid_attribute_pre_filter(self):
        target_user = "u_filter_1"
        custom_settings = {
            'filter_strategy': context.BEFORE_SCORING
        }
        tests.init_session(user_id=target_user, custom_settings=custom_settings,
                           context_filter_string="{\"xxxx\": \"yyyy\"}")

    @nose.tools.raises(ValueError)
    def test_invalid_attribute_pos_filter(self):
        target_user = "u_filter_1"
        custom_settings = {
            'filter_strategy': context.AFTER_SCORING
        }
        tests.init_session(user_id=target_user, custom_settings=custom_settings,
                           context_filter_string="{\"xxxx\": \"yyyy\"}")

    @nose.tools.raises(ValueError)
    def test_invalid_date_pre_filter(self):
        target_user = "u_filter_1"
        custom_settings = {
            'filter_strategy': context.BEFORE_SCORING
        }
        tests.init_session(user_id=target_user, custom_settings=custom_settings,
                           context_filter_string="{\"date\": \"1234X\"}")

    @nose.tools.raises(ValueError)
    def test_invalid_date_post_filter(self):
        target_user = "u_filter_1"
        custom_settings = {
            'filter_strategy': context.AFTER_SCORING
        }
        tests.init_session(user_id=target_user, custom_settings=custom_settings,
                           context_filter_string="{\"date\": \"1234X\"}")

    @staticmethod
    def test_singleton_context_loading():
        proxy1 = context.get_preloaded_customer_context(tests.TEST_ENV)
        proxy2 = context.get_preloaded_customer_context(tests.TEST_ENV)
        nose.tools.eq_(proxy1, proxy2, 'Two instances of CustomerContext were created - should be a singleton')
        nose.tools.eq_(proxy1.data_proxy, proxy2.data_proxy,
                       'Two instances of the data proxy were created - should be a singleton')

    @staticmethod
    def test_previous_consumption_factor():
        target_user = "u_eco_1"
        session_context = tests.init_session(user_id=target_user,
                                             custom_settings={'previous_consumption_factor': 0.1})

        activity = {"external_user_id": target_user,
                    "external_product_id": "p_eco_2",
                    "activity": "buy",
                    "created_at": session_context.get_present_date()}
        tasks.update_summaries(session_context, activity)
        session_context.refresh()

        nose.tools.eq_(session_context.obtain_previous_consumption_factor("p_eco_1"), 1,
                       "Previous consumption factor should be 1 for non-consumed products")
        nose.tools.ok_(abs(session_context.obtain_previous_consumption_factor("p_eco_2") - 0.1) < tests.FLOAT_DELTA,
                       "Wrong previous consumption factor")

