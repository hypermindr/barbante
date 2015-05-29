""" Test settings.
"""
import logging

from barbante import context

# Disable logging to speed up tests:
logging.disable(logging.ERROR)

TEST_ENV = "unit-test"

INCLUDE_RANDOM_TESTS = False
""" If True, several random tests will be run.
    It can be set to False to speed up daily testing,
    but it won't hurt to run the random harness on a regular basis.
"""

FLOAT_DELTA = 0.00001
""" Accepted error for float comparisons.
"""

_test_db_proxy_singleton = None
""" Use a single data proxy instance to all tests.
"""


# noinspection PyProtectedMember
def _init_context(custom_settings, use_custom_data_proxy=False):
    global _test_db_proxy_singleton

    customer_ctx = context.clone_preloaded_customer_context(
        TEST_ENV,
        _test_db_proxy_singleton if not use_custom_data_proxy else None,
        use_custom_data_proxy)

    if _test_db_proxy_singleton is None:
        _test_db_proxy_singleton = customer_ctx.data_proxy

    if custom_settings:
        for attribute in custom_settings:
            setattr(customer_ctx, attribute, custom_settings[attribute])
    return customer_ctx


# noinspection PyProtectedMember
def init_session(custom_settings=None, context_filter_string=None, user_id=None,
                 algorithm=None, use_custom_data_proxy=False):
    """ Inits a test session.

        :param custom_settings: Used to override customer settings defined in the customer config
        :param context_filter_string: A ContextFilter instance.
        :param user_id: If None, UserContext won't be created.
        :param algorithm: The algorithm to be used for recommendations during the tests session.
        :param use_custom_data_proxy: Do not reuse the singleton data proxy; instead, create a new one.

        :returns: the SessionContext that can be used to access settings and database
    """

    customer_context = _init_context(custom_settings, use_custom_data_proxy)

    # sets write concern level to 1 - necessary for tests to pass
    customer_context.data_proxy.write_concern_level = 1

    test_session = context.init_session(customer_ctx=customer_context,
                                        user_id=user_id,
                                        context_filter_string=context_filter_string,
                                        algorithm=algorithm)
    return test_session
