import datetime as dt
import pytz

from barbante.context.user_context import UserContext
from barbante.recommendation.filters.context_filter import ContextFilter
import barbante.utils.date as du

import barbante.utils.logging as barbante_logging
log = barbante_logging.get_logger(__name__)


class SessionContext(object):

    def __init__(self, customer_context, user_id=None, context_filter_string=None, algorithm=None):
        self.customer_context = customer_context
        """ The customer context.
        """
        self.user_context = None
        """ Reference to a UserContext object,if user_id is not None.
        """
        self.user_id = user_id
        """ The user id. If not None, the SessionContext will hold a UserContext reference.
        """
        self.context_filter_string = context_filter_string
        """ String representing a context filter.
        """
        self.context_filter = ContextFilter(self, context_filter_string)
        """ The user-defined ContextFilter object for filtering recommendations.
        """
        self.algorithm = algorithm
        """ The algorithm that will be used for recommending products throughout this session.
        """
        self._present_date = None
        """ The system date. Can be overriden for tests.
        """
        self.short_term_cutoff_date = None
        """ The short term cutoff date is calculated applying the short_term_window to the present date,
            whenever the present date is set (via setter method).
        """
        self.long_term_cutoff_date = None
        """ The long term cutoff date is calculated applying the long_term_window to the present date,
            whenever the present date is set (via setter method).
        """
        self.popularity_cutoff_date = None
        """ The popularity cutoff date is calculated applying the popularity to the present date,
            whenever the present date is set (via setter method).
        """

        self.set_present_date(self.customer_context.initial_date)
        self.refresh()

    def __getattr__(self, item):
        if self.user_context is None:
            return getattr(self.customer_context, item)
        else:
            return getattr(self.user_context, item)

    def get_present_date(self):
        return self._present_date

    def set_present_date(self, present_date):
        if present_date and str(present_date).lower() != 'none':
            if present_date.tzinfo is None:
                present_date = pytz.utc.localize(present_date)
            self._present_date = present_date
        else:
            self._present_date = dt.datetime.now(pytz.utc)

        self.short_term_cutoff_date = self._present_date - dt.timedelta(self.short_term_window)
        self.short_term_cutoff_date = du.get_day(self.short_term_cutoff_date)

        self.long_term_cutoff_date = self._present_date - dt.timedelta(self.long_term_window)
        self.long_term_cutoff_date = du.get_day(self.long_term_cutoff_date)

        self.popularity_cutoff_date = self._present_date - dt.timedelta(self.popularity_window)
        self.popularity_cutoff_date = du.get_day(self.popularity_cutoff_date)

    def obtain_product_age_decay_factor(self, product_date):
        """ See CustomerContext.obtain_product_age_decay_factor(). """
        return self.customer_context.obtain_product_age_decay_factor(product_date, self.get_present_date())

    def refresh(self):
        if self.user_id is not None:
            self.user_context = UserContext(self, self.user_id, self.context_filter, self.algorithm)

    def new_session(self):
        return SessionContext(self.customer_context, self.user_id, self.context_filter_string, self.algorithm)

