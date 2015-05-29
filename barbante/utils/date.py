""" Module for date utility functions.
"""


def get_day(date):
    """ Rounds the datetime to midnight.

        :param date: a Python datetime.datetime object.
        :returns: a datetime.datetime object representing midnight of the informed day.
    """
    return date.replace(hour=0, minute=0, second=0, microsecond=0)