""" Utilities module.
"""

import datetime as dt

import pkg_resources

import barbante


def local_import(name):
    """ Returns the module *name*.

        Attributes:
            name - the name of the module to be imported.

        Exception:
            TypeError - if *name* is not a string.
            ImportError - if there is no module *name* in your namespace.
    """
    if type(name) is not str:
        raise TypeError("barbante.api.recommend: name must be a string.")
    try:
        mod = __import__(name)
    except ImportError:
        raise ImportError(
            "barbante.api.recommend: there is no module name {0}".format(name))
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def resource_stream(resource_path, package=barbante.__name__):
    """ Encapsulates pkg_resources.resource_stream.
    """
    return pkg_resources.resource_stream(package, resource_path)


def resource_filename(resource_path, package=barbante.__name__):
    """ Encapsulates pkg_resources.resource_filename.
    """
    return pkg_resources.resource_filename(package, resource_path)


def parse_date_series(series, date_format, queue=None):
    """ Implements pandas' apply function.

        **Parameters**
            date - date string in some format, e.g.: 31/12/2014.

            date_format - string format for *date*, e.g.: %d/%m/%Y %H:%M:%S.

            queue - multiprocessing.Queue for returning results.
    """
    new_series = series.apply(string_to_date, args=[date_format])
    if queue is not None:
        queue.put(new_series)


def parse_date_list(*args):
    """ Replaces pandas' *apply* function.

        :param args: (date_list,
                      start_index,
                      end_index,
                      date string in some format, e.g.: 31/12/2014,
                      string format for *date*, e.g.: %d/%m/%Y %H:%M:%S,
                      multiprocessing.Queue for returning results)
    """
    date_list = args[0]
    queue = args[1]
    date_format = args[2]
    new_series = []
    for date in date_list:
        new_series.append(string_to_date(date, date_format))

    queue.put(new_series)


def string_to_date(date, date_format):
    """ Returns the Unix date format from pandas.datetime.

        **Parameters**
        date - date string in some format, e.g.: 31/12/2014.

        date_format - string format for *date*, e.g.: %d/%m/%Y %H:%M:%S.
    """
    try:
        # return int(time.mktime(dt.datetime.strptime(date,
        # date_format).timetuple()))
        return dt.datetime.strptime(date, date_format)
    except TypeError:
        return


def flatten_dict(product_model_dict):
    """ Converts a dict with nested attributes into a flattened dict,
        where the path to each attribute in the original dict maps to
        a key in the new dict.

            > d = {'a': 1, 'b': {'c': 2, 'd': {'e': 3, 'f': 4}}}
            > ProductModel._flatten_dict(d)
            {'a': 1, 'b.c': 2, 'b.d.e': 3, 'b.d.f': 4}

        :param product_model_dict: the original dict.
        :returns: a flattened dict.
    """
    result = {}
    for key, value in product_model_dict.items():
        if isinstance(value, dict):
            subdict = flatten_dict(value)
            for sk, sv in subdict.items():
                result[key + '.' + sk] = sv
        else:
            result[key] = value
    return result