""" Decay functions.
"""


def linear(x, root):
    """ Returns a decay factor based on the linear function

        .. math::
            f(x) = \\begin{cases} -x/root + 1  & \\mbox{if} x < root; \\\\
                                  0            & otherwise.
                   \\end{cases}

        :param x: The function argument *x*.
        :param root: The value of *x* after which the decay factor is zero.
    """
    return 1 - min(x, root) / root


def rational(x):
    """ Returns a decay factor based on the rational function

        .. math::
            f(x) = 1 / (x+1)

        :param x: The function argument.
    """
    return 1 / (x + 1)


def exponential(x, halflife):
    """ Returns a decay factor based on the exponential function

        .. math::
            f(x) = 2^(-x/halflife).

        :param x: The function argument.
        :param halflife: The half-life of the decay process.
    """
    return 2 ** (-x / halflife)


def step(x, high, low, threshold):
    """ Returns a decay factor based on the step function

        .. math::
            f(x) = \\begin{cases} high & \\mbox{if} x < threshold; \\\\
                                  low & otherwise.
                   \\end{cases}

        :param x: The function argument.
        :param threshold: The first value of *x* that yields *low* as the decay factor.
    """
    if x < threshold:
        return high
    else:
        return low
