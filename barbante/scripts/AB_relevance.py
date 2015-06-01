#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    This script runs a simulation to obtain (empirically) the relevance of A/B tests.

    There are three possible usages:

    1st: 'ab_test'
        It simulates a single run of an A/B test spanning n_days, returning the accrued gain of B as compared to A,
        as well as the absolute number of conversions in A and B groups, the number of days won by A and B, etc.
        It assumes that the users in B have a greater probability of conversion than those in A. How greater
        is given by the parameter expected_increase.

        usage:
          ``python3 AB_relevance.py ab_test <avg_daily_trials> <avg_daily_successes> <n_days> <ab_size>
               <expected_increase>``

        parameters:
          avg_daily_trials -- the average number of daily sessions (with no system changes)
          avg_daily_successes -- the average number of daily successes (buys, reads, etc.)
          n_days -- the number of days the A/B test will span through
          ab_size -- the percentage of users used in the tests (50% in A, 50% in B)
          expected_increase -- the believed nominal gain due to the modifications in B (i.e., recommendations)

    2nd: 'relevance'
        It runs several repetitions of an A/B test spanning n_days, for n_days varying from 1 to the minimum number
        of days that is sufficient for the probability of the test being successful is greater than the min_reliability
        parameter. A test is considered successful if the observed accrued gain is greater than or equal to
        expected_increase minus accepted_deviation. For instance, if expected_gain is 10%, accepted_deviation is 2%,
        and min_reliability is 95%, the test will run until it figures out the minimum number of days so that
        the A/B test returns an accrued increase of at least 10% - 2% = 8% with probability at least 95%.
        This whole thing is repeated for test group sizes (half in A, half in B) varying from 5% to 100%,
        in 5% increments.

        usage:
          ``python3 AB_relevance.py relevance <avg_daily_trials> <avg_daily_successes> <expected_increase>
                <accepted_deviation> <min_reliability>``

        parameters:
          avg_daily_trials -- the average number of daily sessions (with no system changes)
          avg_daily_successes -- the average number of daily successes (buys, reads, etc.)
          expected_increase -- the believed nominal gain due to the modifications in B (i.e., recommendations)
          accepted_deviation -- defines the accepted lower bound to the accrued gain on a 'successful' A/B test
          min_reliability -- the intended minimum reliability

    3rd: 'gain curve'
        It uses an empirical approach to obtain the prior probabilities of obtaining the reported_increase
        considering the actual gain of the recommender is 1%, 2%, etc. (actually in [0.5%, 1.5%[, in
        [1.5%, 2.5%[, etc., respectively), then it uses Bayes to compute the conditional probability that
        the actual gain of the recommender is 1%, 2%, etc., given the reported gain.

        usage:
          ``python3 AB_relevance.py gain_curve <avg_daily_trials> <avg_daily_successes> <n_days> <ab_size>
               <reported_increase>``

        parameters:
          avg_daily_trials -- the average number of daily sessions (with no system changes)
          avg_daily_successes -- the average number of daily successes (buys, reads, etc.)
          n_days -- the believed nominal gain due to the modifications in B (i.e., recommendations)
          ab_size -- the percentage of users used in the tests (50% in A, 50% in B)
          reported_increase -- the reported increase in the conversion rate
"""

import numpy.random


# hard-coded settings
N_TESTS = 20000
MIN_REASONABLE_GAIN = -0.3
MAX_REASONABLE_GAIN = 0.5
GAIN_CURVE_GRANULARITY = 0.01

# constants
A = 1
B = 2
INFINITY = 2**64

# global variables
sessions_per_day = 0
buys_per_day = 0


def simulate_ab_test(n_days, ab_size, expected_gain, should_print=False):
    total_buys_a, total_buys_b = 0, 0
    days_won_a, days_won_b, days_drawn = 0, 0, 0

    for _ in range(n_days):
        buys_a = simulate_day(A, ab_size, expected_gain)
        buys_b = simulate_day(B, ab_size, expected_gain)
        total_buys_a += buys_a
        total_buys_b += buys_b

        if buys_a > buys_b:
            days_won_a += 1
        elif buys_b > buys_a:
            days_won_b += 1
        else:
            days_drawn += 1

    if should_print:
        print("total buys A = %d" % total_buys_a)
        print("total buys B = %d" % total_buys_b)
        print("gain = %.2f%%" % (100 * (total_buys_b - total_buys_a) / total_buys_a))
        print("days won by A = %d" % days_won_a)
        print("days won by B = %d" % days_won_b)
        print("days drawn = %d" % days_drawn)

    return total_buys_a, total_buys_b, days_won_a, days_won_b, days_drawn


def simulate_day(group, ab_size, expected_gain):
    base_prob_buy = buys_per_day / sessions_per_day
    n_sessions = round(sessions_per_day * ab_size / 2)
    prob_buy = base_prob_buy
    if group == B:
        prob_buy *= (1 + expected_gain)
    return numpy.random.binomial(n_sessions, prob_buy)


def get_accrued_gain(n_days, ab_size, expected_gain):
    total_buys_a, total_buys_b, days_won_a, days_won_b, days_drawn = simulate_ab_test(
        n_days, ab_size, expected_gain)

    if total_buys_a > 0:
        result = (total_buys_b - total_buys_a) / total_buys_a
    else:
        result = INFINITY

    return result


def get_probabilities_by_rounded_reported_gain(n_days, ab_size, expected_gain):
    result = {}

    for test in range(1, N_TESTS + 1):
        accrued_gain = get_accrued_gain(n_days, ab_size, expected_gain)

        rounded_gain = round(100 * accrued_gain) / 100
        probability = result.get(rounded_gain, 0)
        probability += 1 / N_TESTS
        result[rounded_gain] = probability

    return result


def get_reliability(ab_size, n_days, expected_gain, accepted_deviation):
    total_good_answers = 0

    for _ in range(N_TESTS):
        accrued_gain = get_accrued_gain(n_days, ab_size, expected_gain)

        if accrued_gain >= expected_gain - accepted_deviation:
            total_good_answers += 1

    return total_good_answers / N_TESTS


def run_relevance_tests(expected_gain, accepted_deviation, min_reliability):

    for ab_size in [0.05 * i for i in range(1, 21)]:

        print("\nAB size = %d%%" % (100 * ab_size))
        print("expected increase = %.2f%%" % (100 * expected_gain))
        print("minimum accepted increase = %.2f%%" % (100 * (expected_gain - accepted_deviation)))
        print("desired reliability = %.2f%%" % (100 * min_reliability))
        print("random repetitions of each AB test = %d" % N_TESTS)

        n_days = 1
        while True:
            reliability = get_reliability(ab_size, n_days, expected_gain, accepted_deviation)

            print("n_days = %d --> reliability = %.2f%%"
                  % (n_days, (100 * reliability) if reliability != INFINITY else "infinity"))

            if reliability >= min_reliability:
                break

            n_days += 1


def run_gain_curve_tests(n_days, ab_size, reported_gain):
    probabilities_by_reported_gain_by_actual_gain = {}
    a_priori_prob = 0.01 / MAX_REASONABLE_GAIN
    reported_gain = round(100 * reported_gain) / 100
    reported_gain_prob = 0

    actual_gain = MIN_REASONABLE_GAIN
    while actual_gain <= MAX_REASONABLE_GAIN:
        reverse_cond_probabilities = get_probabilities_by_rounded_reported_gain(n_days, ab_size, actual_gain)
        probabilities_by_reported_gain_by_actual_gain[actual_gain] = reverse_cond_probabilities
        reported_gain_prob_given_actual_gain = reverse_cond_probabilities.get(reported_gain, 0)
        reported_gain_prob += reported_gain_prob_given_actual_gain * a_priori_prob
        actual_gain += GAIN_CURVE_GRANULARITY

    actual_gain = MIN_REASONABLE_GAIN
    accrued_prob = 0
    while actual_gain <= MAX_REASONABLE_GAIN:
        reverse_cond_prob = probabilities_by_reported_gain_by_actual_gain.get(actual_gain, {}).get(reported_gain, 0)
        numerator = reverse_cond_prob * a_priori_prob
        prob_by_actual_gain_given_reported_gain = numerator / reported_gain_prob
        accrued_prob += prob_by_actual_gain_given_reported_gain
        print("actual gain: %.2f%% ---> probability: %.10f%% ---> accrued probability: %.10f%%"
              % (100 * actual_gain, 100 * prob_by_actual_gain_given_reported_gain, 100 * accrued_prob))
        actual_gain += GAIN_CURVE_GRANULARITY

def main(argv):

    global sessions_per_day
    global buys_per_day

    sessions_per_day = int(argv[1])  # e.g., 131061
    buys_per_day = int(argv[2])  # e.g., 3205

    if argv[0] == "ab_test":
        n_days = int(argv[3])  # e.g., 15
        ab_size = float(argv[4])  # e.g., 0.14
        expected_gain = float(argv[5])  # e.g., 0.06

        print("------- AB test -------\n")
        print("days = %d" % n_days)
        print("AB size = %d%%" % (100 * ab_size))
        print("expected increase = %.2f%%" % (100 * expected_gain))

        simulate_ab_test(n_days, ab_size, expected_gain, should_print=True)

    elif argv[0] == "relevance":
        expected_gain = float(argv[3])  # e.g., 0.06
        accepted_deviation = float(argv[4])  # e.g., 0.1
        min_reliability = float(argv[5])  # e.g., 0.95

        print("------- relevance test -------\n")
        print("minimum reliability = %.2f%%" % (100 * min_reliability))

        run_relevance_tests(expected_gain, accepted_deviation, min_reliability)

    elif argv[0] == "gain_curve":
        n_days = int(argv[3])  # e.g., 15
        ab_size = float(argv[4])  # e.g., 0.14
        reported_gain = float(argv[5])  # e.g., 0.06

        print("------- gain curve -------\n")
        print("days = %d" % n_days)
        print("AB size = %d%%" % (100 * ab_size))
        print("reported gain = %.2f%%" % (100 * reported_gain))

        run_gain_curve_tests(n_days, ab_size, reported_gain)
