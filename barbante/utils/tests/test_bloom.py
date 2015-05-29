""" Test module for barbante.utils.bloom.
"""

import nose.tools
import random

import barbante.utils.bloom as bloom
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


def test_bloom():
    """ Tests the Bloom filter with thousands of random entries.
    """
    all_elements = 100000  # 100k elements in the domain
    bloom_filter_size = 20000  # 20k bits
    number_of_elements = 3650  # one year worth of elements @ 10 elements/day

    log.debug("Generating %d random elements..." % number_of_elements)

    selected_elements = set()
    while len(selected_elements) < number_of_elements:
        selected_elements.add(random.randint(0, all_elements - 1))

    bloom_filter = bloom.BloomFilter(selected_elements, bloom_filter_size)

    false_positive_prob = bloom_filter.get_false_positive_prob(all_elements)

    log.debug(
        "Running the simulation with all %d elements in the hypothetical base..." % all_elements)

    accepted_list = []
    for element in range(all_elements):
        if bloom_filter.accepts(element):
            accepted_list += [element]

    false_positive_count = len(accepted_list) - number_of_elements
    false_positive_ratio = false_positive_count / (all_elements - number_of_elements)

    log.debug("Number of elements in the domain = %d" %
              all_elements)
    log.debug("Number of elements in the set = %d" %
              number_of_elements)
    log.debug("Number of hash functions = %d" %
              bloom_filter.get_number_of_hashes())
    log.debug("bloom_filter_size filter size = %d bits" %
              bloom_filter_size)
    log.debug("Filter size over number of elements ratio = %.2f" %
              (1.0 * bloom_filter_size / number_of_elements))
    log.debug("Theoretical probability of false positives = %.2f%%" %
              (100 * false_positive_prob))
    log.debug("Obtained false positives = %d (%.2f%%)" %
              (false_positive_count, (100.0 * false_positive_ratio)))

    nose.tools.ok_(false_positive_ratio < false_positive_prob + 0.05,
                   "Too much false positives")  # allowing for a 5% slack
