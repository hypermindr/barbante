""" Module for text processing.
"""

import math
import mmh3

from barbante.utils.profiling import profile


class BloomFilter:
    """ Class that implements Bloom filters for testing element-set inclusion
        efficiently.
    """

    @profile
    def __init__(self, elements, bloom_filter_size):
        self.bloom_size = bloom_filter_size
        """ Bloom filter bitsize.
        """
        self.n_elements = len(elements)
        """ Number of elements added to the filter.
        """
        self.n_hashes = self._optimal_n_hashes()
        """ Number of hash functions to be used (expected number of bits
            to be set per element).
        """
        self.bloom = self.generate_bloom_filter(elements)
        """ The Bloom filter itself.
        """

    def _calculate_hash(self, element, h_index):
        """ Family of hash functions.
        """
        return mmh3.hash(str(element), h_index) % self.bloom_size

    def _obtain_signature(self, element):
        """ Computes the signature of the element.
        """
        result = []
        for i in range(self.n_hashes):
            result += [self._calculate_hash(element, i)]
        return result

    def _optimal_n_hashes(self):
        """ Calculates the optimal number of hash functions to be used.
        """
        k = math.log(2) * self.bloom_size / self.n_elements
        return int(round(k))

    def get_number_of_hashes(self):
        """ Returns the number of hash functions.
        """
        return self.n_hashes

    def get_false_positive_prob(self, domain_size):
        """ Returns the theoretical probability of false positives.

            Parameters:
                domain_size: the number of elements that may ever be inserted
                    in the set.
        """
        return (1 - (1 - 1.0 / self.bloom_size) ** (self.n_elements * self.n_hashes)) ** \
            self.n_hashes * (domain_size - self.n_elements) / domain_size

    def generate_bloom_filter(self, elements):
        """ Generates the filter based on the elements of the set.

            Parameters:
                elements: A collection with all the elements of the set.

            Returns:
                The Bloom filter as a list of booleans.
        """
        self.bloom = [False] * self.bloom_size

        for element in elements:
            signature = self._obtain_signature(element)
            for index in signature:
                self.bloom[index] = True

        return self.bloom

    def accepts(self, element):
        """ Tests whether a given element belongs to the set.
            False positives may occur.

            Parameters:
                element: The element to be tested.

            Returns:
                True, if the element passes the filter;
                False, otherwise.
        """
        signature = self._obtain_signature(element)
        for index in signature:
            if not self.bloom[index]:
                return False
        return True
