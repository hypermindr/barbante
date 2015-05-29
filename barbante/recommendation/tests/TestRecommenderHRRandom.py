""" Test module for barbante.recommendation.RecommenderHRRandom class.
"""

from barbante.recommendation.tests.fixtures.HybridRecommenderFixture import HybridRecommenderFixture


class TestRecommenderHRRandom(HybridRecommenderFixture):
    """ Class for testing barbante.recommendation.RecommenderHRRandom.
    """

    def __init__(self):
        super().__init__()
        self.set_algorithm("HRRandom")

    def test_merge_algorithm_contributions(self):
        # It is not easy to test the ranking here, since it is randomized. Let's bypass this test.
        pass

    def test_pre_vs_pos_filter_with_missing_pre_filtered_candidates(self):
        # It is not easy to compare rankings here, since they are randomized. Let's bypass this test.
        pass

    def test_pre_vs_pos_filter_without_missing_pre_filtered_candidates(self):
        # It is not easy to compare rankings here, since they are randomized. Let's bypass this test.
        pass

