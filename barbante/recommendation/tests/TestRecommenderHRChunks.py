""" Test module for barbante.recommendation.RecommenderHRChunks class.
"""

import nose.tools

import barbante.tests as tests
from barbante.recommendation.tests.fixtures.HybridRecommenderFixture import HybridRecommenderFixture


class TestRecommenderHRChunks(HybridRecommenderFixture):
    """ Class for testing barbante.recommendation.RecommenderHRChunks.
    """

    def __init__(self):
        super().__init__()
        self.set_algorithm('HRChunks')

    def test_merge_algorithm_contributions(self):
        """ Tests the merge based on fixed slices.
        """
        recommendations = {"UBCF": [[[50], "UBCF_1"],
                                    [[30], "UBCF_2"],
                                    [[10], "UBCF_3"],
                                    [[5], "UBCF_4"],
                                    [[2], "UBCF_5"]],

                           "PBCF": [[[50], "PBCF_1"],
                                    [[30], "PBCF_2"],
                                    [[10], "PBCF_3"],
                                    [[5], "PBCF_4"]],

                           "CB": [[[50], "CB_1"],
                                  [[40], "CB_2"],
                                  [[30], "CB_3"],
                                  [[20], "CB_4"],
                                  [[10], "CB_5"],
                                  [[9], "CB_6"],
                                  [[8], "CB_7"],
                                  [[7], "CB_8"],
                                  [[4], "CB_9"]],

                           "POP": [[[50], "POP_1"],
                                   [[30], "POP_2"],
                                   [[10], "POP_3"],
                                   [[5], "POP_4"],
                                   [[4], "POP_5"],
                                   [[3], "POP_6"],
                                   [[4], "POP_7"]]}

        session = tests.init_session(user_id="u_eco_1", algorithm=self.algorithm)
        recommender = session.get_recommender()

        merged_recommendations = recommender.merge_algorithm_contributions(recommendations, 20)
        products_rank = [rec[1] for rec in merged_recommendations]
        nose.tools.eq_(products_rank,
                       ['UBCF_1', 'PBCF_1', 'CB_1', 'CB_2',
                        'UBCF_2', 'PBCF_2', 'CB_3', 'CB_4',
                        'UBCF_3', 'PBCF_3', 'CB_5', 'CB_6',
                        'UBCF_4', 'PBCF_4', 'CB_7', 'CB_8',
                        'UBCF_5',           'CB_9'],
                       "Wrong rank after merge")
