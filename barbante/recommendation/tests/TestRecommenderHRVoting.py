""" Test module for barbante.recommendation.RecommenderHRVoting class.
"""

import nose.tools

from barbante.recommendation.tests.fixtures.HybridRecommenderFixture import HybridRecommenderFixture
import barbante.tests as tests


class TestRecommenderHRVoting(HybridRecommenderFixture):
    """ Class for testing barbante.recommendation.RecommenderHRVoting.
    """

    def __init__(self):
        super().__init__()
        self.set_algorithm("HRVoting")

    def test_merge_algorithm_contributions(self):
        """ Tests the merge based on fixed slices without bonuses.
        """
        recommendations_by_alg = {"UBCF": [[[50], "a"],
                                           [[30], "b"],
                                           [[10], "c"],
                                           [[5], "d"],
                                           [[2], "e"]],

                                  "PBCF": [[[50], "f"],
                                           [[30], "b"],
                                           [[10], "d"],
                                           [[5], "g"]],

                                  "CB": [[[50], "g"],
                                         [[40], "h"],
                                         [[30], "c"],
                                         [[20], "d"],
                                         [[10], "i"],
                                         [[9], "j"],
                                         [[8], "k"],
                                         [[7], "l"],
                                         [[4], "m"]],

                                  "POP": [[[50], "POP_1"],
                                          [[30], "POP_2"],
                                          [[10], "POP_3"],
                                          [[5], "POP_4"],
                                          [[4], "POP_5"],
                                          [[3], "POP_6"],
                                          [[4], "POP_7"]]}

        session = tests.init_session(user_id="u_eco_1", algorithm=self.algorithm)
        recommender = session.get_recommender()
        merged_recommendations = recommender.merge_algorithm_contributions(recommendations_by_alg, 20)
        products_rank = [rec[1] for rec in merged_recommendations]
        nose.tools.eq_(products_rank,
                       ['f', 'g', 'd', 'c', 'h', 'b', 'i', 'a', 'j', 'k', 'l', 'm', 'e'],
                       "Wrong rank after merge")
