""" Test fixture for user-based recommendations.
"""

import nose.tools

import barbante.maintenance.user_templates as ut
from barbante.recommendation.tests.fixtures.RecommenderFixture import RecommenderFixture
import barbante.maintenance.tasks as tasks
import barbante.tests as tests


class UserBasedRecommenderFixture(RecommenderFixture):
    """ Class for testing barbante.recommendation.UserBasedRecommender subclasses.
    """

    def __init__(self):
        super().__init__()

    def setup(self):
        super().setup()
        ut.generate_templates(self.session_context)

    def teardown(self):
        super().teardown()

    def test_out_boost(self):
        """ Tests the effect of applying an out-boost on recommendations for some activity types.
            It applies to all user-based heuristics.
        """
        target = "u_eco_2"
        session = tests.init_session(user_id=target, algorithm=self.algorithm)
        recommender = session.get_recommender()

        # Determines the index of the first actual value in the score tuples
        # produced by the recommender (note that hybrid recommenders use the first
        # position to indicate the algorithm number)
        if recommender.is_hybrid():
            start_index = 1
        else:
            start_index = 0

        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        former_top_product = recommendations[0][1]
        old_strength = recommendations[0][0]

        # Meta-test
        boost_activity_type = None
        out_boost = 1
        for boost_activity_type, out_boost in self.session_context.out_boost_by_activity.items():
            if out_boost != 1:
                break
        nose.tools.ok_(out_boost > 1, "Weak text fixture. There should be at least one out-boosted activity.")

        # Saves out-boosted activities for all templates who had consumed the former top product
        templates = [t[1] for t in session.user_templates]
        for template in templates:
            recent_product_activities_of_template = session.recent_activities_by_product_by_template_user.get(
                template, {})
            if former_top_product in recent_product_activities_of_template:
                activity = {"external_user_id": template,
                            "external_product_id": former_top_product,
                            "activity": boost_activity_type,
                            "created_at": session.get_present_date()}
                tasks.update_summaries(session, activity)

        session.refresh()
        recommendations = recommender.recommend(100)
        nose.tools.ok_(len(recommendations) > 0, "No recommendations were returned!")
        new_strength = None
        for rec in recommendations:
            if rec[1] == former_top_product:
                new_strength = rec[0]
                break

        nose.tools.ok_(new_strength is not None,
                       "The former top recommendation should have been recommended again.")
        for i in range(start_index, len(new_strength)):
            old_strength_value = old_strength[i]
            new_strength_value = new_strength[i]
            nose.tools.ok_(abs(new_strength_value / old_strength_value - out_boost) < tests.FLOAT_DELTA,
                           "Incorrect application of the activity in-boost")

