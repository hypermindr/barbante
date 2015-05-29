import unittest
import logging
import time
import os

from barbante.utils.profiling import profile
from barbante.utils.profiling import Reporter


class Test(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(Test, self).__init__(*args, **kwargs)
        self._logger = logging.getLogger(__name__)

    def setUp(self):
        """
        Set up test fixtures.
        """
        unittest.TestCase.setUp(self)

    def tearDown(self):
        """
        Tear down test fixtures.
        """
        unittest.TestCase.tearDown(self)

    @profile
    def foo(self):
        time.sleep(0.1)

    @profile
    def bar(self):
        time.sleep(0.2)

        @profile
        def closure():
            time.sleep(0.3)

        closure()

    def test_log(self):
        self._logger.setLevel(logging.INFO)
        self._logger.info("tests foo log")
        self.foo()

    def test_nolog(self):
        self._logger.setLevel(logging.WARN)
        self._logger.info("tests foo nolog")
        self.foo()

    def test_log_bar(self):
        self._logger.setLevel(logging.INFO)
        self._logger.info("tests bar log")
        self.bar()

    def test_reporter(self):
        with Reporter.profile('Testing reporter'):
            time.sleep(0.01)
        with open(os.devnull, "w") as f:
            Reporter.dump(file=f)
