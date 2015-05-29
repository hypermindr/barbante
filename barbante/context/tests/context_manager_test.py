
import threading
from functools import partial

import nose.tools
from tornado.testing import AsyncTestCase

from barbante.context.context_manager import global_context_manager as gcm, new_context, get_context


class ContextManagerTest(AsyncTestCase):

    def setUp(self):
        """ Make sure the stack is reset between tests.

        Note: usually I prefer to use `setup()` over `setUp()` as Nose supports both names and `setup()` is more
        Pythonic, but for some reason when inheriting from AsyncTestCase, `setup()` is never called.
        """
        super().setUp()
        gcm.reset()

    def test_stacking_requests(self):
        """ The arrival of a second request in the same thread should stack a new context over the first request's
            context.
        """
        tid1 = 'd551573a-01dc-41b2-b197-ea8afb7fbac1'.replace('-', '')
        tid2 = '6c006821-0cb1-42e8-8ab1-fb6e059cabab'.replace('-', '')

        def bar():
            nose.tools.eq_(len(gcm.stack), 2, "Context stack length is wrong (got {}, should be 2)".format(
                len(gcm.stack)))
            nose.tools.eq_(str(gcm.get_context().tracer_id), tid2, "Tracer id does not match")
            # Stop Tornado loop:
            self.stop()

        def foo():
            nose.tools.eq_(len(gcm.stack), 1, "Context stack length is wrong (got {}, should be 1)".format(
                len(gcm.stack)))
            nose.tools.eq_(str(gcm.get_context().tracer_id), tid1, "Tracer id does not match")
            # Wrap execution of bar inside the second context:
            gcm.run_with_new_context(bar, tid2)

        # Wrap execution of foo inside the first context:
        p = partial(gcm.run_with_new_context, foo, tid1)
        # Schedule execution of foo:
        self.io_loop.add_callback(p)

        # Wait for stop() to be called:
        self.wait()

        nose.tools.eq_(len(gcm.stack), 0, "Context stack length is wrong (got {}, should be 0)".format(
            len(gcm.stack)))

    def test_concurrent_threads(self):
        """ Each thread should have its own context stack.
        """
        tid1 = 'd551573a-01dc-41b2-b197-ea8afb7fbac1'.replace('-', '')
        tid2 = '6c006821-0cb1-42e8-8ab1-fb6e059cabab'.replace('-', '')
        stacks = []
        tids = []

        def append_to_context():
            # print('@gcs=0x{:0X} @gcs.tracer_id=0x{:0X}'.format(id(gcs), id(gcs.tracer_id)))
            stacks.append(id(gcm.stack))
            tids.append(gcm.get_context().tracer_id)

        gcm.run_with_new_context(append_to_context, tracer_id=tid1)

        p = partial(gcm.run_with_new_context, append_to_context, tracer_id=tid2)
        t = threading.Thread(target=p)
        t.start()
        t.join()

        nose.tools.assert_not_equal(stacks[0], stacks[1], "Both threads are sharing the same stack")
        nose.tools.assert_not_equal(tids[0], tids[1], "Tracer ids are the same")

    def test_context_creation_and_retrieval(self):
        """ Context should be created and retrieved successfully.
        """
        tracer_id = 'd551573a-01dc-41b2-b197-ea8afb7fbac1'.replace('-', '')

        with new_context(tracer_id=tracer_id):
            context = get_context()
            nose.tools.eq_(tracer_id, str(context.tracer_id))
