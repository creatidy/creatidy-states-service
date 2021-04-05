from unittest import TestCase

from process_controller.states_processor import StatesManager


class TestStatesManager(TestCase):
    def test_get_transactions(self):
        sm = StatesManager()
        t = sm.get_transactions()
        self.assertTrue(len(t) > 0)

    def test_process_transactions(self):
        sm = StatesManager()
        res = sm.process_transactions()
        self.assertTrue(len(res) > 0)
