from unittest import TestCase

from process_controller.states_processor import StatesManager
from data_model.states_db import StatesDatabase


class TestStatesManager(TestCase):
    def test_get_transactions(self):
        sm = StatesManager()
        t = sm.get_transactions()
        self.assertTrue(len(t) > 0)

    def test_process_transactions(self):
        sm = StatesManager()
        res = sm.process_transactions()
        self.assertTrue(res)

    def test_process_costs(self):
        db = StatesDatabase()
        db.load_test_db()
        sm = StatesManager()
        res = sm.process_costs()
        self.assertTrue(res)
