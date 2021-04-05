from unittest import TestCase
from data_model.grpc_client import Rates
from datetime import datetime

class TestRates(TestCase):
    def test_get_rates(self):
        r = Rates()
        t = datetime(2020, 1, 1)
        res = r.get_rates_pln('USD', t)
        self.assertEqual(3.7977, res.value)
