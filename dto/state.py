from decimal import Decimal
from enum import Enum


class State(object):

    def __init__(self, state_id=None, client_id=None, data_source=None, account=None,
                 transaction_id=None, transaction_type=None, asset=None,
                 timestamp=None, amount_of_asset=None, cost_of_asset=None, cost_currency=None, amount_in_fifo=None,
                 cost_in_fifo=None, fifo_currency=None, income=None, cost=None, profit=None, profit_currency=None,
                 dividend=None, withholding_tax=None, dividend_currency=None, currency_pair=None, currency_rate=None,
                 last_update=None):
        self.state_id = state_id
        self.client_id = client_id
        self.data_source = data_source
        self.account = account
        self.transaction_id = transaction_id
        self.transaction_type = transaction_type
        self.asset = asset
        self.timestamp = timestamp
        self.amount_of_asset = amount_of_asset
        self.cost_of_asset = cost_of_asset
        self.cost_currency = cost_currency
        self.amount_in_fifo = amount_in_fifo
        self.cost_in_fifo = cost_in_fifo
        self.fifo_currency = fifo_currency
        self.income = income
        self.cost = cost
        self.profit = profit
        self.profit_currency = profit_currency
        self.dividend = dividend
        self.withholding_tax = withholding_tax
        self.dividend_currency = dividend_currency
        self.currency_pair = currency_pair
        self.currency_rate = currency_rate
        self.last_update = last_update
