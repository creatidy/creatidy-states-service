from decimal import Decimal
from enum import Enum


class State(object):

    def __init__(self, state_id=None, client_id=None, data_source=None, account=None, transaction_id=None, asset=None,
                 timestamp=None, amount_of_asset=0, cost_of_asset=0, cost_currency=None, amount_in_fifo=0,
                 cost_in_fifo=0, fifo_currency=None, income=0, cost=0, profit=0, profit_currency=None,
                 dividend=0, withholding_tax=0, dividend_currency=None, last_update=None):
        self.state_id = state_id
        self.client_id = client_id
        self.data_source = data_source
        self.account = account
        self.transaction_id = transaction_id
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
        self.last_update = last_update
        
    def copy_last_state_values(self, last_state):
        self.amount_of_asset = last_state.amount_of_asset
        self.cost_of_asset = last_state.cost_of_asset
        self.cost_currency = last_state.cost_currency
        self.amount_in_fifo = last_state.amount_in_fifo
        self.cost_in_fifo = last_state.cost_in_fifo
        self.fifo_currency = last_state.fifo_currency
        self.income = last_state.income
        self.cost = last_state.cost
        self.profit = last_state.profit
        self.profit_currency = last_state.profit_currency
        self.dividend = last_state.dividend
        self.withholding_tax = last_state.withholding_tax
        self.dividend_currency = last_state.dividend_currency
