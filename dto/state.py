from decimal import Decimal
from enum import Enum


class State(object):

    def __init__(self, **kwargs):
        for field in self.fields():
            if field in kwargs:
                setattr(self, field, kwargs[field])
            else:
                setattr(self, field, None)

    @staticmethod
    def fields():
        return ['id', 'client_id', 'data_source', 'account', 'transaction_id', 'asset', 'timestamp',
                'amount_of_asset', 'cost_of_asset', 'cost_in_fifo', 'currency'
                'income', 'cost', 'profit', 'last_update']
