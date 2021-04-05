from decimal import Decimal
from enum import Enum


class TransactionType(Enum):
    DEPOSIT = 1
    WITHDRAWAL = 2
    TRADE = 3  # include CONVERSION
    FEE = 4  # include COMMISSION, BANK CHARGE
    INTEREST = 5
    DIVIDEND = 6
    TRANSFER = 7
    TAX = 8


class Transaction(object):

    def __init__(self, **kwargs):
        for field in self.fields():
            if field in kwargs:
                setattr(self, field, kwargs[field])
            else:
                setattr(self, field, None)

    @staticmethod
    def fields():
        return ['data_source', 'client_id', 'account', 'transaction_id', 'transaction_type', 'asset',
                'timestamp', 'order_id', 'order_position', 'related_account', 'transaction_sum']


class TransactionsList(list):

    def __init__(self):
        super().__init__()
        self.list = []
