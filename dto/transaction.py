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
    COMMISSION = 9
    STOCK_SPLIT = 10


class Transaction(object):

    def __init__(self, data_source=None, client_id=None, account=None, transaction_id=None, related_asset=None,
                 transaction_type=None, asset=None, timestamp=None, order_id=None, order_position=None,
                 related_account=None, transaction_sum=None, split=None):
        self.data_source = data_source
        self.client_id = client_id
        self.account = account
        self.transaction_id = transaction_id
        self.related_asset = related_asset
        self.transaction_type = transaction_type
        self.asset = asset
        self.timestamp = timestamp
        self.order_id = order_id
        self.order_position = order_position
        self.related_account = related_account
        self.transaction_sum = transaction_sum
        self.split = split


class TransactionsList(list):

    def __init__(self):
        super().__init__()

