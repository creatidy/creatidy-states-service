from data_model.grpc_client import Transactions
from data_model.states_db import StatesDatabase
from dto.transaction import Transaction, TransactionType, TransactionsList
from dto.state import State
from decimal import Decimal
from datetime import datetime

class StatesManager:

    transactions: TransactionsList

    def __init__(self):
        self.transactions = TransactionsList()
        self.states_db = StatesDatabase()

    def get_transactions(self) -> TransactionsList:
        transactions_grpc = Transactions()
        for t in transactions_grpc.get_transactions():
            kwargs = {
                'data_source': t.data_source,
                'client_id': t.client_id,
                'account': t.account,
                'transaction_id': t.transaction_id,
                'transaction_type': TransactionType.__getattr__(t.transaction_type),
                'asset': t.asset,
                'timestamp': Decimal(t.timestamp),
                'order_id': t.order_id,
                'order_position': t.order_position,
                'related_account': t.related_account,
                'transaction_sum': Decimal(t.transaction_sum)
            }
            self.transactions.append(Transaction(**kwargs))
        return self.transactions

    def process_transactions(self):
        i = 0
        for t in self.get_transactions():
            s = State()
            if datetime(2020, 1, 1).timestamp() <= t.timestamp <= datetime(2020, 12, 31).timestamp():
                print(datetime.fromtimestamp(t.timestamp))
                i += 1
                s.id = i
                s.client_id = s.client_id
                s.data_source = t.data_source
                s.account = t.account
                s.transaction_id = t.transaction_id
                s.asset = t.asset
                s.timestamp = t.timestamp
                if t.transaction_type == TransactionType.DEPOSIT:
                    last_state = self.states_db.get_latest_state(s.client_id, s.account, s.asset)
                    s.currency = t.asset
                    if last_state is None:
                        s.amount_of_asset = t.account
                    else:
                        raise Exception()
                    s.cost_of_asset = 0
                    s.cost_in_fifo = 0
        pass