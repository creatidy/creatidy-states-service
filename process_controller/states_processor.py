from data_model.grpc_client import Transactions
from data_model.states_db import StatesDatabase
from data_model.grpc_client import Rates
from dto.transaction import Transaction, TransactionType, TransactionsList
from dto.state import State

from decimal import Decimal
from datetime import datetime
from prettytable import PrettyTable


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
                'related_asset': t.related_asset,
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
        last_state_id = 1
        self.states_db.delete_all_states()
        table = PrettyTable(header=False)
        all_transactions = self.get_transactions()
        for t in all_transactions:
            s = State()
            if True:  # datetime(2020, 1, 1).timestamp() <= t.timestamp <= datetime(2020, 12, 31).timestamp():
                table.clear()
                table.add_row(t.__dict__.keys())
                table.add_row(t.__dict__.values())
                print(table)

                pln_exchange_rates = Rates()
                transaction_date = datetime.fromtimestamp(float(t.timestamp))

                s.state_id = last_state_id
                last_state_id += 1
                s.client_id = t.client_id
                s.data_source = t.data_source
                s.account = t.account
                s.transaction_id = t.transaction_id
                s.timestamp = t.timestamp
                s.last_update = datetime.now().timestamp()

                last_asset_state = self.states_db.get_latest_state(t.client_id, t.account, t.asset)
                if t.related_asset is not None:
                    last_related_asset_state = self.states_db.get_latest_state(t.client_id, t.account, t.related_asset)
                else:
                    last_related_asset_state = None

                if t.transaction_type == TransactionType.DEPOSIT or \
                        t.transaction_type == TransactionType.WITHDRAWAL or \
                        t.transaction_type == TransactionType.TRANSFER:
                    s.asset = t.asset
                    if last_asset_state.client_id is not None:
                        s.copy_last_state_values(last_asset_state)
                        s.amount_of_asset += t.transaction_sum
                    else:
                        s.amount_of_asset = t.transaction_sum
                    self.states_db.insert_state(s)
                    table.clear()
                    table.add_row(s.__dict__.keys())
                    table.add_row(s.__dict__.values())
                    print(table)

                elif t.transaction_type == TransactionType.FEE:
                    s.asset = t.asset
                    if last_asset_state.client_id is not None:
                        s.copy_last_state_values(last_asset_state)
                        s.amount_of_asset += t.transaction_sum
                    else:
                        s.amount_of_asset = t.transaction_sum
                    self.states_db.insert_state(s)
                    table.clear()
                    table.add_row(s.__dict__.keys())
                    table.add_row(s.__dict__.values())
                    print(table)

                elif t.transaction_type == TransactionType.COMMISSION:
                    s.asset = t.asset
                    if last_asset_state.client_id is not None:
                        s.copy_last_state_values(last_asset_state)
                        s.amount_of_asset += t.transaction_sum
                    else:
                        s.amount_of_asset = t.transaction_sum
                    self.states_db.insert_state(s)
                    table.clear()
                    table.add_row(s.__dict__.keys())
                    table.add_row(s.__dict__.values())
                    print(table)

                elif t.transaction_type == TransactionType.DIVIDEND:
                    if last_related_asset_state.amount_of_asset is not None:
                        s.copy_last_state_values(last_related_asset_state)
                        s.asset = t.related_asset
                        s.dividend = Decimal(
                            pln_exchange_rates.get_rates_pln(t.asset, transaction_date).value) * t.transaction_sum
                        s.withholding_tax = 0.0
                        s.dividend_currency = 'PLN'
                        self.states_db.insert_state(s)
                        table.clear()
                        table.add_row(s.__dict__.keys())
                        table.add_row(s.__dict__.values())
                    else:
                        raise Exception("Dividend from non-existing asset!")
                    # Adding to payed dividend to an account
                    if last_asset_state.amount_of_asset is not None:
                        s.copy_last_state_values(last_asset_state)
                        s.asset = t.asset
                        s.amount_of_asset += t.transaction_sum
                        s.state_id = last_state_id
                        last_state_id += 1
                        self.states_db.insert_state(s)
                    table.add_row(s.__dict__.values())
                    print(table)

                elif t.transaction_type == TransactionType.TAX:
                    if last_related_asset_state.dividend > 0:
                        s.copy_last_state_values(last_related_asset_state)
                        s.asset = t.related_asset
                        s.withholding_tax = Decimal(
                            pln_exchange_rates.get_rates_pln(t.asset, transaction_date).value) * t.transaction_sum
                        if s.dividend_currency != 'PLN':
                            raise "Unexpected currency type!"
                        s.state_id = last_related_asset_state.state_id
                        self.states_db.update_state(s)
                        table.clear()
                        table.add_row(s.__dict__.keys())
                        table.add_row(s.__dict__.values())
                    else:
                        raise Exception("Tax for dividend from non-existing asset!")
                    # Paying a tax for the dividend from an account
                    if last_asset_state.amount_of_asset is not None:
                        s.copy_last_state_values(last_asset_state)
                        s.asset = t.asset
                        s.amount_of_asset += t.transaction_sum
                        s.state_id = last_state_id
                        last_state_id += 1
                        self.states_db.insert_state(s)
                    table.add_row(s.__dict__.values())
                    print(table)

                elif t.transaction_type == TransactionType.TRADE:
                    s.asset = t.asset
                    if last_asset_state.client_id is not None:
                        s.copy_last_state_values(last_asset_state)
                        s.amount_of_asset += t.transaction_sum
                    else:
                        s.amount_of_asset = t.transaction_sum
                    self.states_db.insert_state(s)
                    table.clear()
                    table.add_row(s.__dict__.keys())
                    table.add_row(s.__dict__.values())
                    print(table)

                    # Costs handling
                    # if t.asset != t.related_asset:
                    #     pass

                else:
                    pass

        return True
