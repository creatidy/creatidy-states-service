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
        self.prettytable = PrettyTable(header=False)

    # TODO: INTERESTS/FEE

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
                'transaction_sum': Decimal(t.transaction_sum),
                'split': t.split
            }
            self.transactions.append(Transaction(**kwargs))
        return self.transactions

    def process_transactions(self):
        last_state_id = 1
        self.states_db.delete_all_states()
        all_transactions = self.get_transactions()
        for t in all_transactions:
            if t.timestamp < datetime(2024, 1, 1).timestamp():
                self.prettytable.clear()
                self.prettytable.add_row(t.__dict__.keys())
                self.prettytable.add_row(t.__dict__.values())
                print(self.prettytable)

                pln_exchange_rates = Rates()
                transaction_date = datetime.fromtimestamp(float(t.timestamp))

                initial_kwargs = {
                    'state_id': last_state_id,
                    'client_id': t.client_id,
                    'data_source': t.data_source,
                    'account': t.account,
                    'transaction_id': t.transaction_id,
                    'transaction_type': t.transaction_type.name,
                    'timestamp': datetime.fromtimestamp(float(t.timestamp)),
                    'last_update': datetime.now()
                }
                s = State(**initial_kwargs)
                last_state_id += 1

                last_asset_state = self.states_db.get_latest_state(t.client_id, t.account, t.asset)
                last_related_asset_state = self.states_db.get_latest_state(t.client_id, t.account, t.related_asset) if t.related_asset is not None else None
                last_related_asset_trade_state = self.states_db.get_latest_state(t.client_id, t.account, t.asset, 'TRADE') if t.related_asset is not None else None
                if t.transaction_type == TransactionType.DEPOSIT or \
                        t.transaction_type == TransactionType.WITHDRAWAL or \
                        t.transaction_type == TransactionType.TRANSFER:
                    s.asset = t.asset
                    if last_asset_state is not None:
                        s.amount_of_asset = last_asset_state.amount_of_asset + t.transaction_sum
                    else:
                        s.amount_of_asset = t.transaction_sum
                    self.states_db.insert_state(s)
                    self.prettytable.clear()
                    self.prettytable.add_row(s.__dict__.keys())
                    self.prettytable.add_row(s.__dict__.values())
                    print(self.prettytable)

                elif t.transaction_type == TransactionType.FEE:
                    s.asset = t.asset
                    if last_asset_state is not None:
                        s.amount_of_asset = last_asset_state.amount_of_asset + t.transaction_sum
                    else:
                        s.amount_of_asset = t.transaction_sum
                    self.states_db.insert_state(s)
                    self.prettytable.clear()
                    self.prettytable.add_row(s.__dict__.keys())
                    self.prettytable.add_row(s.__dict__.values())
                    print(self.prettytable)

                # elif t.transaction_type == TransactionType.COMMISSION:
                #     s.asset = t.asset
                #     if last_asset_state.client_id is not None:
                #         s.copy_last_state_values(last_asset_state)
                #         s.amount_of_asset += t.transaction_sum
                #     else:
                #         s.amount_of_asset = t.transaction_sum
                #     self.states_db.insert_state(s)
                #     self.prettytable.clear()
                #     self.prettytable.add_row(s.__dict__.keys())
                #     self.prettytable.add_row(s.__dict__.values())
                #     print(self.prettytable)

                elif t.transaction_type == TransactionType.DIVIDEND:
                    if last_related_asset_state.amount_of_asset is not None:
                        s.amount_of_asset = last_related_asset_state.amount_of_asset
                        s.asset = t.related_asset
                        s.dividend = Decimal(t.transaction_sum)
                        s.currency_pair = f'{t.asset}PLN'
                        s.currency_rate = pln_exchange_rates.get_rates_pln(t.asset, transaction_date).value
                        s.dividend_currency = t.asset
                        s.transaction_type = 'DIVIDEND'
                        self.states_db.insert_state(s)
                        self.prettytable.clear()
                        self.prettytable.add_row(s.__dict__.keys())
                        self.prettytable.add_row(s.__dict__.values())
                    else:
                        raise Exception("Dividend from non-existing asset!")
                    # Adding to paid dividend to an account
                    if last_asset_state.amount_of_asset is not None:
                        s2 = State(**initial_kwargs)
                        s2.amount_of_asset = last_asset_state.amount_of_asset
                        s2.asset = t.asset
                        s2.amount_of_asset += t.transaction_sum
                        s2.state_id = last_state_id
                        s2.currency_pair = None
                        s2.currency_rate = None
                        last_state_id += 1
                        self.states_db.insert_state(s2)
                        self.prettytable.add_row(s2.__dict__.values())
                    print(self.prettytable)

                elif t.transaction_type == TransactionType.TAX:
                    if last_related_asset_state.dividend is not None and last_related_asset_state.dividend > 0:
                        s.amount_of_asset = last_related_asset_state.amount_of_asset
                        s.asset = t.related_asset
                        s.state_id = last_related_asset_state.state_id
                        s.dividend = last_related_asset_state.dividend
                        if last_related_asset_state.dividend_currency == t.asset:
                            s.dividend_currency = last_related_asset_state.dividend_currency
                        else:
                            raise "Unexpected currency type!"
                        s.withholding_tax = Decimal(t.transaction_sum)
                        s.currency_pair = last_related_asset_state.currency_pair
                        s.currency_rate = last_related_asset_state.currency_rate
                        s.transaction_type = 'TAX'
                        self.states_db.update_state(s)
                        self.prettytable.clear()
                        self.prettytable.add_row(s.__dict__.keys())
                        self.prettytable.add_row(s.__dict__.values())
                    else:
                        print("VERIFY! Tax for dividend from non-existing asset!? OR TAX")
                        # TODO: Verify if it is a tax for dividend from non-existing asset or just a tax
                        s.asset = t.asset
                        if last_asset_state is not None:
                            s.amount_of_asset = last_asset_state.amount_of_asset + t.transaction_sum
                            if last_related_asset_state is not None and last_related_asset_state.cost_currency is not None:
                                s.cost_of_asset = last_related_asset_state.cost_of_asset
                                s.cost_currency = last_related_asset_state.cost_currency
                            elif last_related_asset_trade_state is not None:
                                s.cost_of_asset = last_related_asset_trade_state.cost_of_asset
                                s.cost_currency = last_related_asset_trade_state.cost_currency
                        else:
                            s.amount_of_asset = t.transaction_sum
                        s.transaction_type = 'TAX'
                        self.states_db.insert_state(s)
                        self.prettytable.clear()
                        self.prettytable.add_row(s.__dict__.keys())
                        self.prettytable.add_row(s.__dict__.values())

                    # Paying a tax for the dividend from an account
                    if last_asset_state.amount_of_asset is not None:
                        s2 = State(**initial_kwargs)
                        s2.amount_of_asset = last_asset_state.amount_of_asset
                        s2.asset = t.asset
                        s2.amount_of_asset += t.transaction_sum
                        s2.state_id = last_state_id
                        s2.currency_pair = None
                        s2.currency_rate = None
                        last_state_id += 1
                        s2.transaction_type = 'TAX'
                        self.states_db.insert_state(s2)
                        self.prettytable.add_row(s2.__dict__.values())
                    print(self.prettytable)

                elif t.transaction_type == TransactionType.TRADE or t.transaction_type == TransactionType.COMMISSION:
                    s.asset = t.asset
                    if last_asset_state is not None:
                        s.amount_of_asset = last_asset_state.amount_of_asset + t.transaction_sum
                        if last_related_asset_state is not None and last_related_asset_state.cost_currency is not None:
                            s.cost_of_asset = last_related_asset_state.cost_of_asset
                            s.cost_currency = last_related_asset_state.cost_currency
                        elif last_related_asset_trade_state is not None:
                            s.cost_of_asset = last_related_asset_trade_state.cost_of_asset
                            s.cost_currency = last_related_asset_trade_state.cost_currency
                    else:
                        s.amount_of_asset = t.transaction_sum
                    self.states_db.insert_state(s)
                    self.prettytable.clear()
                    self.prettytable.add_row(s.__dict__.keys())
                    self.prettytable.add_row(s.__dict__.values())

                    # Costs handling
                    if t.asset == t.related_asset:  # stocks, options, etc.
                        if last_asset_state is not None:
                            if t.transaction_sum > 0:  # buying
                                s.amount_in_fifo = t.transaction_sum
                            elif t.transaction_sum < 0:  # selling
                                s.amount_in_fifo = t.transaction_sum
                            else:
                                raise "Error: zero in data"
                        else:
                            s.amount_in_fifo = t.transaction_sum
                        self.states_db.update_state(s)
                        self.prettytable.add_row(s.__dict__.values())
                    else:  # fiat
                        if last_related_asset_state is None:
                            last_related_asset_state = State()
                        if t.asset == 'PLN':
                            last_related_asset_state.currency_pair = f'PLNPLN'
                            last_related_asset_state.currency_rate = 1.0
                        else:
                            last_related_asset_state.currency_pair = f'{t.asset}PLN'
                            last_related_asset_state.currency_rate = pln_exchange_rates.get_rates_pln(t.asset, transaction_date).value
                        if last_related_asset_state is None:
                            raise "Costs for non-existing asset!"
                        if t.transaction_sum < 0:  # buying
                            if last_related_asset_state.cost_of_asset is None:  # first buying
                                last_related_asset_state.cost_of_asset = t.transaction_sum
                                last_related_asset_state.cost_in_fifo = t.transaction_sum
                            else:
                                last_related_asset_state.cost_of_asset += t.transaction_sum
                                if last_related_asset_state.cost_in_fifo is None:
                                    last_related_asset_state.cost_in_fifo = t.transaction_sum
                                else:
                                    last_related_asset_state.cost_in_fifo += t.transaction_sum
                            last_related_asset_state.cost_currency = t.asset
                            last_related_asset_state.fifo_currency = t.asset
                        elif t.transaction_sum > 0:  # selling
                            if last_related_asset_state.client_id is None:
                                print(t.related_asset)  # TODO: test it
                            else:
                                if last_related_asset_state.cost_of_asset is None:
                                    last_related_asset_state.cost_of_asset = 0
                                last_related_asset_state.cost_of_asset += t.transaction_sum
                            if last_related_asset_state.cost_in_fifo is None:
                                last_related_asset_state.cost_in_fifo = t.transaction_sum
                            else:
                                last_related_asset_state.cost_in_fifo += t.transaction_sum
                        else:
                            raise "Error: zero in data"
                        self.states_db.update_state(last_related_asset_state)
                        self.prettytable.add_row(last_related_asset_state.__dict__.values())
                    print(self.prettytable)

                elif t.transaction_type == TransactionType.STOCK_SPLIT:
                    split_ratio = tuple(t.split.split(':'))
                    if len(split_ratio) != 2:
                        raise "Error: wrong split ratio"
                    if int(split_ratio[0]) == 0 or int(split_ratio[1]) == 0:
                        raise "Error: wrong split ratio"
                    self.states_db.split_asset(t.asset, split_ratio)
                else:
                    pass
        return True

    def process_costs(self):
        processed_states = self.states_db.get_not_processed_states()
        processed_state: State
        for processed_state in processed_states:
            processed_state.income = 0
            processed_state.cost = 0
            costs_states = self.states_db.get_fifo_costs_states(processed_state.client_id,
                                                                processed_state.account,
                                                                processed_state.asset)
            cost_state: State
            self.prettytable.clear()
            self.prettytable.add_row(processed_state.__dict__.keys())
            self.prettytable.add_row(processed_state.__dict__.values())
            for cost_state in costs_states:
                if processed_state.amount_in_fifo < 0:
                    if cost_state.amount_in_fifo > -processed_state.amount_in_fifo:
                        if cost_state.amount_in_fifo == 0:
                            print()
                            continue
                        else:
                            self.process_cost(processed_state, cost_state)
                    else:  # cost could be calculated only from this record
                        self.process_cost(processed_state, cost_state)
                elif processed_state.amount_in_fifo > 0:
                    raise "Problem!"
                else:
                    print(self.prettytable)
        return True

    def process_cost(self, processed_state: State, cost_state: State):
        self.prettytable.add_row(cost_state.__dict__.values())
        amount_in_fifo = cost_state.amount_in_fifo
        rest = -processed_state.amount_in_fifo
        if rest > amount_in_fifo:
            processed_state.amount_in_fifo += amount_in_fifo
            cost_state.amount_in_fifo = 0
            share = amount_in_fifo / rest
            processed_state.income += processed_state.cost_in_fifo * share
            processed_state.cost_in_fifo = processed_state.cost_in_fifo * (1 - share)
            processed_state.cost += cost_state.cost_in_fifo
            cost_state.cost_in_fifo = 0
        elif rest < amount_in_fifo:
            cost_state.amount_in_fifo -= rest
            processed_state.amount_in_fifo = 0
            processed_state.income += processed_state.cost_in_fifo
            processed_state.cost_in_fifo = 0
            share = rest / amount_in_fifo
            processed_state.cost += cost_state.cost_in_fifo * share
            cost_state.cost_in_fifo = cost_state.cost_in_fifo * (1 - share)
        else:
            cost_state.amount_in_fifo = 0
            processed_state.amount_in_fifo = 0
            processed_state.income += processed_state.cost_in_fifo
            processed_state.cost_in_fifo = 0
            processed_state.cost += cost_state.cost_in_fifo
            cost_state.cost_in_fifo = 0
        processed_state.profit = processed_state.income + processed_state.cost
        processed_state.profit_currency = processed_state.cost_currency
        self.states_db.update_state(processed_state)
        self.states_db.update_state(cost_state)
        self.prettytable.add_row(processed_state.__dict__.values())
        self.prettytable.add_row(cost_state.__dict__.values())