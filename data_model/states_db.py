from typing import Optional

import mysql.connector
import configparser
from dto.state import State
from decimal import Decimal
from datetime import datetime

STATES_TABLE = "`ib_states`"

class StatesDatabase:

    def __init__(self, cfg='config/config.ini'):
        config = configparser.ConfigParser()
        config.read(cfg)

        user = config['CreatidyDB']['USER']
        password = config['CreatidyDB']['PASSWORD']
        host = config['CreatidyDB']['HOST']
        database = config['CreatidyDB']['DATABASE']
        self.cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)

    def read_query(self, query, params) -> list:
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query, params)
        kwargs = {}
        states = []
        if cursor.rowcount > 0:
            for item in cursor:
                kwargs = {
                    'state_id': item[0],
                    'client_id': item[1],
                    'data_source': item[2],
                    'account': item[3],
                    'transaction_id': item[4],
                    'transaction_type': item[5],
                    'asset': item[6],
                    'timestamp': item[7],
                    'amount_of_asset': Decimal(item[8]) if item[8] is not None else None,
                    'cost_of_asset': Decimal(item[9]) if item[9] is not None else None,
                    'cost_currency': item[10],
                    'amount_in_fifo': Decimal(item[11]) if item[11] is not None else None,
                    'cost_in_fifo': Decimal(item[12]) if item[12] is not None else None,
                    'fifo_currency': item[13],
                    'income': Decimal(item[14]) if item[14] is not None else None,
                    'cost': Decimal(item[15]) if item[15] is not None else None,
                    'profit': Decimal(item[16]) if item[16] is not None else None,
                    'profit_currency': item[17] if item[17] is not None else None,
                    'dividend': Decimal(item[18]) if item[18] is not None else None,
                    'withholding_tax': Decimal(item[19]) if item[19] is not None else None,
                    'dividend_currency': item[20] if item[20] is not None else None,
                    'currency_pair': item[21] if item[21] is not None else None,
                    'currency_rate': Decimal(item[22]) if item[22] is not None else None,
                    'last_update': item[23]
                }
                states.append(State(**kwargs))
        cursor.close()
        return states

    def get_latest_state(self, client_id: str, account: str, asset: str, transaction_type=None) -> Optional[State]:
        """

        :rtype: State
        """
        query = f""" SELECT {STATES_TABLE}.`state_id`,
                        {STATES_TABLE}.`client_id`,
                        {STATES_TABLE}.`data_source`,
                        {STATES_TABLE}.`account`,
                        {STATES_TABLE}.`transaction_id`,
                        {STATES_TABLE}.`transaction_type`,
                        {STATES_TABLE}.`asset`,
                        {STATES_TABLE}.`timestamp`,
                        {STATES_TABLE}.`amount_of_asset`,
                        {STATES_TABLE}.`cost_of_asset`,
                        {STATES_TABLE}.`cost_currency`,
                        {STATES_TABLE}.`amount_in_fifo`,
                        {STATES_TABLE}.`cost_in_fifo`,
                        {STATES_TABLE}.`fifo_currency`,
                        {STATES_TABLE}.`income`,
                        {STATES_TABLE}.`cost`,
                        {STATES_TABLE}.`profit`,
                        {STATES_TABLE}.`profit_currency`,
                        {STATES_TABLE}.`dividend`,
                        {STATES_TABLE}.`withholding_tax`,
                        {STATES_TABLE}.`dividend_currency`,
                        {STATES_TABLE}.`currency_pair`,
                        {STATES_TABLE}.`currency_rate`,
                        {STATES_TABLE}.`last_update`
                    FROM {STATES_TABLE}
                    WHERE {STATES_TABLE}.`client_id` = %s AND 
                        {STATES_TABLE}.`account` = %s AND
                        {STATES_TABLE}.`asset` = %s"""
        if transaction_type is not None:
            query += f" AND {STATES_TABLE}.`transaction_type` = '{transaction_type}'"
        query += f""" ORDER BY {STATES_TABLE}.`timestamp` DESC, {STATES_TABLE}.`state_id` DESC LIMIT 1"""
        res = self.read_query(query, (client_id, account, asset))
        if len(res) == 0:
            return None
        else:
            return res[0]

    def execute_query(self, query):
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query)
        cursor.close()

    def delete_all_states(self):
        query = f"DELETE FROM {STATES_TABLE} WHERE {STATES_TABLE}.`state_id` > 0"
        self.execute_query(query)

    def load_test_db(self):
        cursor = self.cnx.cursor(buffered=True)
        with open('config/states_for_tests.sql', 'r') as file:
            result_iterator = cursor.execute(file.read(), multi=True)
            for res in result_iterator:
                print("Running query: ", res)  # Will print out a short representation of the query
                print(f"Affected {res.rowcount} rows")
            self.cnx.commit()

    def insert_state(self, state: State):
        query = f"""INSERT INTO {STATES_TABLE}
                        ({STATES_TABLE}.`state_id`,
                        {STATES_TABLE}.`client_id`,
                        {STATES_TABLE}.`data_source`,
                        {STATES_TABLE}.`account`,
                        {STATES_TABLE}.`transaction_id`,
                        {STATES_TABLE}.`transaction_type`,
                        {STATES_TABLE}.`asset`,
                        {STATES_TABLE}.`timestamp`,
                        {STATES_TABLE}.`amount_of_asset`,
                        {STATES_TABLE}.`cost_of_asset`,
                        {STATES_TABLE}.`cost_currency`,
                        {STATES_TABLE}.`amount_in_fifo`,
                        {STATES_TABLE}.`cost_in_fifo`,
                        {STATES_TABLE}.`fifo_currency`,
                        {STATES_TABLE}.`income`,
                        {STATES_TABLE}.`cost`,
                        {STATES_TABLE}.`profit`,
                        {STATES_TABLE}.`profit_currency`,
                        {STATES_TABLE}.`dividend`,
                        {STATES_TABLE}.`withholding_tax`,
                        {STATES_TABLE}.`dividend_currency`,
                        {STATES_TABLE}.`currency_pair`,
                        {STATES_TABLE}.`currency_rate`,
                        {STATES_TABLE}.`last_update`)
                    VALUES (%s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query, (state.state_id,
                               state.client_id,
                               state.data_source,
                               state.account,
                               state.transaction_id,
                               state.transaction_type,
                               state.asset,
                               state.timestamp,
                               float("{:.2f}".format(state.amount_of_asset)) if state.amount_of_asset is not None else None,
                               float("{:.2f}".format(state.cost_of_asset)) if state.cost_of_asset is not None else None,
                               state.cost_currency,
                               float("{:.2f}".format(state.amount_in_fifo)) if state.amount_in_fifo is not None else None,
                               float("{:.2f}".format(state.cost_in_fifo)) if state.cost_in_fifo is not None else None,
                               state.fifo_currency,
                               float("{:.2f}".format(state.income)) if state.income is not None else None,
                               float("{:.2f}".format(state.cost)) if state.cost is not None else None,
                               float("{:.2f}".format(state.profit)) if state.profit is not None else None,
                               state.profit_currency,
                               float("{:.2f}".format(state.dividend)) if state.dividend is not None else None,
                               float("{:.2f}".format(state.withholding_tax)) if state.withholding_tax is not None else None,
                               state.dividend_currency,
                               state.currency_pair,
                               float("{:.4f}".format(state.currency_rate)) if state.currency_rate is not None else None,
                               state.last_update))
        self.cnx.commit()

    def update_state(self, state: State):
        query = f"""UPDATE {STATES_TABLE}
                   SET  {STATES_TABLE}.`client_id` = %s,
                        {STATES_TABLE}.`data_source` = %s,
                        {STATES_TABLE}.`account` = %s,
                        {STATES_TABLE}.`transaction_id` = %s,
                        {STATES_TABLE}.`transaction_type` = %s,
                        {STATES_TABLE}.`asset` = %s,
                        {STATES_TABLE}.`timestamp` = %s,
                        {STATES_TABLE}.`amount_of_asset` = %s,
                        {STATES_TABLE}.`cost_of_asset` = %s,
                        {STATES_TABLE}.`cost_currency` = %s,
                        {STATES_TABLE}.`amount_in_fifo` = %s,
                        {STATES_TABLE}.`cost_in_fifo` = %s,
                        {STATES_TABLE}.`fifo_currency` = %s,
                        {STATES_TABLE}.`income` = %s,
                        {STATES_TABLE}.`cost` = %s,
                        {STATES_TABLE}.`profit` = %s,
                        {STATES_TABLE}.`profit_currency` = %s,
                        {STATES_TABLE}.`dividend` = %s,
                        {STATES_TABLE}.`withholding_tax` = %s,
                        {STATES_TABLE}.`dividend_currency` = %s,
                        {STATES_TABLE}.`currency_pair` = %s,
                        {STATES_TABLE}.`currency_rate` = %s,
                        {STATES_TABLE}.`last_update` = %s
                   WHERE {STATES_TABLE}.`state_id` = %s"""
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query, (state.client_id,
                               state.data_source,
                               state.account,
                               state.transaction_id,
                               state.transaction_type,
                               state.asset,
                               state.timestamp,
                               float("{:.2f}".format(state.amount_of_asset)) if state.amount_of_asset is not None else None,
                               float("{:.2f}".format(state.cost_of_asset)) if state.cost_of_asset is not None else None,
                               state.cost_currency,
                               float("{:.2f}".format(state.amount_in_fifo)) if state.amount_in_fifo is not None else None,
                               float("{:.2f}".format(state.cost_in_fifo)) if state.cost_in_fifo is not None else None,
                               state.fifo_currency,
                               float("{:.2f}".format(state.income)) if state.income is not None else None,
                               float("{:.2f}".format(state.cost)) if state.cost is not None else None,
                               float("{:.2f}".format(state.profit))if state.profit is not None else None,
                               state.profit_currency,
                               float("{:.2f}".format(state.dividend)) if state.dividend is not None else None,
                               float("{:.2f}".format(state.withholding_tax)) if state.withholding_tax is not None else None,
                               state.dividend_currency,
                               state.currency_pair,
                               float("{:.4f}".format(state.currency_rate)) if state.currency_rate is not None else None,
                               state.last_update,
                               state.state_id
                               ))
        self.cnx.commit()

    def get_fifo_costs_states(self, client_id: str, account: str, asset: str) -> list:
        """

        :rtype: State
        """
        query = f""" SELECT {STATES_TABLE}.`state_id`,
                        {STATES_TABLE}.`client_id`,
                        {STATES_TABLE}.`data_source`,
                        {STATES_TABLE}.`account`,
                        {STATES_TABLE}.`transaction_id`,
                        {STATES_TABLE}.`transaction_type`,
                        {STATES_TABLE}.`asset`,
                        {STATES_TABLE}.`timestamp`,
                        {STATES_TABLE}.`amount_of_asset`,
                        {STATES_TABLE}.`cost_of_asset`,
                        {STATES_TABLE}.`cost_currency`,
                        {STATES_TABLE}.`amount_in_fifo`,
                        {STATES_TABLE}.`cost_in_fifo`,
                        {STATES_TABLE}.`fifo_currency`,
                        {STATES_TABLE}.`income`,
                        {STATES_TABLE}.`cost`,
                        {STATES_TABLE}.`profit`,
                        {STATES_TABLE}.`profit_currency`,
                        {STATES_TABLE}.`dividend`,
                        {STATES_TABLE}.`withholding_tax`,
                        {STATES_TABLE}.`dividend_currency`,
                        {STATES_TABLE}.`currency_pair`,
                        {STATES_TABLE}.`currency_rate`,
                        {STATES_TABLE}.`last_update`
                    FROM {STATES_TABLE}
                    WHERE {STATES_TABLE}.`client_id` = %s AND 
                        {STATES_TABLE}.`account` = %s AND
                        {STATES_TABLE}.`asset` = %s AND
                        {STATES_TABLE}.`amount_in_fifo` > 0
                    ORDER BY {STATES_TABLE}.`timestamp` ASC , {STATES_TABLE}.`state_id` ASC """
        return self.read_query(query, (client_id, account, asset))

    def get_not_processed_states(self) -> list:
        query = f""" SELECT {STATES_TABLE}.`state_id`,
                        {STATES_TABLE}.`client_id`,
                        {STATES_TABLE}.`data_source`,
                        {STATES_TABLE}.`account`,
                        {STATES_TABLE}.`transaction_id`,
                        {STATES_TABLE}.`transaction_type`,
                        {STATES_TABLE}.`asset`,
                        {STATES_TABLE}.`timestamp`,
                        {STATES_TABLE}.`amount_of_asset`,
                        {STATES_TABLE}.`cost_of_asset`,
                        {STATES_TABLE}.`cost_currency`,
                        {STATES_TABLE}.`amount_in_fifo`,
                        {STATES_TABLE}.`cost_in_fifo`,
                        {STATES_TABLE}.`fifo_currency`,
                        {STATES_TABLE}.`income`,
                        {STATES_TABLE}.`cost`,
                        {STATES_TABLE}.`profit`,
                        {STATES_TABLE}.`profit_currency`,
                        {STATES_TABLE}.`dividend`,
                        {STATES_TABLE}.`withholding_tax`,
                        {STATES_TABLE}.`dividend_currency`,
                        {STATES_TABLE}.`currency_pair`,
                        {STATES_TABLE}.`currency_rate`,
                        {STATES_TABLE}.`last_update`
                    FROM {STATES_TABLE}
                    WHERE {STATES_TABLE}.`amount_in_fifo` < 0
                    ORDER BY {STATES_TABLE}.`timestamp` ASC , {STATES_TABLE}.`state_id` ASC"""
        return self.read_query(query, ())

    def split_asset(self, asset: str, split_ratio: tuple):
        query = f"""UPDATE {STATES_TABLE}
                      SET {STATES_TABLE}.`amount_of_asset` = {STATES_TABLE}.`amount_of_asset` * %s / %s
                    WHERE {STATES_TABLE}.`asset` = %s AND {STATES_TABLE}.`amount_of_asset` != 0"""
        self.cnx.cursor().execute(query, (split_ratio[0], split_ratio[1], asset))

        query = f"""UPDATE {STATES_TABLE}
                      SET {STATES_TABLE}.`amount_in_fifo` = {STATES_TABLE}.`amount_in_fifo` * %s / %s
                    WHERE {STATES_TABLE}.`asset` = %s AND {STATES_TABLE}.`amount_in_fifo` != 0"""
        self.cnx.cursor().execute(query, (split_ratio[0], split_ratio[1], asset))

        self.cnx.commit()
