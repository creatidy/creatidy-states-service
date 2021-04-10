from typing import Optional

import mysql.connector
import configparser
from dto.state import State
from decimal import Decimal
from datetime import datetime


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

    def get_latest_state(self, client_id: str, account: str, asset: str) -> Optional[State]:
        """

        :rtype: State
        """
        query = """ SELECT `states`.`state_id`,
                        `states`.`client_id`,
                        `states`.`data_source`,
                        `states`.`account`,
                        `states`.`transaction_id`,
                        `states`.`transaction_type`,
                        `states`.`asset`,
                        `states`.`timestamp`,
                        `states`.`amount_of_asset`,
                        `states`.`cost_of_asset`,
                        `states`.`cost_currency`,
                        `states`.`amount_in_fifo`,
                        `states`.`cost_in_fifo`,
                        `states`.`fifo_currency`,
                        `states`.`income`,
                        `states`.`cost`,
                        `states`.`profit`,
                        `states`.`profit_currency`,
                        `states`.`dividend`,
                        `states`.`withholding_tax`,
                        `states`.`dividend_currency`,
                        `states`.`currency_pair`,
                        `states`.`currency_rate`,
                        `states`.`last_update`
                    FROM `creatidy`.`states`
                    WHERE `states`.`client_id` = %s AND 
                        `states`.`account` = %s AND
                        `states`.`asset` = %s 
                    ORDER BY `states`.`timestamp` DESC, `states`.`state_id` DESC
                    LIMIT 1"""
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
        query = "DELETE FROM `creatidy`.`states` WHERE `states`.`state_id` > 0"
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
        query = """INSERT INTO `creatidy`.`states`
                        (`states`.`state_id`,
                        `states`.`client_id`,
                        `states`.`data_source`,
                        `states`.`account`,
                        `states`.`transaction_id`,
                        `states`.`transaction_type`,
                        `states`.`asset`,
                        `states`.`timestamp`,
                        `states`.`amount_of_asset`,
                        `states`.`cost_of_asset`,
                        `states`.`cost_currency`,
                        `states`.`amount_in_fifo`,
                        `states`.`cost_in_fifo`,
                        `states`.`fifo_currency`,
                        `states`.`income`,
                        `states`.`cost`,
                        `states`.`profit`,
                        `states`.`profit_currency`,
                        `states`.`dividend`,
                        `states`.`withholding_tax`,
                        `states`.`dividend_currency`,
                        `states`.`currency_pair`,
                        `states`.`currency_rate`,
                        `states`.`last_update`)
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
        query = """UPDATE `creatidy`.`states`
                   SET  `states`.`client_id` = %s,
                        `states`.`data_source` = %s,
                        `states`.`account` = %s,
                        `states`.`transaction_id` = %s,
                        `states`.`transaction_type` = %s,
                        `states`.`asset` = %s,
                        `states`.`timestamp` = %s,
                        `states`.`amount_of_asset` = %s,
                        `states`.`cost_of_asset` = %s,
                        `states`.`cost_currency` = %s,
                        `states`.`amount_in_fifo` = %s,
                        `states`.`cost_in_fifo` = %s,
                        `states`.`fifo_currency` = %s,
                        `states`.`income` = %s,
                        `states`.`cost` = %s,
                        `states`.`profit` = %s,
                        `states`.`profit_currency` = %s,
                        `states`.`dividend` = %s,
                        `states`.`withholding_tax` = %s,
                        `states`.`dividend_currency` = %s,
                        `states`.`currency_pair` = %s,
                        `states`.`currency_rate` = %s,
                        `states`.`last_update` = %s
                   WHERE `states`.`state_id` = %s"""
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
        query = """ SELECT `states`.`state_id`,
                        `states`.`client_id`,
                        `states`.`data_source`,
                        `states`.`account`,
                        `states`.`transaction_id`,
                        `states`.`transaction_type`,
                        `states`.`asset`,
                        `states`.`timestamp`,
                        `states`.`amount_of_asset`,
                        `states`.`cost_of_asset`,
                        `states`.`cost_currency`,
                        `states`.`amount_in_fifo`,
                        `states`.`cost_in_fifo`,
                        `states`.`fifo_currency`,
                        `states`.`income`,
                        `states`.`cost`,
                        `states`.`profit`,
                        `states`.`profit_currency`,
                        `states`.`dividend`,
                        `states`.`withholding_tax`,
                        `states`.`dividend_currency`,
                        `states`.`currency_pair`,
                        `states`.`currency_rate`,
                        `states`.`last_update`
                    FROM `creatidy`.`states`
                    WHERE `states`.`client_id` = %s AND 
                        `states`.`account` = %s AND
                        `states`.`asset` = %s AND
                        `states`.`amount_in_fifo` > 0
                    ORDER BY `states`.`timestamp` ASC , `states`.`state_id` ASC """
        return self.read_query(query, (client_id, account, asset))

    def get_not_processed_states(self) -> list:
        query = """ SELECT `states`.`state_id`,
                        `states`.`client_id`,
                        `states`.`data_source`,
                        `states`.`account`,
                        `states`.`transaction_id`,
                        `states`.`transaction_type`,
                        `states`.`asset`,
                        `states`.`timestamp`,
                        `states`.`amount_of_asset`,
                        `states`.`cost_of_asset`,
                        `states`.`cost_currency`,
                        `states`.`amount_in_fifo`,
                        `states`.`cost_in_fifo`,
                        `states`.`fifo_currency`,
                        `states`.`income`,
                        `states`.`cost`,
                        `states`.`profit`,
                        `states`.`profit_currency`,
                        `states`.`dividend`,
                        `states`.`withholding_tax`,
                        `states`.`dividend_currency`,
                        `states`.`currency_pair`,
                        `states`.`currency_rate`,
                        `states`.`last_update`
                    FROM `creatidy`.`states`
                    WHERE `states`.`amount_in_fifo` < 0
                    ORDER BY `states`.`timestamp` ASC , `states`.`state_id` ASC"""
        return self.read_query(query, ())

