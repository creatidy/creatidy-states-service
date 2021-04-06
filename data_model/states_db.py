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

    def get_latest_state(self, client_id: str, account: str, asset: str) -> State:
        """

        :rtype: State
        """
        query = """ SELECT `states`.`state_id`,
                        `states`.`client_id`,
                        `states`.`data_source`,
                        `states`.`account`,
                        `states`.`transaction_id`,
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
                        `states`.`last_update`
                    FROM `creatidy`.`states`
                    WHERE `states`.`client_id` = %s AND 
                        `states`.`account` = %s AND
                        `states`.`asset` = %s 
                    ORDER BY `states`.`timestamp` DESC, `states`.`state_id` DESC
                    LIMIT 1"""
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query, (client_id, account, asset))
        kwargs = {}
        if cursor.rowcount > 0:
            for item in cursor:
                kwargs = {
                    'state_id': item[0],
                    'client_id': item[1],
                    'data_source': item[2],
                    'account': item[3],
                    'transaction_id': item[4],
                    'asset': item[5],
                    'timestamp': item[6],
                    'amount_of_asset': Decimal(item[7]) if item[7] is not None else None,
                    'cost_of_asset': Decimal(item[8]) if item[8] is not None else None,
                    'cost_currency': item[9],
                    'amount_in_fifo': Decimal(item[10]) if item[10] is not None else None,
                    'cost_in_fifo': Decimal(item[11]) if item[11] is not None else None,
                    'fifo_currency': item[12],
                    'income': Decimal(item[13]) if item[13] is not None else None,
                    'cost': Decimal(item[14]) if item[14] is not None else None,
                    'profit': Decimal(item[15]) if item[15] is not None else None,
                    'profit_currency': item[16] if item[16] is not None else None,
                    'dividend': Decimal(item[17]) if item[17] is not None else None,
                    'withholding_tax': Decimal(item[18]) if item[18] is not None else None,
                    'dividend_currency': item[19] if item[19] is not None else None,
                    'last_update': item[20]
                }
        cursor.close()
        return State(**kwargs)

    def delete_all_states(self):
        query = "DELETE FROM `creatidy`.`states` WHERE `states`.`state_id` > 0"
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query,)
        cursor.close()

    def insert_state(self, state: State):
        query = """INSERT INTO `creatidy`.`states`
                        (`states`.`state_id`,
                        `states`.`client_id`,
                        `states`.`data_source`,
                        `states`.`account`,
                        `states`.`transaction_id`,
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
                        `states`.`last_update`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query, (state.state_id,
                               state.client_id,
                               state.data_source,
                               state.account,
                               state.transaction_id,
                               state.asset,
                               datetime.fromtimestamp(float(state.timestamp)),
                               float("{:.2f}".format(state.amount_of_asset)),
                               float("{:.2f}".format(state.cost_of_asset)),
                               state.cost_currency,
                               float("{:.2f}".format(state.amount_in_fifo)),
                               float("{:.2f}".format(state.cost_in_fifo)),
                               state.fifo_currency,
                               float("{:.2f}".format(state.income)),
                               float("{:.2f}".format(state.cost)),
                               float("{:.2f}".format(state.profit)),
                               state.profit_currency,
                               float("{:.2f}".format(state.dividend)),
                               float("{:.2f}".format(state.withholding_tax)),
                               state.dividend_currency,
                               datetime.fromtimestamp(float(state.last_update))))
        self.cnx.commit()

    def update_state(self, state: State):
        query = """UPDATE `creatidy`.`states`
                   SET  `states`.`client_id` = %s,
                        `states`.`data_source` = %s,
                        `states`.`account` = %s,
                        `states`.`transaction_id` = %s,
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
                        `states`.`last_update` = %s
                   WHERE `states`.`state_id` = %s"""
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query, (state.client_id,
                               state.data_source,
                               state.account,
                               state.transaction_id,
                               state.asset,
                               datetime.fromtimestamp(float(state.timestamp)),
                               float("{:.2f}".format(state.amount_of_asset)),
                               float("{:.2f}".format(state.cost_of_asset)),
                               state.cost_currency,
                               float("{:.2f}".format(state.amount_in_fifo)),
                               float("{:.2f}".format(state.cost_in_fifo)),
                               state.fifo_currency,
                               float("{:.2f}".format(state.income)),
                               float("{:.2f}".format(state.cost)),
                               float("{:.2f}".format(state.profit)),
                               state.profit_currency,
                               float("{:.2f}".format(state.dividend)),
                               float("{:.2f}".format(state.withholding_tax)),
                               state.dividend_currency,
                               datetime.fromtimestamp(float(state.last_update)),
                               state.state_id
                               ))
        self.cnx.commit()
