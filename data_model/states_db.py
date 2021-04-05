import mysql.connector
import configparser
from dto.state import State
from decimal import Decimal


class StatesDatabase:

    def __init__(self, cfg='config/config.ini'):
        config = configparser.ConfigParser()
        config.read(cfg)

        user = config['CreatidyDB']['USER']
        password = config['CreatidyDB']['PASSWORD']
        host = config['CreatidyDB']['HOST']
        database = config['CreatidyDB']['DATABASE']
        self.cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)

    def get_latest_state(self, client_id, account, asset):
        query = """ SELECT `states`.`id`,
                        `states`.`client_id`,
                        `states`.`data_source`,
                        `states`.`account`,
                        `states`.`transaction_id`,
                        `states`.`asset`,
                        `states`.`datetime`,
                        `states`.`amount_of_asset`,
                        `states`.`cost_of_asset`,
                        `states`.`amount_in_fifo`,
                        `states`.`cost_in_fifo`,
                        `states`.`currency`,
                        `states`.`income`,
                        `states`.`cost`,
                        `states`.`profit`,
                        `states`.`last_update`
                    FROM `creatidy`.`states`
                    WHERE `states`.`client_id` = %s AND 
                        `states`.`account` = %s AND
                        `states`.`asset` = %s 
                    ORDER BY `states`.`datetime` DESC, `states`.`id` DESC
                    LIMIT 1"""
        cursor = self.cnx.cursor(buffered=True)
        cursor.execute(query, (client_id, account, asset))
        kwargs = {}
        if cursor.rowcount > 0:
            for item in cursor:
                kwargs = {
                    'id': item[0],
                    'client_id': item[1],
                    'data_source': item[2],
                    'account': item[3],
                    'transaction_id': item[4],
                    'asset': item[5],
                    'datetime': item[6],
                    'amount_of_asset': Decimal(item[7]),
                    'cost_of_asset': Decimal(item[8]),
                    'amount_in_fifo': Decimal(item[9]),
                    'cost_in_fifo': Decimal(item[10]),
                    'currency': item[11],
                    'income': Decimal(item[12]),
                    'cost': Decimal(item[13]),
                    'profit': Decimal(item[14]),
                    'last_update': item[15]
                }
            cursor.close()
            return State(**kwargs)
        else:
            cursor.close()
            return None


