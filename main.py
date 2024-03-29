from process_controller.states_processor import StatesManager
from data_model.states_db import StatesDatabase

def main():
    sm = StatesManager()
    t = sm.get_transactions()
    sm = StatesManager()
    res = sm.process_transactions()
    # db = StatesDatabase()
    # db.load_test_db()
    sm = StatesManager()
    res = sm.process_costs()


if __name__ == '__main__':
    main()
    