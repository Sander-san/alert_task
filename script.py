import pandas as pd
from datetime import datetime, timedelta
import logging
from multiprocessing import Process


'''PROCESSING FILE'''
df = pd.read_csv('data/mobile_logs.csv')

column_name = ['error_code', 'error_message', 'severity', 'log_location',
               'mode', 'model', 'graphics', 'session_id', 'sdkv', 'test_mode',
               'flow_id', 'flow_type', 'sdk_date', 'publisher_id', 'game_id',
               'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']
df.columns = column_name

bundle_ids = df['bundle_id'].unique()
df = df.sort_values(by='date')


def check_fatal_error(dataframe, time=1, logger_id=None, error_amount=10):
    """CATCH ERRORS FUNC"""

    """SET UP LOGGERS"""
    logging.basicConfig(level=logging.WARNING,
                        filename=f"logs/{logger_id}_fatal_errors.log",
                        filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")

    alert_logger = logging.getLogger('alert_logger')
    alert_logger.setLevel(logging.WARNING)
    py_handler = logging.FileHandler(f"logs/{logger_id}_alerts.log", mode='w')
    py_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    py_handler.setFormatter(py_formatter)
    alert_logger.addHandler(py_handler)

    error_logger = logging.getLogger('error_logger')
    error_logger.setLevel(logging.WARNING)
    py_handler = logging.FileHandler(f"logs/{logger_id}_errors.log", mode='w')
    py_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    py_handler.setFormatter(py_formatter)
    error_logger.addHandler(py_handler)

    """SET UP VARS"""
    start_date = None
    error_counter = 0
    index = 0
    last_index = dataframe['error_code'].count()

    while index < last_index:
        if start_date is None:
            start_date = datetime.fromtimestamp(dataframe['date'].values[0])

        '''Catch ordinary errors'''
        if dataframe['severity'].values[index] == 'Error' and dataframe['error_code'].values[index] != 203:
            res = dataframe.loc[index, [i for i in column_name]]
            error_logger.error(f"[ERROR]: \n{res}")

        '''Catch fatal errors'''
        if dataframe['error_code'].values[index] == 203:
            error_counter += 1
            res = dataframe.loc[index, [i for i in column_name]]
            logging.critical(f"[FATAL ERROR]: \n{res}")

        '''Checking an interval'''
        interval = start_date + timedelta(minutes=time)
        if datetime.fromtimestamp(dataframe['date'].values[index]) > interval:
            start_date = datetime.fromtimestamp(dataframe['date'].values[index])
            if error_counter > error_amount:
                alert_logger.warning("[WARNING]: \nMore than 10 fatal errors")
            error_counter = 0

        '''Alert about more than 10 fatal errors'''
        if error_counter > error_amount:
            alert_logger.warning("[WARNING]: \nMore than 10 fatal errors")
            error_counter = 0

        index += 1
    return error_counter


def main():
    """PARALLEL RUN"""
    processes = []

    p = Process(target=check_fatal_error, args=(df, 1, 'general'))
    processes.append(p)

    for bundle in bundle_ids:
        bundle_df = df[df['bundle_id'] == bundle]
        bundle_df = bundle_df.reset_index(drop=True)
        p = Process(target=check_fatal_error, args=(bundle_df, 60, bundle))
        processes.append(p)

    for p in processes:
        p.start()
    for p in processes:
        p.join()


if __name__ == '__main__':
    main()

