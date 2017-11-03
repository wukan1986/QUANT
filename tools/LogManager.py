import logging
import datetime as dt
import os


def create_logger(name):
    logger = logging.getLogger(name)
    dir_path = os.path.split(os.path.realpath(__file__))[0]
    file_name = '{2}/Log/{0}-{1}.log'.format(name, dt.datetime.now().strftime('%Y-%m-%d'), dir_path)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(file_name)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(thread)s %(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(console_handler)
    return logger