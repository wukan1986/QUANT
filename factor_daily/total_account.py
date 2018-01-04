# -*- coding: utf-8 -*-

import time
from time import strftime, localtime
import sys
import datetime as dt
import yaml
import os
import warnings

sys.path.append("F:/QUANT/")
reload(sys)
from tools.LogManager import record
from tools.EmailSender import send

warnings.filterwarnings("ignore")

from xutils.custom_logger import CustomLogger
from xutils.decorators import handle_exception

LOGGER = CustomLogger(logger_name='Update_error_log', log_level='info',
                      log_file='F:/logger_file/Update_error.log')

logger123 = CustomLogger(logger_name='log_update', log_level='info',
                         log_file='F:/logger_file/log%s.log' % strftime("%Y%m%d", localtime()))

from xutils import (Date,
                    Calendar,
                    Period)

cal = Calendar('China.SSE')

sys.path.append("F:/QUANT/factor_daily/ESG100_weight/")
reload(sys)
import esg100_holdings

sys.path.append("F:/QUANT/simulation/")
reload(sys)
import settle_accounts



with open('F:/QUANT/config/config.yaml') as f:
    temp = yaml.load(f.read())
username = temp['ct_mail']['username']
password = temp['ct_mail']['password']
host = temp['ct_mail']['host']
receiver = temp['ct_mail']['receiver']
receiver = receiver.split(',')


@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【esg100持仓-更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_esg100_holdings():
    print('update esg100_holdings')
    start = '20171130'
    end = strftime("%Y%m%d", localtime())
    today = strftime("%Y%m%d", localtime())
    dirpath = 'G:/ESG100Enhanced/FA'
    output_path = 'Z:/daily_data/holding/production/account-ESG100-pdt'
    fre = 'day'
    flag = 1
    esg100_holdings.run(start, end, today, dirpath, output_path,flag, fre)
    print('Done')
    return u"esg100_持仓更新成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【账户清算-更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def settle_accounts():
    print('update settle_accounts')
    config_path = 'Z:/daily_data/config/config.yaml'
    settle = settle_accounts.settle_account(config_path)
    settle.run()
    settle.run_backup()
    print('Done')
    return u"账户清算 更新成功"



if __name__ == '__main__':
    print(sys.argv[1])
    if sys.argv[1] == 'esg100_holdings':
        update_esg100_holdings()
    elif sys.argv[1] == 'settle_accounts':
        settle_accounts()
