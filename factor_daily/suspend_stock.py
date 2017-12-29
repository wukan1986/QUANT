# -*- coding: utf-8 -*-

import schedule
import time
from time import strftime, localtime
import sys
import datetime as dt
import yaml
import os
import warnings
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


sys.path.append("F:/QUANT/factor_daily/Suspension/")
reload(sys)
import get_suspension

sys.path.append("F:/QUANT/factor_daily/Insider_Code/")
reload(sys)
import Insider

with open('F:/QUANT/config/config.yaml') as f:
    temp = yaml.load(f.read())
username = temp['ct_mail']['username']
password = temp['ct_mail']['password']
host = temp['ct_mail']['host']
receiver = temp['ct_mail']['receiver']
receiver = receiver.split(',')

########################################################################################################

@record(logger123)
@handle_exception(logger=LOGGER, subject="【suspended_stock更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_suspended():
    start = '20171130'
    end = strftime("%Y%m%d", localtime())
    today1 = strftime("%Y%m%d", localtime())
    dirpath = 'Z:/daily_data/suspended'

    fre = 'day'
    flag = 1
    get_suspension.run(start, end, today1, dirpath, flag, fre)
    return u"停牌股票更新成功"


def job():
    today = strftime("%Y%m%d", localtime())
    today = Date.strptime(today, '%Y%m%d')
    today = cal.advanceDate(today, Period('-1b'))
    today = today.strftime("%Y%m%d")

    update_barra_model()

    update_risk_model()
    update_risk_tolocal()

    update_esg100_weight()
    update_barra_model_backtest(today)


    f = open('F:/logger_file/log%s.log' % strftime("%Y%m%d", localtime()), "r")
    lines = f.readlines()
    msg = ''.join(lines)
    send('test', msg, receiver)


def run_at_9():
    update_suspended()


schedule.every().day.at("07:15").do(job)
schedule.every().day.at("09:00").do(run_at_9)

while True:
    schedule.run_pending()
    time.sleep(1)
