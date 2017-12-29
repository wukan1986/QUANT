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



sys.path.append("F:/QUANT/factor_daily/riskmodel_benchmark/")
reload(sys)
import exposureDayGet

sys.path.append("F:/QUANT/riskmodel_update/")
reload(sys)
import risk_tolocal

sys.path.append("F:/QUANT/factor_daily/barra_risk/")
reload(sys)
import read_barra_daily

sys.path.append("F:/QUANT/factor_daily/barra_risk/")
reload(sys)
import read_barra_daily_backtest

sys.path.append("F:/QUANT/factor_daily/ESG100_weight/")
reload(sys)
import esg100_weight

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
# risk_model
@record(logger123)
@handle_exception(logger=LOGGER, subject="【risk_model更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_risk_model():
    print('update risk_model')
    start = '2017-11-02'
    end = strftime("%Y-%m-%d", localtime())
    fre = 'day'  #
    dirpath = 'Z:/daily_data/uqer_model'
    fre_type1 = 'short'  # long

    firstRun_update = 1  ##  0代表第一次运行  1代表更新

    test1 = exposureDayGet.Barra_data(start, end, dirpath, fre, fre_type1)
    if firstRun_update == 0:
        test1.getAllRMExposure()
        test1.getAllRMCovarianceShort()
    elif firstRun_update == 1:
        test1.updateRMExposure()
        test1.updateRMCovarianceShort()
        test1.check_csv()
    print('Done')
    return u"uqer 风险模型更新成功"


@record(logger123)
@handle_exception(logger=LOGGER, subject="【risk_tolocal更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_risk_tolocal():
    start = '2017-11-16'
    end = strftime("%Y-%m-%d", localtime())
    fre = 'day'  #
    #    dirpath = '../database'
    dirpath = 'F:/uqer_riskmodel'

    firstRun_update = 1  ##  0代表第一次运行  1代表更新

    fre_type1 = 'short'  # long
    test1 = risk_tolocal.Barra_data(start, end, dirpath, firstRun_update, fre, fre_type1)
    test1.history_update()

    fre_type2 = 'long'  # long
    test2 = risk_tolocal.Barra_data(start, end, dirpath, firstRun_update, fre, fre_type2)
    test2.history_update()
    return u"uqer 风险模型保存到本地备份"


@record(logger123)
@handle_exception(logger=LOGGER, subject="【barra_model更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_barra_model():
    print('update barra_model')
    dirpath = 'Z:/MSCI/daily'
    output1 = 'Z:/daily_data/barra_model/Exposure'
    output2 = 'Z:/daily_data/barra_model/Covariance'

    start = '20171127'
    end = strftime("%Y%m%d", localtime())

    flag = 1
    read_barra_daily.run(start, end, flag, dirpath, output1, output2)
    print('Done')
    return u"Barra 风险模型更新成功"


@record(logger123)
@handle_exception(logger=LOGGER, subject="【esg100_weight更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_esg100_weight():
    print('update esg100_weight')
    dir_path = 'G:/ESG100Enhanced/ZZZS'
    start = '20171001'
    end = strftime("%Y%m%d", localtime())

    firstRun_update = 1  ####  0代表第一次运行  1代表更新

    fre = 'day'
    beachmark_file = 'Z:/daily_data/benchmark'
    index_code1 = u"000846"
    esg100_weight.run(firstRun_update, start, end, fre, index_code1, dir_path, beachmark_file)
    print('Done')
    return u"ESG100权重更新成功"


@record(logger123)
@handle_exception(logger=LOGGER, subject="【barra_model_backtest更新失败！！】", sender=username, username=username,
                  password=password,
                  host=host, receiver=receiver)
def update_barra_model_backtest(today):
    print('update barra_model_backtest')
    dirpath = 'Z:/MSCI/daily'
    output1 = 'Z:/axioma_data/barra_model/Exposure'
    output2 = 'Z:/axioma_data/barra_model/Covariance'

    start = '20171201'
    # end = strftime("%Y%m%d", localtime())
    flag = 1
    print(today)
    read_barra_daily_backtest.run(start, today, flag, dirpath, output1, output2)
    print('Done')
    return u"Barra风险模型更新成功 路径Z:/axioma_data/barra_model"


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
