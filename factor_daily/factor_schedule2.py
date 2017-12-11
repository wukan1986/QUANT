# -*- coding: utf-8 -*-

import schedule
import time
from time import strftime, localtime
import sys
import datetime as dt
import yaml
import warnings

warnings.filterwarnings("ignore")

from xutils.custom_logger import CustomLogger
from xutils.decorators import handle_exception

LOGGER = CustomLogger(logger_name='uqer_riskmodel_factor_update', log_level='info',
                      log_file='F:/logger_file/factor_update.log')

from xutils import (Date,
                    Calendar,
                    Period)
cal = Calendar('China.SSE')


sys.path.append("F:/QUANT/factor_daily/fcff/")
reload(sys)
import fcff2_der

sys.path.append("F:/QUANT/factor_daily/net_income/")
reload(sys)
import net_income2_der

sys.path.append("F:/QUANT/factor_daily/PMOM/")
reload(sys)
import PMOM

sys.path.append("F:/QUANT/factor_daily/to_reverse/")
reload(sys)
import re_to_reverse

sys.path.append("F:/QUANT/factor_daily/risk_factor/")
reload(sys)
import risk_factor

sys.path.append("F:/QUANT/factor_daily/riskmodel_benchmark/")
reload(sys)
import exposureDayGet

sys.path.append("F:/QUANT/riskmodel_update/")
reload(sys)
import risk_tolocal

with open('F:/QUANT/config/config.yaml') as f:
    temp = yaml.load(f.read())
username = temp['ct_mail']['username']
password = temp['ct_mail']['password']
host = temp['ct_mail']['host']


@handle_exception(logger=LOGGER, subject="【stdfcf更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=['369030612@qq.com'])
def update_stdfcf():
    print('update stdfcf')
    flag = 1  # 0 回测 1更新
    fre = 'day'
    today = strftime("%Y%m%d", localtime())  # '20170929'
    raw_dirpath = 'F:/QUANT/factor_daily/fcff/raw_data'
    new_dirpath = 'Z:/daily_data/alpha'

    srdfcf = fcff2_der.stdfcff(today, raw_dirpath, new_dirpath, flag, fre)
    srdfcf.run()
    print('Done')


@handle_exception(logger=LOGGER, subject="【stdni更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=['369030612@qq.com'])
def update_stdni():
    print('update stdni')
    flag = 1  # 0 回测 1更新
    fre = 'day'

    today = strftime("%Y%m%d", localtime())  # '20170929'
    raw_dirpath = 'F:/QUANT/factor_daily/net_income/raw_data'
    new_dirpath = 'Z:/daily_data/alpha'

    srdfcf = net_income2_der.stdfcff(today, raw_dirpath, new_dirpath, flag, fre)
    srdfcf.run()
    print('Done')


@handle_exception(logger=LOGGER, subject="【pmom更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=['369030612@qq.com'])
def update_pmom():
    print('update pmom')
    pm = PMOM.pmom()
    # output_path = 'F:/factor_data/raw_data'
    output_path = 'Z:/daily_data/alpha'
    vals = [(9, 185, 40), (6, 130, 40), (3, 65, 60), (6, 130, 60), (12, 240, 20)]
    today = dt.datetime.strptime(strftime("%Y%m%d", localtime()), '%Y%m%d').date()
    for val in vals:
        print(val)
        pm.update(today, val[0], val[1], val[2], output_path)
    print('Done')


@handle_exception(logger=LOGGER, subject="【to_reverse更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=['369030612@qq.com'])
def update_to_reverse():
    print('update to_reverse')
    start = '20171101'
    factor_filename = 'TO_REVERSE'
    flag = 1
    day_range = 21
    dirpath = 'Z:/daily_data/alpha'
    turnover = re_to_reverse.factorGet(start, flag, factor_filename, day_range, dirpath)
    turnover.run()
    print('Done')


@handle_exception(logger=LOGGER, subject="【risk_factor更新失败！！】", sender=username, username=username, password=password,host=host, receiver=['369030612@qq.com'])
def update_risk_factor():
    print('update risk_factor')
    output_path = 'Z:/daily_data/alpha'
    start = '20170701'
    end = strftime("%Y%m%d", localtime())
    flag = 1  # 0 回测 1更新

    risk_factor.run(output_path, start, end, flag)
    print('Done')


########################################################################################################
# risk_model
@handle_exception(logger=LOGGER, subject="【risk_model更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=['369030612@qq.com'])
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


@handle_exception(logger=LOGGER, subject="【risk_tolocal更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=['369030612@qq.com'])
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


def job():
    update_stdfcf()
    update_stdni()
    update_pmom()
    update_to_reverse()
    update_risk_factor()

    update_risk_model()
    update_risk_tolocal()


schedule.every().day.at("06:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
