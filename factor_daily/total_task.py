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

sys.path.append("F:/QUANT/factor_daily/risk_factor/")
reload(sys)
import barra_factor_daily

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


@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【stdfcf更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_stdfcf(today):
    print('update stdfcf')
    flag = 1  # 0 回测 1更新
    fre = 'day'
    # today = strftime("%Y%m%d", localtime())  # '20170929'
    raw_dirpath = 'F:/QUANT/factor_daily/fcff/raw_data'
    new_dirpath = 'Z:/daily_data/alpha/raw'

    srdfcf = fcff2_der.stdfcff(today, raw_dirpath, new_dirpath, flag, fre)
    srdfcf.run()
    print('Done')
    return u"stdfcf更新成功 FCFSR2更新成功"


@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【stdni更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_stdni(today):
    print('update stdni')
    flag = 1  # 0 回测 1更新
    fre = 'day'

    # today = strftime("%Y%m%d", localtime())  # '20170929'
    raw_dirpath = 'F:/QUANT/factor_daily/net_income/raw_data'
    new_dirpath = 'Z:/daily_data/alpha/raw'

    srdfcf = net_income2_der.stdfcff(today, raw_dirpath, new_dirpath, flag, fre)
    srdfcf.run()
    print('Done')
    return u"stdni更新成功 NISR2更新成功"


@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【pmom更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_pmom(today):
    print('update pmom')
    output_path = 'Z:/daily_data/alpha/raw'
    pm = PMOM.pmom(output_path)
    vals = [(9, 185, 40), (6, 130, 40), (3, 65, 60), (6, 130, 60), (12, 240, 20)]
    today1 = dt.datetime.strptime(today, '%Y%m%d').date()
    print(today1)
    for val in vals:
        print(val)
        pm.update(today1, val[0], val[1], val[2])
    print('Done')
    return u"pmom360 pmom640 pmom660 pmom940 pmom1220 更新成功"


@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【to_reverse更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_to_reverse(today):
    print('update to_reverse')
    start = '20171101'
    factor_filename = 'TO_REVERSE'
    flag = 1
    fre = 'day'
    day_range = 21
    dirpath = 'Z:/daily_data/alpha/raw'
    print(today)
    turnover = re_to_reverse.factorGet(start, flag, fre, factor_filename, day_range, dirpath, date=today)
    turnover.run()
    print('Done')
    return u"to_reverse 更新成功"


@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【risk_factor更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_risk_factor(today):
    print('update risk_factor')
    output_path = 'Z:/daily_data/alpha/raw'
    start = '20170701'
    end = today
    flag = 1  # 0 回测 1更新
    fre = 'day'
    risk_factor.run(output_path, start, end, flag, fre)
    print('Done')
    return u"uqer风险因子SIZE SIZENL RESVOL LIQUIDITY更新成功"


@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【barra_factor更新失败！！】", sender=username, username=username, password=password,
                  host=host, receiver=receiver)
def update_barra_factor(today):
    print('update barra_factor')
    dir_path = 'Z:/daily_data/barra_model/Exposure'
    output_path = 'Z:/axioma_data/alpha'
    output_path = 'Z:/daily_data/alpha/raw'

    start = '20171201'
    fre = 'day'
    flag = 1  # 0 回测 1更新

    barra_factor_daily.run(output_path, start, today, flag, fre, dir_path)
    print('Done')
    return u"Barra风险因子SIZE SIZENL RESVOL LIQUIDITY更新成功"


########################################################################################################
# risk_model
@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【risk_model更新失败！！】", sender=username, username=username, password=password,
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
@handle_exception(logger=LOGGER, subject=u"【risk_tolocal更新失败！！】", sender=username, username=username, password=password,
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
@handle_exception(logger=LOGGER, subject=u"【barra_model更新失败！！】", sender=username, username=username, password=password,
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
@handle_exception(logger=LOGGER, subject=u"【esg100_weight更新失败！！】", sender=username, username=username, password=password,
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
@handle_exception(logger=LOGGER, subject=u"【barra_model_backtest更新失败！！】", sender=username, username=username,
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
@handle_exception(logger=LOGGER, subject=u"【suspended_stock更新失败！！】", sender=username, username=username,
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


@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【insider_data更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_insider_data(today, N, Fre, datapath, barapath):
    Insider.LoadInsiderData_main(today, N, Fre, datapath, barapath)
    return u"insider基础数据保存到本地成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【PV insider_更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_PV(today, N, Fre, Halflife, datapath, outputpath):
    print('****** PV ******')
    PV = Insider.GetPV(today, N, Fre, Halflife, datapath)
    PV = PV[['S_INFO_WINDCODE', 'PV_raw']]
    path = outputpath + 'PV'
    if not os.path.exists(path):
        os.mkdir(path)
    PV.to_csv(path + '/PV_raw_CN_' + today + '.csv', index=None)
    return u"PV insider_更新成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【AES insider_更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_AES(today, datapath, outputpath):
    print('****** AES ******')
    AES = Insider.GetAE(today, datapath)
    AES = AES[['S_INFO_WINDCODE', 'AE_raw']]
    AES.columns = ['S_INFO_WINDCODE', 'AES_raw']
    path = outputpath + 'AES'
    if not os.path.exists(path):
        os.mkdir(path)
    AES.to_csv(path + '/AES_raw_CN_' + today + '.csv', index=None)
    return u"AES insider_更新成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【NSTS insider_更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_NSTS(today, N, Fre, Halflife, datapath, outputpath):
    print('****** NSTS ******')
    NSTS = Insider.GetNST(today, N, Fre, Halflife, datapath)
    NSTS = NSTS[['S_INFO_WINDCODE', 'NST_raw']]
    NSTS.columns = ['S_INFO_WINDCODE', 'NSTS_raw']
    path = outputpath + 'NSTS'
    if not os.path.exists(path):
        os.mkdir(path)
    NSTS.to_csv(path + '/NSTS_raw_CN_' + today + '.csv', index=None)
    return u"NSTS insider_更新成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【SA/SL/Accrual/Accrualwgt insider_更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_SSAA(today, N, Fre, Halflife, datapath, outputpath):
    print('****** SA/SL/Accrual/Accrualwgt ******')
    SApara = {'aetopQ': 0.25, 'aebomQ': 0.5, 'sizeQ': 5.0, 'nstbomQ': 0.5, 'indtype': 'BARAIND', 'sizetype': 'SIZE'}
    SA = Insider.GetSA(today, N, Fre, Halflife, SApara, datapath)
    SL = SA[['S_INFO_WINDCODE', 'SL_raw']]
    path = outputpath + 'SL'
    if not os.path.exists(path):
        os.mkdir(path)
    SL.to_csv(path + '/SL_raw_CN_' + today + '.csv', index=None)

    Accrual = SA[['S_INFO_WINDCODE', 'Accrual_raw']]
    path = outputpath + 'Accrual'
    if not os.path.exists(path):
        os.mkdir(path)
    Accrual.to_csv(path + '/Accrual_raw_CN_' + today + '.csv', index=None)

    Accrualwgt = SA[['S_INFO_WINDCODE', 'Accrualwgt_raw']]
    path = outputpath + 'Accrualwgt'
    if not os.path.exists(path):
        os.mkdir(path)
    Accrualwgt.to_csv(path + '/Accrualwgt_raw_CN_' + today + '.csv', index=None)

    SA = SA[['S_INFO_WINDCODE', 'SA_raw']]
    path = outputpath + 'SA'
    if not os.path.exists(path):
        os.mkdir(path)
    SA.to_csv(path + '/SA_raw_CN_' + today + '.csv', index=None)
    return u"SA/SL/Accrual/Accrualwgt insider_更新成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【MJRR insider_更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_MJRR(today, N, Fre, Halflife, datapath, outputpath):
    print('****** MJRR ******')
    MJRR = Insider.GetMJRR(today, N, Fre, Halflife, datapath)
    MJRR = MJRR[['S_INFO_WINDCODE', 'MJRR_raw']]
    path = outputpath + 'MJRR'
    if not os.path.exists(path):
        os.mkdir(path)
    MJRR.to_csv(path + '/MJRR_raw_CN_' + today + '.csv', index=None)
    return u"MJRR insider_更新成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【MJRV insider_更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_MJRV(today, N, Fre, Halflife, datapath, outputpath):
    print('****** MJRV ******')
    MJRV = Insider.GetMJRV(today, N, Fre, Halflife, datapath)
    MJRV = MJRV[['S_INFO_WINDCODE', 'MJRV_raw']]
    path = outputpath + 'MJRV'
    if not os.path.exists(path):
        os.mkdir(path)
    MJRV.to_csv(path + '/MJRV_raw_CN_' + today + '.csv', index=None)
    return u"MJRV insider_更新成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【LIO insider_更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_LIO(today, datapath, outputpath):
    print('****** LIO ******')
    LIO = Insider.GetLIO(today, datapath)
    LIO = LIO[['S_INFO_WINDCODE', 'LIO_raw']]
    path = outputpath + 'LIO'
    if not os.path.exists(path):
        os.mkdir(path)
    LIO.to_csv(path + '/LIO_raw_CN_' + today + '.csv', index=None)
    return u"LIO insider_更新成功"

@record(logger123)
@handle_exception(logger=LOGGER, subject=u"【LIOCHANGE insider_更新失败！！】", sender=username, username=username,
                  password=password, host=host, receiver=receiver)
def update_LIOCHANGE(today, datapath, outputpath):
    print('****** LIOCHANGE ******')
    LIOCHANGE = Insider.GetLIOCHANGE(today, datapath)
    LIOCHANGE = LIOCHANGE[['S_INFO_WINDCODE', 'LIOCHANGE_raw']]
    path = outputpath + 'LIOCHANGE'
    if not os.path.exists(path):
        os.mkdir(path)
    LIOCHANGE.to_csv(path + '/LIOCHANGE_raw_CN_' + today + '.csv', index=None)
    return u"LIOCHANGE insider_更新成功"


def base_data():
    today = strftime("%Y%m%d", localtime())
    today = Date.strptime(today, '%Y%m%d')
    today = cal.advanceDate(today, Period('-1b'))
    today = today.strftime("%Y%m%d")

    update_barra_model()

    update_risk_model()
    update_risk_tolocal()

    update_esg100_weight()
    update_barra_model_backtest(today)

def factor_multi():
    today = strftime("%Y%m%d", localtime())
    today = Date.strptime(today, '%Y%m%d')
    today = cal.advanceDate(today, Period('-1b'))
    today = today.strftime("%Y%m%d")

    update_risk_factor(today)
    update_barra_factor(today)
    update_stdfcf(today)
    update_stdni(today)
    update_pmom(today)
    update_to_reverse(today)


def insider_data():
    today = strftime("%Y%m%d", localtime())
    today = Date.strptime(today, '%Y%m%d')
    today = cal.advanceDate(today, Period('-1b'))
    today = today.strftime("%Y%m%d")

    N = -1
    Fre = 'Y'
    Halflife = 180.0
    datapath = 'F:/factor_data/Insider_Data/'
    barapath = 'Z:/axioma_data/barra_model/Exposure/'
    outputpath = 'F:/factor_data/test_data/'

    update_insider_data([today], N, Fre, datapath, barapath)


def factor_insider():
    today = strftime("%Y%m%d", localtime())
    today = Date.strptime(today, '%Y%m%d')
    today = cal.advanceDate(today, Period('-1b'))
    today = today.strftime("%Y%m%d")

    N = -1
    Fre = 'Y'
    Halflife = 180.0
    datapath = 'F:/factor_data/Insider_Data/'
    barapath = 'Z:/axioma_data/barra_model/Exposure/'
    outputpath = 'F:/factor_data/test_data/'

    update_PV(today, N, Fre, Halflife, datapath, outputpath)
    update_AES(today, datapath, outputpath)
    update_NSTS(today, N, Fre, Halflife, datapath, outputpath)
    update_SSAA(today, N, Fre, Halflife, datapath, outputpath)
    update_MJRR(today, N, Fre, Halflife, datapath, outputpath)
    update_MJRV(today, N, Fre, Halflife, datapath, outputpath)
    update_LIO(today, datapath, outputpath)
    update_LIOCHANGE(today, datapath, outputpath)


def suspended_stock():
    update_suspended()

def email_log():
    f = open('F:/logger_file/log%s.log' % strftime("%Y%m%d", localtime()), "r")
    lines = f.readlines()
    msg = ''.join(lines)
    send('test', msg, receiver)



if __name__ == '__main__':
    print sys.argv[1]
    if sys.argv[1] == 'base_data':
        base_data()
    elif sys.argv[1] == 'factor_multi':
        factor_multi()
    elif sys.argv[1] == 'insider_data':
        insider_data()
    elif sys.argv[1] == 'factor_insider':
        factor_insider()
    elif sys.argv[1] == 'suspended_stock':
        suspended_stock()
    elif sys.argv[1] == 'email_log':
        email_log()