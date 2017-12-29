# -*- coding: utf-8 -*-

import schedule
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



if __name__ == '__main__':
    print sys.argv[1]
    if sys.argv[1] == 'insider_data':
        insider_data()
    elif sys.argv[1] == 'factor_insider':
        factor_insider()


