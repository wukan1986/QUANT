# -*- coding: UTF-8 -*-

import cx_Oracle
import pandas as pd
import numpy as np
import math
import sys
import os
import GetOracle as orl
import GetTradeDay as td
import GetMarketData as md
import Insider

begt = '20080131'
endt = '20171130'
N = -1
Fre = 'Y'
Halflife = 180
SApara = {'aetopQ': 1 / 4, 'aebomQ': 1 / 2, 'sizeQ': 5, 'nstbomQ': 1 / 2, 'indtype': 'BARAIND', 'sizetype': 'SIZE'}
dateslist = td.tdays(begt, endt, 'M')
datapath = 'D:/work/Factors/Insider/Insider_Data/'
outputpath = 'D:/work/Factors/Insider/TestSA/'
barapath = 'Z:/axioma_data/barra_model/Exposure/'

for t in range(len(dateslist)):
    print(dateslist[t])

    SA = Insider.GetSA(dateslist[t], N, Fre, Halflife, SApara, datapath)
    SL = SA[['S_INFO_WINDCODE', 'SL_raw']]
    path = outputpath + 'SL'
    if not os.path.exists(path): os.mkdir(path)
    SL.to_csv(path + '/SL_raw_CN_' + dateslist[t] + '.csv', index=None)

    Accrual = SA[['S_INFO_WINDCODE', 'Accrual_raw']]
    path = outputpath + 'Accrual'
    if not os.path.exists(path): os.mkdir(path)
    Accrual.to_csv(path + '/Accrual_raw_CN_' + dateslist[t] + '.csv', index=None)

    Accrualwgt = SA[['S_INFO_WINDCODE', 'Accrualwgt_raw']]
    path = outputpath + 'Accrualwgt'
    if not os.path.exists(path): os.mkdir(path)
    Accrualwgt.to_csv(path + '/Accrualwgt_raw_CN_' + dateslist[t] + '.csv', index=None)

    SA = SA[['S_INFO_WINDCODE', 'SA_raw']]
    path = outputpath + 'SA'
    if not os.path.exists(path): os.mkdir(path)
    SA.to_csv(path + '/SA_raw_CN_' + dateslist[t] + '.csv', index=None)

    # AES = Insider.GetAE(dateslist[t], datapath)
    # AES = AES[['S_INFO_WINDCODE', 'AE_raw']]
    # AES.columns = ['S_INFO_WINDCODE', 'AES_raw']
    # path = outputpath + 'AES'
    # if not os.path.exists(path): os.mkdir(path)
    # AES.to_csv(path + '/AES_raw_CN_' + dateslist[t] + '.csv', index=None)
    #
    # NSTS = Insider.GetNST(dateslist[t], N, Fre, Halflife, datapath)
    # NSTS = NSTS[['S_INFO_WINDCODE', 'NST_raw']]
    # NSTS.columns = ['S_INFO_WINDCODE', 'NSTS_raw']
    # path = outputpath + 'NSTS'
    # if not os.path.exists(path): os.mkdir(path)
    # NSTS.to_csv(path + '/NSTS_raw_CN_' + dateslist[t] + '.csv', index=None)