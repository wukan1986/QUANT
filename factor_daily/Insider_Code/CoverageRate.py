# -*- coding: UTF-8 -*-

import cx_Oracle
import pandas as pd
import sys
import numpy as np
import GetOracle as orl
import GetTradeDay as td
import GetMarketData as md
import os

factors = ['PV', 'MJRR', 'MJRV', 'SA', 'SL', 'Accrual', 'Accrual', 'LIO', 'LIOCHANGE']
path = 'D:/work/Factors/Insider/'
path300 = 'Z:/axioma_data/benchmark/000300/'
path500 = 'Z:/axioma_data/benchmark/000905/'
pathuniverse = 'Z:/axioma_data/universe/'
begt = '20080101'
endt = '20171130'
factortype = 'neut' # neut
dates = td.tdays(begt, endt, 'M')
CR_A = pd.DataFrame()
CR_CSI300 = pd.DataFrame()
CR_CSI500 = pd.DataFrame()
CR_Universe = pd.DataFrame()

for i in range(len(dates)):
    print(dates[i])
    begt = td.tdaysoffset(dates[i], -1, 'Y')
    A = md.GetAllStocks_byDate(dates[i])
    A = A[A.LISTDATE < begt]
    CSI300 = pd.read_csv(path300 + 'benchmark_' + dates[i] + '.csv', header=None)
    CSI500 = pd.read_csv(path500 + 'benchmark_' + dates[i] + '.csv', header=None)
    Universe = pd.read_csv(pathuniverse + 'universe_' + dates[i] + '.csv', header=None)
    for j in range(len(factors)):
        factorpath = path + factors[j] + '/'
        data = pd.read_csv(path + factors[j] + '/' + factors[j] + '_' + factortype + '_CN_' + dates[i] + '.csv')
        data_300 = data[data['S_INFO_WINDCODE'].isin(CSI300[0])]
        data_500 = data[data['S_INFO_WINDCODE'].isin(CSI500[0])]
        data_un = data[data['S_INFO_WINDCODE'].isin(Universe[0])]
        CR_A.at[i, factors[j]] = len(data) / len(A)
        CR_CSI300.at[i, factors[j]] = len(data_300) / len(CSI300)
        CR_CSI500.at[i, factors[j]] = len(data_500) / len(CSI500)
        CR_Universe.at[i, factors[j]] = len(data_un) / len(Universe)
CR_A.index = list(dates)
CR_CSI300.index = list(dates)
CR_CSI500.index = list(dates)
CR_Universe.index = list(dates)
CR_A.to_csv(path + 'CoverageRate_' + factortype + '_A.csv', index=True)
CR_CSI300.to_csv(path + 'CoverageRate_' + factortype + '_300.csv', index=True)
CR_CSI500.to_csv(path + 'CoverageRate_' + factortype + '_500.csv', index=True)
CR_Universe.to_csv(path + 'CoverageRate_' + factortype + '_Universe.csv', index=True)








