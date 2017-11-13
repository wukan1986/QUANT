# -*- coding: utf-8 -*-


import pandas as pd
import os
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import cx_Oracle
matplotlib.style.use('ggplot')
from pylab import mpl

mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False

import uqer
from uqer import DataAPI

client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')

import sys

sys.path.append("..")
from tools import get_tradeDay

reload(sys)

start = '20080401'
end = '20170510'

df_index = DataAPI.MktIdxdGet(tradeDate=u"", indexID=u"", ticker=u"000300", beginDate=start, endDate=end,
                              exchangeCD=u"XSHE,XSHG", field=['tradeDate', 'closeIndex'], pandas="1")
df_index.columns = ['trade_day', 'close']
df_index['trade_day'] = df_index['trade_day'].apply(lambda x: x[:4] + x[5:7] + x[8:])



fre = 'month'
tradeday = get_tradeDay.wind(start, end, fre=fre)

daily = get_tradeDay.wind(start, end, fre='day')

factor_name = 'fcff_sign_r2'
# factor_path = 'Z:/axioma_data/alpha/%s' % factor_name
factor_path = 'E:/factor_data/filter_data/%s'%factor_name
universe_path = 'Z:/axioma_data/universe'

outputpath = 'E:/QUANT/analyse_factor/fig2'
outputpath = '%s/%s' % (outputpath, factor_name)

if not os.path.exists(outputpath):
    os.mkdir(outputpath)

close_path = 'Z:/backtest_data/close_data/gogo_data'
df_close = pd.DataFrame([])
for i in tradeday:
    print(i)
    close = pd.read_csv('%s/close_%s.csv' % (close_path, i), dtype={'ticker': str})
    close['trade_day'] = i
    df_close = pd.concat([df_close, close])

df_close = df_close.pivot(index='trade_day', columns='ticker', values='close_s')
# df_close.index = pd.to_datetime(df_close.index, format='%Y%m%d')

df_factor = pd.DataFrame([])
for i in tradeday:
    factor = pd.read_csv('%s/%s_raw_CN_%s.csv' % (factor_path, factor_name, i))

    df_universe = pd.read_csv('%s/universe_%s.csv' % (universe_path, i), header=None)
    df_universe.columns = ['code', 'value']
    factor = factor[factor['S_INFO_WINDCODE'].isin(df_universe['code'])]

    factor['trade_day'] = i
    df_factor = pd.concat([df_factor, factor])
df_factor.columns = ['code', factor_name, 'trade_day']

df_factor = df_factor.pivot(index='trade_day', columns='code', values=factor_name)
# df_factor.index = pd.to_datetime(df_factor.index, format='%Y%m%d')
df_factor.columns = list(map(lambda x: x.split('-')[0], df_factor.columns.tolist()))

df_ret = df_close.pct_change().shift(-1)

df_dt = pd.DataFrame(tradeday)
# df_dt['trade_day']= pd.to_datetime(df_dt['trade_day'], format='%Y%m%d')
# df_index['trade_day']= pd.to_datetime(df_index['trade_day'], format='%Y-%m-%d')
df_index = pd.merge(df_dt, df_index, on='trade_day')
df_index['ret'] = df_index['close'].pct_change()
df_index = df_index[['trade_day', 'ret']]
df_index['ret'] = df_index['ret'].shift(-1).values
df_index = df_index.set_index('trade_day')

n_quantile = 10
pct_quantiles = 1 / float(n_quantile)

weight = pd.DataFrame(index=df_factor.index.tolist(), columns=df_factor.columns)
weight = weight.fillna(1)

from alphatool import get_cum_ret, caculate_ret, get_decay

get_cum_ret(df_factor, df_ret, df_index, weight, factor_name, n_quantile, pct_quantiles, outputpath)

lag_num = 6


#####################################################


get_decay(df_factor, df_ret, df_index, weight, lag_num, factor_name, n_quantile, pct_quantiles, outputpath)

daily = get_tradeDay.wind('20060104', '20171109', fre='day')
daily = daily.values

df_index_daily = DataAPI.MktIdxdGet(tradeDate=u"", indexID=u"", ticker=u"000300", beginDate='20060104',
                                    endDate='20171109',
                                    exchangeCD=u"XSHE,XSHG", field=['tradeDate', 'closeIndex'], pandas="1")
df_index_daily.columns = ['trade_day', 'close']
df_index_daily['trade_day'] = df_index_daily['trade_day'].apply(lambda x: x[:4] + x[5:7] + x[8:])

# close_path = 'E:/QUANT/Market_data/gogo_data/'
# df_close_daily = pd.DataFrame([])
# for i in daily:
#     print(i)
#     close = pd.read_csv('%s/close_%s.csv' % (close_path, i), dtype={'ticker': str})
#     close['trade_day'] = i
#     df_close_daily = pd.concat([df_close_daily, close])
#
# df_close_daily = df_close_daily.pivot(index='trade_day', columns='ticker', values='close_s')
# df_close_daily.to_csv('close.csv')
df_close_daily = pd.read_csv('close.csv', dtype={'trade_day': str})
df_close_daily = df_close_daily.set_index('trade_day')
df_ret_daily = df_close_daily.pct_change().shift(-1)
df_index_daily['ret'] = df_index_daily['close'].pct_change()
df_index_daily = df_index_daily[['trade_day', 'ret']]
df_index_daily['ret'] = df_index_daily['ret'].shift(-1).values
df_index_daily = df_index_daily.set_index('trade_day')
df_index_daily.columns = ['benchmark']

date_range = 20

from alphatool import get_eventdrive

get_eventdrive(df_factor, daily, df_ret_daily, df_index_daily, date_range, factor_name, n_quantile, pct_quantiles,
               outputpath)
