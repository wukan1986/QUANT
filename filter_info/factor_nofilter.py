# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 13:56:58 2017

@author: shiyunchao
"""

import pandas as pd
import os
import sys
import numpy as np
from scipy.stats import mstats

sys.path.append("..")
from tools import get_tradeDay

reload(sys)

dirpath = 'E:/factor_data/raw_data'
excludepath = 'E:/QUANT/filter_info/code_exclude'
includepath = 'E:/QUANT/filter_info/code_include'
output = 'E:/factor_data/filter_data'

factor_name = ['stdfcf0', 'stdni0']

start = '20061201'
end = '20171012'

if not os.path.exists(output):
    os.mkdir(output)

fre = 'month'
tradeday = get_tradeDay.wind(start, end, fre=fre)

for factor in factor_name:
    if not os.path.exists('%s/%s' % (output, factor.upper())):
        os.mkdir('%s/%s' % (output, factor.upper()))
    for day in tradeday:
        df = pd.read_csv('%s/%s/%s_%s.csv' % (dirpath, factor, factor, day), header=None)
        df.columns = ['code', 'factor']
        df_include = pd.read_csv('%s/%s.csv' % (includepath, day), usecols=['code'], dtype={'code': str})
        df_include.columns = ['code']
        df_include['code'] = df_include['code'].apply(lambda x: x + '-CN')
        df_exclude = pd.read_csv('%s/%s.csv' % (excludepath, day), usecols=['code'], dtype={'code': str})
        df_exclude.columns = ['code']
        df_exclude['code'] = df_exclude['code'].apply(lambda x: x + '-CN')

        df = df[df['code'].isin(df_include['code'])]

        # 把涨跌停 停牌的都没过滤  去极值 标准化 也都注释掉了
        # df = df[~df['code'].isin(df_exclude['code'])]

        # 取log 负数 根据因子看需不需要
        df = df[df['factor']!=0]
        df['factor'] =  np.log(df['factor'])
        df['factor'] = -df['factor'].values

        # 去掉因子缺失值
        # df = df.dropna()
        # # 5倍标准差以外扔掉
        # mean_df = df['factor'].mean()
        # std3 = 3 * df['factor'].std()
        # df['factor1'] = df['factor'][~((df['factor'] - df['factor'].mean()).abs() > 5 * df['factor'].std())]
        # df = df.dropna()
        # # 拉回三倍标准差
        # df['factor1'][((df['factor1'] - mean_df) > std3)] = mean_df + std3
        # df['factor1'][((-df['factor1'] + mean_df) > std3)] = mean_df - std3
        # # df['factor1'] = mstats.winsorize(df['factor'], limits=0.05)
        # # 标准化
        # df['factor2'] = ((df['factor1'] - df['factor1'].mean()) / df['factor1'].std()).values
        ###
        df = df[['code','factor']]
        df.columns = ['S_INFO_WINDCODE','%s_raw'%factor.upper()]
        df.to_csv('%s/%s/%s_raw_CN_%s.csv' % (output, factor.upper(), factor.upper(), day), index=None)
