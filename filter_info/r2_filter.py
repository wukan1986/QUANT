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

factor_name = ['fcff'] #['net_income']

start = '20061201'
end = '20170901'

if not os.path.exists(output):
    os.mkdir(output)

fre = 'month'
tradeday = get_tradeDay.wind(start, end, fre=fre)


def standard(df, col):
    df1 = df.copy()
    df1.columns = ['code','factor']
    # 去掉因子缺失值
    df1 = df1.dropna()
    # 5倍标准差以外扔掉
    mean_df = df1['factor'].mean()
    std3 = 3 * df1['factor'].std()
    df1['factor1'] = df1['factor'][~((df1['factor'] - df1['factor'].mean()).abs() > 5 * df1['factor'].std())]
    df1 = df1.dropna()

    # 拉回三倍标准差
    df1['factor1'][((df1['factor1'] - mean_df) > std3)] = mean_df + std3
    df1['factor1'][((-df1['factor1'] + mean_df) > std3)] = mean_df - std3
    # df['factor1'] = mstats.winsorize(df['factor'], limits=0.05)
    # 标准化
    df1['factor2'] = ((df1['factor1'] - df1['factor1'].mean()) / df1['factor1'].std()).values
    ###
    df1= df1[['code','factor2']]
    df1.columns = col
    return df1

for factor in factor_name:
    for i in ['coef','r2','sign_r2','coef_r2']:
        if not os.path.exists('%s/%s_%s' % (output, factor, i)):
            os.mkdir('%s/%s_%s' % (output, factor, i))
    for day in tradeday:
        df = pd.read_csv('%s/%s/%s.csv' % (dirpath, factor, day))
        df_include = pd.read_csv('%s/%s.csv' % (includepath, day), usecols=['code'], dtype={'code': str})
        df_include.columns = ['code']
        df_include['code'] = df_include['code'].apply(lambda x: x + '-CN')
        df_exclude = pd.read_csv('%s/%s.csv' % (excludepath, day), usecols=['code'], dtype={'code': str})
        df_exclude.columns = ['code']
        df_exclude['code'] = df_exclude['code'].apply(lambda x: x + '-CN')

        df = df[df['code'].isin(df_include['code'])]
        df = df[~df['code'].isin(df_exclude['code'])]
        df.index = range(len(df))

        df['sign_r2'] = np.sign(df['coef'])*df['r2']
        df['coef_r2'] = df['coef']*df['r2']

        # df = df[['code','coef_r2']]
        # df.columns = ['code','factor']
        # # 去掉因子缺失值
        # df = df.dropna()
        # # 5倍标准差以外扔掉
        # mean_df = df['factor'].mean()
        # std3 = 3 * df['factor'].std()
        # df['factor1'] = df['factor'][~((df['factor'] - df['factor'].mean()).abs() > 5 * df['factor'].std())]
        # df = df.dropna()
        #
        # # 拉回三倍标准差
        # df['factor1'][((df['factor1'] - mean_df) > std3)] = mean_df + std3
        # df['factor1'][((-df['factor1'] + mean_df) > std3)] = mean_df - std3
        # # df['factor1'] = mstats.winsorize(df['factor'], limits=0.05)
        # # 标准化
        # df['factor2'] = ((df['factor1'] - df['factor1'].mean()) / df['factor1'].std()).values
        # ###
        # df = df[['code','factor2']]
        # df.columns = ['code','coef_r2']
        col_name = df.columns.tolist()[1:]
        for ii in col_name:
            name = factor + "_" + ii
            df2 = df[['code',ii]]
            df1 = standard(df2, df2.columns.tolist())
            df1.columns = ['S_INFO_WINDCODE', '%s_raw' % name.upper()]
            df1.to_csv('%s/%s/%s_raw_CN_%s.csv' % (output, name, name, day), index=None)


