# -*- coding: utf-8 -*-
"""
Created on Wed Mar 01 11:04:22 2017

@author: leiton
"""

import alphalens
from alphalens import utils
import pandas as pd
import os
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')
from pylab import mpl   #画图显示中文
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False

# import uqer
# from uqer import DataAPI
# client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')

import sys
sys.path.append("..")
from tools import get_tradeDay, client_db
reload(sys)

import alphalens_func



class simple_backtest():
    def __init__(self, start, end, fre, factor_name_list, type, benchmark, input_path, universe_path, n_quantile,
                 outputpath):
        self.start = start
        self.end = end

        self.fre = fre
        self.factor_name_list = factor_name_list
        self.tradeday = get_tradeDay.wind(self.start, self.end, fre=self.fre)

        self.daily = get_tradeDay.wind(self.start, self.end, fre='day')

        self.type = type
        self.benchmark = benchmark

        self.input_path = input_path
        self.universe_path = universe_path

        self.n_quantile = n_quantile
        self.pct_quantiles = 1 / float(self.n_quantile)

        self.outputpath0 = outputpath
        self.getdb = client_db.read_db(type='wind')

    def get_index(self):

        sql = "select TRADE_DT,S_DQ_CLOSE from AIndexEODPrices where (TRADE_DT>='%s') and (TRADE_DT<='%s') and (S_INFO_WINDCODE = '%s')" % (
            self.start, self.end, self.benchmark + '.SH')
        df_index = self.getdb.db_query(sql)
        df_index.columns = ['trade_day', 'close']
        return df_index

    def get_close(self):
        close_path = 'Z:/backtest_data/close_data/gogo_data'
        df_close = pd.DataFrame([])
        for i in self.tradeday:
            close = pd.read_csv('%s/close_%s.csv' % (close_path, i), dtype={'ticker': str})
            close['trade_day'] = i
            df_close = pd.concat([df_close, close])

        df_close = df_close.pivot(index='trade_day', columns='ticker', values='close_s')
        df_close.index = pd.to_datetime(df_close.index, format='%Y%m%d')
        return df_close

    def get_factor(self):
        # factor_path = 'Z:/axioma_data/alpha/%s' % factor_name
        self.factor_path = '%s/%s' % (self.input_path, self.factor_name)
        df_factor = pd.DataFrame([])
        for i in self.tradeday:
            factor = pd.read_csv('%s/%s_%s_CN_%s.csv' % (self.factor_path, self.factor_name, self.type, i))

            df_universe = pd.read_csv('%s/universe_%s.csv' % (self.universe_path, i),header=None)
            df_universe.columns = ['code', 'value']
            # df_universe['code'] = df_universe['code'].apply(lambda x: x.split('.')[0] + "-CN")
            factor = factor[factor['S_INFO_WINDCODE'].isin(df_universe['code'])]

            factor['trade_day'] = i
            df_factor = pd.concat([df_factor, factor])
        df_factor.columns = ['code', self.factor_name, 'trade_day']
        df_factor = df_factor.drop_duplicates()

        try:
            df_factor = df_factor.pivot(index='trade_day', columns='code', values=self.factor_name)
        except:
            df_factor = pd.pivot_table(df_factor, index=['trade_day'], columns='code', values=self.factor_name)

        df_factor.index = pd.to_datetime(df_factor.index, format='%Y%m%d')
        df_factor.columns = list(map(lambda x: x.split('-')[0], df_factor.columns.tolist()))
        return df_factor

    def get_benchmark(self, df_index):
        df_dt = pd.DataFrame(self.tradeday)
        df_dt['trade_day'] = pd.to_datetime(df_dt['trade_day'], format='%Y%m%d')
        df_index['trade_day'] = pd.to_datetime(df_index['trade_day'], format='%Y-%m-%d')
        df_index = pd.merge(df_dt, df_index, on='trade_day')
        df_index['ret'] = df_index['close'].pct_change()
        df_index = df_index[['trade_day', 'ret']]
        df_index['ret'] = df_index['ret'].shift(-1).values
        df_index = df_index.set_index('trade_day')

        col_index = df_index.columns.tolist()[0]
        df_benchmark = pd.DataFrame([])
        num_group = range(10)
        for i in num_group:
            df_benchmark.loc[:, i + 1] = df_index[col_index]
        df_benchmark = df_benchmark.T.stack()

        df_benchmark = pd.DataFrame(df_benchmark)
        df_benchmark.columns = [col_index]
        df_benchmark.index.names = [u'factor_quantile', u'date']
        test = df_benchmark.loc[(df_benchmark.index.get_level_values(u'factor_quantile') == 1)]
        benchmark_group = df_benchmark.loc[(df_benchmark.index.get_level_values(u'factor_quantile') == 5)]
        return df_benchmark, benchmark_group, col_index

    def data_prepare(self):
        df_index = self.get_index()
        df_close = self.get_close()
        df_factor = self.get_factor()
        df_benchmark, benchmark_group, col_index = self.get_benchmark(df_index)

        return df_factor, df_close, df_benchmark, benchmark_group, col_index


    def backtest(self):
        df_factor, df_close, df_benchmark, benchmark_group,col_index = self.data_prepare()
        alphalens_func.plot(self.outputpath, df_factor, df_close, df_benchmark, benchmark_group, col_index)


    def run(self):
        for factor_name in self.factor_name_list:
            print(factor_name)
            self.factor_name = factor_name
            self.outputpath = '%s/%s' % (self.outputpath0, self.factor_name)
            if not os.path.exists(self.outputpath):
                os.mkdir(self.outputpath)
            self.outputpath = '%s/%s' % (self.outputpath, self.type)
            if not os.path.exists(self.outputpath):
                os.mkdir(self.outputpath)

            self.backtest()

if __name__ == '__main__':
    start = '20080401'
    # start = '20131201'
    end = '20171210'
    fre = 'month'

    factor_name_list = ['NISR2']
    factor_name = 'NISR2'
    type = 'neut'  # 'raw'
    benchmark = "000300"
    n_quantile = 10

    input_path = 'Z:/axioma_data/alpha'
    # # input_path = 'F:/factor_data/derivative'
    # input_path = 'F:/factor_data/ba'
    universe_path = 'Z:/axioma_data/universe'
    outputpath = 'Z:/syc/simple_backtest'

    test = simple_backtest(start, end, fre, factor_name_list, type, benchmark, input_path, universe_path, n_quantile,
                           outputpath)
    test.run()




