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

from alphatool import get_cum_ret, caculate_ret, get_decay, get_group_factor, get_group_ret, factor_group_plot
from alphatool import get_eventdrive

import uqer
from uqer import DataAPI

client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')

from tools import get_tradeDay, client_db


class simple_backtest():
    def __init__(self, start, end, fre, factor_name_list, type, benchmark, input_path, universe_path, n_quantile,
                 outputpath, risk_factor_path):
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
        self.risk_factor_path = risk_factor_path

        self.n_quantile = n_quantile
        self.pct_quantiles = 1 / float(self.n_quantile)

        self.outputpath0 = outputpath
        self.getdb = client_db.read_db(type='wind')

    def get_index2(self):
        df_index = DataAPI.MktIdxdGet(tradeDate=u"", indexID=u"", ticker=u"000300", beginDate=self.start,
                                      endDate=self.end,
                                      exchangeCD=u"XSHE,XSHG", field=['tradeDate', 'closeIndex'], pandas="1")
        df_index.columns = ['trade_day', 'close']
        df_index['trade_day'] = df_index['trade_day'].apply(lambda x: x[:4] + x[5:7] + x[8:])
        return df_index

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
        # df_close.index = pd.to_datetime(df_close.index, format='%Y%m%d')
        return df_close

    def get_factor(self):
        # factor_path = 'Z:/axioma_data/alpha/%s' % factor_name
        self.factor_path = '%s/%s' % (self.input_path, self.factor_name)
        df_factor = pd.DataFrame([])
        for i in self.tradeday:
            factor = pd.read_csv('%s/%s_%s_CN_%s.csv' % (self.factor_path, self.factor_name, self.type, i))

            df_universe = pd.read_csv('%s/universe_%s.csv' % (self.universe_path, i), header=None)
            df_universe.columns = ['code', 'value']
            factor = factor[factor['S_INFO_WINDCODE'].isin(df_universe['code'])]

            factor['trade_day'] = i
            df_factor = pd.concat([df_factor, factor])
        df_factor.columns = ['code', self.factor_name, 'trade_day']

        df_factor = df_factor.pivot(index='trade_day', columns='code', values=self.factor_name)
        # df_factor.index = pd.to_datetime(df_factor.index, format='%Y%m%d')
        df_factor.columns = list(map(lambda x: x.split('-')[0], df_factor.columns.tolist()))
        return df_factor

    def get_risk_factor(self):
        df_factor = pd.DataFrame([])
        for i in self.tradeday:
            factor = pd.read_csv('%s/Exposure_%s.csv' % (self.risk_factor_path, i), skiprows=1)
            factor = factor[
                ['Unnamed: 0', 'BETA', 'MOMENTUM', 'SIZE', 'EARNYILD', 'RESVOL', 'GROWTH', 'BTOP', 'LEVERAGE',
                 'LIQUIDTY', 'SIZENL']]
            factor.columns = ['code', 'BETA', 'MOMENTUM', 'SIZE', 'EARNYILD', 'RESVOL', 'GROWTH', 'BTOP', 'LEVERAGE',
                              'LIQUIDTY', 'SIZENL']
            factor['trade_day'] = i
            df_factor = pd.concat([df_factor, factor])

        df_factor.index = range(len(df_factor))
        # df_factor.set_index(['trade_day','code'], inplace=True)
        # a = df_factor.loc['20080430']
        return df_factor

    def data_prepare(self):
        df_index = self.get_index()
        df_close = self.get_close()
        df_factor = self.get_factor()
        df_risk_model = self.get_risk_factor()

        df_ret = df_close.pct_change().shift(-1)

        df_dt = pd.DataFrame(self.tradeday)
        # df_dt['trade_day']= pd.to_datetime(df_dt['trade_day'], format='%Y%m%d')
        # df_index['trade_day']= pd.to_datetime(df_index['trade_day'], format='%Y-%m-%d')
        df_index = pd.merge(df_dt, df_index, on='trade_day')
        df_index['ret'] = df_index['close'].pct_change()
        df_index = df_index[['trade_day', 'ret']]
        df_index['ret'] = df_index['ret'].shift(-1).values
        df_index = df_index.set_index('trade_day')

        weight = pd.DataFrame(index=df_factor.index.tolist(), columns=df_factor.columns)
        weight = weight.fillna(1)
        return df_factor, df_ret, df_index, weight, df_risk_model

    def risk_factor_plot(self):
        df_factor, df_ret, df_index, weight, df_risk_model = self.data_prepare()

        risk_model_name_list = ['BETA', 'MOMENTUM', 'SIZE', 'EARNYILD', 'RESVOL', 'GROWTH', 'BTOP', 'LEVERAGE',
                                'LIQUIDTY', 'SIZENL']

        df_ret = get_group_ret(df_factor, df_ret, df_index, weight, self.n_quantile, self.pct_quantiles)
        for risk_model_name in risk_model_name_list:
            df_risk_group = get_group_factor(df_factor, df_risk_model, weight, self.n_quantile, self.pct_quantiles,
                                             risk_model_name)

            outputpath1 = self.outputpath + '/' + 'risk_factor'
            if not os.path.exists(outputpath1):
                os.mkdir(outputpath1)
            factor_group_plot(self.factor_name, risk_model_name, df_ret, df_risk_group, self.n_quantile,
                              outputpath1)

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
            self.risk_factor_plot()


if __name__ == '__main__':
    start = '20080401'
    end = '20170510'
    fre = 'month'
    factor_name_list = ['DVDRAW']  # 'PMOM1240', 'PMOM1260', 'REVERSE', 'TO_REVERSE'
    type = 'raw'  # 'raw'
    benchmark = "000300"
    n_quantile = 10

    input_path = 'E:/factor_data/raw_data'
    universe_path = 'Z:/axioma_data/universe'
    outputpath = 'E:/QUANT/analyse_factor/fig2'
    risk_factor_path = 'Z:/axioma_data/barra_model/Exposure'

    # input_path = 'Z:/axioma_data/alpha'
    # universe_path = 'Z:/axioma_data/universe'
    # outputpath = 'Z:/backtest_data/simple_backtest'

    test = simple_backtest(start, end, fre, factor_name_list, type, benchmark, input_path, universe_path, n_quantile,
                           outputpath, risk_factor_path)
    test.run()
