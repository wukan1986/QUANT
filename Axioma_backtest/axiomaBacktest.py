# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 17:33:31 2017

@author: shiyunchao
"""

import pandas as pd
import numpy as np
import yaml
import os
import uqer
from uqer import DataAPI

client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')


def replaceCode(x):
    if 'XSHE' in x:
        return x.replace('XSHE', 'SZ')
    elif 'XSHG' in x:
        return x.replace('XSHG', 'SH')


def convertTicker(tickerInt):
    if tickerInt[0] == '6':
        return tickerInt.split('-')[0] + '.SH'
    elif tickerInt[0] == '3' or tickerInt[0] == '0':
        return tickerInt.split('-')[0] + '.SZ'


def getRangeRet(ticker, start, end, market_path):
    df1 = pd.read_csv('%s/close_%s.csv' % (market_path, start), dtype={'ticker': str})
    df1 = df1[df1.ticker.isin(ticker)]
    df1.columns = ['ticker', 'close_s']

    df2 = pd.read_csv('%s/close_%s.csv' % (market_path, end), dtype={'ticker': str})
    df2 = df2[df2.ticker.isin(ticker)]
    df2.columns = ['ticker', 'close_e']

    df_total = pd.merge(df1, df2, on='ticker', how='outer')
    df_total = df_total.set_index('ticker')
    df_total['ret'] = (df_total['close_e'] - df_total['close_s']) / df_total['close_s']
    return df_total[['ret']]


def getTradeday(start, end, fre='month'):
    trade_day = DataAPI.TradeCalGet(exchangeCD=u"XSHG", beginDate=start, endDate=end, field=u"", pandas="1")

    trade_day = trade_day[trade_day.isOpen == 1][['calendarDate', 'isWeekEnd', 'isMonthEnd']]

    if fre == 'month':
        df = trade_day[trade_day.isMonthEnd == 1]['calendarDate']
    elif fre == 'week':
        df = trade_day[trade_day.isWeekEnd == 1]['calendarDate']
    elif fre == 'day':
        df = trade_day['calendarDate']
    elif fre == '2week':
        df = trade_day[trade_day.isWeekEnd == 1][['calendarDate', 'isWeekEnd']]
        df = df[1::2]
        df = df['calendarDate']
    else:
        print 'change fre parameter'
        raise ValueError
    df.index = range(len(df))
    df = pd.DataFrame(df)
    df['calendarDate'] = pd.to_datetime(df['calendarDate'], format='%Y-%m-%d')
    df['calendarDate'] = df['calendarDate'].apply(lambda x: x.strftime("%Y%m%d"))
    df = df['calendarDate']
    return df


def get_ret_lag(factor, lag, dirpath, market_path, start, end):
    for factor_name in factor:
        print(factor_name)
        filelist = os.listdir('%s/%s_finalhodings' % (dirpath, factor_name))
        trade_day_file = map(lambda x: (x.split('_')[1]).split('.')[0], filelist)

        trade_day = getTradeday(start, end, fre='month')
        final_day = trade_day[trade_day > trade_day_file[-1]].iloc[0]

        trade_day = trade_day[(trade_day >= start) & (trade_day <= end)]
        trade_day = trade_day.values.tolist()
        trade_day = trade_day + [final_day]

        ret_lag_summary = pd.DataFrame([], columns=['ret%s' % j for j in range(lag + 1)])

        for loop in range(lag + 1):
            print(loop)
            for num in range(len(filelist)):
                #            print('##################################')
                #            print(filelist[num])
                try:
                    tt = trade_day[num]
                    t0 = trade_day[num + loop]
                    t1 = trade_day[num + loop + 1]
                    #            print(t0)
                    #            print(t1)
                    #            tday_part = trade_day[num:(num + lag + 2)]

                    df = pd.read_csv('%s/%s_finalhodings/%s' % (dirpath, factor_name, filelist[num]), index_col=0)
                    df = df.dropna()
                    df['Ticker'] = df['Ticker'].apply(lambda x: x.split('-')[0])
                    df.set_index('Ticker', inplace=True)

                    ret0 = getRangeRet(df.index.tolist(), t0, t1, market_path)
                    ret0.columns = ['ret%s' % loop]
                    # del df['return']
                    df = pd.merge(df, ret0, how='left', left_index=True, right_index=True)

                    initialAsset = (df['shares'] * df['price']).sum()
                    ret_asset = (df['shares'] * df['price'] * df['ret%s' % loop]).sum()
                    ret_lag_summary.loc[tt, 'ret%s' % loop] = ret_asset / float(initialAsset)
                except:
                    ret_lag_summary.loc[tt, 'ret%s' % loop] = np.nan

                    #    ret_lag_summary = ret_lag_summary.dropna()
        ret_lag_summary = ret_lag_summary.reset_index()
        ret_lag_summary.columns = ['Period'] + ret_lag_summary.columns.tolist()[1:]

        #    ret_lag_summary = ret_lag_summary.iloc[:-1]

        ret_lag_summary.to_csv('%s/%s_lagret.csv' % (dirpath, factor_name), index=None)


#

if __name__ == "__main__":
    #    dirpath = 'uqer_factorAxioma'#'output_2'#'negative_factor' #'local_model'  # 'output'#
    #    outputpath = 'report_uqer_factorAxioma' #'report_output_2' #'report_uqer_factor'#'report_negative_factor' #'report_localModel'  # 'report_axiomaModel'
    #    market_path = 'D:/Project/axioma_backtest/compute_lag/Market_uqer'
    #
    #    if not os.path.exists(outputpath):
    #        os.makedirs(outputpath)
    #
    #    with open('config.yaml') as f:
    #        temp = yaml.load(f.read())
    #    lag = temp['lag']
    #
    #    factor = temp['factor_name']
    #    factor = factor.split(',')
    #    get_ret_lag(factor, lag, dirpath, market_path)

    trade_day = getTradeday('20061201', '20171001', fre='month')
