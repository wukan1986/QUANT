# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 09:04:08 2017

@author: shiyunchao
"""

import pandas as pd
from time import strftime, localtime
import numpy as np
import os
from tools import get_tradeDay, client_db


class factorGet(object):
    def __init__(self, start, sql, flag, factor_filename, day_range, date=None):
        self.sql = sql
        self.flag = flag  # 0 回测 1更新

        self.day_range = day_range  ##

        self.dirpath = 'raw_data'  # 新建data文件夹 存各种因子数据
        self.factor_filename = factor_filename  # data文件夹里 具体因子的文件夹

        self.factor_path = self.dirpath + '/' + self.factor_filename
        # 确定运行日期
        if date == None:
            self.today = strftime("%Y%m%d", localtime())
        else:
            self.today = date
        self.initial = '20170101'   # 留点窗口 为了下面start往前推能取到日期
        self.start = start

        self.all_trade_day = get_tradeDay.wind(self.initial, self.today, fre='day')  ##用于定位一段交易日区间
        self.trade_day = get_tradeDay.wind(self.start, self.today, fre='day')
        #        self.trade_day = self.trade_day.iloc[:-1]

        self.getdb = client_db.read_db(type='ctquant2')

        if not os.path.exists(self.dirpath):
            os.mkdir(self.dirpath)

        if not os.path.exists('%s' % (self.factor_path)):
            os.mkdir('%s' % (self.factor_path))

    ## 其实这步不用排序的  排序只是为了手动看数据方便
    def db_clean(self, data):
        data.sort_values(data.columns.tolist()[:2], inplace=True)
        return data

    def get_timerange(self, today):
        end_index = pd.Index(self.all_trade_day).get_loc(today)
        start_index = end_index - self.day_range
        s_trade_day = self.all_trade_day.iloc[start_index:end_index]
        return s_trade_day

    def backtest_or_update(self):
        if self.flag == 0:
        #     self.part_trade_day = self.trade_day[1:-1]
            for today1 in self.trade_day:
                #        today = '20110412'
                ttm_df = self.get_current_factor(today1)
                ttm_df.to_csv('%s/%s_raw_CN_%s.csv' % (self.factor_path, self.factor_filename, today1), index=None)
        elif self.flag == 1:
            ttm_df = self.get_current_factor(self.today)
            ttm_df.to_csv('%s/%s_raw_CN_%s.csv' % (self.factor_path, self.factor_filename, self.today), index=None)

    def get_current_factor(self, today):
        s_trade_day = self.get_timerange(today)
        sql1 = self.sql + " where trade_dt >= %s and trade_dt < %s" % (s_trade_day.iloc[0], s_trade_day.iloc[-1])
        df = self.getdb.db_query(sql1)
        df = self.db_clean(df)

        col_name = df.columns.tolist()[0]

        '''
        可以理解为求和的时候nan值都用0 来填充了  都是nan的一列 最后会等于0   ln之后就是负无穷！！！！！！
        所以都是0 的和都是nan的一列最后结果是一样的  负无穷
        最后把负无穷替换为np.nan
        '''
        df1 = df[df['TRADE_DT'].isin(s_trade_day.values.tolist())]
        df1 = df1.pivot(index='TRADE_DT', columns='S_INFO_WINDCODE', values='S_DQ_FREETURNOVER')
        df1 = df1.sum()
        df1 = np.log(df1)

        df1 = df1.replace(-np.inf, np.nan)
        df1 = df1.reset_index()
        df1[col_name] = df1[col_name].apply(lambda x: x.split('.')[0] + '-CN')
        df1.columns = ['S_INFO_WINDCODE', '%s_raw' % self.factor_filename]
        return df1


if __name__ == "__main__":
    start = '20170101'
    sql = "select s_info_windcode,trade_dt,s_dq_freeturnover from AShareEODDerivativeIndicator"

    factor_filename = 'STOM'

    flag = 1

    day_range = 21
    turnover = factorGet(start, sql, flag, factor_filename, day_range)

    # turnover.backtest_or_update()
    df = turnover.get_current_factor('20171130')
