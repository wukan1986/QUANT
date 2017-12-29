# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 09:36:06 2017

@author: shiyunchao
"""

import pandas as pd
from time import strftime, localtime
import numpy as np
import os
import sys
sys.path.append("F:/QUANT/")
reload(sys)
from tools import get_tradeDay, client_db


def splist(L, s):
    return [L[i:i + s] for i in range(len(L)) if i % s == 0]


class factorGet(object):
    def __init__(self, start, flag, fre, factor_filename, day_range, dirpath, date=None):
        self.sql = "select s_info_windcode,trade_dt,s_dq_freeturnover from AShareEODDerivativeIndicator"
        self.flag = flag  # 0 回测 1更新

        self.day_range = day_range  ##

        self.dirpath = dirpath  # 新建data文件夹 存各种因子数据
        self.factor_filename = factor_filename  # data文件夹里 具体因子的文件夹

        self.factor_path = self.dirpath + '/' + self.factor_filename
        # 确定运行日期
        if date == None:
            self.today = strftime("%Y%m%d", localtime())
        else:
            self.today = date
        self.initial = '20080101'  # 留点窗口 为了下面start往前推能取到日期
        self.start = start
        self.fre = fre

        self.all_trade_day = get_tradeDay.wind(self.initial, self.today, fre='day')  ##用于定位一段交易日区间
        self.trade_day = get_tradeDay.wind(self.start, self.today, fre=self.fre)
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
        s_trade_day = self.all_trade_day.iloc[(start_index+1):(end_index+1)]
        return s_trade_day

    def get_current_factor(self, today):
        s_trade_day = self.get_timerange(today)
        sql1 = self.sql + " where trade_dt >= %s and trade_dt <= %s" % (s_trade_day.iloc[0], s_trade_day.iloc[-1])
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

    def stox(self, today, trail_range, new_factorname):
        list_len = self.day_range * trail_range
        all_trade_day1 = self.all_trade_day[self.all_trade_day <= today]
        part_trade_day = all_trade_day1.iloc[-list_len:]
        part_trade_day = splist(part_trade_day.values.tolist(), self.day_range)
        last_day_list = [ii[-1] for ii in part_trade_day]

        df = pd.DataFrame(columns=['code'])
        for day1 in last_day_list:
            df1 = self.get_current_factor(day1)
            df1.columns = ['code', day1]
            df = pd.merge(df, df1, on='code', how='outer')

        df = df.set_index('code')
        df = np.exp(df)
        # df = df.sum(axis=1)
        # df = np.log(df / float(trail_range))
        df = df.mean(axis=1)
        df = np.log(df)
        df = df.replace(-np.inf, np.nan)

        df = pd.DataFrame(df)
        df = df.reset_index()
        df.columns = ['S_INFO_WINDCODE', '%s_raw' % new_factorname]
        # newpath = dirpath + '/' + new_factorname + '/' + new_factorname \
        #           + '_raw_CN_' + day1 + '.csv'
        # df.to_csv(newpath, index=None)
        return df

    def to3_to1(self, today):
        to3 = self.stox(today, 3, 'STOQ')
        to1 = self.stox(today, 1, "STOM")
        to = pd.merge(to3, to1, on='S_INFO_WINDCODE')
        to['%s_raw' % self.factor_filename] = to['STOQ_raw'] / to['STOM_raw']
        df_to = to[['S_INFO_WINDCODE', '%s_raw' % self.factor_filename]]
        '''
        1 表示之前两个月的换手率都是np.nan 相当于最近1个月除以自己 没有意义 所以变成缺失值 
        '''
        df_to = df_to.replace(1, np.nan)
        df_to.to_csv('%s/%s/%s_raw_CN_%s.csv' % (self.dirpath, self.factor_filename, self.factor_filename, today),
                     index=None)

    def run(self):
        if self.flag == 0:
            #     self.part_trade_day = self.trade_day[1:-1]
            for today1 in self.trade_day:
                #        today = '20110412'
                # ttm_df = self.get_current_factor(today1)
                self.to3_to1(today1)
                # ttm_df.to_csv('%s/%s_raw_CN_%s.csv' % (self.factor_path, self.factor_filename, today1), index=None)
        elif self.flag == 1:
            # ttm_df = self.get_current_factor(self.today)
            self.to3_to1(self.today)
            # ttm_df.to_csv('%s/%s_raw_CN_%s.csv' % (self.factor_path, self.factor_filename, self.today), index=None)


if __name__ == "__main__":

    # 每天更新
    start = '20170701'
    factor_filename = 'TO_REVERSE'
    flag = 1
    day_range = 21
    fre = 'day'
    dirpath = 'Z:/daily_data/alpha'
    turnover = factorGet(start, flag, fre, factor_filename, day_range, dirpath, date='20171211')
    turnover.run()


    # turnover.backtest_or_update()
    # df = turnover.get_current_factor('20171130')
    # turnover.stox('20171130', 3, 'raw_data', 'test')
    # turnover.to3_to1('20171130')

    # # 月频率回测
    # start = '20150101'
    # factor_filename = 'TO_REVERSE'
    # flag = 0
    # day_range = 21
    # fre = 'month'
    # dirpath = 'F:/factor_data/test_data'
    # turnover = factorGet(start, flag, fre, factor_filename, day_range, dirpath, date='20171208')
    # turnover.run()