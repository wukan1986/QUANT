# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 09:04:08 2017

@author: shiyunchao
"""

import pandas as pd
import cx_Oracle
from time import strftime, localtime, time
import numpy as np
import os


class factorGet(object):
    def __init__(self,sql,flag,factor_filename,day_range,fre_shift=0,date = None, fre='month'):
        #database information
        # self.DB_HOST = '10.180.10.139:1521/WINDB'
        # self.DB_USER = 'rwind'
        # self.DB_PASSWORD = 'rwind'
        self.DB_HOST = '10.180.10.179:1521/GOGODB'
        self.DB_USER = 'ctquant2'
        self.DB_PASSWORD = 'ctquant2'

        self.sql = sql
        self.flag = flag   # 0 回测 1更新
        self.fre_shift = fre_shift

        self.day_range = day_range  ##      

        self.dirpath = 'raw_data'   # 新建data文件夹 存各种因子数据
        self.factor_filename = factor_filename  # data文件夹里 具体因子的文件夹

#        self.factor_path = self.dirpath + '/' + self.factor_filename + '_' + self.cal_mode + '_' + str(self.fre_shift)
        self.factor_path = self.dirpath + '/' + self.factor_filename
        #确定运行日期
        if date == None:
            self.today=strftime("%Y%m%d",localtime())
        else:
            self.today = date
        self.initial = '20060101'

        self.all_trade_day =  self.wind_trade_day()   ##用于定位一段交易日区间
        self.trade_day =  self.tradeday_to_month_fre()
#        self.trade_day = self.trade_day.iloc[:-1]

#        self.prevDate = self.getPrevTradingDays(self.today)
        if not os.path.exists(self.dirpath):
            os.mkdir(self.dirpath)

        if not os.path.exists('%s'%(self.factor_path)):
            os.mkdir('%s'%(self.factor_path))

    def wind_trade_day(self):
        sql="select TRADE_DAYS from ASHARECALENDAR where (S_INFO_EXCHMARKET='SZSE') and (TRADE_DAYS>=%s) and (TRADE_DAYS<=%s)"%(self.initial, self.today)
        trade_day = self.db_query(sql)
        trade_day = trade_day['TRADE_DAYS'].sort_values()
        trade_day.index = range(len(trade_day))
        return trade_day

    def tradeday_to_month_fre(self):
        all_day = self.wind_trade_day()
        all_day = pd.DataFrame(all_day.values,index= range(len(all_day)))
        all_day.columns = ['TRADE_DAYS']
        all_day['group'] = all_day['TRADE_DAYS'].apply(lambda x: x[:6])

        all_day = all_day.groupby(['group']).apply(lambda x: x.iloc[-1])
        all_day = all_day[['TRADE_DAYS']]
        all_day.index = range(len(all_day))
#        all_day = all_day.iloc[:-1]   # 截止到上月越底
        return all_day['TRADE_DAYS']

    def db_query(self,query):
        db=cx_Oracle.connect(self.DB_USER,self.DB_PASSWORD,self.DB_HOST)
        data = pd.read_sql(query, db)
        db.close()
        return data

    ## 其实这步不用排序的  排序只是为了手动看数据方便
    def db_clean(self,data):
        data.sort_values(data.columns.tolist()[:2], inplace=True)
        return data


    def get_timerange(self,today):
#        today = trade_day.iloc[50]
        end_index = pd.Index(self.all_trade_day).get_loc(today)
        start_index = end_index - self.day_range
        s_trade_day = self.all_trade_day.iloc[start_index:end_index]
        return s_trade_day

    def backtest_or_update(self):
        if self.flag == 0:
            self.part_trade_day = self.trade_day[1:-1]
            for today1 in self.part_trade_day:
    #        today = '20110412'
                ttm_df = self.get_current_factor(today1)
                ttm_df.to_csv('%s/%s_raw_CN_%s.csv'%(self.factor_path,self.factor_filename, today1),index=None)
#                return ttm_df

        # 用于测试历史某一天的
#            today1 = '20110412'
#            ttm_df = self.get_current_factor(today1)
#            ttm_df.to_csv('%s/%s_%s_%s.csv'%(self.factor_path,self.factor_filename, self.cal_mode, today1),index=None)
#            return ttm_df
        elif self.flag == 1:
            ttm_df = self.get_current_factor(self.today)
            ttm_df.to_csv('%s/%s_raw_CN_%s.csv'%(self.factor_path,self.factor_filename, self.today),index=None)


    def get_current_factor(self,today):
        s_trade_day = self.get_timerange(today)
        sql1 = self.sql + " where trade_dt >= %s and trade_dt <= %s"%(s_trade_day.iloc[0], s_trade_day.iloc[-1])
        df = self.db_query(sql1)
        df = self.db_clean(df)

        col_name = df.columns.tolist()[0]

        '''
        可以理解为求和的时候nan值都用0 来填充了  都是nan的一列 最后会等于0   ln之后就是负无穷！！！！！！
        所以都是0 的和都是nan的一列最后结果是一样的  负无穷
        最后把负无穷替换为np.nan
        '''
        df1 = df[df['TRADE_DT'].isin(s_trade_day.values.tolist())]
        df1 = df1.pivot(index='TRADE_DT', columns='S_INFO_WINDCODE', values='S_DQ_FREETURNOVER')


        filter_pct = 0.6#2/float(3)
        num = self.day_range* filter_pct
        df2 = df1.replace(0,np.nan)
        df3 =df2.isnull().sum(axis=0)

        df3 = df3[df3<=num]
        df2 = df2[df3.index.tolist()]
        dfsum2 = df2.sum()
        dfsum2 = dfsum2.replace(0,np.nan)
        dfsum2 = np.log(dfsum2)


#        df1 = df1.sum()
#        df1 = np.log(df1)
        df1 = dfsum2
        df1 = df1.replace(-np.inf, np.nan)
        df1 = df1.reset_index()
        df1[col_name] = df1[col_name].apply(lambda x: x.split('.')[0] + '-CN')
        df1.columns = ['S_INFO_WINDCODE','%s_raw'%self.factor_filename]
        return df1

if __name__ == "__main__":
    sql="select s_info_windcode,trade_dt,s_dq_freeturnover from AShareEODDerivativeIndicator"

    factor_filename = 'STOM'

    flag = 1

    day_range = 21
    turnover = factorGet(sql,flag,factor_filename,day_range)

    turnover.backtest_or_update()
#    turnover = factorGet(sql,flag,factor_filename,day_range)
#    s_trade_day = turnover.get_timerange(turnover.today)
#    df = turnover.get_current_factor(turnover.today)


#filter_pct = 0.6#2/float(3)
#
#num = day_range* filter_pct
#
#df2 = df1.replace(0,np.nan)
#df3 =df2.isnull().sum(axis=0)
#
#
#dfsum1 = df1.sum()
#dfsum1 = dfsum1.replace(0,np.nan)
#dfsum1 = np.log(dfsum1)
#dfsum1_normal = (dfsum1 - dfsum1.mean())/dfsum1.std()
#dfsum1_normal.hist(bins = 30)
#
#
#df3 = df3[df3<=num]
#df2 = df2[df3.index.tolist()]
#dfsum2 = df2.sum()
#dfsum2 = dfsum2.replace(0,np.nan)
#dfsum2 = np.log(dfsum2)
#
#dfsum2_normal = (dfsum2 - dfsum2.mean())/dfsum2.std()
#dfsum2_normal.hist(bins = 30)







