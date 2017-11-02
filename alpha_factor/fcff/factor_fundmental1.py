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
from retrying import retry
import time as dt


class factorGet(object):
    def __init__(self, sql, cal_mode, flag, factor_filename, fre_shift=0, date=None, fre='month'):
        # database information
        # self.DB_HOST = '10.180.10.139:1521/WINDB'
        # self.DB_USER = 'rwind'
        # self.DB_PASSWORD = 'rwind'
        self.DB_HOST = '10.180.10.179:1521/GOGODB'
        self.DB_USER = 'ctquant2'
        self.DB_PASSWORD = 'ctquant2'

        self.sql = sql
        self.flag = flag  # 0 回测 1更新

        #  4种模式 current最新的一个季度的值  latest yeartoday  ttm最近四季度之和  year_ave  四季度平均值 用于总资产这种数据计算过去12个月平均值
        self.cal_mode = cal_mode
        self.fre_shift = fre_shift  ##

        self.dirpath = 'raw_data'  # 新建data文件夹 存各种因子数据
        self.factor_filename = factor_filename  # data文件夹里 具体因子的文件夹

        self.factor_path = self.dirpath + '/' + self.factor_filename + '_' + self.cal_mode + '_' + str(self.fre_shift)
        # 确定运行日期
        if date == None:
            self.today = strftime("%Y%m%d", localtime())
        else:
            self.today = date
        self.initial = '20061201'
        if fre == 'day':
            self.trade_day = self.wind_trade_day()
        elif fre == 'month':
            self.trade_day = self.tradeday_to_month_fre()

        # self.prevDate = self.getPrevTradingDays(self.today)
        if not os.path.exists(self.dirpath):
            os.mkdir(self.dirpath)

        if not os.path.exists('%s' % (self.factor_path)):
            os.mkdir('%s' % (self.factor_path))

    def wind_trade_day(self):
        sql = "select TRADE_DAYS from ASHARECALENDAR where (S_INFO_EXCHMARKET='SZSE') and (TRADE_DAYS>=%s) and (TRADE_DAYS<=%s)" % (
            self.initial, self.today)
        trade_day = self.db_query(sql)
        trade_day = trade_day['TRADE_DAYS'].sort_values()
        trade_day.index = range(len(trade_day))
        return trade_day

    def tradeday_to_month_fre(self):
        all_day = self.wind_trade_day()
        all_day = pd.DataFrame(all_day.values, index=range(len(all_day)))
        all_day.columns = ['TRADE_DAYS']
        all_day['group'] = all_day['TRADE_DAYS'].apply(lambda x: x[:6])

        all_day = all_day.groupby(['group']).apply(lambda x: x.iloc[-1])
        all_day = all_day[['TRADE_DAYS']]
        all_day.index = range(len(all_day))
        all_day = all_day.iloc[:-1]  # 截止到上月越底
        return all_day['TRADE_DAYS']

    def db_query(self, query):
        db = cx_Oracle.connect(self.DB_USER, self.DB_PASSWORD, self.DB_HOST)
        data = pd.read_sql(query, db)
        db.close()
        return data

    def db_clean1(self, data):
        reprot_type = ['408001000', '408004000', '408005000']
        data = data[data.STATEMENT_TYPE.isin(reprot_type)]
        data.sort_values(['WIND_CODE', 'REPORT_PERIOD', 'ANN_DT', 'STATEMENT_TYPE'], inplace=True)
        return data

    def db_clean(self, data):
        data.sort_values(['WIND_CODE', 'REPORT_PERIOD', 'ANN_DT'], inplace=True)
        return data

    def cal_current_latest_ttm(self, df_tmp1, s_name):
        if self.cal_mode == 'ttm':
            return self.cal_ttm(df_tmp1, s_name)
        elif self.cal_mode == 'latest':
            return self.cal_latest(df_tmp1, s_name)
        elif self.cal_mode == 'current':
            return self.cal_current(df_tmp1, s_name)
        elif self.cal_mode == 'year_ave':
            return self.cal_year_average(df_tmp1, s_name)
        else:
            raise ValueError("latest_ttm key error : Please input 'latest' or 'ttm'")

    def cal_ttm(self, df_tmp1, s_name):
        try:
            df_tmp1 = df_tmp1.iloc[:(len(df_tmp1) - self.fre_shift)]
            type_current = df_tmp1.index.tolist()[-1]
            type_last_current = str(int(type_current[:4]) - 1) + type_current[4:]  # 去年同期
            type_last_year = str(int(type_current[:4]) - 1) + '1231'

            indicator_current = df_tmp1.ix[type_current, s_name]
            indicator_last_current = df_tmp1.ix[type_last_current, s_name]
            indicator_last_year = df_tmp1.ix[type_last_year, s_name]

            indicator_ttm = indicator_current + indicator_last_year - indicator_last_current
        except:
            indicator_ttm = np.nan  ###出现这种情况下 一般就是还没上市 所以财报不全 里面没有 无法索引
        return indicator_ttm

    def cal_latest(self, df_tmp1, s_name):
        try:
            df_tmp1 = df_tmp1.iloc[:(len(df_tmp1) - self.fre_shift)]
            type_current = df_tmp1.index.tolist()[-1]
            indicator_latest = df_tmp1.ix[type_current, s_name]
        except:
            indicator_latest = np.nan  ###出现这种情况下 一般就是还没上市 所以财报不全 里面没有 无法索引
        return indicator_latest

    def convertForcurrnet(self, period):
        if period[4:6] == '03':
            last_period = period
        elif period[4:6] == '06':
            last_period = period[:4] + '0331'
        elif period[4:6] == '09':
            last_period = period[:4] + '0630'
        elif period[4:6] == '12':
            last_period = period[:4] + '0930'
        return last_period

    def cal_current(self, df_tmp1, s_name):
        try:
            df_tmp1 = df_tmp1.iloc[:(len(df_tmp1) - self.fre_shift)]
            type_current = df_tmp1.index.tolist()[-1]
            if type_current[4:6] != '03':
                indicator_current = df_tmp1.ix[type_current, s_name]
                type_current_last = self.convertForcurrnet(type_current)  # 上一期
                indicator_current_last = df_tmp1.ix[type_current_last, s_name]
                indicator_current1 = indicator_current - indicator_current_last
            else:
                indicator_current1 = df_tmp1.ix[type_current, s_name]
        except:
            indicator_current1 = np.nan  ###出现这种情况下 一般就是还没上市 所以财报不全 里面没有 无法索引
        return indicator_current1

    def convertForlastest(self, period):
        if period[4:6] == '03':
            last_period = str(int(period[:4]) - 1) + '1231'
        elif period[4:6] == '06':
            last_period = period[:4] + '0331'
        elif period[4:6] == '09':
            last_period = period[:4] + '0630'
        elif period[4:6] == '12':
            last_period = period[:4] + '0930'
        return last_period

    def cal_year_average(self, df_tmp1, s_name):
        try:
            df_tmp1 = df_tmp1.iloc[:(len(df_tmp1) - self.fre_shift)]
            type_current = df_tmp1.index.tolist()[-1]
            type_list = []
            for _ in range(4):
                type_list.append(type_current)
                type_current = self.convertForlastest(type_current)
            indicator_year_average = 0
            for type_current in type_list:
                indicator_year_average += df_tmp1.ix[type_current, s_name]
            indicator_year_average = indicator_year_average / 4.0

        except:
            indicator_year_average = np.nan  ###出现这种情况下 一般就是还没上市 所以财报不全 里面没有 无法索引
        return indicator_year_average

        #### 添加获取最新一期的财务数据   77 78 start的日期要改掉

    #    def cal_season
    def backtest_or_update(self):
        if self.flag == 0:
            for today1 in self.trade_day:
                #        today = '20110412'
                ttm_df = self.get_current_factor(today1)
                ttm_df.to_csv('%s/%s_%s_%s.csv' % (self.factor_path, self.factor_filename, self.cal_mode, today1),
                              index=None, header=None)
                #                return ttm_df

                # 用于测试历史某一天的
                #            today1 = '20110412'
                #            ttm_df = self.get_current_factor(today1)
                #            ttm_df.to_csv('%s/%s_%s_%s.csv'%(self.factor_path,self.factor_filename, self.cal_mode, today1),index=None)
                #            return ttm_df
        elif self.flag == 1:
            ttm_df = self.get_current_factor(self.today)
            ttm_df.to_csv('%s/%s_%s_%s.csv' % (self.factor_path, self.factor_filename, self.cal_mode, self.today),
                          index=None, header=None)
            return ttm_df

    def get_timerange(self, today):
        end = today
        #        if self.flag == 0:
        #            start = str(int(today[:4])-2) + today[4:]
        #        elif self.flag == 1:
        #            start = str(int(today[:4])-1) + today[4:]
        #        else:
        #            ValueError("flag key error : Please input 0 or 1")
        start = str(int(today[:4]) - 3) + today[4:]
        sql1 = self.sql + " where (ANN_DT >= %s) and (ANN_DT <= %s)" % (start, end)
        df = self.db_query(sql1)
        df = self.db_clean(df)
        return df

    def opr_error(exception):
        print type(exception)
        if isinstance(exception, ValueError):  # OperationalError
            print "I/O OperationalError"
            dt.sleep(1)
            return True
        else:
            return False

    @retry(stop_max_attempt_number=3, retry_on_exception=opr_error)
    def get_current_factor(self, today):
        df = self.get_timerange(today)

        code = df.WIND_CODE.unique().tolist()
        s_name = df.columns.tolist()[-1]

        s_time = time()

        ttm_list = []
        for i in range(len(code)):
            print('%s  : %s/%s' % (today, i, len(code)))
            df_tmp = df[df['WIND_CODE'] == code[i]]
            ###   groupby 效率十分低  程序的时间主要在这一步
            '''
            groupby 加速的方法 转成numpy  之后参考alpha-mind
            https://github.com/wegamekinglc/alpha-mind/blob/master/alphamind/data/standardize.py
            '''
            #            df_tmp1 = df_tmp.groupby(['REPORT_PERIOD']).apply(lambda x: x.iloc[-1])
            df_tmp1 = df_tmp.copy()
            df_tmp1.index = df_tmp['REPORT_PERIOD'].values.tolist()

            indicator_ttm = self.cal_current_latest_ttm(df_tmp1, s_name)
            ####
            # 如果当期ttm是缺失值  计算往前一期 往前两期的值作为结果 如果都是nan  
            if np.isnan(indicator_ttm):  ##数据里的nan 都是float64格式的 而np.nan == np.nan 这种方式是无效的
                df_tmp1 = df_tmp1.iloc[:-1]
                indicator_ttm = self.cal_current_latest_ttm(df_tmp1, s_name)
                if np.isnan(indicator_ttm):
                    df_tmp1 = df_tmp1.iloc[:-1]
                    indicator_ttm = self.cal_current_latest_ttm(df_tmp1, s_name)
                    # if np.isnan(indicator_ttm):
                    #     indicator_ttm = np.nan  # (not available)
            ttm_list.append(indicator_ttm)

        e_time = time()
        print(e_time - s_time)

        ttm_df = pd.DataFrame([])
        ttm_df['Code'] = code
        ttm_df[self.cal_mode] = ttm_list
        return ttm_df


# sql="select WIND_CODE,ANN_DT,REPORT_PERIOD,STATEMENT_TYPE,tot_assets from AShareBalanceSheet where (REPORT_PERIOD >= %s) and (REPORT_PERIOD <= %s)"%(start, end)
# tot_asset = getdb.db_query(sql)
# tot_asset = getdb.db_clean(tot_asset)



if __name__ == "__main__":
    #    sql = "select WIND_CODE,ANN_DT,REPORT_PERIOD,STATEMENT_TYPE,net_profit_incl_min_int_inc from ashareincome"
    sql = "select WIND_CODE,ANN_DT,REPORT_PERIOD,s_fa_fcff from AShareFinancialIndicator"
    # sql = "select WIND_CODE,ANN_DT,REPORT_PERIOD,STATEMENT_TYPE,FREE_CASH_FLOW from AShareCashFlow"

    factor_filename = 'fcff'

    cal_mode, flag = 'ttm', 1

    net_income = factorGet(sql, cal_mode, flag, factor_filename)

    asd1 = net_income.backtest_or_update()


# 要做  文件夹保存
#
#
#
#
#
#
#
# all_day['CALENDAR_DATE'] = pd.to_datetime(all_day['CALENDAR_DATE'], format='%Y-%m-%d')
# all_day = all_day.set_index('CALENDAR_DATE')
# all_day['t'] = 1
# all_day1 = all_day.resample('BM', how=lambda x: x.iloc[-1])
