# -*- coding: utf-8 -*-
"""
Created on Mon Aug 28 09:08:44 2017

@author: shiyunchao
"""

import os
import pandas as pd
from time import strftime, localtime
import uqer
from uqer import DataAPI
from xutils.custom_logger import CustomLogger
from xutils.decorators import handle_exception
import numpy as np

client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')
LOGGER = CustomLogger(logger_name='updateLogger', log_level='info', log_file='update.log')


class Barra_data():
    def __init__(self, start, end, dirpath, fre='month', fre_type='short'):
        ###----传入参数----
        self.start = start  # 开始时间
        self.end = end  # 结束时间
        self.fre = fre  # 数据频率，日度，周度，月度三个选择
        self.fre_type = fre_type
        self.trade_day = self.getTradeday().iloc[:-1]  #### 第一次运行获取截止到前一天
        #        self.today = '20170829'
        #        self.today = strftime("%Y%m%d",localtime())
        self.today = self.trade_day.iloc[-1]  ## 改成更新到前一天
        self.day_before = self.trade_day.iloc[-2]  ##前一天和前前一天的文件大小对比 ， 为了查看最新数据是否完整

        if (not os.path.exists('%s' % dirpath)):
            os.mkdir('%s' % dirpath)

        if fre_type == 'short':
            self.dirpath = dirpath
        #            self.dirpath = dirpath + '/short'
        elif fre_type == 'long':
            self.dirpath = dirpath
        #            self.dirpath = dirpath + '/long'
        if (not os.path.exists('%s' % self.dirpath)) and fre_type == 'short':
            os.mkdir('%s' % self.dirpath)
        if (not os.path.exists('%s' % self.dirpath)) and fre_type == 'long':
            os.mkdir('%s' % self.dirpath)

    def getTradeday(self):
        #        if not os.path.isfile('tradeday_%s_%s.csv'%(self.start,self.end)):
        #            trade_day = DataAPI.TradeCalGet(exchangeCD=u"XSHG",beginDate=self.start,endDate=self.end,field=u"",pandas="1")
        #            trade_day.to_csv('tradeday_%s_%s.csv'%(self.start,self.end))
        #
        #        trade_day = pd.read_csv('tradeday_%s_%s.csv'%(self.start,self.end),index_col = 0)
        trade_day = DataAPI.TradeCalGet(exchangeCD=u"XSHG", beginDate=self.start, endDate=self.end, field=u"",
                                        pandas="1")
        trade_day = trade_day[trade_day.isOpen == 1][['calendarDate', 'isWeekEnd', 'isMonthEnd']]

        if self.fre == 'month':
            df = trade_day[trade_day.isMonthEnd == 1]['calendarDate']
        elif self.fre == 'week':
            df = trade_day[trade_day.isWeekEnd == 1]['calendarDate']
        elif self.fre == 'day':
            df = trade_day['calendarDate']
        elif self.fre == '2week':
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

    def replaceCode(self, x):
        if 'XSHE' in x:
            return x.replace('XSHE', 'SZ')
        elif 'XSHG' in x:
            return x.replace('XSHG', 'SH')
        ###############################################################################
        ###  因子暴露数据

    def data_merge(self, data1, data2):
        del data1['secShortName']
        del data1['updateTime']

        del data2['secShortName']
        del data2['updateTime']
        data2['SRISK'] = data2['SRISK'] / 100.0

        data3 = pd.merge(data1, data2, on='secID')
        data3['secID'] = data3['secID'].apply(lambda x: x.split('.')[0] + '-CN')

        col = [np.nan] + data3.columns.tolist()[1:-1] + [np.nan]
        df_tmp = pd.DataFrame([col, col], columns=data3.columns)
        df_new = pd.concat([df_tmp, data3])
        return df_new

    def getRMExposure1(self, i):
        data = DataAPI.RMExposureDayGet(secID=u"", ticker=u"", tradeDate="%s" % i, \
                                        beginDate=u"", endDate=u"", field=u"", pandas="1")
        del data['tradeDate']
        del data['ticker']
        del data['exchangeCD']
        path = '%s/Exposure' % self.dirpath
        if not os.path.exists(path):
            os.mkdir(path)
        if not data.empty:
            data['secID'] = data['secID'].apply(self.replaceCode)
            #            data.to_csv('%s/%s.csv'%(path, i), encoding = 'gb18030',index=None)

            self.check2 = data
        return data

    def getRMSriskShort1(self, i):
        if self.fre_type == 'short':
            data = DataAPI.RMSriskShortGet(tradeDate="%s" % i, beginDate=u"", \
                                           endDate=u"", field=u"", pandas="1")
        elif self.fre_type == 'long':
            data = DataAPI.RMSriskLongGet(tradeDate="%s" % i, beginDate=u"", \
                                          endDate=u"", field=u"", pandas="1")
        del data['tradeDate']
        del data['ticker']
        del data['exchangeCD']
        path = '%s/getRMSrisk' % self.dirpath
        #        if not os.path.exists(path):
        #            os.mkdir(path)
        if not data.empty:
            data['secID'] = data['secID'].apply(self.replaceCode)
            #            data.to_csv('%s/%s.csv'%(path, i), encoding = 'gb18030',index=None)
            self.check3 = data
        return data

    def getRMExposure(self, i):
        path = '%s/Exposure' % self.dirpath
        data1 = self.getRMExposure1(i)
        data2 = self.getRMSriskShort1(i)
        data3 = self.data_merge(data1, data2)
        data3.to_csv('%s/Exposure_%s.csv' % (path, i), encoding='gb18030', index=None, header=None)

    def getAllRMExposure(self):
        for i in self.trade_day:
            print i, 'Exposure'
            self.getRMExposure(i)

    def updateRMExposure(self):
        filename = os.listdir('%s/Exposure' % self.dirpath)
        filename = map(lambda x: x.split('.csv')[0], filename)
        #        if not self.today in filename:
        #            self.getRMExposure(self.today)
        self.getRMExposure(self.today)

    ###############################################################################
    ###  因子收益数据
    def RMFactorRet(self, i):
        data = DataAPI.RMFactorRetDayGet(tradeDate="%s" % i, beginDate=u"", endDate=u"", field=u"", pandas="1")
        del data['tradeDate']
        path = '%s/RMFactorRet' % self.dirpath
        if not os.path.exists(path):
            os.mkdir(path)
        if not data.empty:
            data.to_csv('%s/%s.csv' % (path, i), encoding='gb18030', index=None)

    def getAllRMRMFactorRet(self):
        for i in self.trade_day:
            print i, 'RMFactorRet'
            self.RMFactorRet(i)

    def updateRMRMFactorRet(self):
        filename = os.listdir('%s/RMFactorRet' % self.dirpath)
        filename = map(lambda x: x.split('.csv')[0], filename)
        #        if not self.today in filename:
        #            self.RMFactorRet(self.today)
        self.RMFactorRet(self.today)

    ###############################################################################
    ###  特质收益数据
    def getRMSpecificRet(self, i):
        data = DataAPI.RMSpecificRetDayGet(secID=u"", ticker=u"", tradeDate="%s" % i, \
                                           beginDate=u"", endDate=u"", field=u"", pandas="1")
        del data['tradeDate']
        del data['ticker']
        del data['exchangeCD']
        path = '%s/RMSpecificRet' % self.dirpath
        if not os.path.exists(path):
            os.mkdir(path)
        if not data.empty:
            data['secID'] = data['secID'].apply(self.replaceCode)
            data.to_csv('%s/%s.csv' % (path, i), encoding='gb18030', index=None)

    def getAllRMSpecificRet(self):
        for i in self.trade_day:
            print i, 'RMSpecificRet'
            self.getRMSpecificRet(i)

    def updateRMSpecificRet(self):
        filename = os.listdir('%s/RMSpecificRet' % self.dirpath)
        filename = map(lambda x: x.split('.csv')[0], filename)
        #        if not self.today in filename:
        #            self.getRMSpecificRet(self.today)
        self.getRMSpecificRet(self.today)

    ###############################################################################
    ###  风险因子协方差矩阵
    #  day
    # data = DataAPI.RMCovarianceDayGet(tradeDate="20170517",beginDate=u"",endDate=u"",Factor=u"",field=u"",pandas="1")

    def getRMCovarianceShort(self, i):
        if self.fre_type == 'short':
            data = DataAPI.RMCovarianceShortGet(tradeDate="%s" % i, beginDate=u"", \
                                                endDate=u"", Factor=u"", field=u"", pandas="1")
        elif self.fre_type == 'long':
            data = DataAPI.RMCovarianceLongGet(tradeDate="%s" % i, beginDate=u"", \
                                               endDate=u"", Factor=u"", field=u"", pandas="1")
        del data['tradeDate']
        del data['FactorID']
        del data['updateTime']
        path = '%s/Covariance' % self.dirpath
        if not os.path.exists(path):
            os.mkdir(path)
        if not data.empty:
            df1 = pd.DataFrame([np.nan] + data.columns[1:].tolist())
            df1 = df1.T
            df1.columns = data.columns

            df_new = pd.concat([df1, data])
            df_new.iloc[1:, 1:] = df_new.iloc[1:, 1:] / 10000.0
            df_new.to_csv('%s/Covariance_%s.csv' % (path, i), encoding='gb18030', index=None, header=None)
            self.check1 = df_new

    def getAllRMCovarianceShort(self):
        for i in self.trade_day:
            print i, 'RMCovariance'
            self.getRMCovarianceShort(i)

    def updateRMCovarianceShort(self):
        filename = os.listdir('%s/Covariance' % self.dirpath)
        filename = map(lambda x: x.split('.csv')[0], filename)
        #        if not self.today in filename:
        #            self.getRMCovarianceShort(self.today)
        self.getRMCovarianceShort(self.today)

    ###############################################################################
    @handle_exception(logger=LOGGER)
    def check(self, path, pathx):  # 最后检测是否更新了
        try:
            data = pd.read_csv(path, encoding='gb18030')
        except:
            raise ValueError('Update failed !!!  try again')
        if data.empty:  ##其实这步不可能发生  更新时空的已经过滤了
            raise ValueError('New data is empty!!!  try again')
        filesize = os.path.getsize(path) / 1024
        filesizex = os.path.getsize(pathx) / 1024
        if abs(filesize - filesizex) >= 10:
            raise ValueError('New data in %s is not complete!  check !!!' % path.split('/')[-2])
        else:
            print('Done!  update %s' % path.split('/')[-2])

    def check_csv(self):
        #        path = '%s/getRMCovariance'%self.dirpath
        #        to_csv('%s/Covariance_%s.csv'%(path, i), encoding = 'gb18030',index=None, header=None)
        self.path1 = '%s/Exposure/Exposure_%s.csv' % (self.dirpath, self.today)
        self.path4 = '%s/Covariance/Covariance_%s.csv' % (self.dirpath, self.today)
        self.path11 = '%s/Exposure/Exposure_%s.csv' % (self.dirpath, self.day_before)
        self.path44 = '%s/Covariance/Covariance_%s.csv' % (self.dirpath, self.day_before)
        self.check(self.path1, self.path11)
        self.check(self.path4, self.path44)


if __name__ == "__main__":

    start = '2017-12-04'
    end = strftime("%Y-%m-%d", localtime())
    fre = 'day'  #
    #    dirpath = '../database'
    dirpath = 'Z:/daily_data/uqer_model'
    fre_type1 = 'short'  # long

    firstRun_update = 1  ##  1代表第一次运行  0代表更新

    test1 = Barra_data(start, end, dirpath, fre, fre_type1)
    if firstRun_update == 1:
        test1.getAllRMExposure()
        #        test1.getAllRMRMFactorRet()
        #        test1.getAllRMSpecificRet()
        test1.getAllRMCovarianceShort()
    #        test1.getAllRMSriskShort()
    elif firstRun_update == 0:
        test1.updateRMExposure()
        #        test1.updateRMRMFactorRet()
        #        test1.updateRMSpecificRet()
        test1.updateRMCovarianceShort()
        #        test1.updateRMSriskShort()
        test1.check_csv()
