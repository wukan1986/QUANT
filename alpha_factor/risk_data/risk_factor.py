# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 09:19:36 2017

@author: shiyunchao
"""


import os
import pandas as pd
from time import strftime, localtime
import uqer
from uqer import DataAPI

client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')


class uqer_risk():
    def __init__(self, start, end, flag, fre='month'):
    ###----传入参数----
        self.start = start  #开始时间
        self.end = end      #结束时间
        self.fre = fre  #数据频率，日度，周度，月度三个选择
        self.trade_day = self.getTradeday()
#        self.today = '20170829'
#        self.today = strftime("%Y%m%d",localtime())
        self.today = self.trade_day.iloc[-2]   ## 改成更新到前一天
        self.day_before = self.trade_day.iloc[-3]  ##前一天和前前一天的文件大小对比 ， 为了查看最新数据是否完整
        self.flag = flag        
        

    def getTradeday(self):
        trade_day = DataAPI.TradeCalGet(exchangeCD=u"XSHG",beginDate=self.start,endDate=self.end,field=u"",pandas="1")
        trade_day = trade_day[trade_day.isOpen==1][['calendarDate','isWeekEnd','isMonthEnd']]
        
        if self.fre == 'month':
            df= trade_day[trade_day.isMonthEnd == 1]['calendarDate']
        elif self.fre == 'week':
            df = trade_day[trade_day.isWeekEnd == 1]['calendarDate']
        elif self.fre == 'day':
            df = trade_day['calendarDate']
        elif self.fre == '2week':
            df = trade_day[trade_day.isWeekEnd == 1][['calendarDate','isWeekEnd']]
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

dirpath = 'E:/uqer_riskmodel/short/RMExposure/'
start = '20061201'
end = strftime("%Y%m%d",localtime())      
factor_name = ['SIZE','RESVOL','SIZENL','ULIQUIDTY']
factor_name1 = ['SIZE','RESVOL','SIZENL','LIQUIDTY']
for i in factor_name:
    if not os.path.exists(i):
        os.mkdir(i)

test =  uqer_risk(start, end, 1, fre='month')
trade_day = test.getTradeday() 

for i in trade_day:
    print(i)
    path = dirpath + i + '.csv'
    df = pd.read_csv(path,encoding = 'gb18030')
    
    df = df[['secID']+factor_name1]
    df['secID'] = df['secID'].apply(lambda x: x.split('.')[0] + '-CN')
    df = df.set_index('secID')
    df = -df
    for j in range(len(factor_name)):
        df_temp = df[[factor_name1[j]]]
        df_temp = df_temp.reset_index()
        df_temp.columns = ['S_INFO_WINDCODE', '%s_raw' % factor_name[j]]
        df_temp.to_csv('%s/%s_raw_CN_%s.csv'%(factor_name[j],factor_name[j],i), index=None)














