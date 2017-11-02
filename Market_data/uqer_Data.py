# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 11:41:00 2017

@author: shiyunchao
"""

import uqer
from uqer import DataAPI

client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')
import os
import pandas as pd
from time import strftime, localtime


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

start = '2006-01-01'
end = strftime("%Y-%m-%d",localtime())
outputpath = 'Market_uqer'
if not os.path.exists(outputpath):
    os.makedirs(outputpath)

trade_day = getTradeday(start, end, fre = 'month')
#MktEqudAdjAfGet   MktEqudGet
dt = trade_day[0]
cxzxczx
for dt in trade_day:
    df1 = DataAPI.MktEqudAdjAfGet(secID=u"",ticker='',tradeDate=dt,isOpen="",\
                            beginDate=u"",endDate=u"",field=['ticker','closePrice'],pandas="1")
    df1.columns = ['ticker','close_s']

    df1.to_csv('%s/close_%s.csv'%(outputpath,dt),index = None)

