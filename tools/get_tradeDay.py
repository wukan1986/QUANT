# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 15:55:48 2017

@author: shiyunchao
"""


import cx_Oracle
import pandas as pd
import uqer
from uqer import DataAPI


def uqer1(start, end, fre = 'day'):
    client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')
    trade_day = DataAPI.TradeCalGet(exchangeCD=u"XSHG",beginDate=start,endDate=end,field=u"",pandas="1")

    trade_day = trade_day[trade_day.isOpen==1][['calendarDate','isWeekEnd','isMonthEnd']]
    
    if fre == 'month':
        df= trade_day[trade_day.isMonthEnd == 1]['calendarDate']
    elif fre == 'week':
        df = trade_day[trade_day.isWeekEnd == 1]['calendarDate']
    elif fre == 'day':
        df = trade_day['calendarDate']
    elif fre == '2week':
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
    df.name = 'trade_day'
    return df 



def db_query(query):
    DB_HOST = '10.180.10.139:1521/WINDB'
    DB_USER = 'rwind'
    DB_PASSWORD = 'rwind'
    db=cx_Oracle.connect(DB_USER,DB_PASSWORD,DB_HOST)
    data = pd.read_sql(query, db)
    db.close()
    return data

def get_winddb(start, end):
    sql="select TRADE_DAYS from WIND.ASHARECALENDAR where (S_INFO_EXCHMARKET='SZSE') and (TRADE_DAYS>=%s) and (TRADE_DAYS<=%s)"%(start, end)
    trade_day = db_query(sql)
    trade_day = trade_day['TRADE_DAYS'].sort_values()
    trade_day.index = range(len(trade_day))
    return trade_day

def wind(start, end, fre = 'day'):
    if fre == 'day':
        df = get_winddb(start, end) 
    elif fre == 'month':
        all_day = get_winddb(start, end) 
        all_day = pd.DataFrame(all_day.values,index= range(len(all_day)))
        all_day.columns = ['TRADE_DAYS'] 
        all_day['group'] = all_day['TRADE_DAYS'].apply(lambda x: x[:6])

        all_day = all_day.groupby(['group']).apply(lambda x: x.iloc[-1])
        all_day = all_day[['TRADE_DAYS']]   
        all_day.index = range(len(all_day))
        all_day = all_day.iloc[:-1]   # 截止到上月越底
        df = all_day['TRADE_DAYS']
    df.name = 'trade_day'   
    return df
        
if __name__ =='__main__':      
    start = u'20121231'
    end = u'20170928'
    fre = 'day'

    trade = uqer1(start, end, fre = fre)
    trade1 = wind(start, end, fre = fre)       
        
        