# -*- coding: utf-8 -*-

import cx_Oracle
import pandas as pd
import uqer
from uqer import DataAPI
from WindPy import w
import datetime
import calendar
import time

def tdays(begt, endt, fre):
    w.start()
    t = w.tdays(begt, endt, 'Period=' + fre).Times
    dates = [str(i).replace('-','') for i in t]
    dates = pd.Series(dates)
    return dates

def tdaysoffset(tdate, N , fre):
    w.start()
    t = w.tdaysoffset(N, tdate, 'Period=' + fre).Times[0]
    t = str(t).replace('-','')
    return t

def ttradedayscount(begt, endt):
    w.start()
    days = w.tdayscount(begt, endt).Data[0]
    return days[0]

def tdays_uqer(start, end, fre = 'day'):
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
        print('change fre parameter')
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

def tdays_wind(start, end, fre = 'day'):
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

def tdaysoffset_uqer(tdate, N, fre, offset):
    client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')
    all_day = DataAPI.TradeCalGet(exchangeCD=u"XSHG",beginDate='19000101',endDate='20181231',field=u"",pandas="1")
    all_day = all_day[all_day.isOpen == 1]
    all_day = all_day.reset_index(drop=True)
    all_day['calendarDate'] = pd.to_datetime(all_day['calendarDate'], format='%Y-%m-%d')
    all_day['calendarDate'] = all_day['calendarDate'].apply(lambda x: x.strftime("%Y%m%d"))

    tdate_index = all_day[all_day.calendarDate <= tdate]
    tdate_index = len(tdate_index)-1
    if fre == 'day':
        t_index = tdate_index + N * offset
    elif fre == 'week':
        t_index = tdate_index + N * 5 * offset

    if t_index > len(all_day)-1:
        t = None
    elif t_index < 0:
        t = None
    else:
        t = all_day.ix[t_index, 'calendarDate']
    return t

def tmonthscount(T1, T2):
    y1 = int(T1[0:4])
    y2 = int(T2[0:4])
    m1 = int(T1[4:6])
    m2 = int(T2[4:6])
    if T1 >= T2:
        y_diff = y1-y2
        m_diff = m1-m2
    elif T1 < T2:
        y_diff = y2-y1
        m_diff = m2-m1
    return y_diff * 12 + m_diff

def tdayscount(t1, t2):
    t1 = pd.to_datetime(t1, format='%Y-%m-%d')
    t2 = pd.to_datetime(t2, format='%Y-%m-%d')
    if t1 > t2: return (t1-t2).days
    else: return (t2-t1).days

def adjedreportdate(tdate):
    if int(tdate[4:6]) in [1, 2]:
        return str(int(tdate[0:4]) - 1) + '1231'
    elif int(tdate[4:6]) in [4, 5]:
        return tdate[0:4] + '0331'
    elif int(tdate[4:6]) in [7, 8]:
        return tdate[0:4] + '0630'
    elif int(tdate[4:6]) in [10, 11]:
        return tdate[0:4] + '0930'

def reportdate_1y(tdate):
    if tdate[4:8] == '0331':
        rp0 = str(int(tdate[0:4]) - 1) + tdate[4:8]
        rp1 = str(int(tdate[0:4]) - 1) + '0630'
        rp2 = str(int(tdate[0:4]) - 1) + '0930'
        rp3 = str(int(tdate[0:4]) - 1) + '1231'
    elif tdate[4:8] == '0630':
        rp0 = str(int(tdate[0:4]) - 1) + tdate[4:8]
        rp1 = str(int(tdate[0:4]) - 1) + '0930'
        rp2 = str(int(tdate[0:4]) - 1) + '1231'
        rp3 = tdate[0:4] + '0331'
    elif tdate[4:8] == '0930':
        rp0 = str(int(tdate[0:4]) - 1) + tdate[4:8]
        rp1 = str(int(tdate[0:4]) - 1) + '1231'
        rp2 = tdate[0:4] + '0331'
        rp3 = tdate[0:4] + '0630'
    elif tdate[4:8] == '1231':
        rp0 = str(int(tdate[0:4]) - 1) + tdate[4:8]
        rp1 = tdate[0:4] + '0331'
        rp2 = tdate[0:4] + '0630'
        rp3 = tdate[0:4] + '0930'
    return [rp0, rp1, rp2, rp3]

def lastreportdate(tdate):
    if tdate[4:8] == '0331':
        return str(int(tdate[0:4]) - 1) + '1231'
    elif tdate[4:8] == '0630':
        return tdate[0:4] + '0331'
    elif tdate[4:8] == '0930':
        return tdate[0:4] + '0630'
    elif tdate[4:8] == '1231':
        return tdate[0:4] + '0930'

if __name__ =='__main__':
    start = '20170101'
    end = '20170126'
    fre = 'week'
    tdate = '20171231'
    # trade = uqer1(start, end, fre = fre)
    # trade1 = wind(start, end, fre = fre)
    # t = TradeDaysOffset(end, 1, fre, -1)
    # t = tdays(start, end, 'M')
    # t = tdaysoffset(end, -1, 'M')
    # days = tdayscount(start, end)
    # t = adjedreportdate(end)
    # t= tdayscount(start, end)
    t = reportdate_1y(tdate)
    print(t)
    a = 1
        